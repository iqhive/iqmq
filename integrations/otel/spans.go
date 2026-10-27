package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	mq "github.com/iqhive/iqmq"
)

// now reads the configured clock, or the wall clock.
func (i *Instrumentation) now() time.Time {
	if i.cfg.Clock != nil {
		return i.cfg.Clock.Now()
	}
	return time.Now()
}

// PublishMiddleware returns the publish middleware that opens the specification's
// `publish` span and writes the trace context into every message
// (mq.PublishMiddleware, installed with middleware.WrapPublisher).
//
//	pub, err := middleware.WrapPublisher(handle, i.PublishMiddleware())
//
// Two things happen here rather than in a hook, and both need what a hook
// does not have. The span needs a context.Context to be a child of the
// caller's span — mq.Hooks has none, and PublishStartEvent and
// PublishDoneEvent carry no token that would let a hook pair a start with its
// own done — and the injection needs the message, to put a header on it.
//
// The trace context is written even with [Spans.NoPublish] set: a consumer's
// process span needs a parent whether or not the producer recorded a span of
// its own.
//
// When nothing is instrumented this is the identity, so a chain built with it
// has no publish middleware at all and costs nothing per publication.
func (i *Instrumentation) PublishMiddleware() mq.PublishMiddleware {
	return i.PublishMiddlewareFor("")
}

// PublishMiddlewareFor is [Instrumentation.PublishMiddleware] for a publisher
// BOUND to one topic, which is the common case and the one where the chain
// cannot work the destination out for itself.
//
// mq.PublishFunc's topic argument is read from Message.Topic by
// middleware.WrapPublisher, and the specification says a bound publisher IGNORES
// Message.Topic — the handle's own topic wins. So a publication through a
// bound handle reaches a publish chain with an empty topic, and a span built
// from it would be called "publish" with no `messaging.destination.name` at
// all: present, plausible and impossible to group by. Naming the topic here
// is the only place that knows it:
//
//	pub, _ := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
//	wrapped, err := middleware.WrapPublisher(pub, i.PublishMiddlewareFor("orders"))
//
// A topic the call itself carries always wins over this one, so the same
// middleware is correct on an unbound publisher too.
func (i *Instrumentation) PublishMiddlewareFor(bound string) mq.PublishMiddleware {
	if !i.tracing {
		return identityPublish
	}
	span := !i.cfg.Spans.NoPublish
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
			if !span {
				i.injectAll(ctx, ms)
				return next(ctx, topic, ms, opts)
			}
			named := topic
			if named == "" {
				named = bound
			}
			set := i.setFor(opPublish, named, "")
			ctx, sp := i.tracer.Start(ctx, set.name, set.start...)
			if len(ms) > 1 {
				// The batch count is the one attribute that cannot be
				// cached: it is a property of the call, not of the
				// destination. It is set only for a real batch, so a
				// single publication pays nothing for it.
				sp.SetAttributes(batchCount(len(ms)))
			}
			i.injectAll(ctx, ms)
			err := next(ctx, topic, ms, opts)
			finish(sp, err)
			return err
		}
	}
}

// identityPublish is the disabled publish middleware: it returns the chain it
// was given, so composition removes it entirely.
func identityPublish(next mq.PublishFunc) mq.PublishFunc { return next }

// injectAll writes the propagation headers onto every message of a
// publication.
func (i *Instrumentation) injectAll(ctx context.Context, ms []*mq.Message) {
	for _, m := range ms {
		if m != nil {
			i.inject(ctx, &m.Headers)
		}
	}
}

// Middleware returns the handler middleware that opens the specification's `process`
// span and records the processing-time and end-to-end-latency histograms
// (mq.Middleware, installed with mq.WithMiddleware).
//
//	consumer.Run(ctx, handler, mq.WithMiddleware(i.Middleware()))
//
// The span's parent is the PUBLISHER's span, read back out of the delivery's
// headers and marked remote, which is what makes one trace span the broker.
// A delivery whose headers carry no trace context, or a malformed one, starts
// a root span instead: the specification makes broker-delivered metadata untrusted, so a
// publisher must not be able to break a consumer by writing nonsense into a
// header.
//
// When nothing is instrumented this is the identity.
func (i *Instrumentation) Middleware() mq.Middleware {
	if !i.Enabled() {
		return identityHandler
	}
	span := i.tracing && !i.cfg.Spans.NoProcess
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			info := d.Info()
			set := i.setFor(opProcess, info.Topic, info.Subscription)
			ctx = i.extract(ctx, &d.Headers)
			var sp trace.Span
			if span {
				ctx, sp = i.tracer.Start(ctx, set.name, set.start...)
			}
			var start time.Time
			if i.metrics {
				start = i.now()
			}
			// The runner recovers a panic OUTSIDE the whole middleware chain,
			// so a panicking handler never returns here: without this its
			// span would never end or be exported and the call would record
			// no duration. The panic is observed and then continues with the
			// same value, so the runner's recovery still happens.
			defer func() {
				if v := recover(); v != nil {
					i.processed(ctx, d, set, sp, start, &mq.PanicError{Value: v})
					panic(v)
				}
			}()
			err := next.Handle(ctx, d)
			i.processed(ctx, d, set, sp, start, err)
			return err
		})
	}
}

// processed records the end of one handler call: the processing-time and
// end-to-end histograms when metrics are on, and the process span when there
// is one.
func (i *Instrumentation) processed(ctx context.Context, d *mq.Delivery, set *attrSet, sp trace.Span, start time.Time, err error) {
	if i.metrics {
		end := i.now()
		i.inst.handlerDuration.Record(ctx, end.Sub(start).Seconds(), set.record...)
		// End-to-end latency needs a creation timestamp, which only a
		// publisher configured to stamp one produces (the publish path never
		// reads the clock on its own). A message without one contributes no
		// observation rather than an observation measured from the zero
		// time.
		if created := d.Properties.CreatedAt; !created.IsZero() {
			i.inst.e2eDuration.Record(ctx, start.Sub(created).Seconds(), set.record...)
		}
	}
	if sp != nil {
		finish(sp, err)
	}
}

// identityHandler is the disabled handler middleware.
func identityHandler(next mq.Handler) mq.Handler { return next }

// Hooks returns the core's observation points.
//
//	client := mq.NewClient(backend, mq.WithHooks(i.Hooks()))
//
// A disabled instrumentation returns the ZERO mq.Hooks, so the core builds no
// event, reads no clock and calls nothing — the disabled observer path,
// reached by not installing anything rather than by a branch per message.
//
// Each field is set only when something needs it: OnPublishDone when metrics
// are on (the publish span comes from [Instrumentation.PublishMiddleware]),
// OnReceive and OnSettle when their spans are on. OnPublishStart is never
// set: the specification gives it no metric and it cannot be paired with its own
// OnPublishDone.
//
// OnReconnect is never set either, and that is not an omission this package
// can fix: nothing in the core fires it, a driver's own Config is where the
// event exists at v0, and there is no reconnect span or instrument in the specification's
// OpenTelemetry sentence — the metric it names, iqmq_reconnects_total, is the
// Prometheus integration's.
func (i *Instrumentation) Hooks() mq.Hooks {
	var h mq.Hooks
	if i.metrics {
		h.OnPublishDone = i.onPublishDone
	}
	if i.tracing && !i.cfg.Spans.NoReceive {
		h.OnReceive = i.onReceive
	}
	if i.tracing && !i.cfg.Spans.NoSettle {
		h.OnSettle = i.onSettle
	}
	return h
}

// onPublishDone records the publish-latency histogram.
func (i *Instrumentation) onPublishDone(ctx context.Context, ev mq.PublishDoneEvent) {
	set := i.setFor(opPublish, ev.Topic, "")
	// The hook's own context, not Background: a metric recorded with a
	// context carries whatever baggage and exemplar the caller's span has,
	// and there is now a context to carry.
	i.inst.publishDuration.Record(ctx, ev.Duration.Seconds(), set.record...)
}

// onReceive records the specification's `receive` span over one fetch.
//
// It is a CHILD of whatever span the fetch happened inside, which is the whole
// point of the hooks taking a context: this recorded a root span until
// hooks v2 gave it one — right in time and nowhere in the trace (closed). The
// runner's own fetches run under the context Run was given, so their spans
// have a parent only when Run was started inside one, and are root spans
// otherwise. Its start and end come from the event's own Duration measured
// back from the clock.
func (i *Instrumentation) onReceive(ctx context.Context, ev mq.ReceiveEvent) {
	set := i.setFor(opReceive, ev.Topic, ev.Subscription)
	end := i.now()
	// The hook's own context, so the span lands where the operation is:
	// this was context.Background() and recorded a ROOT span, right in time
	// and nowhere in the trace (closed by hooks v2).
	_, sp := i.tracer.Start(ctx, set.name,
		trace.WithTimestamp(end.Add(-ev.Duration)), set.kindOpt, set.attrOpt,
	)
	if ev.Messages > 1 {
		sp.SetAttributes(batchCount(ev.Messages))
	}
	if ev.Err != nil {
		sp.RecordError(ev.Err)
		sp.SetStatus(codes.Error, ev.Err.Error())
	}
	sp.End(trace.WithTimestamp(end))
}

// onSettle records the specification's `settle` span over one settlement.
//
// It is a root span. OnSettle is given context.Background(), because a
// settlement has no context of its own: Delivery.Ack takes none, and the
// runner and its lease scheduler settle through the same methods, so there
// is nothing to parent it to. Its `messaging.operation.name` is the
// settlement's own -- ack, nack or extend -- under the pinned operation type
// "settle".
func (i *Instrumentation) onSettle(ctx context.Context, ev mq.SettleEvent) {
	variant := uint8(0)
	if ev.Kind.Valid() {
		variant = uint8(ev.Kind)
	}
	set := i.setForVariant(opSettle, variant, ev.Topic, ev.Subscription)
	end := i.now()
	// The hook's own context, which today holds no span: if the core ever
	// gives a settlement one, the span becomes its child with no change
	// here.
	_, sp := i.tracer.Start(ctx, set.name,
		trace.WithTimestamp(end.Add(-ev.Duration)), set.kindOpt, set.attrOpt,
	)
	if ev.Err != nil {
		sp.RecordError(ev.Err)
		sp.SetStatus(codes.Error, ev.Err.Error())
	}
	sp.End(trace.WithTimestamp(end))
}

// finish ends a span, recording the error when there is one.
//
// A failure is recorded as an event and a status, and the status description
// is the error's own message — which the specification already keeps free of payloads,
// credentials and addresses, so a span is not a second place those could
// leak.
func finish(sp trace.Span, err error) {
	if err != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, err.Error())
	}
	sp.End()
}

package prometheus

import (
	"context"
	"errors"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"

	mq "bitbucket.org/iqhive/iqmq"
)

// publisherSeries caches the per-topic children of the publish families.
type publisherSeries struct {
	m       *Metrics
	entries cache[string, *pubEntry]
}

// pubEntry is every child one topic needs on the publish path. They are
// resolved once and held, because prometheus.CounterVec.WithLabelValues hashes
// a label slice on every call and rule 2 forbids that per message.
type pubEntry struct {
	messagesOK   prom.Counter
	messagesFail prom.Counter
	bytes        prom.Counter
	durationOK   prom.Observer
	durationFail prom.Observer
	batch        prom.Observer
}

func (p *publisherSeries) init(m *Metrics) {
	p.m = m
	p.entries.limit = m.card.MaxSeries
}

// get resolves the children for a topic.
func (p *publisherSeries) get(topic string) *pubEntry {
	return p.entries.get(p.m.topicLabel(topic), p.build, p.buildOverflow)
}

func (p *publisherSeries) buildOverflow() *pubEntry { return p.build(p.m.card.Overflow) }

func (p *publisherSeries) build(topic string) *pubEntry {
	m := p.m
	return &pubEntry{
		messagesOK:   m.publishMessages.WithLabelValues(topic, outcomeSuccess),
		messagesFail: m.publishMessages.WithLabelValues(topic, outcomeFailure),
		bytes:        m.publishBytes.WithLabelValues(topic),
		durationOK:   m.publishDuration.WithLabelValues(topic, outcomeSuccess),
		durationFail: m.publishDuration.WithLabelValues(topic, outcomeFailure),
		batch:        m.publishBatch.WithLabelValues(topic),
	}
}

// consumerSeries caches the per-(topic, subscription) children of the consume
// families.
type consumerSeries struct {
	m       *Metrics
	entries cache[conKey, *conEntry]
}

// conEntry is every child one (topic, subscription) needs.
type conEntry struct {
	consumed prom.Counter
	// handler is indexed by outcome: success, failure, panic.
	handler [3]prom.Observer
	// settle is indexed by mq.SettleKind: ack, nack, extend.
	settle [3]prom.Observer
	// settleFail is indexed the same way, and the extend slot is nil: an
	// extension is not a settlement , so a
	// `kind="extend"` series of iqmq_settle_failures_total could never be
	// anything but zero — and a permanently-zero series reads as evidence
	// that extensions never fail, which is what iqmq_lease_extensions_total
	// is for.
	settleFail [3]prom.Counter
	extendOK   prom.Counter
	extendFail prom.Counter
}

func (c *consumerSeries) init(m *Metrics) {
	c.m = m
	c.entries.limit = m.card.MaxSeries
}

// get resolves the children for a topic and subscription.
func (c *consumerSeries) get(topic, sub string) *conEntry {
	return c.entries.get(conKey{c.m.topicLabel(topic), c.m.subLabel(sub)}, c.build, c.buildOverflow)
}

func (c *consumerSeries) buildOverflow() *conEntry {
	return c.build(conKey{c.m.card.Overflow, c.m.card.Overflow})
}

func (c *consumerSeries) build(k conKey) *conEntry {
	m := c.m
	e := &conEntry{consumed: m.consumeMessages.WithLabelValues(k.topic, k.sub)}
	for i, o := range [3]string{outcomeSuccess, outcomeFailure, outcomePanic} {
		e.handler[i] = m.handlerDuration.WithLabelValues(k.topic, k.sub, o)
	}
	for i := range e.settle {
		kind := mq.SettleKind(i).String()
		e.settle[i] = m.settleDuration.WithLabelValues(k.topic, k.sub, kind)
		if mq.SettleKind(i) != mq.SettleExtend {
			e.settleFail[i] = m.settleFailures.WithLabelValues(k.topic, k.sub, kind)
		}
	}
	e.extendOK = m.leaseExtensions.WithLabelValues(k.topic, k.sub, outcomeSuccess)
	e.extendFail = m.leaseExtensions.WithLabelValues(k.topic, k.sub, outcomeFailure)
	return e
}

// Hooks returns the core's observation points wired to this metric set
//
// Install them on the client:
//
//	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
//	client := mq.NewClient(backend, mq.WithHooks(m.Hooks()))
//
// OnPublishStart is deliberately nil. The specification gives the start point no metric
// of its own — every publish family is about a publication's outcome — and a
// hook that is set costs the core an event value and a call for nothing.
//
// OnReconnect is set, and nothing in the core fires it: a Backend is built
// before the Client that holds the hooks, so at v0 the reconnect event
// reaches a driver's own Config and not this one. Pass the same Hooks value
// to the driver that offers it. Passing it to BOTH counts every reconnection
// twice; see [Unsourced].
func (m *Metrics) Hooks() mq.Hooks {
	return mq.Hooks{
		OnPublishDone: m.onPublishDone,
		OnReceive:     m.onReceive,
		OnSettle:      m.onSettle,
		OnReconnect:   m.onReconnect,
	}
}

// onPublishDone feeds iqmq_publish_messages_total, _publish_bytes_total,
// _publish_duration_seconds and _publish_batch_size.
func (m *Metrics) onPublishDone(ctx context.Context, ev mq.PublishDoneEvent) {
	e := m.pub.get(ev.Topic)
	secs := ev.Duration.Seconds()
	e.batch.Observe(float64(ev.Messages))
	// The bytes left this process whether or not the broker took them, and
	// _publish_bytes_total carries no outcome label to split them by.
	if ev.Bytes != 0 {
		e.bytes.Add(float64(ev.Bytes))
	}
	failed := failedOf(ev.Err, ev.Messages)
	switch {
	case failed == 0:
		e.messagesOK.Add(float64(ev.Messages))
		e.durationOK.Observe(secs)
	case failed >= ev.Messages:
		e.messagesFail.Add(float64(ev.Messages))
		e.durationFail.Observe(secs)
	default:
		// A partial batch is neither: the *mq.BatchError is index-aligned,
		// so the two counters get the real split rather than the whole batch
		// landing on one of them. The duration is one measurement of one
		// call and is recorded once, as a failure, because that is what the
		// caller was told.
		e.messagesOK.Add(float64(ev.Messages - failed))
		e.messagesFail.Add(float64(failed))
		e.durationFail.Observe(secs)
	}
}

// failedOf is how many messages of a publication failed: none, all, or the
// count an index-aligned *mq.BatchError names.
func failedOf(err error, messages int) int {
	if err == nil {
		return 0
	}
	if be := batchOf(err); be != nil {
		return be.Failed()
	}
	return messages
}

// batchOf finds a *mq.BatchError in err's chain.
//
// It walks the chain by hand instead of calling errors.As, and the reason is
// the whole point of this package: errors.As takes its target as an `any`, so
// `errors.As(err, &be)` makes the local escape and costs ONE ALLOCATION PER
// FAILED PUBLICATION. A broker outage fails every publication, which is
// exactly when the metrics path must not start allocating — an observer that
// changes the path it observes under load is worse than no observer. A shared
// pre-boxed target would fix the allocation and introduce a data race across
// concurrent publishers, which is worse again.
//
// The walk covers a wrapped BatchError, which is what the core returns
// ("ErrPartial's *mq.BatchError"). A BatchError inside a multi-error
// Unwrap() []error is not a publication outcome and is not looked for.
func batchOf(err error) *mq.BatchError {
	for range maxUnwrap {
		//nolint:errorlint // errors.As allocates its escaping target on every
		// failed publication; see the comment above. The chain is walked
		// explicitly so the wrapped case still works.
		if be, ok := err.(*mq.BatchError); ok {
			return be
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil
		}
		if err = u.Unwrap(); err == nil {
			return nil
		}
	}
	return nil
}

// maxUnwrap bounds the error chain batchOf walks. The specification makes a driver's error
// chain untrusted input like any other, and an unbounded walk over a chain a
// driver built is an unbounded loop on the publish path.
const maxUnwrap = 16

// onReceive feeds iqmq_consume_messages_total.
//
// A fetch that returned nothing is not counted and is not an error: the specification's
// Fetch blocks until it has a delivery or the context is done, so an empty
// return is a closing consumer rather than an event a counter should record.
func (m *Metrics) onReceive(ctx context.Context, ev mq.ReceiveEvent) {
	if ev.Messages <= 0 {
		return
	}
	m.con.get(ev.Topic, ev.Subscription).consumed.Add(float64(ev.Messages))
}

// onSettle feeds iqmq_settle_duration_seconds, _settle_failures_total and
// _lease_extensions_total.
//
// A lease extension reports itself as a settlement of kind "extend" (the specification's
// label), so it lands in both the duration histogram — which has the label —
// and the extension counter, which is the one the specification gives an outcome. It is
// NOT counted in _settle_failures_total: an extension is not a settlement.
func (m *Metrics) onSettle(ctx context.Context, ev mq.SettleEvent) {
	e := m.con.get(ev.Topic, ev.Subscription)
	i := int(ev.Kind)
	if i < 0 || i >= len(e.settle) {
		return
	}
	e.settle[i].Observe(ev.Duration.Seconds())
	if ev.Kind == mq.SettleExtend {
		if ev.Err != nil {
			e.extendFail.Inc()
		} else {
			e.extendOK.Inc()
		}
		return
	}
	if ev.Err != nil && e.settleFail[i] != nil {
		e.settleFail[i].Inc()
	}
}

// onReconnect feeds iqmq_reconnects_total.
func (m *Metrics) onReconnect(ctx context.Context, ev mq.ReconnectEvent) {
	outcome := outcomeSuccess
	if ev.Err != nil {
		outcome = outcomeFailure
	}
	m.reconnects.WithLabelValues(labelValue(ev.Driver), outcome).Inc()
}

// Recorder returns the middleware.Recorder that feeds
// iqmq_handler_duration_seconds.
//
// It is the interface the middleware.Metrics takes, so the handler families
// are installed with the middleware set and not with the hooks:
//
//	mq.WithMiddleware(middleware.Metrics(m.Recorder(), nil))
//
// The interface is satisfied structurally and this package does not import
// middleware for it: the root module's middleware package would otherwise be
// a compile-time dependency of every program that only wants the hooks.
func (m *Metrics) Recorder() HandlerRecorder { return recorder{m} }

// HandlerRecorder is the handler-observation interface middleware.Recorder
// declares. It is restated here rather than imported so that this package's
// dependency on the root module is the SPI and not the middleware set; a
// *Metrics's Recorder satisfies middleware.Recorder because the method set
// is identical.
type HandlerRecorder interface {
	// HandlerDuration records one handler call. outcome is
	// middleware.OutcomeSuccess, OutcomeFailure or OutcomePanic.
	HandlerDuration(topic, subscription, outcome string, d time.Duration)
}

// recorder adapts *Metrics to HandlerRecorder.
type recorder struct{ m *Metrics }

// HandlerDuration observes one handler call.
func (r recorder) HandlerDuration(topic, subscription, outcome string, d time.Duration) {
	e := r.m.con.get(topic, subscription)
	switch outcome {
	case outcomeSuccess:
		e.handler[0].Observe(d.Seconds())
	case outcomeFailure:
		e.handler[1].Observe(d.Seconds())
	case outcomePanic:
		e.handler[2].Observe(d.Seconds())
	default:
		// An outcome this package does not know is an outcome the middleware
		// set grew; record it under its own label rather than dropping the
		// observation or guessing.
		r.m.handlerDuration.WithLabelValues(
			r.m.topicLabel(topic), r.m.subLabel(subscription), labelValue(outcome),
		).Observe(d.Seconds())
	}
}

// Middleware returns a handler middleware that records the handler families
// without the middleware set: it is middleware.Metrics(m.Recorder(), clk)
// written out, for a program that installs no other middleware.
//
// It does NOT count redeliveries. iqmq_redeliveries_total comes from
// ConsumerStats.Redelivered through [Metrics.AddConsumer], because that
// counts every delivery the consumer handed out and this sees only the ones
// that reached a handler — and a quantity with two sources double-counts.
func (m *Metrics) Middleware(clk mq.Clock) mq.Middleware {
	rec := recorder{m}
	now := time.Now
	if clk != nil {
		now = clk.Now
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			start := now()
			err := next.Handle(ctx, d)
			info := d.Info()
			rec.HandlerDuration(info.Topic, info.Subscription, outcomeOf(err), now().Sub(start))
			return err
		})
	}
}

// outcomeOf maps a handler result to its label value, the same way
// middleware.Metrics does.
func outcomeOf(err error) string {
	switch {
	case err == nil:
		return outcomeSuccess
	case isPanic(err):
		return outcomePanic
	default:
		return outcomeFailure
	}
}

// isPanic reports whether err is a recovered panic.
func isPanic(err error) bool {
	var pe *mq.PanicError
	return errors.As(err, &pe)
}

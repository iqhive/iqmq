// Command otel-tracing is the API's span tree, printed: a caller's
// span, the publish span under it, and — after the message has crossed a
// broker — the process span under THAT, in one trace.
//
// It runs over the in-process memory broker and over Redis Streams
// (github.com/iqhive/iqmq/drivers/redis), and the tree is the same on both,
// because what carries the trace across the broker is a W3C `traceparent`
// header on the message and nothing else.
//
// # The four spans, and why only two of them are in the tree
//
//	publish   mq.PublishMiddleware   child of the caller's span; writes the
//	                                 traceparent into the message
//	process   mq.Middleware          child of the PUBLISHER's span, read back
//	                                 out of the delivery's headers
//	receive   mq.Hooks.OnReceive     a ROOT span over one fetch
//	settle    mq.Hooks.OnSettle      a ROOT span over one settlement
//
// The first two are proper children and they are what makes one trace span the
// broker. The last two cannot be: mq.Hooks carries no context.Context, so
// there is nothing to parent them to and nothing to put them in. They are
// recorded with explicit start and end timestamps and a reader sees them
// BESIDE the trace rather than inside it. That is a gap in the hook surface
// rather than a choice , and this program prints it as a
// result instead of leaving a reader to discover it in a trace viewer.
//
// # What this program does NOT do
//
// It exports nothing. The spans go to an in-memory recorder and the metrics to
// a manual reader, because the subject is the SHAPE of the telemetry and an
// OTLP exporter would make the example about a collector. Swapping the two
// providers below for otlptracegrpc and a periodic reader is the whole change
// a service makes.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	otelmq "github.com/iqhive/iqmq/integrations/otel"

	mq "github.com/iqhive/iqmq"
	rd "github.com/iqhive/iqmq/drivers/redis"
	"github.com/iqhive/iqmq/middleware"
)

// The settlement operation names the settle span can carry. They are the
// settlement's own -- a settle span is called "ack <topic>", never
// "settle <topic>" -- and this list is what identifies one.
var settleOperations = []string{settleAck, "nack", "extend"}

// settleAck is the settlement this program makes.
const settleAck = "ack"

// stampTimestamps is whether the publisher fills Properties.CreatedAt.
//
// It is a variable with one writer -- TestWithoutTimestampTheEndToEndHistogramIsSilent
// -- and it exists because the consequence of leaving it false is the kind of
// silence a test has to be able to observe: no error, no log line, and one of the three histograms simply absent from the export. The example itself
// never changes it.
var stampTimestamps = true

// The names this program uses.
const (
	// callerSpan is the span a service's own request handler would already
	// have open. It is the root of the trace, and it is here because a
	// publish span with no parent proves nothing about parenting.
	callerSpan = "checkout"
	// topicBase is the logical topic; newTopic makes it unique per run.
	topicBase = "orders"
	// subscription is the consumer group.
	subscription = "billing"
	// body is the one message.
	body = "order-1001 placed"
)

// newTopic returns a topic name nothing else is using.
//
// A Redis stream and its consumer group outlive the process, so a second run
// of a shared name inherits the first run's messages -- and this program
// counts spans, one per message. The lesson.
func newTopic() string {
	return topicBase + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func main() {
	if err := demo(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// demo is main's body as a function that returns.
func demo() error {
	addr := os.Getenv("IQMQ_REDIS_ADDR")
	if addr == "" {
		// conformance/harness's convention: IQMQ_<BROKER>_ADDR names an
		// externally managed broker as host:port.
		addr = "127.0.0.1:6379"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return run(ctx, os.Stdout, "redis", func(c context.Context, hooks mq.Hooks) (*mq.Client, error) {
		return rd.Open(c, Config(addr, hooks))
	})
}

// Config is the Redis configuration the broker arm uses, exported so that the
// test cannot fork it.
func Config(addr string, hooks mq.Hooks, mutate ...func(*rd.Config)) rd.Config {
	cfg := rd.Config{
		Addrs: []string{addr},
		// The specification forbids Open from creating production topology; this is the
		// caller saying "development stream, make it".
		AutoCreate: true,
		// The hooks are how the receive and settle spans are reached at
		// all: the core raises the events and the instrumentation turns them
		// into spans.
		Hooks: hooks,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	return cfg
}

// telemetry is the OpenTelemetry SDK, an in-memory recorder for the spans and
// a manual reader for the metrics.
type telemetry struct {
	inst   *otelmq.Instrumentation
	spans  *tracetest.SpanRecorder
	tracer trace.Tracer
	tp     *sdktrace.TracerProvider
	reader *metric.ManualReader
}

// newTelemetry builds the SDK and the instrumentation.
//
// system is `messaging.system`, which the convention requires and which every
// messaging dashboard groups by. It is the DRIVER's name for the broker
// family, so it is a parameter rather than a constant: the same program
// instrumented as "memory" and as "redis" produces the same tree with a
// different value on that one attribute.
func newTelemetry(system string) (*telemetry, error) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	inst, err := otelmq.New(otelmq.Config{
		TracerProvider: tp,
		MeterProvider:  mp,
		System:         system,
	})
	if err != nil {
		return nil, err
	}
	if !inst.Enabled() {
		// A Config with neither provider is the disabled path, whose
		// Hooks() is the zero mq.Hooks and whose middlewares are the
		// identity. An example that had accidentally built that would print
		// "0 spans" and look like a broken broker.
		return nil, errors.New("the instrumentation is disabled, so nothing below would be recorded")
	}
	return &telemetry{inst: inst, spans: rec, tracer: tp.Tracer("example/otel-tracing"), tp: tp, reader: reader}, nil
}

// run is the whole program.
//
// openClient is the arm's own constructor and it takes the hooks, because a
// client's hooks are fixed when it is built: there is no way to
// instrument a client after the fact, which is why this is a parameter and not
// a client.
func run(ctx context.Context, out io.Writer, system string, openClient func(context.Context, mq.Hooks) (*mq.Client, error)) error {
	tel, err := newTelemetry(system)
	if err != nil {
		return err
	}
	defer func() { _ = tel.tp.Shutdown(ctx) }()

	cl, err := openClient(ctx, tel.inst.Hooks())
	if err != nil {
		return err
	}
	defer func() { _ = cl.Shutdown(ctx) }()
	_, _ = fmt.Fprintf(out, "instrumented: driver %s as messaging.system=%s\n", cl.Info().Name, system)

	topic := newTopic()
	defer dropTopic(ctx, cl, topic)
	consumer, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: subscription})
	if err != nil {
		return err
	}
	// The in-flight gauge needs a handle to read, and this is the only way to
	// give it one: the gauge is a callback and a consumer has to register
	// itself.
	tel.inst.AddConsumer(topic, subscription, consumer.Stats)

	if err := publish(ctx, cl, tel, topic); err != nil {
		return err
	}
	if err := consume(ctx, consumer, tel); err != nil {
		return err
	}
	// Every span is ended and every histogram recorded before anything is
	// read: the consume step above waits for Run to RETURN, which is where
	// the settle hook and the process span have finished.
	return describe(ctx, out, tel, topic)
}

// publish sends one message inside a caller's span, through a publish chain
// that carries the instrumentation.
func publish(ctx context.Context, cl *mq.Client, tel *telemetry, topic string) error {
	// Timestamp is not decoration. The end-to-end histogram is measured
	// from Properties.CreatedAt to the handler being called, and the rule
	// is that "Publish never generates IDs or timestamps on the hot path
	// unless configured" -- so a publisher without this stamps nothing, the
	// consumer has no origin to measure from, and iqmq_end_to_end_duration
	// records NO POINT AT ALL. Nothing reports that; the series is simply
	// absent from the export, which is indistinguishable from a consumer that
	// is not running.
	handle, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic, Timestamp: stampTimestamps})
	if err != nil {
		return err
	}
	// PublishMiddlewareFor and not PublishMiddleware, because this publisher
	// is BOUND to a topic: the specification makes a bound handle ignore Message.Topic, so
	// the chain cannot work the destination out for itself and a span built
	// from it would carry no messaging.destination.name at all.
	pub, err := middleware.WrapPublisher(handle, tel.inst.PublishMiddlewareFor(topic))
	if err != nil {
		return fmt.Errorf("wrap publisher: %w", err)
	}

	// The caller's span. Everything about this example rests on it: the
	// publish span is a CHILD of whatever span the context carries, and a
	// program that published outside one would produce a publish span that
	// was a root and could not tell a broken parent from no parent.
	ctx, span := tel.tracer.Start(ctx, callerSpan)
	defer span.End()
	m := &mq.Message{Body: []byte(body), ID: "order-1001"}
	if err := pub.Publish(ctx, m); err != nil {
		return fmt.Errorf("publishing: %w", err)
	}
	// The traceparent is on the message NOW, written by the middleware. It is
	// checked here, on the publish side, because it is the one fact the
	// consumer side cannot distinguish from "the broker dropped the header":
	// a process span with no parent looks the same either way.
	if _, ok := m.Headers.Get("traceparent"); !ok {
		return errors.New("the publish middleware wrote no traceparent; nothing would carry the trace across the broker")
	}
	return nil
}

// consume runs the handler once, with the process-span middleware installed,
// and returns when the runner has finished accounting for the delivery.
func consume(ctx context.Context, consumer *mq.ConsumerHandle, tel *telemetry) error {
	var (
		mu   sync.Mutex
		got  []string
		done = make(chan struct{})
		once sync.Once
	)
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	runErr := make(chan error, 1)
	go func() {
		runErr <- consumer.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			mu.Lock()
			got = append(got, string(d.Body))
			mu.Unlock()
			// Closed after this handler's own accounting. The SETTLE hook and
			// the process span still have to finish, and both happen after
			// this returns -- which is why the caller waits for Run rather
			// than for this channel.
			once.Do(func() { close(done) })
			return nil
		}), mq.WithMiddleware(tel.inst.Middleware()))
	}()
	select {
	case <-done:
	case err := <-runErr:
		return fmt.Errorf("the run ended before the message arrived: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := consumer.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutting the consumer down: %w", err)
	}
	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, mq.ErrClosed) {
		return fmt.Errorf("the run ended with %w", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(got, []string{body}) {
		return fmt.Errorf("the handler saw %v, want %v", got, []string{body})
	}
	return nil
}

// describe prints the span tree, the parenting, the two unparented spans and
// the instruments — and checks every one of them.
func describe(ctx context.Context, out io.Writer, tel *telemetry, topic string) error {
	spans := tel.spans.Ended()
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}
	// the specification's own names for the operations, with the destination appended the
	// way the convention asks. The publish and process spans are named
	// "<operation> <destination>"; the two unparented ones are named for
	// their operation alone, because a fetch and a settlement are not about
	// one message's destination.
	pubName := otelmq.OperationPublish + " " + topic
	procName := otelmq.OperationProcess + " " + topic
	caller, okCaller := byName[callerSpan]
	pubSpan, okPub := byName[pubName]
	procSpan, okProc := byName[procName]
	switch {
	case !okCaller:
		return fmt.Errorf("no %q span was recorded; the spans were %v", callerSpan, names(spans))
	case !okPub:
		return fmt.Errorf("no %q span was recorded; the spans were %v", pubName, names(spans))
	case !okProc:
		return fmt.Errorf("no %q span was recorded; the spans were %v", procName, names(spans))
	}

	// The tree. Three assertions, and each is a different claim.
	if pubSpan.Parent().SpanID() != caller.SpanContext().SpanID() {
		return fmt.Errorf("the publish span's parent is %v, want the caller's span %v: mq.PublishMiddleware is what makes it a child, and a publication outside a span would be a root",
			pubSpan.Parent().SpanID(), caller.SpanContext().SpanID())
	}
	if procSpan.Parent().SpanID() != pubSpan.SpanContext().SpanID() {
		return fmt.Errorf("the process span's parent is %v, want the PUBLISH span %v: the parent is read out of the delivery's traceparent header, so this failing means the header did not survive the broker",
			procSpan.Parent().SpanID(), pubSpan.SpanContext().SpanID())
	}
	if !procSpan.Parent().IsRemote() {
		return errors.New("the process span's parent is not marked remote; a context extracted from a message is another process's span, and marking it local would make the trace look like one process")
	}
	if procSpan.SpanContext().TraceID() != caller.SpanContext().TraceID() {
		return fmt.Errorf("the process span is in trace %v and the caller's span in %v: the whole point of propagating a trace context is that these are equal",
			procSpan.SpanContext().TraceID(), caller.SpanContext().TraceID())
	}
	_, _ = fmt.Fprintf(out, "trace: %s -> %s -> %s, one trace id across the broker\n", callerSpan, pubName, procName)

	// The two that cannot be in it.
	//
	// Their names are "<operation> <topic>" like the others, and the settle
	// span's operation is the SETTLEMENT's own -- ack, nack or extend --
	// under the pinned operation type "settle". So the ack of this one
	// message produces a span called "ack <topic>" and there is no span
	// called "settle <topic>" at all, which is worth knowing before
	// somebody searches a trace viewer for one.
	//
	// The receive span is counted rather than matched exactly: a fetch loop
	// makes as many fetches as it makes, and the number depends on timing.
	// What is asserted of every one of them is that it is a ROOT.
	roots := map[string]int{}
	for _, sp := range spans {
		op, dest, ok := strings.Cut(sp.Name(), " ")
		if !ok || dest != topic {
			continue
		}
		if op != otelmq.OperationReceive && !slices.Contains(settleOperations, op) {
			continue
		}
		roots[op]++
		if sp.Parent().IsValid() {
			return fmt.Errorf("the %q span has a parent (%v); if mq.Hooks has learned to carry a context.Context then the specification is closed and this example should stop saying it has not",
				sp.Name(), sp.Parent().SpanID())
		}
	}
	for _, op := range []string{otelmq.OperationReceive, settleAck} {
		if roots[op] == 0 {
			return fmt.Errorf("no %q span was recorded; the specification raises it from mq.Hooks and the spans were %v", op+" "+topic, names(spans))
		}
	}
	if byName[otelmq.OperationSettle+" "+topic] != nil {
		return fmt.Errorf("a span called %q exists; the settle span is named for the SETTLEMENT (%q) and this example says so", otelmq.OperationSettle+" "+topic, settleAck)
	}
	_, _ = fmt.Fprintf(out, "beside it: %q and %q are ROOT spans — mq.Hooks carries no context.Context, so the two hook spans cannot be parented, and the settle span is named for the settlement rather than %q \n",
		otelmq.OperationReceive, settleAck, otelmq.OperationSettle)

	// The attribute every dashboard groups by, read off the span rather than
	// off the configuration.
	system, ok := attr(pubSpan, "messaging.system")
	if !ok {
		return errors.New("the publish span carries no messaging.system; the convention requires it and every messaging dashboard groups by it")
	}
	dest, ok := attr(pubSpan, "messaging.destination.name")
	if !ok {
		return errors.New("the publish span carries no messaging.destination.name; a bound publisher needs PublishMiddlewareFor for exactly this reason")
	}
	if dest != topic {
		return fmt.Errorf("the publish span's destination is %q, want %q", dest, topic)
	}
	_, _ = fmt.Fprintf(out, "attributes: messaging.system=%s, messaging.destination.name=<topic>, semantic conventions pinned at %s\n",
		system, otelmq.SemconvVersion)

	return instruments(ctx, out, tel)
}

// instruments prints which of the instruments recorded a point.
func instruments(ctx context.Context, out io.Writer, tel *telemetry) error {
	var rm metricdata.ResourceMetrics
	if err := tel.reader.Collect(ctx, &rm); err != nil {
		return fmt.Errorf("collecting the metrics: %w", err)
	}
	recorded := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			recorded[m.Name] = true
		}
	}
	// The three histograms one publication and one handled delivery must
	// produce. The gauges are deliberately not required: an in-flight gauge
	// is observed at COLLECTION time and reads zero once the consumer has
	// drained, and the lag gauge needs an Admin.Stats round trip this
	// program does not make.
	want := []string{
		otelmq.InstrumentPublishDuration,
		otelmq.InstrumentHandlerDuration,
		otelmq.InstrumentEndToEndDuration,
	}
	missing := []string{}
	for _, name := range want {
		if !recorded[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		have := make([]string, 0, len(recorded))
		for n := range recorded {
			have = append(have, n)
		}
		sort.Strings(have)
		return fmt.Errorf("%v recorded no point; what was recorded was %v", missing, have)
	}
	_, _ = fmt.Fprintf(out, "instruments: %s recorded a point each, with no unit suffix — the exporter appends it \n",
		strings.Join(want, ", "))
	return nil
}

// attr reads one string attribute off a span.
func attr(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String(), true
		}
	}
	return "", false
}

// names is the recorded span names, sorted, for an error message that is the
// same on every run.
func names(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	sort.Strings(out)
	return out
}

// dropTopic deletes the run's topic, best-effort.
func dropTopic(ctx context.Context, cl *mq.Client, topic string) {
	adm, err := cl.Admin()
	if err != nil {
		return
	}
	_ = adm.DeleteSubscription(ctx, topic, subscription)
	_ = adm.DeleteTopic(ctx, topic)
}

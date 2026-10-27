package otel_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/baggage"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"

	otelmq "github.com/iqhive/iqmq/integrations/otel"
)

var update = flag.Bool("update", false, "rewrite testdata/spans.golden")

const goldenPath = "testdata/spans.golden"

// tracing returns an instrumentation whose spans land in a recorder.
func tracing(t *testing.T, cfg otelmq.Config) (*otelmq.Instrumentation, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	cfg.TracerProvider = tp
	if cfg.System == "" {
		cfg.System = "memory"
	}
	i, err := otelmq.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return i, sr
}

// TestSemanticConventionsArePinned is the golden file for the span surface:
// every span's name, its kind and every attribute key and value.
//
// The semantic conventions are an API in exactly the way a metric name is.
// `messaging.destination` became `messaging.destination.name` and
// `messaging.operation` became `messaging.operation.type` with a different
// value set; a dashboard built on the old name shows an empty graph after the
// upgrade that renamed it, and nothing at compile time notices. So the
// attributes come from one pinned convention version (ADR-024) and the whole
// rendered surface is compared byte for byte. An intended change is
// `go test ./... -update` plus the ADR entry that says why.
func TestSemanticConventionsArePinned(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{System: "kafka"})
	driveEverySpan(t, i)

	got := renderSpans(sr.Ended())
	if *update {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", goldenPath)
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("reading the golden span surface: %v\n\nthe current surface is:\n%s", err, got)
	}
	if got != string(want) {
		t.Errorf("the span surface changed.\n--- got ---\n%s\n--- want (%s) ---\n%s\n"+
			"Semantic conventions are an API. If the change is intended, move "+
			"otelmq.SemconvVersion deliberately, record it in ADR-024, then "+
			"`go test ./... -update`.", got, goldenPath, want)
	}
}

// renderSpans writes the recorded spans in a stable, diffable form.
func renderSpans(spans []sdktrace.ReadOnlySpan) string {
	type row struct {
		name, kind string
		attrs      []string
		parented   bool
	}
	rows := make([]row, 0, len(spans))
	seen := map[string]bool{}
	for _, s := range spans {
		r := row{name: s.Name(), kind: s.SpanKind().String(), parented: s.Parent().IsValid()}
		for _, a := range s.Attributes() {
			r.attrs = append(r.attrs, string(a.Key)+"="+a.Value.String())
		}
		slices.Sort(r.attrs)
		// The surface is the set of distinct spans, not how many of each a
		// run produced: two acks of two messages are one row here.
		fp := r.name + "|" + r.kind + "|" + strings.Join(r.attrs, "|")
		if seen[fp] {
			continue
		}
		seen[fp] = true
		rows = append(rows, r)
	}
	sort.Slice(rows, func(a, b int) bool { return rows[a].name < rows[b].name })
	var b strings.Builder
	b.WriteString("# OpenTelemetry span surface, semconv " + otelmq.SemconvVersion + ".\n")
	b.WriteString("# Written by TestSemanticConventionsArePinned; regenerate with `go test ./... -update`.\n")
	b.WriteString("# schema: " + otelmq.SchemaURL + "\n")
	for _, r := range rows {
		b.WriteString("\n" + r.name + "\n  kind:     " + r.kind + "\n")
		if r.parented {
			b.WriteString("  parent:   yes\n")
		} else {
			b.WriteString("  parent:   none (see Config.Spans)\n")
		}
		for _, a := range r.attrs {
			b.WriteString("  attr:     " + a + "\n")
		}
	}
	return b.String()
}

// driveEverySpan creates one of each of the four spans, through a real
// client wherever the span needs real delivery metadata: a Delivery a test
// builds itself carries none , so a process span built
// over one would have no destination and the golden would pin a shape no
// program produces.
func driveEverySpan(t *testing.T, i *otelmq.Instrumentation) {
	t.Helper()
	const (
		topic = "orders"
		sub   = "billing"
	)
	cl, c := realClient(t, i, topic, sub)
	ctx := context.Background()

	pub, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	wrapped, err := middleware.WrapPublisher(pub, i.PublishMiddlewareFor(topic))
	if err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	buf := make([]mq.Delivery, 1)
	if n, err := c.Receive(ctx, buf); err != nil || n != 1 {
		t.Fatalf("Receive = %d, %v", n, err)
	}
	h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	if err := h.Handle(ctx, &buf[0]); err != nil {
		t.Fatal(err)
	}
	// The ack span comes from the real settlement; the other two settlements
	// are driven through the hook, because one delivery can only be settled
	// once and the golden needs all three operation names.
	if err := buf[0].Extend(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := buf[0].Ack(); err != nil {
		t.Fatal(err)
	}
	i.Hooks().OnSettle(context.Background(), mq.SettleEvent{
		Topic: topic, Subscription: sub, Kind: mq.SettleNack, Duration: time.Millisecond,
	})
}

// realClient opens a memory-backed client with the instrumentation installed
// and a consumer over the topic.
func realClient(t *testing.T, i *otelmq.Instrumentation, topic, sub string) (*mq.Client, *mq.ConsumerHandle) {
	t.Helper()
	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	cl := mq.NewClient(b, mq.WithHooks(i.Hooks()))
	t.Cleanup(func() { _ = cl.Close() })
	// The consumer is opened before anything is published: the memory
	// engine's Queue profile holds a record for the subscriptions that exist
	// when it is published.
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return cl, c
}

// TestTheProcessSpanIsAChildOfThePublishSpan is the propagation, end to end
// through a real client: one trace spans the broker.
//
// It is the test that would fail if injection and extraction agreed with each
// other and with nothing else — two halves of one package can share a
// mistake, and what makes this one real is that the message goes through the
// memory engine and its header table in between.
func TestTheProcessSpanIsAChildOfThePublishSpan(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	const (
		topic = "orders"
		sub   = "billing"
	)
	cl, c := realClient(t, i, topic, sub)
	ctx := context.Background()

	pub, err := cl.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	wrapped, err := middleware.WrapPublisher(pub, i.PublishMiddlewareFor(topic))
	if err != nil {
		t.Fatal(err)
	}
	if err := wrapped.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	buf := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, buf)
	if err != nil || n != 1 {
		t.Fatalf("Receive = %d, %v", n, err)
	}
	if _, ok := buf[0].Headers.Get(middleware.HeaderTraceParent); !ok {
		t.Fatal("the delivery carries no traceparent: the publish middleware did not inject one")
	}
	var seen oteltrace.SpanContext
	h := i.Middleware()(mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		seen = oteltrace.SpanContextFromContext(ctx)
		return nil
	}))
	if err := h.Handle(ctx, &buf[0]); err != nil {
		t.Fatal(err)
	}
	if err := buf[0].Ack(); err != nil {
		t.Fatal(err)
	}

	publish := findSpan(t, sr, "publish orders")
	process := findSpan(t, sr, "process orders")
	if process.Parent().SpanID() != publish.SpanContext().SpanID() {
		t.Errorf("the process span's parent is %v, want the publish span %v",
			process.Parent().SpanID(), publish.SpanContext().SpanID())
	}
	if process.SpanContext().TraceID() != publish.SpanContext().TraceID() {
		t.Errorf("the publication and the delivery are in two traces: %v and %v",
			publish.SpanContext().TraceID(), process.SpanContext().TraceID())
	}
	if seen.TraceID() != publish.SpanContext().TraceID() {
		t.Errorf("the handler's context carries trace %v, want %v",
			seen.TraceID(), publish.SpanContext().TraceID())
	}
	if !process.Parent().IsRemote() {
		t.Error("the process span's parent is not marked remote: it is another process's span")
	}
}

// TestABoundPublisherStillNamesItsDestination is the finding behind
// PublishMiddlewareFor: a bound publisher's messages carry no Topic, so the
// plain middleware sees an empty one.
func TestABoundPublisherStillNamesItsDestination(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	ctx := context.Background()
	send := i.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	if err := send(ctx, "", []*mq.Message{{Body: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	bound := i.PublishMiddlewareFor("orders")(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	if err := bound(ctx, "", []*mq.Message{{Body: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	// And a call that names its own topic wins over the bound one, so the
	// same middleware is right on an unbound publisher.
	if err := bound(ctx, "invoices", []*mq.Message{{Body: []byte("x")}}, nil); err != nil {
		t.Fatal(err)
	}
	names := map[string]int{}
	for _, s := range sr.Ended() {
		names[s.Name()]++
	}
	if names["publish"] != 1 {
		t.Errorf("the plain middleware over a bound publisher produced %v, want one unnamed publish span", names)
	}
	if names["publish orders"] != 1 || names["publish invoices"] != 1 {
		t.Errorf("PublishMiddlewareFor produced %v", names)
	}
}

// findSpan returns the one ended span with this name.
func findSpan(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	ended := sr.Ended()
	names := make([]string, 0, len(ended))
	for _, s := range ended {
		names = append(names, s.Name())
		if s.Name() == name {
			if found != nil {
				t.Fatalf("two spans called %q", name)
			}
			found = s
		}
	}
	if found == nil {
		t.Fatalf("no span called %q; the recorder has %v", name, names)
	}
	return found
}

// TestAMalformedTraceParentDoesNotFailTheDelivery is on the receive side:
// broker-delivered metadata is untrusted, so a publisher must not be able to
// stop a consumer by writing nonsense into a header.
func TestAMalformedTraceParentDoesNotFailTheDelivery(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	for _, bad := range []string{
		"", "not a traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",
		strings.Repeat("0", 4096),
	} {
		d := &mq.Delivery{}
		d.Headers.Set(middleware.HeaderTraceParent, []byte(bad))
		d.Headers.Set(middleware.HeaderTraceState, []byte(strings.Repeat("a=1,", 4096)))
		h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
		if err := h.Handle(context.Background(), d); err != nil {
			t.Fatalf("traceparent %q failed the delivery: %v", bad, err)
		}
	}
	// Every one of them started a root span rather than a child of a trace
	// that does not exist.
	for _, s := range sr.Ended() {
		if s.Parent().IsValid() {
			t.Errorf("span %q was parented to a malformed trace context", s.Name())
		}
	}
}

// TestDisabledIsTheIdentity is the disabled observer path: an
// instrumentation with no provider adds nothing to the chain at all — not a
// branch per message, because there is no code per message.
func TestDisabledIsTheIdentity(t *testing.T) {
	i, err := otelmq.New(otelmq.Config{})
	if err != nil {
		t.Fatalf("New with no provider: %v", err)
	}
	if i.Enabled() {
		t.Error("an instrumentation with no provider reports itself enabled")
	}
	if h := i.Hooks(); h.OnPublishStart != nil || h.OnPublishDone != nil ||
		h.OnReceive != nil || h.OnSettle != nil || h.OnReconnect != nil {
		t.Error("a disabled instrumentation returned a non-empty mq.Hooks")
	}

	handler := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	if got := i.Middleware()(handler); !sameHandler(got, handler) {
		t.Error("a disabled Middleware wrapped the handler")
	}
	terminal := mq.PublishFunc(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	if got := i.PublishMiddleware()(terminal); !samePublish(got, terminal) {
		t.Error("a disabled PublishMiddleware wrapped the publish function")
	}
	// A disabled instrumentation also registers no source: there is nothing
	// for it to report to.
	if i.AddConsumer("orders", "billing", func() mq.ConsumerStats { return mq.ConsumerStats{} }) {
		t.Error("a disabled instrumentation accepted a consumer source")
	}
}

// sameHandler reports whether a middleware returned the handler it was given.
// Function values are not comparable, so the identity is checked by calling
// them and comparing a side effect that only the wrapper could not preserve:
// the number of frames is not observable, so this compares the pointers
// through a marker instead.
func sameHandler(got, want mq.Handler) bool {
	gf, ok1 := got.(mq.HandlerFunc)
	wf, ok2 := want.(mq.HandlerFunc)
	if !ok1 || !ok2 {
		return false
	}
	return funcPointer(gf) == funcPointer(wf)
}

// samePublish reports whether a publish middleware returned what it was
// given.
func samePublish(got, want mq.PublishFunc) bool {
	return funcPointer(got) == funcPointer(want)
}

// funcPointer is the code pointer of a function value, which is how a test
// asks "is this the same function?" -- function values are not comparable.
func funcPointer(f any) uintptr { return reflect.ValueOf(f).Pointer() }

// TestNewRefusesNoSystem pins the one required field.
func TestNewRefusesNoSystem(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	if _, err := otelmq.New(otelmq.Config{TracerProvider: tp}); !errors.Is(err, otelmq.ErrNoSystem) {
		t.Errorf("New with no System = %v, want ErrNoSystem", err)
	}
	if _, err := otelmq.New(otelmq.Config{}); err != nil {
		t.Errorf("New with no provider and no System = %v, want nil: "+
			"messaging.system is only needed by something that reports it", err)
	}
}

// TestSpansCanBeTurnedOff covers Config.Spans, which is how a program that
// would rather have no span than an unparented one says so.
func TestSpansCanBeTurnedOff(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{Spans: otelmq.Spans{NoReceive: true, NoSettle: true}})
	h := i.Hooks()
	if h.OnReceive != nil || h.OnSettle != nil {
		t.Fatal("the suppressed hooks are still installed, so the core would build their events")
	}

	// The publish span can go too, and the trace context is still injected:
	// a consumer's process span needs a parent whether or not the producer
	// recorded one.
	j, _ := tracing(t, otelmq.Config{Spans: otelmq.Spans{NoPublish: true}})
	m := &mq.Message{Topic: "orders", Body: []byte("x")}
	send := j.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	ctx, sp := sdktrace.NewTracerProvider().Tracer("test").Start(context.Background(), "caller")
	if err := send(ctx, "orders", []*mq.Message{m}, nil); err != nil {
		t.Fatal(err)
	}
	sp.End()
	if _, ok := m.Headers.Get(middleware.HeaderTraceParent); !ok {
		t.Error("NoPublish also stopped the injection")
	}
	if len(sr.Ended()) != 0 {
		t.Errorf("the first recorder saw %d spans, want none", len(sr.Ended()))
	}
}

// TestCardinalityFoldsPastTheLimit is the cardinality control: past the
// limit a new destination lands on one overflow attribute set.
func TestCardinalityFoldsPastTheLimit(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{Cardinality: otelmq.Cardinality{MaxSeries: 2}})
	for _, topic := range []string{"a", "b", "c", "d"} {
		send := i.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
		if err := send(context.Background(), topic, []*mq.Message{{Topic: topic}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	names := map[string]int{}
	for _, s := range sr.Ended() {
		names[s.Name()]++
	}
	if names["publish a"] != 1 || names["publish b"] != 1 {
		t.Errorf("the first two topics did not get their own spans: %v", names)
	}
	if names["publish other"] != 2 {
		t.Errorf("topics past the limit did not fold onto one overflow set: %v", names)
	}
	for _, topic := range []string{"c", "d"} {
		if names["publish "+topic] != 0 {
			t.Errorf("topic %q got a span of its own past the limit", topic)
		}
	}
}

// TestOmitTopicKeepsTheNameOffTheSpan is the other cardinality control, for a
// deployment whose topic names are per-tenant.
func TestOmitTopicKeepsTheNameOffTheSpan(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{Cardinality: otelmq.Cardinality{OmitTopic: true}})
	send := i.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	if err := send(context.Background(), "tenant-42", []*mq.Message{{Topic: "tenant-42"}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, s := range sr.Ended() {
		if strings.Contains(s.Name(), "tenant-42") {
			t.Errorf("span name %q carries the topic with OmitTopic set", s.Name())
		}
		for _, a := range s.Attributes() {
			if strings.Contains(a.Value.String(), "tenant-42") {
				t.Errorf("attribute %s carries the topic with OmitTopic set", a.Key)
			}
		}
	}
}

// TestNoMessageIdentifierIsEverAnAttribute is the rule: a message
// identifier as an attribute is one attribute value per message, which is how
// a tracing backend's index falls over.
func TestNoMessageIdentifierIsEverAnAttribute(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	const id = "01J0000000000000000000MSGID"
	send := i.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	if err := send(context.Background(), "orders", []*mq.Message{
		{Topic: "orders", ID: id, Key: []byte("customer-7"), Body: []byte("secret body")},
	}, nil); err != nil {
		t.Fatal(err)
	}
	d := &mq.Delivery{}
	d.ID = id
	d.Key = []byte("customer-7")
	d.Body = []byte("secret body")
	h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	for _, s := range sr.Ended() {
		for _, a := range s.Attributes() {
			v := a.Value.String()
			for _, forbidden := range []string{id, "customer-7", "secret body"} {
				if strings.Contains(v, forbidden) {
					t.Errorf("span %q attribute %s = %q carries %q", s.Name(), a.Key, v, forbidden)
				}
			}
		}
	}
}

// TestAFailedPublicationIsRecordedOnTheSpan pins the error path: the span
// carries the failure as an event and a status, so a trace shows what a
// counter only totals.
func TestAFailedPublicationIsRecordedOnTheSpan(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	want := errors.New("the broker refused it")
	send := i.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return want })
	if err := send(context.Background(), "orders", []*mq.Message{{Topic: "orders"}}, nil); !errors.Is(err, want) {
		t.Fatalf("the middleware changed the error: %v", err)
	}
	s := findSpan(t, sr, "publish orders")
	if s.Status().Code.String() != "Error" {
		t.Errorf("span status = %v, want Error", s.Status().Code)
	}
	if len(s.Events()) == 0 {
		t.Error("the failure was not recorded as a span event")
	}
}

// TestABatchCarriesItsMessageCount covers the one attribute that cannot be
// cached, and the fact that a single publication does not carry it.
func TestABatchCarriesItsMessageCount(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	send := i.PublishMiddleware()(func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil })
	ms := []*mq.Message{{Topic: "orders"}, {Topic: "orders"}, {Topic: "orders"}}
	if err := send(context.Background(), "orders", ms, nil); err != nil {
		t.Fatal(err)
	}
	s := findSpan(t, sr, "publish orders")
	var count int64 = -1
	for _, a := range s.Attributes() {
		if a.Key == "messaging.batch.message_count" {
			count = a.Value.AsInt64()
		}
	}
	if count != 3 {
		t.Errorf("messaging.batch.message_count = %d, want 3", count)
	}
	// Every message of the batch carries the same trace context: they were
	// published in one call.
	first, _ := ms[0].Headers.Get(middleware.HeaderTraceParent)
	for n, m := range ms {
		got, ok := m.Headers.Get(middleware.HeaderTraceParent)
		if !ok {
			t.Fatalf("message %d of the batch carries no traceparent", n)
		}
		if !bytes.Equal(got, first) {
			t.Errorf("message %d carries %q, want the batch's %q", n, got, first)
		}
	}
}

// TestABrokerSuppliedTopicIsMadeValidUTF8 is the OpenTelemetry half of the
// defect FuzzMetricLabels found on the Prometheus side.
//
// The specification makes a delivery's topic the concrete one the broker reported and
// makes it untrusted, so a destination name can be arbitrary bytes. An OTLP
// exporter encodes a span name and an attribute value into protobuf `string`
// fields, which proto3 requires to be valid UTF-8: one odd topic would fail
// the export of the whole batch, losing every span in it and not just the one
// with the odd name.
func TestABrokerSuppliedTopicIsMadeValidUTF8(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	hooks := i.Hooks()
	hooks.OnReceive(context.Background(), mq.ReceiveEvent{
		Topic: "orders\xff\xfe", Subscription: "billing\x80",
		Messages: 1, Duration: time.Millisecond,
	})
	send := i.PublishMiddlewareFor("orders\xed\xa0\x80")(
		func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil },
	)
	if err := send(context.Background(), "", []*mq.Message{{}}, nil); err != nil {
		t.Fatal(err)
	}
	spans := sr.Ended()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(spans))
	}
	for _, s := range spans {
		if !utf8.ValidString(s.Name()) {
			t.Errorf("span name %q is not valid UTF-8", s.Name())
		}
		for _, a := range s.Attributes() {
			if !utf8.ValidString(a.Value.String()) {
				t.Errorf("attribute %s = %q is not valid UTF-8", a.Key, a.Value.String())
			}
		}
	}
	// And the readable part of the name survives, so the span is still
	// attributable to a destination somebody can recognise.
	if got := spans[0].Name(); got != "receive orders�" {
		t.Errorf("span name = %q, want %q", got, "receive orders�")
	}
}

// TestTheCarrierIsAPropagationTextMapCarrier covers HeaderCarrier directly:
// Get, Set and Keys over a message's header table, including the nil-pointer
// case that makes an extraction from a message with no headers a miss rather
// than a panic.
func TestTheCarrierIsAPropagationTextMapCarrier(t *testing.T) {
	var h mq.Headers
	c := otelmq.HeaderCarrier{Headers: &h}
	if got := c.Get("absent"); got != "" {
		t.Errorf("Get of an absent key = %q, want the empty string", got)
	}
	c.Set("traceparent", "value")
	c.Set("traceparent", "replaced")
	if got := c.Get("traceparent"); got != "replaced" {
		t.Errorf("Get = %q, want the replaced value", got)
	}
	c.Set("tracestate", "acme=1")
	keys := c.Keys()
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"traceparent", "tracestate"}) {
		t.Errorf("Keys = %v, want the two headers that were set", keys)
	}

	// A carrier over no headers reads as empty and discards what is written
	// to it, which is what a Delivery a caller built itself looks like.
	var nilc otelmq.HeaderCarrier
	if got := nilc.Get("traceparent"); got != "" {
		t.Errorf("Get on a nil carrier = %q", got)
	}
	nilc.Set("traceparent", "value")
	if nilc.Keys() != nil {
		t.Errorf("Keys on a nil carrier = %v, want nil", nilc.Keys())
	}
}

// TestTheConfiguredPropagatorCarriesBaggage covers Config.Propagator and
// W3CPropagator, which is how the specification's `baggage` is carried — the one part of
// its propagation sentence this package does not implement itself.
//
// It is a real round trip through a message: the publish middleware injects
// with the configured propagator and the handler middleware extracts with it,
// so a baggage member set by the producer reaches the consumer's context.
func TestTheConfiguredPropagatorCarriesBaggage(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{Propagator: otelmq.W3CPropagator()})

	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(context.Background(), bag)

	m := &mq.Message{Topic: "orders", Body: []byte("x")}
	send := i.PublishMiddlewareFor("orders")(
		func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil },
	)
	if err := send(ctx, "orders", []*mq.Message{m}, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Headers.Get(otelmq.HeaderBaggage); !ok {
		t.Fatalf("the message carries no %q header; it has %d headers",
			otelmq.HeaderBaggage, m.Headers.Len())
	}
	if _, ok := m.Headers.Get(middleware.HeaderTraceParent); !ok {
		t.Error("the composite propagator did not write a traceparent")
	}

	d := &mq.Delivery{}
	d.Headers = m.Headers.Clone()
	var seen baggage.Baggage
	var parent oteltrace.SpanContext
	h := i.Middleware()(mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		seen = baggage.FromContext(ctx)
		parent = oteltrace.SpanContextFromContext(ctx)
		return nil
	}))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if got := seen.Member("tenant").Value(); got != "acme" {
		t.Errorf("baggage member tenant = %q, want acme", got)
	}
	if !parent.IsValid() {
		t.Error("the configured propagator did not carry the trace context")
	}
	publish := findSpan(t, sr, "publish orders")
	if parent.TraceID() != publish.SpanContext().TraceID() {
		t.Errorf("the handler is in trace %v, want the publication's %v",
			parent.TraceID(), publish.SpanContext().TraceID())
	}
}

// TestInjectReplacesTheVendorStateAndBaggageItFinds is the reported defect: a
// message whose headers came from another one -- here a forwarded delivery's
// -- already carries that one's tracestate and baggage. Inject wrote the new
// traceparent and left the old tracestate beside it, and the configured
// propagator left the old baggage on a publication whose context had none, so
// the next hop read another trace's vendor state and another request's
// baggage as this one's.
func TestInjectReplacesTheVendorStateAndBaggageItFinds(t *testing.T) {
	forwarded := func() *mq.Message {
		m := &mq.Message{Topic: "orders", Body: []byte("x")}
		m.Headers.Set(middleware.HeaderTraceParent, []byte("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"))
		m.Headers.Set(middleware.HeaderTraceState, []byte("upstream=stale"))
		m.Headers.Set(otelmq.HeaderBaggage, []byte("tenant=upstream"))
		return m
	}
	send := func(i *otelmq.Instrumentation, m *mq.Message) {
		t.Helper()
		pub := i.PublishMiddlewareFor("orders")(
			func(context.Context, string, []*mq.Message, []mq.PublishOption) error { return nil },
		)
		if err := pub(context.Background(), "orders", []*mq.Message{m}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name string
		cfg  otelmq.Config
	}{
		{"default", otelmq.Config{}},
		{"W3CPropagator", otelmq.Config{Propagator: otelmq.W3CPropagator()}},
	} {
		t.Run(c.name, func(t *testing.T) {
			i, sr := tracing(t, c.cfg)
			m := forwarded()
			send(i, m)
			tp, _ := m.Headers.Get(middleware.HeaderTraceParent)
			parsed, err := middleware.ParseTraceParent(string(tp))
			if err != nil {
				t.Fatalf("the published traceparent %q does not parse: %v", tp, err)
			}
			if publish := findSpan(t, sr, "publish orders"); oteltrace.TraceID(parsed.TraceID) != publish.SpanContext().TraceID() {
				t.Fatalf("the traceparent names trace %x, want the publication's %v", parsed.TraceID, publish.SpanContext().TraceID())
			}
			if v, ok := m.Headers.Get(middleware.HeaderTraceState); ok {
				t.Errorf("the message kept the forwarded tracestate %q beside a traceparent it does not belong to", v)
			}
			if c.cfg.Propagator == nil {
				return
			}
			if v, ok := m.Headers.Get(otelmq.HeaderBaggage); ok {
				t.Errorf("the message kept the forwarded baggage %q, and the publication's context has none", v)
			}
		})
	}
}

// TestALongTraceStateIsCutBetweenMembers is the reported defect: a tracestate
// over the bound was cut at the byte, mid-member. A member cut inside its
// value survived as that vendor's state with a wrong value, and one cut before
// its '=' failed the grammar and took every other vendor's member with it.
func TestALongTraceStateIsCutBetweenMembers(t *testing.T) {
	const parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	// Members of 100 bytes, so the sixth crosses the 512-byte bound.
	members := make([]string, 6)
	for n := range members {
		members[n] = "v" + string(rune('a'+n)) + "=" + strings.Repeat(string(rune('0'+n)), 97)
	}
	for _, c := range []struct {
		name  string
		state string
	}{
		{"cut inside a value", strings.Join(members, ",")},
		// Shifted by a five-byte member so the bound falls one byte into
		// the sixth member's key.
		{"cut before the equals", "x=yyy," + strings.Join(members, ",")},
	} {
		t.Run(c.name, func(t *testing.T) {
			var h mq.Headers
			h.Set(middleware.HeaderTraceParent, []byte(parent))
			h.Set(middleware.HeaderTraceState, []byte(c.state))
			ts := otelmq.Extract(&h).TraceState()
			if ts.Len() == 0 {
				t.Fatalf("the whole vendor list was dropped for one member that did not fit")
			}
			if len(ts.String()) > middleware.MaxTraceStateLen {
				t.Fatalf("tracestate is %d bytes, over the bound", len(ts.String()))
			}
			for _, m := range members {
				k, v, _ := strings.Cut(m, "=")
				if got := ts.Get(k); got != "" && got != v {
					t.Errorf("member %s carries %q, a prefix of its value %q", k, got, v)
				}
			}
			if ts.Get("vf") != "" {
				t.Error("the member that crossed the bound was kept")
			}
		})
	}
}

// TestOmitSubscriptionKeepsTheGroupOffTheSpan is the other half of the
// cardinality control, for a deployment whose subscription names are
// per-tenant.
func TestOmitSubscriptionKeepsTheGroupOffTheSpan(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{
		Cardinality: otelmq.Cardinality{OmitSubscription: true},
	})
	i.Hooks().OnReceive(context.Background(), mq.ReceiveEvent{
		Topic: "orders", Subscription: "tenant-42", Messages: 1, Duration: time.Millisecond,
	})
	s := findSpan(t, sr, "receive orders")
	for _, a := range s.Attributes() {
		if strings.Contains(a.Value.String(), "tenant-42") {
			t.Errorf("attribute %s carries the subscription with OmitSubscription set", a.Key)
		}
		if a.Key == "messaging.consumer.group.name" && a.Value.String() != "other" {
			t.Errorf("messaging.consumer.group.name = %q, want the overflow value",
				a.Value.String())
		}
	}
}

// TestAPanickingHandlerStillEndsItsSpan pins the process span across a
// handler panic. The runner recovers a panic outside the whole middleware
// chain, so a middleware that ended its span only on a normal return left
// the span open and never exported. The panic must still reach the caller
// with its own value: this middleware observes it and does not swallow it.
func TestAPanickingHandlerStillEndsItsSpan(t *testing.T) {
	i, sr := tracing(t, otelmq.Config{})
	h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { panic("boom") }))
	if v := handlePanicking(h); v != "boom" {
		t.Fatalf("the panic reached the caller as %v, want the handler's own value", v)
	}
	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("%d spans ended, want the process span", len(ended))
	}
	s := ended[0]
	if s.Status().Code.String() != "Error" {
		t.Errorf("span status = %v, want Error", s.Status().Code)
	}
	if !strings.Contains(s.Status().Description, "boom") {
		t.Errorf("span status description = %q, want the panic value in it", s.Status().Description)
	}
}

// handlePanicking calls h the way the runner does, recovering a panic, and
// returns what it panicked with.
func handlePanicking(h mq.Handler) (v any) {
	defer func() { v = recover() }()
	_ = h.Handle(context.Background(), &mq.Delivery{})
	return nil
}

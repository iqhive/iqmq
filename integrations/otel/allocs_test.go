package otel_test

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/middleware"

	otelmq "bitbucket.org/iqhive/iqmq/integrations/otel"
)

// TestAllocs says what this integration costs per event, and it says it in
// three columns: nothing installed, installed but not recording, and a real
// SDK recording every span.
//
// An observability integration must not change what it observes, and a
// tracing integration cannot claim zero: a span is an object and the SDK
// allocates one. What it CAN claim, and what this pins, is that
//
//   - with nothing installed the cost is zero and there is no code on the
//     path at all — the specification's "disabled hooks/observer path: 0 allocs, ≤ 1
//     predictable branch", reached by the middlewares being the identity and
//     Hooks() being the zero value;
//   - with a non-recording tracer — the state a sampled-out span is in — the
//     cost is three to five allocations, every one of them the OpenTelemetry
//     API's own: a span's context value, the bytes of an injected header, and
//     the boxed timestamp options an unparented span needs. Not one of them is
//     an attribute slice or a formatted span name, because those are cached
//     per (operation, topic, subscription) and the cache is what this column
//     exists to pin;
//   - with a recording SDK the cost is the SDK's, bounded and stated below,
//     and it does not grow with the number of topics or messages.
//
// The first three columns are equalities, so a change in what an event costs
// has to be looked at. The recording column is a ceiling: how many objects
// the OTel SDK allocates per span is upstream's business and changes between
// releases. What must not change is the order of magnitude, and a regression
// in what THIS package adds — an attribute slice per span, say — moves the
// number well past it.
func TestAllocs(t *testing.T) {
	if allocsRaceDetector {
		t.Skip("the race detector perturbs allocation counts; `make allocs` runs without it")
	}
	ctx := context.Background()
	noopHandler := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	noopSend := mq.PublishFunc(func(context.Context, string, []*mq.Message) error { return nil })

	t.Run("disabled", func(t *testing.T) {
		i, err := otelmq.New(otelmq.Config{})
		if err != nil {
			t.Fatal(err)
		}
		h := i.Middleware()(noopHandler)
		send := i.PublishMiddleware()(noopSend)
		d := &mq.Delivery{}
		m := &mq.Message{Topic: "orders"}
		one := []*mq.Message{m}

		// Not "close to zero": exactly zero, because composition returned
		// what it was given and there is no frame to enter.
		if got := testing.AllocsPerRun(200, func() { _ = h.Handle(ctx, d) }); got != 0 {
			t.Errorf("disabled Middleware: %.1f allocs/op, want 0", got)
		}
		if got := testing.AllocsPerRun(200, func() { _ = send(ctx, "orders", one) }); got != 0 {
			t.Errorf("disabled PublishMiddleware: %.1f allocs/op, want 0", got)
		}
		if hooks := i.Hooks(); hooks.OnReceive != nil || hooks.OnSettle != nil ||
			hooks.OnPublishDone != nil {
			t.Error("a disabled instrumentation installed a hook, so the core would build its event")
		}
	})

	t.Run("non-recording tracer", func(t *testing.T) {
		i, err := otelmq.New(otelmq.Config{
			TracerProvider: noop.NewTracerProvider(),
			System:         "memory",
		})
		if err != nil {
			t.Fatal(err)
		}
		send := i.PublishMiddleware()(noopSend)
		h := i.Middleware()(noopHandler)
		hooks := i.Hooks()

		// A delivery whose headers carry a trace context, because extraction
		// is on this path in a real program.
		d := &mq.Delivery{}
		d.Headers.Set(middleware.HeaderTraceParent,
			[]byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))

		cases := []struct {
			name string
			run  func()
			want float64
			why  string
		}{
			{
				name: "publish", want: 3,
				why: "the span's context value, and the two allocations the " +
					"injected traceparent header costs (the rendered string and " +
					"its bytes)",
				run: func() {
					m := &mq.Message{Topic: "orders"}
					_ = send(ctx, "orders", []*mq.Message{m})
				},
			},
			{
				name: "process", want: 3,
				why: "a context value for the extracted remote parent, one for " +
					"the span, and the traceparent header value read as a string",
				run: func() { _ = h.Handle(ctx, d) },
			},
			{
				name: "receive", want: 5,
				why: "two boxed trace.WithTimestamp options and the two variadic " +
					"option slices they travel in, plus the span's context " +
					"value. The OpenTelemetry API takes options as interface " +
					"values, so a per-event timestamp cannot be cached the way " +
					"the attribute set and the span name are -- and an " +
					"unparented span needs its timestamps explicitly, because " +
					"the hook that reports it is called after the operation it " +
					"describes. Spans.NoReceive is the knob for a program that " +
					"will not pay it",
				run: func() {
					hooks.OnReceive(context.Background(), mq.ReceiveEvent{
						Topic: "orders", Subscription: "billing",
						Messages: 1, Duration: time.Millisecond,
					})
				},
			},
			{
				name: "settle", want: 5,
				why: "the same five as the receive span, and this is the one " +
					"there is most of: one per message rather than one per " +
					"fetch. Spans.NoSettle turns it off",
				run: func() {
					hooks.OnSettle(context.Background(), mq.SettleEvent{
						Topic: "orders", Subscription: "billing",
						Kind: mq.SettleAck, Duration: time.Millisecond,
					})
				},
			},
		}
		for _, c := range cases {
			c.run()
			got := testing.AllocsPerRun(200, c.run)
			if got != c.want {
				t.Errorf("non-recording %s: %.1f allocs/op, want %.0f (%s)",
					c.name, got, c.want, c.why)
			}
		}
	})

	t.Run("metrics only", func(t *testing.T) {
		r := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
		t.Cleanup(func() { _ = mp.Shutdown(ctx) })
		i, err := otelmq.New(otelmq.Config{MeterProvider: mp, System: "memory"})
		if err != nil {
			t.Fatal(err)
		}
		hooks := i.Hooks()
		ev := mq.PublishDoneEvent{Topic: "orders", Messages: 1, Duration: time.Millisecond}
		hooks.OnPublishDone(context.Background(), ev)
		got := testing.AllocsPerRun(200, func() { hooks.OnPublishDone(context.Background(), ev) })
		t.Logf("publish histogram: %.1f allocs/op", got)
		// The record option carrying the attribute set is cached, so a
		// measurement is an aggregation update and nothing else.
		if got != 0 {
			t.Errorf("publish histogram: %.1f allocs/op, want 0 "+
				"(the attribute set and its metric.RecordOption are cached per destination)", got)
		}
	})

	t.Run("recording SDK", func(t *testing.T) {
		tp := sdktrace.NewTracerProvider()
		t.Cleanup(func() { _ = tp.Shutdown(ctx) })
		i, err := otelmq.New(otelmq.Config{TracerProvider: tp, System: "memory"})
		if err != nil {
			t.Fatal(err)
		}
		send := i.PublishMiddleware()(noopSend)
		run := func() {
			m := &mq.Message{Topic: "orders"}
			_ = send(ctx, "orders", []*mq.Message{m})
		}
		run()
		got := testing.AllocsPerRun(200, run)
		t.Logf("recording publish span: %.1f allocs/op", got)
		// The ceiling is an order of magnitude, not a budget: what it catches
		// is this package starting to build an attribute slice or a span
		// name per message, which would put it well past this.
		if got > 20 {
			t.Errorf("recording publish span: %.1f allocs/op, want at most 20 "+
				"(a recording span is the SDK's cost; this package adds none of its own)", got)
		}
	})
}

// TestAllocsCacheDoesNotGrowWithMessages pins the property the cardinality
// bound exists for: a thousand publications to one topic admit one attribute
// set, and a thousand publications to a thousand topics admit at most the
// limit.
func TestAllocsCacheDoesNotGrowWithMessages(t *testing.T) {
	if allocsRaceDetector {
		t.Skip("the race detector perturbs allocation counts")
	}
	i, err := otelmq.New(otelmq.Config{
		TracerProvider: noop.NewTracerProvider(),
		System:         "memory",
		Cardinality:    otelmq.Cardinality{MaxSeries: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	send := i.PublishMiddleware()(mq.PublishFunc(
		func(context.Context, string, []*mq.Message) error { return nil },
	))
	ctx := context.Background()

	// Warm every admitted key and the folded one.
	for _, topic := range []string{"a", "b", "c", "d", "e", "f"} {
		_ = send(ctx, topic, []*mq.Message{{Topic: topic}})
	}
	n := 0
	run := func() {
		n++
		topic := string(rune('a' + n%26))
		_ = send(ctx, topic, []*mq.Message{{Topic: topic}})
	}
	got := testing.AllocsPerRun(500, run)
	t.Logf("publishing to a new topic every call: %.1f allocs/op", got)
	// Past the limit every topic folds onto the one overflow set, so the
	// per-call cost stops growing: no map is copied and no attribute slice
	// is built. Without the bound this would climb with the number of
	// distinct topics, because each new key copies the map.
	if got > 5 {
		t.Errorf("%.1f allocs/op publishing to a new topic every call, want at most 5: "+
			"the cardinality bound is what stops the cache growing", got)
	}
}

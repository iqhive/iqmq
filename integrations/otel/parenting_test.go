package otel_test

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	mq "github.com/iqhive/iqmq"
	otelmq "github.com/iqhive/iqmq/integrations/otel"
	"github.com/iqhive/iqmq/middleware"
)

// TestHookSpansAreChildrenOfTheOperation is why the hooks take a context.
//
// Before hooks v2 the receive and settle events were values with no context,
// so this integration had nothing to start a span from and called
// tracer.Start(context.Background(), …): the spans were right in time and
// nowhere in the trace -- ROOT spans, unattached to the caller's work
// Two of the four spans were affected, which is half
// the observability story the section promises.
//
// The test states the property rather than the shape: whatever span the
// caller is inside when the operation happens must be the parent of the span
// the hook records.
func TestHookSpansAreChildrenOfTheOperation(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	i, err := otelmq.New(otelmq.Config{System: "memory", TracerProvider: tp})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := i.Hooks()

	// The caller's own span, as a handler or a request would have.
	parentCtx, parent := tp.Tracer("test").Start(context.Background(), "caller")
	want := parent.SpanContext().SpanID()

	for name, fire := range map[string]func(){
		"OnReceive": func() {
			h.OnReceive(parentCtx, mq.ReceiveEvent{
				Topic: "orders", Subscription: "workers", Messages: 1, Duration: time.Millisecond,
			})
		},
		"OnSettle": func() {
			h.OnSettle(parentCtx, mq.SettleEvent{
				Topic: "orders", Subscription: "workers", Kind: mq.SettleAck, Duration: time.Millisecond,
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if fire == nil {
				t.Skipf("%s is not set by this configuration", name)
			}
			before := len(sr.Ended())
			fire()
			ended := sr.Ended()
			if len(ended) == before {
				t.Fatalf("%s recorded no span", name)
			}
			got := ended[len(ended)-1]
			if !got.Parent().IsValid() {
				t.Fatalf("%s recorded %q as a ROOT span: it is right in time and nowhere in the trace, which is "+
					"what the context on the event exists to fix", name, got.Name())
			}
			if got.Parent().SpanID() != want {
				t.Errorf("%s recorded %q under parent %v, want the caller's span %v",
					name, got.Name(), got.Parent().SpanID(), want)
			}
		})
	}
	parent.End()
}

// TestAProcessSpanWithNoTraceContextTakesTheHandlersSpan pins the rule
// Middleware documents for a delivery whose headers carry no trace context, or
// a malformed one: the process span is a child of the span in the handler's
// context, as the messaging conventions make it a child of an HTTP server span
// on a push endpoint, and a root when that context holds none. A traceparent,
// when there is one, wins over the handler's span.
func TestAProcessSpanWithNoTraceContextTakesTheHandlersSpan(t *testing.T) {
	outer := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = outer.Shutdown(context.Background()) })
	const remote = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	for name, cfg := range map[string]otelmq.Config{
		"default propagation":   {},
		"configured propagator": {Propagator: otelmq.W3CPropagator()},
	} {
		t.Run(name, func(t *testing.T) {
			i, sr := tracing(t, cfg)
			inner := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
			// An outer middleware that starts a span per delivery, as an HTTP
			// server does for a push endpoint.
			var server oteltrace.SpanContext
			push := mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
				ctx, sp := outer.Tracer("test").Start(ctx, "http.server POST /push")
				defer sp.End()
				server = sp.SpanContext()
				return inner.Handle(ctx, d)
			})
			for _, traceparent := range []string{"", "not a traceparent", remote} {
				for _, h := range []mq.Handler{push, inner} {
					d := &mq.Delivery{}
					if traceparent != "" {
						d.Headers.Set(middleware.HeaderTraceParent, []byte(traceparent))
					}
					server = oteltrace.SpanContext{}
					before := len(sr.Ended())
					if err := h.Handle(context.Background(), d); err != nil {
						t.Fatal(err)
					}
					ended := sr.Ended()
					if len(ended) != before+1 {
						t.Fatalf("traceparent %q: %d spans recorded, want the process span", traceparent, len(ended)-before)
					}
					got := ended[len(ended)-1].Parent()
					switch {
					case traceparent == remote:
						if got.SpanID().String() != "00f067aa0ba902b7" || !got.IsRemote() {
							t.Errorf("with a traceparent the parent is %v, want the publisher's remote span", got.SpanID())
						}
					case server.IsValid():
						if got.SpanID() != server.SpanID() {
							t.Errorf("traceparent %q: the parent is %v, want the handler's span %v", traceparent, got.SpanID(), server.SpanID())
						}
					case got.IsValid():
						t.Errorf("traceparent %q: the parent is %v with no span in the handler's context, want a root", traceparent, got.SpanID())
					}
				}
			}
		})
	}
}

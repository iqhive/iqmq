package otel_test

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	mq "github.com/iqhive/iqmq"
	otelmq "github.com/iqhive/iqmq/integrations/otel"
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

package middleware_test

import (
	"context"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/middleware"
)

// TestALongTraceStateIsCutBetweenMembers is the reported defect: a tracestate
// over MaxTraceStateLen was cut at the byte, so the member that crossed the
// bound was handed on with a prefix of its value -- a vendor's state, wrong,
// in a form nothing downstream can tell from right -- or as a key with no
// '=' at all. The W3C specification drops whole members from the end.
func TestALongTraceStateIsCutBetweenMembers(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	// Members of 100 bytes, so the sixth crosses the 512-byte bound.
	members := make([]string, 6)
	for n := range members {
		members[n] = "v" + string(rune('a'+n)) + "=" + strings.Repeat(string(rune('0'+n)), 97)
	}
	for _, c := range []struct {
		name  string
		state string
		want  string
	}{
		{"cut inside a value", strings.Join(members, ","), strings.Join(members[:5], ",")},
		// Shifted by a five-byte member so the bound falls one byte into
		// the sixth member's key.
		{"cut inside a key", "x=yyy," + strings.Join(members, ","), "x=yyy," + strings.Join(members[:5], ",")},
		{"whitespace before the cut", strings.Join(members[:4], ",") + "  ,vf=" + strings.Repeat("z", 200), strings.Join(members[:4], ",")},
		{"one member over the bound", "v=" + strings.Repeat("a", 600), ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got middleware.TraceContext
			h := middleware.CorrelationID(middleware.CorrelationConfig{Trace: true})(
				mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
					got, _ = middleware.TraceFromContext(ctx)
					return nil
				}),
			)
			d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte("x")}}
			d.Headers.Set(middleware.HeaderTraceParent, []byte(tp))
			d.Headers.Set(middleware.HeaderTraceState, []byte(c.state))
			if err := h.Handle(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			if got.State != c.want {
				t.Fatalf("tracestate = %q (%d bytes), want the whole members that fit, %q (%d bytes)",
					got.State, len(got.State), c.want, len(c.want))
			}
		})
	}
}

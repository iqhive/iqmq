package mwchain

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// benchLengths returns the chain lengths measured; IQMQ_BENCH_MATRIX=full
// adds the lengths at and beyond the generated maximum.
func benchLengths() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{0, 1, 3, 5, 8, 9}
	}
	return []int{0, 1, 3, 5}
}

// benchDelivery is what the benchmark pushes through the chain: a pointer, as
// a real delivery is, so the middlewares have something to touch without
// allocating.
type benchDelivery struct{ n int }

// BenchmarkMiddleware/<kind>/n=<n>/{disabled,enabled} is the budget a chain of five no-op middlewares must cost at most 30 ns and
// allocate nothing. "enabled" runs n middlewares; "disabled" builds the same
// n and turns them off, which is the cost a user pays for a feature nobody
// switched on.
func BenchmarkMiddleware(b *testing.B) {
	noop := Middleware[*benchDelivery]{Name: "noop", Fn: func(ctx context.Context, d *benchDelivery, next Handler[*benchDelivery]) error {
		return next(ctx, d)
	}}
	handler := func(_ context.Context, d *benchDelivery) error {
		d.n++
		return nil
	}
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			for _, n := range benchLengths() {
				for _, on := range []bool{false, true} {
					state := "disabled"
					if on {
						state = "enabled"
					}
					b.Run(fmt.Sprintf("n=%d/%s", n, state), func(b *testing.B) {
						mws := make([]Middleware[*benchDelivery], n)
						for i := range mws {
							mws[i] = noop
							mws[i].Disabled = !on
						}
						c := New(k, handler, mws...)
						d := &benchDelivery{}
						ctx := context.Background()
						b.ReportAllocs()
						for b.Loop() {
							if err := c.Handle(ctx, d); err != nil {
								b.Fatal(err)
							}
						}
						if d.n == 0 {
							b.Fatal("chain did not run")
						}
					})
				}
			}
		})
	}
}

// BenchmarkMiddlewareCompose measures what composing a chain costs, which is
// paid once per Run and is where the kinds are allowed to allocate.
func BenchmarkMiddlewareCompose(b *testing.B) {
	noop := Middleware[*benchDelivery]{Name: "noop", Fn: func(ctx context.Context, d *benchDelivery, next Handler[*benchDelivery]) error {
		return next(ctx, d)
	}}
	handler := func(context.Context, *benchDelivery) error { return nil }
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			for _, n := range benchLengths() {
				b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
					mws := make([]Middleware[*benchDelivery], n)
					for i := range mws {
						mws[i] = noop
					}
					b.ReportAllocs()
					for b.Loop() {
						if c := New(k, handler, mws...); c.Len() != n {
							b.Fatal("composition lost a middleware")
						}
					}
				})
			}
		})
	}
}

package completion

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// benchWindows returns the numbers of publishes in flight while the next one
// starts; IQMQ_BENCH_MATRIX=full widens them.
func benchWindows() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 8, 64, 512}
	}
	return []int{1, 64}
}

// BenchmarkPublish/Async/<kind>/inflight=<n> is the per-message cost of an
// asynchronous publish's completion machinery start the publish, complete it, and get the outcome back to the
// caller. `BenchmarkPublish/Async/<adapter>/deferred/inflight=<n>` is the
// second arm of the two adapters, whose delivery position Config.Defer can
// move; together the rows are the {callback,future,channel} x
// {inline,deferred} grid, six arms, spelled in the four names this package's
// Kind actually offers a caller.
//
// The row WITHOUT a delivery segment is each kind in its own delivery
// position -- pinned by the kind for `inline` and `deferred`
// (Kind.FixedDelivery), Config.Defer=false for the adapters -- and those four
// rows are the family's comparable set: they are the four values `Auto` can
// resolve to, compared on the metric the specification is decided on. The adapters'
// `deferred` rows are the crossing, retained because the cost of moving the
// delivery off the completing goroutine is what the row's second half
// ("inline only if documented and never under locks") is decided against.
//
// The adapters' inline arm used to be named `<kind>/inline/inflight=<n>`,
// which left the family with no benchmark name common to all four kinds after
// `bakeoff decide` strips the kind segment -- so the campaign would have
// measured the family for twenty minutes and then failed with "no benchmark
// is present for every kind". The specification records it.
func BenchmarkPublish(b *testing.B) {
	b.Run("Async", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				// Every kind's own delivery position, unqualified: the row
				// the four kinds are compared on.
				benchWindowed(b, k, k.Resolve() == Deferred)
				if k.FixedDelivery() {
					return
				}
				b.Run("deferred", func(b *testing.B) { benchWindowed(b, k, true) })
			})
		}
	})
}

func benchWindowed(b *testing.B, k Kind, deferred bool) {
	for _, window := range benchWindows() {
		b.Run(fmt.Sprintf("inflight=%d", window), func(b *testing.B) {
			benchAsync(b, k, deferred, window)
		})
	}
}

// benchAsync keeps window publishes outstanding and measures one start plus
// one completion, which is what one message costs the publisher.
func benchAsync(b *testing.B, k Kind, deferred bool, window int) {
	c := mustNew(b, k, Config{Capacity: 2 * window, Defer: deferred})
	defer func() { _ = c.Close() }()
	var delivered int64
	cb := func(Result) { delivered++ }
	if deferred || k == Deferred {
		// The callback runs on the completion goroutine, so the counter has
		// to stay off the benchmark's goroutine.
		cb = func(Result) {}
	}
	inflight := make([]Token, 0, window)
	for range window {
		tok, _, err := c.Start(cb)
		if err != nil {
			b.Fatalf("Start: %v", err)
		}
		inflight = append(inflight, tok)
	}
	next := 0
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		tok, _, err := c.Start(cb)
		if err != nil {
			b.Fatalf("Start: %v", err)
		}
		// Complete the oldest outstanding publish, as a broker confirming in
		// order would.
		if !c.Complete(inflight[next], nil) {
			b.Fatal("Complete refused")
		}
		inflight[next] = tok
		next = (next + 1) % window
	}
	b.StopTimer()
	for _, tok := range inflight {
		c.Complete(tok, nil)
	}
	if err := c.Drain(context.Background()); err != nil {
		b.Fatalf("Drain: %v", err)
	}
	_ = delivered
}

// BenchmarkPublishAwait measures what the adapters cost a caller that
// actually waits for the outcome, which is the only reason to pay for a
// handle at all.
func BenchmarkPublishAwait(b *testing.B) {
	ctx := context.Background()
	for _, k := range []Kind{Future, Channel} {
		b.Run(k.String(), func(b *testing.B) {
			c := mustNew(b, k, Config{Capacity: 8})
			defer func() { _ = c.Close() }()
			b.ReportAllocs()
			for b.Loop() {
				tok, h, err := c.Start(nil)
				if err != nil {
					b.Fatalf("Start: %v", err)
				}
				if !c.Complete(tok, nil) {
					b.Fatal("Complete refused")
				}
				if err := h.Await(ctx); err != nil {
					b.Fatalf("Await: %v", err)
				}
			}
		})
	}
}

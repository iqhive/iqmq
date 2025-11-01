package lifecycle

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// BenchmarkLifecycle/Gate/{ticket,allow}/P=<n> is what the shutdown perimeter
// costs the operations it guards.
//
// It is not a canonical name and it decides nothing: the engine's
// publish path takes one ticket per placement, so this is the price of the
// property that a record is never placed behind the drain
// (adr/ADR-017-assumptions.md), and it is here so that the
// price is a number rather than an assertion. ticket is the Enter/Leave pair
// a placement brackets itself with; allow is the phase load a route makes
// when it only needs to know whether it may proceed.
//
// What a smoke run on a busy host showed, as a shape and not a measurement
// (benchguard reported verdict=contended, so no decision rests on it): the
// pair is a little over 10 ns at P=1 and falls to about 5 ns as the cells
// stop colliding, while a single shared counter is about 5 ns at P=1 and
// rises past 20 ns at P=64. The striped form was kept for that reason and
// because internal/stats makes the same trade for the same reason; the cell
// count is Config.Shards, so an adapter that knows its own concurrency can
// change it, and re-measures the whole thing against
// BenchmarkMemory/PubSubAck, which now exists in./internal/engine (the specification moved the name there from the nested benchmarks module, where no gate could reach it; adr/ADR-017-assumptions.md).
func BenchmarkLifecycle(b *testing.B) {
	for _, op := range []string{"ticket", "allow"} {
		for _, p := range benchGateParallelism() {
			b.Run(fmt.Sprintf("Gate/%s/P=%d", op, p), func(b *testing.B) {
				benchGate(b, op, p)
			})
		}
	}
}

// benchGateParallelism is the P sweep: the operation on its own, and the same
// operation with every processor taking tickets, which is what a fan-out
// publish looks like.
func benchGateParallelism() []int {
	p := runtime.GOMAXPROCS(0)
	if p < 8 {
		return []int{1, p}
	}
	return []int{1, 8, p}
}

func benchGate(b *testing.B, op string, p int) {
	l := New(Config{})
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for g := range p {
		n := b.N / p
		if g == 0 {
			n += b.N - (b.N/p)*p
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if op == "allow" {
				for range n {
					if !l.Allow(OpPublish) {
						b.Error("refused while running")
						return
					}
				}
				return
			}
			for range n {
				tk := l.Enter(OpPublish)
				if !tk.OK() {
					b.Error("refused while running")
					return
				}
				l.Leave(tk)
			}
		}()
	}
	wg.Wait()
}

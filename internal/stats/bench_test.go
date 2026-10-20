package stats

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"unsafe"
)

// benchParallelism returns the goroutine counts; IQMQ_BENCH_MATRIX=full
// widens the sweep to the spec's 8/32/64 contention points.
func benchParallelism() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 8, 32, 64}
	}
	return []int{1, 8}
}

// footprint reports the counter's cell bytes for the given worker count.
func footprint(k Kind, workers int) int {
	switch k {
	case Atomic:
		return int(unsafe.Sizeof(atomicCounter{}))
	default:
		return k.Cells(workers) * int(unsafe.Sizeof(cell{}))
	}
}

// BenchmarkStats/<kind>/P=<n>/op=<add|addat|load> spreads b.N operations
// over P goroutines on one counter built for P workers: add is the
// anonymous increment, addat the hinted one (goroutine i passes worker i),
// load a read-only sweep of a pre-loaded counter.
func BenchmarkStats(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			for _, p := range benchParallelism() {
				for _, op := range []string{"add", "addat", "load"} {
					b.Run(fmt.Sprintf("P=%d/op=%s", p, op), func(b *testing.B) { benchOp(b, k, p, op) })
				}
			}
		})
	}
}

func benchOp(b *testing.B, k Kind, p int, op string) {
	c := mustNew(b, k, p)
	for w := range p {
		c.AddAt(w, int64(w))
	}
	per := b.N / p
	extra := b.N - per*p
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for g := range p {
		n := per
		if g == 0 {
			n += extra
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch op {
			case "add":
				for range n {
					c.Add(1)
				}
			case "addat":
				for range n {
					c.AddAt(g, 1)
				}
			case "load":
				var sink int64
				for range n {
					sink += c.Load()
				}
				_ = sink
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	if op != "load" {
		if got, want := c.Load(), int64(b.N)+int64(p*(p-1)/2); got != want {
			b.Fatalf("lost updates: Load = %d, want %d", got, want)
		}
	}
	b.ReportMetric(float64(footprint(k, p)), "bytes/counter")
}

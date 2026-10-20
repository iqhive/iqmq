package dispatch

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"
)

// benchMatrix returns the worker counts and the key skews;
// IQMQ_BENCH_MATRIX=full widens the pool sizes to the specification's
// contention points.
func benchMatrix() (workers []int, skews []string) {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 2, 4, 8, 16, 64}, []string{"uniform", "zipf"}
	}
	return []int{1, 8, 64}, []string{"uniform", "zipf"}
}

// benchKeys is the number of distinct ordering keys a benchmark draws from:
// a partition count a broker would plausibly hand a consumer.
const benchKeys = 256

// keyStream is a precomputed sequence of routes, so that the measured loop
// contains the dispatch and nothing else — no random number generation, no
// allocation, no clock.
func keyStream(skew string, ordered bool, n int) []Route {
	out := make([]Route, n)
	rng := rand.New(rand.NewPCG(0x1eaf, 0xd15))
	zipf := rand.NewZipf(rng, 1.3, 1, benchKeys-1)
	for i := range out {
		if !ordered {
			out[i] = Keyless()
			continue
		}
		var key uint64
		if skew == "zipf" {
			// A skewed workload is the interesting one: a handful of keys
			// carry most of the traffic, so lanes are unevenly loaded and a
			// pool that cannot move work stalls.
			key = zipf.Uint64() + 1
		} else {
			key = uint64(i%benchKeys) + 1
		}
		out[i] = Keyed(key).InPartition(int32(key % 8))
	}
	return out
}

// BenchmarkRun/Dispatch/<kind>/<w>workers/{ordered,unordered}/skew=<s> is the
// runner's dispatch hot path:
// one fetch loop routes an item to a lane and a worker runs it. "ordered"
// submits keyed items whose per-key order the pool must keep; "unordered"
// submits keyless ones, which any worker may take.
func BenchmarkRun(b *testing.B) {
	workers, skews := benchMatrix()
	b.Run("Dispatch", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, w := range workers {
					for _, ordered := range []bool{true, false} {
						mode := "unordered"
						if ordered {
							mode = "ordered"
						}
						for _, skew := range skews {
							if !ordered && skew == "zipf" {
								continue // keyless items have no key to skew
							}
							b.Run(fmt.Sprintf("%dworkers/%s/skew=%s", w, mode, skew), func(b *testing.B) {
								benchDispatch(b, k, w, ordered, skew, 0)
							})
						}
					}
				}
			})
		}
	})
	// One slow handler must not stop the pool: the straggler run keeps every
	// hundred-and-twenty-eighth item in its handler for 50 µs.
	b.Run("Straggler", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, w := range workers {
					if w == 1 {
						continue // a single worker cannot route around it
					}
					b.Run(fmt.Sprintf("%dworkers", w), func(b *testing.B) {
						benchDispatch(b, k, w, true, "zipf", 50*time.Microsecond)
					})
				}
			})
		}
	})
}

// benchDispatch submits b.N items through one pool and waits for the last of
// them to be handled. The reported time is therefore the whole path: route,
// enqueue, wake, dequeue, handler.
func benchDispatch(b *testing.B, k Kind, workers int, ordered bool, skew string, straggle time.Duration) {
	const streamLen = 4096
	stream := keyStream(skew, ordered, streamLen)
	var sunk int
	h := func(it item) {
		sunk += it.seq
		if straggle > 0 && it.seq%128 == 0 {
			time.Sleep(straggle)
		}
	}
	if workers > 1 {
		// With more than one worker the sink would be a data race; count
		// nothing instead and keep the handler equally cheap.
		h = func(it item) {
			if straggle > 0 && it.seq%128 == 0 {
				time.Sleep(straggle)
			}
		}
	}
	p := mustNew(b, k, Config{Workers: workers, Lanes: 4 * workers, Capacity: 1024}, h)
	defer func() { _ = p.Close() }()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := p.Submit(ctx, stream[i%streamLen], item{key: stream[i%streamLen].Key, seq: i}); err != nil {
			b.Fatalf("Submit: %v", err)
		}
	}
	if err := p.Drain(ctx); err != nil {
		b.Fatalf("Drain: %v", err)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	_ = sunk
}

// BenchmarkRun/Route isolates the router half of the family: what one lane
// decision costs without the queue, the wake or the handler.
func BenchmarkRoute(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			p := mustNew(b, k, Config{Workers: 8, Lanes: 32, Capacity: 8}, func(item) {})
			defer func() { _ = p.Close() }()
			stream := keyStream("uniform", true, 4096)
			b.ReportAllocs()
			lane := 0
			i := 0
			for b.Loop() {
				lane += p.Route(stream[i%len(stream)])
				i++
			}
			if lane < 0 {
				b.Fatal("unreachable")
			}
		})
	}
}

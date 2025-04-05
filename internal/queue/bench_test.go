package queue

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// benchCapacity is the lane capacity used by every queue benchmark.
const benchCapacity = 4096

// benchMatrix returns the dimension sweeps IQMQ_BENCH_MATRIX selects.
//
// `full` is the grid the bake-off measures this family the points the primary
// criterion -- "ns/op & p99 under MPMC contention, 0 allocs", over a
// benchmark column that names near-full and near-empty -- is actually read
// at. P=1 is the uncontended reference and P=32 the contention point; the
// occupancies are the two ends the row names; the batch sizes are the two
// ends of the row's {1,16,256}, so that both halves of the queue interface
// (per-item and batch-fill, the specification) are measured.
//
// `huge` is the row's whole benchmark column, {1,4,8,32} x {1,16,256} x
// {0,50,95}. It is not what the campaign measures: it is 873 rows against
// `full`'s 226, and at the harness's ten counts x four GOMAXPROCS rungs x one
// second that is 9.7 hours per build against 2.5 -- for the middle rungs of a
// contention axis the harness already sweeps on every row (-cpu=1,4,8,32) and
// a middle occupancy no criterion reads. The specification records the trade.
//
// The default is the smoke grid: `make bench-quick`, ` <fam>
// quick` and `bakeoff lint`'s one-iteration listing run here, and nothing
// decides anything from it.
func benchMatrix() (ps, batches, occupancies []int) {
	switch os.Getenv("IQMQ_BENCH_MATRIX") {
	case "huge":
		return []int{1, 4, 8, 32}, []int{1, 16, 256}, []int{0, 50, 95}
	case "full":
		return []int{1, 32}, []int{1, 256}, []int{0, 95}
	default:
		return []int{1, 4}, []int{1, 16}, []int{50}
	}
}

// maxLatencySamples bounds the per-producer sample buffer of the latency arm.
const maxLatencySamples = 1 << 14

// slotBytes reports the per-slot footprint of each kind.
func slotBytes(k Kind) int {
	switch k {
	case MutexRing, Chan, SPSC, Chunked:
		return int(unsafe.Sizeof(uint64(0)))
	case SeqMPMC, MPSC, ShardedMPMC:
		return int(unsafe.Sizeof(seqSlot[uint64]{}))
	case Disruptor:
		return int(unsafe.Sizeof(uint64(0)) + 2*unsafe.Sizeof(atomic.Int64{}))
	case AppendLog:
		return int(unsafe.Sizeof(uint64(0)) + unsafe.Sizeof(atomic.Int64{}))
	default:
		return 0
	}
}

// BenchmarkMemory/Queue/<kind>/<mode>/P=<n>/batch=<b>/occupancy=<pct>
// transfers b.N uint64 values through a queue of benchCapacity slots with
// the producer/consumer counts implied by the mode (P per multi side).
//
// The same name with a trailing /latency is the p99 arm: the throughput rows
// report ns/op, B/op and allocs/op, and the latency arm reports the
// distribution of one enqueue's own wall time as p50-ns, p99-ns and p999-ns,
// which is the other half of the criterion and the quantity the specification's
// veto is stated against. It is a row of its own because timing every
// operation costs more than the operation does -- a time.Now pair is tens of
// nanoseconds against a queue op's tens -- so measuring the tail inside the
// throughput rows would report neither. It exists only at the point the
// criterion names it at (MPMC, the grid's widest P, per-item pushes), which
// is two rows per kind under `full` and one under the smoke grid; kinds whose
// Modes() exclude MPMC have none, and declares them in
// PARTIAL_KINDS. The specification records the design.
func BenchmarkMemory(b *testing.B) {
	b.Run("Queue", func(b *testing.B) {
		ps, batches, occs := benchMatrix()
		widest := ps[len(ps)-1]
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, m := range []Mode{ModeSPSC, ModeMPSC, ModeSPMC, ModeMPMC} {
					if !k.Modes().Has(m) {
						continue
					}
					for _, p := range ps {
						if m == ModeSPSC && p != 1 {
							continue
						}
						for _, batch := range batches {
							for _, occ := range occs {
								name := fmt.Sprintf("%s/P=%d/batch=%d/occupancy=%d", m, p, batch, occ)
								b.Run(name, func(b *testing.B) { benchTransfer(b, k, m, p, batch, occ) })
								if m == ModeMPMC && p == widest && batch == 1 {
									b.Run(name+"/latency", func(b *testing.B) { benchLatency(b, k, p, occ) })
								}
							}
						}
					}
				}
			})
		}
	})
}

// benchLatency measures how long one enqueue takes under MPMC contention:
// p producers each time their own time-to-enqueue -- the whole retry loop for
// one item, which is what a producer waits when the queue is full -- while p
// consumers drain. The samples are per producer and preallocated, so the
// measured loop allocates nothing beyond the queue's own.
func benchLatency(b *testing.B, k Kind, p, occ int) {
	q := New[uint64](k, benchCapacity)
	prefill := benchCapacity * occ / 100
	for i := range prefill {
		q.TryPush(uint64(i))
	}
	total := int64(b.N + prefill)
	perProducer := b.N / p
	extra := b.N - perProducer*p
	var popped atomic.Int64
	var wg sync.WaitGroup
	// The sample buffers are allocated before the timer starts, so the
	// latency arm reports the same 0 allocs/op the throughput rows do.
	samples := make([][]int64, p)
	for pi := range samples {
		samples[pi] = make([]int64, 0, min(perProducer+extra, maxLatencySamples))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for pi := range p {
		n := perProducer
		if pi == 0 {
			n += extra
		}
		wg.Add(1)
		go func(pi, n int) {
			defer wg.Done()
			s := samples[pi]
			for i := range n {
				t0 := time.Now()
				for !q.TryPush(uint64(i)) {
					runtime.Gosched()
				}
				d := int64(time.Since(t0))
				if len(s) < cap(s) {
					s = append(s, d)
				}
			}
			samples[pi] = s
		}(pi, n)
	}
	for range p {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for popped.Load() < total {
				if _, ok := q.TryPop(); ok {
					popped.Add(1)
				} else {
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	reportQuantiles(b, samples)
}

// reportQuantiles reports the enqueue-latency distribution of the latency
// arm. `samples` is how many observations are behind the quantiles, so a
// reader can tell a tail from a rounding error.
func reportQuantiles(b *testing.B, perProducer [][]int64) {
	var all []int64
	for _, s := range perProducer {
		all = append(all, s...)
	}
	if len(all) == 0 {
		return
	}
	slices.Sort(all)
	q := func(pct float64) float64 {
		return float64(all[int(pct*float64(len(all)-1))])
	}
	b.ReportMetric(q(0.50), "p50-ns")
	b.ReportMetric(q(0.99), "p99-ns")
	b.ReportMetric(q(0.999), "p999-ns")
	b.ReportMetric(float64(len(all)), "samples")
}

func benchTransfer(b *testing.B, k Kind, m Mode, p, batch, occ int) {
	producers, consumers := 1, 1
	if m.MultiProducer() {
		producers = p
	}
	if m.MultiConsumer() {
		consumers = p
	}
	q := New[uint64](k, benchCapacity)
	prefill := benchCapacity * occ / 100
	for i := range prefill {
		q.TryPush(uint64(i))
	}
	total := int64(b.N + prefill)
	perProducer := b.N / producers
	extra := b.N - perProducer*producers
	var popped atomic.Int64
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for pi := range producers {
		n := perProducer
		if pi == 0 {
			n += extra
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if batch == 1 {
				for i := 0; i < n; {
					if q.TryPush(uint64(i)) {
						i++
					} else {
						runtime.Gosched()
					}
				}
				return
			}
			buf := make([]uint64, batch)
			for i := 0; i < n; {
				m := min(batch, n-i)
				for j := range m {
					buf[j] = uint64(i + j)
				}
				pushed := q.PushBatch(buf[:m])
				if pushed == 0 {
					runtime.Gosched()
				}
				i += pushed
			}
		}()
	}
	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if batch == 1 {
				for popped.Load() < total {
					if _, ok := q.TryPop(); ok {
						popped.Add(1)
					} else {
						runtime.Gosched()
					}
				}
				return
			}
			buf := make([]uint64, batch)
			for popped.Load() < total {
				n := q.PopBatch(buf)
				if n == 0 {
					runtime.Gosched()
					continue
				}
				popped.Add(int64(n))
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(slotBytes(k)), "bytes/slot") // after ResetTimer, which clears metrics
}

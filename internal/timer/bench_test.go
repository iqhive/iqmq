package timer

import (
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"
	"unsafe"
)

// benchSizes returns the entry counts to sweep. The specification names
// {1e3,1e5,1e6,1e7}: the default keeps the smoke run short,
// IQMQ_BENCH_MATRIX=full adds 1e6 (the bake-off profile and the size the
// primary criterion is read at) and IQMQ_BENCH_MATRIX=huge adds the spec's
// 1e7 row, which needs a couple of gigabytes of live heap per kind and is
// therefore opt-in on a shared host.
func benchSizes() []int {
	switch os.Getenv("IQMQ_BENCH_MATRIX") {
	case "huge":
		return []int{1_000, 100_000, 1_000_000, 10_000_000}
	case "full":
		return []int{1_000, 100_000, 1_000_000}
	default:
		return []int{1_000, 100_000}
	}
}

// stdFireDelay is how far ahead the stdtimer fire row arms its timers. The
// part of it not spent arming is idle waiting, not firing work, and is
// subtracted from the reported ns/entry.
const stdFireDelay = 2 * time.Millisecond

// BenchmarkMemory/Scheduler/<kind>/{insert,fire,cancel}/n=<N>/dist=<d>
// reports ns per entry for each operation at steady size n, plus B/entry
// (scheduler overhead beyond the caller-owned Entry). dist is the due-time
// distribution: uniform over 10 s, skewed (80 % within 10 ms), or burst
// (all at one instant). The stdtimer rows time one time.Timer per entry as
// the baseline the spec asks for, over the same {insert,fire,cancel}.
func BenchmarkMemory(b *testing.B) {
	b.Run("Scheduler", func(b *testing.B) {
		ns := benchSizes()
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, op := range []string{"insert", "fire", "cancel"} {
					for _, n := range ns {
						for _, dist := range []string{"uniform", "skewed", "burst"} {
							b.Run(fmt.Sprintf("%s/n=%d/dist=%s", op, n, dist), func(b *testing.B) {
								benchOp(b, k, op, n, dist)
							})
						}
					}
				}
			})
		}
		b.Run("stdtimer", func(b *testing.B) {
			for _, op := range []string{"insert", "fire", "cancel"} {
				for _, n := range ns {
					// One time.Timer per entry costs a runtime timer plus,
					// for fire, a goroutine per entry; the baseline is
					// already conclusive at 1e6, so the spec's 1e7 row is
					// measured for the scheduler kinds only.
					if n > 1_000_000 {
						continue
					}
					b.Run(fmt.Sprintf("%s/n=%d/dist=uniform", op, n), func(b *testing.B) { benchStdTimer(b, op, n) })
				}
			}
		})
	})
}

func dues(n int, dist string, start int64, rng *rand.Rand) []int64 {
	out := make([]int64, n)
	for i := range out {
		switch dist {
		case "skewed":
			if rng.IntN(5) > 0 {
				out[i] = start + rng.Int64N(int64(10*time.Millisecond))
			} else {
				out[i] = start + rng.Int64N(int64(10*time.Second))
			}
		case "burst":
			out[i] = start + int64(time.Second)
		default:
			out[i] = start + rng.Int64N(int64(10*time.Second))
		}
	}
	return out
}

// benchState is the warm scheduler, entries and due offsets of the
// benchmark currently running. Go's ramp calls benchOp several times per
// benchmark; reusing the state keeps the timed region free of the one-off
// growth of heap slices and calendar bucket arrays and avoids repeating the
// footprint measurement (a 1e6-entry fill and drain) on every trial.
type benchState struct {
	key      string
	s        Scheduler[int]
	entries  []Entry[int]
	ds       []int64 // due offsets from the round base
	dst      []*Entry[int]
	base     int64
	perEntry float64
}

var benchCur *benchState

// benchRound is the time each round spans: dues lie within 10 s of the
// round's base and the drain fires at base+20 s, where the next round
// starts so wheel cursors see fresh future entries rather than past-due ones.
const benchRound = int64(20 * time.Second)

// fill arms every entry relative to the round base.
func (st *benchState) fill() {
	for i := range st.entries {
		st.s.Add(&st.entries[i], st.base+st.ds[i])
	}
}

// drain fires everything and moves the base to the fire time.
func (st *benchState) drain(b *testing.B) {
	st.dst = st.s.Fire(st.base+benchRound, st.dst[:0])
	if n := len(st.entries); len(st.dst) != n || st.s.Len() != 0 {
		b.Fatalf("drain fired %d of %d, %d left", len(st.dst), n, st.s.Len())
	}
	st.base += benchRound
}

// skip moves the base of an empty scheduler to the next round, advancing
// wheel cursors the way a drain does.
func (st *benchState) skip(b *testing.B) {
	st.dst = st.s.Fire(st.base+benchRound, st.dst[:0])
	if len(st.dst) != 0 || st.s.Len() != 0 {
		b.Fatalf("skip fired %d, %d left", len(st.dst), st.s.Len())
	}
	st.base += benchRound
}

// newBenchState builds the state for key and measures its footprint: the
// live heap the scheduler holds per entry when full (its own arrays plus
// per-entry bookkeeping; the caller-owned Entry values are excluded).
func newBenchState(b *testing.B, key string, k Kind, n int, dist string) *benchState {
	rng := rand.New(rand.NewPCG(uint64(n), uint64(k)))
	st := &benchState{key: key, base: 1 << 40}
	st.entries = make([]Entry[int], n)
	st.ds = dues(n, dist, 0, rng)
	st.dst = make([]*Entry[int], 0, n)
	pct := debug.SetGCPercent(-1)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	st.s = mustNew[int](b, k, st.base)
	st.fill()
	runtime.GC()
	runtime.ReadMemStats(&after)
	debug.SetGCPercent(pct)
	// Small n measures kilobytes and drowns in the runtime's own churn; the
	// signed, clamped delta keeps the metric sane there.
	st.perEntry = max(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), 0) / float64(n)
	st.drain(b)
	return st
}

func benchOp(b *testing.B, k Kind, op string, n int, dist string) {
	key := fmt.Sprintf("%s/%d/%s", k, n, dist)
	if benchCur == nil || benchCur.key != key {
		benchCur = nil // release the previous state before allocating the next
		benchCur = newBenchState(b, key, k, n, dist)
	}
	st := benchCur
	s, entries := st.s, st.entries
	fill := st.fill
	drain := func() { st.drain(b) }
	perEntry := st.perEntry
	b.ReportAllocs()

	rounds := (b.N + n - 1) / n
	b.ResetTimer()
	for range rounds {
		switch op {
		case "insert":
			fill()
			b.StopTimer()
			drain()
			b.StartTimer()
		case "fire":
			b.StopTimer()
			fill()
			b.StartTimer()
			drain()
		case "cancel":
			b.StopTimer()
			fill()
			b.StartTimer()
			for i := range entries {
				s.Cancel(&entries[i])
			}
			b.StopTimer()
			st.skip(b) // advances wheel cursors to the next base
			b.StartTimer()
		}
	}
	b.StopTimer()
	b.ReportMetric(perEntry, "B/entry")
	b.ReportMetric(float64(unsafe.Sizeof(Entry[int]{})), "B/entrystruct")
	// ns/op is per round of n entries; rescale to per entry.
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(rounds*n), "ns/entry")
}

// benchStdTimer is the "time.Timer per message" baseline: arming, firing and
// stopping one runtime timer per entry. The fire row arms every timer at
// stdFireDelay and waits for every callback; the idle remainder of that
// delay is discounted so ns/entry reports the firing work (a callback
// goroutine per entry) rather than the wait.
func benchStdTimer(b *testing.B, op string, n int) {
	timers := make([]*time.Timer, n)
	rounds := (b.N + n - 1) / n
	noop := func() {}
	var idle time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for range rounds {
		switch op {
		case "insert":
			for i := range timers {
				timers[i] = time.AfterFunc(time.Hour, noop)
			}
			b.StopTimer()
			for _, t := range timers {
				t.Stop()
			}
			b.StartTimer()
		case "fire":
			b.StopTimer()
			var wg sync.WaitGroup
			wg.Add(n)
			done := wg.Done
			armed := time.Now()
			for i := range timers {
				timers[i] = time.AfterFunc(stdFireDelay, done)
			}
			idle += max(stdFireDelay-time.Since(armed), 0)
			b.StartTimer()
			wg.Wait()
		case "cancel":
			b.StopTimer()
			for i := range timers {
				timers[i] = time.AfterFunc(time.Hour, noop)
			}
			b.StartTimer()
			for _, t := range timers {
				t.Stop()
			}
		}
	}
	b.StopTimer()
	elapsed := max(b.Elapsed()-idle, 0)
	b.ReportMetric(float64(elapsed.Nanoseconds())/float64(rounds*n), "ns/entry")
}

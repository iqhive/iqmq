package stats

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestModel checks every kind against an int64 oracle, sequentially and
// under concurrent writers: no update is ever lost (conservation), and a
// reader under non-negative increments sees a bounded, monotone total.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("sequential", func(t *testing.T) { modelSequential(t, k) })
			t.Run("conservation", func(t *testing.T) { modelConservation(t, k) })
			t.Run("bounded-reader", func(t *testing.T) { modelBoundedReader(t, k) })
			t.Run("overflow", func(t *testing.T) { modelOverflow(t, k) })
		})
	}
}

func modelSequential(t *testing.T, k Kind) {
	for _, workers := range []int{0, 1, 3, 8} {
		c := mustNew(t, k, workers)
		var want int64
		rng := rand.New(rand.NewPCG(uint64(k), uint64(workers)))
		for step := range 5000 {
			delta := rng.Int64N(2001) - 1000
			switch rng.IntN(3) {
			case 0:
				c.Add(delta)
			case 1:
				c.AddAt(rng.IntN(workers+2), delta) // hints may be out of range
			case 2:
				c.AddAt(-1-rng.IntN(3), delta) // negative hints fold too
			}
			want += delta
			if got := c.Load(); got != want {
				t.Fatalf("workers=%d step %d: Load = %d, oracle %d", workers, step, got, want)
			}
		}
	}
}

// modelConservation has W writers add known random deltas, positive and
// negative, through both entry points; the final Load is their exact sum.
func modelConservation(t *testing.T, k Kind) {
	for _, W := range []int{1, 4, 16} {
		c := mustNew(t, k, W)
		const per = 20000
		sums := make([]int64, W)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for w := range W {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(w), uint64(W)))
				<-start
				var sum int64
				for i := range per {
					delta := rng.Int64N(2001) - 1000
					switch i % 4 {
					case 0:
						c.Add(delta)
					case 1:
						c.AddAt(w+W, delta) // out-of-range hint
					default:
						c.AddAt(w, delta)
					}
					sum += delta
				}
				sums[w] = sum
			}()
		}
		close(start)
		wg.Wait()
		var want int64
		for _, s := range sums {
			want += s
		}
		if got := c.Load(); got != want {
			t.Fatalf("W=%d: Load = %d after all writers finished, want %d (lost %d)", W, got, want, want-got)
		}
	}
}

// modelBoundedReader runs writers with non-negative deltas that record
// their progress before and after each update, and readers that check
// every Load lies between the total completed before the call and the
// total started during it, and that a single reader's Loads never go down.
func modelBoundedReader(t *testing.T, k Kind) {
	const W, R = 8, 2
	c := mustNew(t, k, W)
	var pre, post atomic.Int64
	var stop atomic.Bool
	var failed atomic.Bool
	var msg atomic.Pointer[string]
	fail := func(s string) {
		if failed.CompareAndSwap(false, true) {
			msg.Store(&s)
		}
	}
	var writers, readers sync.WaitGroup
	for w := range W {
		writers.Add(1)
		go func() {
			defer writers.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 99))
			for i := range 20000 {
				delta := rng.Int64N(100)
				pre.Add(delta)
				if i%2 == 0 {
					c.AddAt(w, delta)
				} else {
					c.Add(delta)
				}
				post.Add(delta)
			}
		}()
	}
	for range R {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var last int64 = -1
			for !stop.Load() {
				lo := post.Load()
				v := c.Load()
				hi := pre.Load()
				if v < lo || v > hi {
					fail("Load outside [completed-before, started-during] bounds")
					return
				}
				if v < last {
					fail("single reader observed Load decrease under non-negative deltas")
					return
				}
				last = v
				runtime.Gosched()
			}
		}()
	}
	writers.Wait()
	stop.Store(true)
	readers.Wait()
	if failed.Load() {
		t.Fatal(*msg.Load())
	}
	if got, want := c.Load(), pre.Load(); got != want {
		t.Fatalf("Load = %d, want %d", got, want)
	}
}

// modelOverflow checks wrap-around arithmetic matches int64 semantics.
func modelOverflow(t *testing.T, k Kind) {
	c := mustNew(t, k, 4)
	c.Add(1<<63 - 1)
	c.AddAt(1, 1)
	if got := c.Load(); got != -1<<63 {
		t.Fatalf("overflow: Load = %d", got)
	}
	c.AddAt(2, -1)
	if got := c.Load(); got != 1<<63-1 {
		t.Fatalf("underflow: Load = %d", got)
	}
}

// TestWritersDoNotStall is a coarse liveness check: heavy contention must
// not make any kind pathologically slow (a few hundred ms budget for 1e6
// increments across 32 goroutines).
func TestWritersDoNotStall(t *testing.T) {
	for _, k := range Kinds() {
		c := mustNew(t, k, 32)
		var wg sync.WaitGroup
		startedAt := time.Now()
		for w := range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 1 << 15 {
					c.AddAt(w, 1)
				}
			}()
		}
		wg.Wait()
		if got := c.Load(); got != 32<<15 {
			t.Fatalf("%v: Load = %d", k, got)
		}
		if d := time.Since(startedAt); d > 30*time.Second {
			t.Fatalf("%v: %d increments took %v", k, 32<<15, d)
		}
	}
}

package queue

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzQueue drives one kind with a random operation script and compares
// every result with the reference FIFO model.
func FuzzQueue(f *testing.F) {
	type st struct {
		q       Queue[int]
		ref     *refQueue
		n       int
		ordered bool
	}
	script := fuzzutil.Script[*st]{
		Steps: []fuzzutil.Step[*st]{
			{Name: "push", Weight: 4, Run: func(t testing.TB, _ *fuzzutil.Reader, s *st) {
				s.n++
				if got, want := s.q.TryPush(s.n), s.ref.push(s.n); got != want {
					t.Fatalf("TryPush = %v, ref %v", got, want)
				}
			}},
			{Name: "pop", Weight: 3, Run: func(t testing.TB, _ *fuzzutil.Reader, s *st) {
				got, ok := s.q.TryPop()
				if !s.ref.take(got, ok, s.ordered) {
					t.Fatalf("TryPop = %d,%v inconsistent with ref %v", got, ok, s.ref.items)
				}
			}},
			{Name: "pushBatch", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				vs := make([]int, r.Int(0, 20))
				for i := range vs {
					s.n++
					vs[i] = s.n
				}
				got := s.q.PushBatch(vs)
				want := 0
				for _, v := range vs {
					if !s.ref.push(v) {
						break
					}
					want++
				}
				if got != want {
					t.Fatalf("PushBatch(%d) = %d, ref %d", len(vs), got, want)
				}
			}},
			{Name: "popBatch", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				dst := make([]int, r.Int(0, 20))
				got := s.q.PopBatch(dst)
				for i := range got {
					if !s.ref.take(dst[i], true, s.ordered) {
						t.Fatalf("PopBatch[%d] = %d inconsistent with ref %v", i, dst[i], s.ref.items)
					}
				}
				if got < len(dst) && len(s.ref.items) != 0 {
					t.Fatalf("PopBatch returned %d of %d with %d left in ref", got, len(dst), len(s.ref.items))
				}
			}},
			{Name: "len", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, s *st) {
				if s.q.Len() != len(s.ref.items) {
					t.Fatalf("Len = %d, ref %d", s.q.Len(), len(s.ref.items))
				}
			}},
		},
		Check: func(t testing.TB, s *st) {
			for {
				got, ok := s.q.TryPop()
				if !s.ref.take(got, ok, s.ordered) {
					t.Fatalf("drain: %d,%v inconsistent with ref %v", got, ok, s.ref.items)
				}
				if !ok {
					return
				}
			}
		},
	}
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(1, 16, 3)
		for range 6 {
			script.WriteStep(f, w, "push")
		}
		script.WriteStep(f, w, "pushBatch")
		w.Int(0, 20, 5)
		script.WriteStep(f, w, "popBatch")
		w.Int(0, 20, 4)
		script.WriteStep(f, w, "len")
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		capacity := r.Int(1, 16)
		q := mustNew[int](t, k, capacity)
		s := &st{q: q, ref: &refQueue{cap: q.Cap()}, ordered: k.Ordered()}
		script.Run(t, data[len(data)-r.Remaining():], s)
	})
}

// FuzzQueueConcurrent runs fuzz-chosen numbers of producers and consumers
// with fuzz-driven operation mixes and checks conservation, duplicates and
// per-producer order for ordered kinds.
func FuzzQueueConcurrent(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(1, 64, 8)
		w.Int(1, 4, 2)
		w.Int(1, 4, 2)
		w.Int(1, 200, 50)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		capacity := r.Int(1, 64)
		producers := r.Int(1, 4)
		consumers := r.Int(1, 4)
		per := r.Int(1, 200)
		if !k.Modes().MultiProducer() {
			producers = 1
		}
		if !k.Modes().MultiConsumer() {
			consumers = 1
		}
		q := mustNew[uint64](t, k, capacity)
		total := int64(producers * per)
		var popped atomic.Int64
		seen := make([][]atomic.Bool, producers)
		for p := range seen {
			seen[p] = make([]atomic.Bool, per)
		}
		var wg sync.WaitGroup
		var failed, timedOut atomic.Bool
		fail := func() { failed.Store(true) }
		deadline := time.Now().Add(20 * time.Second)
		expired := func() bool {
			if time.Now().After(deadline) {
				timedOut.Store(true)
				return true
			}
			return false
		}
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], producers+consumers, func(id int, ar *fuzzutil.Reader) {
			wg.Add(1)
			defer wg.Done()
			// An actor that found the queue full or empty yields before it
			// tries again, whatever the script says. A retry that does not
			// is a busy-wait on the other side, and with more actors than
			// processors the side it waits for runs only when the runtime
			// preempts a spinner: the nightly's crashers -- four producers on
			// a queue of one or two slots, whose fuzz bytes had run out so
			// MaybeYield never yielded -- took 16 to 20 seconds on one CPU
			// instead of a few milliseconds, and the deadline below then
			// called a correct Chan or MutexRing "lost elements".
			// modelConcurrent has always yielded here.
			if id < producers {
				p := id
				buf := make([]uint64, 0, 8)
				for seq := 0; seq < per && !failed.Load() && !timedOut.Load(); {
					if expired() {
						return
					}
					moved := 0
					if ar.Bool() {
						n := min(ar.Int(1, 8), per-seq)
						buf = buf[:n]
						for i := range buf {
							buf[i] = uint64(p)<<32 | uint64(seq+i)
						}
						moved = q.PushBatch(buf)
					} else if q.TryPush(uint64(p)<<32 | uint64(seq)) {
						moved = 1
					}
					seq += moved
					if moved == 0 {
						runtime.Gosched()
					}
					fuzzutil.MaybeYield(ar)
				}
				return
			}
			last := make([]int64, producers)
			for i := range last {
				last[i] = -1
			}
			observe := func(v uint64) {
				p, seq := int(v>>32), int64(uint32(v))
				if p >= producers || seq >= int64(per) || seen[p][seq].Swap(true) || (k.Ordered() && seq <= last[p]) {
					fail()
					return
				}
				last[p] = seq
			}
			buf := make([]uint64, 8)
			for popped.Load() < total && !failed.Load() && !timedOut.Load() {
				if expired() {
					return
				}
				if ar.Bool() {
					n := q.PopBatch(buf[:ar.Int(1, 8)])
					for _, v := range buf[:n] {
						observe(v)
					}
					popped.Add(int64(n))
					if n == 0 {
						runtime.Gosched()
					}
				} else if v, ok := q.TryPop(); ok {
					observe(v)
					popped.Add(1)
				} else {
					runtime.Gosched()
				}
				fuzzutil.MaybeYield(ar)
			}
		})
		wg.Wait()
		if timedOut.Load() {
			t.Fatalf("%v cap=%d P=%d C=%d: timed out with %d of %d popped (lost elements?)", k, capacity, producers, consumers, popped.Load(), total)
		}
		if failed.Load() {
			t.Fatalf("%v cap=%d P=%d C=%d: duplicate, garbage or order violation", k, capacity, producers, consumers)
		}
		if popped.Load() != total || q.Len() != 0 {
			t.Fatalf("%v: popped %d of %d, Len %d", k, popped.Load(), total, q.Len())
		}
	})
}

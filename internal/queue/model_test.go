package queue

import (
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestModel checks every kind against a reference FIFO model, sequentially
// and under each concurrency mode the kind claims to support.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("sequential", func(t *testing.T) { modelSequential(t, k) })
			t.Run("capacity", func(t *testing.T) { modelCapacity(t, k) })
			t.Run("batch", func(t *testing.T) { modelBatch(t, k) })
			if k.Bounded() && k.Modes().MultiProducer() {
				t.Run("nowait", func(t *testing.T) { modelNoWait(t, k) })
			}
			for _, m := range []Mode{ModeSPSC, ModeMPSC, ModeSPMC, ModeMPMC} {
				if !k.Modes().Has(m) {
					continue
				}
				t.Run(m.String(), func(t *testing.T) { modelConcurrent(t, k, m) })
			}
		})
	}
}

// refQueue is the FIFO oracle.
type refQueue struct {
	items []int
	cap   int // 0 = unbounded
}

func (r *refQueue) push(v int) bool {
	if r.cap > 0 && len(r.items) >= r.cap {
		return false
	}
	r.items = append(r.items, v)
	return true
}

func (r *refQueue) pop() (int, bool) {
	if len(r.items) == 0 {
		return 0, false
	}
	v := r.items[0]
	r.items = r.items[1:]
	return v, true
}

// take removes got from the oracle. Ordered kinds must return the head;
// unordered kinds may return any queued element.
func (r *refQueue) take(got int, ok, ordered bool) bool {
	if !ok {
		return len(r.items) == 0
	}
	if ordered {
		want, wok := r.pop()
		return wok && want == got
	}
	for i, v := range r.items {
		if v == got {
			r.items = append(r.items[:i], r.items[i+1:]...)
			return true
		}
	}
	return false
}

func modelSequential(t *testing.T, k Kind) {
	for _, capacity := range []int{1, 2, 3, 7, 64} {
		q := mustNew[int](t, k, capacity)
		ref := &refQueue{cap: q.Cap()}
		rng := rand.New(rand.NewPCG(uint64(capacity), 42))
		for step := range 4000 {
			switch rng.IntN(5) {
			case 0, 1, 2:
				v := step
				got, want := q.TryPush(v), ref.push(v)
				if got != want {
					t.Fatalf("cap=%d step %d: TryPush = %v, ref %v (len %d)", capacity, step, got, want, len(ref.items))
				}
			case 3, 4:
				got, ok := q.TryPop()
				if !ref.take(got, ok, k.Ordered()) {
					t.Fatalf("cap=%d step %d: TryPop = %d,%v not consistent with ref %v", capacity, step, got, ok, ref.items)
				}
			}
			if q.Len() != len(ref.items) {
				t.Fatalf("cap=%d step %d: Len = %d, ref %d", capacity, step, q.Len(), len(ref.items))
			}
		}
	}
}

func modelCapacity(t *testing.T, k Kind) {
	q := mustNew[int](t, k, 5)
	if !k.Bounded() {
		for i := range 1000 {
			if !q.TryPush(i) {
				t.Fatal("unbounded push refused")
			}
		}
		if q.Len() != 1000 {
			t.Fatalf("Len = %d", q.Len())
		}
		for i := range 1000 {
			if v, ok := q.TryPop(); !ok || v != i {
				t.Fatalf("pop %d: %d,%v", i, v, ok)
			}
		}
		return
	}
	c := q.Cap()
	for i := range c {
		if !q.TryPush(i) {
			t.Fatalf("push %d of %d refused", i, c)
		}
	}
	if q.TryPush(-1) {
		t.Fatal("push beyond Cap accepted")
	}
	if q.Len() != c {
		t.Fatalf("Len = %d, Cap = %d", q.Len(), c)
	}
	// Wrap around several times to exercise index arithmetic.
	for round := range 3 * c {
		v, ok := q.TryPop()
		if !ok || v != round {
			t.Fatalf("round %d: pop %d,%v", round, v, ok)
		}
		if !q.TryPush(round + c) {
			t.Fatalf("round %d: refill refused", round)
		}
	}
	for q.Len() > 0 {
		if _, ok := q.TryPop(); !ok {
			t.Fatal("Len > 0 but pop failed")
		}
	}
	if _, ok := q.TryPop(); ok {
		t.Fatal("pop on empty succeeded")
	}
}

func modelBatch(t *testing.T, k Kind) {
	q := mustNew[int](t, k, 10)
	c := q.Cap()
	if c == 0 {
		c = 16
	}
	in := make([]int, c+3)
	for i := range in {
		in[i] = i
	}
	n := q.PushBatch(in)
	switch {
	case k.Bounded() && n != c:
		t.Fatalf("PushBatch = %d, want Cap %d", n, c)
	case !k.Bounded() && n != len(in):
		t.Fatalf("PushBatch = %d, want %d", n, len(in))
	}
	if q.PushBatch(nil) != 0 {
		t.Fatal("PushBatch(nil)")
	}
	out := make([]int, 4)
	got := q.PopBatch(out)
	if got != 4 {
		t.Fatalf("PopBatch = %d", got)
	}
	if !k.Ordered() {
		// Unordered kinds: only conservation is checked.
		rest := make([]int, n)
		m := q.PopBatch(rest)
		if m != n-4 {
			t.Fatalf("PopBatch rest = %d, want %d", m, n-4)
		}
		return
	}
	for i, v := range out {
		if v != i {
			t.Fatalf("PopBatch[%d] = %d", i, v)
		}
	}
	rest := make([]int, n)
	m := q.PopBatch(rest)
	if m != n-4 {
		t.Fatalf("PopBatch rest = %d, want %d", m, n-4)
	}
	for i, v := range rest[:m] {
		if v != i+4 {
			t.Fatalf("rest[%d] = %d", i, v)
		}
	}
	if q.PopBatch(rest) != 0 || q.PopBatch(nil) != 0 {
		t.Fatal("PopBatch on empty")
	}
}

// modelConcurrent runs producers and consumers for mode m and checks
// conservation, absence of duplicates and (for ordered kinds) per-producer
// FIFO as observed by each consumer.
func modelConcurrent(t *testing.T, k Kind, m Mode) {
	producers, consumers := 1, 1
	if m.MultiProducer() {
		producers = 4
	}
	if m.MultiConsumer() {
		consumers = 4
	}
	const perProducer = 20000
	q := mustNew[uint64](t, k, 64)
	var pushed, popped atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(30 * time.Second)
	seen := make([][]atomic.Uint32, producers) // seen[p][seq] = consumer id + 1
	for p := range seen {
		seen[p] = make([]atomic.Uint32, perProducer)
	}
	orderErr := make(chan string, producers+consumers)
	for p := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]uint64, 8)
			for seq := 0; seq < perProducer; {
				if time.Now().After(deadline) {
					orderErr <- "producer " + itoa(p) + ": timeout at seq " + itoa(seq)
					return
				}
				if seq%3 == 0 && seq+len(batch) <= perProducer {
					for i := range batch {
						batch[i] = uint64(p)<<32 | uint64(seq+i)
					}
					n := q.PushBatch(batch)
					if n == 0 {
						runtime.Gosched()
						continue
					}
					seq += n
					pushed.Add(int64(n))
					continue
				}
				if q.TryPush(uint64(p)<<32 | uint64(seq)) {
					seq++
					pushed.Add(1)
				} else {
					runtime.Gosched()
				}
			}
		}()
	}
	total := int64(producers * perProducer)
	for c := range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := make([]int64, producers)
			for i := range last {
				last[i] = -1
			}
			buf := make([]uint64, 8)
			observe := func(v uint64) bool {
				p, seq := int(v>>32), int64(uint32(v))
				if p >= producers || seq >= perProducer {
					orderErr <- "consumer " + itoa(c) + ": garbage value"
					return false
				}
				if !seen[p][seq].CompareAndSwap(0, uint32(c)+1) {
					orderErr <- "consumer " + itoa(c) + ": duplicate p=" + itoa(p) + " seq=" + itoa(int(seq))
					return false
				}
				if k.Ordered() && seq <= last[p] {
					orderErr <- "consumer " + itoa(c) + ": order violation p=" + itoa(p) + " seq " + itoa(int(seq)) + " after " + itoa(int(last[p]))
					return false
				}
				last[p] = seq
				return true
			}
			for popped.Load() < total {
				if time.Now().After(deadline) {
					orderErr <- "consumer " + itoa(c) + ": timeout"
					return
				}
				if c%2 == 0 {
					n := q.PopBatch(buf)
					if n == 0 {
						runtime.Gosched()
						continue
					}
					for _, v := range buf[:n] {
						if !observe(v) {
							return
						}
					}
					popped.Add(int64(n))
					continue
				}
				v, ok := q.TryPop()
				if !ok {
					runtime.Gosched()
					continue
				}
				if !observe(v) {
					return
				}
				popped.Add(1)
			}
		}()
	}
	wg.Wait()
	select {
	case msg := <-orderErr:
		t.Fatal(msg)
	default:
	}
	if pushed.Load() != total || popped.Load() != total {
		t.Fatalf("pushed %d popped %d want %d", pushed.Load(), popped.Load(), total)
	}
	if q.Len() != 0 {
		t.Fatalf("Len after drain = %d", q.Len())
	}
	if _, ok := q.TryPop(); ok {
		t.Fatal("pop after drain succeeded")
	}
	for p := range seen {
		for s := range seen[p] {
			if seen[p][s].Load() == 0 {
				t.Fatalf("p=%d seq=%d never delivered", p, s)
			}
		}
	}
}

// modelNoWait holds a multi-producer kind to the package contract that a
// push never waits for capacity: producers racing for the last free slots of
// a queue nobody is consuming must each come back, and between them take no
// more slots than were free.
//
// It is a race, and the defect it is about lands only when two producers
// pass a fullness check before either claims: the Disruptor's fetch-add
// claim did so about once in two hundred rounds on a 64-core host, and the
// producer that lost then spun until a consumer took the slot's previous
// occupant -- for ever, here. A correct kind returns in
// microseconds, so the wait below is a hang detector, not a timing: it can
// only fail on a push that has not returned at all.
//
// It is a probabilistic pin. Against the fetch-add claim it failed at rounds
// 57, 208, 13 and 302 of 2,000 on a 64-core host, so a pass there is strong
// evidence; at GOMAXPROCS=1 two producers are never between the check and the
// claim at once -- nothing preempts one inside that window often enough -- and
// a pass there says nothing about the claim.
func modelNoWait(t *testing.T, k Kind) {
	const (
		rounds    = 2000
		producers = 4
	)
	for round := range rounds {
		q := mustNew[int](t, k, 1+round%3)
		free := q.Cap() - 1
		for i := range free {
			if !q.TryPush(i) {
				t.Fatalf("round %d: push %d of %d refused on a queue with room", round, i, q.Cap())
			}
		}
		free = q.Cap() - q.Len()
		start := make(chan struct{})
		took := make(chan int, producers)
		for p := range producers {
			go func() {
				<-start
				if p%2 == 0 {
					took <- q.PushBatch([]int{-1, -2})
					return
				}
				if q.TryPush(-1) {
					took <- 1
					return
				}
				took <- 0
			}()
		}
		close(start)
		n := 0
		hang := time.NewTimer(10 * time.Second)
		for range producers {
			select {
			case got := <-took:
				n += got
			case <-hang.C:
				t.Fatalf("round %d: a push into a %d-slot queue with %d free and no consumer had not "+
					"returned after 10s: it is waiting for capacity (Len=%d)", round, q.Cap(), free, q.Len())
			}
		}
		hang.Stop()
		if n > free {
			t.Fatalf("round %d: the racing pushes took %d slots, %d were free (Len=%d, Cap=%d)",
				round, n, free, q.Len(), q.Cap())
		}
	}
}

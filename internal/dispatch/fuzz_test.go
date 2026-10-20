package dispatch

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/wait"
)

// keylessKey is the sink key under which items with no ordering key are
// recorded. They may be handled in any order and several at a time, which is
// the whole point of submitting them without a key.
const keylessKey uint64 = 0

// fuzzQueues are the lane queues a pool may be built on: every kind that
// is bounded, FIFO and multi-consumer, which is what Config.Validate allows.
var fuzzQueues = []queue.Kind{queue.MutexRing, queue.Chan, queue.SeqMPMC, queue.Disruptor, queue.AppendLog}

// fuzzWaits are the strategies a pool may park on. The two spinning kinds
// are left out on purpose: they never yield a processor, and a fuzz run that
// holds four of them for a whole corpus would burn four cores for the whole
// run without testing anything the parking kinds do not.
var fuzzWaits = []wait.Kind{wait.Chan, wait.Cond, wait.EventCount, wait.SpinPark, wait.Hybrid}

// dispatchHarness drives one pool and the model of what it was given: the
// per-key streams that were submitted. The pool is correct when everything
// accepted comes back exactly once, per-key streams come back in submission
// order for the kinds that promise it, and no two items of one key are ever
// in a handler at the same time.
type dispatchHarness struct {
	kind       Kind
	cfg        Config
	p          Pool[item]
	s          *sink
	want       map[uint64][]int
	next       map[uint64]int
	keys       int
	partitions int32
	closed     bool
	ctx        context.Context //nolint:containedctx // the script steps share one background context
}

func newDispatchHarness(tb testing.TB, k Kind, cfg Config, keys int, partitions int32) *dispatchHarness {
	s := newSink()
	return &dispatchHarness{
		kind:       k,
		cfg:        cfg,
		p:          mustNew(tb, k, cfg, s.handle),
		s:          s,
		want:       map[uint64][]int{},
		next:       map[uint64]int{},
		keys:       keys,
		partitions: partitions,
		ctx:        context.Background(),
	}
}

// route returns the Route of key, giving it the partition a publisher would
// have chosen for it: the same key always reaches the same partition, which
// is what lets the per-partition router preserve its order.
func (h *dispatchHarness) route(key uint64) Route {
	if key == keylessKey {
		r := Keyless()
		if h.partitions > 0 {
			r = r.InPartition(int32(h.next[keylessKey] % int(h.partitions)))
		}
		return r
	}
	r := Keyed(key)
	if h.partitions > 0 {
		r = r.InPartition(int32(mix(key) % uint64(h.partitions)))
	}
	return r
}

// submit blocks until the item is accepted, and records what the pool was
// told. A closed pool must refuse it.
func (h *dispatchHarness) submit(t testing.TB, key uint64) {
	t.Helper()
	seq := h.next[key]
	err := h.p.Submit(h.ctx, h.route(key), item{key: key, seq: seq})
	switch {
	case err == nil:
		if h.closed {
			t.Fatalf("Submit succeeded on a closed pool")
		}
		h.next[key] = seq + 1
		h.want[key] = append(h.want[key], seq)
	case errors.Is(err, ErrClosed):
		if !h.closed {
			t.Fatalf("Submit reported ErrClosed on an open pool")
		}
	default:
		t.Fatalf("Submit: %v", err)
	}
}

// trySubmit records the item only when the pool took it: a full lane is a
// legitimate answer, and the item was then never accepted.
func (h *dispatchHarness) trySubmit(t testing.TB, key uint64) {
	t.Helper()
	seq := h.next[key]
	if h.p.TrySubmit(h.route(key), item{key: key, seq: seq}) {
		if h.closed {
			t.Fatalf("TrySubmit succeeded on a closed pool")
		}
		h.next[key] = seq + 1
		h.want[key] = append(h.want[key], seq)
	}
}

// drain waits for the pool to fall idle and compares it with the model.
func (h *dispatchHarness) drain(t testing.TB) {
	t.Helper()
	if err := h.p.Drain(h.ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if n := h.p.Pending(); n != 0 {
		t.Fatalf("Pending = %d after Drain", n)
	}
	h.s.compare(t, h.want, h.kind.PreservesKeyOrder())
}

// close stops the pool; everything accepted must have been handled by the
// time it returns.
func (h *dispatchHarness) close(t testing.TB) {
	t.Helper()
	if err := h.p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h.closed = true
	if n := h.p.Pending(); n != 0 {
		t.Fatalf("Pending = %d after Close", n)
	}
	h.s.compare(t, h.want, h.kind.PreservesKeyOrder())
}

// dispatchScript is the op script.
func dispatchScript() *fuzzutil.Script[*dispatchHarness] {
	key := func(r *fuzzutil.Reader, h *dispatchHarness) uint64 { return uint64(r.Int(1, h.keys)) }
	return &fuzzutil.Script[*dispatchHarness]{
		Steps: []fuzzutil.Step[*dispatchHarness]{
			{Name: "submit", Weight: 6, Run: func(t testing.TB, r *fuzzutil.Reader, h *dispatchHarness) {
				h.submit(t, key(r, h))
			}},
			{Name: "keyless", Weight: 3, Run: func(t testing.TB, _ *fuzzutil.Reader, h *dispatchHarness) {
				h.submit(t, keylessKey)
			}},
			{Name: "try", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *dispatchHarness) {
				h.trySubmit(t, key(r, h))
			}},
			{Name: "burst", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *dispatchHarness) {
				// A burst on one key is what a partition of a broker looks
				// like; it is also what makes a lane fill up.
				k := key(r, h)
				for range r.Int(2, 12) {
					h.submit(t, k)
				}
			}},
			{Name: "drain", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, h *dispatchHarness) {
				h.drain(t)
			}},
			{Name: "close", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, h *dispatchHarness) {
				h.close(t)
			}},
		},
		MaxSteps: 128,
		Check: func(t testing.TB, h *dispatchHarness) {
			if n := h.p.Pending(); n < 0 {
				t.Fatalf("Pending = %d", n)
			}
			if h.closed && h.p.Pending() != 0 {
				t.Fatalf("closed pool holds %d items", h.p.Pending())
			}
		},
	}
}

// dispatchConfigFrom decodes a kind, a pool configuration and the shape of
// the workload from fuzz input.
func dispatchConfigFrom(r *fuzzutil.Reader) (Kind, Config, int, int32) {
	k := Kinds()[r.Choice(len(Kinds()))]
	cfg := Config{
		Workers:  r.Int(1, 4),
		Lanes:    r.Int(1, 8),
		Capacity: r.Int(1, 8),
		Batch:    r.Int(1, 4),
		Steal:    r.Int(1, 4),
		Queue:    fuzzQueues[r.Choice(len(fuzzQueues))],
		Wait:     fuzzWaits[r.Choice(len(fuzzWaits))],
	}
	keys := r.Int(1, 4)
	partitions := int32(r.Int(0, 3))
	return k, cfg, keys, partitions
}

// dispatchSeeds authors readable seed scripts for every kind.
func dispatchSeeds(tb testing.TB, sc *fuzzutil.Script[*dispatchHarness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	submit := func(key int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "submit"); w.Int(1, 4, key) }
	}
	try := func(key int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "try"); w.Int(1, 4, key) }
	}
	burst := func(key, n int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "burst"); w.Int(1, 4, key); w.Int(2, 12, n) }
	}
	plain := func(name string) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, name) }
	}
	scripts := [][]step{
		// One key, in order, drained.
		{submit(1), submit(1), submit(1), plain("drain")},
		// Several keys interleaved: they may be handled in parallel, each in
		// its own order.
		{submit(1), submit(2), submit(3), submit(1), submit(2), plain("drain")},
		// A burst larger than the lane makes the producer wait for room.
		{burst(1, 12), plain("drain"), burst(2, 6), plain("drain")},
		// Keyless items go anywhere; a full lane refuses TrySubmit.
		{plain("keyless"), plain("keyless"), try(1), try(1), plain("drain")},
		// Closing with work outstanding must still handle all of it, and
		// refuse everything afterwards.
		{burst(1, 8), plain("close"), submit(1), plain("drain")},
	}
	kinds := Kinds()
	seeds := make([][]byte, 0, len(scripts)*len(kinds))
	for ki := range kinds {
		for i, steps := range scripts {
			w := fuzzutil.NewWriter()
			w.Choice(len(kinds), ki)
			w.Int(1, 4, 1+i%4) // workers
			w.Int(1, 8, 1+i%8) // lanes
			w.Int(1, 8, 1+i%4) // capacity
			w.Int(1, 4, 1+i%4) // batch
			w.Int(1, 4, 1+i%4) // steal
			w.Choice(len(fuzzQueues), i%len(fuzzQueues))
			w.Choice(len(fuzzWaits), i%len(fuzzWaits))
			w.Int(1, 4, 4)   // keys
			w.Int(0, 3, i%4) // partitions
			for _, st := range steps {
				st(w)
			}
			seeds = append(seeds, w.Data())
		}
	}
	return seeds
}

// runDispatchScript runs one script against a fresh pool and leaves the pool
// closed and fully compared.
func runDispatchScript(t testing.TB, data []byte) int {
	sc := dispatchScript()
	r := fuzzutil.NewReader(data)
	k, cfg, keys, partitions := dispatchConfigFrom(r)
	h := newDispatchHarness(t, k, cfg, keys, partitions)
	defer func() {
		if !h.closed {
			_ = h.p.Close()
		}
	}()
	n := sc.Run(t, data[len(data)-r.Remaining():], h)
	if t.Failed() {
		return n
	}
	if !h.closed {
		h.close(t)
	}
	return n
}

// FuzzDispatcher is the differential fuzz target (the test suite): a fuzz-chosen kind, pool configuration,
// lane queue, wait strategy and operation script run against a pool and the
// model of what was submitted. Checked after every drain and after the
// close: every accepted item is handled exactly once, per-key streams come
// back in submission order for the kinds that promise it, two items of one
// key are never in a handler at once, a closed pool accepts nothing and
// leaves nothing behind, and a refused TrySubmit never handled the item.
func FuzzDispatcher(f *testing.F) {
	sc := dispatchScript()
	for _, s := range dispatchSeeds(f, sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		runDispatchScript(t, data)
	})
}

// FuzzDispatcherConcurrent races several producers, each owning its own
// keys, against one pool while a sweeper drains it. Checked: nothing is lost
// or duplicated however the producers interleave, each producer's per-key
// stream keeps its order on the kinds that promise it, and the pool closes
// with everything handled while producers are still submitting.
func FuzzDispatcherConcurrent(f *testing.F) {
	for _, seed := range [][4]int{{0, 2, 40, 3}, {1, 3, 60, 5}, {4, 4, 25, 2}} {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), seed[0]%len(Kinds()))
		w.Int(1, 4, seed[1])
		w.Int(1, 8, seed[1]*2)
		w.Int(1, 8, seed[3])
		w.Int(1, 4, 2)
		w.Int(1, 4, 2)
		w.Choice(len(fuzzQueues), seed[0]%len(fuzzQueues))
		w.Choice(len(fuzzWaits), seed[1]%len(fuzzWaits))
		w.Int(1, 4, 2)
		w.Int(0, 3, seed[3]%4)
		w.Int(1, 200, seed[2])
		for i := range 64 {
			w.Byte(byte(i * 37))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k, cfg, _, partitions := dispatchConfigFrom(r)
		per := r.Int(1, 200)
		s := newSink()
		s.yield = runtime.Gosched
		p := mustNew(t, k, cfg, s.handle)
		defer func() { _ = p.Close() }()

		workers := cfg.Workers + 1
		var accepted sync.Map // key -> the sequence numbers the pool took
		ctx := context.Background()
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], workers, func(id int, ar *fuzzutil.Reader) {
			key := uint64(id + 1)
			seqs := make([]int, 0, per)
			for i := range per {
				route := Keyed(key)
				if partitions > 0 {
					route = route.InPartition(int32(mix(key) % uint64(partitions)))
				}
				err := p.Submit(ctx, route, item{key: key, seq: i})
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("Submit: %v", err)
					}
					break
				}
				seqs = append(seqs, i)
				fuzzutil.MaybeYield(ar)
				if ar.Bool() {
					if err := p.Drain(ctx); err != nil {
						t.Errorf("Drain: %v", err)
						break
					}
				}
			}
			accepted.Store(key, seqs)
		})
		if t.Failed() {
			return
		}
		if err := p.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		want := map[uint64][]int{}
		accepted.Range(func(key, v any) bool {
			seqs := v.([]int)
			if len(seqs) > 0 {
				want[key.(uint64)] = seqs
			}
			return true
		})
		s.compare(t, want, k.PreservesKeyOrder())
	})
}

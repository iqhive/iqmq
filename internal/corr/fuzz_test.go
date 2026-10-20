package corr

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// oracle is the reference correlation store: one map, no concurrency, the
// contract written out directly.
type oracle struct {
	entries map[ID]Pending[*call]
	cap     int
}

func newOracle(capacity int) *oracle {
	return &oracle{entries: make(map[ID]Pending[*call]), cap: capacity}
}

func (o *oracle) put(id ID, deadline int64, v *call) bool {
	if id == 0 || len(o.entries) >= o.cap {
		return false
	}
	if _, ok := o.entries[id]; ok {
		return false
	}
	o.entries[id] = Pending[*call]{ID: id, Deadline: deadline, Value: v}
	return true
}

func (o *oracle) take(id ID) (*call, bool) {
	e, ok := o.entries[id]
	if !ok {
		return nil, false
	}
	delete(o.entries, id)
	return e.Value, true
}

func (o *oracle) expire(now int64) []Pending[*call] {
	var out []Pending[*call]
	for id, e := range o.entries {
		if e.Deadline <= now {
			out = append(out, e)
			delete(o.entries, id)
		}
	}
	return out
}

// corrHarness pairs one store with the oracle; both see identical operations.
type corrHarness struct {
	s    Store[*call]
	o    *oracle
	live []ID
	next ID
	now  int64
	dst  []Pending[*call]
}

func newCorrHarness(tb testing.TB, k Kind, cfg Config) *corrHarness {
	cfg = cfg.Normalize()
	return &corrHarness{s: mustNew[*call](tb, k, cfg), o: newOracle(cfg.Capacity), next: 1}
}

func (h *corrHarness) put(t testing.TB, id ID, deadline int64) {
	t.Helper()
	c := newCall(id)
	got := h.s.Put(id, deadline, c)
	want := h.o.put(id, deadline, c)
	if got != want {
		t.Fatalf("Put(%d, %d) = %v, oracle %v (len %d/%d)", id, deadline, got, want, h.s.Len(), len(h.o.entries))
	}
	if got {
		h.live = append(h.live, id)
	}
	h.check(t, "put")
}

func (h *corrHarness) take(t testing.TB, id ID) {
	t.Helper()
	got, ok := h.s.Take(id)
	want, wantOK := h.o.take(id)
	if ok != wantOK {
		t.Fatalf("Take(%d) = %v, oracle %v", id, ok, wantOK)
	}
	if ok && got != want {
		t.Fatalf("Take(%d) returned call %v, oracle %v", id, got.id, want.id)
	}
	if ok {
		h.forget(id)
	}
	h.check(t, "take")
}

func (h *corrHarness) expire(t testing.TB, now int64) {
	t.Helper()
	h.dst = h.s.Expire(now, h.dst[:0])
	want := h.o.expire(now)
	got := slices.Clone(h.dst)
	byID := func(a, b Pending[*call]) int { return int(a.ID) - int(b.ID) }
	slices.SortFunc(got, byID)
	slices.SortFunc(want, byID)
	if len(got) != len(want) {
		t.Fatalf("Expire(%d) returned %d entries, oracle %d", now, len(got), len(want))
	}
	for i := range got {
		if got[i].ID != want[i].ID || got[i].Deadline != want[i].Deadline || got[i].Value != want[i].Value {
			t.Fatalf("Expire(%d)[%d] = %+v, oracle %+v", now, i, got[i], want[i])
		}
		h.forget(got[i].ID)
	}
	h.check(t, "expire")
}

func (h *corrHarness) forget(id ID) {
	if i := slices.Index(h.live, id); i >= 0 {
		h.live = slices.Delete(h.live, i, i+1)
	}
}

func (h *corrHarness) check(t testing.TB, op string) {
	t.Helper()
	if got, want := h.s.Len(), len(h.o.entries); got != want {
		t.Fatalf("%s: Len = %d, oracle %d", op, got, want)
	}
	if len(h.live) != len(h.o.entries) {
		t.Fatalf("%s: harness holds %d live ids, oracle %d", op, len(h.live), len(h.o.entries))
	}
}

// corrScript is the op-script differential between a Store and the oracle.
func corrScript() *fuzzutil.Script[*corrHarness] {
	pickLive := func(r *fuzzutil.Reader, h *corrHarness) ID {
		if len(h.live) == 0 {
			return ID(r.Int(1, 1<<16))
		}
		return h.live[r.Choice(len(h.live))]
	}
	return &fuzzutil.Script[*corrHarness]{
		Steps: []fuzzutil.Step[*corrHarness]{
			{Name: "put", Weight: 5, Run: func(t testing.TB, r *fuzzutil.Reader, h *corrHarness) {
				id := h.next
				h.next++
				h.put(t, id, h.now+int64(r.Int(0, 100)))
			}},
			{Name: "putSparse", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *corrHarness) {
				// Ids from a different range exercise the probe chains and the
				// shard distribution.
				h.put(t, ID(r.Int(0, 1<<20)), h.now+int64(r.Int(0, 100)))
			}},
			{Name: "putDuplicate", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, h *corrHarness) {
				h.put(t, pickLive(r, h), h.now+int64(r.Int(0, 100)))
			}},
			{Name: "take", Weight: 5, Run: func(t testing.TB, r *fuzzutil.Reader, h *corrHarness) {
				h.take(t, pickLive(r, h))
			}},
			{Name: "takeUnknown", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *corrHarness) {
				h.take(t, ID(r.Int(0, 1<<20)))
			}},
			{Name: "expire", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, h *corrHarness) {
				h.now += int64(r.Int(0, 60))
				h.expire(t, h.now)
			}},
			{Name: "drain", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, h *corrHarness) {
				h.expire(t, math.MaxInt64)
			}},
		},
		MaxSteps: 2048,
		Check: func(t testing.TB, h *corrHarness) {
			// Every live id is still takeable and nothing else is: checked
			// cheaply through Len by the harness, and exactly here.
			for _, id := range h.live {
				if _, ok := h.o.entries[id]; !ok {
					t.Fatalf("oracle lost id %d", id)
				}
			}
		},
	}
}

// corrConfigFrom decodes a store configuration from fuzz input.
func corrConfigFrom(r *fuzzutil.Reader) Config {
	capacity := r.Int(1, 64)
	shards := r.Int(1, 16)
	return Config{Capacity: capacity, Shards: shards}
}

// corrSeeds authors readable seed scripts for every kind.
func corrSeeds(tb testing.TB, sc *fuzzutil.Script[*corrHarness]) [][]byte {
	type step func(w *fuzzutil.Writer)
	put := func(d int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "put"); w.Int(0, 100, d) }
	}
	take := func(i int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "take"); w.Choice(4, i) }
	}
	expire := func(d int) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, "expire"); w.Int(0, 60, d) }
	}
	plain := func(name string) step {
		return func(w *fuzzutil.Writer) { sc.WriteStep(tb, w, name) }
	}
	scripts := [][]step{
		// Request, reply, request, reply.
		{put(10), take(0), put(10), take(0)},
		// Several outstanding requests, replies out of order.
		{put(10), put(20), put(30), take(1), take(1), take(0)},
		// Timeouts free their entries; a late reply then finds nothing.
		{put(5), put(50), expire(10), take(0), plain("takeUnknown")},
		// Duplicate ids and a full drain.
		{put(10), plain("putDuplicate"), plain("drain"), plain("putDuplicate")},
	}
	seeds := make([][]byte, 0, len(scripts)*len(Kinds()))
	for ki := range Kinds() {
		for _, steps := range scripts {
			w := fuzzutil.NewWriter()
			w.Choice(len(Kinds()), ki)
			w.Int(1, 64, 8)
			w.Int(1, 16, 4)
			for _, st := range steps {
				st(w)
			}
			seeds = append(seeds, w.Data())
		}
	}
	return seeds
}

// FuzzReqReplyCorrelation is the differential fuzz target (the test suite): a fuzz-chosen kind, capacity, shard count and
// operation script run against a Store and a map oracle. Every result is
// compared, so a reply is matched at most once, a duplicate id is refused, a
// timed-out entry is freed exactly once and the capacity bound holds.
func FuzzReqReplyCorrelation(f *testing.F) {
	sc := corrScript()
	for _, s := range corrSeeds(f, sc) {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		cfg := corrConfigFrom(r)
		h := newCorrHarness(t, k, cfg)
		sc.Run(t, data[len(data)-r.Remaining():], h)
		// A drain must return everything that is left, once.
		h.expire(t, math.MaxInt64)
		if h.s.Len() != 0 || len(h.live) != 0 {
			t.Fatalf("after draining: Len %d, %d live ids", h.s.Len(), len(h.live))
		}
	})
}

// FuzzReqReplyConcurrent races fuzz-chosen numbers of requesters, repliers
// and one expiry sweeper against one store. Checked: every entry comes back
// exactly once, through Take or through Expire but never both; a value always
// arrives at the id it was stored under; the length stays within [0,
// capacity] throughout (Put reserves its slot before the entry is visible)
// and ends at zero.
func FuzzReqReplyConcurrent(f *testing.F) {
	for _, seed := range [][4]int{{1, 2, 50, 16}, {3, 4, 100, 64}, {2, 8, 40, 8}} {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), seed[0]%len(Kinds()))
		w.Int(1, 8, seed[1])
		w.Int(1, 200, seed[2])
		w.Int(1, 256, seed[3])
		for i := range 64 {
			w.Byte(byte(i * 31))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		workers := r.Int(1, 8)
		per := r.Int(1, 200)
		capacity := r.Int(1, 256)
		s := mustNew[*call](t, k, Config{Capacity: capacity, Shards: 4})

		var mu sync.Mutex
		seen := make(map[ID]int)
		var bad atomic.Pointer[string]
		fail := func(msg string) { bad.CompareAndSwap(nil, &msg) }
		record := func(id ID, c *call) {
			if c == nil || c.id != id {
				fail("value came back under the wrong id")
				return
			}
			mu.Lock()
			seen[id]++
			mu.Unlock()
		}
		var stored atomic.Int64
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], workers+1, func(id int, ar *fuzzutil.Reader) {
			if id == workers { // the sweeper
				var dst []Pending[*call]
				for range per {
					dst = s.Expire(int64(ar.Int(0, 1<<20)), dst[:0])
					for _, p := range dst {
						record(p.ID, p.Value)
					}
					fuzzutil.MaybeYield(ar)
				}
				return
			}
			base := ID(id)*ID(per)*4 + 1
			for i := range per {
				cid := base + ID(i)
				if s.Put(cid, int64(ar.Int(0, 1<<20)), newCall(cid)) {
					stored.Add(1)
					fuzzutil.MaybeYield(ar)
					if c, ok := s.Take(cid); ok {
						record(cid, c)
					}
				}
				if n := s.Len(); n < 0 || n > capacity {
					fail("length outside [0, capacity]")
				}
			}
		})
		// Whatever the sweeper and the takers left behind is drained here.
		var dst []Pending[*call]
		for _, p := range s.Expire(math.MaxInt64, dst) {
			record(p.ID, p.Value)
		}
		if msg := bad.Load(); msg != nil {
			t.Fatalf("%v workers=%d per=%d cap=%d: %s", k, workers, per, capacity, *msg)
		}
		if s.Len() != 0 {
			t.Fatalf("%v: Len = %d after draining", k, s.Len())
		}
		total := 0
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("%v: entry %d came back %d times", k, id, n)
			}
			total++
		}
		if int64(total) != stored.Load() {
			t.Fatalf("%v: %d entries stored but %d came back", k, stored.Load(), total)
		}
	})
}

package registry

import (
	"math/rand/v2"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// TestModel checks every kind against a plain-map oracle, sequentially and
// under concurrent creation, deletion and lookup.
func TestModel(t *testing.T) {
	for _, k := range Kinds() {
		t.Run(k.String(), func(t *testing.T) {
			t.Run("sequential", func(t *testing.T) { modelSequential(t, k) })
			t.Run("range", func(t *testing.T) { modelRange(t, k) })
			t.Run("range-reentrant", func(t *testing.T) { modelRangeReentrant(t, k) })
			t.Run("loadorstore-race", func(t *testing.T) { modelLoadOrStoreRace(t, k) })
			t.Run("concurrent", func(t *testing.T) { modelConcurrent(t, k) })
			t.Run("len-bound", func(t *testing.T) { modelLenBound(t, k) })
			t.Run("len-exact", func(t *testing.T) { modelLenExact(t, k) })
			t.Run("shards", func(t *testing.T) { modelShardCounts(t, k) })
		})
	}
}

// testKeys returns n distinct interned keys.
func testKeys(n int) []Key {
	keys := make([]Key, n)
	for i := range keys {
		keys[i] = Intern("topic/" + itoa(i))
	}
	return keys
}

// oracle is the reference registry.
type oracle[V comparable] map[Key]V

// entries returns the registry's Range output sorted by key name.
func entries[V any](r Registry[V]) []entry[V] {
	var out []entry[V]
	r.Range(func(k Key, v V) bool {
		out = append(out, entry[V]{k, v})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].k.Value() < out[j].k.Value() })
	return out
}

// assertSame fails unless Range, Len and Load all agree with the oracle.
func assertSame[V comparable](t testing.TB, r Registry[V], want oracle[V]) {
	t.Helper()
	if r.Len() != len(want) {
		t.Fatalf("Len = %d, oracle %d", r.Len(), len(want))
	}
	got := entries(r)
	if len(got) != len(want) {
		t.Fatalf("Range visited %d entries, oracle has %d", len(got), len(want))
	}
	seen := map[Key]bool{}
	for _, e := range got {
		if seen[e.k] {
			t.Fatalf("Range visited %q twice", e.k.Value())
		}
		seen[e.k] = true
		if wv, ok := want[e.k]; !ok || wv != e.v {
			t.Fatalf("Range %q = %v, oracle %v,%v", e.k.Value(), e.v, wv, ok)
		}
	}
	for k, wv := range want {
		if v, ok := r.Load(k); !ok || v != wv {
			t.Fatalf("Load %q = %v,%v, oracle %v", k.Value(), v, ok, wv)
		}
	}
}

func modelSequential(t *testing.T, k Kind) {
	r := mustNew[int](t, k)
	want := oracle[int]{}
	keys := testKeys(12)
	rng := rand.New(rand.NewPCG(uint64(k), 7))
	for step := range 5000 {
		key := keys[rng.IntN(len(keys))]
		switch rng.IntN(6) {
		case 0, 1:
			v, ok := r.Load(key)
			wv, wok := want[key]
			if ok != wok || v != wv {
				t.Fatalf("step %d: Load = %d,%v, oracle %d,%v", step, v, ok, wv, wok)
			}
		case 2, 3:
			actual, loaded := r.LoadOrStore(key, step)
			wv, wok := want[key]
			if !wok {
				want[key] = step
				wv = step
			}
			if loaded != wok || actual != wv {
				t.Fatalf("step %d: LoadOrStore = %d,%v, oracle %d,%v", step, actual, loaded, wv, wok)
			}
		case 4:
			old, ok := r.Delete(key)
			wv, wok := want[key]
			delete(want, key)
			if ok != wok || (ok && old != wv) {
				t.Fatalf("step %d: Delete = %d,%v, oracle %d,%v", step, old, ok, wv, wok)
			}
		case 5:
			if r.Len() != len(want) {
				t.Fatalf("step %d: Len = %d, oracle %d", step, r.Len(), len(want))
			}
		}
		if step%97 == 0 {
			assertSame(t, r, want)
		}
	}
	assertSame(t, r, want)
	for _, key := range keys {
		r.Delete(key)
	}
	assertSame(t, r, oracle[int]{})
}

func modelRange(t *testing.T, k Kind) {
	r := mustNew[int](t, k)
	calls := 0
	r.Range(func(Key, int) bool { calls++; return true })
	if calls != 0 {
		t.Fatalf("Range over empty registry called fn %d times", calls)
	}
	keys := testKeys(8)
	for i, key := range keys {
		r.LoadOrStore(key, i)
	}
	// Early stop: exactly three visits.
	calls = 0
	r.Range(func(Key, int) bool { calls++; return calls < 3 })
	if calls != 3 {
		t.Fatalf("Range stopped after %d calls, want 3", calls)
	}
	// Keys and values line up.
	r.Range(func(key Key, v int) bool {
		if keys[v] != key {
			t.Fatalf("Range pairs %q with %d", key.Value(), v)
		}
		return true
	})
	if got := entries(r); len(got) != len(keys) {
		t.Fatalf("Range visited %d, want %d", len(got), len(keys))
	}
}

// modelRangeReentrant deletes and inserts from inside the callback: no
// deadlock, every pre-existing entry is visited exactly once, and entries
// inserted during Range are visited at most once.
func modelRangeReentrant(t *testing.T, k Kind) {
	r := mustNew[int](t, k)
	keys := testKeys(16)
	for i, key := range keys[:8] {
		r.LoadOrStore(key, i)
	}
	visits := map[Key]int{}
	r.Range(func(key Key, v int) bool {
		visits[key]++
		r.Delete(key)                       // remove the visited entry
		r.LoadOrStore(keys[8+v%8], 100+v)   // insert a new one
		r.LoadOrStore(keys[(v+1)%8], 200+v) // maybe re-insert a not-yet-visited one
		return true
	})
	for i, key := range keys[:8] {
		if visits[key] < 1 || visits[key] > 2 {
			t.Fatalf("pre-existing key %d visited %d times", i, visits[key])
		}
	}
	for i, key := range keys[8:] {
		if visits[key] > 1 {
			t.Fatalf("inserted key %d visited %d times", 8+i, visits[key])
		}
	}
	if k.RangeSnapshot() {
		for i, key := range keys {
			if want := i < 8; (visits[key] == 1) != want || (visits[key] > 1) {
				t.Fatalf("snapshot Range: key %d visited %d times", i, visits[key])
			}
		}
	}
	// The registry is consistent afterwards.
	want := oracle[int]{}
	r.Range(func(key Key, v int) bool { want[key] = v; return true })
	assertSame(t, r, want)
}

// modelLoadOrStoreRace has G goroutines race to create each of K keys with
// distinct values: exactly one stores per key and everyone observes it.
func modelLoadOrStoreRace(t *testing.T, k Kind) {
	const K, G = 32, 8
	r := mustNew[int](t, k)
	keys := testKeys(K)
	results := make([][G]struct {
		v      int
		loaded bool
	}, K)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range G {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i, key := range keys {
				v, loaded := r.LoadOrStore(key, g*1000+i)
				results[i][g].v, results[i][g].loaded = v, loaded
			}
		}()
	}
	close(start)
	wg.Wait()
	for i, key := range keys {
		stored := 0
		for g := range G {
			res := results[i][g]
			if !res.loaded {
				stored++
				if res.v != g*1000+i {
					t.Fatalf("key %d: storer %d got %d back", i, g, res.v)
				}
			}
			if res.v%1000 != i {
				t.Fatalf("key %d: goroutine %d got value %d of another key", i, g, res.v)
			}
			if res.v != results[i][0].v {
				t.Fatalf("key %d: goroutines disagree on the value: %d vs %d", i, res.v, results[i][0].v)
			}
		}
		if stored != 1 {
			t.Fatalf("key %d: %d goroutines stored, want 1", i, stored)
		}
		if v, ok := r.Load(key); !ok || v != results[i][0].v {
			t.Fatalf("key %d: Load = %d,%v after race, want %d", i, v, ok, results[i][0].v)
		}
	}
	if r.Len() != K {
		t.Fatalf("Len = %d, want %d", r.Len(), K)
	}
}

// modelConcurrent mixes Load, LoadOrStore, Delete, Range and Len from
// several goroutines over a small key space. Values encode the key index
// so a lookup that returns another key's value is detected, and after all
// goroutines finish Range, Len and Load must agree.
func modelConcurrent(t *testing.T, k Kind) {
	const K, G, ops = 16, 8, 20000
	r := mustNew[uint64](t, k)
	keys := testKeys(K)
	var failed atomic.Bool
	var msg atomic.Pointer[string]
	fail := func(s string) {
		if failed.CompareAndSwap(false, true) {
			msg.Store(&s)
		}
	}
	var wg sync.WaitGroup
	for g := range G {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), uint64(k)))
			for i := 0; i < ops && !failed.Load(); i++ {
				ki := rng.IntN(K)
				key := keys[ki]
				val := uint64(ki)<<32 | uint64(g)<<16 | uint64(i&0xffff)
				switch rng.IntN(10) {
				case 0, 1, 2, 3:
					if v, ok := r.Load(key); ok && int(v>>32) != ki {
						fail("Load returned another key's value")
					}
				case 4, 5:
					if v, _ := r.LoadOrStore(key, val); int(v>>32) != ki {
						fail("LoadOrStore returned another key's value")
					}
				case 6, 7:
					if v, ok := r.Delete(key); ok && int(v>>32) != ki {
						fail("Delete returned another key's value")
					}
				case 8:
					seen := 0
					r.Range(func(key Key, v uint64) bool {
						seen++
						if keys[v>>32] != key {
							fail("Range paired a key with another key's value")
						}
						return seen < K
					})
					if seen > K {
						fail("Range visited more entries than keys exist")
					}
				case 9:
					if n := r.Len(); n < 0 || n > K {
						fail("Len out of range")
					}
				}
				if i%512 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
	if failed.Load() {
		t.Fatal(*msg.Load())
	}
	want := oracle[uint64]{}
	r.Range(func(key Key, v uint64) bool { want[key] = v; return true })
	assertSame(t, r, want)
	for _, key := range keys {
		if v, ok := r.Load(key); ok {
			if actual, loaded := r.LoadOrStore(key, 0); !loaded || actual != v {
				t.Fatalf("LoadOrStore on present key %q = %d,%v, Load says %d", key.Value(), actual, loaded, v)
			}
		}
	}
}

// lenWatch records the smallest and the largest value Len returned while
// goroutines mutated the registry.
type lenWatch struct {
	lo, hi atomic.Int64
	done   atomic.Bool
	wg     sync.WaitGroup
}

// watch starts n goroutines calling r.Len() until stop is called. lo starts
// at upper — the number of keys in play — so that the minimum it reports is
// an observation and not the zero value.
func (w *lenWatch) watch(r interface{ Len() int }, n, upper int) {
	w.lo.Store(int64(upper))
	for range n {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for !w.done.Load() {
				v := int64(r.Len())
				for m := w.hi.Load(); v > m && !w.hi.CompareAndSwap(m, v); m = w.hi.Load() {
				}
				for m := w.lo.Load(); v < m && !w.lo.CompareAndSwap(m, v); m = w.lo.Load() {
				}
			}
		}()
	}
}

func (w *lenWatch) stop() (lo, hi int) {
	w.done.Store(true)
	w.wg.Wait()
	return int(w.lo.Load()), int(w.hi.Load())
}

// modelLenBound holds Len to the contract every kind promises while the
// registry churns (Registry.Len): a key present for the whole call is
// counted exactly once, no absent key is counted, and no key is counted
// twice. Residents stored before the mutators start and never deleted give
// the lower bound; the key space gives the upper one.
//
// A counter maintained beside the map rather than derived from it breaks
// the upper bound: the mutation and the counter update are two steps, so a
// store, a delete and a store of one key can apply both increments before
// the decrement and report entries that never existed. A Len that skips
// live entries breaks the lower bound.
func modelLenBound(t *testing.T, k Kind) {
	const residents, churnKeys, G, ops = 4, 2, 8, 10000
	r := mustNew[int](t, k)
	keys := testKeys(residents + churnKeys)
	for i, key := range keys[:residents] {
		r.LoadOrStore(key, i)
	}
	churn := keys[residents:]
	var w lenWatch
	w.watch(r, 2, residents+churnKeys)
	var mutators sync.WaitGroup
	for g := range G {
		mutators.Add(1)
		go func() {
			defer mutators.Done()
			for i := range ops {
				key := churn[(i+g)%churnKeys]
				r.LoadOrStore(key, i)
				r.Delete(key)
			}
		}()
	}
	mutators.Wait()
	lo, hi := w.stop()
	if hi > residents+churnKeys {
		t.Errorf("Len reported %d entries, but only %d keys exist", hi, residents+churnKeys)
	}
	if lo < residents {
		t.Errorf("Len reported %d entries, but %d residents are present throughout", lo, residents)
	}
}

// modelLenExact pins what Kind.ExactLen declares. Every goroutine owns a
// pair of keys and swaps between them — delete the one it holds, store the
// other — so at most one key per goroutine is present at any instant. A
// Len that reports the count at one instant of the call therefore never
// exceeds the number of goroutines, while one that reads the registry in
// pieces (SyncMap walks the map, Sharded sums its shards) may count both
// keys of a pair and reach twice that, which is all its kind promises.
func modelLenExact(t *testing.T, k Kind) {
	const G, ops = 8, 10000
	r := mustNew[int](t, k)
	keys := testKeys(2 * G)
	for g := range G {
		r.LoadOrStore(keys[2*g], g)
	}
	limit := 2 * G
	if k.ExactLen() {
		limit = G
	}
	var w lenWatch
	w.watch(r, 2, 2*G)
	var mutators sync.WaitGroup
	for g := range G {
		mutators.Add(1)
		go func() {
			defer mutators.Done()
			held, other := keys[2*g], keys[2*g+1]
			for i := range ops {
				r.Delete(held)
				r.LoadOrStore(other, i)
				held, other = other, held
			}
		}()
	}
	mutators.Wait()
	_, hi := w.stop()
	if hi > limit {
		t.Errorf("Len reported %d entries with ExactLen=%v; at most %d could be counted",
			hi, k.ExactLen(), limit)
	}
}

// modelShardCounts exercises Sharded with degenerate and odd shard counts;
// other kinds have nothing to configure.
func modelShardCounts(t *testing.T, k Kind) {
	if k != Sharded {
		t.Skip("only Sharded has a shard count")
	}
	for _, n := range []int{0, 1, 2, 3, 5, 64} {
		r := newSharded[int](n)
		want := oracle[int]{}
		for i, key := range testKeys(40) {
			r.LoadOrStore(key, i)
			want[key] = i
		}
		assertSame(t, r, want)
		for i, key := range testKeys(40) {
			if i%3 == 0 {
				r.Delete(key)
				delete(want, key)
			}
		}
		assertSame(t, r, want)
	}
}

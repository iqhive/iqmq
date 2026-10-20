package registry

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// fuzzKeyCount is the size of the key pool the fuzz scripts draw from.
const fuzzKeyCount = 8

// FuzzRegistry drives one kind with a random operation script and compares
// every result with the plain-map oracle; Range, Len and Load are cross
// checked after every step.
func FuzzRegistry(f *testing.F) {
	type st struct {
		r    Registry[int]
		want oracle[int]
		keys []Key
		n    int
	}
	script := fuzzutil.Script[*st]{
		Steps: []fuzzutil.Step[*st]{
			{Name: "load", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				key := s.keys[r.Choice(len(s.keys))]
				v, ok := s.r.Load(key)
				wv, wok := s.want[key]
				if ok != wok || v != wv {
					t.Fatalf("Load %q = %d,%v, oracle %d,%v", key.Value(), v, ok, wv, wok)
				}
			}},
			{Name: "loadOrStore", Weight: 3, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				key := s.keys[r.Choice(len(s.keys))]
				s.n++
				actual, loaded := s.r.LoadOrStore(key, s.n)
				wv, wok := s.want[key]
				if !wok {
					s.want[key], wv = s.n, s.n
				}
				if loaded != wok || actual != wv {
					t.Fatalf("LoadOrStore %q = %d,%v, oracle %d,%v", key.Value(), actual, loaded, wv, wok)
				}
			}},
			{Name: "delete", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				key := s.keys[r.Choice(len(s.keys))]
				old, ok := s.r.Delete(key)
				wv, wok := s.want[key]
				delete(s.want, key)
				if ok != wok || (ok && old != wv) {
					t.Fatalf("Delete %q = %d,%v, oracle %d,%v", key.Value(), old, ok, wv, wok)
				}
			}},
			{Name: "rangeStop", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				limit := r.Int(0, fuzzKeyCount+1)
				calls := 0
				s.r.Range(func(key Key, v int) bool {
					calls++
					if wv, ok := s.want[key]; !ok || wv != v {
						t.Fatalf("Range %q = %d, oracle %d,%v", key.Value(), v, wv, ok)
					}
					return calls < limit
				})
				// fn is called until it returns false: min(limit, entries)
				// times, or once when limit is zero and an entry exists.
				if want := min(max(limit, 1), len(s.want)); calls != want {
					t.Fatalf("Range with limit %d over %d entries called fn %d times, want %d", limit, len(s.want), calls, want)
				}
			}},
			{Name: "rangeMutate", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *st) {
				// Delete or insert from inside the callback; the oracle follows.
				victim := s.keys[r.Choice(len(s.keys))]
				extra := s.keys[r.Choice(len(s.keys))]
				s.n++
				visited := map[Key]int{}
				s.r.Range(func(key Key, _ int) bool {
					visited[key]++
					s.r.Delete(victim)
					delete(s.want, victim)
					if _, ok := s.want[extra]; !ok {
						s.r.LoadOrStore(extra, s.n)
						s.want[extra] = s.n
					}
					return true
				})
				for key, n := range visited {
					if n > 1 {
						t.Fatalf("Range visited %q %d times", key.Value(), n)
					}
				}
			}},
		},
		Check: func(t testing.TB, s *st) {
			assertSame(t, s.r, s.want)
		},
	}
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		for range 3 {
			script.WriteStep(f, w, "loadOrStore")
			w.Choice(fuzzKeyCount, ki)
		}
		script.WriteStep(f, w, "load")
		w.Choice(fuzzKeyCount, ki)
		script.WriteStep(f, w, "rangeStop")
		w.Int(0, fuzzKeyCount+1, 2)
		script.WriteStep(f, w, "delete")
		w.Choice(fuzzKeyCount, ki)
		script.WriteStep(f, w, "rangeMutate")
		w.Choice(fuzzKeyCount, 1)
		w.Choice(fuzzKeyCount, 5)
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		var reg Registry[int]
		if k == Sharded {
			reg = newSharded[int](r.Int(0, 8))
		} else {
			reg = mustNew[int](t, k)
		}
		s := &st{r: reg, want: oracle[int]{}, keys: testKeys(fuzzKeyCount)}
		script.Run(t, data[len(data)-r.Remaining():], s)
	})
}

// FuzzRegistryConcurrent runs fuzz-chosen numbers of goroutines with
// fuzz-driven operation mixes over a shared key pool. Values encode the
// key index, so any lookup that surfaces another key's value fails, and
// after the actors finish Range, Len and Load must agree.
func FuzzRegistryConcurrent(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(1, 8, 4)
		w.Int(1, 200, 60)
		for range 64 {
			w.Byte(byte(ki*37 + 11))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		actors := r.Int(1, 8)
		per := r.Int(1, 200)
		var reg Registry[uint64]
		if k == Sharded {
			reg = newSharded[uint64](r.Int(0, 8))
		} else {
			reg = mustNew[uint64](t, k)
		}
		keys := testKeys(fuzzKeyCount)
		var failed atomic.Bool
		var msg atomic.Pointer[string]
		fail := func(s string) {
			if failed.CompareAndSwap(false, true) {
				msg.Store(&s)
			}
		}
		var wg sync.WaitGroup
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], actors, func(id int, ar *fuzzutil.Reader) {
			wg.Add(1)
			defer wg.Done()
			for i := 0; i < per && !failed.Load(); i++ {
				ki := ar.Choice(fuzzKeyCount)
				key := keys[ki]
				val := uint64(ki)<<32 | uint64(id)<<16 | uint64(i&0xffff)
				switch ar.Int(0, 5) {
				case 0, 1:
					if v, ok := reg.Load(key); ok && int(v>>32) != ki {
						fail("Load returned another key's value")
					}
				case 2:
					if v, _ := reg.LoadOrStore(key, val); int(v>>32) != ki {
						fail("LoadOrStore returned another key's value")
					}
				case 3:
					if v, ok := reg.Delete(key); ok && int(v>>32) != ki {
						fail("Delete returned another key's value")
					}
				case 4:
					seen := 0
					reg.Range(func(key Key, v uint64) bool {
						seen++
						if keys[v>>32] != key {
							fail("Range paired a key with another key's value")
						}
						return true
					})
					if seen > fuzzKeyCount {
						fail("Range visited more entries than keys exist")
					}
				case 5:
					if n := reg.Len(); n < 0 || n > fuzzKeyCount {
						fail("Len out of range")
					}
				}
				fuzzutil.MaybeYield(ar)
			}
		})
		wg.Wait()
		if failed.Load() {
			t.Fatalf("%v actors=%d per=%d: %s", k, actors, per, *msg.Load())
		}
		want := oracle[uint64]{}
		reg.Range(func(key Key, v uint64) bool { want[key] = v; return true })
		assertSame(t, reg, want)
	})
}

package stats

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzStats drives one kind with a random script of Add, AddAt and Load
// and compares every Load with an int64 oracle.
func FuzzStats(f *testing.F) {
	type st struct {
		c    Counter
		want int64
	}
	script := fuzzutil.Script[*st]{
		Steps: []fuzzutil.Step[*st]{
			{Name: "add", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *st) {
				d := int64(r.Uint64())
				s.c.Add(d)
				s.want += d
			}},
			{Name: "addAt", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *st) {
				w := r.Int(-4, 70)
				d := int64(r.Int(-1000, 1000))
				s.c.AddAt(w, d)
				s.want += d
			}},
			{Name: "load", Weight: 2, Run: func(t testing.TB, _ *fuzzutil.Reader, s *st) {
				if got := s.c.Load(); got != s.want {
					t.Fatalf("Load = %d, oracle %d", got, s.want)
				}
			}},
		},
		Check: func(t testing.TB, s *st) {
			if got := s.c.Load(); got != s.want {
				t.Fatalf("Load = %d, oracle %d", got, s.want)
			}
		},
	}
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(0, 64, 4)
		script.WriteStep(f, w, "add")
		w.Uint64(5)
		script.WriteStep(f, w, "addAt")
		w.Int(-4, 70, 2)
		w.Int(-1000, 1000, -7)
		script.WriteStep(f, w, "load")
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		workers := r.Int(0, 64)
		s := &st{c: mustNew(t, k, workers)}
		script.Run(t, data[len(data)-r.Remaining():], s)
	})
}

// FuzzStatsConcurrent runs fuzz-chosen numbers of writers with fuzz-driven
// non-negative deltas and interleaved Loads. Every Load must lie between
// the total completed before it and the total started during it, and the
// final Load must equal the sum of every delta (conservation).
func FuzzStatsConcurrent(f *testing.F) {
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Int(1, 8, 4)
		w.Int(0, 64, 4)
		w.Int(1, 300, 50)
		for range 48 {
			w.Byte(byte(ki*29 + 3))
		}
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		actors := r.Int(1, 8)
		workers := r.Int(0, 64)
		per := r.Int(1, 300)
		c := mustNew(t, k, workers)
		var pre, post atomic.Int64
		var failed atomic.Bool
		var wg sync.WaitGroup
		fuzzutil.Actors(t, data[len(data)-r.Remaining():], actors, func(id int, ar *fuzzutil.Reader) {
			wg.Add(1)
			defer wg.Done()
			var last int64 = -1
			for i := 0; i < per && !failed.Load(); i++ {
				switch ar.Int(0, 3) {
				case 0:
					lo := post.Load()
					v := c.Load()
					hi := pre.Load()
					if v < lo || v > hi || v < last {
						failed.Store(true)
						return
					}
					last = v
				case 1:
					d := int64(ar.Int(0, 1000))
					pre.Add(d)
					c.Add(d)
					post.Add(d)
				default:
					d := int64(ar.Int(0, 1000))
					w := ar.Int(-2, 70)
					if ar.Bool() {
						w = id
					}
					pre.Add(d)
					c.AddAt(w, d)
					post.Add(d)
				}
				fuzzutil.MaybeYield(ar)
			}
		})
		wg.Wait()
		if failed.Load() {
			t.Fatalf("%v actors=%d workers=%d: Load outside bounds or not monotone", k, actors, workers)
		}
		if got, want := c.Load(), pre.Load(); got != want {
			t.Fatalf("%v actors=%d workers=%d: Load = %d, want %d", k, actors, workers, got, want)
		}
	})
}

package timer

import (
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/fuzzutil"
)

// fuzzOrigins are the monotonic origins the fuzzer chooses between. Two are
// negative, where every tick a wheel kind computes is negative, and the last
// one is crossed by the first few steps of a script.
var fuzzOrigins = [...]int64{1 << 40, -(1 << 40), -int64(90 * 24 * time.Hour), -int64(2 * time.Second)}

// FuzzTimer drives a fuzz-chosen kind from a fuzz-chosen origin with a
// random Add/Cancel/Fire/Next script and validates every step against the
// oracle.
func FuzzTimer(f *testing.F) {
	type st struct {
		c  *checker
		es []Entry[int]
	}
	script := fuzzutil.Script[*st]{
		Steps: []fuzzutil.Step[*st]{
			{Name: "add", Weight: 5, Run: func(_ testing.TB, r *fuzzutil.Reader, s *st) {
				e := &s.es[r.Choice(len(s.es))]
				if e.Scheduled() {
					return
				}
				d := r.Duration(10 * time.Second)
				if r.Int(0, 3) == 0 {
					d = -r.Duration(2 * time.Millisecond) // slightly in the past
				}
				if r.Int(0, 63) == 0 {
					d = r.Duration(400 * 24 * time.Hour)
				}
				s.c.add(e, s.c.now+int64(d))
			}},
			{Name: "cancel", Weight: 2, Run: func(_ testing.TB, r *fuzzutil.Reader, s *st) {
				s.c.cancel(&s.es[r.Choice(len(s.es))])
			}},
			{Name: "fire", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *st) {
				s.c.fire(s.c.now + int64(r.Duration(100*time.Millisecond)))
			}},
			{Name: "jump", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *st) {
				s.c.fire(s.c.now + int64(r.Duration(48*time.Hour)))
			}},
			{Name: "progress", Weight: 1, Run: func(_ testing.TB, _ *fuzzutil.Reader, s *st) {
				s.c.progress()
			}},
		},
		// Every step already validates the fired set against the oracle;
		// after each step Len and Next must agree with it too.
		Check: func(_ testing.TB, s *st) {
			s.c.checkLen()
			s.c.checkNext()
		},
	}
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		w.Choice(len(fuzzOrigins), ki%len(fuzzOrigins))
		for range 4 {
			script.WriteStep(w, "add")
			w.Choice(16, ki)
			w.Duration(10*time.Second, 5*time.Millisecond)
			w.Int(0, 3, 1)
			w.Int(0, 63, 1)
		}
		script.WriteStep(w, "fire")
		w.Duration(100*time.Millisecond, 7*time.Millisecond)
		script.WriteStep(w, "progress")
		f.Add(w.Data())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		start := fuzzOrigins[r.Choice(len(fuzzOrigins))]
		t.Logf("kind=%v start=%d", k, start)
		s := &st{c: newChecker(t, k, start), es: make([]Entry[int], 16)}
		script.Run(t, data[len(data)-r.Remaining():], s)
		// Whatever is left must fire on a jump past every due time used.
		s.c.fire(s.c.now + int64(500*24*time.Hour))
		if s.c.s.Len() != 0 {
			t.Fatalf("%v: %d entries left after draining", k, s.c.s.Len())
		}
	})
}

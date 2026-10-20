package chaos

import (
	"strconv"
	"testing"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// state pairs the Ledger under test with a naive oracle of the same events.
type state struct {
	l         *Ledger
	outcome   map[string]Outcome
	delivered map[string]int
}

func newState() *state {
	return &state{l: NewLedger(), outcome: map[string]Outcome{}, delivered: map[string]int{}}
}

// FuzzLedger drives a Ledger with a random script and checks Check() against
// the oracle after every "check" step and at the end.
func FuzzLedger(f *testing.F) {
	script := fuzzutil.Script[*state]{
		Steps: []fuzzutil.Step[*state]{
			{Name: "pubA", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *state) {
				id := strconv.Itoa(r.Int(0, 7))
				s.outcome[id] = Accepted
				s.l.Published(id, Accepted)
			}},
			{Name: "pubR", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *state) {
				id := strconv.Itoa(r.Int(0, 7))
				if s.outcome[id] == 0 {
					s.outcome[id] = Rejected
				}
				s.l.Published(id, Rejected)
			}},
			{Name: "pubX", Weight: 1, Run: func(_ testing.TB, r *fuzzutil.Reader, s *state) {
				id := strconv.Itoa(r.Int(0, 7))
				if s.outcome[id] == 0 || s.outcome[id] == Rejected {
					s.outcome[id] = Ambiguous
				}
				s.l.Published(id, Ambiguous)
			}},
			{Name: "deliver", Weight: 3, Run: func(_ testing.TB, r *fuzzutil.Reader, s *state) {
				id := strconv.Itoa(r.Int(0, 9)) // 8 and 9 are never published: Unknown
				s.delivered[id]++
				s.l.Delivered(id)
			}},
			{Name: "check", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, s *state) {
				compare(t, s)
			}},
			{Name: "reset", Weight: 1, Run: func(_ testing.TB, _ *fuzzutil.Reader, s *state) {
				s.l.Reset()
				s.outcome = map[string]Outcome{}
				s.delivered = map[string]int{}
			}},
		},
		Check: compare,
	}
	w := fuzzutil.NewWriter()
	script.WriteStep(f, w, "pubA")
	w.Int(0, 7, 3)
	script.WriteStep(f, w, "deliver")
	w.Int(0, 9, 3)
	script.WriteStep(f, w, "deliver")
	w.Int(0, 9, 3)
	script.WriteStep(f, w, "pubR")
	w.Int(0, 7, 4)
	script.WriteStep(f, w, "deliver")
	w.Int(0, 9, 4)
	script.WriteStep(f, w, "deliver")
	w.Int(0, 9, 9)
	script.WriteStep(f, w, "check")
	f.Add(w.Data())
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	f.Fuzz(func(t *testing.T, data []byte) {
		script.Run(t, data, newState())
	})
}

// compare checks the Ledger report against the oracle.
func compare(t testing.TB, s *state) {
	t.Helper()
	rep := s.l.Check()
	var missing, rejected, unknown, dups, ambiguous, distinct int
	for id, o := range s.outcome {
		n := s.delivered[id]
		if n > 1 {
			dups += n - 1 // duplicates count for every published id
		}
		switch o {
		case Accepted:
			if n == 0 {
				missing++
			}
		case Rejected:
			if n > 0 {
				rejected++
			}
		case Ambiguous:
			if n > 0 {
				ambiguous++
			}
		}
	}
	for id := range s.delivered {
		if _, ok := s.outcome[id]; ok {
			distinct++ // Delivered counts distinct published ids
			continue
		}
		unknown++
	}
	if len(rep.Missing) != missing || len(rep.RejectedDelivered) != rejected || len(rep.Unknown) != unknown ||
		rep.Duplicates != dups || rep.AmbiguousDelivered != ambiguous || rep.Delivered != distinct {
		t.Fatalf("report %+v; oracle missing=%d rejected=%d unknown=%d dups=%d ambiguous=%d distinct=%d",
			rep, missing, rejected, unknown, dups, ambiguous, distinct)
	}
	if rep.OK() != (missing == 0 && rejected == 0 && unknown == 0) {
		t.Fatalf("OK() = %v for %+v", rep.OK(), rep)
	}
	if (rep.Error() == nil) != rep.OK() {
		t.Fatalf("Error() nil=%v but OK()=%v", rep.Error() == nil, rep.OK())
	}
}

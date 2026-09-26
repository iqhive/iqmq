package flush

import (
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// FuzzSettleFlush is the differential op-script fuzz.
//
// It drives one policy through an arbitrary interleaving of arrivals, ticks
// and explicit flushes, and checks two things after every step. First, that
// the policy's decision is the one [oracleSend] — the family's documentation
// written out as a table, independently of the four kind files — would have
// made. Second, the invariants no kind may break whatever it decides:
//
//   - conservation: everything that arrived has either been sent or is still
//     queued, exactly once;
//   - no batch exceeds MaxItems, which is the broker's own limit;
//   - a policy that claims a bound on what it holds keeps to it;
//   - the round-trip count never falls below the floor MaxItems allows.
//
// The clock is virtual and the whole target is pure, so a seed is a script and
// nothing here can flake.
func FuzzSettleFlush(f *testing.F) {
	for _, seed := range fuzzSeeds() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		kind := Kinds()[r.Choice(len(Kinds()))]
		cfg := Config{
			MaxItems: r.Int(1, MaxBatchForFuzz),
			Interval: time.Duration(r.Int(1, 1000)) * time.Microsecond,
		}
		p, err := New(kind, cfg)
		if err != nil {
			t.Fatalf("New(%v, %+v): %v", kind, cfg, err)
		}
		st := &fuzzRun{p: p, kind: kind.Resolve(), maxItems: p.MaxItems()}
		// The script reads what the header left. fuzzutil.Reader has no
		// cursor accessor, so the tail is computed from what it says is
		// unread — which is why the header is read first and once.
		fuzzScript.Run(t, data[len(data)-r.Remaining():], st)
	})
}

// MaxBatchForFuzz bounds the batch size the target explores. It is the
// service limit exists for: ten entries.
const MaxBatchForFuzz = DefaultMaxItems

// eventKind is what happened.
type eventKind uint8

const (
	evArrive eventKind = iota
	evTick
)

// oracleSend is the family's documentation as a table, written independently
// of the four kind files so that a change to one of them is a divergence
// rather than a change to both sides of the comparison.
func oracleSend(k Kind, ev eventKind, pending, maxItems int) bool {
	full := pending >= maxItems
	switch k {
	case Immediate:
		return ev == evArrive
	case Count:
		return ev == evArrive && full
	case Time:
		return ev == evTick && pending > 0
	case Hybrid:
		return (ev == evArrive && full) || (ev == evTick && pending > 0)
	case Auto, kindCount:
		return false
	default:
		return false
	}
}

// fuzzRun is the state one script drives.
type fuzzRun struct {
	p        Policy
	kind     Kind
	maxItems int

	arrived    int
	sent       int
	queued     int
	roundTrips int
	maxBatch   int
}

// arrive queues one settlement and applies the policy's decision.
func (s *fuzzRun) arrive(t testing.TB) {
	s.arrived++
	s.queued++
	got := s.p.Submitted(s.queued)
	want := oracleSend(s.kind, evArrive, s.queued, s.maxItems)
	if got != want {
		t.Fatalf("%v.Submitted(%d) = %v with MaxItems=%d; the family's table says %v",
			s.kind, s.queued, got, s.maxItems, want)
	}
	if got {
		s.flush(t)
	}
}

// tick fires the shared ticker and applies the policy's decision.
func (s *fuzzRun) tick(t testing.TB) {
	if _, ok := s.p.Ticker(); !ok {
		// A policy with no ticker is never ticked by the kit, and one that
		// answered a tick anyway would be deciding on an event it told the
		// driver not to deliver.
		if s.p.Ticked(s.queued) {
			t.Fatalf("%v asked for no ticker and yet sends on a tick", s.kind)
		}
		return
	}
	got := s.p.Ticked(s.queued)
	want := oracleSend(s.kind, evTick, s.queued, s.maxItems)
	if got != want {
		t.Fatalf("%v.Ticked(%d) = %v with MaxItems=%d; the family's table says %v",
			s.kind, s.queued, got, s.maxItems, want)
	}
	if got {
		s.flush(t)
	}
}

// flush is the kit's own send: everything queued, split on MaxItems.
func (s *fuzzRun) flush(testing.TB) {
	for s.queued > 0 {
		n := min(s.queued, s.maxItems)
		if n > s.maxBatch {
			s.maxBatch = n
		}
		s.roundTrips++
		s.sent += n
		s.queued -= n
	}
}

// check is the invariant set, run after every step.
func (s *fuzzRun) check(t testing.TB) {
	if s.sent+s.queued != s.arrived {
		t.Fatalf("%d arrived, %d sent, %d queued: a settlement was lost or sent twice", s.arrived, s.sent, s.queued)
	}
	if s.maxBatch > s.maxItems {
		t.Fatalf("a batch carried %d entries, over the limit of %d", s.maxBatch, s.maxItems)
	}
	if bound := s.p.MaxHeld(); bound >= 0 && s.queued > bound {
		t.Fatalf("%v holds %d settlements but claims to hold at most %d", s.kind, s.queued, bound)
	}
	if floor := minTrips(s.sent, s.maxItems); s.roundTrips < floor {
		t.Fatalf("%d round trips for %d settlements, below the floor of %d", s.roundTrips, s.sent, floor)
	}
	if s.roundTrips > s.sent {
		t.Fatalf("%d round trips for %d settlements: no policy costs more than one call each", s.roundTrips, s.sent)
	}
}

// fuzzScript is the op script: arrivals, ticks and the flush a Close performs.
var fuzzScript = fuzzutil.Script[*fuzzRun]{
	MaxSteps: 512,
	Steps: []fuzzutil.Step[*fuzzRun]{
		{Name: "arrive", Weight: 6, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzRun) { s.arrive(t) }},
		{Name: "tick", Weight: 2, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzRun) { s.tick(t) }},
		{Name: "close", Weight: 1, Run: func(t testing.TB, _ *fuzzutil.Reader, s *fuzzRun) { s.flush(t) }},
	},
	Check: func(t testing.TB, s *fuzzRun) { s.check(t) },
}

// fuzzSeeds are the committed inputs: one script per kind that reaches a full
// batch, a tick over a partial queue and a close over a held one.
func fuzzSeeds() [][]byte {
	out := make([][]byte, 0, len(Kinds()))
	for i := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), i)
		w.Int(1, MaxBatchForFuzz, 3)
		w.Int(1, 1000, 50)
		for range 4 {
			fuzzScript.WriteStep(w, "arrive")
		}
		fuzzScript.WriteStep(w, "tick")
		fuzzScript.WriteStep(w, "arrive")
		fuzzScript.WriteStep(w, "close")
		out = append(out, w.Data())
	}
	return out
}

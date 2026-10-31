package backoff

import (
	"math"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// backoffState is the op-script state: the policy under test, the delay
// chain a caller would keep, and the running invariants.
type backoffState struct {
	kind    Kind
	policy  Policy
	prev    time.Duration
	attempt int
	total   time.Duration
	lastIv  time.Duration
	lastN   int
}

// FuzzBackoff drives every policy with fuzz-chosen configurations,
// attempt numbers, previous delays and random words. The invariants are
// the family contract (the test suite): every delay is in
// [0, Max], nothing overflows into a negative duration, the unjittered
// schedule is monotone in the attempt number, and Next is a pure function
// of its arguments.
func FuzzBackoff(f *testing.F) {
	script := fuzzutil.Script[*backoffState]{
		Steps: []fuzzutil.Step[*backoffState]{
			{Name: "next", Weight: 6, Run: func(t testing.TB, r *fuzzutil.Reader, s *backoffState) {
				s.attempt++
				word := r.Uint64()
				d := s.policy.Next(s.attempt, s.prev, word)
				s.checkDelay(t, d, s.attempt)
				if again := s.policy.Next(s.attempt, s.prev, word); again != d {
					t.Fatalf("Next is not pure: %v then %v", d, again)
				}
				s.prev = d
				// The caller's own accumulator saturates; the invariant
				// under test is that no single delay is negative.
				if s.total = saturate(s.total, d); s.total < 0 {
					t.Fatalf("saturating accumulator went negative at attempt %d", s.attempt)
				}
			}},
			{Name: "jump", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *backoffState) {
				// An arbitrary attempt number and previous delay, including
				// the extremes a buggy caller can pass.
				attempt := r.Int(-1000, math.MaxInt32)
				prev := time.Duration(r.Uint64())
				d := s.policy.Next(attempt, prev, r.Uint64())
				s.checkDelay(t, d, attempt)
			}},
			{Name: "interval", Weight: 2, Run: func(t testing.TB, r *fuzzutil.Reader, s *backoffState) {
				n := r.Int(1, 1<<20)
				iv := s.policy.Interval(n)
				cfg := s.policy.Config()
				if iv < 0 || iv > cfg.Max {
					t.Fatalf("Interval(%d) = %v out of [0, %v]", n, iv, cfg.Max)
				}
				// Base is the first delay of every kind's schedule but None's.
				if first := s.policy.Interval(1); s.kind != None && first != cfg.Base {
					t.Fatalf("Interval(1) = %v, want Base %v (config %+v)", first, cfg.Base, cfg)
				}
				if n >= s.lastN && iv < s.lastIv {
					t.Fatalf("Interval(%d) = %v is below Interval(%d) = %v", n, iv, s.lastN, s.lastIv)
				}
				s.lastN, s.lastIv = n, iv
			}},
			{Name: "reconfigure", Weight: 1, Run: func(t testing.TB, r *fuzzutil.Reader, s *backoffState) {
				s.policy = mustNewWith(t, s.kind, fuzzConfig(r))
				s.prev, s.attempt = 0, 0
				s.lastN, s.lastIv = 0, 0
			}},
		},
		MaxSteps: 128,
	}
	for ki := range Kinds() {
		w := fuzzutil.NewWriter()
		w.Choice(len(Kinds()), ki)
		writeConfig(w, 200*time.Millisecond, 30*time.Second, 2)
		for range 8 {
			script.WriteStep(f, w, "next")
			w.Uint64(0x9E3779B97F4A7C15)
		}
		script.WriteStep(f, w, "interval")
		w.Int(1, 1<<20, 5)
		script.WriteStep(f, w, "jump")
		w.Int(-1000, math.MaxInt32, 1<<20)
		w.Uint64(uint64(time.Hour))
		w.Uint64(1)
		script.WriteStep(f, w, "reconfigure")
		writeConfig(w, time.Nanosecond, math.MaxInt64, 1000)
		f.Add(w.Data())
		// A growth the caller computed can come out NaN or +Inf.
		for _, corner := range []int{growthNaN, growthInf} {
			w := fuzzutil.NewWriter()
			w.Choice(len(Kinds()), ki)
			w.Uint64(uint64(200 * time.Millisecond))
			w.Uint64(uint64(30 * time.Second))
			w.Int(0, 1000, 20)
			w.Choice(8, corner)
			for range 4 {
				script.WriteStep(f, w, "next")
				w.Uint64(0x9E3779B97F4A7C15)
			}
			script.WriteStep(f, w, "interval")
			w.Int(1, 1<<20, 2)
			f.Add(w.Data())
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		k := Kinds()[r.Choice(len(Kinds()))]
		s := &backoffState{kind: k, policy: mustNewWith(t, k, fuzzConfig(r))}
		script.Run(t, data[len(data)-r.Remaining():], s)
	})
}

// saturate adds d to total without wrapping past the largest duration.
func saturate(total, d time.Duration) time.Duration {
	if total > math.MaxInt64-d {
		return math.MaxInt64
	}
	return total + d
}

// checkDelay asserts the envelope for one delay.
func (s *backoffState) checkDelay(t testing.TB, d time.Duration, attempt int) {
	t.Helper()
	cfg := s.policy.Config()
	switch {
	case d < 0:
		t.Fatalf("attempt %d: negative delay %v", attempt, d)
	case d > cfg.Max:
		t.Fatalf("attempt %d: %v exceeds Max %v (config %+v)", attempt, d, cfg.Max, cfg)
	case s.kind == None && d != 0:
		t.Fatalf("none returned %v", d)
	case s.kind == Constant && d != cfg.Base:
		t.Fatalf("constant returned %v, want %v", d, cfg.Base)
	case s.kind == Full && d > s.policy.Interval(attempt):
		t.Fatalf("full jitter %v exceeds the envelope %v", d, s.policy.Interval(attempt))
	case s.kind == Equal && attempt >= 1 && d < s.policy.Interval(attempt)/2:
		t.Fatalf("equal jitter %v is below half the envelope %v", d, s.policy.Interval(attempt))
	case s.kind == Decorrelated && d < min(cfg.Base, cfg.Max):
		t.Fatalf("decorrelated %v is below the floor %v", d, cfg.Base)
	}
}

// fuzzConfig decodes a configuration, including the degenerate corners.
func fuzzConfig(r *fuzzutil.Reader) Config {
	cfg := Config{
		Base:   time.Duration(r.Uint64() % uint64(time.Hour)),
		Max:    time.Duration(r.Uint64() % uint64(24*time.Hour)),
		Growth: float64(r.Int(0, 1000)) / 10,
	}
	switch r.Choice(8) {
	case 0:
		cfg.Max = -1
	case 1:
		cfg.Base = -1
	case 2:
		cfg.Base, cfg.Max = math.MaxInt64, math.MaxInt64
	case 3:
		cfg = Config{}
	case growthNaN:
		cfg.Growth = math.NaN()
	case growthInf:
		cfg.Growth = math.Inf(1)
	}
	return cfg
}

// The fuzzConfig corners that give a growth no caller should mean literally.
const (
	growthNaN = 5
	growthInf = 6
)

// writeConfig mirrors fuzzConfig for seed authoring.
func writeConfig(w *fuzzutil.Writer, base, max time.Duration, growth float64) {
	w.Uint64(uint64(base))
	w.Uint64(uint64(max))
	w.Int(0, 1000, int(growth*10))
	w.Choice(8, 4)
}

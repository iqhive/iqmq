package differential

import (
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

// script is a hand-written run: three records on two keys, fetched, one
// nacked and redelivered, all settled.
func script() Script {
	return Script{
		Limits: Limits{Keys: 2, Ops: 32, MaxSize: 8, MaxWait: time.Second},
		Ops: []Op{
			// Two records on key 0 and one on key 1, all settled first time,
			// which is the per-key order Compare is about.
			{Kind: Publish, Key: 0, Size: 4},
			{Kind: Publish, Key: 0, Size: 4},
			{Kind: Publish, Key: 1, Size: 4},
			{Kind: Fetch, Max: 4},
			{Kind: Ack},
			{Kind: Ack},
			{Kind: Ack},
			// One more on key 0, returned once and settled on its second
			// delivery, which is the redelivery an at-least-once broker may
			// settle out of order.
			{Kind: Publish, Key: 0, Size: 4},
			{Kind: Fetch, Max: 4},
			{Kind: Nack},
			{Kind: Wait, Dt: 10 * time.Millisecond},
			{Kind: Fetch, Max: 4},
			{Kind: Ack},
		},
	}
}

func run(t *testing.T, s Script) Observation {
	t.Helper()
	o := NewOracle(s.Limits, Config{AckDeadline: time.Second})
	obs, err := o.Run(s)
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	return obs
}

// TestOracleSettlesEveryRecord is the oracle's own sanity check: the script
// above settles all three records, the one it nacked twice over.
func TestOracleSettlesEveryRecord(t *testing.T) {
	obs := run(t, script())
	if len(obs.Settled) != 4 {
		t.Fatalf("settled %+v", obs.Settled)
	}
	if obs.Nacked != 1 {
		t.Fatalf("nacked %d, want 1", obs.Nacked)
	}
	if len(obs.Published[0]) != 3 || len(obs.Published[1]) != 1 {
		t.Fatalf("published %+v", obs.Published)
	}
	// The redelivered record is the one whose attempt count is above one,
	// which is what makes an at-least-once redelivery visible.
	redelivered := 0
	for _, d := range obs.Settled {
		if d.Attempt > 1 {
			redelivered++
		}
	}
	if redelivered != 1 {
		t.Fatalf("no settlement records a second attempt: %+v", obs.Settled)
	}
	if err := Compare(obs, obs, Requirements{}); err != nil {
		t.Fatalf("a run does not match itself: %v", err)
	}
}

// TestCompareFindsEveryKindOfDifference is the reason this package exists: a
// comparison that cannot fail proves nothing about a broker.
func TestCompareFindsEveryKindOfDifference(t *testing.T) {
	oracle := run(t, script())
	cases := []struct {
		name string
		req  Requirements
		warp func(o Observation) Observation
		want string
	}{
		{"lost", Requirements{}, func(o Observation) Observation {
			o.Settled = o.Settled[1:]
			return o
		}, "lost by the broker"},
		{"duplicated", Requirements{}, func(o Observation) Observation {
			// A second settlement of one record, as a redelivery, so that
			// the duplicate rule is the only one it can break.
			again := o.Settled[0]
			again.Attempt++
			o.Settled = append(append([]Delivery(nil), o.Settled...), again)
			return o
		}, "claims no duplicates"},
		{"invented", Requirements{AllowDuplicates: true, AllowMissing: true}, func(o Observation) Observation {
			o.Settled = append(append([]Delivery(nil), o.Settled...), Delivery{Key: 0, Record: 999, Attempt: 1})
			return o
		}, "the oracle never settled it"},
		{"reordered", Requirements{Ordering: PerKey, AllowDuplicates: true}, func(o Observation) Observation {
			// Swap the two first deliveries of key 0, which is the order
			// guarantee and nothing else.
			out := append([]Delivery(nil), o.Settled...)
			var first []int
			for i, d := range out {
				if d.Key == 0 && d.Attempt == 1 {
					first = append(first, i)
				}
			}
			if len(first) < 2 {
				t.Fatalf("the script does not settle two first deliveries of key 0: %+v", out)
			}
			out[first[0]], out[first[1]] = out[first[1]], out[first[0]]
			o.Settled = out
			return o
		}, "published after it"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Compare(oracle, tc.warp(oracle), tc.req)
			if err == nil {
				t.Fatalf("Compare accepted a %s observation", tc.name)
			}
			if !errors.Is(err, ErrDifference) {
				t.Fatalf("error %v does not wrap ErrDifference", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not say %q", err, tc.want)
			}
		})
	}
}

// TestComparePermitsWhatABrokerMayDo: an at-least-once broker redelivers, and
// a run may end with deliveries in flight. Neither is a difference when the
// broker claims no more than that.
func TestComparePermitsWhatABrokerMayDo(t *testing.T) {
	oracle := run(t, script())
	dup := oracle
	again := oracle.Settled[0]
	again.Attempt++
	dup.Settled = append(append([]Delivery(nil), oracle.Settled...), again)
	if err := Compare(oracle, dup, Requirements{AllowDuplicates: true}); err != nil {
		t.Fatalf("a duplicate under at-least-once: %v", err)
	}
	short := oracle
	short.Settled = oracle.Settled[:1]
	if err := Compare(oracle, short, Requirements{AllowMissing: true}); err != nil {
		t.Fatalf("an unfinished run: %v", err)
	}
	// A redelivery settled out of publication order is not an ordering
	// breach: the record went back to its lane.
	late := oracle
	out := append([]Delivery(nil), oracle.Settled...)
	for i := range out {
		if out[i].Attempt > 1 {
			d := out[i]
			out = append(append(out[:i:i], out[i+1:]...), d)
			break
		}
	}
	late.Settled = out
	if err := Compare(oracle, late, Requirements{Ordering: PerKey}); err != nil {
		t.Fatalf("a redelivery settled last: %v", err)
	}
}

// TestScriptRoundTrip: an interesting script can be committed as a seed and
// decodes to itself, which is what makes a differential failure reproducible.
func TestScriptRoundTrip(t *testing.T) {
	s := script()
	got := Decode(s.Encode(), s.Limits)
	if len(got.Ops) != len(s.Ops) {
		t.Fatalf("decoded %d ops, wrote %d", len(got.Ops), len(s.Ops))
	}
	for i, op := range s.Ops {
		if got.Ops[i] != op {
			t.Fatalf("op %d decoded as %+v, wrote %+v", i, got.Ops[i], op)
		}
	}
	for k := Kind(0); k < kindCount; k++ {
		if strings.Contains(k.String(), "?") {
			t.Fatalf("kind %d has no name", k)
		}
	}
	if Kind(99).String() != "kind(?)" {
		t.Fatalf("undefined kind renders as %q", Kind(99))
	}
}

// TestDecodedScriptsRunAndSettleSomething: the decoder's output is a run the
// oracle can complete, whatever the bytes were.
func TestDecodedScriptsRunAndSettleSomething(t *testing.T) {
	settled := 0
	rng := rand.New(rand.NewPCG(1, 2))
	for range 64 {
		data := make([]byte, 24)
		for i := range data {
			data[i] = byte(rng.UintN(256))
		}
		s := Decode(data, Limits{Keys: 2, MaxSize: 8})
		o := NewOracle(s.Limits, Config{MaxAttempts: 2})
		obs, err := o.Run(s)
		if err != nil {
			t.Fatalf("decoded script: %v", err)
		}
		settled += len(obs.Settled)
	}
	if settled == 0 {
		t.Fatal("no decoded script settled anything, so the alphabet is unreachable")
	}
}

func FuzzDifferentialOracle(f *testing.F) {
	f.Add(script().Encode())
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		s := Decode(data, Limits{Keys: 2, MaxSize: 16})
		o := NewOracle(s.Limits, Config{MaxAttempts: 2, Capacity: 4})
		obs, err := o.Run(s)
		if err != nil {
			t.Fatalf("oracle: %v", err)
		}
		// A script that settles nothing is not an interesting input, and
		// since it has a NAME: Compare answers ErrVacuous
		// rather than nil, so the self-match below would fail on it. Seed
		// #1 of this target is exactly such a script (publish, nack,
		// expire, wait — no fetch and no ack), and the assertion it used to
		// pass was itself vacuous.
		if len(obs.Settled) == 0 {
			if err := Compare(obs, obs, Requirements{Ordering: PerKey}); !errors.Is(err, ErrVacuous) {
				t.Fatalf("a script that settled nothing compared as %v, want ErrVacuous\nscript: %+v", err, s.Ops)
			}
			return
		}
		// Whatever the script, the oracle's own observation must match
		// itself under the strictest requirements: Compare is what a broker
		// is judged by, so a bug there is a bug in every differential run.
		if err := Compare(obs, obs, Requirements{Ordering: PerKey}); err != nil {
			t.Fatalf("the oracle does not match itself: %v\nscript: %+v", err, s.Ops)
		}
		if len(obs.Settled) > 1 {
			short := obs
			short.Settled = obs.Settled[1:]
			// ErrDifference and not merely "an error": with one settlement
			// removed from a set of one the answer would be ErrVacuous,
			// which is a different finding and would let this half pass
			// without the loss ever being noticed.
			if err := Compare(obs, short, Requirements{Ordering: PerKey}); !errors.Is(err, ErrDifference) {
				t.Fatalf("Compare answered %v to a lost settlement, want ErrDifference\nscript: %+v", err, s.Ops)
			}
		}
	})
}

// TestCompareRefusesAVacuousComparison is the guard.
//
// Every loop in Compare iterates over one of the two settled sets, so an empty
// set makes each check trivially true — and AllowMissing, which every
// unfinished run sets, removes the one loop that would have noticed. Both
// shapes returned nil, so a differential run that compared nothing was
// indistinguishable from one that found nothing.
func TestCompareRefusesAVacuousComparison(t *testing.T) {
	if err := Compare(Observation{}, Observation{}, Requirements{}); !errors.Is(err, ErrVacuous) {
		t.Errorf("two empty observations: %v, want ErrVacuous", err)
	}
	oracle := run(t, script())
	if len(oracle.Settled) == 0 {
		t.Fatal("the fixture oracle settled nothing, so the case below proves nothing")
	}
	err := Compare(oracle, Observation{Published: oracle.Published}, Requirements{AllowMissing: true})
	if !errors.Is(err, ErrVacuous) {
		t.Errorf("a broker that settled nothing under AllowMissing: %v, want ErrVacuous", err)
	}
	// And the two directions that must still be accepted: a real partial run
	// and a complete one.
	short := oracle
	short.Settled = oracle.Settled[:1]
	if err := Compare(oracle, short, Requirements{AllowMissing: true}); err != nil {
		t.Errorf("a partial run was refused: %v", err)
	}
	if err := Compare(oracle, oracle, Requirements{}); err != nil {
		t.Errorf("an identical run was refused: %v", err)
	}
}

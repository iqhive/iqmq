package differential

import (
	"strings"
	"testing"
)

// TestRelationAcceptsLegalAndRejectsDefects is the relation's negative
// control. Every row of [Transforms] is applied to [Subject] and the verdict
// is required in both directions: a broker-legal edit must be accepted, and a
// defect must be reported UNDER ITS OWN KIND rather than merely rejected — a
// relation that answered "reorder-within-key" to a lost message would pass a
// looser check and tell a reader the wrong thing.
//
// A transform that did not change its input is a failure of the table rather
// than a skip: a no-op row is a check that cannot fail, which is this
// repository's most-repeated defect.
func TestRelationAcceptsLegalAndRejectsDefects(t *testing.T) {
	oracle := Fixture()
	legal, defects := 0, 0
	for _, tr := range Transforms() {
		t.Run(tr.Name, func(t *testing.T) {
			got, changed := tr.Apply(oracle, Subject())
			if !changed {
				t.Fatalf("the transform changed nothing, so the assertion below could not fail")
			}
			res := Compare(oracle, got, tr.Perm)
			switch {
			case tr.Legal && !res.OK():
				t.Fatalf("a broker is allowed to do this and the relation refused it: %v", res.Err())
			case tr.Legal:
				legal++
			case res.OK():
				t.Fatalf("this is a defect and the relation accepted it [%s]", res.Summary)
			case !res.Has(tr.Kind):
				t.Fatalf("the relation rejected it as %v and not as %v: %v", res.Kinds(), tr.Kind, res.Err())
			default:
				defects++
			}
		})
	}
	if legal == 0 || defects == 0 {
		t.Fatalf("the table proved nothing: %d legal row(s), %d defect row(s)", legal, defects)
	}
	t.Logf("relation held to %d legal transform(s) and %d injected defect(s)", legal, defects)
}

// TestRelationRejectsAScriptDisagreement covers the two HARNESS kinds, which
// are not broker behaviour at all: two arms that published different ids did
// not run the same script, and an oracle that redelivered an accepted message
// is not an oracle. Both used to be indistinguishable from a broker defect,
// and a run that blames the broker for its own harness is worse than a red
// build.
func TestRelationRejectsAScriptDisagreement(t *testing.T) {
	t.Run("the subject published an id the oracle did not", func(t *testing.T) {
		s := Subject()
		s.Pubs = append(s.Pubs, Pub{Key: "a", ID: "extra", Outcome: Accepted})
		s.Dels = append(s.Dels, Del{Key: "a", ID: "extra", Settlement: Acked})
		res := Compare(Fixture(), s, Permissive())
		requireKind(t, res, KindScript)
	})
	t.Run("the arms disagree about a key", func(t *testing.T) {
		s := Subject()
		for i := range s.Pubs {
			if s.Pubs[i].ID == "m2" {
				s.Pubs[i].Key = "z"
			}
		}
		res := Compare(Fixture(), s, Permissive())
		requireKind(t, res, KindScript)
	})
	t.Run("the oracle redelivered an accepted message", func(t *testing.T) {
		o := Fixture()
		o.Dels = append(o.Dels, Del{Key: "a", ID: "m1", Settlement: Acked})
		res := Compare(o, Subject(), Permissive())
		requireKind(t, res, KindOracle)
	})
	t.Run("the oracle lost one", func(t *testing.T) {
		o, changed := removeDeliveries(Fixture(), "m5")
		if !changed {
			t.Fatal("the fixture has no m5 delivery to remove")
		}
		res := Compare(o, Subject(), Permissive())
		requireKind(t, res, KindOracle)
	})
}

// TestRelationHoldsAmbiguousPublishesBothWays covers the direction the
// transform table cannot: the fixture's ambiguous message has already
// vanished, so no EDIT makes it vanish. The specification says an ambiguous publish's fate
// is unknown and stays unknown, so the default relation permits both readings
// and counts them; a caller that wants one of them forbidden gets it.
func TestRelationHoldsAmbiguousPublishesBothWays(t *testing.T) {
	oracle, subject := Fixture(), Subject()
	id, ok := anyOutcome(subject, Ambiguous)
	if !ok {
		t.Fatal("the fixture has no ambiguous publish")
	}
	t.Run("vanishing is refused when AmbiguousMayVanish is false", func(t *testing.T) {
		res := Compare(oracle, subject, strictAmbiguousVanish())
		requireKind(t, res, KindLost)
		if res.Summary.AmbiguousVanished != 1 {
			t.Errorf("ambiguous-vanished = %d, want 1", res.Summary.AmbiguousVanished)
		}
	})
	t.Run("both readings are permitted by default", func(t *testing.T) {
		if err := Compare(oracle, subject, Permissive()).Err(); err != nil {
			t.Fatalf("a vanished ambiguous publish was refused by default: %v", err)
		}
		arrived, changed := append1(Subject(), Del{Key: keyOfPub(subject, id), ID: id, Settlement: Acked})
		if !changed {
			t.Fatal("the edit was a no-op")
		}
		res := Compare(oracle, arrived, Permissive())
		if err := res.Err(); err != nil {
			t.Fatalf("an arrived ambiguous publish was refused by default: %v", err)
		}
		if res.Summary.AmbiguousArrived != 1 {
			t.Errorf("ambiguous-arrived = %d, want 1", res.Summary.AmbiguousArrived)
		}
	})
}

// TestRelationRejectsAVacuousRun is the anti-vacuity half. `go test` cannot
// tell a comparison that found nothing from one that compared nothing, so the
// relation says which it was.
func TestRelationRejectsAVacuousRun(t *testing.T) {
	empty := Observation{Arm: "subject"}
	res := Compare(Observation{Arm: "oracle"}, empty, Permissive())
	requireKind(t, res, KindVacuous)
	if res.Summary.Compared != 0 {
		t.Fatalf("summary says %d compared over an empty run", res.Summary.Compared)
	}
	// A run with publishes but no deliveries is vacuous too, and is the
	// shape a broker arm takes when the consumer never connected.
	res = Compare(Fixture(), Observation{Arm: "subject", Pubs: Fixture().Pubs}, Permissive())
	if !res.Has(KindVacuous) && !res.Has(KindLost) {
		t.Fatalf("a subject that published everything and delivered nothing was accepted: %v", res.Kinds())
	}
}

// TestSummaryCountsWhatItSaw holds the summary to the observation, because the
// summary is what a green run's log line reports: a run whose summary says
// "compared=0" has proved nothing however green it is.
func TestSummaryCountsWhatItSaw(t *testing.T) {
	s, changed := insertAfterFirst(Subject(), "m3", Del{Key: "a", ID: "m3", Settlement: Acked})
	if !changed {
		t.Fatal("no m3 delivery to duplicate")
	}
	res := Compare(Fixture(), s, Permissive())
	if !res.OK() {
		t.Fatalf("a legal duplicate was refused: %v", res.Err())
	}
	// m1..m7 are accepted on both arms; m8 is ambiguous and m9 rejected.
	if res.Summary.Compared != 7 {
		t.Errorf("compared = %d, want 7 (m1..m7)", res.Summary.Compared)
	}
	if res.Summary.Duplicates != 1 {
		t.Errorf("duplicates = %d, want 1", res.Summary.Duplicates)
	}
	if res.Summary.AmbiguousVanished != 1 {
		t.Errorf("ambiguous-vanished = %d, want 1 (m8)", res.Summary.AmbiguousVanished)
	}
	if res.Summary.Keys != 3 || res.Summary.OrderedKeys != 3 {
		t.Errorf("keys = %d, ordered = %d, want 3 and 3", res.Summary.Keys, res.Summary.OrderedKeys)
	}
	if got := res.Summary.String(); !strings.Contains(got, "compared=7") {
		t.Errorf("summary %q does not report what it compared", got)
	}
}

// TestKindStringsAreDistinct keeps the failure messages readable: two kinds
// that render the same make a divergence report ambiguous, and Kinds() would
// collapse them.
func TestKindStringsAreDistinct(t *testing.T) {
	seen := map[string]Kind{}
	for k := KindScript; k <= KindVacuous; k++ {
		s := k.String()
		if s == "invalid" {
			t.Errorf("kind %d has no name", k)
		}
		if prev, ok := seen[s]; ok {
			t.Errorf("kinds %d and %d both render as %q", prev, k, s)
		}
		seen[s] = k
	}
	if Kind(0).String() != "invalid" || !KindScript.Harness() || !KindOracle.Harness() || KindLost.Harness() {
		t.Error("Harness() does not separate the harness kinds from the subject's")
	}
}

func requireKind(t *testing.T, res Result, want Kind) {
	t.Helper()
	if res.OK() {
		t.Fatalf("the relation accepted it [%s]", res.Summary)
	}
	if !res.Has(want) {
		t.Fatalf("reported %v, want %v: %v", res.Kinds(), want, res.Err())
	}
}

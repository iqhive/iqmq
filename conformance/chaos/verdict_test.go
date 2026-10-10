package chaos

import (
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// goodRun is a Run every check below accepts: a fault that bit, a ledger with
// no loss and some duplicates, an ambiguous settlement, a prompt resume and a
// bounded buffer.
func goodRun() *Run {
	l := NewLedger()
	for i := range 20 {
		id := "m" + string(rune('a'+i%20))
		l.Published(id, Accepted)
		l.Delivered(id)
	}
	l.Delivered("ma") // one duplicate
	l.Published("amb", Ambiguous)
	l.Published("rej", Rejected)
	return &Run{
		Scenario: "good", Report: l.Check(),
		Faulted: 3, Unknown: 2, SettleFailed: 1,
		Resumed: true, ResumeAfter: 2 * time.Second, Drained: true,
		PeakPending: 64, PendingLimit: 256,
		Codes: map[mq.ErrorCode]int{mq.CodeUnavailable: 3, mq.CodeNotFound: 1},
	}
}

// checkCase is one check with a Run it must accept and a Run it must reject.
type checkCase struct {
	name  string
	check Check
	// break_ makes a good Run bad in the one way this check is about.
	break_ func(*Run)
	// want is a fragment the rejection must name, so that a failure message
	// says which property failed rather than only that one did.
	want string
}

// TestChecksAcceptAndReject is the verdict's negative control: every check is
// handed a Run it must accept and a Run it must reject.
//
// The `break_` half is what makes this worth having. Six checks in this
// repository turned out not to be able to fail, and a chaos verdict is the
// easiest place for that to happen: a scenario whose toxic did not bite
// produces a run in which every property holds trivially, and a check written
// as "if something went wrong, require X" passes such a run silently.
func TestChecksAcceptAndReject(t *testing.T) {
	cases := []checkCase{
		{
			name: "FaultBit", check: FaultBit(2), want: "nothing was injected",
			break_: func(r *Run) { r.Faulted = 1 },
		},
		{
			name: "DrainConverged", check: DrainConverged(), want: "did not converge",
			break_: func(r *Run) { r.Drained = false },
		},
		{
			name: "NoLoss", check: NoLoss(), want: "lost",
			break_: func(r *Run) {
				l := NewLedger()
				l.Published("gone", Accepted)
				r.Report = l.Check()
			},
		},
		{
			name: "NoLoss/a rejected publish that was delivered", check: NoLoss(), want: "rejected",
			break_: func(r *Run) {
				l := NewLedger()
				l.Published("no", Rejected)
				l.Delivered("no")
				r.Report = l.Check()
			},
		},
		{
			name: "NoLoss/an id nobody published", check: NoLoss(), want: "unknown",
			break_: func(r *Run) {
				l := NewLedger()
				l.Published("ok", Accepted)
				l.Delivered("ok")
				l.Delivered("stranger")
				r.Report = l.Check()
			},
		},
		{
			name: "Accounted", check: Accounted(10), want: "needs 10",
			break_: func(r *Run) {
				l := NewLedger()
				l.Published("one", Accepted)
				l.Delivered("one")
				r.Report = l.Check()
			},
		},
		{
			name: "SettlementUnknownSeen", check: SettlementUnknownSeen(), want: "ErrSettlementUnknown",
			break_: func(r *Run) { r.Unknown = 0 },
		},
		{
			name: "ResumedWithin/never", check: ResumedWithin(5 * time.Second), want: "never made progress",
			break_: func(r *Run) { r.Resumed = false },
		},
		{
			// The bound has to be one the GOOD run also satisfies: written
			// as ResumedWithin(time.Second) against a good run that
			// resumed after two seconds, the accepting half failed and the
			// case tested the fixture rather than the check.
			name: "ResumedWithin/too late", check: ResumedWithin(5 * time.Second), want: "over the 5s bound",
			break_: func(r *Run) { r.ResumeAfter = 30 * time.Second },
		},
		{
			name: "PendingBounded", check: PendingBounded(), want: "not bounded",
			break_: func(r *Run) { r.PeakPending = r.PendingLimit + 1 },
		},
		{
			name: "PendingBounded/unrecorded bound", check: PendingBounded(), want: "did not record",
			break_: func(r *Run) { r.PendingLimit = 0 },
		},
		{
			name: "CodeSeen", check: CodeSeen(mq.CodeUnavailable), want: "never saw Unavailable",
			break_: func(r *Run) { delete(r.Codes, mq.CodeUnavailable) },
		},
		{
			name: "AnyCodeSeen", check: AnyCodeSeen(mq.CodeNotFound, mq.CodeInvalidArgument), want: "saw none of",
			break_: func(r *Run) { r.Codes = map[mq.ErrorCode]int{mq.CodeUnavailable: 1} },
		},
		{
			name: "DegradedOrCode", check: DegradedOrCode(mq.CodeNotFound), want: "neither refused an operation nor reported itself degraded",
			break_: func(r *Run) { r.Codes = nil; r.Degraded = false },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := c.check(goodRun()); err != nil {
				t.Fatalf("the check refused a run that satisfies it: %v", err)
			}
			bad := goodRun()
			c.break_(bad)
			err := c.check(bad)
			if err == nil {
				t.Fatalf("the check accepted a run that breaks it (%s)", bad)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the rejection says %q, which does not name %q", err, c.want)
			}
		})
	}
	t.Logf("%d check(s) held in both directions", len(cases))
}

// TestNoLossWillNotClaimLossFromAnUnfinishedDrain is the guard.
//
// Against a live JetStream under a reset storm the run reported "missing=8"
// beside "resumed=false": the client never came back, so the drain could not
// fetch anything, and eight messages were counted as LOST that had merely
// never been fetched. A loss claim needs a drain that finished — and the run
// is still red, on ResumedWithin, which is the real finding.
func TestNoLossWillNotClaimLossFromAnUnfinishedDrain(t *testing.T) {
	l := NewLedger()
	for _, id := range []string{"a", "b", "c"} {
		l.Published(id, Accepted)
	}
	l.Delivered("a")
	unfinished := &Run{Scenario: "unfinished", Report: l.Check(), Faulted: 1, Drained: false}
	if err := NoLoss()(unfinished); err != nil {
		t.Fatalf("NoLoss claimed loss from a drain that never converged: %v", err)
	}
	if err := DrainConverged()(unfinished); err == nil {
		t.Fatal("DrainConverged accepted a run whose drain never converged")
	}
	// With the drain converged the SAME ledger is loss, and NoLoss says so.
	finished := *unfinished
	finished.Drained = true
	err := NoLoss()(&finished)
	if err == nil {
		t.Fatal("NoLoss accepted two undelivered accepted messages after a converged drain")
	}
	if !strings.Contains(err.Error(), "lost") {
		t.Fatalf("the rejection does not name the loss: %v", err)
	}
	t.Logf("with the drain converged: %v", err)
	// And the defects that do not depend on the drain are still caught when
	// it did not converge: a rejected publish that arrived, and a stranger.
	l2 := NewLedger()
	l2.Published("no", Rejected)
	l2.Delivered("no")
	l2.Delivered("stranger")
	bad := &Run{Scenario: "unfinished", Report: l2.Check(), Faulted: 1, Drained: false}
	if err := NoLoss()(bad); err == nil {
		t.Fatal("an unfinished drain excused a rejected-but-delivered message and an unknown id")
	}
}

// TestDegradedOrCodeTakesEitherAnswer covers the one check with two honest
// answers: a deleted topic is a refused operation on one broker and a degraded
// health report on another.
func TestDegradedOrCodeTakesEitherAnswer(t *testing.T) {
	byCode := goodRun()
	byCode.Degraded = false
	byCode.Codes = map[mq.ErrorCode]int{mq.CodeNotFound: 1}
	if err := DegradedOrCode(mq.CodeNotFound)(byCode); err != nil {
		t.Errorf("a broker that refused the operation was failed: %v", err)
	}
	byHealth := goodRun()
	byHealth.Codes = nil
	byHealth.Degraded = true
	if err := DegradedOrCode(mq.CodeNotFound)(byHealth); err != nil {
		t.Errorf("a broker that reported itself degraded was failed: %v", err)
	}
}

// TestAllJoinsEveryFailure keeps a scenario's report complete: one run that
// breaks three properties must say all three, because a chaos run is expensive
// and finding out about the second failure on the next run costs a container.
func TestAllJoinsEveryFailure(t *testing.T) {
	bad := goodRun()
	bad.Faulted, bad.Unknown, bad.Resumed = 0, 0, false
	err := All(bad, FaultBit(1), SettlementUnknownSeen(), ResumedWithin(time.Second), NoLoss())
	if err == nil {
		t.Fatal("All accepted a run that breaks three of its four checks")
	}
	for _, want := range []string{"nothing was injected", "ErrSettlementUnknown", "never made progress"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the joined error does not name %q:\n%v", want, err)
		}
	}
	if err := All(goodRun(), FaultBit(1), NoLoss(), PendingBounded()); err != nil {
		t.Errorf("All refused a good run: %v", err)
	}
}

// TestRunStringReportsWhatItSaw holds the log line to the run, because the log
// line is the only record a green chaos run leaves.
func TestRunStringReportsWhatItSaw(t *testing.T) {
	got := goodRun().String()
	for _, want := range []string{"faulted=3", "unknown=2", "resumed=true", "pending=64/256", "duplicates=1", "missing=0"} {
		if !strings.Contains(got, want) {
			t.Errorf("Run.String() = %q, missing %q", got, want)
		}
	}
	// A run that recorded nothing must not read like a run that passed.
	empty := (&Run{Scenario: "empty"}).String()
	if strings.Contains(empty, "ledger") {
		t.Errorf("a run with no ledger rendered one: %q", empty)
	}
}

// TestConvergedNeedsAllThreeFacts holds the drain's rule to a table.
//
// The rule decides whether the ledger's Missing column may be read as loss, so
// each of its three facts has to be load-bearing — and the third, that the
// client is usable, was missing: a receive that times out with nothing to show
// is counted as "the topic is empty", which it is on a healthy client and is
// not on one that cannot reach the broker. A reset storm produced three
// timeouts in a row on a dead connection and the drain read them as three
// empty topics.
func TestConvergedNeedsAllThreeFacts(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		healthy                 int
		pastLease, usable, want bool
	}{
		{"all three", 3, true, true, true},
		{"more than enough empties", 9, true, true, true},
		{"too few empty receives", 2, true, true, false},
		{"no empty receive at all", 0, true, true, false},
		{"before the lease could expire", 3, false, true, false},
		{"the client did not work after the fault", 3, true, false, false},
		{"none of them", 0, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := converged(tc.healthy, tc.pastLease, tc.usable); got != tc.want {
				t.Fatalf("converged(%d, %v, %v) = %v, want %v",
					tc.healthy, tc.pastLease, tc.usable, got, tc.want)
			}
		})
	}
	// ready() is the fallback for a scenario with no resume probe, and on a
	// nil client it is false: a drain with no client to ask has not
	// converged either.
	if ready(nil) {
		t.Error("ready(nil) is true")
	}
}

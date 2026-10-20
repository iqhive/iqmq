package shutdown

import (
	"errors"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/fuzzutil"
)

// step applies op and checks the invariants, which is how every operation in
// this package's tests is applied.
func step(t testing.TB, m *Model, op Op) Result {
	t.Helper()
	r := m.Step(op)
	if err := m.Invariants(); err != nil {
		t.Fatalf("after %s(rec=%d closer=%d): %v", op.Kind, op.Rec, op.Closer, err)
	}
	return r
}

// publish admits and places one record.
func publish(t testing.TB, m *Model, rec int) {
	t.Helper()
	if r := step(t, m, Op{Kind: Admit, Rec: rec}); !r.OK {
		t.Fatalf("Admit(%d): %v", rec, r.Err)
	}
	if r := step(t, m, Op{Kind: Place, Rec: rec}); !r.OK {
		t.Fatalf("Place(%d): %v", rec, r.Err)
	}
}

// deliver publishes and leases one record.
func deliver(t testing.TB, m *Model, rec int) {
	t.Helper()
	publish(t, m, rec)
	if r := step(t, m, Op{Kind: Claim, Rec: rec}); !r.OK {
		t.Fatalf("Claim(%d): %v", rec, r.Err)
	}
	if r := step(t, m, Op{Kind: Take, Rec: rec}); !r.OK {
		t.Fatalf("Take(%d): %v", rec, r.Err)
	}
}

func TestNames(t *testing.T) {
	if len(Phases()) != int(phaseCount) {
		t.Fatalf("Phases() returned %d", len(Phases()))
	}
	for _, p := range Phases() {
		if p.String() == "" || !p.valid() {
			t.Fatalf("phase %d has no name", p)
		}
	}
	if got := phaseCount.String(); got != "phase(?)" {
		t.Fatalf("out-of-range phase named %q", got)
	}
	for s := Unpublished; s < stateCount; s++ {
		if s.String() == "" {
			t.Fatalf("state %d has no name", s)
		}
	}
	if got := stateCount.String(); got != "state(?)" {
		t.Fatalf("out-of-range state named %q", got)
	}
	if len(Kinds()) != int(kindCount) {
		t.Fatalf("Kinds() returned %d", len(Kinds()))
	}
	for _, k := range Kinds() {
		if k.String() == "" {
			t.Fatalf("kind %d has no name", k)
		}
	}
	if got := kindCount.String(); got != "op(?)" {
		t.Fatalf("out-of-range kind named %q", got)
	}
	for s := Unpublished; s < stateCount; s++ {
		want := s == Acked || s == DeadLettered || s == Dropped
		if s.Terminal() != want {
			t.Fatalf("%s.Terminal() = %v", s, s.Terminal())
		}
	}
}

func TestUndefinedOperationFails(t *testing.T) {
	if r := New(Config{Records: 1}).Step(Op{Kind: kindCount}); r.Err == nil {
		t.Fatal("an undefined operation did not fail")
	}
}

func TestCloseDropsWhatTheLanesHold(t *testing.T) {
	m := New(Config{Records: 3})
	publish(t, m, 0)
	deliver(t, m, 1)
	// Record 2 is left unpublished on purpose: nothing to release.
	step(t, m, Op{Kind: Close})
	if m.Phase() != Closed {
		t.Fatalf("phase %s after Close", m.Phase())
	}
	for _, rec := range []int{0, 1} {
		if got := m.State(rec); got != Dropped {
			t.Fatalf("record %d is %s, want dropped", rec, got)
		}
		if got := m.Releases(rec); got != 1 {
			t.Fatalf("record %d was released %d times", rec, got)
		}
	}
	if got := m.Releases(2); got != 0 {
		t.Fatalf("an unpublished record was released %d times", got)
	}
	if !m.Done(0) {
		t.Fatal("the closer did not return")
	}
	if r := step(t, m, Op{Kind: Admit, Rec: 2}); r.OK || !errors.Is(r.Err, errcode.ErrClosed) {
		t.Fatalf("Admit after Close: %v", r.Err)
	}
}

func TestPromptCloseDoesNotWaitForHandlers(t *testing.T) {
	m := New(Config{Records: 1})
	deliver(t, m, 0)
	step(t, m, Op{Kind: Close})
	if m.Phase() != Closed {
		t.Fatalf("prompt Close stopped in %s with a handler in flight", m.Phase())
	}
}

func TestGracefulShutdownWaitsForTheHandler(t *testing.T) {
	m := New(Config{Records: 1, DrainTimeout: time.Minute})
	deliver(t, m, 0)
	step(t, m, Op{Kind: Shutdown})
	if m.Phase() != Draining {
		t.Fatalf("phase %s, want draining while the handler runs", m.Phase())
	}
	step(t, m, Op{Kind: Ack, Rec: 0})
	if m.Phase() != Closed {
		t.Fatalf("phase %s after the handler settled", m.Phase())
	}
	if got := m.State(0); got != Acked {
		t.Fatalf("record 0 is %s", got)
	}
}

func TestDrainDeadlineEndsTheWait(t *testing.T) {
	m := New(Config{Records: 1, DrainTimeout: time.Minute})
	deliver(t, m, 0)
	step(t, m, Op{Kind: Shutdown})
	if m.Phase() != Draining {
		t.Fatalf("phase %s, want draining", m.Phase())
	}
	step(t, m, Op{Kind: Tick, Delay: 30 * time.Second})
	if m.Phase() != Draining {
		t.Fatalf("phase %s: the drain ended before its deadline", m.Phase())
	}
	step(t, m, Op{Kind: Tick, Delay: 30 * time.Second})
	if m.Phase() != Closed {
		t.Fatalf("phase %s: the deadline did not end the drain", m.Phase())
	}
	if got := m.State(0); got != Dropped {
		t.Fatalf("record 0 is %s, want dropped by the deadline", got)
	}
}

func TestUnboundedDrainNeverExpires(t *testing.T) {
	m := New(Config{Records: 1, DrainTimeout: -1})
	deliver(t, m, 0)
	step(t, m, Op{Kind: Shutdown})
	step(t, m, Op{Kind: Tick, Delay: 1000 * time.Hour})
	if m.Phase() != Draining {
		t.Fatalf("phase %s: an unbounded drain expired", m.Phase())
	}
	step(t, m, Op{Kind: Ack, Rec: 0})
	if m.Phase() != Closed {
		t.Fatalf("phase %s", m.Phase())
	}
}

func TestDeadLetterOrdering(t *testing.T) {
	m := New(Config{Records: 2})
	deliver(t, m, 0)
	deliver(t, m, 1)
	step(t, m, Op{Kind: DeadLetter, Rec: 0})
	if got := m.State(0); got != DLQPending {
		t.Fatalf("record 0 is %s", got)
	}
	if got := m.Releases(0); got != 0 {
		t.Fatal("a dead-letter-pending record was released")
	}
	// A failed publication puts the record back rather than settling it.
	step(t, m, Op{Kind: DeadLetter, Rec: 1})
	step(t, m, Op{Kind: FailDeadLetter, Rec: 1})
	if got := m.State(1); got != Held {
		t.Fatalf("a failed dead-letter left record 1 %s, want held", got)
	}
	if got := m.Releases(1); got != 0 {
		t.Fatal("a failed dead-letter released the record")
	}
	step(t, m, Op{Kind: CommitDeadLetter, Rec: 0})
	if got := m.State(0); got != DeadLettered {
		t.Fatalf("record 0 is %s after a committed publication", got)
	}
	if got := m.Releases(0); got != 1 {
		t.Fatalf("record 0 was released %d times", got)
	}
}

func TestSettlingWaitsForDeadLetters(t *testing.T) {
	m := New(Config{Records: 1, DrainTimeout: time.Minute})
	deliver(t, m, 0)
	step(t, m, Op{Kind: DeadLetter, Rec: 0})
	step(t, m, Op{Kind: Shutdown})
	if m.Phase() != Settling {
		t.Fatalf("phase %s, want settling while the publication is in flight", m.Phase())
	}
	if got := m.DeadLettersPending(); got != 1 {
		t.Fatalf("%d publications pending", got)
	}
	step(t, m, Op{Kind: CommitDeadLetter, Rec: 0})
	if m.Phase() != Closed {
		t.Fatalf("phase %s after the publication committed", m.Phase())
	}
	if got := m.State(0); got != DeadLettered {
		t.Fatalf("record 0 is %s", got)
	}
}

func TestFailedDeadLetterAfterCloseReleases(t *testing.T) {
	m := New(Config{Records: 1})
	deliver(t, m, 0)
	step(t, m, Op{Kind: DeadLetter, Rec: 0})
	step(t, m, Op{Kind: Close})
	if m.Phase() != Closed {
		t.Fatalf("prompt Close waited for the publication (%s)", m.Phase())
	}
	if got := m.State(0); got != DLQPending {
		t.Fatalf("record 0 is %s: the close settled a pending publication", got)
	}
	step(t, m, Op{Kind: FailDeadLetter, Rec: 0})
	if got := m.State(0); got != Dropped {
		t.Fatalf("record 0 is %s: a failed publication after close has no lane", got)
	}
	if got := m.Releases(0); got != 1 {
		t.Fatalf("record 0 was released %d times", got)
	}
}

func TestSeveralClosersRunTheSequenceOnce(t *testing.T) {
	m := New(Config{Records: 1, Closers: 3, DrainTimeout: time.Minute})
	deliver(t, m, 0)
	step(t, m, Op{Kind: Shutdown, Closer: 0})
	step(t, m, Op{Kind: Close, Closer: 1})
	step(t, m, Op{Kind: Shutdown, Closer: 2})
	if got := m.Runs(); got != 1 {
		t.Fatalf("the sequence ran %d times", got)
	}
	// The first caller asked for a graceful shutdown, so the run stays
	// graceful whatever the later ones asked for: they join the run under
	// way rather than changing it.
	if m.Phase() != Draining {
		t.Fatalf("phase %s: a later prompt Close changed the run", m.Phase())
	}
	for c := range 3 {
		if m.Done(c) {
			t.Fatalf("closer %d returned before the handle closed", c)
		}
	}
	step(t, m, Op{Kind: Ack, Rec: 0})
	for c := range 3 {
		if !m.Done(c) {
			t.Fatalf("closer %d did not return", c)
		}
	}
	if r := step(t, m, Op{Kind: Close, Closer: 7}); r.OK {
		t.Fatal("an unknown closer was accepted")
	}
}

func TestOperationsRefusedPastTheirBarrier(t *testing.T) {
	m := New(Config{Records: 2, DrainTimeout: time.Minute})
	publish(t, m, 0)
	deliver(t, m, 1)
	step(t, m, Op{Kind: Shutdown})
	if m.Phase() != Draining {
		t.Fatalf("phase %s", m.Phase())
	}
	if r := step(t, m, Op{Kind: Admit, Rec: 0}); r.OK {
		t.Fatal("a publication was admitted while draining")
	}
	if r := step(t, m, Op{Kind: Claim, Rec: 0}); r.OK {
		t.Fatal("a delivery was claimed while draining")
	}
	// A settlement is still admitted: that is what the drain is waiting for.
	if r := step(t, m, Op{Kind: Ack, Rec: 1}); !r.OK {
		t.Fatalf("the in-flight delivery could not settle: %v", r.Err)
	}
	if r := step(t, m, Op{Kind: Ack, Rec: 1}); r.OK {
		t.Fatal("a settled delivery settled twice")
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	m := New(Config{Records: 2, Closers: 2})
	deliver(t, m, 0)
	step(t, m, Op{Kind: DeadLetter, Rec: 0})
	snap := m.Snapshot()
	if snap.Steps != m.Steps() || snap.Phase != m.Phase() || snap.DeadLettersPending != 1 {
		t.Fatalf("snapshot %+v does not match the model", snap)
	}
	snap.Recs[0].State = Dropped
	snap.Done[0] = true
	if m.State(0) != DLQPending || m.Done(0) {
		t.Fatal("mutating the snapshot changed the model")
	}
	if m.Records() != 2 || m.Closers() != 2 || m.Now() != 0 {
		t.Fatal("the model reports the wrong shape")
	}
	if m.State(-1) != Unpublished || m.Releases(-1) != 0 {
		t.Fatal("an out-of-range record is not unpublished")
	}
}

func TestOutOfRangeOperationsAreRefused(t *testing.T) {
	m := New(Config{Records: -1})
	if m.Records() != 0 || m.Closers() != 1 {
		t.Fatalf("a negative record count gave %d records", m.Records())
	}
	for _, k := range []Kind{Admit, Place, Claim, Take, Ack, DeadLetter, CommitDeadLetter, FailDeadLetter} {
		if r := step(t, m, Op{Kind: k, Rec: 4}); r.OK {
			t.Fatalf("%s on an unknown record was accepted", k)
		}
	}
}

// TestRandomInvariants walks the model with a deterministic pseudo-random
// script and checks the invariants after every step.
func TestRandomInvariants(t *testing.T) {
	for seed := range 200 {
		m := New(Config{Records: 3, Closers: 2, DrainTimeout: 10 * time.Second})
		x := uint64(seed)*2862933555777941757 + 3037000493
		for range 60 {
			x = x*6364136223846793005 + 1442695040888963407
			op := Op{
				Kind:   Kinds()[int(x>>33)%int(kindCount)],
				Rec:    int(x>>13) % 3,
				Closer: int(x >> 7 % 2),
				Delay:  time.Duration(x>>3%8) * time.Second,
			}
			step(t, m, op)
		}
	}
}

// FuzzShutdownModel drives the model with fuzz-chosen operations and checks
// the invariants after every step. It is the model's own consistency check;
// the differential against internal/lifecycle is FuzzShutdown.
func FuzzShutdownModel(f *testing.F) {
	seed := func(kinds []Kind, recs []int) []byte {
		w := fuzzutil.NewWriter()
		w.Int(1, 4, 3)
		w.Int(1, 3, 2)
		w.Int(0, 60, 10)
		for i, k := range kinds {
			w.Choice(int(kindCount), int(k))
			w.Int(0, 3, recs[i%len(recs)])
			w.Int(0, 2, 0)
			w.Int(0, 8, 1)
		}
		return w.Data()
	}
	f.Add(seed(
		[]Kind{Admit, Place, Claim, Take, Ack, Close},
		[]int{0},
	))
	f.Add(seed(
		[]Kind{Admit, Place, Claim, Take, DeadLetter, Shutdown, CommitDeadLetter},
		[]int{1},
	))
	f.Add(seed(
		[]Kind{Admit, Place, Claim, Take, Shutdown, Tick, Tick, Tick},
		[]int{2},
	))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := fuzzutil.NewReader(data)
		m := New(Config{
			Records:      r.Int(1, 4),
			Closers:      r.Int(1, 3),
			DrainTimeout: time.Duration(r.Int(0, 60)) * time.Second,
		})
		for !r.Exhausted() {
			op := Op{
				Kind:   Kind(r.Choice(int(kindCount))),
				Rec:    r.Int(0, 3),
				Closer: r.Int(0, 2),
				Delay:  time.Duration(r.Int(0, 8)) * time.Second,
			}
			m.Step(op)
			if err := m.Invariants(); err != nil {
				t.Fatalf("after %s(rec=%d closer=%d delay=%v): %v\n%+v",
					op.Kind, op.Rec, op.Closer, op.Delay, err, m.Snapshot())
			}
		}
	})
}

// TestInvariantsHaveTeeth forces each fault the invariants exist to catch
// directly onto the model's state and checks that the invariant that names it
// reports it. The transitions cannot produce these states — that is what the
// model is for — so without this test an invariant could be silently
// unfalsifiable, which is the failure mode reviews of this repository keep
// finding.
func TestInvariantsHaveTeeth(t *testing.T) {
	closed := func() *Model {
		m := New(Config{Records: 2, Closers: 2, DrainTimeout: time.Minute})
		deliver(t, m, 0)
		return m
	}
	cases := []struct {
		name string
		want string
		fold func(m *Model)
	}{
		{"TypeOK/phase", "TypeOK", func(m *Model) { m.phase = phaseCount }},
		{"TypeOK/state", "TypeOK", func(m *Model) { m.recs[0].state = stateCount }},
		{"TypeOK/dlq", "TypeOK", func(m *Model) { m.recs[0].dlqOpen = true }},
		{"PhaseMonotone", "PhaseMonotone", func(m *Model) {
			m.to(Settling)
			m.to(StopAccept)
		}},
		{"PhaseMonotone/maxPhase", "PhaseMonotone", func(m *Model) {
			m.maxPhase = Closed
		}},
		{"CloseIdempotent/runs", "CloseIdempotent", func(m *Model) { m.runs = 2 }},
		{"CloseIdempotent/early", "CloseIdempotent", func(m *Model) {
			m.waiting[0], m.done[0] = true, true
		}},
		{"CloseIdempotent/unasked", "CloseIdempotent", func(m *Model) {
			m.to(Closed)
			m.done[1] = true
		}},
		{"NoPublishAfterStopAccept", "NoPublishAfterStopAccept", func(m *Model) {
			// The placement the barrier is there to prevent: the phase moves
			// on with a publication still between its two steps, and the
			// placement lands afterwards.
			m.Step(Op{Kind: Admit, Rec: 1})
			m.phase, m.maxPhase = Settling, Settling
			m.Step(Op{Kind: Place, Rec: 1})
		}},
		{"NoFetchAfterStopFetch", "NoFetchAfterStopFetch", func(m *Model) {
			publish(t, m, 1)
			m.Step(Op{Kind: Claim, Rec: 1})
			m.phase, m.maxPhase = Draining, Draining
			m.Step(Op{Kind: Take, Rec: 1})
		}},
		{"DLQBeforeAck/ack", "DLQBeforeAck", func(m *Model) {
			m.recs[0].dlqOpen = true
			m.recs[0].state = InFlight
			m.Step(Op{Kind: Ack, Rec: 0})
			m.recs[0].dlqOpen = false
		}},
		{"DLQBeforeAck/uncommitted", "DLQBeforeAck", func(m *Model) {
			m.recs[0].state = DeadLettered
			m.recs[0].releases = 1
		}},
		{"NoLostInFlight", "NoLostInFlight", func(m *Model) {
			m.to(Closed)
		}},
		{"ReleaseOnce/twice", "ReleaseOnce", func(m *Model) { m.recs[0].releases = 2 }},
		{"ReleaseOnce/unreleased", "ReleaseOnce", func(m *Model) {
			m.recs[0].state = Acked
		}},
		{"DrainBounded", "DrainBounded", func(m *Model) {
			m.Step(Op{Kind: Shutdown})
			m.now = m.deadline
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := closed()
			if err := m.Invariants(); err != nil {
				t.Fatalf("the fixture already violates an invariant: %v", err)
			}
			tc.fold(m)
			err := m.Invariants()
			if err == nil {
				t.Fatalf("%s did not fail on %s", tc.want, tc.name)
			}
			if !containsName(err.Error(), tc.want) {
				t.Fatalf("%s reported %q, want the %s violation", tc.name, err, tc.want)
			}
		})
	}
}

// containsName reports whether an invariant failure names the invariant.
func containsName(msg, name string) bool {
	return len(msg) > len(name) && msg[:len("shutdown: ")] == "shutdown: " &&
		msg[len("shutdown: "):len("shutdown: ")+len(name)] == name
}

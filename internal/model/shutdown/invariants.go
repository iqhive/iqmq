package shutdown

import (
	"errors"
	"fmt"
)

// errAckedBeforeDLQ reports the one invariant with nothing to say beyond its
// name: a record was acknowledged with its dead-letter publication still in
// flight.
var errAckedBeforeDLQ = errors.New(
	"shutdown: DLQBeforeAck: a record was acknowledged with its dead-letter publication in flight",
)

// Snapshot is a copy of the whole model.
type Snapshot struct {
	// Steps is the number of operations applied.
	Steps int
	// Now is the model clock.
	Now int64
	// Phase is the current phase.
	Phase Phase
	// Runs is the number of shutdown sequences started.
	Runs int
	// Graceful reports whether the run under way is a Shutdown rather than
	// a prompt Close.
	Graceful bool
	// Recs holds one entry per record slot, in index order.
	Recs []RecSnapshot
	// Waiting and Done hold one entry per closer.
	Waiting, Done []bool
	// DeadLettersPending is the number of dead-letter publications in
	// flight.
	DeadLettersPending int
	// Released is the total number of payload releases.
	Released int
}

// RecSnapshot is what one record looks like.
type RecSnapshot struct {
	// State is the record's state.
	State State
	// DLQOpen and DLQCommitted describe its dead-letter publication.
	DLQOpen, DLQCommitted bool
	// Releases counts the payload releases; the contract is exactly one
	// once the record is terminal.
	Releases int
}

// Snapshot returns a copy of the model state.
func (m *Model) Snapshot() Snapshot {
	snap := Snapshot{
		Steps: m.steps, Now: int64(m.now), Phase: m.phase, Runs: m.runs,
		Graceful:           m.graceful,
		Recs:               make([]RecSnapshot, 0, len(m.recs)),
		Waiting:            append([]bool(nil), m.waiting...),
		Done:               append([]bool(nil), m.done...),
		DeadLettersPending: m.DeadLettersPending(),
	}
	for i := range m.recs {
		snap.Recs = append(snap.Recs, RecSnapshot{
			State:   m.recs[i].state,
			DLQOpen: m.recs[i].dlqOpen, DLQCommitted: m.recs[i].dlqCommitted,
			Releases: m.recs[i].releases,
		})
		snap.Released += m.recs[i].releases
	}
	return snap
}

// Invariants checks the shutdown invariants after every step. Their names are
// the ones the TLA+ specification carries, so that tools/modelcheck can pair
// them:
//
//   - TypeOK: every record is in exactly one defined state, the phase is a
//     defined phase, no record has been released more than once, and a
//     dead-letter publication is open for a record only while that record is
//     dead-letter-pending.
//   - PhaseMonotone: the phase only ever moves forward. A handle that went
//     back to an earlier phase would re-admit operations a later one had
//     already barred, so every guarantee below would be read off a phase the
//     handle had left.
//   - CloseIdempotent: however many goroutines call Close or Shutdown, the
//     sequence runs at most once, and a caller returns only once the handle
//     is Closed. That is the specification's "idempotent, concurrency-safe Close": a
//     second Close observes the first one's outcome rather than a half-closed
//     handle.
//   - NoPublishAfterStopAccept: no publication is placed once the phase has
//     passed StopAccept. This is the engine's recurring close-race defect
//     class stated as a property: a record placed into a lane the settling
//     drain has already walked is a payload nothing will release and a
//     reservation nothing will return.
//   - NoFetchAfterStopFetch: no delivery is handed to a consumer once the
//     phase has passed StopFetch, for the same reason on the fetch side.
//   - DLQBeforeAck: a record only reaches the DeadLettered state after its
//     dead-letter publication committed, and no record is acknowledged while
//     one is in flight. A failed or ambiguous publication never settles the
//     original — the one rule ADR-011 states twice.
//   - NoLostInFlight: once the handle is Closed, no record is queued, in
//     flight or half-placed. Everything is terminal, unpublished, or waiting
//     on a dead-letter publication that still owns it.
//   - ReleaseOnce: a payload is released exactly once, and only by the
//     transition into a terminal state. A terminal record has been released;
//     a non-terminal one has not.
//   - DrainBounded: neither graceful wait — the drain that waits for the
//     in-flight handlers, nor the settling wait for the dead-letter
//     publications — is still parked once the clock has reached the drain
//     deadline. Close "never waits indefinitely", so a handler that
//     will not finish costs the shutdown the deadline and no more.
func (m *Model) Invariants() error {
	if !m.phase.valid() {
		return fmt.Errorf("shutdown: TypeOK: phase %d is not a phase", m.phase)
	}
	if m.backwards {
		return fmt.Errorf("shutdown: PhaseMonotone: the phase went back to %s from %s",
			m.phase, m.maxPhase)
	}
	if m.phase != m.maxPhase {
		return fmt.Errorf("shutdown: PhaseMonotone: the phase is %s but has been %s",
			m.phase, m.maxPhase)
	}
	if m.runs > 1 {
		return fmt.Errorf("shutdown: CloseIdempotent: the sequence ran %d times", m.runs)
	}
	for c := range m.done {
		if m.done[c] && m.phase != Closed {
			return fmt.Errorf("shutdown: CloseIdempotent: closer %d returned in phase %s", c, m.phase)
		}
		if m.done[c] && !m.waiting[c] {
			return fmt.Errorf("shutdown: CloseIdempotent: closer %d returned without asking", c)
		}
	}
	if m.publishedLate {
		return fmt.Errorf("shutdown: NoPublishAfterStopAccept: a publication was placed in phase %s", m.phase)
	}
	if m.fetchedLate {
		return fmt.Errorf("shutdown: NoFetchAfterStopFetch: a delivery was taken in phase %s", m.phase)
	}
	if m.ackedBeforeDLQ {
		return errAckedBeforeDLQ
	}
	if m.inWait() && m.graceful && m.expired() {
		return fmt.Errorf("shutdown: DrainBounded: still in %s at %v with a deadline of %v",
			m.phase, m.now, m.deadline)
	}
	for i := range m.recs {
		if err := m.recInvariants(i); err != nil {
			return err
		}
	}
	return nil
}

// recInvariants checks the per-record half.
func (m *Model) recInvariants(i int) error {
	r := &m.recs[i]
	if r.state >= stateCount {
		return fmt.Errorf("shutdown: TypeOK: record %d is in state %d", i, r.state)
	}
	if r.dlqOpen && r.state != DLQPending {
		return fmt.Errorf("shutdown: TypeOK: record %d has a publication in flight in state %s", i, r.state)
	}
	if r.releases > 1 {
		return fmt.Errorf("shutdown: ReleaseOnce: record %d was released %d times", i, r.releases)
	}
	if r.state.Terminal() != (r.releases == 1) {
		return fmt.Errorf("shutdown: ReleaseOnce: record %d is %s with %d release(s)", i, r.state, r.releases)
	}
	if r.state == DeadLettered && !r.dlqCommitted {
		return fmt.Errorf("shutdown: DLQBeforeAck: record %d was dead-lettered without a committed publication", i)
	}
	if m.phase == Closed {
		switch r.state {
		case Admitting, Held, Claiming, InFlight:
			return fmt.Errorf("shutdown: NoLostInFlight: record %d is %s after the handle closed", i, r.state)
		case Unpublished, DLQPending, Acked, DeadLettered, Dropped, stateCount:
		}
	}
	return nil
}

// valid reports whether p names a phase.
func (p Phase) valid() bool { return p < phaseCount }

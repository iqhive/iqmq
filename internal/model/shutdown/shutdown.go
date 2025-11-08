// Package shutdown is the executable reference model of graceful shutdown
// (the message contract, the specification D). It is a pure,
// single-goroutine state machine over a phase, a handful of records, the
// operations in flight against them and the dead-letter publications that
// have not resolved, driven by Step and Advance, with Invariants checked
// after every step.
//
// The model is the oracle of FuzzShutdown in internal/lifecycle: the same
// script of admissions, placements, fetches, settlements, dead-letter
// publications and closes is applied to internal/lifecycle.Lifecycle with a
// fake handle and to this model. Their agreement is checked operation by
// operation up to the first close, and not after it: from there the handle
// runs its phases on a goroutine of its own while the model runs them
// synchronously, so the two cannot be stepped together and a comparison of
// their states would be racy rather than wrong. What the fuzz target checks
// past that point is the model's own invariants and the handle's own —
// nothing placed after the drain, every record released exactly once, and a
// joined run that holds nothing and admits nothing (internal/lifecycle's
// fuzz_test.go states the same scope). The TLA+ specification is the formal
// counterpart and the trace table maps both onto the engine.
//
// # What it models and what it does not
//
// The order in which a handle stops doing things, and what that order
// guarantees about the records it holds. It deliberately does not model the
// lane structures, the leases or the settlement race: a record here is in one
// of six states and a settlement names its record rather than a generation
// token, which is internal/model/settle's business.
//
// # The two-step operations
//
// Admitting a publication and placing it are separate operations, and so are
// claiming a delivery and taking it. That is the whole point: the interval
// between them is where the engine's recurring close-race defect class lives
// (hack/PLAYBOOK.md), and a model in which a publication is atomic could not
// express it. A phase transition may not pass a barrier while an operation
// it bars is between its two steps.
package shutdown

import (
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Phase is a step of the shutdown order. The values and names match
// internal/lifecycle.Phase one for one; the model repeats them rather than
// importing the implementation, so that a differential compares two
// independent state machines.
type Phase uint8

// The phases, in the order the specification fixes.
const (
	// Running admits everything.
	Running Phase = iota
	// StopAccept has stopped admitting publications.
	StopAccept
	// StopFetch has stopped claiming deliveries.
	StopFetch
	// Flushing is completing the admitted asynchronous publications.
	Flushing
	// Draining is waiting for the in-flight handlers.
	Draining
	// Settling is settling or requeueing what the handle still holds.
	Settling
	// Committing is committing the safe boundaries.
	Committing
	// Closed has released the native resources.
	Closed

	phaseCount
)

var phaseNames = [phaseCount]string{
	"running", "stopaccept", "stopfetch", "flushing", "draining", "settling", "committing", "closed",
}

// String returns the lower-case phase name, which is the engine's slug.
func (p Phase) String() string {
	if p < phaseCount {
		return phaseNames[p]
	}
	return "phase(?)"
}

// Phases returns every phase in order.
func Phases() []Phase {
	out := make([]Phase, 0, phaseCount)
	for p := Running; p < phaseCount; p++ {
		out = append(out, p)
	}
	return out
}

// State is what one record is doing.
type State uint8

// The record states of the memory engine, reduced to what a shutdown
// distinguishes.
const (
	// Unpublished has not been published. It is the specification's "new"; the model
	// spells it out because New is the constructor.
	Unpublished State = iota
	// Admitting is a publication between its admission and its placement:
	// the handle has accepted it and it is not yet anywhere a drain would
	// find it.
	Admitting
	// Held is queued in a lane.
	Held
	// Claiming is a delivery between the claim and the hand-over.
	Claiming
	// InFlight is out with a consumer under a lease.
	InFlight
	// DLQPending is out of its lane with a dead-letter publication in
	// flight; it is neither settled nor requeued.
	DLQPending
	// Acked was settled by its consumer.
	Acked
	// DeadLettered left through a committed dead-letter publication.
	DeadLettered
	// Dropped was released by the shutdown without being acknowledged.
	Dropped

	stateCount
)

var stateNames = [stateCount]string{
	"new", "admitting", "held", "claiming", "inflight", "dlqpending", "acked", "deadlettered", "dropped",
}

// String returns the lower-case state name.
func (s State) String() string {
	if s < stateCount {
		return stateNames[s]
	}
	return "state(?)"
}

// Terminal reports whether the state is one a record never leaves and whose
// entry released the payload exactly once.
func (s State) Terminal() bool { return s == Acked || s == DeadLettered || s == Dropped }

// Kind identifies an operation.
type Kind uint8

// The operations.
const (
	// Admit accepts a publication. It is refused once the phase has passed
	// StopAccept.
	Admit Kind = iota
	// Place completes an admitted publication by putting the record in a
	// lane.
	Place
	// Claim accepts a fetch. It is refused once the phase has passed
	// StopFetch.
	Claim
	// Take completes a claim by handing the delivery to the consumer.
	Take
	// Ack settles an in-flight delivery. It is refused once the phase has
	// passed Committing.
	Ack
	// DeadLetter takes an in-flight record out of its lane and opens a
	// dead-letter publication for it.
	DeadLetter
	// CommitDeadLetter records that the publication was accepted, which is
	// the only thing that settles the original.
	CommitDeadLetter
	// FailDeadLetter records that it failed or was ambiguous, which returns
	// the record to its lane — or releases it when there is no lane left.
	FailDeadLetter
	// Close asks for the prompt shutdown every phase runs and
	// nothing is waited for.
	Close
	// Shutdown asks for the graceful one: the drain and the dead-letter
	// wait are bounded by DrainTimeout.
	Shutdown
	// Tick advances the model clock, which is what makes the drain deadline
	// reachable.
	Tick

	kindCount
)

var kindNames = [kindCount]string{
	"admit", "place", "claim", "take", "ack", "deadletter",
	"commitdeadletter", "faildeadletter", "close", "shutdown", "tick",
}

// String returns the lower-case operation name.
func (k Kind) String() string {
	if k < kindCount {
		return kindNames[k]
	}
	return "op(?)"
}

// Kinds returns every operation in declaration order.
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount)
	for k := Admit; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Config configures the model to match the handle under test.
type Config struct {
	// Records is the number of record slots the model tracks.
	Records int
	// Closers is the number of goroutines that may call Close or Shutdown.
	Closers int
	// DrainTimeout bounds the graceful drain and the dead-letter wait. Zero
	// means DefaultDrainTimeout; a negative value means no bound.
	DrainTimeout time.Duration
}

// DefaultDrainTimeout is the default.
const DefaultDrainTimeout = 30 * time.Second

// Op is one operation applied by Step.
type Op struct {
	// Kind selects the operation.
	Kind Kind
	// Rec identifies the record for every record operation.
	Rec int
	// Closer identifies the goroutine for Close and Shutdown.
	Closer int
	// Delay is how far Tick moves the clock.
	Delay time.Duration
}

// Result is what Step returns; it is what the handle must produce.
type Result struct {
	// Err is the error the handle must return, or nil.
	Err error
	// OK reports whether the operation was admitted.
	OK bool
	// Phase is the phase after the step.
	Phase Phase
	// State is the state of the record the step named.
	State State
	// Released reports that the step released a payload.
	Released bool
}

// rec is one record.
type rec struct {
	state State
	// dlqOpen reports that a dead-letter publication is in flight.
	dlqOpen bool
	// dlqCommitted reports that one committed, which is the only thing
	// that may put the record in the DeadLettered state.
	dlqCommitted bool
	// releases counts the payload releases the record has seen; the
	// contract is exactly one.
	releases int
	// admittedAt and claimedAt are the phases the two-step operations
	// started in, which is what NoPublishAfterStopAccept and
	// NoFetchAfterStopFetch are read from.
	admittedAt Phase
	claimedAt  Phase
}

// Model is the reference model of one handle's shutdown.
type Model struct {
	cfg  Config
	recs []rec

	phase Phase
	// maxPhase is the furthest the phase has ever been, which is what
	// PhaseMonotone compares against.
	maxPhase Phase
	// backwards is set if the phase ever went back, which nothing may do.
	backwards bool

	// runs counts the shutdown sequences started; the contract is at most
	// one however many closers arrive.
	runs int
	// waiting are the closers that have asked and not yet returned; done
	// are the ones that have.
	waiting  []bool
	done     []bool
	graceful bool
	// deadline is when the graceful waits stop, in model time.
	deadline time.Duration
	// bounded reports that a deadline applies at all.
	bounded bool

	now   time.Duration
	steps int

	// violations record the ordering faults the invariants report. They are
	// set by the transitions rather than derived, because the fault is an
	// event: a publication that landed after the barrier leaves a record
	// that looks exactly like one that landed before it.
	publishedLate, fetchedLate, ackedBeforeDLQ bool
}

// New returns a model in the Running phase with cfg.Records unpublished
// records.
func New(cfg Config) *Model {
	if cfg.Records < 0 {
		cfg.Records = 0
	}
	if cfg.Closers < 1 {
		cfg.Closers = 1
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	return &Model{
		cfg:     cfg,
		recs:    make([]rec, cfg.Records),
		waiting: make([]bool, cfg.Closers),
		done:    make([]bool, cfg.Closers),
	}
}

// Phase returns the current phase.
func (m *Model) Phase() Phase { return m.phase }

// Steps returns the number of operations applied.
func (m *Model) Steps() int { return m.steps }

// Now returns the model clock.
func (m *Model) Now() time.Duration { return m.now }

// Closers returns the number of closers the model tracks.
func (m *Model) Closers() int { return m.cfg.Closers }

// Records returns the number of records the model tracks.
func (m *Model) Records() int { return len(m.recs) }

// State returns the state of record i, or New for an index the model does
// not have.
func (m *Model) State(i int) State {
	if i < 0 || i >= len(m.recs) {
		return Unpublished
	}
	return m.recs[i].state
}

// Releases returns how many times record i's payload has been released. The
// contract is at most one, and exactly one once the record is terminal.
func (m *Model) Releases(i int) int {
	if i < 0 || i >= len(m.recs) {
		return 0
	}
	return m.recs[i].releases
}

// Done reports whether closer c has returned from its Close or Shutdown.
func (m *Model) Done(c int) bool { return c >= 0 && c < len(m.done) && m.done[c] }

// Runs returns the number of shutdown sequences started.
func (m *Model) Runs() int { return m.runs }

// DeadLettersPending returns the number of dead-letter publications in
// flight.
func (m *Model) DeadLettersPending() int {
	n := 0
	for i := range m.recs {
		if m.recs[i].dlqOpen {
			n++
		}
	}
	return n
}

// Advance moves the model clock and lets a shutdown that was waiting for the
// deadline continue.
func (m *Model) Advance(d time.Duration) {
	if d > 0 {
		m.now += d
	}
	m.progress()
}

// Step applies op and returns the result the handle must produce.
func (m *Model) Step(op Op) Result {
	m.steps++
	var r Result
	switch op.Kind {
	case Admit:
		r = m.admit(op.Rec)
	case Place:
		r = m.place(op.Rec)
	case Claim:
		r = m.claim(op.Rec)
	case Take:
		r = m.take(op.Rec)
	case Ack:
		r = m.ack(op.Rec)
	case DeadLetter:
		r = m.deadLetter(op.Rec)
	case CommitDeadLetter:
		r = m.resolveDeadLetter(op.Rec, true)
	case FailDeadLetter:
		r = m.resolveDeadLetter(op.Rec, false)
	case Close:
		r = m.startShutdown(op.Closer, false)
	case Shutdown:
		r = m.startShutdown(op.Closer, true)
	case Tick:
		m.Advance(op.Delay)
		r = Result{OK: true}
	default:
		panic("shutdown: undefined operation " + op.Kind.String())
	}
	m.progress()
	r.Phase = m.phase
	r.State = m.State(op.Rec)
	return r
}

// valid reports whether i names a record.
func (m *Model) valid(i int) bool { return i >= 0 && i < len(m.recs) }

// admit accepts a publication, which the handle does only while running.
func (m *Model) admit(i int) Result {
	if !m.valid(i) || m.recs[i].state != Unpublished {
		return Result{Err: errcode.ErrNotSettleable}
	}
	if m.phase != Running {
		return Result{Err: errcode.ErrClosed}
	}
	m.recs[i].state = Admitting
	m.recs[i].admittedAt = m.phase
	return Result{OK: true}
}

// place completes an admitted publication.
//
// A placement that lands once the phase has passed StopAccept is the defect
// class this whole model exists for: the record is in a lane the settling
// drain has already walked, so nothing will ever release it. It is recorded
// rather than refused, because refusing it here would be assuming the
// property NoPublishAfterStopAccept is meant to check.
func (m *Model) place(i int) Result {
	if !m.valid(i) || m.recs[i].state != Admitting {
		return Result{Err: errcode.ErrNotSettleable}
	}
	if m.phase > StopAccept {
		m.publishedLate = true
	}
	m.recs[i].state = Held
	return Result{OK: true}
}

// claim accepts a fetch, which the handle does until it stops fetching.
func (m *Model) claim(i int) Result {
	if !m.valid(i) || m.recs[i].state != Held {
		return Result{Err: errcode.ErrNotSettleable}
	}
	if m.phase >= StopFetch {
		return Result{Err: errcode.ErrClosed}
	}
	m.recs[i].state = Claiming
	m.recs[i].claimedAt = m.phase
	return Result{OK: true}
}

// take completes a claim by leasing the record to a consumer.
func (m *Model) take(i int) Result {
	if !m.valid(i) || m.recs[i].state != Claiming {
		return Result{Err: errcode.ErrNotSettleable}
	}
	if m.phase > StopFetch {
		m.fetchedLate = true
	}
	m.recs[i].state = InFlight
	return Result{OK: true}
}

// ack settles an in-flight delivery.
func (m *Model) ack(i int) Result {
	if !m.valid(i) || m.recs[i].state != InFlight {
		return Result{Err: errcode.ErrNotSettleable}
	}
	if m.phase >= Committing {
		return Result{Err: errcode.ErrClosed}
	}
	if m.recs[i].dlqOpen {
		// A record whose dead-letter publication is in flight is out of the
		// in-flight list, so this cannot happen; recording it is what gives
		// DLQBeforeAck something to fail on if it ever does.
		m.ackedBeforeDLQ = true
	}
	m.recs[i].state = Acked
	m.release(i)
	return Result{OK: true, Released: true}
}

// deadLetter opens a dead-letter publication for an in-flight record.
func (m *Model) deadLetter(i int) Result {
	if !m.valid(i) || m.recs[i].state != InFlight || m.recs[i].dlqOpen {
		return Result{Err: errcode.ErrNotSettleable}
	}
	if m.phase >= Committing {
		return Result{Err: errcode.ErrClosed}
	}
	m.recs[i].state = DLQPending
	m.recs[i].dlqOpen = true
	return Result{OK: true}
}

// resolveDeadLetter commits or fails a dead-letter publication.
//
// Only a committed publication settles the original — the one rule ADR-011
// states twice. A failed one puts the record back in its lane, or releases
// it when the shutdown has already drained the lanes: there is nothing left
// to put it back into, and leaving it unreleased would leak the reservation.
func (m *Model) resolveDeadLetter(i int, commit bool) Result {
	if !m.valid(i) || m.recs[i].state != DLQPending || !m.recs[i].dlqOpen {
		return Result{Err: errcode.ErrNotSettleable}
	}
	m.recs[i].dlqOpen = false
	if commit {
		m.recs[i].dlqCommitted = true
		m.recs[i].state = DeadLettered
		m.release(i)
		return Result{OK: true, Released: true}
	}
	if m.phase >= Settling {
		m.recs[i].state = Dropped
		m.release(i)
		return Result{OK: true, Released: true}
	}
	m.recs[i].state = Held
	return Result{OK: true}
}

// release runs the payload's release callback once.
func (m *Model) release(i int) { m.recs[i].releases++ }

// startShutdown registers a closer and starts the sequence if none is
// running. Every later caller — of either Close or Shutdown — joins the run
// that is already under way rather than starting a second one.
func (m *Model) startShutdown(c int, graceful bool) Result {
	if c < 0 || c >= len(m.waiting) {
		return Result{Err: errcode.ErrNotSettleable}
	}
	m.waiting[c] = true
	if m.runs == 0 {
		m.runs++
		m.graceful = graceful
		m.bounded = graceful && m.cfg.DrainTimeout >= 0
		m.deadline = m.now + m.cfg.DrainTimeout
		m.to(StopAccept)
	}
	return Result{OK: true}
}

// progress advances the shutdown as far as the current state allows. It is
// called after every operation and every clock advance, because both can be
// what a phase was waiting for.
func (m *Model) progress() {
	if m.runs == 0 {
		return
	}
	for {
		next, ok := m.next()
		if !ok {
			return
		}
		m.to(next)
	}
}

// next returns the phase the shutdown may move to, and whether it may.
func (m *Model) next() (Phase, bool) {
	switch m.phase {
	case StopAccept:
		// The barrier: no publication may be between its admission and its
		// placement. This is the wait that makes the settling drain safe.
		if m.countState(Admitting) > 0 {
			return 0, false
		}
		return StopFetch, true
	case StopFetch:
		if m.countState(Claiming) > 0 {
			return 0, false
		}
		return Flushing, true
	case Flushing:
		return Draining, true
	case Draining:
		// The graceful drain waits for the in-flight handlers, bounded by
		// the deadline; a prompt Close waits for nothing.
		if m.graceful && m.countState(InFlight) > 0 && !m.expired() {
			return 0, false
		}
		return Settling, true
	case Committing:
		return Closed, true
	case Settling:
		// Settling waits for the dead-letter publications, because a record
		// whose publication is in flight is neither settled nor in a lane;
		// then it drops everything the lanes still hold.
		if m.graceful && m.DeadLettersPending() > 0 && !m.expired() {
			return 0, false
		}
		m.dropAll()
		return Committing, true
	default:
		return 0, false
	}
}

// expired reports whether the graceful deadline has passed.
func (m *Model) expired() bool { return m.bounded && m.now >= m.deadline }

// waiting reports whether the model is parked in one of the two phases that
// wait — the drain and the dead-letter wait of the settling phase. It is what
// DrainBounded is read from: a bounded wait may not still be parked once the
// clock has reached its deadline.
func (m *Model) inWait() bool { return m.phase == Draining || m.phase == Settling }

// to moves the phase, recording the fault if it ever goes backwards and
// releasing the closers once the last phase is entered.
func (m *Model) to(p Phase) {
	if p < m.phase {
		m.backwards = true
	}
	m.phase = p
	if p > m.maxPhase {
		m.maxPhase = p
	}
	if p == Closed {
		for c := range m.waiting {
			if m.waiting[c] {
				m.done[c] = true
			}
		}
	}
}

// dropAll releases every record the lanes still hold. A record with a
// dead-letter publication in flight is not in a lane and is left alone: its
// publication still decides its fate.
func (m *Model) dropAll() {
	for i := range m.recs {
		switch m.recs[i].state {
		case Held, InFlight:
			m.recs[i].state = Dropped
			m.release(i)
		case Unpublished, Admitting, Claiming, DLQPending, Acked, DeadLettered, Dropped, stateCount:
		}
	}
}

// countState returns the number of records in state s.
func (m *Model) countState(s State) int {
	n := 0
	for i := range m.recs {
		if m.recs[i].state == s {
			n++
		}
	}
	return n
}

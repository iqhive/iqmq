// Package lifecycle implements the shutdown state machine that
// the message contract prescribes for every iqmq handle, and that the
// in-memory engine's Close and Shutdown are built on.
//
// # Phases
//
// The specification fixes the order a handle shuts down in: stop accepting publications,
// stop fetching, flush the asynchronous publications already admitted, wait
// for the in-flight handlers within DrainTimeout, settle or requeue what is
// left, commit the safe boundaries, and only then close the native clients.
// Each step is a Phase, phases only ever move forward, and a Lifecycle runs
// them once however many goroutines call Close.
//
// # The admission gates
//
// A phase does not merely record that a step has happened: it closes the door
// on the operations the step invalidates. Every operation that puts a record
// somewhere the handle will hold it — a lane, a store, a retained log, a
// dead-letter queue — brackets that placement between Enter and Leave, and a
// phase transition bars the matching gate and then waits for every admitted
// operation to leave before the step that drains runs.
//
// That ordering is the whole point. Checking a closed flag and then placing
// is not enough: the flag can be set, the drain can run, and the placement
// can land afterwards, leaving a record nothing will ever release. Barring
// the gate first and waiting for it to empty makes such a placement
// impossible rather than unlikely — a record is either placed before the
// drain, or refused.
//
// the specification's "never waits indefinitely" applies to the phases and not only to
// the steps between them, and the two are reconciled by separating the run
// from the caller: a barrier never abandons a placement, because a placement
// that lands behind the drain is the fault this package exists to prevent,
// but a graceful Shutdown whose context or drain deadline expires while a
// barrier is still waiting returns to its caller and reports that bound. The
// run goes on, waits out the placement and drains it. A prompt Close waits
// for the run, which is prompt because a placement is bounded work: no
// callback of the caller's runs between Enter and Leave. A handle whose
// placement is a broker round trip is bounded only by the native client, and
// the Interrupt step is how a barrier waiting for one releases it.
//
// # Dead-letter ordering
//
// A record that has used up its attempts is dead-letter-pending in the sense
// it may not reach a terminal state until its dead-letter
// publication is committed. BeginDeadLetter opens such a publication and
// Pending.SettleAllowed reports whether the original may be settled yet; the
// Settling phase waits for every open publication, so a shutdown never
// acknowledges a record whose dead-letter publication it abandoned.
package lifecycle

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DefaultDrainTimeout is the default for the graceful wait on in-flight
// handlers.
const DefaultDrainTimeout = 30 * time.Second

// ErrDrainTimeout reports that the drain deadline passed with work still
// outstanding. Shutdown returns it and stops waiting; the run itself carries
// on through every remaining phase, so the handle still closes and still
// drains what it holds — Done is what says it has.
var ErrDrainTimeout = errcode.New(errcode.DeadlineExceeded, "iqmq/lifecycle: drain deadline exceeded")

// Phase is a step of the shutdown order. Phases are totally ordered and
// a Lifecycle only ever moves forward through them.
type Phase uint8

// The phases, in the order the specification fixes.
const (
	// Running admits everything: the handle is open.
	Running Phase = iota
	// StopAccept has stopped admitting publications and attachments.
	StopAccept
	// StopFetch has stopped claiming deliveries.
	StopFetch
	// Flushing is completing the asynchronous publications already admitted.
	Flushing
	// Draining is waiting for the in-flight handlers, bounded by the drain
	// deadline.
	Draining
	// Settling is settling, requeueing or expiring what the handle still
	// holds, per policy.
	Settling
	// Committing is committing the safe boundaries.
	Committing
	// Closed has released the native resources. It is terminal.
	Closed

	phaseCount
)

var phaseSlugs = [phaseCount]string{
	"running", "stopaccept", "stopfetch", "flushing", "draining", "settling", "committing", "closed",
}

var phaseIdents = [phaseCount]string{
	"Running", "StopAccept", "StopFetch", "Flushing", "Draining", "Settling", "Committing", "Closed",
}

// String returns the phase's slug, as used in traces and model states.
func (p Phase) String() string {
	if p < phaseCount {
		return phaseSlugs[p]
	}
	return "phase(" + itoa(int(p)) + ")"
}

// Ident returns the phase's Go identifier.
func (p Phase) Ident() string {
	if p < phaseCount {
		return phaseIdents[p]
	}
	return ""
}

// Valid reports whether p names a phase.
func (p Phase) Valid() bool { return p < phaseCount }

// Phases lists every phase in order.
func Phases() []Phase {
	out := make([]Phase, 0, phaseCount)
	for p := Running; p < phaseCount; p++ {
		out = append(out, p)
	}
	return out
}

// Op is a class of operation the phases gate.
type Op uint8

// The gated operation classes. Each is barred when the phase named in
// Barrier is entered, so an operation admitted while the gate was open has
// completed before the step that phase performs.
const (
	// OpPublish covers every route that admits a new record: a publication,
	// an attachment, a retained-log append. Barred at StopAccept.
	OpPublish Op = iota
	// OpFetch covers claiming a delivery, and the redelivery routes that
	// only exist to feed one: promotion, lease expiry, replay. Barred at
	// StopFetch.
	OpFetch
	// OpSettle covers the dead-letter publication a settlement makes.
	// Barred at Committing, so a handler still running during Draining can
	// still open one.
	//
	// It does not cover an acknowledgement or a requeue: the engine's
	// settlement cell already admits exactly one outcome per delivery, and
	// the record a requeue puts back is one the settling drain walks
	// (adr/ADR-017-assumptions.md). Widening this class to
	// them would be a ticket per settlement on the fetch path, for a race
	// the cell already decides.
	OpSettle

	opCount
)

var opSlugs = [opCount]string{"publish", "fetch", "settle"}

var opIdents = [opCount]string{"OpPublish", "OpFetch", "OpSettle"}

// String returns the op's slug.
func (o Op) String() string {
	if o < opCount {
		return opSlugs[o]
	}
	return "op(" + itoa(int(o)) + ")"
}

// Ident returns the op's Go identifier.
func (o Op) Ident() string {
	if o < opCount {
		return opIdents[o]
	}
	return ""
}

// Ops lists every gated operation class.
func Ops() []Op {
	out := make([]Op, 0, opCount)
	for o := OpPublish; o < opCount; o++ {
		out = append(out, o)
	}
	return out
}

// Barrier returns the phase at which o stops being admitted. An operation is
// admitted while Phase() < o.Barrier().
func (o Op) Barrier() Phase {
	switch o {
	case OpPublish:
		return StopAccept
	case OpFetch:
		return StopFetch
	case OpSettle:
		return Committing
	default:
		return Running
	}
}

// Steps are the handle's own shutdown actions, one per phase. A nil step is
// a phase the handle has nothing to do in; the phase is still entered, and
// its gate still barred and drained, so the ordering holds either way.
//
// A step runs after the phase has been entered and after every operation
// admitted under the previous phase has left its gate.
//
// A step must not call Close or Shutdown, and must not wait for a goroutine
// that may: both wait for the run the step is part of, which cannot finish
// until the step returns. That includes code a step runs on someone else's
// behalf. A publisher's CloseNative delivers the callbacks of the publications
// it abandons, and a callback that closes its own publisher, inline or from a
// deferred completion goroutine the step waits for, never returns.
type Steps struct {
	// StopAccept runs once no further publication can be admitted.
	StopAccept func()
	// StopFetch runs once no further delivery can be claimed.
	StopFetch func()
	// Flush completes the asynchronous publications already admitted. It is
	// bounded by the drain deadline and may return early on ctx.
	Flush func(ctx context.Context) error
	// Drain waits for the in-flight handlers. It is bounded by the drain
	// deadline and must return promptly when ctx is done. Close (deadline
	// zero) does not call it at all.
	Drain func(ctx context.Context) error
	// Settle settles, requeues or expires whatever the handle still holds.
	// It runs with every publication, fetch and in-flight placement
	// already excluded, which is what makes it safe to drain the lanes.
	Settle func()
	// Commit commits the safe boundaries.
	Commit func() error
	// CloseNative releases the native resources. It runs last and always
	// runs, whatever the earlier steps returned.
	CloseNative func() error
	// Interrupt releases the admitted operations parked in the native
	// resources, for a handle whose placement is a round trip a broker may
	// never answer: a barrier waiting for such an operation would otherwise
	// wait for ever, ahead of the CloseNative that could end it. A prompt
	// Close calls it when a barrier finds an admitted operation still
	// inside; a graceful Shutdown when one is still inside once its bound
	// has expired. It runs at most once, concurrently with the operations it
	// releases, and must not wait for them.
	Interrupt func()
}

// Config configures a Lifecycle.
type Config struct {
	// Clock is the time source the drain deadline is measured on. Nil means
	// clock.Real, which follows the bubble clock under testing/synctest.
	Clock clock.Clock
	// DrainTimeout bounds Shutdown's Flush, Drain and Settling waits.
	// Zero means DefaultDrainTimeout; a negative value means no bound.
	DrainTimeout time.Duration
	// Steps are the handle's own actions.
	Steps Steps
	// Shards is the number of gate counter cells. Zero means GOMAXPROCS
	// rounded up to a power of two, capped at 64.
	Shards int
}

// Lifecycle runs the shutdown order once, whatever number of goroutines
// ask it to, and gates the operations each phase invalidates. The zero value
// is not usable: call New.
type Lifecycle struct {
	clock clock.Clock
	drain time.Duration
	steps Steps

	// phase is the current Phase. It is written only under mu and only
	// upwards, and read without mu on every gated operation.
	phase atomic.Uint32

	gates [opCount]gate

	// dlq counts the dead-letter publications that have neither committed
	// nor aborted. Settling waits for it to reach zero.
	dlq     atomic.Int64
	dlqWake chan struct{}

	// stuck is closed when a phase barrier is still waiting for an admitted
	// operation after the bound the *run* was given has expired. The barrier
	// keeps waiting — abandoning it would let the placement land behind the
	// drain — but a graceful caller stops waiting for the run once its own
	// bound has passed as well. It is closed at most once, and it says
	// nothing about the bound of a caller that arrived after it closed,
	// which is why Lifecycle.run waits for that caller's own (Shutdown).
	stuck     chan struct{}
	stuckOnce sync.Once

	// interruptOnce runs Steps.Interrupt at most once.
	interruptOnce sync.Once

	once sync.Once
	done chan struct{}
	err  error
}

// New builds a Lifecycle in the Running phase.
func New(cfg Config) *Lifecycle {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = DefaultDrainTimeout
	}
	shards := cfg.Shards
	if shards <= 0 {
		shards = runtime.GOMAXPROCS(0)
	}
	if shards > maxShards {
		shards = maxShards
	}
	l := &Lifecycle{
		clock:   cfg.Clock,
		drain:   cfg.DrainTimeout,
		steps:   cfg.Steps,
		done:    make(chan struct{}),
		stuck:   make(chan struct{}),
		dlqWake: make(chan struct{}, 1),
	}
	for i := range l.gates {
		l.gates[i].init(shards)
	}
	return l
}

// Phase returns the current phase.
func (l *Lifecycle) Phase() Phase { return Phase(l.phase.Load()) }

// Closing reports whether Close or Shutdown has been called, which is what a
// handle's Closed() answers: an operation refused in any phase but Running is
// refused because the handle is closing.
func (l *Lifecycle) Closing() bool { return l.phase.Load() != uint32(Running) }

// Done returns a channel closed once the last phase has run. It is closed
// exactly once, by the goroutine that ran the phases.
func (l *Lifecycle) Done() <-chan struct{} { return l.done }

// Allow reports whether op would be admitted now. It is a hint: only Enter
// admits, and only Enter keeps the phase from moving past the placement.
func (l *Lifecycle) Allow(op Op) bool { return Phase(l.phase.Load()) < op.Barrier() }

// Ticket is what Enter returns and Leave gives back. Its zero value is not a
// ticket; do not copy one after Enter.
type Ticket struct {
	op   Op
	cell uint32
	ok   bool
}

// OK reports whether the ticket admits its operation.
func (t Ticket) OK() bool { return t.ok }

// Enter admits one operation of class op and returns a ticket that must be
// given back with Leave. It reports false — a ticket that Leave ignores —
// once the phase has passed op's barrier.
//
// The section between Enter and Leave must not block: a phase transition
// waits for every admitted operation to leave before it drains, so an
// operation that parks inside the gate parks the shutdown with it. Park
// outside the gate and take a ticket for the placement itself.
func (l *Lifecycle) Enter(op Op) Ticket {
	if op >= opCount {
		return Ticket{}
	}
	g := &l.gates[op]
	cell := g.acquire()
	if g.barred.Load() {
		// The phase moved between the acquire and this load, so this
		// operation is on the far side of the barrier: give the count back
		// before the waiting phase can see it and refuse.
		g.release(cell)
		return Ticket{op: op}
	}
	return Ticket{op: op, cell: cell, ok: true}
}

// Leave returns a ticket. Leaving a ticket Enter refused is a no-op, so a
// caller may defer Leave unconditionally.
func (l *Lifecycle) Leave(t Ticket) {
	if !t.ok {
		return
	}
	l.gates[t.op].release(t.cell)
}

// Close shuts the handle down promptly: it runs every phase without waiting
// for in-flight handlers (the Drain step is skipped) and unblocks whatever is
// parked. It is idempotent and safe for concurrent use; every caller returns
// the same error.
func (l *Lifecycle) Close() error { return l.run(context.Background(), false) }

// Shutdown shuts the handle down gracefully: the Flush, Drain and Settling
// waits are bounded by the drain deadline and by ctx, whichever comes first,
// and so is Shutdown's own wait for the run. It is the same run as Close —
// the first of the two to arrive performs it, and the other waits for it and
// returns its error.
//
// A caller whose bound expires while a phase barrier is still waiting for an
// admitted operation returns ErrDrainTimeout, or ctx's error when ctx is what
// ended the wait, rather than the run's outcome; the run itself continues and
// still drains everything it holds.
func (l *Lifecycle) Shutdown(ctx context.Context) error { return l.run(ctx, true) }

// run performs the phases once. Concurrent and later callers block on done
// and return the same error, which is what makes Close idempotent in the
// sense the specification asks for: a second Close observes the first one's outcome
// rather than a half-closed handle.
func (l *Lifecycle) run(ctx context.Context, graceful bool) error {
	l.once.Do(func() {
		go func() {
			l.err = l.phases(ctx, graceful)
			close(l.done)
		}()
	})
	if !graceful {
		// A prompt Close waits for the run. Every wait a barrier makes is for
		// a placement, and a placement is bounded work — no callback of the
		// caller's runs between Enter and Leave — so the specification's "Close never
		// waits indefinitely" is a property of the sections, not of a
		// deadline laid over them. The steps are the handle's own, and the
		// run waits for them too: see Steps for the one call they must not
		// make.
		<-l.done
		return l.err
	}
	// The report says a barrier is waiting past the *run's* bound. This
	// caller has a bound of its own — its context, cut short by the drain
	// deadline measured from its own arrival — and it is that one that ends
	// its wait. The two are the same instant for the caller that started the
	// run, and they are not for any later one: the report is a one-shot for
	// the life of the handle, so a second Shutdown carrying a context of its
	// own and no deadline used to be told the drain deadline had been
	// exceeded without having waited at all.
	bctx, release := l.bounded(ctx, true)
	defer release()
	select {
	case <-l.done:
		return l.err
	case <-l.stuck:
		select {
		case <-l.done:
			return l.err
		case <-bctx.Done():
			// Both cases can be ready at once — a run that finished as the
			// bound passed — and select picks at random between them. A
			// finished run wins: there is nothing left for this caller to
			// wait for, so it is told the outcome rather than a deadline
			// that no longer bounds anything.
			select {
			case <-l.done:
				return l.err
			default:
			}
			// A barrier outlived this shutdown's bound. The run goes on — it
			// still waits for the operation and still drains what the
			// operation places, so nothing is lost — and the caller is told
			// which of its own bounds ended its wait, exactly as a drain
			// that timed out is.
			if err := ctx.Err(); err != nil {
				return err
			}
			return ErrDrainTimeout
		}
	}
}

// phases walks the order. Every step runs, whatever the previous one
// returned: a handle that failed to flush must still close its natives.
func (l *Lifecycle) phases(ctx context.Context, graceful bool) error {
	var errs [4]error
	bctx, release := l.bounded(ctx, graceful)
	defer release()

	// The barriers are told the bound the graceful waits are under, so that
	// one still waiting when it expires reports itself. It matters because
	// enter(StopAccept) runs before any step does: a barrier that said
	// nothing would make ctx and DrainTimeout govern nothing at all until
	// the first step ran. The barrier still waits — see gate.wait — and it
	// is Shutdown's caller, not the run, that stops waiting.
	//
	// A prompt Close has no deadline to report against, and its barriers
	// do not wait for an admitted operation before interrupting it: a bound
	// that has already passed is what makes the first wait that finds one
	// call Interrupt. Without an Interrupt step it passes nil.
	var bound <-chan struct{}
	cut := l.interrupt
	switch {
	case graceful:
		bound = bctx.Done()
		cut = func() { l.reportStuck(); l.interrupt() }
	case l.steps.Interrupt != nil:
		bound = passed
	}

	l.enter(StopAccept, bound, cut)
	if l.steps.StopAccept != nil {
		l.steps.StopAccept()
	}

	l.enter(StopFetch, bound, cut)
	if l.steps.StopFetch != nil {
		l.steps.StopFetch()
	}

	// A prompt Close skips the two waiting steps outright rather than
	// calling them with a deadline that has already passed: Close is
	// the one that does not wait, and Shutdown is the one that does. A step
	// that is skipped still has its phase entered, so an observer sees the
	// same sequence either way.
	l.enter(Flushing, bound, cut)
	if graceful && l.steps.Flush != nil {
		errs[0] = l.timeout(ctx, bctx, l.steps.Flush(bctx))
	}

	l.enter(Draining, bound, cut)
	if graceful && l.steps.Drain != nil {
		errs[1] = l.timeout(ctx, bctx, l.steps.Drain(bctx))
	}

	l.enter(Settling, bound, cut)
	// A dead-letter publication in flight holds a record that is neither
	// settled nor back in its lane; waiting for it here is what keeps
	// "accepted before acknowledged" true through a graceful shutdown.
	if graceful {
		errs[2] = l.timeout(ctx, bctx, l.waitDeadLetters(bctx))
	}
	if l.steps.Settle != nil {
		l.steps.Settle()
	}

	l.enter(Committing, bound, cut)
	if l.steps.Commit != nil {
		errs[3] = l.steps.Commit()
	}

	l.enter(Closed, bound, cut)
	var closeErr error
	if l.steps.CloseNative != nil {
		closeErr = l.steps.CloseNative()
	}
	return errors.Join(errs[0], errs[1], errs[2], errs[3], closeErr)
}

// enter advances to p, bars the gates whose barrier is p and waits for every
// operation admitted through them to leave. bound is the graceful shutdown's
// deadline: a barrier still waiting when it expires calls cut — it does not
// stop waiting.
//
// The wait is what makes the phase mean something: once enter returns, no
// operation of a barred class is running and none can start, so the step that
// follows may drain what the handle holds without racing a placement.
func (l *Lifecycle) enter(p Phase, bound <-chan struct{}, cut func()) {
	l.phase.Store(uint32(p))
	for op := OpPublish; op < opCount; op++ {
		if op.Barrier() != p {
			continue
		}
		g := &l.gates[op]
		g.bar()
		g.wait(bound, cut)
	}
}

// reportStuck releases whoever is waiting on a graceful Shutdown when a
// barrier is still waiting past that shutdown's bound.
func (l *Lifecycle) reportStuck() { l.stuckOnce.Do(func() { close(l.stuck) }) }

// interrupt runs the Interrupt step, once.
func (l *Lifecycle) interrupt() {
	if l.steps.Interrupt != nil {
		l.interruptOnce.Do(l.steps.Interrupt)
	}
}

// passed is a bound that has already expired.
var passed = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// bounded derives the context the waiting steps run under: the caller's, cut
// short by the drain deadline. It is built from the injected clock rather
// than context.WithTimeout so that a shutdown inside a testing/synctest
// bubble is driven by the bubble clock like everything else.
func (l *Lifecycle) bounded(ctx context.Context, graceful bool) (context.Context, func()) {
	if !graceful || l.drain < 0 {
		return ctx, func() {}
	}
	bctx, cancel := context.WithCancel(ctx)
	timer := l.clock.NewTimer(l.drain)
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-timer.C():
			cancel()
		case <-bctx.Done():
			timer.Stop()
		}
	}()
	return bctx, func() { cancel(); <-done }
}

// timeout names the deadline that actually ended a step. A step that stopped
// because the drain deadline passed reports ErrDrainTimeout; one that stopped
// because the caller's own context ended reports that context's error, which
// is what a caller who cancelled needs to see.
func (l *Lifecycle) timeout(ctx, bctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() == nil && bctx.Err() != nil {
		return ErrDrainTimeout
	}
	return err
}

// waitDeadLetters blocks until every open dead-letter publication has
// committed or aborted, or until the drain deadline or the caller's context
// ends the wait. A prompt Close does not wait at all: the publications it
// leaves open are still counted by DeadLettersPending, and their records are
// the ones the settling drain returns to no lane — unsettled, which is the
// conservative half of the ordering rule.
func (l *Lifecycle) waitDeadLetters(ctx context.Context) error {
	for l.dlq.Load() > 0 {
		select {
		case <-l.dlqWake:
		case <-ctx.Done():
			if ctx.Err() != nil && l.dlq.Load() > 0 {
				// The caller's own cancellation and the drain deadline are
				// told apart by Lifecycle.timeout, which is why this names
				// the context's error rather than the deadline's: the
				// deadline may not have passed at all.
				return ctx.Err()
			}
			return nil
		}
	}
	return nil
}

// DeadLettersPending returns the number of dead-letter publications that have
// neither committed nor aborted.
func (l *Lifecycle) DeadLettersPending() int { return int(l.dlq.Load()) }

// Pending is one dead-letter publication in flight. The record it belongs to
// is dead-letter-pending in the sense of the memory engine: it is out of its
// lane, out of the in-flight list, and may not reach a terminal state until
// SettleAllowed reports true.
type Pending struct {
	l     *Lifecycle
	state atomic.Uint32
}

// The states of a Pending.
const (
	pendingOpen uint32 = iota
	pendingCommitted
	pendingAborted
)

// BeginDeadLetter opens a dead-letter publication. It returns nil once the
// phase has passed OpSettle's barrier, which is the point after which no
// record may leave a lane at all.
func (l *Lifecycle) BeginDeadLetter() *Pending {
	tk := l.Enter(OpSettle)
	if !tk.OK() {
		return nil
	}
	defer l.Leave(tk)
	l.dlq.Add(1)
	return &Pending{l: l}
}

// Commit records that the dead-letter publication was accepted. Only then may
// the original record be settled.
func (p *Pending) Commit() { p.resolve(pendingCommitted) }

// Abort records that the dead-letter publication failed or was ambiguous. The
// original record must go back to its lane: ADR-011 states that rule twice.
func (p *Pending) Abort() { p.resolve(pendingAborted) }

// SettleAllowed reports whether the record this publication belongs to may
// reach a terminal state — that is, whether the dead-letter publication has
// committed. It is the ordering hook itself: a settlement that consults it
// cannot overtake the publication.
func (p *Pending) SettleAllowed() bool { return p.state.Load() == pendingCommitted }

// Resolved reports whether the publication has committed or aborted.
func (p *Pending) Resolved() bool { return p.state.Load() != pendingOpen }

// resolve moves the publication out of the open state exactly once and wakes
// a Settling phase waiting for it.
func (p *Pending) resolve(to uint32) {
	if !p.state.CompareAndSwap(pendingOpen, to) {
		return
	}
	if p.l.dlq.Add(-1) == 0 {
		select {
		case p.l.dlqWake <- struct{}{}:
		default:
		}
	}
}

// itoa renders a small non-negative int without importing strconv, which the
// hot-path packages that embed a phase in a message would otherwise pull in.
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

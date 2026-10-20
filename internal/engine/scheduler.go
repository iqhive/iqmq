package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/timer"
)

// maxFireBatch bounds the work one wake-up of the scheduler does before it
// looks at the clock again, so that a mass expiry cannot hold the scheduler
// in one pass for an unbounded time (the memory engine: callbacks do bounded
// work and hand off large batches).
const maxFireBatch = 256

// minRearm is how far ahead a lane is re-armed when the time it reports has
// already passed, which keeps a lost race from spinning the scheduler.
const minRearm = time.Millisecond

// scheduler is the engine's single timing goroutine (the memory engine):
// delayed publication and delayed requeue, records that outlive their time to
// live, and the lease deadline of every lane — one entry per lane, never a
// timer or a goroutine per message. It sleeps on the injected clock until the
// earliest entry is due and never ticks while it has nothing to do, so an
// idle engine costs nothing and a synctest bubble can advance instantly.
//
// A lane's entry carries the earliest of its three times. It is armed only
// when the new time is earlier than the one the entry already holds: a later
// time is left alone, the scheduler wakes at the earlier one, finds nothing
// due and re-arms from what the lane says then. That keeps the fetch and
// settle paths off the scheduler mutex in the steady state.
type scheduler[P any] struct {
	eng *Engine[P]

	mu    sync.Mutex
	lanes timer.Scheduler[*lane[P]]
	due   []*timer.Entry[*lane[P]]
	// sleeping is the time the goroutine's timer is set for, or 0 when it is
	// parked with nothing scheduled.
	sleeping int64

	wake   chan struct{}
	stop   chan struct{}
	exited chan struct{}
	once   sync.Once
}

func newScheduler[P any](eng *Engine[P], now int64) (*scheduler[P], error) {
	lanes, err := timer.New[*lane[P]](eng.kinds.Timer, now)
	if err != nil {
		return nil, fmt.Errorf("engine: lane scheduler: %w", err)
	}
	s := &scheduler[P]{
		eng:    eng,
		lanes:  lanes,
		wake:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
		exited: make(chan struct{}),
	}
	go s.run()
	return s, nil
}

// arm records that ln needs attention at at. A zero at means the lane has
// nothing scheduled; the entry it already holds is left in place and expires
// harmlessly.
func (s *scheduler[P]) arm(ln *lane[P], at int64) {
	if at == 0 {
		return
	}
	if cur := ln.armed.Load(); cur != 0 && at >= cur {
		return
	}
	s.mu.Lock()
	if cur := ln.armed.Load(); cur != 0 && at >= cur {
		s.mu.Unlock()
		return
	}
	s.lanes.Cancel(&ln.snode)
	s.lanes.Add(&ln.snode, at)
	ln.armed.Store(at)
	earlier := s.sleeping == 0 || at < s.sleeping
	s.mu.Unlock()
	if earlier {
		s.signal()
	}
}

// signal nudges the goroutine to recompute its sleep.
func (s *scheduler[P]) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// close stops the goroutine and waits for it, so that a synctest bubble the
// engine was built in can end.
func (s *scheduler[P]) close() {
	s.once.Do(func() { close(s.stop) })
	<-s.exited
}

// run is the scheduler goroutine.
func (s *scheduler[P]) run() {
	defer close(s.exited)
	for {
		at, ok := s.next()
		now := s.eng.now()
		if ok && at <= now {
			if s.fire(now) > 0 {
				continue
			}
			// A scheduler kind may name a time that is only an
			// approximation of what it holds — a wheel names its bucket —
			// so a due time can arrive with nothing behind it. Waiting a
			// tick keeps that from spinning the goroutine.
			at = now + int64(minRearm)
		}
		if !ok {
			select {
			case <-s.wake:
			case <-s.stop:
				return
			}
			continue
		}
		s.setSleeping(at)
		t := s.eng.clock.NewTimer(time.Duration(at - now))
		select {
		case <-t.C():
		case <-s.wake:
			t.Stop()
		case <-s.stop:
			t.Stop()
			s.setSleeping(0)
			return
		}
		s.setSleeping(0)
	}
}

// setSleeping records the time the goroutine is parked until.
func (s *scheduler[P]) setSleeping(at int64) {
	s.mu.Lock()
	s.sleeping = at
	s.mu.Unlock()
}

// next returns the earliest scheduled time.
func (s *scheduler[P]) next() (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lanes.Next()
}

// fire ticks every lane whose time has come, at most maxFireBatch of them per
// pass, and returns how many it ticked. A lane re-arms itself afterwards from
// what it holds then.
func (s *scheduler[P]) fire(now int64) int {
	s.mu.Lock()
	s.due = s.lanes.Fire(now, s.due[:0])
	over := s.due
	if len(over) > maxFireBatch {
		over = s.due[maxFireBatch:]
		s.due = s.due[:maxFireBatch]
		for _, ent := range over {
			s.lanes.Add(ent, now)
			ent.Value.armed.Store(now)
		}
	}
	batch := s.due
	s.mu.Unlock()
	for _, ent := range batch {
		ln := ent.Value
		ln.armed.Store(0)
		ln.tick(now)
	}
	for _, ent := range batch {
		ent.Value.rearm()
	}
	s.mu.Lock()
	n := len(s.due)
	clear(s.due)
	s.due = s.due[:0]
	s.mu.Unlock()
	return n
}

// notifyGate is what a goroutine waiting for a state change of the engine
// parks on: WaitIdle waiting for the last record to leave, and a publisher
// under OverflowBlock waiting for a lane to give capacity back. It carries a
// channel that is closed and replaced whenever the change may have happened,
// so that a waiter never misses a transition it was about to observe, and it
// is only ever notified while somebody is waiting.
type notifyGate struct {
	waiting atomic.Int64
	mu      sync.Mutex
	ch      chan struct{}
}

// chanFor returns the channel a waiter parks on.
func (g *notifyGate) chanFor() <-chan struct{} {
	g.mu.Lock()
	if g.ch == nil {
		g.ch = make(chan struct{})
	}
	ch := g.ch
	g.mu.Unlock()
	return ch
}

// notify wakes every waiter. It is a plain atomic load when nobody waits,
// which is what keeps it off the settle path's cost.
func (g *notifyGate) notify() {
	if g.waiting.Load() == 0 {
		return
	}
	g.mu.Lock()
	if g.ch != nil {
		close(g.ch)
		g.ch = nil
	}
	g.mu.Unlock()
}

// IdleMode selects what WaitIdle waits for (the memory engine). The zero
// value is IdleDelivered.
type IdleMode uint8

// The idle modes.
const (
	// IdleDelivered waits until nothing is ready or in flight: every record
	// the engine could hand to a consumer has been handed over and settled.
	// Records waiting for a due time in the future do not hold it.
	IdleDelivered IdleMode = iota
	// IdleIncludingDelayed waits until the engine holds no record at all,
	// delayed and redelivery-pending records included (the memory engine
	// memory.IdleIncludingDelayed).
	IdleIncludingDelayed
	// IdleSettled waits until no lease is outstanding, whatever is still
	// queued: it is "every consumer has finished what it took", which is the
	// condition a rebalance or a shutdown drain waits for.
	IdleSettled

	idleModeCount
)

var idleModeNames = [idleModeCount]string{"delivered", "includingdelayed", "settled"}

var idleModeIdents = [idleModeCount]string{"IdleDelivered", "IdleIncludingDelayed", "IdleSettled"}

// IdleModes returns the idle modes in declaration order.
func IdleModes() []IdleMode { return []IdleMode{IdleDelivered, IdleIncludingDelayed, IdleSettled} }

// String returns the lower-case mode name.
func (m IdleMode) String() string {
	if m < idleModeCount {
		return idleModeNames[m]
	}
	return "idle(?)"
}

// Ident returns the Go identifier of the mode.
func (m IdleMode) Ident() string {
	if m < idleModeCount {
		return idleModeIdents[m]
	}
	return "IdleMode(?)"
}

// Valid reports whether m names an idle mode.
func (m IdleMode) Valid() bool { return m < idleModeCount }

// settled is called whenever a record leaves the engine — and, through
// Subscription.unleased, whenever a lease is given back without the record
// leaving — and wakes whoever is waiting in WaitIdle. Both are needed:
// IdleSettled is satisfied by an empty in-flight list whatever is still
// queued, so a nack, a lease expiry, a consumer that closes or a rebalance
// can make the engine idle without a single record leaving it. The common
// case is one atomic load: the gate is only ever touched while somebody is
// waiting on it.
//
// It deliberately does not ask whether the engine is now idle. Deciding that
// means walking the lanes and taking every lane mutex, and a settlement may
// hold one already — the drain of a close does, and so does a lease expiry —
// so the check would close a lock cycle on a mutex that is not reentrant.
// The waiter re-examines the engine after every wake instead, which is the
// same answer arrived at from a goroutine that holds no lane.
func (e *Engine[P]) settled() { e.idle.notify() }

// isIdle reports whether the engine holds nothing the mode cares about. The
// engine-wide record count settles it outright when it is zero; otherwise the
// lane-walking modes walk the lanes, which happens only while a waiter is
// parked.
func (e *Engine[P]) isIdle(mode IdleMode) bool {
	if e.msgs.Load() == 0 {
		return true
	}
	if mode == IdleIncludingDelayed {
		return false
	}
	done := true
	e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			snap := s.Inspect()
			if snap.InFlight > 0 || (mode == IdleDelivered && snap.Ready > 0) {
				done = false
				return false
			}
		}
		return true
	})
	return done
}

// WaitIdle blocks until the engine holds no record that a consumer could take
// — and, in IdleIncludingDelayed, no record at all — or ctx is done.
//
// It is driven by the settlement path rather than by polling: a waiter parks
// on the idle gate, which the last record to leave closes. A record still
// waiting for a due time keeps IdleIncludingDelayed blocked until the
// scheduler releases it, which is what makes "advance the clock, then wait"
// deterministic under synctest.
func (e *Engine[P]) WaitIdle(ctx context.Context, mode IdleMode) error {
	e.idle.waiting.Add(1)
	defer e.idle.waiting.Add(-1)
	for {
		if e.isIdle(mode) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		ch := e.idle.chanFor()
		if e.isIdle(mode) {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

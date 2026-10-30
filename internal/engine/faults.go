package engine

import (
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// ErrPublishRejected is what a publication refused by Faults.RejectPublish
// returns. Nothing is admitted, so ownership of the payload never leaves the
// caller (Publish.Release).
var ErrPublishRejected = errcode.New(errcode.Unavailable, "iqmq/engine: publication rejected by fault injection")

// ErrSettlementFailed is what a settlement refused by Faults.FailSettlement
// returns. The refusal is definite: the lease survives it and the caller may
// settle again, which is the Reject path of internal/settlement.
var ErrSettlementFailed = errcode.New(errcode.Unavailable, "iqmq/engine: settlement rejected by fault injection")

// Faults is the engine's fault injector (the memory engine). It is the whole
// list that section names: reject the nth publication, fail a settlement,
// force a duplicate delivery, expire leases, close consumers, simulate
// reconnect generations, reorder where no order is promised, fail a
// dead-letter publication and advance deterministic time — plus the record
// expiry and retention sweeps, which the scheduler does on its own and which a
// fault makes happen now rather than being the only way they happen.
//
// Every counted fault is armed with a number of occurrences: a positive n
// fires that many times, a negative n fires until it is disarmed with 0, and
// each has a counter reporting how often it fired, so a test can assert that
// the path it wanted was taken rather than that nothing crashed.
//
// Faults is obtained from Engine.Faults and is safe for concurrent use.
type Faults[P any] struct {
	e *Engine[P]

	// dlqFail is the number of dead-letter publications still to be failed,
	// or -1 for every one of them.
	dlqFail atomic.Int64
	dlqSeen atomic.Int64

	// pubReject fails the next publications outright.
	pubReject atomic.Int64
	pubSeen   atomic.Int64

	// settleFail refuses the next settlement attempts.
	settleFail atomic.Int64
	settleSeen atomic.Int64

	// settleLate revokes the lease before the next settlements are applied.
	settleLate atomic.Int64
	lateSeen   atomic.Int64

	// dup duplicates the next deliveries.
	dup     atomic.Int64
	dupSeen atomic.Int64

	// reorder reverses the next claim batches on lanes that promise no
	// order.
	reorder     atomic.Int64
	reorderSeen atomic.Int64
}

// fire decrements an armed counter and reports whether the fault fires. A
// negative arm count fires for ever.
func fire(arm, seen *atomic.Int64) bool {
	for {
		n := arm.Load()
		if n == 0 {
			return false
		}
		if n < 0 {
			seen.Add(1)
			return true
		}
		if arm.CompareAndSwap(n, n-1) {
			seen.Add(1)
			return true
		}
	}
}

// FailDeadLetter makes the next n dead-letter publications fail with
// ErrUnavailable, so that the "a failed or ambiguous DLQ publish never acks"
// rule of the message contract can be exercised without a broker. A negative n
// fails every dead-letter publication until FailDeadLetter(0) turns it off.
// Both routes take it: the engine's own attempt-limit route and the explicit
// dead-letter publication of Topic.PublishNoWait.
func (f *Faults[P]) FailDeadLetter(n int) { f.dlqFail.Store(int64(n)) }

// DeadLetterFailures returns the number of dead-letter publications the fault
// has failed so far.
func (f *Faults[P]) DeadLetterFailures() int64 { return f.dlqSeen.Load() }

// dlqPublish reports the fault a dead-letter publication must take, or nil.
func (f *Faults[P]) dlqPublish() error {
	if fire(&f.dlqFail, &f.dlqSeen) {
		return ErrDeadLetterFailed
	}
	return nil
}

// RejectPublish makes the next n publications fail with ErrUnavailable before
// anything is admitted, so the payload stays the caller's. A negative n
// rejects every publication until RejectPublish(0) turns it off. A
// dead-letter publication does not take it (FailDeadLetter).
func (f *Faults[P]) RejectPublish(n int) { f.pubReject.Store(int64(n)) }

// PublishRejections returns the number of publications the fault refused.
func (f *Faults[P]) PublishRejections() int64 { return f.pubSeen.Load() }

// publish reports the fault a publication must take, or nil.
func (f *Faults[P]) publish() error {
	if fire(&f.pubReject, &f.pubSeen) {
		return ErrPublishRejected
	}
	return nil
}

// FailSettlement makes the next n settlement attempts fail definitely: the
// attempt is rejected with ErrSettlementFailed and the lease survives it
// intact, so the caller may settle the same delivery again. It is the broker
// that refused an acknowledgement rather than the one that lost it. A
// negative n fails every settlement until FailSettlement(0) turns it off.
func (f *Faults[P]) FailSettlement(n int) { f.settleFail.Store(int64(n)) }

// SettlementFailures returns the number of settlement attempts the fault
// refused.
func (f *Faults[P]) SettlementFailures() int64 { return f.settleSeen.Load() }

// settle reports whether a settlement attempt must be refused.
func (f *Faults[P]) settle() bool { return fire(&f.settleFail, &f.settleSeen) }

// DelaySettlement makes the next n settlements arrive too late: the lease is
// revoked first, exactly as an expiry would revoke it, and the settlement
// then finds the delivery gone and fails with ErrLeaseLost while the record
// goes back to its lane. It is the deterministic form of the
// settlement-races-expiry window that the message contract names, without
// needing the two to be timed against each other. A negative n delays every
// settlement until DelaySettlement(0) turns it off.
func (f *Faults[P]) DelaySettlement(n int) { f.settleLate.Store(int64(n)) }

// SettlementDelays returns the number of settlements the fault delayed past
// their lease.
func (f *Faults[P]) SettlementDelays() int64 { return f.lateSeen.Load() }

// delaySettle reports whether a settlement must find its lease already gone.
func (f *Faults[P]) delaySettle() bool { return fire(&f.settleLate, &f.lateSeen) }

// DuplicateDelivery makes the next n deliveries be handed out twice: the
// record is leased to the consumer that claimed it and admitted to its lane
// again, so a second consumer receives it as well. It is the duplicate an
// at-least-once broker produces when an acknowledgement is lost, and the
// engine accounts for it as a second admission, so conservation still holds.
//
// It is honoured under the per-subscription architectures. A shared partition
// log (Arch.Shared) hands each slot to each cursor exactly once by
// construction and has no second envelope to give; the fault is a documented
// no-op there, which Duplicates reports.
func (f *Faults[P]) DuplicateDelivery(n int) { f.dup.Store(int64(n)) }

// Duplicates returns the number of duplicate deliveries the fault produced.
func (f *Faults[P]) Duplicates() int64 { return f.dupSeen.Load() }

// duplicate reports whether a delivery must be duplicated.
func (f *Faults[P]) duplicate() bool {
	if f.e.kinds.Arch.Shared() {
		return false
	}
	return fire(&f.dup, &f.dupSeen)
}

// Reorder reverses the next n claim batches of a lane that promises no order,
// so that a consumer of a relaxed topic sees the records in an order the
// engine is allowed to produce but does not normally. It never fires on a
// lane whose topic asked for strict ordering, nor on an engine whose kinds
// order what they hold: a fault that broke a promise would test the test, not
// the engine.
func (f *Faults[P]) Reorder(n int) { f.reorder.Store(int64(n)) }

// Reorders returns the number of claim batches the fault reversed.
func (f *Faults[P]) Reorders() int64 { return f.reorderSeen.Load() }

// reorderBatch reverses es when the fault is armed and the lane promises no
// order.
func (f *Faults[P]) reorderBatch(ln *lane[P], es []*Envelope[P]) {
	if len(es) < 2 || ln.sub.strict || f.reorder.Load() == 0 {
		return
	}
	if !fire(&f.reorder, &f.reorderSeen) {
		return
	}
	for i, j := 0, len(es)-1; i < j; i, j = i+1, j-1 {
		es[i], es[j] = es[j], es[i]
	}
}

// CloseConsumers detaches every consumer of every subscription and returns
// how many it closed. Their leases go back to their lanes, which is the close
// policy of the memory engine.
func (f *Faults[P]) CloseConsumers() int {
	n := 0
	f.e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			n += s.closeConsumers()
		}
		return true
	})
	return n
}

// Reconnect simulates a client that lost its connection and came back: every
// subscription rebalances, which increments its assignment generation and
// revokes every lease outstanding under the old one. It returns the number of
// subscriptions it rebalanced.
//
// The deliveries revoked this way are not failures, so they do not consume a
// record's attempts: each gives back the attempt its lease counted, and the
// redelivery reports the attempt the revoked one would have
// (Subscription.revokeAll).
func (f *Faults[P]) Reconnect() int {
	n := 0
	f.e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			s.reconnect()
			n++
		}
		return true
	})
	return n
}

// Advance moves the engine's clock forward by d and runs the sweeps the move
// implies. It fails with ErrInvalidConfig on an engine that was not built
// with a controllable clock, because a fault that silently did nothing would
// make a test that relies on it pass for the wrong reason.
func (f *Faults[P]) Advance(d time.Duration) error {
	fake, ok := f.e.clock.(*clock.Fake)
	if !ok {
		return ErrInvalidConfig
	}
	if d > 0 {
		fake.Advance(d)
	}
	f.ExpireRecords()
	f.TrimRetention()
	f.ExpireLeases()
	f.e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			for _, ln := range s.lanes {
				ln.store.Promote(f.e.now())
				ln.rearm()
			}
			s.waiters.NotifyAll()
		}
		return true
	})
	return nil
}

// ExpireLeases reclaims every lease whose deadline has passed on the engine
// clock and returns records to their lane for redelivery. It returns the
// number of leases reclaimed; the scheduler normally has them already. A
// lease whose settlement is already in flight is left to that settlement,
// which is the pending-revocation rule of the message contract.
func (f *Faults[P]) ExpireLeases() int {
	n := 0
	now := f.e.now()
	f.e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			n += s.expire(now)
		}
		return true
	})
	return n
}

// expire reclaims the due leases of one subscription.
func (s *Subscription[P]) expire(now int64) int {
	total := 0
	for _, ln := range s.lanes {
		n := ln.expireLeases(now)
		if n > 0 {
			ln.rearm()
		}
		total += n
	}
	return total
}

// closeConsumers detaches every consumer of the subscription.
func (s *Subscription[P]) closeConsumers() int {
	s.cmu.Lock()
	ids := make([]uint32, len(s.members))
	copy(ids, s.members)
	s.cmu.Unlock()
	for _, id := range ids {
		s.detach(id)
	}
	return len(ids)
}

// reconnect rebalances the subscription, revoking every lease held under the
// previous assignment generation.
func (s *Subscription[P]) reconnect() {
	s.Rebalance()
	s.revokeAll()
}

// ExpireRecords discards every record whose time to live has run out on the
// engine clock and returns how many. The scheduler does this on its own; the
// fault runs the sweep now.
func (f *Faults[P]) ExpireRecords() int {
	n := 0
	now := f.e.now()
	f.e.Topics(func(t *Topic[P]) bool {
		for _, s := range t.subs.Load().subs {
			for _, ln := range s.lanes {
				n += ln.expireTTL(now)
			}
		}
		return true
	})
	return n
}

// TrimRetention applies the age bound of every stream topic's retained log
// now and returns the number of records it released.
func (f *Faults[P]) TrimRetention() int {
	before, after := 0, 0
	f.e.Topics(func(t *Topic[P]) bool {
		n, _ := t.retainedLen()
		before += n
		t.trimRetention(f.e.now())
		n, _ = t.retainedLen()
		after += n
		return true
	})
	return before - after
}

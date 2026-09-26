package memory

import (
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
)

// Faults is the broker's fault injector (the memory engine).
//
// It is the whole list that section names: reject the nth publication, delay
// or fail a settlement, force a duplicate delivery, expire leases, close
// consumers, simulate reconnect generations, reorder where no order is
// promised, fail a dead-letter publication and advance deterministic time —
// plus the record-expiry and retention sweeps, which the scheduler does on
// its own and which a fault makes happen now rather than being the only way
// they happen.
//
// Every counted fault is armed with a number of occurrences: a positive n
// fires that many times, a negative n fires until it is disarmed with 0, and
// each has a counter reporting how often it fired. The counters are the point
// — a test asserts that the path it wanted was taken, rather than that
// nothing crashed — and a fault that fired zero times is how a test that
// proves nothing looks from the outside.
//
// Faults is obtained from [Broker.Faults] and is safe for concurrent use.
type Faults struct {
	b *Broker
	f *engine.Faults[*mq.Message]
}

// RejectPublish makes the next n publications fail with ErrUnavailable
// before anything is admitted, so an owned payload stays the caller's. A
// negative n rejects every publication until RejectPublish(0).
func (fl *Faults) RejectPublish(n int) { fl.f.RejectPublish(n) }

// PublishRejections returns the number of publications the fault refused.
func (fl *Faults) PublishRejections() int64 { return fl.f.PublishRejections() }

// FailSettlement makes the next n settlement attempts fail definitely: the
// attempt is rejected and the lease survives it intact, so the caller may
// settle the same delivery again. It is the broker that refused an
// acknowledgement rather than the one that lost it. A negative n fails every
// settlement until FailSettlement(0).
func (fl *Faults) FailSettlement(n int) { fl.f.FailSettlement(n) }

// SettlementFailures returns the number of settlement attempts the fault
// refused.
func (fl *Faults) SettlementFailures() int64 { return fl.f.SettlementFailures() }

// DelaySettlement makes the next n settlements arrive too late: the lease is
// revoked first, exactly as an expiry would revoke it, and the settlement
// then finds the delivery gone and fails with ErrLeaseLost while the record
// goes back to its lane. It is the deterministic form of the
// settlement-races-expiry window the specification names, without needing the two to be
// timed against each other. A negative n delays every settlement until
// DelaySettlement(0).
func (fl *Faults) DelaySettlement(n int) { fl.f.DelaySettlement(n) }

// SettlementDelays returns the number of settlements the fault delayed past
// their lease.
func (fl *Faults) SettlementDelays() int64 { return fl.f.SettlementDelays() }

// DuplicateDelivery makes the next n deliveries be handed out twice, which is
// the duplicate an at-least-once broker produces when an acknowledgement is
// lost. It is a documented no-op under a shared partition log, which hands
// each slot to each cursor exactly once by construction; Duplicates reports
// what actually happened.
func (fl *Faults) DuplicateDelivery(n int) { fl.f.DuplicateDelivery(n) }

// Duplicates returns the number of duplicate deliveries the fault produced.
func (fl *Faults) Duplicates() int64 { return fl.f.Duplicates() }

// ExpireLeases reclaims every lease whose deadline has passed on the
// broker's clock and returns the records to their lanes for redelivery. It
// returns the number of leases it reclaimed; the scheduler normally has them
// already. A lease whose settlement is already in flight is left to that
// settlement, which is the pending-revocation rule.
func (fl *Faults) ExpireLeases() int { return fl.f.ExpireLeases() }

// ExpireRecords discards every record whose time to live has run out on the
// broker's clock and returns how many.
func (fl *Faults) ExpireRecords() int { return fl.f.ExpireRecords() }

// TrimRetention applies the age bound of every stream topic's retained log
// now and returns the number of records it released.
func (fl *Faults) TrimRetention() int { return fl.f.TrimRetention() }

// CloseConsumers detaches every consumer of every subscription and returns
// how many it closed. Their leases go back to their lanes, which is the specification's
// close policy, and the sources those consumers belonged to then answer
// ErrClosed.
func (fl *Faults) CloseConsumers() int { return fl.f.CloseConsumers() }

// Reconnect simulates a client that lost its connection and came back: every
// subscription rebalances, which increments its assignment generation and
// revokes every lease outstanding under the old one. It returns the number of
// subscriptions it rebalanced.
//
// The deliveries revoked this way are not failures, so they do not consume a
// record's attempts.
func (fl *Faults) Reconnect() int { return fl.f.Reconnect() }

// Reorder reverses the next n claim batches of a lane that promises no order,
// so that a consumer of a relaxed topic sees the records in an order the
// broker is allowed to produce but does not normally. It never fires on a
// lane whose topic asked for strict ordering, nor on a broker whose kinds
// order what they hold: a fault that broke a promise would test the test.
func (fl *Faults) Reorder(n int) { fl.f.Reorder(n) }

// Reorders returns the number of claim batches the fault reversed.
func (fl *Faults) Reorders() int64 { return fl.f.Reorders() }

// FailDeadLetter makes the next n dead-letter publications fail, so that
// the specification's "a failed or ambiguous dead-letter publish never acks" can be
// exercised without a broker. A negative n fails every one until
// FailDeadLetter(0).
func (fl *Faults) FailDeadLetter(n int) { fl.f.FailDeadLetter(n) }

// DeadLetterFailures returns the number of dead-letter publications the fault
// has failed so far.
func (fl *Faults) DeadLetterFailures() int64 { return fl.f.DeadLetterFailures() }

// Advance moves the broker's clock forward by d and runs the sweeps the move
// implies: the time-to-live sweep, the retention trim and the lease expiry,
// followed by the promotion of every record that has become due.
//
// It fails with ErrUnsupported on a broker that does not own a controllable
// clock — one built with no Config.Clock, or with a clock of the caller's own
// that the broker cannot move — because a fault that silently did nothing
// would make a test that relies on it pass for the wrong reason. [NewTestClock]
// is the clock it needs, and [NewForTest] installs one.
func (fl *Faults) Advance(d time.Duration) error {
	if fl.b.fake == nil {
		return mq.ErrUnsupported
	}
	if err := fl.f.Advance(d); err != nil {
		return err
	}
	return nil
}

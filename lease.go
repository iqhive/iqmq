package iqmq

import (
	"math"
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/timer"
)

// DefaultLeaseExtension is how far a lease is pushed out when the runner
// cannot work the interval out from the delivery itself: the driver reported
// no lease duration and ConsumerConfig.AckDeadline is zero.
//
// It is deliberately longer than ADR-011's 30 s backoff cap, so that a
// strictly ordered runner holding a delivery through one backoff interval
// does not race its own extension.
const DefaultLeaseExtension = time.Minute

// leaseScheduler is the shared lease-extension scheduler: ONE goroutine
// and ONE timer for a whole runner, extending the leases of the deliveries
// its handlers hold.
//
// The specification forbids a timer per delivery and rule 3 forbids a goroutine per
// message, so the deliveries are held in an internal/timer scheduler ordered
// by the moment each one next needs extending, and the run goroutine arms a
// single timer for the earliest of them. A hundred and twenty-eight in-flight
// deliveries cost one timer, exactly as one does. It is the same shape
// internal/leasectx's Clock kind uses for handler deadlines, for the same
// reason and with the same measured answer.
//
// Nothing here allocates per delivery: the scheduler entry lives in the
// runner's admission slot, which is allocated once per Run and recycled for
// ever.
type leaseScheduler struct {
	clock clock.Clock
	// lead is how far before a lease expires the extension is asked for.
	lead time.Duration
	// fallback is the interval used when the delivery names no lease length.
	fallback time.Duration

	mu      sync.Mutex
	sched   timer.Scheduler[*leaseTicket]
	armedAt int64 // the deadline the timer is set for, MaxInt64 when idle
	tmr     clock.Timer
	closed  bool

	wake chan struct{}
	stop chan struct{}
	wg   sync.WaitGroup
}

// leaseTicket is one tracked delivery. It is embedded in the runner's
// admission slot, so tracking a delivery allocates nothing.
//
// The mutex is the whole concurrency contract: an extension runs under it and
// release takes it, so a settlement that gives the slot back cannot overlap
// an extension of the delivery that slot held. Without that, the scheduler
// would be reading a Delivery the worker goroutine was zeroing.
type leaseTicket struct {
	entry timer.Entry[*leaseTicket]

	mu sync.Mutex
	// h is the delivery's settlement handle, nil once the ticket is
	// released. It is a copy of the handle and not a pointer to the
	// delivery, because the delivery's storage is the recycled slot's and
	// the handle is not.
	h  settlementHandle
	by time.Duration
}

// newLeaseScheduler starts a scheduler. The caller must close it.
func newLeaseScheduler(clk clock.Clock, lead, fallback time.Duration) *leaseScheduler {
	s := &leaseScheduler{
		clock:    clk,
		lead:     lead,
		fallback: fallback,
		sched:    timer.New[*leaseTicket](timer.Auto, clk.Now().UnixNano()),
		armedAt:  math.MaxInt64,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// track starts extending d's lease, using t as the scheduler entry.
//
// A delivery whose driver reports no lease expiry has no deadline to race, so
// it is not tracked at all: nothing is scheduled and release is a no-op.
func (s *leaseScheduler) track(t *leaseTicket, d *Delivery) {
	if d.handle == nil {
		return
	}
	info := d.Info()
	if info.LeaseExpires.IsZero() {
		return
	}
	by := info.LeaseExpires.Sub(info.ReceivedAt)
	if by <= 0 {
		by = s.fallback
	}
	t.mu.Lock()
	t.h, t.by = d.handle, by
	t.mu.Unlock()
	t.entry.Value = t
	s.add(t, info.LeaseExpires.Add(-s.lead).UnixNano())
}

// nextIn is how long after a granted extension the next one is due, given the
// amount by which the lease was just moved.
//
// The lead is honoured whenever it can be: an extension of by is asked for a
// lead before it expires, so the interval is by-lead. What it cannot be is at
// or below zero, which is what a lead at or above the lease produced — the
// next moment landed at or before now, the run goroutine fired the same
// ticket immediately, and a 200 ms lead over a 50 ms lease asked the driver
// for 828,959 extensions in 300 ms.
//
// So the interval has a floor of half the amount granted. Half is not a
// tuning constant, it is the largest floor that keeps the invariant the
// scheduler exists for: an extension at T moves the deadline to at least
// T+by, the next lands at T+by/2, and the deadline is therefore never less
// than by/2 away. A lead longer than that cannot be honoured at all, because
// honouring it would mean asking for more time than the broker's lease —
// which the three drivers with a visibility ceiling REFUSE (drivers/sqs's
// twelve hours, drivers/gcppubsub's MaxExtension budget, drivers/nsq's
// max-msg-timeout), so raising the amount asked for would turn a lease this
// scheduler was keeping alive into one it could not extend at all. The
// delivery surviving on a shorter notice than the caller asked for is the
// better failure: what a lead too long for its lease loses is the round-trip
// slack, and the settlement still reports a lease genuinely lost.
func (s *leaseScheduler) nextIn(by time.Duration) time.Duration {
	if in := by - s.lead; in > by/2 {
		return in
	}
	if half := by / 2; half > 0 {
		return half
	}
	// A one-nanosecond lease halves to nothing, and an interval of zero is
	// the spin this function exists to prevent — arrived at by rounding
	// rather than by subtraction, which is why the floor needs its own floor.
	// A driver reporting a lease this short is broken, but the scheduler's
	// answer to a broken driver must not be to saturate a core: one
	// nanosecond still parks the run goroutine, and the extension it asks
	// for will fail and stop the ticket for good.
	return 1
}

// add puts a ticket in the scheduler for the given moment and re-arms the one
// timer when it is now the earliest.
func (s *leaseScheduler) add(t *leaseTicket, due int64) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.sched.Add(&t.entry, due)
	// Only a moment earlier than the one the timer is already set for makes
	// the run goroutine re-arm, which is why a steady stream of deliveries
	// with equal leases arms no timers at all.
	rearm := due < s.armedAt
	s.mu.Unlock()
	if rearm {
		s.signal()
	}
}

// release stops extending the delivery the ticket names and waits for an
// extension already in flight to finish.
//
// The wait is what makes the runner's slot safe to recycle: once release
// returns, no goroutine holds the delivery this ticket named and nothing is
// left in the scheduler.
//
// The ORDER of the two locks is the whole correctness of it, and getting it
// wrong is what FuzzDispatcher found: the handle is dropped first, under the
// ticket's own mutex, so that an extension already in flight either finishes
// its re-scheduling before this call proceeds or sees a released ticket and
// schedules nothing. Cancelling first and dropping the handle afterwards
// leaves the window in which the extension re-schedules an entry nobody will
// take out again -- and the next delivery to reuse that slot panics the
// scheduler with "entry already scheduled".
func (s *leaseScheduler) release(t *leaseTicket) {
	t.mu.Lock()
	t.h = nil
	t.mu.Unlock()
	s.mu.Lock()
	s.sched.Cancel(&t.entry)
	s.mu.Unlock()
}

// signal nudges the run goroutine.
func (s *leaseScheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run is the single goroutine: extend what is due, arm one timer for the
// next one, sleep.
func (s *leaseScheduler) run() {
	defer s.wg.Done()
	var fired []*timer.Entry[*leaseTicket]
	for {
		now := s.clock.Now().UnixNano()
		s.mu.Lock()
		fired = s.sched.Fire(now, fired[:0])
		next, ok := s.sched.Next()
		if ok {
			s.armedAt = next
		} else {
			s.armedAt = math.MaxInt64
		}
		s.mu.Unlock()
		for _, e := range fired {
			s.extend(e.Value)
		}
		if len(fired) > 0 {
			continue
		}
		var ch <-chan time.Time
		if ok {
			ch = s.arm(time.Duration(next - now))
		}
		select {
		case <-ch:
		case <-s.wake:
		case <-s.stop:
			return
		}
	}
}

// extend asks the driver to move one delivery's lease and schedules the next
// extension.
//
// Every outcome is counted where the extension happens, which is the same
// settlement record Delivery.Extend goes through, so the specification's
// iqmq_lease_extensions_total{outcome} has one writer.
//
// A failed extension is NOT retried: the lease is either gone —
// in which case the broker has the message and the settlement that follows
// will say so — or the driver refused, and asking again at the same interval
// would turn one broken delivery into a busy loop against the broker.
func (s *leaseScheduler) extend(t *leaseTicket) {
	// The ticket's mutex is held across the extension AND the rescheduling,
	// so that a release cannot land between the two and leave an entry in the
	// scheduler for a slot the runner has recycled. The lock order is the
	// ticket's mutex and then the scheduler's, everywhere.
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.h == nil {
		// Released between the fire and here: the delivery is settled and
		// there is nothing to extend or to count.
		return
	}
	if err := t.h.extend(t.by); err != nil {
		return
	}
	// The driver moved the lease to at least by from now, so the next
	// extension is due a lead before that — or sooner, when the lead is too
	// long for the lease to allow it.
	s.add(t, s.clock.Now().Add(s.nextIn(t.by)).UnixNano())
}

// arm sets the one timer the scheduler owns and returns its channel.
func (s *leaseScheduler) arm(d time.Duration) <-chan time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tmr == nil {
		s.tmr = s.clock.NewTimer(d)
	} else {
		s.tmr.Reset(d)
	}
	return s.tmr.C()
}

// close stops the scheduler. It is idempotent, and it does not touch the
// tickets: a runner closes its scheduler after its lanes have drained, so
// every delivery it was extending has been settled and released already.
func (s *leaseScheduler) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.tmr != nil {
		s.tmr.Stop()
	}
	s.mu.Unlock()
	close(s.stop)
	s.wg.Wait()
}

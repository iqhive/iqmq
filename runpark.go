package iqmq

import (
	"errors"
	"sync"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
)

// park holds the deliveries whose delayed redelivery the driver could not
// make (runner.requeueAfter), each in its admission slot, and gives each back
// to the broker once its delay has passed.
//
// A held delivery keeps its slot, so it counts against the run's in-flight
// bound as an unsettled delivery does, and it keeps its record's hold and its
// lease ticket, so a run with WithLeaseExtension goes on extending its lease.
// It does not keep its worker: the worker hands the slot over and takes the
// next delivery. One goroutine waits for the earliest due delivery on one
// timer. It is started by the first delivery parked, so a run that never
// parks one costs nothing, and the runner is its owner: flushPark closes it
// and waits for it.
type park struct {
	mu     sync.Mutex
	items  []parked
	closed bool
	// wake tells the goroutine that a delivery was added, stop that the
	// park is closed, and done is closed once the goroutine has returned.
	// All three are made with the goroutine and are nil until then.
	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// parked is one held delivery and when it is due to go back. kept says the
// runner was extending its lease when it was parked: a refusal of that
// extension then ends the hold (due). tries counts the requeues of it the
// driver has refused: past zero the hold has ended, and the delivery waits
// only for its requeue's next offer (endHold).
type parked struct {
	it    *runItem
	due   time.Time
	kept  bool
	tries int
}

// requeueAfter gives the delivery in slot it back to the broker, to be
// redelivered after delay.
//
// The driver delays the redelivery where it can. Where it cannot, because it
// has no delayed redelivery (FeatureDelayedRedelivery) or refuses this delay
// with ErrUnsupported, a delay past what its broker can hold say, the runner
// holds the delivery in its slot for the delay and gives it back then
// (park). Those two answers are the only ones held, and both are held alike.
// Every other refusal of the delayed nack is retried and given up as any
// settlement is, and a stopping runner gives the delivery back at once.
//
// Only a driver whose negative settlement is native (FeatureNack) has its
// deliveries held, because only there is an immediate requeue a delivery put
// back where it was and nothing more. An emulated one does more, and a hold
// breaks it: Kafka's Stop and RetryTopic strategies go on consuming the
// partition behind the delivery and leave its offset uncommitted, and the
// Redis stop arm hands out the entries behind it, while the hold lasts. Those
// keep the immediate requeue, and so does ActiveMQ over STOMP, whose requeue
// is a subscription reset that gives back every unacknowledged delivery.
// NATS core settles nothing, so it has nothing to hold. The capability level was not defined for this
// question, and it is the one answer the SPI has today that tells the two
// kinds apart.
//
// Both used to be answered with an immediate redelivery. The broker handed
// the message straight back, and a handler that kept failing ran as fast as
// the broker could deliver it: for ever on a broker that counts no attempts,
// and through every attempt within milliseconds on one that counts them.
//
// It leaves a held delivery marked in its slot (runItem.parkFor), and the
// worker hands the slot to the park as it finishes with it (leave). Handed
// over here, the park could requeue and recycle the slot while the worker
// was still using it.
func (r *runner) requeueAfter(it *runItem, delay time.Duration) {
	d := &it.d
	if delay <= 0 {
		r.requeue(d)
		return
	}
	r.awaitHaltedFetch()
	if r.delayed {
		err := d.Nack(NackDelay(delay))
		for i := 1; i <= settleRetries && retriable(d, err) && !errors.Is(err, ErrUnsupported); i++ {
			r.pause(i + 1)
			err = d.Nack(NackDelay(delay))
		}
		if !errors.Is(err, ErrUnsupported) || !refused(d, err) {
			r.settleFailed(d, err)
			return
		}
	}
	if r.fetchCtx.Err() != nil || !r.holds || r.c.settler == nil || d.handle == nil || !d.handle.active(d.gen) {
		// The runner is stopping, and nothing will handle the message here
		// again; the driver's negative settlement is emulated; or the
		// delivery is no longer the runner's to give back: a source with no
		// settlement, an outcome already unknown, a lease gone. It goes back,
		// or is given up, at once, as any requeue would be, rather than
		// holding its slot for the delay.
		r.requeueNow(d)
		return
	}
	it.parkFor = delay
}

// leave ends a worker's part in slot it. A delivery its settlement left to
// be held (requeueAfter) goes to the park with its record's hold, its lease
// ticket and its slot; any other slot is released here.
func (r *runner) leave(it *runItem) {
	if it.parkFor > 0 {
		r.park(it)
		return
	}
	r.release(it)
}

// endHold ends a hold: it requeues the delivery at once and releases its
// slot. The lease ticket is released before the requeue, not after it as
// release alone does: the delivery needs no extension once its hold has
// ended, and an extension that met the requeue was refused, the delivery
// being settled already, and counted as a failed extension.
//
// A requeue the driver refuses is retried as requeueNow retries it, after
// the same pauses, but the delivery waits for each retry in the park
// (retryLater) and not on the park's goroutine. That goroutine ends every
// hold, and a pause there held every other delivery due meanwhile past its
// due time and past the half of its lease it was cut to, a backoff for each
// refusal. A park that has closed waits for nothing, so there the retries
// are made in place, the pauses of a stopping runner returning at once.
func (r *runner) endHold(e parked) {
	it := e.it
	if e.tries == 0 && r.leases != nil {
		r.leases.release(&it.lease)
	}
	for tries := e.tries; ; tries++ {
		err := it.d.Nack()
		if tries == settleRetries || !retriable(&it.d, err) {
			r.settleFailed(&it.d, err)
			break
		}
		if r.retryLater(it, tries+1) {
			return
		}
		r.pause(tries + 2)
	}
	r.release(it)
}

// retryLater puts back in the park a delivery whose requeue the driver has
// refused tries times, due when the pause before its next offer ends
// (pause). It reports false, holding nothing, when the park has closed or
// the runner is stopping: a stopping runner does not wait to settle.
func (r *runner) retryLater(it *runItem, tries int) bool {
	due := r.c.clock.Now().Add(r.cfg.backoff.Delay(tries + 1))
	p := &r.parked
	p.mu.Lock()
	if p.closed || r.fetchCtx.Err() != nil {
		p.mu.Unlock()
		return false
	}
	p.items = append(p.items, parked{it: it, due: due, tries: tries})
	wake := p.wake
	p.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
	return true
}

// release ends the delivery in slot it: the lease ticket first, so that no
// extension is in flight against a delivery whose storage is about to be
// reused, then the record's hold, which must go before the slot that
// holds the delivery is cleared, and then the slot.
func (r *runner) release(it *runItem) {
	if r.leases != nil {
		r.leases.release(&it.lease)
	}
	r.unhold(&it.d)
	r.recycle(it)
}

// park holds the delivery in slot it until its delay has passed, starting
// the park's goroutine for the first one. A closed park, that of a stopping
// runner, holds nothing: the delivery goes back at once.
//
// The delay is cut to half of what is left of the delivery's lease when the
// runner is not keeping that lease alive, so the delivery goes back while the
// broker still holds it for this consumer, as a postponement is held
// (holdsPostponed). Held past its lease, the delivery would be redelivered
// underneath the park, and the park's requeue would find it gone. When the
// runner is keeping the lease alive (WithLeaseExtension) the hold lasts its
// whole delay, until the driver refuses an extension: the refusal wakes the
// park, and the hold ends then. Held on, the delivery lost its lease at the
// driver's ceiling and was redelivered while its slot stayed taken.
func (r *runner) park(it *runItem) {
	kept := r.leases != nil && it.lease.keeps()
	due := r.parkUntil(it, it.parkFor, kept)
	it.parkFor = 0
	p := &r.parked
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		r.endHold(parked{it: it})
		return
	}
	if p.done == nil {
		p.wake, p.stop, p.done = make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
		go r.parkLoop(p.wake, p.stop, p.done)
	}
	wake := p.wake
	if kept {
		// A refusal from now on wakes the park. One that came before this
		// is seen by the scan the wake below starts (due). It is set before
		// the delivery is in the park: once it is, the park may end the hold
		// and recycle the slot, and the wake would land on the ticket of the
		// slot's next delivery.
		it.lease.wakeOnRefusal(wake)
	}
	p.items = append(p.items, parked{it: it, due: due, kept: kept})
	p.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

// parkUntil is when a delivery parked now for delay goes back (park). kept
// says the runner is extending the delivery's lease.
//
// The lease is the one the driver reports on the delivery,
// DeliveryInfo.LeaseExpires, read against the client's clock; a driver that
// reports none has nothing to cut the hold by.
func (r *runner) parkUntil(it *runItem, delay time.Duration, kept bool) time.Time {
	now := r.c.clock.Now()
	due := now.Add(delay)
	if kept {
		return due
	}
	if expires := it.d.Info().LeaseExpires; !expires.IsZero() {
		if limit := now.Add(expires.Sub(now) / 2); limit.Before(due) {
			return limit
		}
	}
	return due
}

// parkLoop is the park's goroutine: it gives back each held delivery that
// is due, then waits for the next one, a new one, or the park's close.
func (r *runner) parkLoop(wake, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	var t clock.Timer
	defer func() {
		if t != nil {
			t.Stop()
		}
	}()
	var ready []parked
	for {
		var next time.Time
		ready, next = r.parked.due(r.c.clock.Now(), ready[:0])
		for _, e := range ready {
			r.endHold(e)
		}
		clear(ready)
		var fire <-chan time.Time
		if !next.IsZero() {
			wait := next.Sub(r.c.clock.Now())
			if t == nil {
				t = r.c.clock.NewTimer(wait)
			} else {
				t.Reset(wait)
			}
			fire = t.C()
		}
		select {
		case <-fire:
		case <-wake:
		case <-stop:
			return
		}
	}
}

// due takes out of the park every delivery due by now, and every one whose
// lease the runner was extending until the driver refused an extension,
// appending it to ready, and reports when the earliest one left is due: the
// zero time when none is.
func (p *park) due(now time.Time, ready []parked) ([]parked, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var next time.Time
	kept := p.items[:0]
	for _, e := range p.items {
		if !e.due.After(now) || e.kept && !e.it.lease.keeps() {
			ready = append(ready, e)
			continue
		}
		kept = append(kept, e)
		if next.IsZero() || e.due.Before(next) {
			next = e.due
		}
	}
	clear(p.items[len(kept):])
	p.items = kept
	return ready, next
}

// flushPark gives back at once every delivery the park holds, closes the
// park, so that a delivery parked afterwards goes back at once too, and
// waits for its goroutine to return. It is idempotent and safe from any
// goroutine.
//
// The runner's shutdown calls it as the fetch stops, so a held delivery goes
// back before Run returns, and so before the drain a consumer's Shutdown or a
// client's waits for has finished and before the backend closes; the stop
// does not wait out its delay. A prompt Close has cancelled the consumer's
// settlements before anything else (ConsumerHandle.interrupt), so there the
// requeue fails, as a cut handler's does, and the source's close gives the
// delivery back.
func (r *runner) flushPark() {
	p := &r.parked
	p.mu.Lock()
	items := p.items
	p.items = nil
	if !p.closed && p.stop != nil {
		close(p.stop)
	}
	p.closed = true
	done := p.done
	p.mu.Unlock()
	for _, e := range items {
		r.endHold(e)
	}
	if done != nil {
		<-done
	}
}

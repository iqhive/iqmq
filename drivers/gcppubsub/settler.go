package gcppubsub

import (
	"context"
	"time"

	mq "github.com/iqhive/iqmq"
)

// The settler implements the mandatory settlement surface, the optional
// batched one and the optional lease extension.
//
// The batched surface is here because the SPI offers it and not because this
// driver has a batch API to use: the client library coalesces acknowledgement
// identifiers into batched Acknowledge and ModifyAckDeadline requests on its
// own schedule, so AckBatch is a loop and the batching happens a layer below.
// That is stated in the capability note and it is why there is no flush policy
// to choose here — the four answers belong to a driver whose
// settlement is its own round trip.
var (
	_ mq.Settler          = (*settler)(nil)
	_ mq.BatchSettler     = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// settler settles one source's deliveries.
type settler struct{ src *source }

// newSettler builds the settler.
func newSettler(src *source) *settler { return &settler{src: src} }

// Ack settles d positively.
//
// Whether it WAITS for the service's answer is the subscription's decision and
// not this driver's: on a subscription with exactly-once delivery enabled the
// acknowledgement has an outcome and this method returns it, and on every
// other subscription the client library readies the same call immediately with
// success. Reporting the two as one answer would be reporting "the request was
// queued" as "the service will never redeliver this", which is the whole
// difference [Config.ExactlyOnce] buys.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	if !s.src.info.ExactlyOnce {
		ref.settle.Ack()
		ref.state.Store(stateSettled)
		s.src.forget(ref)
		return nil
	}
	return s.complete(ref, mq.OpAck, ref.settle.AckWithResult(ctx))
}

// AckBatch settles every delivery in ds, reporting per-delivery outcomes
// through a *BatchError whose Errs are index-aligned with ds.
func (s *settler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = s.Ack(ctx, d)
	}
	return collapse(errs)
}

// Nack settles d negatively.
//
// An immediate negative settlement is native: the library sends a
// ModifyAckDeadline of zero and the service makes the message deliverable at
// once. A DELAYED one is an in-process hold, because Pub/Sub has no nack delay
// at all — see [settler.holdNack].
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	switch o.Action {
	case mq.NackActionRequeue:
	case mq.NackActionDeadLetter:
		// Reject, which on this broker cannot be the subscription's
		// own dead-letter policy: that policy moves a message the SERVICE
		// has delivered at least five times (its own minimum for
		// maxDeliveryAttempts), and a caller rejecting a message once is not
		// asking for four more deliveries. So a Reject publishes the message
		// to the dead-letter topic and acknowledges the original once the
		// publication is confirmed. With no dead-letter topic configured
		// there is nowhere to send it, and the specification makes the refusal the right
		// answer rather than destroying the message or requeuing it for
		// ever.
		if o.Delay > 0 {
			// A delay asks for a REDELIVERY held back, and a dead-letter
			// settlement produces no redelivery at all: the message goes to
			// another topic and the original is acknowledged. Honouring the
			// delay would mean holding the delivery and then dead-lettering
			// it, which is not what either half of the request means, and
			// ignoring it silently is what the specification forbids.
			return errDelayWithDeadLetter
		}
		return s.reject(ctx, d)
	case mq.NackActionDrop:
		return errNoDrop
	default:
		return errNackAction
	}
	if o.Delay > 0 {
		return s.holdNack(ctx, d, o.Delay)
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	if !s.src.info.ExactlyOnce {
		ref.settle.Nack()
		ref.state.Store(stateSettled)
		s.src.forget(ref)
		return nil
	}
	return s.complete(ref, mq.OpNack, ref.settle.NackWithResult(ctx))
}

// reject is the dead-letter settlement: republish, then acknowledge.
//
// The order is what makes it durable. The message reaches the dead-letter
// topic BEFORE the original is acknowledged, so a failure anywhere in between
// leaves the original unacknowledged and the message is redelivered — which is
// at-least-once with a possible duplicate in the dead-letter topic, and is the
// direction the specification requires: a route that acknowledged first and then failed to
// publish would destroy the message.
//
// The destination is [Config.DeadLetterTopic] and the publisher for it is
// created once per source, lazily, so a consumer that never rejects anything
// pays nothing for the route.
func (s *settler) reject(ctx context.Context, d *mq.DriverDelivery) error {
	dlq := s.src.b.cfg.DeadLetterTopic
	if dlq == "" {
		return errNoDeadLetter
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	if perr := s.src.republish(ctx, d.Message); perr != nil {
		// Nothing has been done to the original, so the claim is given back:
		// a refused settlement leaves the delivery settleable, which is, and the message is still in the subscription's backlog.
		//
		// That holds when the publication is AMBIGUOUS too, and the claim is
		// not made sticky here the way [settler.complete] makes an ambiguous
		// acknowledgement: the original has been neither acknowledged nor
		// nacked, so the client library is still holding it, and its
		// shutdown waits for every message it holds to be settled. A
		// delivery stuck in stateUnknown is one Close would never nack, and
		// Close would wait out the whole extension budget for it. The core
		// is what refuses a second settlement of an ambiguous one.
		ref.giveBack()
		return perr
	}
	if !s.src.info.ExactlyOnce {
		ref.settle.Ack()
		ref.state.Store(stateSettled)
		s.src.forget(ref)
		return nil
	}
	return s.complete(ref, mq.OpAck, ref.settle.AckWithResult(ctx))
}

// NackBatch settles every delivery in ds negatively.
func (s *settler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) error {
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = s.Nack(ctx, d, o)
	}
	return collapse(errs)
}

// holdNack is the delayed negative settlement, and it is an EMULATION: the
// capability table says so with [github.com/iqhive/iqmq.EmulatedVolatile] and
// this is the emulation.
//
// Pub/Sub has no nack delay. A nack is a ModifyAckDeadline of zero and the
// message becomes deliverable at once, so the only way to hold a redelivery
// back is to hold the DELIVERY: the driver leaves the message unacknowledged,
// the client library goes on extending its acknowledgement deadline, and the
// nack is sent when the delay elapses.
//
// Three things follow, and all three are the reason this must be declared an
// emulation rather than claimed as native:
//
//   - It does not survive a process death. A hold interrupted by a crash
//     becomes a redelivery when the acknowledgement deadline expires, which is
//     sooner than the delay asked for.
//   - It is bounded by the client library's extension budget
//     ([Config.MaxExtension]). A delay longer than what is left of it is
//     refused before anything is held, because a hold that outlives the lease
//     is a redelivery at a time nobody chose.
//   - It costs a timer and a goroutine per held delivery. That is bounded —
//     a source holds at most its prefetch — and it is the price of a delay on
//     a broker that has none.
//
// Close ends every hold at once ([source.endHolds]). The playbook's recurring
// defect class is exactly this shape one layer out — a record placed somewhere
// the driver holds it while the driver is closing — and the fix used here is
// the second of the two the playbook names: the closing flag is read and the
// hold is registered inside ONE critical section, under the same mutex
// endHolds takes to drain. So a hold either goes in before the drain and is
// ended by it, or reads `closing` and is nacked immediately; there is no
// window between the check and the placement to re-read afterwards, which is
// why this function does not.
//
// A hold that escaped both would be two failures at once: a goroutine and a
// timer nobody can stop, and a message held unacknowledged by a source that no
// longer exists until its deadline expires — a three-hundred-millisecond delay
// silently becoming the acknowledgement deadline.
func (s *settler) holdNack(ctx context.Context, d *mq.DriverDelivery, delay time.Duration) error {
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	now := s.src.b.now()
	if now.Add(delay).After(ref.receivedAt().Add(s.src.budget)) {
		// Nothing has been done to the delivery yet beyond the claim, so the
		// claim is given back: a refused settlement leaves the delivery
		// settleable, which is.
		ref.giveBack()
		return errDelayTooLong
	}
	timer := s.src.b.newTimer(delay)
	s.src.mu.Lock()
	if s.src.closing {
		s.src.mu.Unlock()
		timer.Stop()
		// The source is closing, so the hold cannot be honoured. The message
		// is nacked NOW rather than refused: the caller asked for a negative
		// settlement and the delay is the part that cannot be kept, and a
		// refusal here would leave the delivery unsettled on a source that is
		// going away.
		ref.settle.Nack()
		ref.state.Store(stateSettled)
		s.src.forget(ref)
		return nil
	}
	if !ref.state.CompareAndSwap(stateSettling, stateHeld) {
		s.src.mu.Unlock()
		timer.Stop()
		// The delivery was lost after the claim: a redelivery superseded it
		// and left its message to this settlement ([delivery.release]), or a
		// second settlement found its lease run out. Storing stateHeld over
		// that made a delivery a later one owns settleable again, and held
		// its nack back for a delay that no longer means anything: the
		// message is already redelivered.
		ref.settle.Nack()
		return mq.ErrLeaseLost
	}
	s.src.holds[ref] = timer
	s.src.holdWG.Add(1)
	s.src.mu.Unlock()
	go s.awaitHold(ref, timer)
	_ = ctx
	return nil
}

// awaitHold waits out one delayed negative settlement.
func (s *settler) awaitHold(ref *delivery, timer mq.Timer) {
	defer s.src.holdWG.Done()
	defer timer.Stop()
	select {
	case <-timer.C():
	case <-s.src.stop:
		// endHolds has already claimed the delivery and nacked it, or is
		// about to; either way this goroutine does nothing.
		return
	}
	s.src.mu.Lock()
	_, held := s.src.holds[ref]
	delete(s.src.holds, ref)
	s.src.mu.Unlock()
	if !held {
		return
	}
	if ref.state.CompareAndSwap(stateHeld, stateSettling) {
		ref.settle.Nack()
		ref.state.Store(stateSettled)
		s.src.forget(ref)
	}
}

// Extend moves the delivery's lease.
//
// It is an EMULATION and the capability table says so. The client library
// extends a held message's acknowledgement deadline automatically, up to
// [Config.MaxExtension] in total, so a handler that needs longer than the
// deadline gets it without asking. What this method does is check that `by`
// fits inside what is LEFT of that budget and move the driver's own view of
// the lease; it sends no ModifyAckDeadline of its own, because the
// acknowledgement identifier that request needs is private to the client
// library's message type and there is no accessor for it.
//
// An extension beyond the budget is refused rather than granted and then lost,
// which is the honest direction: a handler told its lease runs for another
// minute, whose message is redelivered in ten seconds, is worse off than one
// told the extension is not available.
func (s *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	if by <= 0 {
		return errExtendZero
	}
	ref, ok := refOf(d)
	if !ok {
		return mq.ErrNotSettleable
	}
	if ref.src != s.src {
		return mq.ErrLeaseLost
	}
	if err := stateError(ref.state.Load()); err != nil {
		return err
	}
	now := s.src.b.now()
	if ref.expired(now) {
		ref.lose()
		s.src.forget(ref)
		return mq.ErrLeaseLost
	}
	ceiling := ref.receivedAt().Add(s.src.budget)
	if now.Add(by).After(ceiling) {
		return errExtendBeyondBudget
	}
	ref.extend(now.Add(by))
	_ = ctx
	return nil
}

// claim takes a delivery out of circulation for settlement, answering the specification's
// question about a second settlement.
func (s *settler) claim(d *mq.DriverDelivery) (*delivery, error) {
	ref, ok := refOf(d)
	if !ok {
		return nil, mq.ErrNotSettleable
	}
	if ref.src != s.src {
		// A delivery from another source is one this settler never vouched
		// for, which is the specification's "a foreign consumer reports ErrLeaseLost".
		return nil, mq.ErrLeaseLost
	}
	if ref.gen != d.Gen {
		return nil, mq.ErrLeaseLost
	}
	now := s.src.b.now()
	if ref.expired(now) {
		ref.lose()
		s.src.forget(ref)
		return nil, mq.ErrLeaseLost
	}
	if !ref.state.CompareAndSwap(stateActive, stateSettling) {
		if err := stateError(ref.state.Load()); err != nil {
			return nil, err
		}
		return nil, mq.ErrAlreadySettled
	}
	return ref, nil
}

// complete records a settlement's outcome on the delivery and returns it
// mapped.
//
// An ambiguous outcome is STICKY: the specification says nothing later turns an unknown
// settlement into a success, so the state moves to stateUnknown and every
// later settlement of that delivery reports ErrSettlementUnknown.
//
// The judgement is made on the MAPPED error, and the mapping is told the
// request has left: the client library sends the settlement before the result
// is waited for, so a context that ends during the wait leaves an
// acknowledgement that may have landed. The raw error never says ambiguous.
func (s *settler) complete(ref *delivery, op string, err error) error {
	mapped := s.src.b.errs.MapNet(op, s.src.cfg.Topic, s.src.cfg.Subscription, err, true)
	switch {
	case mapped == nil:
		ref.state.Store(stateSettled)
		s.src.forget(ref)
	case mq.IsAmbiguous(mapped):
		ref.state.Store(stateUnknown)
	default:
		// A settlement the service refused leaves the delivery where it was:
		// the message is still unacknowledged, so the next delivery of it is
		// a redelivery rather than a loss, and a caller may settle again.
		ref.giveBack()
	}
	return mapped
}

// refOf is the delivery this driver put in DriverDelivery.Ref.
func refOf(d *mq.DriverDelivery) (*delivery, bool) {
	if d == nil {
		return nil, false
	}
	ref, ok := d.Ref.(*delivery)
	return ref, ok && ref != nil
}

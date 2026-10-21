package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

var _ mq.Settler = (*settler)(nil)

// settler settles one source's deliveries.
//
// It is the source itself under another name — there is one settler per source
// and it holds no state of its own — which is what keeps a settlement free of
// an allocation: the core hands back a pointer to its copy of the
// DriverDelivery, and everything needed to settle it is the token.
//
// The four actions map onto three AMQP frames and one re-publication:
//
//	Ack                       basic.ack
//	Nack(requeue), no delay   basic.nack{requeue=true}
//	Nack(requeue), with delay ack + re-publish through Nack(deadletter) basic.nack{requeue=false} onto the queue's DLX
//	Nack(drop)                basic.ack
//
// The last one is the surprising mapping and it is the right one. Drop
// destroys the message, and `basic.ack` is the frame that removes a message
// from a queue without routing it anywhere; `basic.nack{requeue=false}` would
// dead-letter it whenever the queue has a dead-letter exchange, which is the
// opposite of destroying it. So Drop is an ack, and the driver page says so
// where a reader will look for it.
type settler source

// Ack settles the delivery positively: `basic.ack`.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	return s.settle(ctx, d, mq.OpAck, func(ref *deliveryRef) error {
		return ref.ch.Ack(ref.tag, false)
	})
}

// Nack settles the delivery negatively under o.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	switch o.Action {
	case mq.NackActionDrop:
		return s.settle(ctx, d, mq.OpNack, func(ref *deliveryRef) error {
			// See the type comment: `basic.ack` is the frame that destroys a
			// message, because a nack with `requeue=false` dead-letters it
			// whenever the queue has a route.
			return ref.ch.Ack(ref.tag, false)
		})
	case mq.NackActionDeadLetter:
		if !s.b.cfg.DeadLetter {
			// Refusing is what the specification asks of a driver that "cannot honour o's
			// action — a delayed requeue or a dead-letter route it has
			// neither natively nor by an enabled emulation": it returns
			// ErrUnsupported and settles NOTHING, so the delivery stays
			// settleable and the core's own policy can take over.
			return s.b.errs.Map(mq.OpNack, s.topic, s.sub, errNoDeadLetterRoute)
		}
		return s.settle(ctx, d, mq.OpNack, func(ref *deliveryRef) error {
			return ref.ch.Nack(ref.tag, false, false)
		})
	case mq.NackActionRequeue:
		if o.Delay <= 0 {
			return s.settle(ctx, d, mq.OpNack, func(ref *deliveryRef) error {
				return ref.ch.Nack(ref.tag, false, true)
			})
		}
		return s.requeueLater(ctx, d, o)
	default:
		return s.b.errs.Map(mq.OpNack, s.topic, s.sub, mq.ErrUnsupported)
	}
}

// settle is the one settlement path, and the staleness guard is in it.
//
// Two checks stand between a settlement and the wrong delivery, and both are
// needed:
//
//  1. [driverkit.TokenTable.Settle] refuses a token whose generation the slot
//     no longer has, which catches every settlement for a delivery handed out
//     before a reconnection or a channel exception. That is the rule.
//  2. The delivery's own channel is compared with the source's current one.
//     Sending an old channel's tag on a NEW channel would acknowledge
//     whatever delivery holds that tag there, which is the disqualifier
//     in AMQP's clothing and what `FuzzRabbitTagSettlement` exists for.
//
// The two checks are REDUNDANT BY CONSTRUCTION and neither is load-bearing on
// its own, which a mutation campaign has to report rather than pick a favourite
// from. Removing the token table's invalidation leaves the channel comparison
// to catch every case, removing the channel comparison leaves the generation to
// catch every case, and TestCriticalStaleTagNeverSettlesOnANewChannel passes in
// both single-mutation runs. Removing BOTH is what it fails, verbatim:
//
//	acknowledging a delivery from the OLD channel = rabbitmq: ack
//	topic=orders subscription=workers: SettlementUnknown: Exception (504)
//	Reason: "channel/connection is not open", want ErrLeaseLost: its tag
//	names a different delivery on the new channel
//
// The redundancy is kept deliberately and cheaply -- the second check is two
// integer comparisons against fields the reference already carries -- because
// the two fail in different directions: the generation catches a settlement
// whose SLOT has been reused and the channel comparison catches one whose
// channel has been replaced without the table being told, which is what a
// future fourth repair route would produce if it forgot the invalidation.
//
// The second check now also refuses a delivery whose channel reports CLOSED
// , which the mutation above predates: a stale tag's
// channel is always closed by the time a new one exists, so that single-and
// double-mutation result now needs this condition removed as well. It is here
// for a different case -- a channel that has closed and not yet been noticed,
// where neither of the first two can fire -- and
// `TestCriticalASettlementOnAClosedChannelIsLeaseLost` is its own test.
func (s *settler) settle(ctx context.Context, d *mq.DriverDelivery, op string, act func(*deliveryRef) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if (*source)(s).isClosed() {
		return mq.ErrClosed
	}
	raw, err := s.tokens.Settle(d.Token)
	if err != nil {
		// ErrAlreadySettled, ErrSettlementInProgress, ErrSettlementUnknown
		// and ErrLeaseLost all arrive here, and every one of them is the
		// portable answer the specification asks for. They are NOT mapped through the error
		// mapper: they are already the SPI's own sentinels, and wrapping
		// them in an *OpError carrying a driver name would change nothing a
		// caller reads and would allocate on a settlement path.
		return err
	}
	ref, ok := raw.(*deliveryRef)
	if !ok {
		_ = s.tokens.Complete(d.Token, mq.ErrLeaseLost)
		return mq.ErrLeaseLost
	}
	if _, gen := (*source)(s).current(); ref.gen != gen || ref.ch == nil || ref.ch.IsClosed() {
		// The second guard. The slot is released rather than completed: the
		// delivery is gone, and the broker has already requeued it.
		//
		// A CLOSED channel is the same answer, and it is not the generation's
		// to give: the source learns of a close through a notification a
		// caller can outrun, and a settlement that got there first used to
		// be sent to the dead channel and reported SettlementUnknown -- an
		// outcome that is in fact known, because a closed channel writes no
		// frame and the broker takes back what it held.
		// A close that lands between this check and the write is still
		// reported unknown, which it then genuinely is.
		_ = s.tokens.Release(d.Token)
		return mq.ErrLeaseLost
	}

	aerr := act(ref)
	if aerr == nil {
		_ = s.tokens.Complete(d.Token, nil)
		return nil
	}
	mapped := s.settlementError(op, aerr)
	_ = s.tokens.Complete(d.Token, mapped)
	return mapped
}

// settlementError classifies a settlement that did not land.
//
// The distinction turns on is whether the outcome is KNOWN. A broker that
// refused the frame answered, so the settlement definitely did not happen and
// the delivery stays settleable. A channel that went away while the frame was
// in the socket is not an answer: the broker may have applied it, and the specification
// makes an unknown settlement STICKY — so it is reported as
// CodeSettlementUnknown, which the token table then records permanently rather
// than leaving the delivery settleable again.
//
// Getting this backwards is the failure the specification names: a settlement reported as
// a definite failure when it may have landed invites a retry, and on this
// broker a second `basic.ack` for a tag the broker has already forgotten is a
// channel exception that takes every publication on that channel with it.
func (s *settler) settlementError(op string, err error) error {
	if isTransportFailure(err) {
		return &mq.OpError{
			Driver: DriverName, Operation: op, Topic: s.topic, Subscription: s.sub,
			Code: mq.CodeSettlementUnknown, Retryable: false, Ambiguous: true,
			Err: err,
		}
	}
	return s.b.errs.Map(op, s.topic, s.sub, err)
}

// requeueLater is the delayed requeue and the whole
// subject.
//
// AMQP 0-9-1 has no delayed nack, so the message is RE-PUBLISHED: either into
// a queue whose `x-message-ttl` is the delay and whose dead-letter route is
// this subscription's own queue, or to the delayed-message exchange when the
// plugin is there. Either way the original delivery is then acknowledged,
// because a message that exists twice is a duplicate and this driver's job is
// to move it rather than to copy it.
//
// The ORDER is the part that decides what a crash costs, and it is: publish,
// wait for the confirm, then ack.
//
//   - Publishing first and acking second means a crash between them leaves
//     both copies, so the message is delivered twice. The at-least-once is
//     what iqmq promises and a duplicate is inside it.
//   - Acking first and publishing second would mean a crash between them
//     loses the message entirely, which is outside every guarantee this
//     driver makes.
//
// Waiting for the confirm is what makes the first option safe at all: without
// it the park might never have landed and the ack would destroy the only
// copy. It is why a CONSUME channel is in confirm mode in this driver.
func (s *settler) requeueLater(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if !s.b.cfg.Declare && s.b.fx.delayKind.Resolve() == delay.TTLDLX {
		return s.b.errs.Map(mq.OpNack, s.topic, s.sub, errDelayNeedsDeclare)
	}
	native, ok := d.Native.(*amqp.Delivery)
	if !ok {
		return s.b.errs.Map(mq.OpNack, s.topic, s.sub, mq.ErrUnsupported)
	}
	plan, err := s.delay.Plan(delay.Request{
		Target:        delay.RedeliveryTarget(s.names.Queue),
		Delay:         o.Delay,
		DelayExchange: s.names.DelayExchange,
	})
	if err != nil {
		return s.b.errs.Map(mq.OpNack, s.topic, s.sub, err)
	}
	return s.settle(ctx, d, mq.OpNack, func(ref *deliveryRef) error {
		if err := s.b.ensureDelay(ref.ch, &plan); err != nil {
			return err
		}
		if err := s.park(ctx, ref.ch, native, &plan, d.Info.Attempt); err != nil {
			return err
		}
		return ref.ch.Ack(ref.tag, false)
	})
}

// park re-publishes the message through the delay plan and waits for its
// confirm.
//
// The republication carries the message's own properties and body unchanged,
// including the header table — which is what makes the delivery that comes
// back the same message. The one thing it does NOT carry
// unchanged is the delivery count: RabbitMQ's `x-delivery-count` belongs to
// the queue entry, and a new publication is a new entry, so the BROKER's count
// restarts. The driver therefore carries the count itself in [AttemptHeader],
// which is what keeps Attempt rising across a delayed redelivery
// rather than reporting 1 for ever.
func (s *settler) park(ctx context.Context, ch client.Channel, d *amqp.Delivery, plan *delay.Plan, attempt uint32) error {
	pub := amqp.Publishing{
		Headers:         parkHeaders(d.Headers, plan, attempt),
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}

	w := &parkWait{done: make(chan struct{}, 1)}
	s.mu.Lock()
	tr := s.tr
	if tr == nil {
		// No channel: a CHANNEL exception nils the tracker through
		// `source.claimLost` with the connection up and no dial loop running,
		// so naming the dial loop here was the lie on the
		// consuming side.
		s.mu.Unlock()
		return errNoChannel
	}
	if s.ch != ch {
		// The delivery's channel was replaced after settle checked it. tr
		// numbers the NEW channel's publications, so a tag taken from it for
		// a publication on the old one would put both trackers one tag off.
		// Nothing is written, and the broker takes back what the old channel
		// held.
		s.mu.Unlock()
		return mq.ErrLeaseLost
	}
	tag, err := tr.Submit("")
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.parks[tag] = w
	// The publish is under the same lock as the tag, for the reason
	// [sink] gives: the broker numbers publications in the order their
	// frames reach it.
	perr := ch.PublishWithContext(ctx, plan.Exchange, plan.RoutingKey, true, false, pub)
	if perr != nil {
		delete(s.parks, tag)
		if ch.IsClosed() {
			s.mu.Unlock()
			return errParkAmbiguous
		}
		_ = tr.Unsubmit(tag)
		s.mu.Unlock()
		return perr
	}
	s.mu.Unlock()

	ctx, cancel := s.b.waitDeadline(ctx)
	defer cancel()
	select {
	case <-w.done:
		switch w.result {
		case confirm.Confirmed:
			return nil
		case confirm.Ambiguous:
			// The channel went while the park was in flight, so the parked
			// copy may or may not be stored. Acknowledging the original now
			// could lose the message, so the settlement reports an unknown
			// outcome and the original stays unacknowledged: the broker
			// redelivers it, which is the duplicate-over-loss choice
			// [settler.requeueLater] explains.
			return errParkAmbiguous
		default:
			// A nack or a `basic.return` on the parked copy: the delay queue
			// or the delayed exchange did not take it, so the original must
			// NOT be acknowledged.
			return errParkRefused
		}
	case <-ctx.Done():
		return errParkAmbiguous
	case <-s.closed:
		return mq.ErrClosed
	}
}

// parkHeaders is the header table the parked copy carries.
//
// Three edits, and each one matters. The plugin's delay goes in (`x-delay`);
// the queue entry's `x-delivery-count` comes OUT, because that counter belongs
// to the entry rather than to the message and a broker that saw it in a
// publication would hand the next delivery a count that was not its own; and
// the attempt this delivery WAS goes in under [AttemptHeader], which is what
// replaces the count the new queue entry will not have.
//
// The attempt floors at one. A classic queue reports attempt zero -- "the
// broker is not counting" -- and a parked copy of such a delivery has still
// been delivered once, so carrying zero would say it had not.
//
// The table is COPIED rather than edited in place, because the delivery's own
// table is the library's storage and a caller may still be reading the message
// it belongs to.
func parkHeaders(src amqp.Table, plan *delay.Plan, attempt uint32) amqp.Table {
	if attempt == 0 {
		attempt = 1
	}
	out := make(amqp.Table, len(src)+2)
	for k, v := range src {
		if k == DeliveryCountHeader || k == AcquiredCountHeader {
			continue
		}
		out[k] = v
	}
	if plan.Header != "" {
		out[plan.Header] = plan.HeaderMillis
	}
	out[AttemptHeader] = int32(attempt)
	return out
}

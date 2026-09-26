package jetstream

import (
	"context"
	"strconv"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
)

// The settler implements the mandatory settlement surface and the optional lease
// extension.
//
// It does NOT implement [github.com/iqhive/iqmq.BatchSettler]. JetStream settles
// one delivery per message on that delivery's own reply subject; there is no
// batch ack verb, so a batch settler here would be a loop wearing a name that
// promises a saving, and [github.com/iqhive/iqmq.FeatureBatchSettle] says
// unsupported instead.
var (
	_ mq.Settler          = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// settler settles one source's deliveries.
//
// Every verb goes through the token table first, and that is the whole design:
// the table is what answers ErrLeaseLost for a delivery the broker has taken
// back, ErrAlreadySettled for one that is done and ErrSettlementInProgress for a
// race — before anything reaches the wire. On this broker the first of those is
// not a nicety: the reply subject of an expired delivery still WORKS, so a
// settlement that skipped the table would acknowledge a record another delivery
// owns.
type settler struct{ src *source }

// Ack settles a delivery positively: the message will not be redelivered.
//
// [Config.DoubleAck] decides the boundary. Without it the ack is a publish and
// the call returns as soon as it is in the write buffer, so a link that dies
// immediately afterwards leaves the message redeliverable — which is
// at-least-once behaving as advertised. With it the ack is a request and the
// call returns only once the server has recorded it, at the cost of a round trip
// per message.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	h, err := s.begin(d, mq.OpAck)
	if err != nil {
		return err
	}
	if s.src.b.cfg.DoubleAck {
		actx, cancel := s.src.b.bound(ctx)
		defer cancel()
		err = h.msg.DoubleAck(actx)
	} else {
		err = h.msg.Ack()
	}
	return s.finish(d, h, err, mq.OpAck)
}

// Nack settles a delivery negatively under o.
//
// The three actions are three different verbs on this broker and none of them is
// the others:
//
//   - Requeue is `-NAK`, or `-NAK {"delay":n}` when o.Delay is set. The server
//     makes the message deliverable again; nothing is held in this process.
//   - Drop is `+TERM`. It destroys the message, which is why the specification makes it
//     something a caller has to write.
//   - DeadLetter is a durable publication to [Config.DeadLetterTopic] followed
//     by `+TERM`, in that order. See deadLetter below.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if o.Action == mq.NackActionDeadLetter && s.src.b.facts.deadLetter == "" {
		// The specification: a backend with no dead-letter destination reports
		// ErrUnsupported and settles NOTHING, so the delivery is still
		// settleable and the message is not lost. Answering before the token
		// table is touched is what keeps that true.
		return errNoDeadLetter
	}
	h, err := s.begin(d, mq.OpNack)
	if err != nil {
		return err
	}
	switch o.Action {
	case mq.NackActionRequeue:
		if o.Delay > 0 {
			err = h.msg.NakWithDelay(o.Delay)
		} else {
			err = h.msg.Nak()
		}
		// A requeued message is coming back, so its slot goes with it: the
		// redelivery claims a new one and the token that named this delivery
		// is stale from here on.
		return s.finish(d, h, err, mq.OpNack)
	case mq.NackActionDrop:
		err = h.msg.Term()
		return s.finish(d, h, err, mq.OpNack)
	case mq.NackActionDeadLetter:
		return s.deadLetter(ctx, d, h, o)
	default:
		_ = s.src.table.Complete(d.Token, mq.ErrUnsupported)
		return mq.ErrUnsupported
	}
}

// deadLetter routes a rejected message to the dead-letter subject and only then
// terminates the original.
//
// **The order is the contract.** A `+TERM` that ran first would destroy the
// message in the stream, and a failure between the Term and the publication
// would lose it outright — the specification's "a failed or ambiguous dead-letter publish
// never acknowledges the original". So the publication goes first and it is
// DURABLE: this is js.PublishMsg, which returns after the stream has committed
// the copy, not a core publish that returns from the write buffer. Only once
// that PubAck is in hand does the Term go out.
//
// A publication that fails leaves the delivery settleable and settles nothing,
// so the message is still in the stream and still redeliverable. A publication
// that succeeded and a Term that then failed is the one outcome that can
// duplicate: the copy is durable and the original will come back. That is the
// right way round — the specification chooses a duplicate over a loss — and it is reported
// with the Term's own error rather than as a success.
func (s *settler) deadLetter(ctx context.Context, d *mq.DriverDelivery, h *held, o mq.NackOptions) error {
	dctx, cancel := s.src.b.bound(ctx)
	defer cancel()
	msg := s.deadLetterMessage(h, o)
	if _, err := s.src.b.js.PublishMsg(dctx, msg); err != nil {
		// Nothing was destroyed. The delivery goes back to active, which
		// lets the caller retry or nack it, and the message is still in the
		// stream.
		//
		// The MAPPED error is what the table is told, not the native one:
		// The specification makes an unknown outcome sticky, and the table decides that
		// from the portable code. A raw client error carries none, so a
		// publication whose outcome was genuinely ambiguous would be
		// recorded as a plain refusal and the delivery would look settleable
		// when nobody knows whether the copy exists.
		mapped := s.src.b.errs.MapNet(mq.OpNack, s.src.topic, s.src.subscription, err, true)
		_ = s.src.table.Complete(d.Token, mapped)
		return mapped
	}
	err := h.msg.Term()
	s.src.b.deadLettered.Add(1)
	return s.finish(d, h, err, mq.OpNack)
}

// deadLetterMessage is the copy that goes to the dead-letter subject.
//
// It carries the original's payload and headers unchanged, plus three reserved
// headers that say where it came from: a dead-letter destination whose messages
// do not name their origin, their attempt count and their reason is a queue
// somebody has to guess about.
func (s *settler) deadLetterMessage(h *held, o mq.NackOptions) *natsgo.Msg {
	hdr := make(natsgo.Header, len(h.msg.Headers())+3)
	for k, vals := range h.msg.Headers() {
		if IsNATSHeader(k) {
			// The server's own headers describe the message's place in the
			// stream it is leaving, so copying them onto a new publication
			// would be describing the wrong stream position — and the
			// server reads some of them as control.
			continue
		}
		for _, v := range vals {
			hdr.Add(k, v)
		}
	}
	hdr.Set(HeaderDeadLetterFrom, h.msg.Subject())
	hdr.Set(HeaderDeadLetterAttempt, strconv.FormatUint(h.delivered, 10))
	if o.Reason != "" && HeaderValueOK([]byte(o.Reason)) {
		// The specification makes Reason local diagnostics "unless the driver documents
		// that it transports it". This driver documents that it does, on
		// this route only, because a dead-letter queue with no reason in it
		// is the one place the diagnosis is worth the bytes.
		hdr.Set(HeaderDeadLetterReason, o.Reason)
	}
	return &natsgo.Msg{
		Subject: s.src.b.facts.deadLetter,
		Header:  hdr,
		// The payload is the client library's own slice, which lives as long
		// as the delivery does; PublishMsg is synchronous, so it is still
		// valid when the call returns.
		Data: h.msg.Data(),
	}
}

// Extend moves a delivery's lease, which on this broker is `+WPI`: the server
// resets the redelivery timer and the message stays this consumer's.
//
// It is NOT a settlement and must not behave like one, so it resolves the token
// without changing the slot's state — and a token the broker has taken back is
// still refused, because a handler that kept working on a delivery someone else
// owns is the duplicate the lease exists to prevent.
func (s *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	if by <= 0 {
		return errExtendRange
	}
	ref, err := s.src.table.Resolve(d.Token)
	if err != nil {
		s.countStale(err)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	h, ok := ref.(*held)
	if !ok || h == nil {
		return mq.ErrLeaseLost
	}
	// InProgress takes no context: it is a publish on the delivery's reply
	// subject and returns as soon as it is in the write buffer, so there is
	// no round trip for a deadline to bound. The caller's context is checked
	// above instead, which is what the specification asks of every call that takes one.
	if err := h.msg.InProgress(); err != nil {
		return s.src.b.errs.MapNet(mq.OpExtend, s.src.topic, s.src.subscription, err, true)
	}
	return nil
}

// begin takes the delivery's slot from active to settling and returns what the
// settlement needs.
//
// Everything a stale, duplicate or racing settlement has to be told happens
// here, before any I/O: the answers come from the table and not from the
// broker, because the broker would take the stale ack.
func (s *settler) begin(d *mq.DriverDelivery, op string) (*held, error) {
	ref, err := s.src.table.Settle(d.Token)
	if err != nil {
		s.countStale(err)
		return nil, err
	}
	h, ok := ref.(*held)
	if !ok || h == nil {
		_ = s.src.table.Complete(d.Token, mq.ErrLeaseLost)
		return nil, mq.ErrLeaseLost
	}
	_ = op
	return h, nil
}

// finish records the settlement's outcome and, when it was terminal, drops the
// record's index entry so that a later delivery of the same record does not
// release a slot that is already free.
//
// Every failure it maps is AMBIGUOUS, and that is not a parameter because it is
// not a choice: every settlement this driver makes is a message on a reply
// subject, so by the time an error is reported the bytes have left and the
// server may have taken the settlement. The specification makes that outcome ambiguous
// rather than failed, and a driver that reported it as definite would let a
// caller believe a redelivery was still coming.
func (s *settler) finish(d *mq.DriverDelivery, h *held, err error, op string) error {
	if err != nil {
		mapped := s.src.b.errs.MapNet(op, s.src.topic, s.src.subscription, err, true)
		_ = s.src.table.Complete(d.Token, mapped)
		return mapped
	}
	_ = s.src.table.Complete(d.Token, nil)
	s.src.forget(h.token, h.seq)
	return nil
}

// countStale records a settlement the guard refused, so that a deployment can
// see how often its handlers outlive their leases.
func (s *settler) countStale(err error) {
	if mq.CodeOf(err) == mq.CodeLeaseLost {
		s.src.b.stale.Add(1)
	}
}

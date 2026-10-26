package nsq

import (
	"context"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this settler is.
//
// There is no BatchSettler: nsqd has no batched settlement verb, and coalescing
// FIN frames into a "batch" would advertise a round trip this broker does not
// have.
var (
	_ mq.Settler          = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// settler settles one source's deliveries.
//
// Every settlement here is a frame written to the connection and never
// answered: nsqd replies to PUB, MPUB and DPUB and to nothing else. So the
// settlement boundary for this driver is "the FIN, REQ or TOUCH frame was
// handed to the connection", which the driver page states, and every
// correctness question a broker would otherwise answer has to be answered
// locally before the frame is written.
//
// That is what [delivery] and [source.inflight] are for. nsqd will accept a FIN
// for a message whose timeout has already expired, and after a redelivery that
// FIN finishes ANOTHER consumer's work — the specification's "lets a stale ack settle a
// later redelivery", which is a disqualifier and not a rough edge. There is no
// verb that asks "do I still hold this?", so the refusal happens here.
type settler struct {
	src *source
}

// Ack settles d positively: FIN.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	owed := s.src.takeOwed(ref)
	ref.msg.Finish()
	s.src.payOwed(owed)
	s.src.forget(ref)
	return nil
}

// Nack settles d negatively under o.
//
//   - NackActionRequeue is REQ with the option's delay, which is nsqd's own
//     delayed redelivery and needs no emulation.
//   - NackActionDrop is FIN. The specification calls it destructive and it is: nsqd's only
//     way to make a message go away is to finish it, and it does not go
//     anywhere afterwards.
//   - NackActionDeadLetter is refused. nsqd has no dead-letter route, so there
//     is nowhere native for the message to go — and emulating one in a driver
//     is exactly what the specification forbids doing in silence. The core's
//     mq.WithDeadLetter publishes and then acknowledges, which is the
//     emulation classes as semantics-preserving, and it needs nothing
//     from this driver.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o.Action == mq.NackActionDeadLetter {
		return errNoDeadLetter
	}
	if o.Delay < 0 {
		return errNegativeDelay
	}
	if o.Delay > s.src.b.cfg.MaxRequeueDelay {
		return errRequeueTooLong
	}
	ref, err := s.claim(d)
	if err != nil {
		return err
	}
	owed := s.src.takeOwed(ref)
	if o.Action == mq.NackActionDrop {
		ref.msg.Finish()
		s.src.payOwed(owed)
		s.src.forget(ref)
		return nil
	}
	// RequeueWithoutBackoff and not Requeue: go-nsq's Requeue sets the
	// consumer's RDY to zero for the whole subscription while a backoff of
	// its own runs, so one message's failure would stop every other
	// message. The delay is the core's and the client library does not get
	// a second one.
	ref.msg.RequeueWithoutBackoff(o.Delay)
	s.src.payOwed(owed)
	s.src.forget(ref)
	return nil
}

// Extend moves d's lease deadline by at least by: TOUCH.
//
// A TOUCH is NOT a settlement. It leaves the delivery settleable, so it takes
// the lease check without taking the delivery's state — which is the whole
// reason [settler.peek] exists beside [settler.claim].
//
// nsqd resets the message's timeout to the CHANNEL's configured timeout when it
// receives a TOUCH; it does not add `by` to what is left. So the local view
// moves to now plus this source's deadline, and `by` is honoured as a floor
// rather than as an amount: an extension longer than the channel's timeout
// cannot be had at all and is refused rather than silently shortened.
//
// A TOUCH for a delivery this source no longer holds is refused with
// ErrLeaseLost and NOTHING IS WRITTEN. It is the disqualifier in NSQ's
// clothing: nsqd accepts a TOUCH by message identifier, and after a redelivery
// that identifier is another consumer's message — so a TOUCH that "succeeded"
// would have extended somebody else's lease and told this caller its own was
// safe.
func (s *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if by <= 0 {
		// An extension of zero or less is not an extension. nsqd would
		// reset the timeout to the channel's default, which for a short
		// remaining lease is an extension the caller did not ask for and
		// for a long one is a shortening.
		return errExtendZero
	}
	if by > s.src.b.cfg.MaxAckDeadline {
		// nsqd caps a message's total in-flight time at --max-msg-timeout
		// from its first delivery, so an extension past that cannot be had
		// however many times it is asked for. Anything shorter is granted,
		// and what nsqd GRANTS is the channel's negotiated timeout from now
		// rather than the amount asked for: TOUCH is a reset and not an
		// addition. The capability table advertises that timeout as MaxLease, and the driver page states it, because the SPI
		// has no way for an extension to report what it actually bought.
		return errExtendTooLong
	}
	ref, err := s.peek(d)
	if err != nil {
		return err
	}
	ref.msg.Touch()
	if state := ref.state.Load(); state != stateActive {
		// The sweeper or Close gave the delivery back between the lease
		// check and the TOUCH, and go-nsq drops a TOUCH for a message it
		// has already answered: nothing was extended.
		return stateError(state)
	}
	// The deadline really has moved, so the local view has to move with it
	// or the next settlement would be refused against a deadline nsqd no
	// longer holds.
	ref.extend(s.src.b.now().Add(s.src.deadline))
	return nil
}

// claim takes a delivery terminal, applying the lease check before anything is
// written.
//
// The order is the order of the reasons. The generation and the supersession
// come first, because a delivery this source no longer vouches for must report
// ErrLeaseLost whatever its deadline says; then the deadline, because a FIN for
// an expired message succeeds at nsqd and takes somebody else's work; and only
// then the state machine, because a second settlement of a delivery that IS
// still ours is ErrAlreadySettled and not a lost lease.
func (s *settler) claim(d *mq.DriverDelivery) (*delivery, error) {
	ref, err := s.peek(d)
	if err != nil {
		return nil, err
	}
	if !ref.claim() {
		return nil, stateError(ref.state.Load())
	}
	return ref, nil
}

// peek recovers a delivery's bookkeeping and applies the lease check without
// changing its state, which is what an extension needs.
func (s *settler) peek(d *mq.DriverDelivery) (*delivery, error) {
	if d == nil {
		return nil, mq.ErrNotSettleable
	}
	ref, ok := d.Ref.(*delivery)
	if !ok || ref == nil || ref.src != s.src {
		// A delivery whose Ref is not one of this source's came from
		// another source or from a zero DriverDelivery. The specification makes that
		// ErrNotSettleable rather than a panic.
		return nil, mq.ErrNotSettleable
	}
	if ref.gen != d.Gen {
		// The core hands back its own copy of the struct, so the
		// generation in the Ref and the one in the delivery are the same
		// value written once. A mismatch is a delivery that was tampered
		// with, and applying a settlement from it would settle a message
		// this source cannot vouch for.
		return nil, mq.ErrLeaseLost
	}
	if state := ref.state.Load(); state != stateActive {
		return nil, stateError(state)
	}
	if ref.pastGuard(s.src.b.now(), s.src.b.cfg.LeaseGuard) {
		// Marked and refused, and NO frame is written here: the local
		// deadline has passed, so nsqd may already have redelivered and a
		// frame now would land on that redelivery if it is still on its
		// way in. The delivery stays in the source's map so that whichever
		// redelivery supersedes it answers for it or, failing that, the
		// lease sweeper does a sweep later; go-nsq's connection does not
		// close until every delivery has been answered (see
		// delivery.lostAndDue).
		if !ref.lose() {
			// A racer settled it between the state check above and here,
			// which is ErrAlreadySettled and not a lost lease.
			return nil, stateError(ref.state.Load())
		}
		s.src.b.stale.Add(1)
		return nil, mq.ErrLeaseLost
	}
	return ref, nil
}

// The settler's refusals.
var (
	errNoDeadLetter = errcode.New(errcode.Unsupported,
		"iqmq/nsq: nsqd has no dead-letter route, so NackDeadLetter cannot be honoured natively; use mq.WithDeadLetter, which publishes to a topic and then acknowledges")
	errNegativeDelay  = errcode.New(errcode.InvalidArgument, "iqmq/nsq: a requeue delay cannot be negative")
	errRequeueTooLong = errcode.New(errcode.InvalidArgument, "iqmq/nsq: the requeue delay is longer than MaxRequeueDelay, which mirrors nsqd's --max-req-timeout")
	errExtendZero     = errcode.New(errcode.InvalidArgument, "iqmq/nsq: an extension of zero or less is not an extension; nsqd would reset the timeout to the channel's default")
	errExtendTooLong  = errcode.New(errcode.InvalidArgument, "iqmq/nsq: the extension is longer than MaxAckDeadline, which mirrors nsqd's --max-msg-timeout: nsqd caps a message's total in-flight time at that from its first delivery")
)

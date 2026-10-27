package memory

import (
	"context"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/errcode"
)

// settler settles the deliveries of one source.
//
// It answers for the native action only. The core has already decided that
// this settlement is the one that may proceed — exactly one terminal attempt
// per delivery reaches a Settler — so nothing here keeps a second settlement
// state machine, which the specification forbids a driver from owning.
//
// Every method carries the generation the delivery was claimed under, which
// is what makes a stale settlement detectable: the engine answers a
// generation that is not the delivery's with ErrLeaseLost rather than
// applying it to the redelivery that took its place.
type settler struct{ src *source }

// Ack settles d positively: the record leaves the subscription and its
// payload is released once every subscription has settled it.
func (st *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	env, gen, err := claim(ctx, d)
	if err != nil {
		return err
	}
	return st.src.cons.Ack(env, gen)
}

// Nack settles d negatively under o.
//
// The three actions reach three different places:
//
//   - Requeue returns the record to its lane, immediately or through the
//     delayed structure when o.Delay is positive. The record keeps its
//     attempt count, which the next delivery increments, and the retry
//     placement kind decides where in the lane it lands.
//   - DeadLetter publishes the message to the topic's dead-letter topic and
//     then acknowledges the delivery, which is ADR-008's publish-then-ack
//     emulation — semantics-preserving, and available under the default
//     SafeOnly policy. A topic with no dead-letter destination answers
//     ErrUnsupported and settles nothing, which is what the specification requires of an
//     action a driver cannot honour.
//   - Drop takes the engine's terminal settlement, which is the same native
//     action an acknowledgement takes: the record leaves the subscription,
//     its payload is released and it is never redelivered. Most brokers are
//     the same — a delete or an offset commit serves both verbs — and the
//     difference is iqmq's own accounting, which the consumer's Stats keeps:
//     Snapshot.Acked counts a dropped record.
func (st *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	env, gen, err := claim(ctx, d)
	if err != nil {
		return err
	}
	switch o.Action {
	case mq.NackActionRequeue:
		return st.src.cons.Nack(env, gen, o.Delay)
	case mq.NackActionDrop:
		return st.src.cons.Ack(env, gen)
	case mq.NackActionDeadLetter:
		return st.deadLetter(ctx, d, env, gen)
	default:
		return mq.ErrUnsupported
	}
}

// Extend moves d's lease deadline to at least by from now.
//
// The scheduler that drives the expiry is one per broker and never one timer
// per delivery, so an extension costs a list move rather than a timer
// reset.
func (st *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	env, gen, err := claim(ctx, d)
	if err != nil {
		return err
	}
	return st.src.cons.Extend(env, gen, by)
}

// deadLetter is the terminal negative settlement: the message is published
// to the dead-letter destination and the delivery is then acknowledged.
//
// The order is what makes it safe to lose the process between the two steps:
// a publication that failed leaves the delivery unsettled and redeliverable,
// which is the specification's "a failed or ambiguous dead-letter publish never acks". The
// duplicate risk is the other half and it is disclosed rather than hidden: a
// publication that succeeded and an acknowledgement that did not leaves the
// message in both places.
func (st *settler) deadLetter(
	ctx context.Context,
	d *mq.DriverDelivery,
	env *engine.Envelope[*mq.Message],
	gen uint32,
) error {
	name := st.src.topic.Topology().DeadLetterTopic
	if name == "" || name == noDeadLetterTopic {
		return mq.ErrUnsupported
	}
	dlq, err := st.src.b.eng.Topic(name)
	if err != nil {
		// The topology named a topic that does not exist. The engine never
		// creates one behind the operator's back, so the action cannot be
		// honoured and nothing is settled.
		return mq.ErrUnsupported
	}
	if d.Message == nil {
		return errNilMessage
	}
	m, mode := d.Message, engine.CopySafe
	if st.src.meta == metaEnvelope {
		// The delivery holds the decoded message, while the engine's own
		// dead-letter route publishes the stored one, still enveloped. Both
		// routes must land in the dead-letter topic in one format, or a
		// source reading it in this mode could not decode what this one
		// sent. The envelope is built here and nobody else holds it, so the
		// engine takes it rather than copying it.
		var err error
		if m, err = envelope(st.src.b.envelope, d.Message); err != nil {
			return err
		}
		mode = engine.CopyOwned
	}
	if err := dlq.Publish(ctx, engine.Publish[*mq.Message]{
		Payload:   m,
		Key:       d.Message.Key,
		Size:      messageSize(m),
		Partition: -1,
		Mode:      mode,
	}); err != nil {
		return err
	}
	return st.src.cons.Ack(env, gen)
}

// noDeadLetterTopic is the spelling the engine reserves for "no dead-letter
// topic, whatever the topic says". It is not a topic name any caller can
// create, which is what makes it usable as a sentinel.
const noDeadLetterTopic = "/none"

// claim recovers the engine handle a delivery carries.
//
// A DriverDelivery the core did not fill in — a zero value, or one a caller
// built — names no record, and a settlement of it is a protocol failure
// rather than a lease that was lost: the two are different answers and the specification
// gives the caller ErrNotSettleable for a delivery that cannot be settled at
// all.
func claim(ctx context.Context, d *mq.DriverDelivery) (*engine.Envelope[*mq.Message], uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if d == nil {
		return nil, 0, errNoDeliveryHandle
	}
	env, ok := d.Ref.(*engine.Envelope[*mq.Message])
	if !ok || env == nil {
		return nil, 0, errNoDeliveryHandle
	}
	return env, d.Gen, nil
}

// errNoDeliveryHandle is what a settlement of a delivery this driver never
// handed out is refused with.
var errNoDeliveryHandle = errcode.New(errcode.NotSettleable,
	"iqmq/memory: the delivery carries no memory-engine handle")

package stomp

import (
	"context"
	"errors"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/activemq"
)

// settler is one source's settlement surface (the winner).
//
// # The mapping, and why the obvious one is wrong
//
// The specification has four verbs and STOMP has two frames, and the obvious pairing --
// Ack to ACK, Nack to NACK -- is a silent data defect on both brokers.
// Measured (doc.go has the numbers): on Artemis a NACK frame is treated
// exactly as an ACK, and on ActiveMQ Classic it is a POISON_ACK that
// dead-letters. Neither returns the message to the queue. So a driver that
// mapped Nack to NACK would report a requeue and destroy the message.
//
// What this does instead:
//
//	Ack                 ACK frame.
//	Nack(requeue)       a subscription RESET. The broker redelivers every
//	                    unacknowledged delivery of the subscription, which
//	                    is the protocol's own recovery and is measured to
//	                    work on both brokers.
//	Nack(requeue,delay) refused. Neither broker reads a delay on a negative
//	                    settlement, and a reset is answered at once.
//	Nack(drop)          NACK frame, whose meaning differs by broker and
//	                    whose difference the capability note states.
//	Nack(deadLetter)    a publication to Config.DeadLetterTopic and then an
//	                    ACK, in that order; or the NACK frame on Classic,
//	                    whose poison-ack reaches the broker's own
//	                    dead-letter strategy.
//
// # The staleness guard
//
// A settlement is refused when the delivery's SUBSCRIPTION is no longer the
// source, which is the rule ("a settlement must never apply to a
// delivery other than the one it names") in this protocol's terms: a STOMP ACK
// names a delivery by the `ack` header the broker sent, and that identifier
// belongs to the subscription it came from. A reset or a reconnection makes
// every one of them somebody else's, so the token table's generation is
// what decides, and the answer is [mq.ErrLeaseLost].
type settler struct{ src *Source }

var _ mq.Settler = (*settler)(nil)

// Ack acknowledges the delivery.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	h, err := s.begin(d)
	if err != nil {
		return err
	}
	conn, err := s.conn(ctx, h)
	if err != nil {
		return s.finish(d, nil, err, mq.OpAck)
	}
	return s.finish(d, conn, conn.Ack(h.msg), mq.OpAck)
}

// Nack settles the delivery negatively.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	// Both refusals happen BEFORE the token table is touched, so that a
	// delivery this driver will not settle stays settleable: a caller told
	// "unsupported" has to be able to do something else with it.
	if o.Action == mq.NackActionRequeue && o.Delay > 0 {
		return s.src.b.errs.Map(mq.OpNack, s.src.topic, s.src.sub, errNoDelayedRedelivery)
	}
	if o.Action == mq.NackActionDeadLetter && !s.canDeadLetter() {
		return s.src.b.errs.Map(mq.OpNack, s.src.topic, s.src.sub, errNoDeadLetterRoute)
	}
	h, err := s.begin(d)
	if err != nil {
		return err
	}
	switch o.Action {
	case mq.NackActionRequeue:
		// The reset invalidates every token including this one, so the
		// delivery is finished with as soon as the reset is under way: a
		// token table entry kept past it would be a slot nothing can ever
		// settle.
		if err := s.src.reset(ctx); err != nil {
			return err
		}
		return nil
	case mq.NackActionDrop:
		conn, cerr := s.conn(ctx, h)
		if cerr != nil {
			return s.finish(d, nil, cerr, mq.OpNack)
		}
		return s.finish(d, conn, conn.Nack(h.msg), mq.OpNack)
	case mq.NackActionDeadLetter:
		return s.deadLetter(ctx, d, h, o)
	}
	return s.src.b.errs.Map(mq.OpNack, s.src.topic, s.src.sub, mq.ErrUnsupported)
}

// canDeadLetter reports whether this configuration has a dead-letter route.
//
// Classic's NACK frame is a poison-ack and reaches the broker's own strategy,
// so it counts; Artemis's destroys the message, so it does not and a route of
// this driver's own is the only answer there.
func (s *settler) canDeadLetter() bool {
	if s.src.b.cfg.DeadLetterTopic != "" {
		return true
	}
	return s.src.b.cfg.Broker == activemq.Classic
}

// deadLetter publishes the delivery to the configured topic and then
// acknowledges the original.
//
// **The order is the contract.** The specification: "a failed or ambiguous dead-letter
// publish never acknowledges the original". So the publication goes first and
// it waits for its RECEIPT -- not for the frame to be written -- and the ACK
// follows only if that receipt arrived. A driver that acknowledged first and
// published second would lose the message whenever the second step failed.
func (s *settler) deadLetter(ctx context.Context, d *mq.DriverDelivery, h *held, o mq.NackOptions) error {
	if s.src.b.cfg.DeadLetterTopic == "" {
		// Classic: the NACK frame is a poison-ack and the broker's own
		// dead-letter strategy is the route.
		conn, cerr := s.conn(ctx, h)
		if cerr != nil {
			return s.finish(d, nil, cerr, mq.OpNack)
		}
		return s.finish(d, conn, conn.Nack(h.msg), mq.OpNack)
	}
	snk, err := s.src.deadLetterSink(ctx)
	if err != nil {
		return s.deadLetterFailed(d, s.src.b.errs.Map(mq.OpNack, s.src.topic, s.src.sub, err))
	}
	out := &mq.Message{Body: h.msg.Body}
	if o.Reason != "" {
		out.Headers.Add(DeadLetterReasonHeader, []byte(o.Reason))
	}
	out.Headers.Add(DeadLetterAttemptHeader, []byte(itoa(int(d.Info.Attempt))))
	// The copy needs no bound of its own: the backend's connection carries
	// Config.ReceiptTimeout as both its send and its receipt timeout, so a
	// broker that never answers ends the publication rather than holding the
	// delivery in settling.
	if err := snk.Publish(ctx, out, nil); err != nil {
		// The original is NOT acknowledged. The specification again: the caller keeps
		// the delivery and can try again, and the broker still has it.
		return s.deadLetterFailed(d, s.src.b.errs.Map(mq.OpNack, s.src.topic, s.src.sub, err))
	}
	conn, cerr := s.conn(ctx, h)
	if cerr != nil {
		return s.finish(d, nil, cerr, mq.OpNack)
	}
	return s.finish(d, conn, conn.Ack(h.msg), mq.OpNack)
}

// deadLetterFailed records a dead-letter route that failed before the original
// was settled, and leaves the delivery settleable.
//
// Mapped under OpNack, a receipt that never came reads as SettlementUnknown and
// a dead backend connection as LeaseLost, and both are wrong here: no frame was
// written for the original, so the delivery is still this source's on its own
// connection whatever became of the copy. An unknown outcome parks the slot for
// ever and a lost lease makes the core forget a delivery the broker still
// counts against this consumer's prefetch. The answer is a definite, retryable
// refusal, so the caller can try again or settle it another way; the cost is a
// second dead-letter copy when the first one was in fact taken.
func (s *settler) deadLetterFailed(d *mq.DriverDelivery, mapped error) error {
	var oe *mq.OpError
	if errors.As(mapped, &oe) {
		oe.Ambiguous = false
		if oe.Code == mq.CodeLeaseLost || oe.Code == mq.CodeSettlementUnknown {
			oe.Code, oe.Retryable = mq.CodeUnavailable, true
		}
	}
	_ = s.src.table.Complete(d.Token, mapped)
	return mapped
}

// The headers a dead-letter publication carries, so that the reason a message
// was rejected survives the hop.
//
// They are NOT under the `x-iqmq-` reserved prefix, and the reason is a defect
// the conformance suite caught: the shared mapping REFUSES a header under that
// prefix, because the prefix is how it carries the four slots neither protocol
// has and a caller's header there would collide with one. The mapping cannot
// tell this driver's header from a caller's -- both arrive through
// `Message.Headers` -- so a dead-letter publication carrying `x-iqmq-…` was
// refused with "the header key is under the reserved prefix" and the ORIGINAL
// was then correctly not acknowledged, which is a dead-letter route that never
// works reported working.
//
// The cost of the names below is stated rather than hidden: they are ordinary
// application headers, so a caller who sets one of them itself will have it
// overwritten on a dead-letter hop. `internal/rabbit/protocol`'s own note
// records the same shape from the other side -- four things travelling under
// one reserved prefix is a sign the header model is a field short.
const (
	// DeadLetterReasonHeader carries NackOptions.Reason.
	DeadLetterReasonHeader = "iqmq-dead-letter-reason"
	// DeadLetterAttemptHeader carries the attempt the message failed on.
	DeadLetterAttemptHeader = "iqmq-dead-letter-attempt"
)

// begin claims the delivery's slot and refuses a stale one.
//
// The three-step guard is `driverkit.TokenTable`'s: the slot moves
// active → settling → settled, so a second settlement of the same delivery is
// refused rather than sent twice, and a slot whose generation has moved is a
// delivery that is no longer this subscription's.
func (s *settler) begin(d *mq.DriverDelivery) (*held, error) {
	if d == nil {
		return nil, errNotSettleable
	}
	s.src.mu.Lock()
	closed, subGen := s.src.closed, s.src.subGen
	s.src.mu.Unlock()
	if closed {
		return nil, s.src.b.errs.Map(mq.OpAck, s.src.topic, s.src.sub, errClosed)
	}
	ref, err := s.src.table.Settle(d.Token)
	if err != nil {
		if errIsLeaseLost(err) {
			s.src.stale.Add(1)
		}
		return nil, err
	}
	h, ok := ref.(*held)
	if !ok || h == nil {
		_ = s.src.table.Release(d.Token)
		return nil, mq.ErrLeaseLost
	}
	if h.subGen != subGen || h.gen != s.src.b.Generation() || d.Gen != h.gen {
		// The subscription this delivery came from is not the one this
		// source has now: either a requeue reset it or the connection went.
		// Its `ack` identifier belongs to a subscription the broker has
		// taken back, so sending it would either be refused or -- worse on
		// a broker that reuses identifiers -- settle somebody else's
		// delivery, which is the disqualifier.
		_ = s.src.table.Release(d.Token)
		s.src.stale.Add(1)
		return nil, mq.ErrLeaseLost
	}
	return h, nil
}

// finish records the settlement's outcome.
//
// A failure is mapped with `sentAlready` set, because by the time an ACK frame
// fails it has been handed to the library and may have left. errors.go decides
// which side that lands on: a receipt that never came is an UNKNOWN
// outcome and permanent, and a connection that died is a LOST LEASE -- the
// broker still holds the delivery and will hand it to the next subscription.
//
// # conn is the socket the frame was written on, and nil when there was none
//
// A settlement goes out on the SOURCE's own connection , so
// a connection death met here is that source's to repair. This used to report
// it through the backend's own teardown -- `Backend.connectionLostFor` with a
// nil connection, which means "tear down whatever the backend has" --
// which disconnected a healthy connection no part of this settlement had
// touched, bumped the generation every other handle compares against, and
// invalidated every other source's deliveries and every sink, for a socket
// none of them was using. `Source.fail` states the rule this broke in the
// same file.
//
// A nil conn is a settlement that never reached a socket, and there are FOUR
// such call sites, each where `settler.conn` found the delivery's connection
// gone. None of them is a death of anything this method can name, so none of
// them reports one.
//
// The dead-letter route's own failures never reach here at all: a sink that
// could not be opened and a publication that failed both leave the original
// unacknowledged and settleable, which is [settler.deadLetterFailed] -- and the
// sink a publication failed in reports its own connection itself
// (`Sink.Publish`, through [Backend.connectionLostFor]).
func (s *settler) finish(d *mq.DriverDelivery, conn *gostomp.Conn, err error, op string) error {
	if err == nil {
		_ = s.src.table.Complete(d.Token, nil)
		return nil
	}
	mapped := s.src.b.errs.MapNet(op, s.src.topic, s.src.sub, err, true)
	switch mq.CodeOf(mapped) {
	case mq.CodeSettlementUnknown:
		// The specification makes an ambiguous settlement PERMANENT: the slot is kept so
		// that the delivery can never be settled again by accident, which is
		// what `Complete` with an unknown error does.
		_ = s.src.table.Complete(d.Token, mapped)
	case mq.CodeLeaseLost:
		_ = s.src.table.Release(d.Token)
		s.src.stale.Add(1)
	default:
		// A definite refusal leaves the delivery settleable: the caller can
		// try again or do something else with it.
		_ = s.src.table.Complete(d.Token, mapped)
	}
	if conn != nil && isConnGone(err) {
		// The source's own repair, with the identity check `socketDied`
		// carries: the pump may have seen the same death and the next fetch
		// may already have re-dialled, and acting then would drop the
		// connection that works.
		s.src.socketDied(conn)
	}
	return mapped
}

// conn is the connection h came in on, or the reason there is none.
//
// A settlement has to go out on the connection the delivery came in on: a
// STOMP ACK names a delivery by the `ack` header its subscription sent, and
// that identifier means nothing on another connection. So it is the
// delivery's own connection and never the source's current one: asking the
// source re-dialled when the connection had died after [settler.begin], and
// the frame went out on a socket with no such subscription. A delivery whose
// connection the source no longer holds has lost its lease.
func (s *settler) conn(ctx context.Context, h *held) (*gostomp.Conn, error) {
	if err := s.src.b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	s.src.mu.Lock()
	live := h.conn != nil && s.src.conn == h.conn
	s.src.mu.Unlock()
	if !live {
		return nil, mq.ErrLeaseLost
	}
	return h.conn, nil
}

// errIsLeaseLost reports whether the token table refused because the slot's
// generation had moved.
func errIsLeaseLost(err error) bool { return mq.CodeOf(err) == mq.CodeLeaseLost }

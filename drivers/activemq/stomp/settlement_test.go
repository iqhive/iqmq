package stomp_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-stomp/stomp/v3/frame"

	mq "bitbucket.org/iqhive/iqmq"
	stomp "bitbucket.org/iqhive/iqmq/drivers/activemq/stomp"
	"bitbucket.org/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

// the four verbs against the wire, asserted on the FRAMES rather than on
// the return values.
//
// The return value is the weaker half of the contract on this protocol,
// because the dangerous answers are the ones that succeed: an `Ack` that sent
// a NACK frame, or a `Nack(drop)` that sent an ACK, would both report success
// and both destroy or keep the message backwards. So each case here says what
// reached the broker.
//
// Every settlement is fire-and-forget on this driver, and that is worth
// stating because it decides how these tests are written. go-stomp's
// `Conn.Ack` and `Conn.Nack` take no options, so neither frame can carry a
// `receipt` header at all — there is no way to ask for one — and the call
// therefore returns as soon as the frame is queued on the library's write
// channel. A test that asserted on the frame log immediately after would be
// racing the library's own I/O goroutine, so each one waits for the frame.
//
// That is also why `doc.go`'s "the at-least-once window runs from the SEND
// frame until the ACK's own RECEIPT" describes something this library cannot
// do, and why the `receipt` rules `errors.go` puts on OpAck and OpNack are
// unreachable from a settlement frame: the only way to reach them would be to
// build the ACK frame without `Conn.Ack`, which means re-implementing the
// library's write channel.

// stompFixture is a server, a backend and one subscribed source.
type stompFixture struct {
	srv     *stompsrv.Server
	src     *stomp.Source
	consume string
}

// settleFixture starts a server for the broker the configuration names, opens
// a backend and one subscribed source against it, and hands over one delivery
// the caller holds.
func settleFixture(t *testing.T, cfg stomp.Config) (*stompFixture, mq.DriverDelivery) {
	t.Helper()
	srv := brokerServer(t, cfg.Broker)
	cfg.Addrs = []string{srv.Addr()}
	src, consume := openSource(t, openBackend(t, cfg), cfg, srv, "orders", "workers", 8)
	f := &stompFixture{srv: srv, src: src, consume: consume}
	deliver(t, srv, consume, body("one"))
	return f, fetchN(t, src, 1)[0]
}

// TestSettlementAckSendsAnAckFrame is the positive verb.
func TestSettlementAckSendsAnAckFrame(t *testing.T) {
	t.Parallel()
	f, d := settleFixture(t, settings(activemq.Artemis))
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := f.src.Settler().Ack(ctx, &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := f.srv.WaitFrames(frame.ACK, 1, budget); err != nil {
		t.Fatalf("no ACK frame reached the server: %v\nframes:%s", err, frameLog(f.srv))
	}
	if got := countOf(f.srv, frame.NACK); got != 0 {
		t.Errorf("an Ack sent %d NACK frame(s). On Artemis a NACK is measured to be treated as an ACK, so the "+
			"message would vanish and the test would otherwise look the same.\nframes:%s", got, frameLog(f.srv))
	}
	// The ACK names the delivery by the `ack` header the MESSAGE frame
	// carried, which is STOMP 1.2's identifier. A driver that named it any
	// other way would settle nothing, or something else.
	ids := f.srv.SettledIDs(frame.ACK)
	if len(ids) != 1 || ids[0] != "srv-ack-1" {
		t.Errorf("the ACK names %v, want the delivery's own `ack` header. STOMP 1.2 identifies a delivery by that "+
			"header and by nothing else -- a 1.0-style `subscription` plus `message-id` pair would be ignored",
			ids)
	}
	// A second settlement of the same delivery is refused rather than sent
	// twice, which is the driver's own half of the rule.
	if err := f.src.Settler().Ack(ctx, &d); err == nil {
		t.Error("acknowledging the same delivery twice reported success")
	}
	if got := countOf(f.srv, frame.ACK); got != 1 {
		t.Errorf("the server received %d ACK frames after two Ack calls, want 1", got)
	}
}

// TestSettlementNackDropSendsANackFrame is the only verb that maps onto the
// NACK frame, and the note in the capability table is what makes it honest.
//
// `Nack(NackDrop())` asks for the message to be DESTROYED, and that is the one
// thing both brokers' NACK frames really do: on Artemis it is treated as an
// acknowledgement, which destroys the message, and on Classic it is a
// poison-ack, which takes it to the broker's dead-letter strategy. Those are
// not the same outcome, and the difference is stated in the table rather than
// hidden — but neither returns the message, which is what `drop` promises.
func TestSettlementNackDropSendsANackFrame(t *testing.T) {
	t.Parallel()
	f, d := settleFixture(t, settings(activemq.Artemis))
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDrop())); err != nil {
		t.Fatalf("Nack(NackDrop()): %v", err)
	}
	if err := f.srv.WaitFrames(frame.NACK, 1, budget); err != nil {
		t.Fatalf("no NACK frame reached the server: %v\nframes:%s", err, frameLog(f.srv))
	}
	if got := countOf(f.srv, frame.ACK); got != 0 {
		t.Errorf("a drop sent %d ACK frame(s).\nframes:%s", got, frameLog(f.srv))
	}
	if got := countOf(f.srv, frame.UNSUBSCRIBE); got != 0 {
		t.Errorf("a drop reset the subscription (%d UNSUBSCRIBE frames). A reset RETURNS the message, which is the "+
			"opposite of what drop asks for, and it would also invalidate every other delivery the caller "+
			"holds.\nframes:%s", got, frameLog(f.srv))
	}
	if ids := f.srv.SettledIDs(frame.NACK); len(ids) != 1 || ids[0] != "srv-ack-1" {
		t.Errorf("the NACK names %v, want the delivery's own `ack` header", ids)
	}
}

// TestSettlementDelayedRequeueIsRefusedBeforeTheTokenTable is the refusal that
// has to happen in the right ORDER, not just at all.
//
// The specification: "A driver that cannot honour o's action returns ErrUnsupported and
// settles nothing." Settling nothing means the delivery is still the caller's:
// they were told this driver will not do that, so they have to be able to do
// something else with it. A driver that claimed the token-table slot first and
// then refused would leave the slot in `settling` for ever — the delivery
// unsettleable and the slot consumed — so the assertion is not only that the
// refusal happens but that an ordinary Ack works afterwards.
//
// The refusal itself is honest rather than lazy: neither broker reads a delay
// on a negative settlement, and this driver's requeue is a subscription reset
// the broker answers at once, so there is nowhere to put the delay.
func TestSettlementDelayedRequeueIsRefusedBeforeTheTokenTable(t *testing.T) {
	t.Parallel()
	f, d := settleFixture(t, settings(activemq.Artemis))
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	err := f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue(), mq.NackDelay(budget)))
	if err == nil {
		t.Fatalf("a delayed requeue reported success. Neither broker reads a delay on a negative settlement and "+
			"this driver's requeue is a subscription reset the broker answers at once, so the delay would be "+
			"silently ignored and the message would come back immediately.\nframes:%s", frameLog(f.srv))
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the refusal is %v (code %v), want ErrUnsupported: the specification names that answer specifically, and the "+
			"core's nack policy reads it to decide whether to emulate", err, mq.CodeOf(err))
	}
	if got := countOf(f.srv, frame.NACK) + countOf(f.srv, frame.ACK) + countOf(f.srv, frame.UNSUBSCRIBE); got != 0 {
		t.Errorf("the refused settlement sent %d settlement frames; the specification says it settles NOTHING.\nframes:%s",
			got, frameLog(f.srv))
	}
	if got := f.src.Resets(); got != 0 {
		t.Errorf("Source.Resets() is %d after a refused settlement, want 0", got)
	}

	// The delivery is still settleable, which is the half a token table
	// touched too early would break.
	if err := f.src.Settler().Ack(ctx, &d); err != nil {
		t.Fatalf("the delivery could not be acknowledged after a refused delayed requeue: %v. The refusal has to "+
			"happen BEFORE the token table is claimed, or the slot is left in `settling` and the delivery can "+
			"never be settled again", err)
	}
	if err := f.srv.WaitFrames(frame.ACK, 1, budget); err != nil {
		t.Fatalf("the ACK after the refused settlement never reached the server: %v", err)
	}
}

// TestSettlementDeadLetterWithNoRoute is the same refusal for the other
// action, and the answer differs by broker because the BROKERS differ.
//
// On ActiveMQ Classic a NACK frame is a POISON_ACK and reaches the broker's
// own dead-letter strategy (ActiveMQ.DLQ by default), so the frame really is a
// dead-letter route and `Nack(NackDeadLetter())` sends it. On Artemis a NACK
// ACKNOWLEDGES the delivery — measured — so there is no frame that reaches a
// dead-letter address at all, and the honest answer is a refusal rather than a
// NACK that quietly destroys the message.
func TestSettlementDeadLetterWithNoRoute(t *testing.T) {
	t.Parallel()

	t.Run("artemis refuses, because a NACK there acknowledges", func(t *testing.T) {
		t.Parallel()
		f, d := settleFixture(t, settings(activemq.Artemis))
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		err := f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter()))
		if err == nil {
			t.Fatalf("a dead-letter settlement with no route reported success on Artemis. A NACK frame there is "+
				"measured to acknowledge the delivery rather than dead-letter it, so the message would be "+
				"destroyed and the caller told it had been kept.\nframes:%s", frameLog(f.srv))
		}
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("the refusal is %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
		}
		if got := countOf(f.srv, frame.NACK) + countOf(f.srv, frame.ACK); got != 0 {
			t.Errorf("the refused settlement sent %d settlement frames.\nframes:%s", got, frameLog(f.srv))
		}
		// And it settles nothing, so the caller can still act.
		if err := f.src.Settler().Ack(ctx, &d); err != nil {
			t.Errorf("the delivery could not be acknowledged after a refused dead-letter settlement: %v", err)
		}
	})

	t.Run("classic sends the NACK, whose poison-ack is the route", func(t *testing.T) {
		t.Parallel()
		f, d := settleFixture(t, settings(activemq.Classic))
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		if err := f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter())); err != nil {
			t.Fatalf("Nack(NackDeadLetter()) on Classic: %v", err)
		}
		if err := f.srv.WaitFrames(frame.NACK, 1, budget); err != nil {
			t.Fatalf("no NACK frame reached the server: %v\nframes:%s", err, frameLog(f.srv))
		}
		if got := countOf(f.srv, frame.ACK); got != 0 {
			t.Errorf("the dead-letter settlement also acknowledged the original (%d ACK frames), which would "+
				"destroy it: on Classic the NACK IS the route.\nframes:%s", got, frameLog(f.srv))
		}
		if got := countOf(f.srv, frame.SEND); got != 0 {
			t.Errorf("the driver published %d messages for a dead-letter settlement with no route configured. "+
				"Classic's own dead-letter strategy is the route, and a publication of the driver's own would "+
				"put the message somewhere the deployment did not ask for.\nframes:%s", got, frameLog(f.srv))
		}
	})
}

// TestSettlementDeadLetterPublishesFirstAndAcksSecond is ORDER, and the
// server's frame log is what proves it.
//
// "A failed or ambiguous dead-letter publish never acknowledges the original"
// is an ordering statement, and the only way to check an ordering is to look
// at the sequence. A driver that acknowledged first and published second would
// pass every assertion about the two frames existing and would lose the
// message on the day the publication failed, which is the case
// [TestCriticalFailedDeadLetterPublishDoesNotAcknowledge] covers from the
// other side.
//
// It runs under the strict default, which for most of this increment it could
// not: `settler.deadLetter` carried the reason under the reserved `x-iqmq-`
// prefix, which the shared mapping refuses outright, so there was no
// publication to order at all. The header names left the prefix.
func TestSettlementDeadLetterPublishesFirstAndAcksSecond(t *testing.T) {
	t.Parallel()
	cfg := settings(activemq.Artemis)
	cfg.DeadLetterTopic = "orders-dead"
	f, d := settleFixture(t, cfg)
	deadLetter, _ := destinations(t, cfg, "orders-dead", "")

	mark := f.srv.Next()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := f.src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter(), mq.NackReason("poison"))); err != nil {
		t.Fatalf("Nack(NackDeadLetter()) with a configured route: %v", err)
	}
	if err := f.srv.WaitFrames(frame.ACK, 1, budget); err != nil {
		t.Fatalf("the original was never acknowledged after a successful dead-letter publication: %v\nframes:%s",
			err, frameLog(f.srv))
	}
	got := f.srv.Commands(mark)
	if len(got) != 2 || got[0] != frame.SEND || got[1] != frame.ACK {
		t.Fatalf("the dead-letter settlement sent %v, want exactly [%s %s]. The order IS the contract: the specification "+
			"forbids acknowledging the original until the dead-lettered copy is the broker's, and a driver that "+
			"acknowledged first would lose the message whenever the second step failed.\nframes:%s",
			got, frame.SEND, frame.ACK, frameLog(f.srv))
	}
	sends := f.srv.FramesOf(frame.SEND)
	if len(sends) != 1 || sends[0].Get(frame.Destination) != deadLetter {
		t.Errorf("the dead-letter publication went to %q, want %q", sends[0].Get(frame.Destination), deadLetter)
	}
	if !sends[0].Has(frame.Receipt) {
		t.Error("the dead-letter publication carries no `receipt` header. The boundary is the broker HAVING the " +
			"dead-lettered copy, and a SEND with no receipt is fire-and-forget: the ACK would follow a frame " +
			"nobody confirmed")
	}
	if got := string(sends[0].Body); got != "one" {
		t.Errorf("the dead-letter publication carries %q, want the original's body %q", got, "one")
	}
	if got := countOf(f.srv, frame.NACK); got != 0 {
		t.Errorf("the driver also sent %d NACK frame(s) on Artemis, where a NACK acknowledges: that is the loss "+
			"The specification forbids, by another route.\nframes:%s", got, frameLog(f.srv))
	}
}

// TestSettlementRejectIsADrop is the fourth verb.
//
// `Reject` is not a separate SPI method: the core maps it onto `Nack` with the
// drop action, so what this pins is that the driver's answer to the action is
// the NACK frame and not the reset — a driver that reset the subscription for
// a rejection would RETURN the message it was told to destroy, and would
// invalidate every other delivery the caller held while doing it.
func TestSettlementRejectIsADrop(t *testing.T) {
	t.Parallel()
	f, d := settleFixture(t, settings(activemq.Artemis))
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var o mq.NackOptions
	o.Action = mq.NackActionDrop
	if err := f.src.Settler().Nack(ctx, &d, o); err != nil {
		t.Fatalf("Nack with the drop action: %v", err)
	}
	if err := f.srv.WaitFrames(frame.NACK, 1, budget); err != nil {
		t.Fatalf("no NACK frame reached the server: %v\nframes:%s", err, frameLog(f.srv))
	}
	if got := countOf(f.srv, frame.UNSUBSCRIBE); got != 0 {
		t.Errorf("a rejection reset the subscription.\nframes:%s", frameLog(f.srv))
	}
}

// TestSettlementUnknownActionIsRefused is the guard on the enumeration itself.
//
// An action outside the three is a caller bug or a core bug, and the driver
// must not fall through to a default that does something. Falling through to
// the requeue arm would reset the subscription for an unknown verb, which
// returns every delivery the consumer holds.
func TestSettlementUnknownActionIsRefused(t *testing.T) {
	t.Parallel()
	f, d := settleFixture(t, settings(activemq.Artemis))
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := f.src.Settler().Nack(ctx, &d, mq.NackOptions{Action: mq.NackAction(9)})
	if err == nil {
		t.Fatalf("an action outside the three was accepted.\nframes:%s", frameLog(f.srv))
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("the refusal is %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
	if got := countOf(f.srv, frame.UNSUBSCRIBE); got != 0 {
		t.Errorf("an unknown action reset the subscription, which RETURNS every delivery the consumer holds; a "+
			"default arm that fell through to the requeue would do exactly that.\nframes:%s", frameLog(f.srv))
	}
}

// TestSettlementAfterCloseIsRefused is a closed source settles nothing.
func TestSettlementAfterCloseIsRefused(t *testing.T) {
	t.Parallel()
	f, d := settleFixture(t, settings(activemq.Artemis))
	closeSource(t, f.src)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := f.src.Settler().Ack(ctx, &d); err == nil {
		t.Fatalf("a closed source acknowledged a delivery. Closing UNSUBSCRIBES, which is what makes the broker "+
			"redeliver everything this consumer never acknowledged -- so an ACK afterwards would name a "+
			"subscription the broker has taken back.\nframes:%s", frameLog(f.srv))
	} else if !errors.Is(err, mq.ErrClosed) {
		t.Errorf("the refusal is %v (code %v), want ErrClosed", err, mq.CodeOf(err))
	}
}

// TestNoExtenderIsOffered is FeatureExtendLease as an interface fact.
//
// STOMP grants no lease: a delivery is this subscription's until the
// subscription goes away, so there is no deadline to extend and nothing to
// expire. The core asks for [mq.DeadlineExtender] once, so a settler that
// carried the method and refused every call would make the core's shared lease
// scheduler schedule extensions that can never succeed.
func TestNoExtenderIsOffered(t *testing.T) {
	t.Parallel()
	f, _ := settleFixture(t, settings(activemq.Artemis))
	var s any = f.src.Settler()
	if _, carries := s.(mq.DeadlineExtender); carries {
		t.Error("the settler satisfies mq.DeadlineExtender. STOMP grants no lease, so there is no deadline to " +
			"extend -- and the core's lease scheduler would schedule extensions that cannot succeed")
	}
	if _, carries := s.(mq.BatchSettler); carries {
		t.Error("the settler satisfies mq.BatchSettler. A STOMP ACK settles one message identifier, because this " +
			"driver subscribes with `client-individual`, so a batch would be a loop the core thinks is one round " +
			"trip")
	}
}

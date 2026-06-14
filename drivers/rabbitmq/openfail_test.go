package rabbitmq_test

import (
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// What happens when opening a handle FAILS.
//
// Every test in this package opens against a broker that says yes, so the
// error paths of `OpenSink` and `OpenSource` -- the declare that is refused,
// the channel that will not open -- ran nowhere. They are the paths where a
// driver leaks a channel: the open has taken one, the declare has failed, and
// the only thing that closes it is the code being tested.

// failingDeclares is a broker configuration that refuses the nth declare a
// channel makes, counting channels from one.
//
// The fault is armed in OnChannel, which is called at the instant the driver
// opens the channel and before its first frame -- the only moment at which
// "the declare fails" can be expressed, because by the time a test can see
// the channel the declare has already happened.
func failingDeclares(after int32, cause *amqp.Error) rabbitfake.Config {
	var n atomic.Int32
	return rabbitfake.Config{
		OnChannel: func(h *rabbitfake.ChannelHandle) {
			if n.Add(1) > after {
				h.FailNextDeclare(cause)
			}
		},
	}
}

// TestAPublisherWhoseDeclareIsRefusedFailsAndLeavesNoChannel.
//
// A topology conflict is the case this really happens in: somebody declared
// the exchange with different arguments, and every client that follows is
// answered PRECONDITION_FAILED.
//
// The channel count is checked but does NOT prove the driver closes what it
// took: a channel exception closes the channel at the BROKER, so the count
// is zero whether or not the driver calls Close. It guards the other
// direction -- an open that failed having taken a second channel -- and the
// mapping is what this really pins, since a 406 on open is a topology
// conflict and not the lost lease the same code means on a settlement.
func TestAPublisherWhoseDeclareIsRefusedFailsAndLeavesNoChannel(t *testing.T) {
	t.Parallel()
	cause := amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg 'x-queue-type'"}
	broker, client := openFakeWith(t, failingDeclares(0, &cause), declaring)

	_, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	if err == nil {
		t.Fatal("opening a publisher succeeded while every declare was refused")
	}
	if got := mq.CodeOf(err); got != mq.CodeConflict {
		t.Errorf("a refused declare is code %v, want CodeConflict: 406 on open is a topology conflict", got)
	}
	// Nothing is left open. A driver that returned the error and kept the
	// channel would leak one per retry, which on a process that reconnects
	// is a slow death nothing else notices.
	if n := broker.OpenChannels(); n != 0 {
		t.Errorf("%d channel(s) are open after the publisher failed to open, want none", n)
	}
}

// TestAConsumerWhoseDeclareIsRefusedFailsAndLeavesNoChannel.
func TestAConsumerWhoseDeclareIsRefusedFailsAndLeavesNoChannel(t *testing.T) {
	t.Parallel()
	cause := amqp.Error{Code: amqp.PreconditionFailed, Reason: "inequivalent arg 'x-max-length'"}
	broker, client := openFakeWith(t, failingDeclares(0, &cause), declaring)

	_, err := client.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	if err == nil {
		t.Fatal("opening a consumer succeeded while every declare was refused")
	}
	if got := mq.CodeOf(err); got != mq.CodeConflict {
		t.Errorf("a refused declare is code %v, want CodeConflict", got)
	}
	if n := broker.OpenChannels(); n != 0 {
		t.Errorf("%d channel(s) are open after the consumer failed to open, want none", n)
	}
}

// TestOpeningAgainstABrokerThatIsDownFails.
//
// The broker is held DOWN rather than failing one dial, because a single
// refused dial is not a failure at all: the driver retries, and it should --
// a broker restarting while a process starts is the ordinary case. What has
// to fail is an open against one that stays down, and it has to fail within
// the caller's deadline rather than retrying past it.
func TestOpeningAgainstABrokerThatIsDownFails(t *testing.T) {
	t.Parallel()
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	broker.SetDown(true)

	cfg := base()
	declaring(&cfg)
	ctx, cancel := contextWithTimeout(t, 2*time.Second)
	defer cancel()

	start := time.Now()
	_, err := rq.OpenWithDialer(ctx, broker.Dialer(), cfg)
	if err == nil {
		t.Fatal("opening against a broker that is down succeeded")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("the open took %s against a 2s deadline", took)
	}
}

// TestAHandleOpenedOnAClosedClientIsRefused.
//
// It is the shutdown race from the other side: Close has run and something is
// still opening. The answer has to be ErrClosed rather than a channel on a
// connection that is going away.
func TestAHandleOpenedOnAClosedClientIsRefused(t *testing.T) {
	t.Parallel()
	_, client := openFake(t, declaring)
	if err := client.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}

	if _, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"}); err == nil {
		t.Error("a publisher opened on a closed client succeeded")
	}
	if _, err := client.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: "orders", Subscription: "workers",
	}); err == nil {
		t.Error("a consumer opened on a closed client succeeded")
	}
}

// TestATopicTheNamingRefusesFailsBeforeAChannelIsTaken.
//
// The refusal is before any I/O, which is what makes a bad topic an immediate
// error naming the rule rather than a channel opened and thrown away.
func TestATopicTheNamingRefusesFailsBeforeAChannelIsTaken(t *testing.T) {
	t.Parallel()
	broker, client := openFake(t, declaring)

	before := broker.OpenChannels()
	if _, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders.*"}); err == nil {
		t.Fatal("a publisher on a wildcard topic opened")
	}
	if got := broker.OpenChannels(); got != before {
		t.Errorf("the refused open left the channel count at %d, was %d; the naming refusal has to come "+
			"before a channel is taken", got, before)
	}
}

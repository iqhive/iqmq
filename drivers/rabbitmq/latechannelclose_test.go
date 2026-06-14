package rabbitmq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestCriticalReconnectSurvivesALateChannelClose pins that a channel's close
// notification cannot take the channel that REPLACED it.
//
// A lost connection detaches every publisher twice and nothing orders the two:
// the supervision loop does it itself -- `Backend.repair` calls
// `Reconnector.Lost`, which runs `detachChildren` before the redial -- and the
// publisher's own loop does it again when it reads its channel's close
// notification. The loop's copy is the one that can arrive late, because it is
// a goroutine wake-up rather than a step of the repair, and a whole
// reconnection (dial, topology replay, re-attach) can complete before it is
// scheduled.
//
// Unguarded, that late detach sets `s.ch, s.tr = nil, nil` on the channel the
// reconnection had just installed, and the publisher is then dead FOR EVER:
// every Publish afterwards is refused with CodeUnavailable and Retryable, and
// nothing re-attaches it, because the reconnection that would have has already
// happened. A caller that retries -- which is what the specification asks of it -- retries
// against a client that will never work again.
//
// It is not a hypothetical interleaving. It made GitHub's runner red as
// `TestCriticalReconnectRestoresTheTopology`, whose own retry loop gives the
// publication a second before it gives up, and it reproduces about once in
// forty runs locally under `GOMAXPROCS=2` with coverage instrumentation.
//
// The ordering here is arranged rather than waited for: the dialer holds the
// first connection's channel-close notifications, so the reconnection is
// COMPLETE -- the restore counter has risen -- before the old notification is
// allowed to travel. A test that reproduced this by scheduling luck would pass
// on the host that has the defect.
func TestCriticalReconnectSurvivesALateChannelClose(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })

	dialer, deliverLateCloses := holdChannelCloses(broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the outage: %v", err)
	}

	b := backendOf(t, cl)
	restores, stale := rq.RestoresForTest(b), rq.StaleChannelClosesForTest(b)

	broker.Restart(&amqp.Error{
		Code: amqp.ConnectionForced, Reason: "broker restarting", Server: true,
	})
	// The whole reconnection, not the first sign of one: the counter rises
	// after the replay and after every handle has been re-attached, so the
	// publisher below is attached to the second connection.
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	if err := publishRetrying(t.Context(), pub, "reattached"); err != nil {
		t.Fatalf("publishing once the reconnection had finished: %v", err)
	}

	// Only now does the first connection's close notification reach the
	// publisher's old loop, which is the reordering the guard is for.
	deliverLateCloses()
	waitFor(t, 10*time.Second, func() bool { return rq.StaleChannelClosesForTest(b) > stale })

	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("the publisher never worked again after a close notification for its PREVIOUS channel "+
			"arrived: %v. The reconnection had already re-attached it -- the restore counter rose and a "+
			"publication succeeded -- so the stale notification detached a live channel and nothing will "+
			"ever attach another one", err)
	}
}

// publishRetrying publishes and retries the refusals the specification marks Retryable,
// which is what a caller is asked to do while a dial loop is running. It gives
// up after a second, which is far longer than a reconnection against a fake
// broker takes and far shorter than "for ever".
//
// It reports the LAST error rather than the first, because the first is
// expected: the question a caller has is whether the client ever comes back.
func publishRetrying(ctx context.Context, pub *mq.PublisherHandle, body string) error {
	var last error
	deadline := time.Now().Add(time.Second)
	for {
		last = pub.Publish(ctx, &mq.Message{Body: []byte(body)})
		if last == nil {
			return nil
		}
		var oe *mq.OpError
		if !errors.As(last, &oe) || !oe.Retryable {
			return last
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// holdChannelCloses wraps a dialer so that the close notification for every
// channel on its FIRST connection is held until the returned function is
// called. Later connections are untouched.
//
// It holds the NOTIFICATION and nothing else: the broker's own state changes
// when the connection closes, exactly as it does without this wrapper, and
// what is delayed is the message travelling to the client. That is a slow
// reader or a descheduled goroutine, which is what the fake's own
// `Broker.Restart` comment calls the thing that travels, and it is the only
// part of an outage whose timing a driver cannot control.
//
// The connection's own close notification is NOT held, because the supervision
// loop has to see it: the whole point is to let the repair finish first.
func holdChannelCloses(inner client.Dialer) (client.Dialer, func()) {
	d := &holdingDialer{inner: inner, release: make(chan struct{})}
	return d, func() { d.once.Do(func() { close(d.release) }) }
}

type holdingDialer struct {
	inner   client.Dialer
	release chan struct{}
	once    sync.Once
	dials   atomic.Int64
}

func (d *holdingDialer) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	cn, err := d.inner.Dial(ctx, urls, cfg)
	if err != nil {
		return cn, err
	}
	if d.dials.Add(1) != 1 {
		return cn, nil
	}
	return &holdingConn{Conn: cn, release: d.release}, nil
}

type holdingConn struct {
	client.Conn
	release chan struct{}
}

func (c *holdingConn) Channel() (client.Channel, error) {
	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return &holdingChannel{Channel: ch, release: c.release}, nil
}

type holdingChannel struct {
	client.Channel
	release chan struct{}
}

// NotifyClose registers a relay rather than the caller's own channel, and
// forwards the one notification the library sends once the hold is released.
//
// A nil value is forwarded for a channel the fake closed without a reason,
// which is what the library does too and what `reasonOf` turns into
// errClosedChannel.
func (ch *holdingChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	relay := make(chan *amqp.Error, 1)
	ch.Channel.NotifyClose(relay)
	go func() {
		aerr := <-relay
		<-ch.release
		select {
		case c <- aerr:
		default:
		}
	}()
	return c
}

// TestCriticalASettlementOnAClosedChannelIsLeaseLost is a
// delivery whose channel has CLOSED is refused with ErrLeaseLost and no frame,
// even before the source has noticed the close.
//
// The generation guard catches a settlement once the source has invalidated
// the channel, and the source learns of the close through a notification --
// which a caller can outrun. A settlement that did was sent to the dead
// channel, the library refused it there, and the driver reported
// SettlementUnknown: an outcome nobody knows. It IS known: the channel is
// closed, so the frame was never written and the broker has taken the
// delivery back. `TestCriticalConnectionLossResolvesWhatItWasHolding` met this
// about once in fifteen hundred runs under `-race` at GOMAXPROCS=2, on `main`
// as well as on this branch; here the close notification is held for the
// whole test, so the caller always gets there first.
func TestCriticalASettlementOnAClosedChannelIsLeaseLost(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer, deliverLateCloses := holdChannelCloses(broker.Dialer())
	t.Cleanup(deliverLateCloses)
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	held := receiveOne(t, cons)

	consumerChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.ResourceError, Reason: "the node is under pressure", Server: true,
	})
	broker.Sync()
	before := settlementCount(broker)
	if err := held.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acknowledging a delivery whose channel has closed = %v, want ErrLeaseLost: the frame cannot "+
			"have been written, and the broker has taken the delivery back", err)
	}
	if after := settlementCount(broker); after != before {
		t.Fatalf("the refused settlement sent %d frame(s)", after-before)
	}
}

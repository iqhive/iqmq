package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// These cases are [driverkit.ClientBorrowed] at the level it is decided: the
// backend's start with that ownership, over the fake broker. Under it the
// first connection is the caller's, lent to the driver, and every later one is
// a connection the driver dialled itself after the lent one died. Ownership is
// per connection, and both directions were wrong: Close left the driver's own
// connection open, and a failed open closed the caller's.

// TestABorrowedClientClosesTheConnectionItDialledItself is the first
// direction. The lent connection dies and the dial loop replaces it with one
// of the driver's own. Close used to close nothing at all under
// ClientBorrowed, so that socket, its heartbeat and reader goroutines and the
// broker's side of it stayed open after the client had gone.
func TestABorrowedClientClosesTheConnectionItDialledItself(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{
		Addrs:         []string{"localhost:5672"},
		ReconnectBase: time.Millisecond,
		ReconnectMax:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientBorrowed); err != nil {
		t.Fatalf("opening over the lent connection: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	lent := broker.LastConnection()

	restores := b.rc.Restores()
	lent.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	deadline := time.Now().Add(10 * time.Second)
	for b.rc.Restores() == restores {
		if time.Now().After(deadline) {
			t.Fatal("the client did not replace the lent connection, so this test proves nothing")
		}
		time.Sleep(time.Millisecond)
	}
	own := broker.LastConnection()
	if own.ID() == lent.ID() {
		t.Fatal("the connection in use is still the lent one, so this test proves nothing")
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !own.IsClosed() {
		t.Fatal("Close left open the connection the driver dialled itself after the lent one died: under " +
			"ClientBorrowed only the lent connection is the caller's")
	}
}

// TestABorrowedClientClosesItsReplyChannel is the direct reply-to endpoint on a
// lent connection. A client that made a request held a channel with a consumer
// on `amq.rabbitmq.reply-to`, and nothing closed it: under ClientOwned the
// connection's close took it, and under ClientBorrowed the connection stays
// open, so the channel outlived the client on the caller's connection.
func TestABorrowedClientClosesItsReplyChannel(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{Addrs: []string{"localhost:5672"}})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientBorrowed); err != nil {
		t.Fatalf("opening over the lent connection: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	lent := broker.LastConnection()
	if _, err := b.requesterFor(t.Context()); err != nil {
		t.Fatalf("starting the reply endpoint: %v", err)
	}
	if open := openChannels(lent); open != 1 {
		t.Fatalf("the lent connection has %d open channels with the reply endpoint up, want 1, so this test "+
			"proves nothing", open)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if lent.IsClosed() {
		t.Fatal("Close closed the lent connection")
	}
	if open := openChannels(lent); open != 0 {
		t.Fatalf("the lent connection still has %d open channel(s) after Close: the reply endpoint's channel "+
			"outlived the client", open)
	}
}

// TestABorrowedClientClosesItsDeclarationChannel is the channel the delay
// queues are declared on, on a lent connection: Close closes it, as it closes
// every other channel the client opened there.
func TestABorrowedClientClosesItsDeclarationChannel(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := Config{Addrs: []string{"localhost:5672"}, Declare: true, DelayKind: delay.TTLDLX, DelayQueueTTL: time.Hour}
	b, err := newBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientBorrowed); err != nil {
		t.Fatalf("opening over the lent connection: %v", err)
	}
	cl, err := clientFor(b, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	lent := broker.LastConnection()
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Publish("", "orders.workers", amqp.Publishing{Body: []byte("later")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	if n, err := cons.Receive(ctx, dst); n != 1 {
		t.Fatalf("receiving: %v", err)
	}
	if err := dst[0].Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}

	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if lent.IsClosed() {
		t.Fatal("Close closed the lent connection")
	}
	if open := openChannels(lent); open != 0 {
		t.Fatalf("the lent connection still has %d open channel(s) after Close: the channel the delay queue was "+
			"declared on outlived the client", open)
	}
}

// TestAClosedBorrowedClientLeavesNoAlarmListenerOnTheLentConnection is the
// `connection.blocked` listener on a lent connection after the client closed.
//
// The client library cannot take a listener off a connection, and it gives
// each notification to each listener in turn, waiting up to five seconds for
// one that nobody reads. A closed client's listener held one notification and
// then stalled every later one by that long, the caller's own listeners
// included, for as long as the caller kept the connection. On a lent
// connection the driver registers no listener at all, so Backend.Blocked,
// Health and the hook do not report an alarm there; the caller owns the
// connection and listens on it. A connection the driver dials itself, after
// the lent one died, is listened to as before.
func TestAClosedBorrowedClientLeavesNoAlarmListenerOnTheLentConnection(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{NotifyTimeout: time.Second})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{
		Addrs:         []string{"localhost:5672"},
		ReconnectBase: time.Millisecond,
		ReconnectMax:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientBorrowed); err != nil {
		t.Fatalf("opening over the lent connection: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	lent := broker.LastConnection()
	callers := b.conn().NotifyBlocked(make(chan amqp.Blocking, 1))
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	start := time.Now()
	for range 3 {
		lent.Block("low on disk")
		<-callers
		lent.Unblock()
		<-callers
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("six alarm notifications took %v to reach the caller's listener after a borrowed client closed: "+
			"the client's own listener, which nobody reads, stalled each one", took)
	}
}

// TestABorrowedClientHearsAlarmsOnlyOnItsOwnConnection is the other side of
// [TestAClosedBorrowedClientLeavesNoAlarmListenerOnTheLentConnection]: an alarm
// on the lent connection is the caller's to hear, and one on a connection the
// dial loop made after the lent one died is the driver's.
func TestABorrowedClientHearsAlarmsOnlyOnItsOwnConnection(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{
		Addrs:         []string{"localhost:5672"},
		ReconnectBase: time.Millisecond,
		ReconnectMax:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientBorrowed); err != nil {
		t.Fatalf("opening over the lent connection: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	lent := broker.LastConnection()
	lent.Block("low on disk")
	time.Sleep(100 * time.Millisecond)
	if b.Blocked() {
		t.Fatal("the client reports an alarm on the lent connection, so it listens on the caller's connection")
	}
	lent.Unblock()

	restores := b.rc.Restores()
	lent.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	deadline := time.Now().Add(10 * time.Second)
	for b.rc.Restores() == restores {
		if time.Now().After(deadline) {
			t.Fatal("the client did not replace the lent connection, so this test proves nothing")
		}
		time.Sleep(time.Millisecond)
	}
	own := broker.LastConnection()
	for deadline := time.Now().Add(5 * time.Second); !b.Blocked(); {
		if time.Now().After(deadline) {
			t.Fatal("the client did not hear connection.blocked on the connection it dialled itself")
		}
		own.Block("low on disk")
		time.Sleep(10 * time.Millisecond)
	}
}

// TestBeginCloseClosesOnlyABlockedConnectionOfTheDriversOwn is BeginClose on
// its own, as iqmq v1.2.0's Client.Close calls it before it closes the
// handles. It closes the connection only while the broker has blocked it,
// which is when a write may be parked in the socket; otherwise the handles
// close first, each ending its own work, as before. The connection the caller
// lent stays open whatever the broker does to it.
func TestBeginCloseClosesOnlyABlockedConnectionOfTheDriversOwn(t *testing.T) {
	for _, tc := range []struct {
		name   string
		own    driverkit.Ownership
		alarm  bool
		closes bool
	}{
		{"the driver's own connection, read", driverkit.ClientOwned, false, false},
		{"the driver's own connection, blocked", driverkit.ClientOwned, true, true},
		{"a lent connection, blocked", driverkit.ClientBorrowed, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := rabbitfake.New(rabbitfake.Config{})
			t.Cleanup(func() { _ = broker.Close() })
			b, err := newBackend(Config{Addrs: []string{"localhost:5672"}, Declare: true})
			if err != nil {
				t.Fatal(err)
			}
			b.dial = broker.Dialer()
			if err := b.start(t.Context(), tc.own); err != nil {
				t.Fatalf("opening: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			conn := broker.LastConnection()
			if tc.alarm {
				sink, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: "orders"})
				if err != nil {
					t.Fatal(err)
				}
				conn.Alarm("low on disk")
				t.Cleanup(conn.ClearAlarm)
				ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
				_ = sink.Publish(ctx, &mq.Message{Body: []byte("caught")}, nil)
				cancel()
				if tc.own.Owned() {
					deadline := time.Now().Add(5 * time.Second)
					for !b.Blocked() {
						if time.Now().After(deadline) {
							t.Fatal("the client never heard connection.blocked, so this test proves nothing")
						}
						time.Sleep(time.Millisecond)
					}
				}
				if conn.Reading() {
					t.Fatal("the broker is still reading the connection, so this test proves nothing")
				}
			}

			b.BeginClose()
			if conn.IsClosed() != tc.closes {
				t.Fatalf("after BeginClose the connection is closed: %v, want %v", conn.IsClosed(), tc.closes)
			}
			if !tc.closes {
				return
			}
			// Nothing dials again, so an operation that needs a connection
			// is refused as closed, not as a reconnection to wait for.
			time.Sleep(50 * time.Millisecond)
			if n := len(broker.Connections()); n != 1 {
				t.Fatalf("the client dialled %d connections, want 1: it reconnected after BeginClose", n)
			}
			if err := b.Ping(t.Context()); !errors.Is(err, mq.ErrClosed) || mq.IsRetryable(err) {
				t.Fatalf("Ping after BeginClose: %v, want ErrClosed", err)
			}
		})
	}
}

// TestAConnectionLostAsTheBackendClosesLeavesCloseClean is a connection lost
// after the teardown has begun, once its cancel has stopped the supervision
// loop, which uninstalls a lost connection. The dead one stayed installed, the
// teardown closed it again, and amqp091 answers that with 504, which Close
// returned; v1.1.1 had the same window. A connection that has already closed is
// not closed again.
func TestAConnectionLostAsTheBackendClosesLeavesCloseClean(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{Addrs: []string{"localhost:5672"}})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	if err := b.start(t.Context(), driverkit.ClientOwned); err != nil {
		t.Fatalf("opening: %v", err)
	}
	// The teardown's first step, and the supervision loop's return on it.
	b.cancel()
	b.wg.Wait()
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	if b.conn() == nil {
		t.Fatal("the lost connection was uninstalled, so this test proves nothing")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close after a loss that followed the teardown's start: %v, want nil", err)
	}
}

// openChannels is how many of a connection's channels are open.
func openChannels(c *rabbitfake.ConnHandle) int {
	n := 0
	for _, ch := range c.Channels() {
		if !ch.IsClosed() {
			n++
		}
	}
	return n
}

// TestAFailedOpenLeavesTheBorrowedConnectionOpen is the other direction. An
// open that fails after its first connection, here because the broker refuses
// the delayed-message probe's declaration, used to close the connection it was
// lent, which iqmq never does with something it was handed. The caller's
// connection was gone when OpenWithClient returned its error.
func TestAFailedOpenLeavesTheBorrowedConnectionOpen(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{
		DelayedMessagePlugin: true,
		// The probe's channel refuses its declaration, as RabbitMQ does for a
		// user without the configure permission on the probe's exchange.
		OnChannel: func(h *rabbitfake.ChannelHandle) {
			h.FailNextDeclare(&amqp.Error{Code: amqp.AccessRefused, Reason: "ACCESS_REFUSED - test", Server: true})
		},
	})
	t.Cleanup(func() { _ = broker.Close() })
	b, err := newBackend(Config{
		Addrs:             []string{"localhost:5672"},
		Declare:           true,
		DetectDelayPlugin: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	b.dial = broker.Dialer()
	serr := b.start(t.Context(), driverkit.ClientBorrowed)
	b.cancel()
	if serr == nil {
		t.Fatal("the open succeeded although the probe was refused, so this test proves nothing")
	}
	if lent := broker.LastConnection(); lent.IsClosed() {
		t.Fatalf("the failed open (%v) closed the connection the caller lent it under ClientBorrowed", serr)
	}
}

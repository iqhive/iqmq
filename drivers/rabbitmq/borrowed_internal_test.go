package rabbitmq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
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

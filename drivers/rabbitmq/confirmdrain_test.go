package rabbitmq_test

import (
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
)

// heldAcks is a publisher with three asynchronous publications whose acks the
// broker has sent and the library has handed to the publisher's loop, which
// has applied only the first: its callback holds the loop until release.
type heldAcks struct {
	broker  *rabbitfake.Broker
	channel *rabbitfake.ChannelHandle
	sink    mq.Sink
	async   mq.AsyncSink
	results chan heldResult
	release func()
}

// heldResult is one publication's outcome.
type heldResult struct {
	n   int
	err error
}

// holdAcks arranges [heldAcks] on a fresh client over a broker that confirms
// only when a test says so.
func holdAcks(t *testing.T) heldAcks {
	t.Helper()
	broker, cl := openFakeWith(t,
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		func(c *rq.Config) {
			c.Declare = true
			c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
			c.ConfirmKind, c.ConfirmWindow = confirm.Async, 8
		})
	sink, err := backendOf(t, cl).OpenSink(t.Context(), mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening a sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	async, ok := sink.(mq.AsyncSink)
	if !ok {
		t.Fatal("the sink does not publish asynchronously")
	}

	h := heldAcks{broker: broker, sink: sink, async: async, results: make(chan heldResult, 4)}
	entered, hold := make(chan struct{}), make(chan struct{})
	released := false
	h.release = func() {
		if !released {
			released = true
			close(hold)
		}
	}
	t.Cleanup(h.release)
	for n := 1; n <= 3; n++ {
		done := func(err error) { h.results <- heldResult{n, err} }
		if n == 1 {
			done = func(err error) {
				close(entered)
				<-hold
				h.results <- heldResult{1, err}
			}
		}
		if err := async.PublishAsync(t.Context(), &mq.Message{Body: fmt.Appendf(nil, "m%d", n)}, &mq.PublishOptions{}, done); err != nil {
			t.Fatalf("publishing m%d: %v", n, err)
		}
	}
	broker.Sync()
	h.channel = publisherChannel(t, broker)
	h.channel.AckPublication(1)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first publication's ack never reached its callback")
	}
	h.channel.AckPublication(2)
	h.channel.AckPublication(3)
	h.channel.Sync()
	return h
}

// expect reads want outcomes and fails on any that is not a success.
func (h heldAcks) expect(t *testing.T, want int, why string) {
	t.Helper()
	for range want {
		select {
		case r := <-h.results:
			if r.err != nil {
				t.Errorf("m%d was acked by the broker and reported %v (ambiguous=%v): %s",
					r.n, r.err, mq.IsAmbiguous(r.err), why)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("a publication the broker acked was never answered: %s", why)
		}
	}
}

// TestAnAckHandedOverBeforeTheConnectionWentIsNotReportedAmbiguous holds the
// supervision loop's detach to the acks the library has already delivered.
//
// A broker that restarts gracefully acks what it has stored and then closes
// the connection, and the library hands the acks to the publisher before it
// reports the close. The supervision loop detached every publisher as soon as
// it heard, and the detach resolved everything outstanding as Ambiguous,
// including publications whose acks were waiting in the publisher's own
// confirm channel. Here the publisher's loop is busy with the first ack's
// callback, so the other two acks are certain to be waiting when the detach
// runs.
func TestAnAckHandedOverBeforeTheConnectionWentIsNotReportedAmbiguous(t *testing.T) {
	h := holdAcks(t)
	h.broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	h.expect(t, 2, "the ack was in the publisher's confirm channel before the connection was lost")
	h.release()
	h.expect(t, 1, "its callback ran from the ack")
}

// TestAnAckHandedOverBeforeAChannelExceptionIsNotReportedAmbiguous is the same
// rule on the publisher's own route: a channel exception, which its loop
// answers itself.
//
// The loop waits on the acks and on the channel's close at once, and a select
// with both ready picks either. When it picked the close it detached first,
// and the acks behind it were resolved as Ambiguous. Each round has two acks
// waiting, so a loop that does not read them first fails a round three times
// in four.
func TestAnAckHandedOverBeforeAChannelExceptionIsNotReportedAmbiguous(t *testing.T) {
	for round := range 10 {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			h := holdAcks(t)
			h.channel.CloseWith(&amqp.Error{Code: amqp.PreconditionFailed, Reason: "test: channel exception", Server: true})
			h.release()
			h.expect(t, 3, "the ack was in the publisher's confirm channel before the channel closed")
		})
	}
}

// TestAnAckHandedOverBeforeAPublicationMetTheClosedChannelIsNotReportedAmbiguous
// is the same rule on the third route that drops a channel: a publication
// whose write meets a channel the broker has already closed.
//
// The publisher's loop is busy with the first ack's callback, so it cannot
// read the channel's close, and on a channel exception the supervision loop
// does not detach anything. The next publication is then the first to find
// the channel gone. It is ambiguous itself, since its frame may have reached
// the broker, but it resolved the two publications whose acks were waiting as
// ambiguous with it.
func TestAnAckHandedOverBeforeAPublicationMetTheClosedChannelIsNotReportedAmbiguous(t *testing.T) {
	h := holdAcks(t)
	h.channel.CloseWith(&amqp.Error{Code: amqp.PreconditionFailed, Reason: "test: channel exception", Server: true})
	err := h.async.PublishAsync(t.Context(), &mq.Message{Body: []byte("m4")}, &mq.PublishOptions{},
		func(err error) { h.results <- heldResult{4, err} })
	if !mq.IsAmbiguous(err) {
		t.Fatalf("a publication written to the closed channel returned %v, want it ambiguous: the frame may have reached the broker", err)
	}
	h.expect(t, 2, "the ack was in the publisher's confirm channel before a publication found the channel closed")
	h.release()
	h.expect(t, 1, "its callback ran from the ack")
}

// TestAnAckHandedOverBeforeThePublisherClosedIsNotReportedAmbiguous is the
// rule on the last route: the caller closing the publisher while acks wait in
// its confirm channel.
//
// Close resolves what the publisher holds and then waits for its loop, which
// the first ack's callback is holding here, so it runs on its own goroutine.
func TestAnAckHandedOverBeforeThePublisherClosedIsNotReportedAmbiguous(t *testing.T) {
	h := holdAcks(t)
	closed := make(chan error, 1)
	go func() { closed <- h.sink.Close() }()
	h.expect(t, 2, "the ack was in the publisher's confirm channel before it was closed")
	h.release()
	h.expect(t, 1, "its callback ran from the ack")
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("closing the publisher: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing the publisher never returned")
	}
}

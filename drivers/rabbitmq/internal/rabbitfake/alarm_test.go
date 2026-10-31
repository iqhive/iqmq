package rabbitfake_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestAnAlarmStopsTheBrokerReadingAfterAPublication holds Alarm to what
// RabbitMQ 4.3.5 does to a publishing connection under a disk alarm: the
// publication that meets it and every write after it return at once and are
// read only when the alarm clears, in order, `connection.blocked` arrives, and
// a round trip waits.
func TestAnAlarmStopsTheBrokerReadingAfterAPublication(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	conn := dial(t, b)
	blocks := conn.NotifyBlocked(make(chan amqp.Blocking, 4))
	ch := openChannel(t, conn)
	if _, err := ch.QueueDeclare("work", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4))
	h := b.LastConnection()
	h.Alarm("low on disk")
	if !h.Reading() {
		t.Fatal("the broker stopped reading before anything was published")
	}

	publish(t, ch, "", "work", false, "first")
	publish(t, ch, "", "work", false, "second")
	select {
	case blk := <-blocks:
		if !blk.Active || blk.Reason != "low on disk" {
			t.Fatalf("the notification is %+v", blk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no connection.blocked")
	}
	if h.Reading() {
		t.Fatal("the broker is still reading after the publication")
	}
	if got := len(b.Ready("work")); got != 0 {
		t.Fatalf("the queue holds %d messages while the broker is not reading", got)
	}
	if got := len(drainConfirms(confirms)); got != 0 {
		t.Fatalf("%d confirms arrived while the broker is not reading", got)
	}

	declared := make(chan error, 1)
	go func() {
		_, err := ch.QueueDeclare("later", true, false, false, false, nil)
		declared <- err
	}()
	select {
	case err := <-declared:
		t.Fatalf("a declaration was answered while the broker is not reading: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	h.ClearAlarm()
	if err := <-declared; err != nil {
		t.Fatalf("the declaration once the alarm cleared: %v", err)
	}
	queued := b.Ready("work")
	bodies := make([]string, 0, len(queued))
	for _, p := range queued {
		bodies = append(bodies, string(p.Body))
	}
	if strings.Join(bodies, ",") != "first,second" {
		t.Fatalf("the queue holds %q after the alarm, want both publications in order", bodies)
	}
	if got := len(drainConfirms(confirms)); got != 2 {
		t.Fatalf("%d confirms arrived after the alarm, want 2", got)
	}
	select {
	case blk := <-blocks:
		if blk.Active {
			t.Fatalf("the second notification is %+v", blk)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no connection.unblocked")
	}
}

// TestCloseDeadlineEndsAHandshakeTheBrokerDoesNotRead is the bounded close
// under an alarm: it reports the socket deadline, the connection is closed,
// and what was written before it reaches the queue.
func TestCloseDeadlineEndsAHandshakeTheBrokerDoesNotRead(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	conn := dial(t, b)
	ch := openChannel(t, conn)
	if _, err := ch.QueueDeclare("work", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	b.LastConnection().Alarm("low on memory")
	publish(t, ch, "", "work", false, "written")
	err := conn.CloseDeadline(time.Now().Add(50 * time.Millisecond))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("CloseDeadline under an alarm reported %v, want the socket deadline", err)
	}
	if !conn.IsClosed() {
		t.Fatal("the connection is open after CloseDeadline")
	}
	if got := b.Ready("work"); len(got) != 1 || string(got[0].Body) != "written" {
		t.Fatalf("the queue holds %v, want the publication written before the close", got)
	}
}

// TestADirectReplyToRequestCarriesTheAddressTheBrokerMints holds the fake to
// the real broker's direct reply-to: the request's `reply_to` names the
// pseudo-queue, the broker puts an address of its own in its place, with '+',
// '/' and the "==" of standard base64 in it, and a reply published to that
// address on the default exchange reaches the requester's consumer.
func TestADirectReplyToRequestCarriesTheAddressTheBrokerMints(t *testing.T) {
	b := newBroker(t, rabbitfake.Config{})
	conn := dial(t, b)
	requester := openChannel(t, conn)
	replies, err := requester.ConsumeWithContext(context.Background(), rabbitfake.ReplyPseudoQueue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consuming the reply pseudo-queue: %v", err)
	}
	responder := openChannel(t, conn)
	if _, err := responder.QueueDeclare("rpc", true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	requests := consume(t, responder, "rpc", "responder", true)
	req := pub("ping")
	req.ReplyTo = rabbitfake.ReplyPseudoQueue
	if err := requester.PublishWithContext(context.Background(), "", "rpc", false, false, req); err != nil {
		t.Fatal(err)
	}
	got := recv(t, requests)
	addr := got.ReplyTo
	if !strings.HasPrefix(addr, rabbitfake.ReplyPseudoQueue+".") || !strings.ContainsAny(addr, "+/") ||
		!strings.HasSuffix(addr, "==") {
		t.Fatalf("the request's reply_to is %q, want an address with the broker's shape", addr)
	}
	if err := responder.PublishWithContext(context.Background(), "", addr, false, false, pub("pong")); err != nil {
		t.Fatal(err)
	}
	if reply := recv(t, replies); string(reply.Body) != "pong" {
		t.Fatalf("the reply is %q", reply.Body)
	}

	// A request from a channel with no reply consumer is refused, as the
	// broker refuses it.
	other := openChannel(t, conn)
	lost := other.NotifyClose(make(chan *amqp.Error, 1))
	if err := other.PublishWithContext(context.Background(), "", "rpc", false, false, req); err != nil {
		t.Fatal(err)
	}
	if aerr := <-lost; aerr == nil || aerr.Code != amqp.PreconditionFailed {
		t.Fatalf("a request with no reply consumer closed its channel with %v, want 406", aerr)
	}
}

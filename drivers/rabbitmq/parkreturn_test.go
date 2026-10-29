package rabbitmq_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// acks is how many deliveries the broker has recorded an acknowledgement for.
func acks(b *rabbitfake.Broker) int {
	n := 0
	settlements := b.Settlements()
	for i := range settlements {
		if settlements[i].Kind == rabbitfake.SettleAck {
			n++
		}
	}
	return n
}

// operatorDeletesQueue deletes a queue over a connection of its own, as an
// operator would.
func operatorDeletesQueue(t *testing.T, b *rabbitfake.Broker, name string) {
	t.Helper()
	conn, err := b.Dialer().Dial(t.Context(), []string{"amqp://fake/"}, client.DialConfig{})
	if err != nil {
		t.Fatalf("dialling as the operator: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("opening the operator's channel: %v", err)
	}
	if _, err := ch.QueueDelete(name, false, false, false); err != nil {
		t.Fatalf("deleting %q: %v", name, err)
	}
}

// TestADelayedNackWhoseParkIsReturnedDoesNotAckTheOriginal holds the
// `mandatory` flag on a park to its purpose. A permanent delay queue the client
// declared once can be gone the next time, because an operator deleted it, and
// the ledger still says it is there, so the park is published into nothing.
// The broker answers with a `basic.return` and then a `basic.ack`, and a
// consume channel that never registered for returns saw only the ack: the
// original was acknowledged and the message was gone.
//
// A returned park must be a refused one, and the next park must declare the
// queue again rather than trust the ledger.
//
// The queue used to go by `x-expires` here. A queue with `x-expires` is now
// declared again before every park, so only an operator, or an expiry between
// that declaration and the park, can leave a park nowhere to go
// (TestADelayQueueInUseIsNotDeletedWithItsParkedCopies).
func TestADelayedNackWhoseParkIsReturnedDoesNotAckTheOriginal(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		c.DelayKind = delay.TTLDLX
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	const bucket = "orders.workers.delay.3200"

	// A first delayed nack declares the bucket and records it in the ledger.
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	first := receiveOne(t, cons)
	if err := first.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("the first delayed nack: %v", err)
	}
	broker.Advance(4 * time.Second)
	back := receiveOne(t, cons)
	if err := back.Ack(); err != nil {
		t.Fatal(err)
	}
	operatorDeletesQueue(t, broker, bucket)
	if _, ok := broker.Queue(bucket); ok {
		t.Fatalf("the delay queue %q is still there", bucket)
	}

	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("retry-me")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	before := acks(broker)
	if err := d.Nack(mq.NackDelay(3 * time.Second)); err == nil {
		t.Fatal("a delayed nack whose park the broker returned reported success")
	}
	broker.Sync()
	if got := acks(broker); got != before {
		t.Fatalf("the original was acknowledged although its parked copy was returned unroutable: %d acks, want %d",
			got, before)
	}

	// The original is still the caller's, and the next park declares the
	// bucket again and lands.
	if err := d.Nack(); err != nil {
		t.Fatalf("the delivery was not settleable after the returned park: %v", err)
	}
	again := receiveOne(t, cons)
	if string(again.Body) != "retry-me" {
		t.Fatalf("the redelivered body is %q", again.Body)
	}
	if err := again.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("the delayed nack after a returned park: %v", err)
	}
	broker.Sync()
	if got := broker.Ready(bucket); len(got) != 1 {
		t.Fatalf("the redeclared delay queue holds %d messages, want 1", len(got))
	}
}

// TestAParkWithoutAConfirmHandsTheOriginalBack is a delayed nack whose parked
// copy is not confirmed in time, on a channel that stays up.
//
// Nobody knows whether the copy was stored, so the original must not be
// acknowledged. But the channel is healthy and nothing will close it, so
// leaving the original unacknowledged held its prefetch slot and its token slot
// for as long as the channel lived: with a prefetch of one the consumer
// received nothing again, while the error said the broker would redeliver it.
// The driver hands the original back with a requeueing nack instead, which is
// what makes the error true: the message is redelivered now, and the parked
// copy delivers it again after the delay.
func TestAParkWithoutAConfirmHandsTheOriginalBack(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		declaring(c)
		c.PublishTimeout = 200 * time.Millisecond
	})
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 1 })
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	ch := consumerChannel(t, broker)
	ch.SetConfirmScript(rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual})

	err := d.Nack(mq.NackDelay(3 * time.Second))
	if code := mq.CodeOf(err); code != mq.CodeSettlementUnknown {
		t.Fatalf("a delayed nack whose park was not confirmed answered %v (code %v), want %v", err, code, mq.CodeSettlementUnknown)
	}
	broker.Sync()
	if acks(broker) != 0 {
		t.Fatal("the original was acknowledged although nobody knows whether its parked copy was stored")
	}

	// With a prefetch of one, a delivery arrives only if the broker has its
	// slot back AND the driver has a token slot free for it.
	ctx, cancel := contextWithTimeout(t, 2*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, rerr := cons.Receive(ctx, dst)
	if n != 1 {
		t.Fatalf("the consumer received nothing after the unconfirmed park (%v), with %d delivery(ies) unacknowledged on its live channel: the original or its slot was never handed back",
			rerr, ch.Unacked())
	}
	if got := string(dst[0].Body); got != "held" {
		t.Fatalf("the redelivery carries %q, want %q", got, "held")
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatalf("acknowledging the redelivery: %v", err)
	}

	// The confirm arriving late changes nothing, and the parked copy delivers
	// the message once more after the delay, which is the duplicate the error
	// warns of.
	for _, tag := range ch.PendingConfirms() {
		ch.AckPublication(tag)
	}
	broker.Sync()
	broker.Advance(4 * time.Second)
	broker.Sync()
	again := receiveOne(t, cons)
	if got := string(again.Body); got != "held" {
		t.Fatalf("the parked copy carries %q, want %q", got, "held")
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("acknowledging the parked copy: %v", err)
	}
}

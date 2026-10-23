package rabbitmq_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
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

// TestADelayedNackWhoseParkIsReturnedDoesNotAckTheOriginal holds the
// `mandatory` flag on a park to its purpose. A delay queue the client declared
// once can be gone the next time -- `x-expires` removes an unused one, and an
// operator can delete it -- and the ledger still says it is there, so the park
// is published into nothing. The broker answers with a `basic.return` and then
// a `basic.ack`, and a consume channel that never registered for returns saw
// only the ack: the original was acknowledged and the message was gone.
//
// A returned park must be a refused one, and the next park must declare the
// queue again rather than trust the ledger.
func TestADelayedNackWhoseParkIsReturnedDoesNotAckTheOriginal(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		c.DelayKind = delay.TTLDLX
		c.DelayQueueTTL = 30 * time.Second
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
	// The bucket is unused for longer than its `x-expires`, so the broker
	// removes it.
	broker.Advance(time.Minute)
	if _, ok := broker.Queue(bucket); ok {
		t.Fatalf("the delay queue %q did not expire", bucket)
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

package rabbitmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// expiring is the configuration of a client whose delay queues carry
// `x-expires`.
func expiring(queueTTL time.Duration) func(*rq.Config) {
	return func(c *rq.Config) {
		c.Declare = true
		c.DelayKind = delay.TTLDLX
		c.DelayQueueTTL = queueTTL
	}
}

// lostWithAQueue is every message the broker deleted together with an expired
// queue, for a failure message.
func lostWithAQueue(b *rabbitfake.Broker) []rabbitfake.Drop {
	var out []rabbitfake.Drop
	dropped := b.Dropped()
	for i := range dropped {
		if dropped[i].Reason == rabbitfake.DropQueueExpired {
			out = append(out, dropped[i])
		}
	}
	return out
}

// TestADelayQueueInUseIsNotDeletedWithItsParkedCopies holds Config.DelayQueueTTL
// to RabbitMQ's `x-expires` rule: a consumer, a declaration and a `basic.get`
// renew the lease, and a publication does not. Nor do the messages a queue
// holds keep it.
//
// The driver declared each delay queue once per connection and then only
// published to it, so the queue was deleted DelayQueueTTL after that one
// declaration however busy it was, with the parked copies in it. Their
// originals had already been acknowledged, so the messages were gone. A park
// now declares its queue again first, which renews the lease.
func TestADelayQueueInUseIsNotDeletedWithItsParkedCopies(t *testing.T) {
	broker, client := openFake(t, expiring(10*time.Second))
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	// The first delayed nack declares the 3.2-second bucket.
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

	// 8s after the declaration the bucket is used again, and its copy is due
	// back after the first declaration's 10s are up.
	broker.Advance(4 * time.Second)
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	second := receiveOne(t, cons)
	if err := second.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("the second delayed nack: %v", err)
	}
	broker.Sync()
	// In small steps, so that the queue's expiry and the copy's TTL are swept
	// in the order they fall due.
	for range 10 {
		broker.Advance(500 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	if n, err := cons.Receive(ctx, dst); n != 1 {
		t.Fatalf("the second delayed nack never came back (%v): the delay queue was deleted with its parked copy, "+
			"lost %+v", err, lostWithAQueue(broker))
	}
	if got := string(dst[0].Body); got != "second" {
		t.Fatalf("the redelivery carries %q, want %q", got, "second")
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatal(err)
	}
	if lost := lostWithAQueue(broker); len(lost) != 0 {
		t.Fatalf("a delay queue expired with %d message(s) in it: %+v", len(lost), lost)
	}
}

// TestADelayItsDelayQueueCouldNotOutliveIsRefused is the other half of the
// lease. However often a park renews it, a queue whose `x-expires` is not well
// past its bucket can be deleted before the copy parked in it leaves, so such a
// delay is refused with ErrUnsupported and nothing is settled, as a delay
// past the ladder is. The queue has to keep a minute of its lease, or half of
// it when DelayQueueTTL is under 2 minutes.
func TestADelayItsDelayQueueCouldNotOutliveIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name     string
		queueTTL time.Duration
		delay    time.Duration
		bucket   string
		parked   bool
	}{
		{"a bucket longer than the queue's x-expires", time.Second, 3 * time.Second, "orders.workers.delay.3200", false},
		{"a bucket longer than half of a short x-expires", 10 * time.Second, 4 * time.Second, "orders.workers.delay.6400", false},
		{"a bucket within half of a short x-expires", 10 * time.Second, 3 * time.Second, "orders.workers.delay.3200", true},
		{"a bucket within a minute of a long x-expires", 3276800*time.Millisecond + 30*time.Second, 50 * time.Minute, "orders.workers.delay.3276800", false},
		{"a bucket a minute clear of a long x-expires", time.Hour, 50 * time.Minute, "orders.workers.delay.3276800", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, client := openFake(t, expiring(tc.queueTTL))
			cons := consumeOn(t, client, "orders", "workers")
			pub := publishOn(t, client, "orders")
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("later")}); err != nil {
				t.Fatal(err)
			}
			d := receiveOne(t, cons)
			broker.Sync()
			before := settlementCount(broker)

			err := d.Nack(mq.NackDelay(tc.delay))
			broker.Sync()
			if tc.parked {
				if err != nil {
					t.Fatalf("NackDelay(%v) under DelayQueueTTL=%v: %v", tc.delay, tc.queueTTL, err)
				}
				if got := broker.Ready(tc.bucket); len(got) != 1 {
					t.Fatalf("the delay queue %q holds %d message(s), want 1", tc.bucket, len(got))
				}
				return
			}
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("NackDelay(%v) under DelayQueueTTL=%v answered %v, want ErrUnsupported: its queue could "+
					"expire with the copy in it", tc.delay, tc.queueTTL, err)
			}
			if got := settlementCount(broker); got != before {
				t.Fatalf("the refused delayed nack settled something: %d settlements, want %d", got, before)
			}
			if _, ok := broker.Queue(tc.bucket); ok {
				t.Fatalf("the refused delayed nack declared %q", tc.bucket)
			}
			// The delivery is still the caller's.
			if err := d.Nack(); err != nil {
				t.Fatalf("the delivery was not settleable after the refusal: %v", err)
			}
			again := receiveOne(t, cons)
			if got := string(again.Body); got != "later" {
				t.Fatalf("the redelivery carries %q, want %q", got, "later")
			}
			if err := again.Ack(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// brokerClock is the fake broker's clock as the driver's, so that the moment a
// parked copy is due back and the moment the broker has moved to are read off
// one clock. Its timers are the wall clock's: only the dial path asks for one.
type brokerClock struct{ b *rabbitfake.Broker }

func (c brokerClock) Now() time.Time { return c.b.Now() }

func (brokerClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{t: time.NewTimer(d)} }

// wallTimer is a [time.Timer] as an [mq.Timer].
type wallTimer struct{ t *time.Timer }

func (w wallTimer) C() <-chan time.Time        { return w.t.C }
func (w wallTimer) Stop() bool                 { return w.t.Stop() }
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// openOnTheBrokersClock is [openFake] with the driver on the broker's clock, for
// the tests that compare the moment a parked copy is due back with the moment
// it is received.
func openOnTheBrokersClock(t *testing.T, shape func(*rq.Config)) (*rabbitfake.Broker, *mq.Client) {
	t.Helper()
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	shape(&cfg)
	cfg.Clock = brokerClock{b: broker}
	client, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return broker, client
}

// nothingWithin reports a delivery that arrived within d, or nil.
func nothingWithin(t *testing.T, cons *mq.ConsumerHandle, d time.Duration) *mq.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	if n, _ := cons.Receive(ctx, dst); n == 1 {
		return &dst[0]
	}
	return nil
}

// TestADelayedNackOfAMessageWithATTLWaitsOutTheWholeDelay is a delayed nack of
// a message that has a TTL of its own, on the TTL-and-dead-letter arm.
//
// The parked copy carried the message's `expiration`, and a queue entry dies
// at the shorter of the queue's `x-message-ttl` and its own, so a message with
// a 1s TTL left the 3.2s delay queue after 1s and was
// dead-lettered straight back: NackDelay(3s) was redelivered after 1.5s. A
// delay is a minimum, so the copy now goes without its `expiration`.
func TestADelayedNackOfAMessageWithATTLWaitsOutTheWholeDelay(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Second} {
		t.Run(ttl.String(), func(t *testing.T) {
			broker, client := openFake(t, func(c *rq.Config) {
				c.Declare = true
				c.DelayKind = delay.TTLDLX
			})
			cons := consumeOn(t, client, "orders", "workers")
			pub := publishOn(t, client, "orders")
			var opts []mq.PublishOption
			if ttl > 0 {
				opts = append(opts, mq.TTL(ttl))
			}
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("later")}, opts...); err != nil {
				t.Fatalf("publishing: %v", err)
			}
			d := receiveOne(t, cons)
			if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
				t.Fatalf("a delayed nack: %v", err)
			}
			broker.Sync()
			const bucket = "orders.workers.delay.3200"
			parked := broker.Ready(bucket)
			if len(parked) != 1 {
				t.Fatalf("the delay queue holds %d message(s), want 1", len(parked))
			}
			if got := parked[0].Expiration; got != "" {
				t.Errorf("the parked copy has expiration %q: it would leave the delay queue when its own TTL ran out", got)
			}

			broker.Advance(1500 * time.Millisecond)
			if early := nothingWithin(t, cons, 300*time.Millisecond); early != nil {
				t.Fatalf("ttl=%v: NackDelay(3s) was redelivered after 1.5s (attempt %d)", ttl, early.Info().Attempt)
			}
			broker.Advance(2 * time.Second)
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

// TestTheDelayedExchangeHoldsAMessagesTTLUntilTheDelayIsOver is the plugin
// arm of the same delay. The delayed-message exchange holds the message and
// routes it with its properties once the delay is over, so the parked copy
// keeps its `expiration` and the queue starts the TTL when the copy arrives.
// It carries none of the headers the delay queue arm needs.
func TestTheDelayedExchangeHoldsAMessagesTTLUntilTheDelayIsOver(t *testing.T) {
	broker, client := openFakeWith(t,
		rabbitfake.Config{DelayedMessagePlugin: true},
		func(c *rq.Config) { c.Declare, c.DetectDelayPlugin = true, true })
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("later")}, mq.TTL(time.Second)); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	d := receiveOne(t, cons)
	if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}
	broker.Sync()
	delayed := broker.Delayed()
	if len(delayed) != 1 {
		t.Fatalf("the broker is holding %d delayed messages, want 1", len(delayed))
	}
	if got := delayed[0].Expiration; got != "1000" {
		t.Errorf("the delayed copy has expiration %q, want %q", got, "1000")
	}
	for _, h := range []string{"x-iqmq-ttl", "x-iqmq-due"} {
		if v, ok := delayed[0].Headers[h]; ok {
			t.Errorf("the delayed copy carries %s=%v, which only a delay queue's copy needs", h, v)
		}
	}
	broker.Advance(3 * time.Second)
	again := receiveOne(t, cons)
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestAMessagesOwnTTLRunsAgainOnceItsDelayedRedeliveryIsBack is the rest of
// the TTL's journey through a delay queue.
//
// RabbitMQ removes `expiration` from a message it dead-letters, so the copy
// that comes back from the delay queue would never expire. The copy carries
// the TTL instead, and the driver applies it from the moment the copy is due
// back: a copy that waited in its queue for longer than its TTL is expired as
// the broker would expire it, onto the queue's dead-letter route, and never
// handed to the consumer. One taken within its TTL is delivered.
//
// The consumer holds a prefetch of one with another delivery, so the copy
// waits in the queue rather than in the driver's buffer.
func TestAMessagesOwnTTLRunsAgainOnceItsDelayedRedeliveryIsBack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		waited  time.Duration
		expired bool
	}{
		{"taken within its TTL", 500 * time.Millisecond, false},
		{"left in the queue for longer than its TTL", 2 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, client := openOnTheBrokersClock(t, func(c *rq.Config) {
				c.Declare, c.DeadLetter = true, true
				c.DelayKind = delay.TTLDLX
			})
			cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 1 })
			pub := publishOn(t, client, "orders")

			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("short-lived")}, mq.TTL(time.Second)); err != nil {
				t.Fatalf("publishing: %v", err)
			}
			first := receiveOne(t, cons)
			if err := first.Nack(mq.NackDelay(3 * time.Second)); err != nil {
				t.Fatalf("a delayed nack: %v", err)
			}
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
				t.Fatalf("publishing: %v", err)
			}
			held := receiveOne(t, cons)

			broker.Advance(3200 * time.Millisecond)
			if got := broker.Ready("orders.workers"); len(got) != 1 {
				t.Fatalf("the subscription's queue holds %d message(s) once the delay is over, want the copy", len(got))
			}
			broker.Advance(tc.waited)
			if err := held.Ack(); err != nil {
				t.Fatal(err)
			}

			got := nothingWithin(t, cons, 500*time.Millisecond)
			broker.Sync()
			dlq := broker.Ready("orders.workers.dlq")
			if !tc.expired {
				if got == nil {
					t.Fatalf("the copy taken %v after it was due back, within its 1s TTL, was not delivered "+
						"(%d in the dead-letter queue)", tc.waited, len(dlq))
				}
				if body := string(got.Body); body != "short-lived" {
					t.Fatalf("the redelivery carries %q, want %q", body, "short-lived")
				}
				if err := got.Ack(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if got != nil {
				t.Fatalf("the copy was delivered %v after it was due back, past its 1s TTL", tc.waited)
			}
			if len(dlq) != 1 || string(dlq[0].Body) != "short-lived" {
				t.Fatalf("the dead-letter queue holds %d message(s), want the expired copy", len(dlq))
			}
		})
	}
}

// backFromADelay publishes a message with a 1s TTL, nacks it with a 3s delay
// and returns the copy that comes back, received the moment it is due and so
// within its TTL.
func backFromADelay(t *testing.T, broker *rabbitfake.Broker, pub *mq.PublisherHandle, cons *mq.ConsumerHandle) mq.Delivery {
	t.Helper()
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("short-lived")}, mq.TTL(time.Second)); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	first := receiveOne(t, cons)
	if err := first.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}
	broker.Advance(3200 * time.Millisecond)
	return receiveOne(t, cons)
}

// parksWithoutATTL nacks d with a 3s delay and fails the test if the copy in
// bucket carries a TTL of any kind.
func parksWithoutATTL(t *testing.T, broker *rabbitfake.Broker, d mq.Delivery, bucket string) {
	t.Helper()
	if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
		t.Fatalf("a delayed nack of the dead letter: %v", err)
	}
	broker.Sync()
	parked := broker.Ready(bucket)
	if len(parked) != 1 {
		t.Fatalf("%s holds %d message(s), want the dead letter's copy", bucket, len(parked))
	}
	if got := parked[0].Expiration; got != "" {
		t.Errorf("the dead letter's parked copy has expiration %q", got)
	}
	for _, h := range []string{"x-iqmq-ttl", "x-iqmq-due"} {
		if v, ok := parked[0].Headers[h]; ok {
			t.Errorf("the dead letter's parked copy carries %s=%v, a TTL the dead letter no longer has", h, v)
		}
	}
}

// TestADeadLetterMovedBackToItsQueueIsDelivered is a dead letter moved back
// to the queue it came from, which is what a shovel or the management UI's
// "Move messages" does: a publication through the default exchange with the
// queue's name, the headers kept.
//
// RabbitMQ removes `expiration` on every dead-letter hop, so a dead letter has
// no TTL. A copy back from a delay queue keeps `x-iqmq-ttl` and `x-iqmq-due`
// when it is dead-lettered, though, and the driver read them as a TTL that had
// run out long ago: the moved message went straight back to the dead-letter
// queue on every attempt, or was dropped where there was no route. The carried
// TTL applies only while the latest dead-lettering is the delay queue's
// expiry, so the moved message is delivered, and its next delay carries no
// TTL either.
func TestADeadLetterMovedBackToItsQueueIsDelivered(t *testing.T) {
	broker, client := openOnTheBrokersClock(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		c.DelayKind = delay.TTLDLX
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	back := backFromADelay(t, broker, pub, cons)
	if err := back.Nack(mq.NackDeadLetter()); err != nil {
		t.Fatalf("dead-lettering the copy: %v", err)
	}
	broker.Sync()
	dlq := broker.Ready("orders.workers.dlq")
	if len(dlq) != 1 {
		t.Fatalf("the dead-letter queue holds %d message(s), want the copy", len(dlq))
	}

	broker.Advance(time.Hour)
	moved := amqp.Publishing{Headers: dlq[0].Headers, Body: dlq[0].Body}
	if _, err := broker.Publish("", "orders.workers", moved); err != nil {
		t.Fatalf("moving the dead letter back: %v", err)
	}
	got := nothingWithin(t, cons, time.Second)
	if got == nil {
		broker.Sync()
		t.Fatalf("the dead letter moved back to its queue was not delivered (%d in the dead-letter queue)",
			len(broker.Ready("orders.workers.dlq")))
	}
	if body := string(got.Body); body != "short-lived" {
		t.Fatalf("the moved message carries %q, want %q", body, "short-lived")
	}

	parksWithoutATTL(t, broker, *got, "orders.workers.delay.3200")
	broker.Advance(5 * time.Second)
	again := receiveOne(t, cons)
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestADelayedNackOfADeadLetterTopicsDeliveryCarriesNoTTL is the same dead
// letter read from a [rq.Config.DeadLetterTopic] subscription, whose handler
// asks for a delayed redelivery.
//
// The park read the TTL a dead letter still carries in `x-iqmq-ttl` and parked
// the copy with it, so a copy that waited longer than that in the dead-letter
// topic's queue after its delay was expired by the driver and never delivered.
func TestADelayedNackOfADeadLetterTopicsDeliveryCarriesNoTTL(t *testing.T) {
	broker, client := openOnTheBrokersClock(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter, c.DeadLetterTopic = true, true, "parked"
		c.DelayKind = delay.TTLDLX
	})
	audit := consumeOn(t, client, "parked", "audit")
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	back := backFromADelay(t, broker, pub, cons)
	if err := back.Reject(); err != nil {
		t.Fatalf("rejecting the copy: %v", err)
	}
	dead := receiveOne(t, audit)
	parksWithoutATTL(t, broker, dead, "parked.audit.delay.3200")

	// Due back after 3.2s and left in the queue for 2s more, twice the TTL
	// the message had before it was dead-lettered.
	broker.Advance(5200 * time.Millisecond)
	got := receiveOne(t, audit)
	if err := got.Ack(); err != nil {
		t.Fatal(err)
	}
	for _, dl := range broker.DeadLettered() {
		if dl.From == "parked.audit" {
			t.Fatalf("the dead letter's delayed redelivery was dead-lettered again (%s), by the TTL it had before it was first dead-lettered", dl.Reason)
		}
	}
}

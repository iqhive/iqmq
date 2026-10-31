package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestAPluginDelayedRedeliveryReachesOnlyTheSubscriptionThatNackedIt holds the
// plugin arm to the rule the TTL arm already keeps: a delayed redelivery
// belongs to the one subscription that nacked the message.
//
// `<exchange>.delayed` is bound to each subscription's queue by the queue's
// name, and a park is routed by that name. The exchange took the topic's own
// type as its `x-delayed-type`, and a fanout or headers exchange ignores the
// routing key. So under a fanout topic one subscription's delayed nack put a
// copy in every subscription's queue once the delay was over, and under a
// headers topic the queue's name played no part in where the copy went (the
// fake routes it nowhere). The delayed exchange now routes by the key whatever
// type the topic's exchange is.
func TestAPluginDelayedRedeliveryReachesOnlyTheSubscriptionThatNackedIt(t *testing.T) {
	for _, kind := range []string{amqp.ExchangeFanout, amqp.ExchangeHeaders, amqp.ExchangeTopic, amqp.ExchangeDirect} {
		t.Run(kind, func(t *testing.T) {
			broker, client := openFakeWith(t,
				rabbitfake.Config{DelayedMessagePlugin: true},
				func(c *rq.Config) {
					c.Declare, c.DetectDelayPlugin = true, true
					c.ExchangeType = kind
				})
			if facts := rq.FactsForTest(backendOf(t, client)); !contains(facts, "delaykind=plugin") {
				t.Fatalf("the client chose %s, want the plugin arm, or this test proves nothing", facts)
			}
			nacking := consumeOn(t, client, "orders", "a")
			_ = consumeOn(t, client, "orders", "b")
			if _, ok := broker.Queue("orders.b"); !ok {
				t.Fatalf("the second subscription's queue was not declared (queues: %v), so this test proves nothing",
					queueNames(broker))
			}
			// The message is put straight into the one queue, because a
			// headers exchange bound with no arguments routes nothing to it.
			if n, err := broker.Publish("", "orders.a", amqp.Publishing{Body: []byte("later")}); err != nil || n != 1 {
				t.Fatalf("seeding orders.a reached %d queues: %v", n, err)
			}
			d := receiveOne(t, nacking)
			if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
				t.Fatalf("a delayed nack: %v", err)
			}
			broker.Sync()
			if ex, ok := broker.Exchange("orders.delayed"); !ok || ex.DelayedType != routedByKey(kind) {
				t.Errorf("orders.delayed has x-delayed-type %q, want %q: the park is routed by the queue's name",
					ex.DelayedType, routedByKey(kind))
			}

			broker.Advance(3 * time.Second)
			broker.Sync()
			a, _ := broker.Queue("orders.a")
			b, _ := broker.Queue("orders.b")
			if a.Depth+a.Unacked != 1 || b.Depth+b.Unacked != 0 {
				t.Fatalf("after the delay orders.a holds %d copies and orders.b holds %d, want 1 and 0: "+
					"a delayed redelivery belongs to the subscription that nacked it",
					a.Depth+a.Unacked, b.Depth+b.Unacked)
			}
			again := receiveOne(t, nacking)
			if err := again.Ack(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestADeletedDelayedExchangeIsDeclaredAgainAfterItsFirstRefusal is the plugin
// arm's `<exchange>.delayed` deleted by an operator while the ledger still
// vouches for it, and with it its binding to the subscription's queue.
//
// A park then publishes to an exchange that is not there, on the consume
// channel, and the broker answers with a 404 that closes it: every delivery
// the consumer holds is taken back. That first refusal is the part waiting on
// the N22 decision. What is fixed is every refusal after it: the TTL arm
// forgets a delay queue that has gone, and nothing forgot the exchange, nor
// did the channel's repair declare anything, so EVERY later delayed nack met
// the same 404 and took the consumer's deliveries again, until the next
// reconnection. The 404 now drops the exchange and its binding from the
// ledger, and the repair declares both again; the parked copy of the nack
// after it has to come back.
func TestADeletedDelayedExchangeIsDeclaredAgainAfterItsFirstRefusal(t *testing.T) {
	broker, cl := openFakeWith(t, rabbitfake.Config{DelayedMessagePlugin: true}, func(c *rq.Config) {
		c.Declare, c.DetectDelayPlugin = true, true
	})
	cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 8 })
	pub := publishOn(t, cl, "orders")
	raw, err := backendOf(t, cl).Conn().Channel()
	if err != nil {
		t.Fatalf("opening a raw channel: %v", err)
	}
	if err := raw.ExchangeDelete("orders.delayed", false, false); err != nil {
		t.Fatalf("deleting the delayed exchange: %v", err)
	}

	for round := 1; round <= 2; round++ {
		for _, body := range []string{"X", "Y"} {
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		dst := make([]mq.Delivery, 8)
		var held []mq.Delivery
		for len(held) < 2 {
			n, err := cons.Receive(ctx, dst)
			if err != nil {
				cancel()
				t.Fatalf("round %d: receiving: %v (held %d)", round, err, len(held))
			}
			held = append(held, dst[:n]...)
		}
		cancel()
		var x, y mq.Delivery
		for _, d := range held {
			switch string(d.Body) {
			case "X":
				x = d
			case "Y":
				y = d
			}
		}
		nerr := x.Nack(mq.NackDelay(time.Second))
		aerr := y.Ack()
		t.Logf("round %d: X's delayed nack: %v; Y's ack: %v", round, nerr, aerr)
		if round == 1 {
			// The broker took back what the consumer held: drain the
			// redeliveries so the next round starts clean.
			dctx, dcancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			for {
				n, err := cons.Receive(dctx, dst)
				if err != nil {
					break
				}
				for i := range n {
					_ = dst[i].Ack()
				}
			}
			dcancel()
			continue
		}
		if nerr != nil || aerr != nil {
			t.Fatalf("round 2: X's delayed nack reported %v and Y's ack %v: the first refused park did not "+
				"get `orders.delayed` declared again, so every delayed nack meets the same 404 on the consume "+
				"channel until the next reconnection", nerr, aerr)
		}
		// The nack acknowledged X's original, so the parked copy has to come
		// back: an exchange declared again without its binding to the
		// subscription's queue took the copy and routed it nowhere.
		broker.Sync()
		broker.Advance(2 * time.Second)
		broker.Sync()
		ctx, cancel = context.WithTimeout(t.Context(), 3*time.Second)
		n, err := cons.Receive(ctx, dst)
		cancel()
		if err != nil || n == 0 || string(dst[0].Body) != "X" {
			drops := broker.Dropped()
			dropped := make([]string, 0, len(drops))
			for _, d := range drops {
				dropped = append(dropped, string(d.Message.Body)+":"+string(d.Reason))
			}
			t.Fatalf("X's delayed nack acknowledged its original, and its parked copy never came back (%v); "+
				"the broker dropped %v: `orders.delayed` was declared again without its binding", err, dropped)
		}
		_ = dst[0].Ack()
	}
}

// routedByKey is the `x-delayed-type` the driver declares `<exchange>.delayed`
// with for a topic exchange of the given type.
func routedByKey(kind string) string {
	switch kind {
	case amqp.ExchangeFanout, amqp.ExchangeHeaders:
		return amqp.ExchangeDirect
	default:
		return kind
	}
}

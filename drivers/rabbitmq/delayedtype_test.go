package rabbitmq_test

import (
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

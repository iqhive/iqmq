package rabbitmq_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// TestAPluginDelayedRedeliveryReachesOnlyTheSubscriptionThatNackedIt holds the
// plugin arm to the rule the TTL arm already keeps: a delayed redelivery
// belongs to the one subscription that nacked the message.
//
// `<exchange>.delayed.direct` is bound to each subscription's queue by the queue's
// name, and a park is routed by that name. The exchange took the topic's own
// type as its `x-delayed-type`, and a fanout or headers exchange ignores the
// routing key. So under a fanout topic one subscription's delayed nack put a
// copy in every subscription's queue once the delay was over, and under a
// headers topic the queue's name played no part in where the copy went (the
// fake routes it nowhere).
//
// The delayed exchange is `direct` for every topic. A topic topic, the
// default, kept `topic`, and the plugin on the pinned delayed image (RabbitMQ
// 4.2.9, plugin 4.2.0) routes nothing through an exchange of that type, so
// every parked copy was dropped once its delay was over (rmq-12). The fake
// routes by a topic type, so on it this test sees only the type.
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
			if ex, ok := broker.Exchange("orders.delayed.direct"); !ok || ex.DelayedType != amqp.ExchangeDirect {
				t.Errorf("orders.delayed.direct has x-delayed-type %q, want %q: the park is routed by the queue's name",
					ex.DelayedType, amqp.ExchangeDirect)
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

// TestAnOlderTopicTypedDelayedExchangeIsLeftAlone is a deployment an earlier
// client used the plugin arm on: `orders.delayed` exists, declared with the
// topic's type and bound to the subscription's queue by its name.
//
// RabbitMQ accepts a later declaration of it with `x-delayed-type: direct`
// without a word and keeps the old type, and on RabbitMQ 4.2 a topic-typed
// delayed exchange routes nothing: a client that parked there lost each
// delayed message once its nack acknowledged the original. A client parks in
// `orders.delayed.direct`, an exchange of its own, and leaves the older one to
// the clients that still use it. The fake routes a topic type, so the loss
// itself shows only on the broker
// (TestAnOlderTopicTypedDelayedExchangeLosesNothingOnABroker); here the park
// has to go to the new exchange and the old one has to be untouched.
func TestAnOlderTopicTypedDelayedExchangeIsLeftAlone(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{DelayedMessagePlugin: true})
	t.Cleanup(func() { _ = broker.Close() })
	older, err := broker.Dialer().Dial(t.Context(), []string{"localhost:5672"}, client.DialConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = older.Close() })
	raw, err := older.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.QueueDeclare("orders.workers", true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatal(err)
	}
	topicTyped := amqp.Table{rabbitfake.DelayedTypeArg: amqp.ExchangeTopic}
	if err := raw.ExchangeDeclare("orders.delayed", rabbitfake.ExchangeDelayed, true, false, false, false, topicTyped); err != nil {
		t.Fatalf("declaring the older client's delayed exchange: %v", err)
	}
	if err := raw.QueueBind("orders.workers", "orders.workers", "orders.delayed", false, nil); err != nil {
		t.Fatal(err)
	}

	cfg := base()
	cfg.Declare, cfg.DetectDelayPlugin = true, true
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	cons := consumeOn(t, cl, "orders", "workers")
	if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("later")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Nack(mq.NackDelay(3*time.Second)) != nil {
		t.Fatal("the delayed nack failed")
	}
	broker.Sync()
	held := broker.Delayed()
	parkedIn := make([]string, 0, len(held))
	for _, p := range held {
		parkedIn = append(parkedIn, p.Exchange)
	}
	if len(parkedIn) != 1 || parkedIn[0] != "orders.delayed.direct" {
		t.Fatalf("the parked copy is held by %q, want [orders.delayed.direct]: the older exchange keeps its topic "+
			"type whatever a later client declares, and on RabbitMQ 4.2 routes nothing", parkedIn)
	}
	if ex, _ := broker.Exchange("orders.delayed"); ex.DelayedType != amqp.ExchangeTopic {
		t.Fatalf("orders.delayed has x-delayed-type %q, want the older client's %q", ex.DelayedType, amqp.ExchangeTopic)
	}
	broker.Advance(4 * time.Second)
	broker.Sync()
	if back := receiveOne(t, cons); string(back.Body) != "later" {
		t.Fatalf("received %q after the delay, want the parked copy", back.Body)
	}
}

// TestAMissingDelayedExchangeFailsTheConsumerOpen is a deployment with
// Config.Declare off on the plugin arm, whose operator has created the
// subscription's topology and not `orders.delayed.direct`.
//
// The consumer opened, and every delayed nack's park then met a 404 on its
// channel: the nack reported SettlementUnknown without naming the exchange,
// and took back every delivery the consumer held. The open now asks for the
// exchange, as it asks for the queue, and fails with NotFound naming it.
func TestAMissingDelayedExchangeFailsTheConsumerOpen(t *testing.T) {
	for _, provisioned := range []bool{false, true} {
		name := "missing"
		if provisioned {
			name = "provisioned"
		}
		t.Run(name, func(t *testing.T) {
			broker := rabbitfake.New(rabbitfake.Config{DelayedMessagePlugin: true})
			t.Cleanup(func() { _ = broker.Close() })
			ops, err := broker.Dialer().Dial(t.Context(), []string{"localhost:5672"}, client.DialConfig{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ops.Close() })
			raw, err := ops.Channel()
			if err != nil {
				t.Fatal(err)
			}
			if err := raw.ExchangeDeclare("orders", amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := raw.QueueDeclare("orders.workers", true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
				t.Fatal(err)
			}
			if err := raw.QueueBind("orders.workers", "orders", "orders", false, nil); err != nil {
				t.Fatal(err)
			}
			if provisioned {
				direct := amqp.Table{rabbitfake.DelayedTypeArg: amqp.ExchangeDirect}
				if err := raw.ExchangeDeclare("orders.delayed.direct", rabbitfake.ExchangeDelayed, true, false, false, false, direct); err != nil {
					t.Fatal(err)
				}
				if err := raw.QueueBind("orders.workers", "orders.workers", "orders.delayed.direct", false, nil); err != nil {
					t.Fatal(err)
				}
			}
			cfg := base()
			cfg.DelayKind = delay.Plugin
			cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
			if err != nil {
				t.Fatalf("opening a client over the fake broker: %v", err)
			}
			t.Cleanup(func() { _ = cl.Close() })
			_, err = cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
			if provisioned {
				if err != nil {
					t.Fatalf("opening a consumer with orders.delayed.direct provisioned: %v", err)
				}
				return
			}
			if mq.CodeOf(err) != mq.CodeNotFound || !contains(err.Error(), "orders.delayed.direct") {
				t.Fatalf("opening a consumer with orders.delayed.direct missing: %v, want NotFound naming the "+
					"exchange", err)
			}
		})
	}
}

// TestADelayedNackToAMissingDelayedExchangeNamesIt is a park that meets the
// broker's 404 for `<exchange>.delayed.direct`, here because an operator
// deleted it after the consumer opened. The broker stored no copy and took the
// original back with the channel, so the outcome is known: LeaseLost, with the
// broker's reason naming the exchange. It was SettlementUnknown, which said
// the copy might have been stored and named nothing.
func TestADelayedNackToAMissingDelayedExchangeNamesIt(t *testing.T) {
	broker, cl := openFakeWith(t, rabbitfake.Config{DelayedMessagePlugin: true}, func(c *rq.Config) {
		c.Declare, c.DetectDelayPlugin = true, true
	})
	cons := consumeOn(t, cl, "orders", "workers")
	if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	raw, err := backendOf(t, cl).Conn().Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.ExchangeDelete("orders.delayed.direct", false, false); err != nil {
		t.Fatal(err)
	}
	err = d.Nack(mq.NackDelay(time.Second))
	if !errors.Is(err, mq.ErrLeaseLost) || mq.IsAmbiguous(err) || !contains(err.Error(), "orders.delayed.direct") {
		t.Fatalf("the delayed nack whose exchange is missing reported %v (ambiguous %v), want LeaseLost naming "+
			"orders.delayed.direct: the broker stored no copy and took the original back", err, mq.IsAmbiguous(err))
	}
	broker.Sync()
	if again := receiveOne(t, cons); string(again.Body) != "x" {
		t.Fatalf("received %q, want the original back", again.Body)
	}
}

// TestALongTopicOpensOffThePluginArm is the 255-byte name limit and the
// delayed exchange's longer name.
//
// `<exchange>.delayed.direct` is 7 bytes longer than `<exchange>.delayed`, and
// it was checked for every handle, so a topic whose exchange name was 241 to
// 247 bytes long no longer opened at all, plugin or not. Only the plugin arm
// declares that exchange or parks in it, so only there is the limit 240 bytes.
func TestALongTopicOpensOffThePluginArm(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin bool
		length int
		opens  bool
	}{
		{"off the plugin arm, 241 bytes", false, 241, true},
		{"off the plugin arm, 247 bytes", false, 247, true},
		{"off the plugin arm, 248 bytes", false, 248, false},
		{"on the plugin arm, 240 bytes", true, 240, true},
		{"on the plugin arm, 241 bytes", true, 241, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cl := openFakeWith(t, rabbitfake.Config{DelayedMessagePlugin: tc.plugin}, func(c *rq.Config) {
				c.Declare, c.DetectDelayPlugin = true, tc.plugin
			})
			topic := strings.Repeat("t", tc.length)
			pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
			if err == nil {
				_ = pub.Close()
			}
			if tc.opens && err != nil {
				t.Fatalf("a publisher on a topic of %d bytes: %v", tc.length, err)
			}
			if !tc.opens && mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Fatalf("a publisher on a topic of %d bytes: %v, want InvalidArgument", tc.length, err)
			}
		})
	}
}

// TestADeletedDelayedExchangeIsDeclaredAgainAfterItsFirstRefusal is the plugin
// arm's `<exchange>.delayed.direct` deleted by an operator while the ledger still
// vouches for it, and with it its binding to the subscription's queue.
//
// A park then publishes to an exchange that is not there, on the consume
// channel, and the broker answers with a 404 that closes it: every delivery
// the consumer holds is taken back. That first refusal stays, because the
// park's confirm has to come before the original's ack, so the park goes out
// on the consume channel. What is fixed is every refusal after it: the TTL arm
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
	if err := raw.ExchangeDelete("orders.delayed.direct", false, false); err != nil {
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
				"get `orders.delayed.direct` declared again, so every delayed nack meets the same 404 on the consume "+
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
				"the broker dropped %v: `orders.delayed.direct` was declared again without its binding", err, dropped)
		}
		_ = dst[0].Ack()
	}
}

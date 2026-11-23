package rabbitmq_test

import (
	"bytes"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// With Config.KeyAsRoutingKey the message key travels as the routing key and
// the consumer reads it back from there. Two publications have another routing
// key: a keyless one, which is routed by the topic so that it reaches the
// queues, and a delayed redelivery's parked copy, which is routed by the
// subscription's queue so that it comes back to that queue alone. Both used to
// arrive with that routing key as their key.

// TestAKeyAsRoutingKeyDelayedRedeliveryKeepsItsKey is the parked copy, on both
// delay arms and across two delays. The redelivery used to arrive with the
// queue's name as its key, which is what KeyAsRoutingKey exists to prevent:
// per-key ordering and every middleware keyed on the message key saw another
// key from the first delayed retry on.
func TestAKeyAsRoutingKeyDelayedRedeliveryKeepsItsKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin bool
	}{{name: "ttldlx", plugin: false}, {name: "plugin", plugin: true}} {
		t.Run(tc.name, func(t *testing.T) {
			broker, client := openFakeWith(t, rabbitfake.Config{DelayedMessagePlugin: tc.plugin}, func(c *rq.Config) {
				c.Declare, c.KeyAsRoutingKey = true, true
				c.DetectDelayPlugin = tc.plugin
			})
			cons := consumeOn(t, client, "orders", "workers")
			pub := publishOn(t, client, "orders")
			if err := pub.Publish(t.Context(), &mq.Message{Key: []byte("customer-42"), Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			d := receiveOne(t, cons)
			if string(d.Key) != "customer-42" {
				t.Fatalf("the first delivery's key is %q, want %q, so this test proves nothing", d.Key, "customer-42")
			}
			for hop := 1; hop <= 2; hop++ {
				if err := d.Nack(mq.NackDelay(100 * time.Millisecond)); err != nil {
					t.Fatalf("delayed nack %d: %v", hop, err)
				}
				broker.Sync()
				broker.Advance(time.Second)
				d = receiveOne(t, cons)
				if string(d.Key) != "customer-42" {
					t.Fatalf("the delayed redelivery after %d delay(s) arrived with key %q, want %q: the parked "+
						"copy is routed by the queue's name, and that is not the message's key", hop, d.Key, "customer-42")
				}
			}
			if err := d.Ack(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestADeadLetterUnderKeyAsRoutingKeyKeepsItsKey is a message the broker
// dead-letters onto a Config.DeadLetterTopic. The broker routes it by the
// dead-letter routing key, so it used to arrive with that as its key, which
// is the dead-letter topic's name.
func TestADeadLetterUnderKeyAsRoutingKeyKeepsItsKey(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.KeyAsRoutingKey = true, true
		c.DeadLetter, c.DeadLetterTopic = true, "parked"
	})
	dead := consumeOn(t, client, "parked", "audit")
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	for _, key := range [][]byte{[]byte("customer-42"), nil} {
		if err := pub.Publish(t.Context(), &mq.Message{Key: key, Body: []byte("poison")}); err != nil {
			t.Fatal(err)
		}
		d := receiveOne(t, cons)
		if err := d.Reject(); err != nil {
			t.Fatalf("rejecting: %v", err)
		}
		broker.Sync()
		got := receiveOne(t, dead)
		if !bytes.Equal(got.Key, key) || (key == nil) != (got.Key == nil) {
			t.Fatalf("a message published with key %q arrived on the dead-letter topic with key %q", key, got.Key)
		}
		if err := got.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestAKeylessPublicationUnderKeyAsRoutingKeyArrivesKeyless is the keyless
// publication. It used to arrive with the topic as its key, so a consumer
// could not tell it from one published with that key, and a keyed one whose
// key is the topic's name still has to keep it.
func TestAKeylessPublicationUnderKeyAsRoutingKeyArrivesKeyless(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare, c.KeyAsRoutingKey = true, true })
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("keyless")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if d.Key != nil {
		t.Fatalf("a keyless publication arrived with key %q, want none", d.Key)
	}
	if err := d.Nack(mq.NackDelay(100 * time.Millisecond)); err != nil {
		t.Fatalf("delayed nack: %v", err)
	}
	broker.Sync()
	broker.Advance(time.Second)
	d = receiveOne(t, cons)
	if d.Key != nil {
		t.Fatalf("a keyless publication's delayed redelivery arrived with key %q, want none", d.Key)
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}

	if err := pub.Publish(t.Context(), &mq.Message{Key: []byte("orders"), Body: []byte("keyed")}); err != nil {
		t.Fatal(err)
	}
	d = receiveOne(t, cons)
	if string(d.Key) != "orders" {
		t.Fatalf("a publication keyed with the topic's name arrived with key %q, want %q", d.Key, "orders")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
}

// TestAKeyOver255BytesIsRefusedAsARoutingKey is the bound on a key that travels
// as the routing key, a shortstr of at most 255 bytes, in every metadata mode:
// an enveloped publication is routed by its key too. amqp091 cannot encode a
// longer one and closed the whole connection, so the publication failed as
// Unknown and every handle on the connection reconnected. It is refused with
// MessageTooLarge before any I/O, and a 255-byte key is routed and arrives.
func TestAKeyOver255BytesIsRefusedAsARoutingKey(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare, c.KeyAsRoutingKey = true, true })
	b := backendOf(t, client)
	cons := consumeOn(t, client, "orders", "workers")
	for _, mode := range []mq.MetadataMode{mq.MetadataNativeStrict, mq.MetadataAdaptive, mq.MetadataPortableEnvelope} {
		pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Metadata: mode})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pub.Close() })
		restores, conn := rq.RestoresForTest(b), broker.LastConnection()
		err = pub.Publish(t.Context(), &mq.Message{Key: bytes.Repeat([]byte("k"), 256), Body: []byte("long")})
		if mq.CodeOf(err) != mq.CodeMessageTooLarge || mq.IsAmbiguous(err) {
			t.Errorf("%v: a 256-byte key as the routing key = %v (code %v, ambiguous %v), want CodeMessageTooLarge "+
				"before any I/O", mode, err, mq.CodeOf(err), mq.IsAmbiguous(err))
		}
		if got := rq.RestoresForTest(b); got != restores || conn.IsClosed() {
			t.Fatalf("%v: a 256-byte key cost the connection: restores %d to %d", mode, restores, got)
		}
	}
	pub := publishOn(t, client, "orders")
	key := bytes.Repeat([]byte("k"), 255)
	if err := pub.Publish(t.Context(), &mq.Message{Key: key, Body: []byte("fits")}); err != nil {
		t.Fatalf("a 255-byte key as the routing key: %v", err)
	}
	d := receiveOne(t, cons)
	if !bytes.Equal(d.Key, key) || string(d.Body) != "fits" {
		t.Errorf("the 255-byte key's publication arrived as %q with a %d-byte key", d.Body, len(d.Key))
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
}

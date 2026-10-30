package rabbitmq_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestDeliveryInfoTopicIsTheTopicAndNotTheExchange holds DeliveryInfo.Topic to
// the topic the consumer was opened on, on a first delivery and on a delayed
// redelivery by either arm.
//
// It used to be the delivery's exchange: the prefixed name under
// Config.Prefix, empty for a TTL+DLX redelivery, which the default exchange
// carries back from the delay queue, and `<exchange>.delayed` for a plugin
// redelivery. That reached HeaderOriginTopic on a dead-lettered copy and every
// metric and log line labelled by topic.
func TestDeliveryInfoTopicIsTheTopicAndNotTheExchange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin bool
	}{{name: "ttldlx", plugin: false}, {name: "plugin", plugin: true}} {
		t.Run(tc.name, func(t *testing.T) {
			broker, client := openFakeWith(t, rabbitfake.Config{DelayedMessagePlugin: tc.plugin}, func(c *rq.Config) {
				c.Declare, c.Prefix = true, "app."
				c.DetectDelayPlugin = tc.plugin
			})
			cons := consumeOn(t, client, "orders", "workers")
			pub := publishOn(t, client, "orders")
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			d := receiveOne(t, cons)
			if got := d.Info().Topic; got != "orders" {
				t.Fatalf("the first delivery's Info().Topic is %q, want %q", got, "orders")
			}
			if err := d.Nack(mq.NackDelay(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			broker.Sync()
			broker.Advance(time.Second)
			again := receiveOne(t, cons)
			if got := again.Info().Topic; got != "orders" {
				t.Fatalf("the delayed redelivery's Info().Topic is %q (Source %q), want %q",
					got, again.Info().Source, "orders")
			}
			if err := again.Ack(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

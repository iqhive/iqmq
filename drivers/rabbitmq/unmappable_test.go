package rabbitmq_test

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
)

// TestUnmappableDeliveriesDoNotStopTheConsumer is the consumer that stopped
// for good.
//
// With no dead-letter route a delivery the consumer cannot map used to be left
// unsettled, and each one holds a `basic.qos` slot, so `prefetch` of them
// stopped the consumer: the message behind them never arrived. They are now
// parked at the tail of the queue, which frees the slot, and none of them is
// destroyed on the way.
func TestUnmappableDeliveriesDoNotStopTheConsumer(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare = true })
	const prefetch = 2
	// An enveloping consumer, so that a plain body is a delivery it cannot
	// map; and an enveloping publisher, so that it has something it can.
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) {
		c.Metadata = mq.MetadataPortableEnvelope
		c.Prefetch = prefetch
	})
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	// The consumer is attached by its first receive, so one mappable message
	// goes first to get the queue declared and consumed from.
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	for range prefetch + 1 {
		if _, err := broker.Publish("", "orders.workers", amqp.Publishing{Body: []byte("not an envelope")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	if string(d.Body) != "after" {
		t.Fatalf("the delivery behind the unmappable ones is %q, want %q", d.Body, "after")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if got := broker.Dropped(); len(got) != 0 {
		t.Fatalf("the broker dropped %d message(s): an unmappable delivery with no dead-letter route must not be destroyed", len(got))
	}
	if got := broker.DeadLettered(); len(got) != 0 {
		t.Fatalf("the broker dead-lettered %d message(s) with no dead-letter route configured", len(got))
	}
}

package rabbitmq_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
)

// TestEnvelopedPublicationRoutesByItsKey is a publication the envelope wraps,
// published with the key as the routing key.
//
// The wrapped copy carries only the topic and the body, and the sink used to
// route by that copy's Key, which is empty: every enveloped publication went
// out with the topic as its routing key, and a binding on a key pattern never
// matched it.
func TestEnvelopedPublicationRoutesByItsKey(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare, c.KeyAsRoutingKey = true, true })
	consumeOn(t, client, "orders", "workers")
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Publish(t.Context(), &mq.Message{Key: []byte("eu.7"), Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	got := broker.Publications("orders.workers")
	if len(got) != 1 {
		t.Fatalf("orders.workers received %d publications, want 1", len(got))
	}
	if got[0].RoutingKey != "eu.7" {
		t.Fatalf("the enveloped publication was routed by %q, want its key %q", got[0].RoutingKey, "eu.7")
	}
}

// TestEnvelopedAsyncPublicationFillsTheCallersReceipt is the asynchronous half
// of the same mix-up: the pending publication kept the wrapped copy, which has
// no receipt slot, so a PublishAsync under an envelope never had its receipt
// filled.
func TestEnvelopedAsyncPublicationFillsTheCallersReceipt(t *testing.T) {
	_, client := openFake(t, declaring)
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{
		Topic: "orders", Metadata: mq.MetadataPortableEnvelope, Receipts: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	var r mq.PublishReceipt
	m := &mq.Message{Body: []byte("x"), Receipt: &r}
	results := make(chan mq.PublishResult, 1)
	if err := pub.PublishAsync(t.Context(), m, func(res mq.PublishResult) { results <- res }); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-results:
		if res.Err != nil {
			t.Fatalf("the publication failed: %v", res.Err)
		}
		if res.Receipt.Sequence == 0 || res.Receipt.ConfirmedAt.IsZero() {
			t.Fatalf("the receipt is %+v, want the delivery tag and the confirmation time", res.Receipt)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the asynchronous publication never resolved")
	}
}

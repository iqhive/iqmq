package memory_test

import (
	"context"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/memory"
)

func TestPublishAndReceive(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("cust-42"), Body: []byte(`{"id":1}`)}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]mq.Delivery, 4)
	n, err := c.Receive(ctx, dst)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if n != 1 {
		t.Fatalf("Receive returned %d deliveries, want 1", n)
	}
	if got := string(dst[0].Body); got != `{"id":1}` {
		t.Fatalf("body %q", got)
	}
	if got := string(dst[0].Key); got != "cust-42" {
		t.Fatalf("key %q", got)
	}
	if dst[0].Attempt != 1 {
		t.Fatalf("attempt %d, want 1", dst[0].Attempt)
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Acked != 1 || snap.Published != 1 || snap.Ready != 0 || snap.InFlight != 0 {
		t.Fatalf("snapshot %+v", snap)
	}
	if !snap.Conserved() {
		t.Fatalf("conservation broken: %+v", snap)
	}
}

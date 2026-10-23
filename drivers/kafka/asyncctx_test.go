package kafka_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestAsyncPublishOutlivesTheCallersContext: a publication that returned nil
// without waiting for the broker — ConfirmNone, PublishAsync, PublishOwned —
// is the producer's from then on. The usual caller cancels its context as soon
// as the call returns, and franz-go fails a buffered record whose context is
// cancelled, so a record bound to that context was dropped after the caller
// had been told it was admitted. The linger keeps every record in the buffer
// across the cancellation.
func TestAsyncPublishOutlivesTheCallersContext(t *testing.T) {
	c, topic := openClient(t, func(cfg *kq.Config) { cfg.Linger = 300 * time.Millisecond })
	cons := consumerOn(t, c, topic, "g-async-ctx")
	none, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("opening a ConfirmNone publisher: %v", err)
	}
	defer func() { _ = none.Close() }()
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	if err := none.Publish(ctx, &mq.Message{Body: []byte("none")}); err != nil {
		t.Fatalf("ConfirmNone Publish: %v", err)
	}
	if err := none.Publish(ctx, &mq.Message{Body: []byte("none-routed")}, mq.Partition(0)); err != nil {
		t.Fatalf("ConfirmNone Publish to a partition: %v", err)
	}
	async := make(chan error, 1)
	if err := p.PublishAsync(ctx, &mq.Message{Body: []byte("async")}, func(res mq.PublishResult) { async <- res.Err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := p.PublishOwned(ctx, mq.OwnedMessage{Message: &mq.Message{Body: []byte("owned")}}); err != nil {
		t.Fatalf("PublishOwned: %v", err)
	}
	cancel()

	if err := <-async; err != nil {
		t.Errorf("PublishAsync's outcome after the caller's context was cancelled = %v, want nil", err)
	}
	for _, pub := range []*mq.PublisherHandle{none, p} {
		if err := pub.Flush(t.Context()); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	got := map[string]bool{}
	receiveN(t, cons, 4, 5*time.Second, func(d *mq.Delivery) error {
		got[string(d.Body)] = true
		return d.Ack()
	})
	for _, body := range []string{"none", "none-routed", "async", "owned"} {
		if !got[body] {
			t.Errorf("%q was admitted and never arrived", body)
		}
	}
}

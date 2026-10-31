package iqmq_test

import (
	"context"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// TestARouteThatReachedSomeDeadLetterSubscriptionsSettles: a dead-letter
// topic whose subscriptions take a copy independently (memory's
// FanoutIndependent) answers mq.ErrPartial when some of them took it and the
// others had no room. The copy is on the subscriptions that took it, so the
// route counts it published and acks the delivery, as the memory driver's
// own two routes do. It used to read the answer as a failed publication and
// requeue the delivery, and every redelivery published the copy again to the
// subscriptions that already had it.
func TestARouteThatReachedSomeDeadLetterSubscriptionsSettles(t *testing.T) {
	ctx := context.Background()
	cl, b := memory.NewForTest(t, memory.TestConfig(memory.Config{
		DefaultTopology: memory.TopologyConfig{
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
			Fanout:           memory.FanoutIndependent,
		},
	}), memory.TestTopics("orders", "orders.dlq"))
	admin, err := cl.Admin()
	if err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{"full", "room"} {
		if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders.dlq", Name: sub}); err != nil {
			t.Fatal(err)
		}
	}
	// The filler fills both subscriptions; room is then emptied, so the
	// copy fits there and not in full.
	if err := cl.Publish(ctx, "orders.dlq", &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatal(err)
	}
	room, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "room"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = room.Close() })
	dst := make([]mq.Delivery, 1)
	if n, err := room.Receive(ctx, dst); n != 1 || err != nil {
		t.Fatalf("setup: emptying room = %d, %v", n, err)
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatal(err)
	}

	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	publishToMemory(t, cl, "orders", 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
	)
	// The route counts the publication before it acks, so it is the ack (or
	// a failure) that says the route has finished with the delivery.
	waitFor(t, "the original to be acked or the route to count a failure", func() bool {
		st := c.Stats()
		return st.Acked > 0 || st.DeadLetterFailures > 0
	})
	got := c.Stats()
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got.DeadLetterPublished != 1 || got.DeadLetterFailures != 0 || got.Acked != 1 {
		t.Fatalf("DeadLetterPublished = %d, DeadLetterFailures = %d, Acked = %d: want the copy that reached "+
			"room counted published and the delivery acked", got.DeadLetterPublished, got.DeadLetterFailures, got.Acked)
	}
	if n := inspect(t, b, "orders.dlq", "room").Published; n != 2 {
		t.Fatalf("room was handed %d record(s), want 2: the filler and one copy", n)
	}
}

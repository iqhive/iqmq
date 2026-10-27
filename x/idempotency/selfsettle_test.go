package idempotency_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/idempotency"
)

// runGuarded runs handler behind a fresh interceptor on a memory broker's
// "orders" topic and returns a publish function and the interceptor. A
// delivery a test built itself carries no settlement right, so the
// self-settling handlers these tests are about need the real runner.
func runGuarded(t *testing.T, handler mq.Handler) (func(id string), *idempotency.Interceptor) {
	t.Helper()
	cl, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	store, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ic, err := idempotency.New(idempotency.Config{Store: store, Key: idempotency.KeyFromID(), Window: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, ic.Middleware()(handler))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	publish := func(id string) {
		t.Helper()
		if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: id, Body: []byte("x")}); err != nil {
			t.Fatalf("publishing %s: %v", id, err)
		}
	}
	return publish, ic
}

// waitUntil polls cond for up to five seconds.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestAHandlerThatNackedItsOwnDeliveryIsNotCommitted is the reported defect:
// a handler that nacked its delivery and returned nil had its key committed,
// so the redelivery it asked for read as a duplicate and was acked without
// reaching it. The work was lost.
func TestAHandlerThatNackedItsOwnDeliveryIsNotCommitted(t *testing.T) {
	var calls atomic.Int64
	publish, ic := runGuarded(t, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		if calls.Add(1) == 1 {
			if err := d.Nack(); err != nil {
				t.Errorf("Nack: %v", err)
			}
		}
		return nil
	}))
	publish("order-1")
	waitUntil(t, "the redelivery to reach the handler", func() bool { return calls.Load() == 2 })
	c := ic.Counts()
	if c.Skipped != 0 {
		t.Fatalf("Counts = %+v: the redelivery of a nacked delivery was skipped as a duplicate", c)
	}
	if c.Failed != 1 || c.Processed != 2 {
		t.Fatalf("Counts = %+v, want Processed 2 and Failed 1: the nack aborts the claim as a failure does", c)
	}
}

// TestAHandlerThatAckedItsOwnDeliveryIsCommitted is the other half: an Ack is
// the work done, so its key is committed and a later duplicate is skipped.
func TestAHandlerThatAckedItsOwnDeliveryIsCommitted(t *testing.T) {
	var calls atomic.Int64
	publish, ic := runGuarded(t, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		calls.Add(1)
		return d.Ack()
	}))
	publish("order-1")
	publish("order-1")
	waitUntil(t, "both publications to be handled or skipped", func() bool {
		return calls.Load()+ic.Counts().Skipped == 2
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times, want 1: a self-acked delivery is work done and its duplicate is skipped", n)
	}
	if c := ic.Counts(); c.Skipped != 1 || c.Failed != 0 {
		t.Fatalf("Counts = %+v, want Skipped 1 and Failed 0", c)
	}
}

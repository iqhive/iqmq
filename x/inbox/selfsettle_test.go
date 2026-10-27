package inbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/inbox"
)

// runInbox runs handler behind a fresh inbox on a memory broker's "orders"
// topic and returns a publish function and the middleware. A delivery a test
// built itself carries no settlement right, so the self-settling handlers
// these tests are about need the real runner.
func runInbox(t *testing.T, handler mq.Handler) (func(id string), *inbox.Middleware) {
	t.Helper()
	cl, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	g, m := guarded(t, inbox.Config{Store: memStore(t, inbox.MemoryOptions{}), Key: inbox.KeyFromID()}, handler)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, g)
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
	return publish, m
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

// TestAHandlerThatNackedItsOwnDeliveryIsRolledBack is the reported defect: a
// handler that nacked its delivery and returned nil had its record committed,
// so the redelivery it asked for read as a duplicate and was acked without
// reaching it. The work was lost.
func TestAHandlerThatNackedItsOwnDeliveryIsRolledBack(t *testing.T) {
	var calls atomic.Uint64
	publish, m := runInbox(t, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		if calls.Add(1) == 1 {
			if err := d.Nack(); err != nil {
				t.Errorf("Nack: %v", err)
			}
		}
		return nil
	}))
	publish("order-1")
	waitUntil(t, "the redelivery to reach the handler", func() bool { return calls.Load() == 2 })
	waitUntil(t, "the redelivery to commit", func() bool { return m.Counts().Processed == 1 })
	c := m.Counts()
	if c.Duplicate != 0 {
		t.Fatalf("Counts = %+v: the redelivery of a nacked delivery was acked as a duplicate", c)
	}
	if c.Failed != 1 {
		t.Fatalf("Counts = %+v, want Failed 1: the nack rolls the transaction back", c)
	}
}

// TestAHandlerThatAckedItsOwnDeliveryCommits is the other half: an Ack is the
// work done, so its record commits and a later duplicate is acked unrun.
func TestAHandlerThatAckedItsOwnDeliveryCommits(t *testing.T) {
	var calls atomic.Uint64
	publish, m := runInbox(t, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		calls.Add(1)
		return d.Ack()
	}))
	publish("order-1")
	publish("order-1")
	waitUntil(t, "both publications to be handled or recognised", func() bool {
		return calls.Load()+m.Counts().Duplicate == 2
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times, want 1: a self-acked delivery is work done and its duplicate is not run", n)
	}
	if c := m.Counts(); c.Processed != 1 || c.Duplicate != 1 || c.Failed != 0 {
		t.Fatalf("Counts = %+v, want Processed 1, Duplicate 1, Failed 0", c)
	}
}

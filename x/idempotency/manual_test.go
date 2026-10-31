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

// TestADuplicateIsSettledUnderManualSettlement is the reported defect: the
// duplicate path returned nil and settled nothing, which under
// mq.SettlementManual -- where the runner settles nothing either -- left the
// duplicate in flight, to be redelivered and skipped again every time its
// lease ran out, for ever.
func TestADuplicateIsSettledUnderManualSettlement(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
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
	var calls atomic.Int64
	// Under manual settlement the handler owns the deliveries it is given,
	// so it acks the one that reaches it; the duplicate never does.
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		calls.Add(1)
		return d.Ack()
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, ic.Middleware()(handler), mq.WithSettlement(mq.SettlementManual))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	for range 2 {
		if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: "order-1", Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, "both deliveries to be settled", func() bool {
		s, err := b.Inspect("orders", "billing")
		return err == nil && s.Acked == 2 && s.InFlight == 0
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times, want 1", n)
	}
	if c := ic.Counts(); c.Skipped != 1 {
		t.Fatalf("Counts = %+v, want Skipped 1", c)
	}
}

// TestAKeptDeliveryNackedLaterIsNotCommitted: under mq.SettlementManual a
// handler may keep its delivery and settle it after it returns. One that
// hands it to a worker and returns nil, the worker then nacking it for
// redelivery, has failed it as surely as a handler that nacked before
// returning, whose claim is aborted. The middleware decides as the handler
// returns, sees an unsettled delivery and a nil error, and commits the claim,
// so the redelivery the Nack asked for is skipped as a duplicate.
func TestAKeptDeliveryNackedLaterIsNotCommitted(t *testing.T) {
	t.Skip("open, waiting for a decision: the middleware cannot see a settlement made after the handler returns, " +
		"nor tell SettlementManual from SettlementAuto, so it cannot tell this case from a success the runner acks")
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
	var calls atomic.Int64
	kept := make(chan *mq.Delivery, 2)
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		calls.Add(1)
		kept <- d // handed to a worker, which settles it later
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, ic.Middleware()(handler), mq.WithSettlement(mq.SettlementManual))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: "order-1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := (<-kept).Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	waitUntil(t, "the redelivery to be handled or skipped", func() bool {
		return calls.Load()+ic.Counts().Skipped == 2
	})
	if c := ic.Counts(); c.Skipped != 0 {
		t.Fatalf("Counts = %+v: the redelivery of a kept delivery nacked after its handler returned was skipped as a duplicate", c)
	}
}

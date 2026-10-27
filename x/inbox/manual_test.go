package inbox_test

import (
	"context"
	"sync/atomic"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/inbox"
)

// TestADuplicateIsSettledUnderManualSettlement is the reported defect: the
// duplicate path returned nil and settled nothing, which under
// mq.SettlementManual -- where the runner settles nothing either -- left the
// duplicate in flight, to be redelivered and recognised again every time its
// lease ran out, for ever.
func TestADuplicateIsSettledUnderManualSettlement(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var calls atomic.Uint64
	// Under manual settlement the handler owns the deliveries it is given,
	// so it acks the one that reaches it; the duplicate never does.
	g, m := guarded(t, inbox.Config{Store: memStore(t, inbox.MemoryOptions{}), Key: inbox.KeyFromID()},
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			calls.Add(1)
			return d.Ack()
		}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, g, mq.WithSettlement(mq.SettlementManual))
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
	if c := m.Counts(); c.Duplicate != 1 {
		t.Fatalf("Counts = %+v, want Duplicate 1", c)
	}
}

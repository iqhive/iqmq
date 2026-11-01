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

// TestAKeptDeliveryIsRecordedAsItsHandlerReturns pins what the inbox does
// with a delivery a handler under mq.SettlementManual keeps to settle after
// it returns. middleware.Dedup and x/idempotency read
// mq.Delivery.HandlerSettles and do not record such a delivery. The inbox
// cannot do the same: its record and the handler's writes are one
// transaction, which has to end as the handler returns, and rolled back it
// would undo what the handler wrote while the worker that kept the delivery
// may still ack it, so the message would be acked with its writes gone. The
// transaction commits ("acked later"), and a Nack after that brings the
// message back as a duplicate: what the handler wrote stands, and the work it
// left for after its return is not tried again ("nacked later"), as the
// package documentation says.
func TestAKeptDeliveryIsRecordedAsItsHandlerReturns(t *testing.T) {
	for _, tc := range []struct {
		name      string
		settle    func(d *mq.Delivery) error
		duplicate uint64
	}{
		{name: "acked later", settle: func(d *mq.Delivery) error { return d.Ack() }, duplicate: 0},
		{name: "nacked later", settle: func(d *mq.Delivery) error { return d.Nack() }, duplicate: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
			c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatalf("opening the consumer: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			var calls atomic.Uint64
			kept := make(chan *mq.Delivery, 2)
			g, m := guarded(t, inbox.Config{Store: memStore(t, inbox.MemoryOptions{}), Key: inbox.KeyFromID()},
				mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
					calls.Add(1)
					kept <- d // handed to a worker, which settles it later
					return nil
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
			if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: "order-1", Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			d := <-kept
			waitUntil(t, "the transaction to commit", func() bool { return m.Counts().Processed == 1 })
			if err := tc.settle(d); err != nil {
				t.Fatalf("settling the kept delivery: %v", err)
			}
			waitUntil(t, "the message to be settled for good", func() bool {
				s, err := b.Inspect("orders", "billing")
				return err == nil && s.Acked == 1 && s.InFlight == 0 && s.Ready == 0
			})
			if n, c := calls.Load(), m.Counts(); n != 1 || c.Processed != 1 || c.Duplicate != tc.duplicate || c.Failed != 0 {
				t.Fatalf("the handler ran %d time(s) and Counts = %+v, want one run, Processed 1 and Duplicate %d",
					n, c, tc.duplicate)
			}
		})
	}
}

package inbox_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/inbox"
)

// firstCommitFails is a MemoryStore whose first transaction runs fn and then
// fails to commit, as a SQL COMMIT does on a serialisation failure: nothing
// is recorded and Once reports the commit's error.
type firstCommitFails struct {
	*inbox.MemoryStore
	failed atomic.Bool
}

var errCommit = errors.New("could not serialise access due to concurrent update")

func (s *firstCommitFails) Once(ctx context.Context, key string, fn func(context.Context) error) (bool, error) {
	if !s.failed.CompareAndSwap(false, true) {
		return s.MemoryStore.Once(ctx, key, fn)
	}
	if err := fn(ctx); err != nil {
		return false, err
	}
	return false, errCommit
}

// TestAnAckAfterTheMiddlewareSurvivesAFailedCommit holds the shape the
// package documentation gives for a commit that fails: the Ack comes after
// the middleware has returned nil, from the runner under SettlementAuto or
// from a wrapper outside the middleware under SettlementManual, so the failed
// commit leaves the message unacked and its redelivery does the work. An Ack
// from inside the handler would have reached the broker before the commit
// failed, and the message would have been lost.
func TestAnAckAfterTheMiddlewareSurvivesAFailedCommit(t *testing.T) {
	for _, mode := range []mq.SettlementMode{mq.SettlementAuto, mq.SettlementManual} {
		t.Run(mode.String(), func(t *testing.T) {
			cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
			c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatalf("opening the consumer: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			var calls atomic.Uint64
			store := &firstCommitFails{MemoryStore: memStore(t, inbox.MemoryOptions{})}
			g, m := guarded(t, inbox.Config{Store: store, Key: inbox.KeyFromID()},
				mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					calls.Add(1)
					return nil
				}))
			h := g
			if mode == mq.SettlementManual {
				h = mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
					if err := g.Handle(ctx, d); err != nil {
						return errors.Join(err, d.Nack())
					}
					return d.Ack()
				})
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = c.Run(ctx, h, mq.WithSettlement(mode), mq.WithRetryBackoff(mq.ConstantBackoff(0)))
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})
			if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: "order-1", Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			waitUntil(t, "the redelivery to commit", func() bool { return m.Counts().Processed == 1 })
			waitUntil(t, "the message to be settled for good", func() bool {
				s, err := b.Inspect("orders", "billing")
				return err == nil && s.Acked == 1 && s.InFlight == 0 && s.Ready == 0 && s.Delayed == 0
			})
			if n, got := calls.Load(), m.Counts(); n != 2 || got.Failed != 1 || got.Processed != 1 || !store.Has("order-1") {
				t.Fatalf("the handler ran %d time(s), Counts = %+v, recorded %v; want two runs, Failed 1, Processed 1 and the key recorded",
					n, got, store.Has("order-1"))
			}
		})
	}
}

// TestAnAckBeforeAFailedCommitSaysTheMessageIsLost: a handler that acks inside
// the transaction cannot be saved when the commit then fails, since the Ack
// reached the broker first. The middleware returned the bare commit error,
// which read like a retry to come. It now joins an error that says the Ack
// came first and the message will not be redelivered, and the commit's error
// is still there for errors.Is.
func TestAnAckBeforeAFailedCommitSaysTheMessageIsLost(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	store := &firstCommitFails{MemoryStore: memStore(t, inbox.MemoryOptions{})}
	g, m := guarded(t, inbox.Config{Store: store, Key: inbox.KeyFromID()},
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			return d.Ack()
		}))
	got := make(chan error, 1)
	h := mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		err := g.Handle(ctx, d)
		got <- err
		return err
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx, h, mq.WithSettlement(mq.SettlementManual))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: "order-1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the delivery to reach the middleware")
	}
	if !errors.Is(err, errCommit) || !strings.Contains(err.Error(), "will not be redelivered") {
		t.Fatalf("the middleware returned %v, want the commit's error joined to one saying the message will not be redelivered", err)
	}
	if c := m.Counts(); c.Failed != 1 || store.Has("order-1") {
		t.Fatalf("Counts = %+v, recorded %v; want Failed 1 and nothing recorded", c, store.Has("order-1"))
	}
	if s, err := b.Inspect("orders", "billing"); err != nil || s.Acked != 1 {
		t.Fatalf("Inspect = (%+v, %v), want the message acked by the handler", s, err)
	}
}

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
	// so it acks the one that reaches it; the duplicate never does. It acks
	// inside the transaction to test the duplicate path, not as the shape to
	// copy: an Ack there is lost with its work if the commit fails (see
	// TestAnAckAfterTheMiddlewareSurvivesAFailedCommit).
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

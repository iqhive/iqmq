package middleware_test

import (
	"context"
	"sync/atomic"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/middleware"
)

// TestDedupForgetsAKeyWhoseHandlerNackedItsOwnDelivery is the reported
// defect: a handler that nacked its delivery and returned nil left its key
// recorded, so the redelivery it asked for read as a duplicate and was acked
// without reaching it. The work was lost. The handler that acks its own
// delivery is TestDedupSettlesTheDuplicateItSuppresses, and still keeps its
// key.
func TestDedupForgetsAKeyWhoseHandlerNackedItsOwnDelivery(t *testing.T) {
	h := newHarness(t)
	var calls, duplicates atomic.Int64
	chain := middleware.Dedup(middleware.DedupConfig{
		Store: mustNewLRUStore(t, 64),
		OnDuplicate: func(context.Context, *mq.Delivery, string) {
			duplicates.Add(1)
		},
	})
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		if calls.Add(1) == 1 {
			if err := d.Nack(); err != nil {
				t.Errorf("Nack: %v", err)
			}
		}
		return nil
	})
	r := h.run(chain(handler))
	h.publish(1, func(int) string { return "id-0" })
	waitFor(t, "the redelivery to reach the handler", func() bool { return calls.Load() == 2 })
	_ = r.stop()
	if n := duplicates.Load(); n != 0 {
		t.Fatalf("Dedup acked %d redelivery(ies) of a nacked delivery as a duplicate", n)
	}
}

// contextStore is an IdempotencyStore that, like any network-backed one,
// does nothing for a call whose context has ended. Forget has no error to
// report, so such a call fails silently. It records whether the Forget it
// carried out had a deadline.
type contextStore struct {
	*middleware.LRUStore
	bounded atomic.Bool
}

func (s *contextStore) Seen(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.LRUStore.Seen(ctx, key)
}

func (s *contextStore) Forget(ctx context.Context, key string) {
	if ctx.Err() != nil {
		return
	}
	_, ok := ctx.Deadline()
	s.bounded.Store(ok)
	s.LRUStore.Forget(ctx, key)
}

// TestDedupForgetsAKeyAfterTheDeliverysContextEnded: a handler that fails
// because its context ended (the lease under mq.WithLeaseDeadline, a Timeout
// outside Dedup, a runner shutting down) has failed, and Forget is what lets
// the redelivery be processed. Dedup called Forget with that same ended
// context, so a store that honours its context kept the key, and the
// redelivery was acked as a duplicate without reaching the handler: the
// message was lost. Forget now runs on the delivery's values without its
// cancellation, under a deadline of its own.
func TestDedupForgetsAKeyAfterTheDeliverysContextEnded(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(ctx context.Context) error
	}{
		{name: "a failure", fail: func(ctx context.Context) error { return ctx.Err() }},
		{name: "a postponement", fail: func(ctx context.Context) error { return mq.Postpone(ctx.Err(), 0) }},
		{name: "a panic", fail: func(context.Context) error { panic("in the handler") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &contextStore{LRUStore: mustNewLRUStore(t, 64)}
			var calls, duplicates atomic.Int64
			var cancel context.CancelFunc
			h := middleware.Dedup(middleware.DedupConfig{
				Store: store,
				OnDuplicate: func(context.Context, *mq.Delivery, string) {
					duplicates.Add(1)
				},
			})(mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
				if calls.Add(1) == 1 {
					cancel() // the first attempt outlives its context
					return tc.fail(ctx)
				}
				return nil
			}))
			first, c := context.WithCancel(context.Background())
			cancel = c
			func() {
				// The runner recovers a panic above Dedup.
				defer func() { _ = recover() }()
				_ = h.Handle(first, delivery2("order", "id-1"))
			}()
			// The broker redelivers the failed message.
			_ = h.Handle(context.Background(), delivery2("order", "id-1"))
			if calls.Load() != 2 || duplicates.Load() != 0 {
				t.Fatalf("the redelivery of a message whose handler failed on its context reached the handler %d time(s) "+
					"and was acked as a duplicate %d time(s): Forget ran on the ended context and the key stayed",
					calls.Load()-1, duplicates.Load())
			}
			if !store.bounded.Load() {
				t.Fatal("Forget ran on a context with no deadline: a store that hangs would hold the delivery for ever")
			}
		})
	}
}

// TestDedupForgetsAKeyWhoseKeptDeliveryIsNackedLater is the same failure one
// step later. Under mq.SettlementManual a handler may keep its delivery and
// settle it after it returns. One that hands it to a worker and returns nil,
// the worker then nacking it for redelivery, has failed it as surely as a
// handler that nacked before returning. Dedup decides as the handler returns,
// saw an unsettled delivery and a nil error, and kept the key, so the
// redelivery the Nack asked for was acked as a duplicate without reaching the
// handler. It now reads mq.Delivery.HandlerSettles and forgets the key of a
// delivery the handler kept.
func TestDedupForgetsAKeyWhoseKeptDeliveryIsNackedLater(t *testing.T) {
	h := newHarness(t)
	var calls, duplicates atomic.Int64
	chain := middleware.Dedup(middleware.DedupConfig{
		Store: mustNewLRUStore(t, 64),
		OnDuplicate: func(context.Context, *mq.Delivery, string) {
			duplicates.Add(1)
		},
	})
	kept := make(chan *mq.Delivery, 2)
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		calls.Add(1)
		kept <- d // handed to a worker, which settles it later
		return nil
	})
	r := h.run(chain(handler), mq.WithSettlement(mq.SettlementManual))
	h.publish(1, func(int) string { return "id-0" })
	if err := (<-kept).Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	waitFor(t, "the redelivery to be handled or acked as a duplicate", func() bool {
		return calls.Load()+duplicates.Load() == 2
	})
	_ = r.stop()
	if n := duplicates.Load(); n != 0 {
		t.Fatalf("Dedup acked %d redelivery(ies) of a kept delivery nacked after its handler returned as a duplicate", n)
	}
}

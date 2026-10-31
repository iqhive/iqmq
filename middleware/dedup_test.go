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

// TestDedupForgetsAKeyWhoseKeptDeliveryIsNackedLater is the same failure one
// step later. Under mq.SettlementManual a handler may keep its delivery and
// settle it after it returns. One that hands it to a worker and returns nil,
// the worker then nacking it for redelivery, has failed it as surely as a
// handler that nacked before returning. Dedup decides as the handler returns,
// sees an unsettled delivery and a nil error, and keeps the key, so the
// redelivery the Nack asked for is acked as a duplicate without reaching the
// handler.
func TestDedupForgetsAKeyWhoseKeptDeliveryIsNackedLater(t *testing.T) {
	t.Skip("open, waiting for a decision: Dedup cannot see a settlement made after the handler returns, " +
		"nor tell SettlementManual from SettlementAuto, so it cannot tell this case from a success the runner acks")
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

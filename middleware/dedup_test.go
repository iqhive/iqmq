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

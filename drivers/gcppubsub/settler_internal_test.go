package gcppubsub

import (
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestAnExpiredDeliveryLeavesTheInflightMap holds that a delivery the local
// lease check finds lost is forgotten as well: a lost delivery is terminal, so
// the in-flight map keeping it is a leak of one entry per expired message.
func TestAnExpiredDeliveryLeavesTheInflightMap(t *testing.T) {
	src := &source{b: &Backend{}, inflight: map[string]*delivery{}}
	set := newSettler(src)
	expired := func(id string) mq.DriverDelivery {
		ref := &delivery{id: id, src: src}
		ref.expires.Store(1)
		src.supersede(ref)
		return mq.DriverDelivery{Ref: ref}
	}
	claimed, extended := expired("claimed"), expired("extended")
	if _, err := set.claim(&claimed); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("claim of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	if err := set.Extend(t.Context(), &extended, time.Second); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Fatalf("the in-flight map holds %d lost deliveries, want 0", n)
	}
}

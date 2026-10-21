package sqs

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
	set := &settler{src: src}
	ref := &delivery{id: "m", src: src}
	ref.expires.Store(1)
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref}
	if err := set.Extend(t.Context(), &d, time.Second); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Extend of an expired delivery returned %v, want ErrLeaseLost", err)
	}
	src.mu.Lock()
	n := len(src.inflight)
	src.mu.Unlock()
	if n != 0 {
		t.Fatalf("the in-flight map holds %d lost deliveries, want 0", n)
	}
}

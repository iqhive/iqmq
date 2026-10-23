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

// fixedClock is an [mq.Clock] whose time moves only when a test moves it.
type fixedClock struct{ at time.Time }

func (c *fixedClock) Now() time.Time { return c.at }

func (c *fixedClock) NewTimer(time.Duration) mq.Timer { panic("not used") }

// TestExtendNeverShortensTheLease holds the specification's rule that Extend
// moves the deadline by at least `by` and never earlier: a short extension on
// a delivery whose local lease runs for the whole budget must not cut it, or
// a later acknowledgement is refused as ErrLeaseLost and the message is
// redelivered.
func TestExtendNeverShortensTheLease(t *testing.T) {
	clk := &fixedClock{at: time.Unix(1_000_000, 0)}
	src := &source{b: &Backend{clock: clk}, budget: 10 * time.Minute, inflight: map[string]*delivery{}}
	set := newSettler(src)
	ref := &delivery{id: "m", src: src}
	ref.received.Store(clk.at.UnixNano())
	ref.expires.Store(clk.at.Add(src.budget).UnixNano())
	src.supersede(ref)
	d := mq.DriverDelivery{Ref: ref}

	clk.at = clk.at.Add(time.Second)
	if err := set.Extend(t.Context(), &d, 30*time.Second); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	clk.at = clk.at.Add(39 * time.Second)
	if _, err := set.claim(&d); err != nil {
		t.Fatalf("claim at t=40s after Extend(30s) on a 10m lease returned %v, want nil", err)
	}
}

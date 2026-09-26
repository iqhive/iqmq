package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
)

// The regression tests for the idle gate's re-entrancy: WaitIdle is woken by
// the settlement path, and the settlement path runs under the lane mutex in
// three places — the time-to-live sweep, a lease expiry that finds an expired
// record, and the drain the engine's own close does. A wake-up that inspected
// the engine would take those lane mutexes again from the goroutine that
// already holds one, which sync.Mutex does not survive.
//
// They run on the real clock rather than in a synctest bubble on purpose: a
// goroutine blocked on a mutex is not durably blocked, so a bubble cannot
// report the deadlock and would hang until the package timeout instead of
// failing. Here the operation runs in a goroutine of its own and the test
// fails within seconds when it does not come back.

// wedgeLimit is how long an operation these tests drive may take before the
// engine is called wedged. Everything they do is a handful of publications on
// an unloaded lane, so the limit is generous by three orders of magnitude.
const wedgeLimit = 10 * time.Second

// mustFinish runs fn in its own goroutine and fails the test when it has not
// returned within wedgeLimit. The goroutine is left behind on failure, which
// is what a deadlock costs: the test binary still exits.
func mustFinish(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(wedgeLimit):
		t.Fatalf("%s did not finish within %v: the engine is wedged", what, wedgeLimit)
	}
}

// idleRig builds an engine of architecture a with one topic, one subscription
// and one consumer. The engine is not closed by a cleanup: a test that has
// already failed on a wedged engine must not then hang in Close.
func idleRig(t *testing.T, a Arch, cfg TopologyConfig) (*Engine[int64], *Topic[int64], *Subscription[int64], *Consumer[int64]) {
	t.Helper()
	e, err := New(Config[int64]{
		Kinds:           KindSet{Arch: a},
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: cfg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	top, sub, c := queueSub(t, e)
	return e, top, sub, c
}

// parkWaiter starts a WaitIdle waiter and returns once the engine can see it.
// The gate is only consulted while somebody is waiting, so the bug these tests
// pin needs a waiter that is already counted.
func parkWaiter(t *testing.T, e *Engine[int64], mode IdleMode) <-chan error {
	t.Helper()
	out := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { out <- e.WaitIdle(ctx, mode) }()
	deadline := time.Now().Add(wedgeLimit)
	for e.idle.waiting.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the WaitIdle waiter never parked")
		}
		time.Sleep(time.Millisecond)
	}
	return out
}

// awaitDropped polls the subscription until it has dropped n records. Inspect
// takes every lane mutex, so a lane whose mutex is held by a wedged settlement
// blocks here and mustFinish reports it.
func awaitDropped(sub *Subscription[int64], n int64) func() {
	return func() {
		for sub.Inspect().Dropped < n {
			time.Sleep(time.Millisecond)
		}
	}
}

// mustIdle waits for a parked waiter to be released.
func mustIdle(t *testing.T, idle <-chan error) {
	t.Helper()
	select {
	case err := <-idle:
		if err != nil {
			t.Fatalf("WaitIdle: %v", err)
		}
	case <-time.After(wedgeLimit):
		t.Fatal("WaitIdle was never woken by the record that left")
	}
}

// TestWaitIdleDuringTimeToLiveSweep: the scheduler's time-to-live sweep drops
// a record while a WaitIdle waiter is parked and another record keeps the
// engine busy. The drop happens under the lane mutex, so a wake-up that walked
// the lanes would deadlock the one scheduler goroutine of the engine.
func TestWaitIdleDuringTimeToLiveSweep(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			e, top, sub, c := idleRig(t, a, TopologyConfig{Partitions: 2, AckDeadline: -1})
			// A record nobody takes keeps the engine-wide count above zero, so
			// that the idle check cannot short-circuit and has to walk lanes.
			// It goes in a lane of its own: under a shared log an expired
			// record is only swept once the subscription's cursor reaches it,
			// so the record that expires must be the one at its own cursor.
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: 1, Size: 1, Partition: 0,
			}); err != nil {
				t.Fatal(err)
			}
			idle := parkWaiter(t, e, IdleDelivered)
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: 2, Size: 1, Partition: 1, TTL: 30 * time.Millisecond,
			}); err != nil {
				t.Fatal(err)
			}
			mustFinish(t, "the time-to-live sweep", awaitDropped(sub, 1))
			// The waiter is still holding out for the record that is left.
			dst := make([]Claim[int64], 1)
			n, err := c.Fetch(poll(t), dst)
			if err != nil || n != 1 || dst[0].Payload != 1 {
				t.Fatalf("fetch: n=%d payload=%d err=%v", n, dst[0].Payload, err)
			}
			if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
				t.Fatal(err)
			}
			mustIdle(t, idle)
			mustFinish(t, "Engine.Close", func() { _ = e.Close() })
		})
	}
}

// TestWaitIdleDuringLeaseExpiry: a lease expires on a record that has outlived
// its time to live, so the scheduler drops it instead of requeueing it — under
// the lane mutex, with a WaitIdle waiter parked.
func TestWaitIdleDuringLeaseExpiry(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			e, top, sub, c := idleRig(t, a, TopologyConfig{
				Partitions: 1, AckDeadline: 40 * time.Millisecond,
			})
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: 1, Size: 1, TTL: 20 * time.Millisecond,
			}); err != nil {
				t.Fatal(err)
			}
			dst := make([]Claim[int64], 1)
			if n, err := c.Fetch(poll(t), dst); n != 1 || err != nil {
				t.Fatalf("fetch: n=%d err=%v", n, err)
			}
			// A second record keeps the engine busy while the first is in
			// flight, so the idle check has to walk the lanes.
			if err := top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 1}); err != nil {
				t.Fatal(err)
			}
			idle := parkWaiter(t, e, IdleDelivered)
			mustFinish(t, "the lease expiry", awaitDropped(sub, 1))
			if s := sub.Inspect(); s.InFlight != 0 || s.Requeued != 0 {
				t.Fatalf("the expired record came back: %+v", s)
			}
			n, err := c.Fetch(poll(t), dst)
			if err != nil || n != 1 || dst[0].Payload != 2 {
				t.Fatalf("fetch: n=%d payload=%d err=%v", n, dst[0].Payload, err)
			}
			if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
				t.Fatal(err)
			}
			mustIdle(t, idle)
			mustFinish(t, "Engine.Close", func() { _ = e.Close() })
		})
	}
}

// TestWaitIdleDuringEngineClose: Engine.Close drains the lanes with their
// mutexes held while a WaitIdle waiter is parked and records are in flight.
func TestWaitIdleDuringEngineClose(t *testing.T) {
	for _, a := range Arches() {
		t.Run(a.String(), func(t *testing.T) {
			e, top, sub, c := idleRig(t, a, TopologyConfig{Partitions: 1, AckDeadline: -1})
			for i := range 2 {
				if err := top.Publish(context.Background(), Publish[int64]{
					Payload: int64(i), Size: 1,
				}); err != nil {
					t.Fatal(err)
				}
			}
			dst := make([]Claim[int64], 1)
			if n, err := c.Fetch(poll(t), dst); n != 1 || err != nil {
				t.Fatalf("fetch: n=%d err=%v", n, err)
			}
			idle := parkWaiter(t, e, IdleDelivered)
			mustFinish(t, "Engine.Close", func() { _ = e.Close() })
			mustIdle(t, idle)
			if s := sub.Inspect(); s.Dropped != 2 || s.Ready != 0 || s.InFlight != 0 {
				t.Fatalf("close left records behind: %+v", s)
			}
		})
	}
}

// TestWaitIdleSettledIsWokenByEveryReturnedLease: IdleSettled is satisfied by
// an empty in-flight list whatever is still queued, so a lease given *back*
// makes the engine idle without a single record leaving it. Every route that
// returns one — a negative settlement, a lease that expires, a consumer that
// closes, a rebalance that revokes — must therefore notify the idle gate, or
// a parked waiter sleeps until its context runs out.
//
// The record stays in its lane throughout: it is delayed by an hour after a
// nack and simply not taken again after the other three, so nothing here is a
// settlement in lane.discard's sense and the wake-up cannot come from one.
func TestWaitIdleSettledIsWokenByEveryReturnedLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		give func(*testing.T, *Engine[int64], *clock.Fake, *Subscription[int64], *Consumer[int64], Claim[int64])
	}{
		{"nack", func(t *testing.T, _ *Engine[int64], _ *clock.Fake, _ *Subscription[int64], c *Consumer[int64], cl Claim[int64]) {
			if err := c.Nack(cl.Env, cl.Gen, time.Hour); err != nil {
				t.Fatalf("Nack: %v", err)
			}
		}},
		{"leaseexpiry", func(t *testing.T, e *Engine[int64], fake *clock.Fake, _ *Subscription[int64], _ *Consumer[int64], _ Claim[int64]) {
			fake.Advance(2 * time.Minute)
			// The scheduler may have reclaimed it already on the advance;
			// either route has to notify, so the count is not asserted.
			e.Faults().ExpireLeases()
		}},
		{"consumerclose", func(t *testing.T, _ *Engine[int64], _ *clock.Fake, _ *Subscription[int64], c *Consumer[int64], _ Claim[int64]) {
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
		}},
		{"rebalance", func(t *testing.T, _ *Engine[int64], _ *clock.Fake, s *Subscription[int64], _ *Consumer[int64], _ Claim[int64]) {
			// A second consumer joins, so the sticky table hands lane 1 to
			// it and the lease the first one holds there is revoked.
			if _, err := s.Attach(ConsumerConfig{}); err != nil {
				t.Fatalf("Attach: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, fake := fakeEngine(t, KindSet{}, TopologyConfig{
				Partitions: 2, CapacityMessages: 8, AckDeadline: time.Minute,
			})
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}
			s, err := top.Subscribe("s", SubscriptionConfig{Sticky: true})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			c, err := s.Attach(ConsumerConfig{})
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			// Lane 1: it is the one a second consumer takes over.
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: 1, Size: 1, Partition: 1,
			}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			dst := make([]Claim[int64], 1)
			n, err := c.Fetch(poll(t), dst)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Fetch: %v", err)
			}
			if n != 1 {
				t.Fatalf("Fetch returned %d claims, want 1", n)
			}
			if got := s.Inspect().InFlight; got != 1 {
				t.Fatalf("InFlight = %d before the lease is given back, want 1", got)
			}
			idle := parkWaiter(t, e, IdleSettled)
			tc.give(t, e, fake, s, c, dst[0])
			mustIdle(t, idle)
			if got := s.Inspect(); got.InFlight != 0 {
				t.Fatalf("%s left %d leases outstanding: %+v", tc.name, got.InFlight, got)
			}
		})
	}
}

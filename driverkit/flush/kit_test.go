package flush

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// The differential half of A [Policy] is a statement about what
// [driverkit.BatchSettler] does, so the two have to agree on every arrival.
//
// This matters more than any of the per-kind claims. The kit is the only
// implementation of the batching mechanism in the repository, and the policy
// is what the bake-off measures; if they drifted, the campaign would be
// deciding a family nothing implements. So the check is exact rather than a
// bound: after every submission, the number of round trips the kit has made
// must be the number of times the policy said "send", and the settlements the
// policy still holds must be the ones whose Ack has not returned.
//
// The arrivals are SERIALISED — one goroutine launched, then
// synctest.Wait — and that is what makes the comparison exact rather than a
// bound. driverkit.BatchSettler.submit appends to the queue and then flushes
// everything in it, so two settlements that arrive before either flush runs
// travel together even under the immediate policy: the cost of a policy under
// concurrency is at most what this test measures and can be less. What a
// policy predicts is the sequential case.
//
// It runs inside testing/synctest, which is what makes it exact. The bubble's
// clock is the kit's default wall clock, so a sleep fires the shared ticker
// deterministically, and synctest.Wait returns once every goroutine is
// durably blocked — which is how "this settlement is still being held" becomes
// an observation rather than a race against a real timer.

// countingSettler is the driver underneath the kit: it records the shape of
// every round trip and settles everything.
type countingSettler struct {
	mu      sync.Mutex
	batches []int
	singles int
}

var (
	_ mq.Settler      = (*countingSettler)(nil)
	_ mq.BatchSettler = (*countingSettler)(nil)
)

func (c *countingSettler) Ack(context.Context, *mq.DriverDelivery) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, 1)
	c.singles++
	return nil
}

func (c *countingSettler) Nack(context.Context, *mq.DriverDelivery, mq.NackOptions) error {
	return c.Ack(context.Background(), nil)
}

func (c *countingSettler) AckBatch(_ context.Context, ds []*mq.DriverDelivery) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, len(ds))
	return nil
}

func (c *countingSettler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, _ mq.NackOptions) error {
	return c.AckBatch(ctx, ds)
}

// trips is how many round trips the settler has been asked for.
func (c *countingSettler) trips() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batches)
}

// shape is the size of every round trip, in order.
func (c *countingSettler) shape() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.batches...)
}

// kitAgrees drives one kind's policy and one kind's kit settler over the same
// arrivals and checks that they never disagree.
func kitAgrees(t *testing.T, k Kind) {
	const maxItems = 4
	// The interval is longer than the whole submission phase, so nothing a
	// ticker does can be mistaken for something the count decided.
	const interval = time.Hour
	synctest.Test(t, func(t *testing.T) {
		sink := &countingSettler{}
		bs, err := driverkit.NewBatchSettler(sink, driverkit.BatchConfig{
			Batching: k.Batching(),
			MaxItems: maxItems,
			Interval: interval,
		})
		if err != nil {
			t.Fatalf("NewBatchSettler: %v", err)
		}
		// Close on the way out even when an assertion fails: a Fatalf inside
		// the bubble exits this goroutine, and a settlement still parked in
		// Ack would then make synctest panic with "blocked goroutines
		// remain" on top of the failure that caused it.
		defer func() { _ = bs.Close() }()
		p, err := New(k, Config{MaxItems: maxItems, Interval: interval})
		if err != nil {
			t.Fatalf("New(%v): %v", k, err)
		}

		// One more than two full batches, so that the ticker phase below
		// starts with a PARTIAL queue. With a whole number of batches the
		// queue is empty by then, every kind's tick decides nothing, and
		// the check cannot tell a hybrid bound to the kit's hybrid strategy
		// from one bound to its count strategy — which is the mutation this
		// test failed to catch the first time it was written.
		const arrivals = maxItems*2 + 1
		ds := make([]mq.DriverDelivery, arrivals)
		done := make([]chan error, arrivals)
		pending, trips := 0, 0
		for i := range arrivals {
			done[i] = make(chan error, 1)
			go func(i int) { done[i] <- bs.Ack(t.Context(), &ds[i]) }(i)
			synctest.Wait()
			pending++
			if p.Submitted(pending) {
				trips += minTrips(pending, maxItems)
				pending = 0
			}
			if got := sink.trips(); got != trips {
				t.Fatalf("after %d arrival(s) the kit had made %d round trip(s) and the policy predicted %d (shape %v); the policy and the mechanism have drifted",
					i+1, got, trips, sink.shape())
			}
			// Everything the policy says has been sent has returned, and
			// everything it says is held has not.
			for j := range i + 1 {
				held := j >= i+1-pending
				if held == resolved(done[j]) {
					t.Fatalf("after %d arrival(s), settlement %d is %s at the kit and the policy says it is %s",
						i+1, j, state(resolved(done[j])), state(!held))
				}
			}
		}

		// The ticker. A policy with no ticker must send nothing however long
		// the driver waits, and one with a ticker must send exactly what it
		// says it sends.
		before := sink.trips()
		time.Sleep(2 * interval)
		synctest.Wait()
		after := sink.trips()
		want := before
		if _, ticks := p.Ticker(); ticks && p.Ticked(pending) {
			want += minTrips(pending, maxItems)
		}
		if after != want {
			t.Fatalf("two ticker periods later the kit had made %d round trip(s), want %d (policy ticks=%v, holding %d): a policy that predicts the wrong ticker behaviour is one the bake-off cannot decide",
				after, want, hasTicker(p), pending)
		}

		// Close drains whatever is left, which is the shutdown-correctness
		// term: no settlement may be lost, whatever the policy was holding.
		if err := bs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		synctest.Wait()
		for i := range arrivals {
			if err := <-done[i]; err != nil {
				t.Fatalf("settlement %d = %v; every settlement queued before a Close is sent by it", i, err)
			}
		}
		for i, n := range sink.shape() {
			if n > maxItems {
				t.Fatalf("round trip %d carried %d entries, over the configured limit of %d", i, n, maxItems)
			}
		}
	})
}

// resolved reports whether a settlement's Ack has returned.
func resolved(ch chan error) bool {
	select {
	case err := <-ch:
		ch <- err
		return true
	default:
		return false
	}
}

// state names a settlement's state for a failure message.
func state(done bool) string {
	if done {
		return "settled"
	}
	return "held"
}

// hasTicker reports whether a policy asked for the shared ticker.
func hasTicker(p Policy) bool {
	_, ok := p.Ticker()
	return ok
}

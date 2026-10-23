package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// This file is RunBatch: the batch-shaped dispatch over the same
// admission slots, the same lanes and the same failure policy as Run.

// batchCollector records every batch a runner handed over.
type batchCollector struct {
	mu      sync.Mutex
	batches [][]string
	keys    []map[string]bool
	fn      func(n int, ds []*mq.Delivery) error
	seen    atomic.Int64
}

// HandleBatch implements mq.BatchHandler.
func (b *batchCollector) HandleBatch(_ context.Context, ds []*mq.Delivery) error {
	n := int(b.seen.Add(1))
	ids := make([]string, len(ds))
	keys := map[string]bool{}
	for i, d := range ds {
		ids[i] = d.ID
		keys[string(d.Key)] = true
	}
	b.mu.Lock()
	b.batches = append(b.batches, ids)
	b.keys = append(b.keys, keys)
	b.mu.Unlock()
	if b.fn != nil {
		return b.fn(n, ds)
	}
	return nil
}

// handled is every message identifier the handler was given, in order.
func (b *batchCollector) handled() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, batch := range b.batches {
		out = append(out, batch...)
	}
	return out
}

// keysPerBatch is the set of ordering keys each batch held.
func (b *batchCollector) keysPerBatch() []map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]map[string]bool, len(b.keys))
	copy(out, b.keys)
	return out
}

// shapes is the size of every batch, largest first.
func (b *batchCollector) shapes() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]int, len(b.batches))
	for i, batch := range b.batches {
		out[i] = len(batch)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(out)))
	return out
}

// startRunBatch starts a batch runner on its own goroutine.
func startRunBatch(tb testing.TB, c *mq.ConsumerHandle, h mq.BatchHandler, opts ...mq.RunOption) *runHandle {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runHandle{tb: tb, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- c.RunBatch(ctx, h, opts...) }()
	tb.Cleanup(func() { _ = r.stop() })
	return r
}

// TestRunBatchAcksEveryDeliveryOfAnAcceptedBatch is the whole point: a batch
// handler that returns nil has every delivery of the batch acked.
func TestRunBatchAcksEveryDeliveryOfAnAcceptedBatch(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 16)
	h := &batchCollector{}
	run := startRunBatch(t, c, h)
	waitFor(t, "every message", func() bool { return len(h.handled()) == 16 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 16 {
		t.Fatalf("acked = %d, want 16", counts.acked)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
}

// TestRunBatchBatchesAWholeFetchWhenNothingIsOrdered is what makes it a batch
// runner rather than a loop: with OrderingUnordered every delivery routes the
// same way, so one fetch is one batch.
func TestRunBatchBatchesAWholeFetchWhenNothingIsOrdered(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 64)
	h := &batchCollector{}
	run := startRunBatch(t, c, h, mq.WithOrdering(mq.OrderingUnordered))
	waitFor(t, "every message", func() bool { return len(h.handled()) == 64 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	if largest := h.shapes()[0]; largest < 2 {
		t.Fatalf("the largest batch held %d deliveries: an unordered batch runner batches a fetch", largest)
	}
}

// TestRunBatchNeverCrossesALane is surviving batching: a batch holds the
// deliveries of ONE lane route, so under the default ordering every batch
// holds one key and the deliveries of that key are still handled in order and
// one at a time.
//
// Two keys that hash to one lane are still two routes and therefore two
// batches, which is what "one distinct key per batch" checks: a runner that
// batched by lane rather than by route would put both in one.
func TestRunBatchNeverCrossesALane(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 32)
	h := &batchCollector{}
	run := startRunBatch(t, c, h, mq.WithConcurrency(4))
	waitFor(t, "every message", func() bool { return len(h.handled()) == 32 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	for _, batch := range h.keysPerBatch() {
		if len(batch) != 1 {
			t.Fatalf("a batch held the keys %v: the specification keeps one key on one lane, and a batch is one lane's", batch)
		}
	}
}

// TestRunBatchKeepsOneKeyInOrder is the ordering half over a single key: one
// lane, one batch at a time, arrival order preserved.
func TestRunBatchKeepsOneKeyInOrder(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 8)
	h := &batchCollector{}
	run := startRunBatch(t, c, h, mq.WithConcurrency(4))
	waitFor(t, "every message", func() bool { return len(h.handled()) == 8 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	want := []string{"acct-0", "acct-1", "acct-2", "acct-3", "acct-4", "acct-5", "acct-6", "acct-7"}
	if got := h.handled(); !equalIDs(got, want) {
		t.Fatalf("handled %v, want %v in order", got, want)
	}
}

// TestRunBatchSettlesEachDeliveryByItsOwnEntry is the *BatchError: the
// entries are index-aligned with the batch, and an entry decides its own
// delivery and nobody else's. The specification disqualifies an implementation that
// reports one message's outcome for another.
//
// One entry of the first batch fails, and the check is WHICH message comes
// back: a runner that read the entries in any other order would requeue the
// same number of deliveries and the wrong ones, which a count cannot tell
// apart.
func TestRunBatchSettlesEachDeliveryByItsOwnEntry(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 4)
	var mu sync.Mutex
	var failedID string
	var redelivered []string
	h := &batchCollector{fn: func(n int, ds []*mq.Delivery) error {
		if n > 1 {
			mu.Lock()
			for _, d := range ds {
				redelivered = append(redelivered, d.ID)
			}
			mu.Unlock()
			return nil
		}
		if len(ds) != 4 {
			return fmt.Errorf("the first batch held %d deliveries, want 4", len(ds))
		}
		mu.Lock()
		failedID = ds[1].ID
		mu.Unlock()
		errs := make([]error, len(ds))
		errs[1] = errors.New("this one failed")
		return &mq.BatchError{Errs: errs}
	}}
	run := startRunBatch(t, c, h,
		mq.WithOrdering(mq.OrderingUnordered),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "every message to be acked, the failure after its retry", func() bool {
		return b.counts("orders", "billing").acked == 4
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	mu.Lock()
	got, want := append([]string(nil), redelivered...), failedID
	mu.Unlock()
	if len(got) != 1 || got[0] != want {
		t.Fatalf("the redelivered messages are %v, want exactly [%s]: each entry settles its own delivery", got, want)
	}
	counts := b.counts("orders", "billing")
	if counts.requeued != 1 {
		t.Fatalf("requeued = %d, want exactly 1", counts.requeued)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
}

// TestRunBatchShortBatchErrorKeepsEntryDirectivesToTheirOwnDelivery is a
// *BatchError shorter than its batch whose one entry discards: that entry
// speaks for its own delivery only, so the deliveries it does not cover fail
// under the ordinary retry policy rather than being discarded with it.
func TestRunBatchShortBatchErrorKeepsEntryDirectivesToTheirOwnDelivery(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 4)
	var mu sync.Mutex
	var discardedID string
	var redelivered []string
	h := &batchCollector{fn: func(n int, ds []*mq.Delivery) error {
		if n > 1 {
			mu.Lock()
			for _, d := range ds {
				redelivered = append(redelivered, d.ID)
			}
			mu.Unlock()
			return nil
		}
		if len(ds) != 4 {
			return fmt.Errorf("the first batch held %d deliveries, want 4", len(ds))
		}
		mu.Lock()
		discardedID = ds[0].ID
		mu.Unlock()
		return &mq.BatchError{Errs: []error{mq.Discard(errors.New("poison"))}}
	}}
	run := startRunBatch(t, c, h,
		mq.WithOrdering(mq.OrderingUnordered),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "every message to be acked", func() bool {
		return b.counts("orders", "billing").acked == 4
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	mu.Lock()
	got := append([]string(nil), redelivered...)
	mu.Unlock()
	if len(got) != 3 {
		t.Fatalf("redelivered %v, want the 3 deliveries the entry did not cover", got)
	}
	for _, id := range got {
		if id == discardedID {
			t.Fatalf("the discarded delivery %s came back", id)
		}
	}
	counts := b.counts("orders", "billing")
	if counts.requeued != 3 {
		t.Fatalf("requeued = %d, want 3", counts.requeued)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
}

// TestRunBatchNacksTheWholeBatchOnAPlainError is the third case: a result
// that is not nil and not a *BatchError settles every delivery of the batch
// the same way.
func TestRunBatchNacksTheWholeBatchOnAPlainError(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 4)
	h := &batchCollector{fn: func(n int, _ []*mq.Delivery) error {
		if n == 1 {
			return errors.New("the whole batch failed")
		}
		return nil
	}}
	run := startRunBatch(t, c, h,
		mq.WithOrdering(mq.OrderingUnordered),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "every delivery of the first batch to be given back", func() bool {
		return b.counts("orders", "billing").requeued >= 4
	})
	waitFor(t, "the retry to be acked", func() bool {
		return b.counts("orders", "billing").acked == 4
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
}

// TestRunBatchRecoversAPanic is the over a batch: the panic
// becomes a *PanicError and the whole batch takes the failure, because a
// panic halfway through a batch says nothing about which deliveries were
// processed.
func TestRunBatchRecoversAPanic(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 4)
	h := &batchCollector{fn: func(n int, _ []*mq.Delivery) error {
		if n == 1 {
			panic("batch boom")
		}
		return nil
	}}
	run := startRunBatch(t, c, h,
		mq.WithOrdering(mq.OrderingUnordered),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the batch to be retried after the panic", func() bool {
		return b.counts("orders", "billing").acked == 4
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled (a recovered panic keeps the runner running)", err)
	}
}

// TestRunBatchRefusesWhatItCannotHonour pins the two options RunBatch refuses
// rather than quietly meaning something else.
func TestRunBatchRefusesWhatItCannotHonour(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	h := &batchCollector{}
	cases := []struct {
		name string
		opts []mq.RunOption
	}{
		{"middleware", []mq.RunOption{mq.WithMiddleware(func(next mq.Handler) mq.Handler { return next })}},
		{"strict ordering", []mq.RunOption{mq.WithOrderingByKey()}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.RunBatch(context.Background(), h, tc.opts...)
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("RunBatch = %v (code %v), want CodeInvalidConfig", err, mq.CodeOf(err))
			}
		})
	}
	if err := c.RunBatch(context.Background(), nil); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("RunBatch(nil) = %v, want CodeInvalidConfig", err)
	}
}

// TestRunBatchAndRunShareTheDoubleOwnerRefusal is over both
// shapes: one runner per consumer, whichever shape it is.
func TestRunBatchAndRunShareTheDoubleOwnerRefusal(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	first := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	waitFor(t, "the first runner to be fetching", func() bool {
		return c.Stats().Received == 0 && len(first.done) == 0
	})
	time.Sleep(20 * time.Millisecond)
	err := c.RunBatch(context.Background(), &batchCollector{})
	if mq.CodeOf(err) != mq.CodeConflict {
		t.Fatalf("RunBatch beside a Run = %v (code %v), want CodeConflict", err, mq.CodeOf(err))
	}
	_ = first.stop()
}

// TestRunBatchAgainstTheMemoryDriver is the batch runner over a real driver,
// beside the fake: everything published is handled once and acked.
func TestRunBatchAgainstTheMemoryDriver(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing")
	publishToMemory(t, cl, "orders", 32)
	h := &batchCollector{}
	run := startRunBatch(t, c, h, mq.WithConcurrency(4), mq.WithOrdering(mq.OrderingUnordered))
	waitFor(t, "every message", func() bool { return len(h.handled()) == 32 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	s := inspect(t, b, "orders", "billing")
	if s.Acked != 32 || s.Ready != 0 || s.InFlight != 0 {
		t.Fatalf("snapshot = %+v, want 32 acked and nothing left", s)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

// TestRunBatchHaltGivesBackTheBatchesItHasNotLookedAt is for
// the batch shape: a halted runner does not start handling another batch, and
// every delivery of the batch it declines goes back to the broker.
//
// A prefetch of one and an in-flight bound of eight is what makes a SECOND
// batch reachable at all: with the default prefetch one fetch is one group,
// and with an in-flight bound of one the runner cannot hold the group that
// halted and another at the same time — either way the halt would have
// nothing queued behind it.
func TestRunBatchHaltGivesBackTheBatchesItHasNotLookedAt(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{Prefetch: 1})
	publishDistinctKeys(t, cl, 8)
	boom := errors.New("stop here")
	h := &batchCollector{fn: func(int, []*mq.Delivery) error {
		time.Sleep(50 * time.Millisecond)
		return mq.Halt(boom)
	}}
	run := startRunBatch(t, c, h, mq.WithConcurrency(1), mq.WithMaxInFlight(8))
	if err := run.wait(); !errors.Is(err, boom) {
		t.Fatalf("RunBatch = %v, want the handler's own error", err)
	}
	if got := len(h.handled()); got != 1 {
		t.Fatalf("the handler was given %d deliveries, want 1: a halted runner hands back what it has not looked at", got)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 0 {
		t.Fatalf("acked = %d, want 0", counts.acked)
	}
	if counts.ready+counts.inflight != 8 {
		t.Fatalf("counts = %+v, want all eight messages still redeliverable", counts)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
}

// TestRunBatchRecyclesItsAdmissionSlots is the batch runner's half of the in-flight bound: the slots a group held go back to the
// free list when the group has been settled, so a runner bounded at four
// in-flight deliveries still handles sixty-four messages.
//
// Without it the runner handles exactly MaxInFlight deliveries and then stops
// for ever, which every test with fewer messages than the default bound
// passes through without noticing.
func TestRunBatchRecyclesItsAdmissionSlots(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 64)
	h := &batchCollector{}
	run := startRunBatch(t, c, h,
		mq.WithMaxInFlight(4),
		mq.WithOrdering(mq.OrderingUnordered))
	waitFor(t, "every message past a bound of four in flight", func() bool { return len(h.handled()) == 64 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 64 || !counts.conserved() {
		t.Fatalf("counts = %+v, want 64 acked and conserved", counts)
	}
	if largest := h.shapes()[0]; largest > 4 {
		t.Fatalf("a batch held %d deliveries with MaxInFlight(4): the bound is exact, not a target", largest)
	}
}

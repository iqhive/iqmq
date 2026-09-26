package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// publishKeyed publishes n messages whose ID is their index and whose key is
// key, so that every one of them routes to the same lane under OrderingByKey.
func publishKeyed(tb testing.TB, cl *mq.Client, key string, n int) {
	tb.Helper()
	ctx := context.Background()
	for i := range n {
		m := &mq.Message{ID: key + "-" + strconv.Itoa(i), Key: []byte(key), Body: []byte("body")}
		if err := cl.Publish(ctx, "orders", m); err != nil {
			tb.Fatalf("publishing %s-%d: %v", key, i, err)
		}
	}
}

// publishDistinctKeys publishes n messages with n different keys.
func publishDistinctKeys(tb testing.TB, cl *mq.Client, n int) {
	tb.Helper()
	ctx := context.Background()
	for i := range n {
		id := strconv.Itoa(i)
		m := &mq.Message{ID: id, Key: []byte("k" + id), Body: []byte("body")}
		if err := cl.Publish(ctx, "orders", m); err != nil {
			tb.Fatalf("publishing %d: %v", i, err)
		}
	}
}

// concurrency watches how many handlers run at once and remembers the most
// that ever did.
type concurrency struct {
	live atomic.Int64
	most atomic.Int64
}

// enter records one more handler in flight.
func (c *concurrency) enter() {
	n := c.live.Add(1)
	for {
		most := c.most.Load()
		if n <= most || c.most.CompareAndSwap(most, n) {
			return
		}
	}
}

// leave records one fewer.
func (c *concurrency) leave() { c.live.Add(-1) }

// TestRunByKeyKeepsOneKeyOnOneLane is the per-key order: with the default
// ordering, every delivery of one key is handled by one worker at a time,
// whatever the concurrency.
func TestRunByKeyKeepsOneKeyOnOneLane(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct-1", 32)
	var seen concurrency
	h := &collector{fn: func(int, *mq.Delivery) error {
		seen.enter()
		defer seen.leave()
		time.Sleep(time.Millisecond)
		return nil
	}}
	run := startRun(t, c, h, mq.WithConcurrency(8))
	waitFor(t, "every delivery of the key", func() bool { return h.count() == 32 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := seen.most.Load(); got != 1 {
		t.Fatalf("%d handlers of one key ran at once, want 1 (one key, one lane)", got)
	}
}

// TestRunUnorderedHandlesOneKeyConcurrently is the other end:
// OrderingUnordered routes as if nothing had a key, so the deliveries of one
// key may be handled at the same time on different workers.
//
// The check is a barrier rather than a maximum: it needs two handlers of one
// key to be inside the handler together, which the by-key routing cannot
// produce at all.
func TestRunUnorderedHandlesOneKeyConcurrently(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct-1", 8)
	together := make(chan struct{})
	var once sync.Once
	arrived := make(chan struct{}, 8)
	h := &collector{fn: func(int, *mq.Delivery) error {
		arrived <- struct{}{}
		if len(arrived) >= 2 {
			once.Do(func() { close(together) })
		}
		select {
		case <-together:
		case <-time.After(2 * time.Second):
		}
		<-arrived
		return nil
	}}
	run := startRun(t, c, h, mq.WithConcurrency(4), mq.WithOrdering(mq.OrderingUnordered))
	select {
	case <-together:
	case <-time.After(runWait):
		t.Fatal("no two deliveries of one key were ever handled at the same time: the ordering is still by key")
	}
	waitFor(t, "every delivery", func() bool { return h.count() == 8 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

// TestRunByPartitionKeepsOnePartitionOnOneLane is the partition scope: the
// deliveries of one partition are handled one at a time even when their keys
// are all different, which is ADR-011's "one serial worker per assigned
// partition".
func TestRunByPartitionKeepsOnePartitionOnOneLane(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 32)
	var seen concurrency
	h := &collector{fn: func(int, *mq.Delivery) error {
		seen.enter()
		defer seen.leave()
		time.Sleep(time.Millisecond)
		return nil
	}}
	run := startRun(t, c, h, mq.WithConcurrency(8), mq.WithOrdering(mq.OrderingByPartition))
	waitFor(t, "every delivery", func() bool { return h.count() == 32 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := seen.most.Load(); got != 1 {
		t.Fatalf("%d handlers of one partition ran at once, want 1 (one partition, one lane)", got)
	}
}

// TestRunByKeyRunsDistinctKeysConcurrently is what makes the two tests above
// findings rather than accidents: the same thirty-two distinct keys, the same
// eight workers, and the default ordering does run them at the same time. A
// runner that serialised everything would pass both of the tests above and
// fail this one.
func TestRunByKeyRunsDistinctKeysConcurrently(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 32)
	var seen concurrency
	reached := make(chan struct{})
	var once sync.Once
	h := &collector{fn: func(int, *mq.Delivery) error {
		seen.enter()
		defer seen.leave()
		if seen.most.Load() > 1 {
			once.Do(func() { close(reached) })
		}
		time.Sleep(2 * time.Millisecond)
		return nil
	}}
	run := startRun(t, c, h, mq.WithConcurrency(8))
	select {
	case <-reached:
	case <-time.After(runWait):
		t.Fatal("no two deliveries were ever handled at the same time with eight workers and thirty-two keys")
	}
	_ = run.stop()
}

// --- the retry interaction -------------------------------------------------

// TestRunStrictOrderingRetriesInPlaceAndHoldsTheLane is StrictOrdering:
// a failed delivery blocks its lane until it is settled or terminally routed.
//
// The runner holds the delivery on the lane's own worker, waits out the
// backoff and calls the handler again, so the retry is handled BEFORE the
// message that was behind it and the broker is never told anything.
func TestRunStrictOrderingRetriesInPlaceAndHoldsTheLane(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	publishKeyed(t, cl, "other", 1)
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			return errors.New("try again")
		}
		return nil
	}}
	run := startRun(t, c, h,
		mq.WithConcurrency(1),
		mq.WithOrderingByKey(),
		mq.WithRetryBackoff(mq.ConstantBackoff(5*time.Millisecond)))
	waitFor(t, "the retry and both messages", func() bool { return h.count() == 3 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got, want := h.seenIDs(), []string{"acct-0", "acct-0", "other-0"}; !equalIDs(got, want) {
		t.Fatalf("handled %v, want %v (a failed item blocks its lane)", got, want)
	}
	counts := b.counts("orders", "billing")
	if counts.requeued != 0 {
		t.Fatalf("the broker was told about %d requeues, want 0: a strict retry never leaves the process", counts.requeued)
	}
	if counts.acked != 2 {
		t.Fatalf("acked = %d, want 2", counts.acked)
	}
}

func TestRunRelaxedOrderingLetsTheMessageBehindOvertake(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	publishKeyed(t, cl, "other", 1)
	h := &collector{fn: func(_ int, d *mq.Delivery) error {
		if d.ID == "acct-0" && d.Attempt == 1 {
			return errors.New("try again")
		}
		return nil
	}}
	run := startRun(t, c, h,
		mq.WithConcurrency(1),
		mq.WithOrderingByKey(mq.OrderingRelaxed),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the retry and both messages", func() bool { return h.count() == 3 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got, want := h.seenIDs(), []string{"acct-0", "other-0", "acct-0"}; !equalIDs(got, want) {
		t.Fatalf("handled %v, want %v (later items may proceed)", got, want)
	}
	if counts := b.counts("orders", "billing"); counts.requeued != 1 {
		t.Fatalf("requeued = %d, want 1: a relaxed retry gives the message back", counts.requeued)
	}
}

// TestRunStopOnFailureStopsTheConsumer is the third interaction: the
// runner stops on the first failure so that the broker's own redelivery
// decides what happens next. The message goes back first, so stopping loses
// nothing.
func TestRunStopOnFailureStopsTheConsumer(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	boom := errors.New("no")
	h := &collector{fn: func(int, *mq.Delivery) error { return boom }}
	run := startRun(t, c, h, mq.WithOrderingByKey(mq.OrderingStopOnFailure))
	err := run.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) || !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want a *PoisonError carrying the handler's error", err)
	}
	if mq.CodeOf(err) != mq.CodePoisonMessage {
		t.Fatalf("CodeOf(Run) = %v, want CodePoisonMessage", mq.CodeOf(err))
	}
	if h.count() != 1 {
		t.Fatalf("the handler ran %d times, want 1: StopOnFailure stops at the first failure", h.count())
	}
	counts := b.counts("orders", "billing")
	if counts.requeued != 1 || counts.acked != 0 {
		t.Fatalf("counts = %+v, want the message given back and nothing acked", counts)
	}
}

// TestRunStrictOrderingBoundsItsOwnAttempts closes half a
// broker that counts no attempts cannot bound a retry-by-nack runner, but a
// delivery the runner HELD never left the process, so the runner's own count
// is exact and WithMaxAttempts binds it.
//
// The fake counts attempts, so the test uses a source that reports none.
func TestRunStrictOrderingBoundsItsOwnAttempts(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{countless: true}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	boom := errors.New("no")
	h := &collector{fn: func(int, *mq.Delivery) error { return boom }}
	run := startRun(t, c, h,
		mq.WithOrderingByKey(),
		mq.WithMaxAttempts(3),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	err := run.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) {
		t.Fatalf("Run = %v, want a *PoisonError once the attempts ran out", err)
	}
	if h.count() != 3 {
		t.Fatalf("the handler ran %d times, want 3 (WithMaxAttempts(3))", h.count())
	}
}

// TestRunRelaxedOrderingCannotBoundACountlessBrokersAttempts is the other
// half and is what keeps the test above honest: the same
// broker, the same bound, and a runner that retries by nack cannot enforce
// it, because the only count there is belongs to the broker.
func TestRunRelaxedOrderingCannotBoundACountlessBrokersAttempts(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{countless: true}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n < 6 {
			return errors.New("no")
		}
		return nil
	}}
	run := startRun(t, c, h, mq.WithMaxAttempts(3), mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the sixth attempt, past a bound the broker cannot keep", func() bool { return h.count() >= 6 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

// --- validation ------------------------------------------------------------

// TestRunOrderingOptionsAreValidated pins the refusals. Each is
// CodeInvalidConfig, which is the code the specification gives a configuration a caller
// has to fix.
func TestRunOrderingOptionsAreValidated(t *testing.T) {
	cases := []struct {
		name string
		opt  mq.RunOption
	}{
		{"ordering out of range", mq.WithOrdering(mq.Ordering(9))},
		{"mode out of range", mq.WithOrderingByKey(mq.OrderingMode(9))},
		{"two modes", mq.WithOrderingByKey(mq.OrderingStrict, mq.OrderingRelaxed)},
		{"handler timeout of zero", mq.WithHandlerTimeout(0)},
		{"negative handler timeout", mq.WithHandlerTimeout(-time.Second)},
		{"lease lead of zero", mq.WithLeaseExtension(0)},
		{"drain timeout of zero", mq.WithDrainTimeout(0)},
		{"dead-letter confirmation with no topic", mq.WithDeadLetterConfirmation(mq.ConfirmDurable)},
		{"two dead-letter destinations", mq.WithNativeDeadLetter()},
	}
	cl, _ := openFakeWith(t, fakeConfig{deadLetter: true}, "orders", "dead")
	c := consume(t, cl, mq.ConsumerConfig{})
	h := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	// The context is already done, so a configuration the runner ACCEPTS
	// returns promptly with the context's error instead of running: the
	// options are resolved before the context is ever consulted, so the
	// refusals below are still the refusals and a test that stops checking
	// them fails in a second rather than in five minutes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := []mq.RunOption{tc.opt}
			if tc.name == "two dead-letter destinations" {
				opts = append(opts, mq.WithDeadLetter("dead"))
			}
			err := c.Run(ctx, h, opts...)
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("Run = %v (code %v), want CodeInvalidConfig", err, mq.CodeOf(err))
			}
		})
	}
}

// TestOrderingNamesAndValidity pins the two enums' text and range, which go
// into metric labels and error messages.
func TestOrderingNamesAndValidity(t *testing.T) {
	orderings := map[mq.Ordering]string{
		mq.OrderingByKey:       "bykey",
		mq.OrderingUnordered:   "unordered",
		mq.OrderingByPartition: "bypartition",
	}
	for o, want := range orderings {
		if got := o.String(); got != want {
			t.Fatalf("Ordering(%d).String() = %q, want %q", o, got, want)
		}
		if !o.Valid() {
			t.Fatalf("Ordering(%d).Valid() = false", o)
		}
	}
	if got := mq.Ordering(9).String(); got != "Ordering(?)" {
		t.Fatalf("Ordering(9).String() = %q", got)
	}
	if mq.Ordering(9).Valid() {
		t.Fatal("Ordering(9).Valid() = true")
	}
	modes := map[mq.OrderingMode]string{
		mq.OrderingStrict:        "strict",
		mq.OrderingRelaxed:       "relaxed",
		mq.OrderingStopOnFailure: "stoponfailure",
	}
	for m, want := range modes {
		if got := m.String(); got != want {
			t.Fatalf("OrderingMode(%d).String() = %q, want %q", m, got, want)
		}
		if !m.Valid() {
			t.Fatalf("OrderingMode(%d).Valid() = false", m)
		}
	}
	if got := mq.OrderingMode(9).String(); got != "OrderingMode(?)" {
		t.Fatalf("OrderingMode(9).String() = %q", got)
	}
	if mq.OrderingMode(9).Valid() {
		t.Fatal("OrderingMode(9).Valid() = true")
	}
}

// equalIDs compares two handling orders.
func equalIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestRunHaltGivesBackTheDeliveriesItHasNotLookedAt is the second half a halted runner does not start handling another delivery.
//
// Eight deliveries of one key go to one lane behind a handler that halts on
// the first of them. The seven behind it are already queued when the halt
// happens, and a runner that drained them into the handler would run seven
// more attempts on its way out — which is what it used to do, and what made a
// StopOnFailure runner take a whole second pass over its lane.
func TestRunHaltGivesBackTheDeliveriesItHasNotLookedAt(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 8)
	boom := errors.New("stop here")
	h := &collector{fn: func(int, *mq.Delivery) error {
		// Long enough for the fetch loop to have queued the rest of the
		// batch on this lane before the halt happens.
		time.Sleep(50 * time.Millisecond)
		return mq.Halt(boom)
	}}
	run := startRun(t, c, h, mq.WithConcurrency(1))
	if err := run.wait(); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the handler's own error", err)
	}
	if got := h.count(); got != 1 {
		t.Fatalf("the handler ran %d times, want 1: a halted runner hands back what it has not looked at", got)
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

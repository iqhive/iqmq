package iqmq_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// runWait is how long a test waits for the runner to do something before it
// calls the runner stuck. It is generous because the fake driver parks a
// fetch until a message arrives, and a stuck runner fails the test either
// way — the number only decides how long a broken build takes to say so.
const runWait = 10 * time.Second

// --- the harness ----------------------------------------------------------

// openFakeWith builds a client over a fake broker holding the named topics.
// openFake in spi_test.go creates one topic; a dead-letter test needs two,
// and a publication to a topic the fake does not hold is ErrTopicNotFound.
func openFakeWith(tb testing.TB, cfg fakeConfig, topics ...string) (*mq.Client, *fakeBroker) {
	tb.Helper()
	b := newFakeBroker(cfg, topics...)
	cl := mq.NewClient(b)
	tb.Cleanup(func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	})
	return cl, b
}

// runHandle is one Run in flight: the context that stops it and the error it
// returned.
type runHandle struct {
	tb     testing.TB
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

// startRun starts a runner on its own goroutine. The test's cleanup stops it,
// so a test that forgets does not hang the package.
func startRun(tb testing.TB, c *mq.ConsumerHandle, h mq.Handler, opts ...mq.RunOption) *runHandle {
	tb.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runHandle{tb: tb, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- c.Run(ctx, h, opts...) }()
	tb.Cleanup(func() { _ = r.stop() })
	return r
}

// wait returns what Run returned, failing the test if it never returns.
func (r *runHandle) wait() error {
	r.once.Do(func() {
		select {
		case r.err = <-r.done:
		case <-time.After(runWait):
			r.tb.Error("Run did not return")
		}
	})
	return r.err
}

// returned reports whether Run has already returned, without waiting for it.
func (r *runHandle) returned() bool {
	select {
	case err := <-r.done:
		r.done <- err
		return true
	default:
		return false
	}
}

// stop cancels the runner's context and returns what Run returned.
func (r *runHandle) stop() error {
	r.cancel()
	return r.wait()
}

// waitFor polls cond until it holds or the test gives up.
func waitFor(tb testing.TB, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(runWait)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// collector is a handler that counts calls and lets a test wait for the nth.
type collector struct {
	mu   sync.Mutex
	ids  []string
	fn   func(n int, d *mq.Delivery) error
	seen atomic.Int64
}

// Handle implements mq.Handler.
func (c *collector) Handle(_ context.Context, d *mq.Delivery) error {
	n := int(c.seen.Add(1))
	c.mu.Lock()
	c.ids = append(c.ids, d.ID)
	c.mu.Unlock()
	if c.fn != nil {
		return c.fn(n, d)
	}
	return nil
}

// count is how many deliveries the handler has been given.
func (c *collector) count() int { return int(c.seen.Load()) }

// seenIDs is a copy of the message identifiers, in handling order.
func (c *collector) seenIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.ids))
	copy(out, c.ids)
	return out
}

// --- the loop -------------------------------------------------------------

// TestRunAcksEveryDeliveryTheHandlerAccepted is the runner's whole point: a
// handler that returns nil has its delivery acked, and the runner keeps
// fetching until its context is done.
func TestRunAcksEveryDeliveryTheHandlerAccepted(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 3)
	h := &collector{}
	run := startRun(t, c, h)
	waitFor(t, "three deliveries", func() bool { return h.count() == 3 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled (Run returns ctx.Err() after drain)", err)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 3 {
		t.Fatalf("acked = %d, want 3", counts.acked)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
	if got := h.seenIDs(); len(got) != 3 {
		t.Fatalf("handled %v, want three messages", got)
	}
}

// TestRunReturnsClosedWhenTheConsumerCloses pins the other way a runner ends:
// the consumer went away under it. It must return rather than spin on a
// source that answers ErrClosed for ever.
func TestRunReturnsClosedWhenTheConsumerCloses(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{}
	run := startRun(t, c, h)
	waitFor(t, "the first delivery", func() bool { return h.count() == 1 })
	if err := c.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	if err := run.wait(); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Run = %v, want ErrClosed", err)
	}
}

// TestRunRefusesASecondRunner is the double-owner rejection. Two runners on one handle would interleave their fetches and share
// one in-flight bound, so the API refuses instead of letting a user discover
// it as a fairness bug.
func TestRunRefusesASecondRunner(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	first := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	// The first runner has to be inside Run before the second is refused for
	// the right reason, and the only thing it does with no messages is park
	// in the fetch. Publishing one and waiting for it is what proves it got
	// that far.
	publishN(t, cl, 1)
	waitFor(t, "the first runner to fetch", func() bool {
		return c.Stats().Acked == 1
	})
	// The second Run is given a bounded context on purpose: a runner that
	// was NOT refused would park in the fetch and the test would hang
	// instead of failing, and a test that hangs when the guard is removed
	// is a test nobody can watch fail.
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer secondCancel()
	err := c.Run(secondCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	if err == nil {
		t.Fatal("a second Run on one consumer was allowed; the specification has one runner per consumer")
	}
	if got := mq.CodeOf(err); got != mq.CodeConflict {
		t.Fatalf("CodeOf(second Run) = %v, want CodeConflict: %v", got, err)
	}
	if serr := first.stop(); !errors.Is(serr, context.Canceled) {
		t.Fatalf("first Run = %v, want context.Canceled", serr)
	}
	// The refusal is for the life of a Run and not of the consumer: a caller
	// restarting after a halt must be able to.
	second := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	publishN(t, cl, 1)
	waitFor(t, "the restarted runner", func() bool { return c.Stats().Acked == 2 })
	if serr := second.stop(); !errors.Is(serr, context.Canceled) {
		t.Fatalf("restarted Run = %v, want context.Canceled", serr)
	}
}

// --- panics ---------------------------------------------------------------

// TestRunTurnsAHandlerPanicIntoAPanicError pins ADR-011's the panic
// is recovered and becomes a *PanicError, which then takes the configured
// failure policy exactly as a returned error would. "No panic may silently
// ack" is the half that matters — a runner that recovered and then acked
// would destroy the message.
//
// The conversion is observed through the halt, because the recovery wraps the
// whole middleware chain: a middleware cannot see it (Recover sits at
// the head of the chain and sees what is INSIDE it), and the failure policy
// is where the converted error surfaces to a caller.
func TestRunTurnsAHandlerPanicIntoAPanicError(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	boom := errors.New("handler exploded")
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		panic(boom)
	}), mq.WithMaxAttempts(1))
	err := run.wait()
	var pe *mq.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("Run = %#v, want a chain carrying a *mq.PanicError", err)
	}
	if pe.Value != any(boom) {
		t.Fatalf("PanicError.Value = %v, want the panicked value", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Fatal("PanicError.Stack is empty; the stack is the only thing that says where the panic was")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, does not unwrap to the panicked error", err)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 0 {
		t.Fatalf("acked = %d after a handler panic, want 0: no panic may silently ack", counts.acked)
	}
}

// TestRunKeepsRunningAfterAHandlerPanic is the other half the
// delivery takes the failure policy and the RUNNER survives. A panic that
// escaped to the fetch or worker goroutine would take the consumer with it.
func TestRunKeepsRunningAfterAHandlerPanic(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			panic("boom")
		}
		// Reaching here at all is the assertion: the runner fetched again
		// after the panic and the redelivery arrived.
		return nil
	}}
	run := startRun(t, c, h, mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the redelivery after the panic", func() bool { return h.count() >= 2 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 1 {
		t.Fatalf("acked = %d, want 1: the panicking delivery must not ack and its retry must", counts.acked)
	}
	if counts.requeued == 0 {
		t.Fatal("the panicking delivery was never requeued")
	}
}

// TestRunPanicDoesNotStopTheSiblingLanes is the second rule. Recovery lives
// inside the item and not around the worker loop, so a lane whose handler
// panics settles its own delivery and every other lane keeps working. A
// runner that let the panic reach dispatch's worker would lose that worker
// and every lane it owns.
func TestRunPanicDoesNotStopTheSiblingLanes(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	const (
		lanes = 4
		good  = 64
	)
	ctx := context.Background()
	// One key panics for ever; the others must still be handled.
	for i := range good {
		m := &mq.Message{ID: strconv.Itoa(i), Key: []byte("ok-" + strconv.Itoa(i)), Body: []byte("b")}
		if err := cl.Publish(ctx, "orders", m); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	if err := cl.Publish(ctx, "orders", &mq.Message{ID: "boom", Key: []byte("boom"), Body: []byte("b")}); err != nil {
		t.Fatalf("publishing the poison message: %v", err)
	}
	var ok atomic.Int64
	var panics atomic.Int64
	run := startRun(t, c, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		if d.ID == "boom" {
			panics.Add(1)
			panic("boom")
		}
		ok.Add(1)
		return nil
	}),
		mq.WithConcurrency(lanes),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
		// The panicking key must never halt the runner, or the test would
		// be measuring the halt rather than the lane isolation.
		mq.WithFailurePolicy(mq.FailureInfiniteRequeue),
	)
	waitFor(t, "every non-panicking message", func() bool { return ok.Load() >= good })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if panics.Load() == 0 {
		t.Fatal("the panicking handler never ran, so the test proves nothing")
	}
}

// --- retry ----------------------------------------------------------------

// TestRunRetriesByNackAndCountsTheBrokersAttempts pins the retry shape
// : a failed delivery is nacked, the broker redelivers it
// with the next attempt number, and the runner reads that number rather than
// keeping a count of its own.
func TestRunRetriesByNackAndCountsTheBrokersAttempts(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	var attempts []int
	var mu sync.Mutex
	h := &collector{fn: func(n int, d *mq.Delivery) error {
		mu.Lock()
		attempts = append(attempts, d.Attempt)
		mu.Unlock()
		if n < 3 {
			return errors.New("try again")
		}
		return nil
	}}
	run := startRun(t, c, h, mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the third attempt to succeed", func() bool { return b.counts("orders", "billing").acked == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) < 3 {
		t.Fatalf("attempts = %v, want at least three deliveries", attempts)
	}
	for i, a := range attempts[:3] {
		if a != i+1 {
			t.Fatalf("attempts = %v, want the broker's 1,2,3 (the runner keeps no count of its own)", attempts)
		}
	}
}

// TestRunRetryAfterOverridesTheBackoffAndNotTheBound pins RetryAfter:
// it replaces the wait, not the attempt limit, so a handler cannot retry for
// ever by returning it.
func TestRunRetryAfterOverridesTheBackoffAndNotTheBound(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders", "orders.dlq")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{fn: func(_ int, _ *mq.Delivery) error {
		return mq.RetryAfter(errors.New("later"), time.Millisecond)
	}}
	run := startRun(t, c, h,
		mq.WithMaxAttempts(2),
		mq.WithDeadLetter("orders.dlq"),
	)
	waitFor(t, "the message to be dead-lettered", func() bool {
		return c.Stats().Acked == 1
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := h.count(); got != 2 {
		t.Fatalf("handler ran %d times, want 2 (WithMaxAttempts bounds RetryAfter too)", got)
	}
}

// refusing is a middleware that postpones the first n deliveries it is given
// without calling the handler, the way a rate limit or an open circuit does.
func refusing(n int64, delay time.Duration) (mq.Middleware, *atomic.Int64) {
	var seen atomic.Int64
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if seen.Add(1) <= n {
				return mq.Postpone(errors.New("not now"), delay)
			}
			return next.Handle(ctx, d)
		})
	}, &seen
}

// TestRunPostponeIsNotAnAttempt pins Postpone against the bound: a message
// postponed more often than WithMaxAttempts allows is never exhausted by it,
// because the handler never ran. Under the default relaxed ordering a wait
// well inside the lease is held on the worker, so the broker hears of none of
// them and has nothing to count.
func TestRunPostponeIsNotAnAttempt(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders", "orders.dlq")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	mw, refused := refusing(5, time.Millisecond)
	h := &collector{}
	run := startRun(t, c, h,
		mq.WithMiddleware(mw),
		mq.WithMaxAttempts(2),
		mq.WithDeadLetter("orders.dlq"),
		mq.WithFailurePolicy(mq.FailureSkip),
	)
	waitFor(t, "the handler to run once the refusals stop", func() bool { return h.count() == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := refused.Load(); got != 6 {
		t.Fatalf("the chain ran %d times, want 6: five postponements and the delivery that was handled", got)
	}
	counts := b.counts("orders", "billing")
	if counts.requeued != 0 || counts.acked != 1 {
		t.Fatalf("counts = %+v, want the postponements held and one ack", counts)
	}
	if s := c.Stats(); s.DeadLetterFailures != 0 {
		t.Fatalf("stats = %+v, want nothing dead-lettered", s)
	}
}

// TestRunRelaxedOrderingHoldsAPostponementWhileTheLeaseAllows is where a
// relaxed run waits out a postponement. A delivery given back to the broker
// comes back counted, so the runner holds it on its worker whenever holding
// cannot outlive the lease, and gives back only a wait that would take more
// than half of what is left of it: a delivery held past its lease is
// redelivered underneath the worker holding it.
func TestRunRelaxedOrderingHoldsAPostponementWhileTheLeaseAllows(t *testing.T) {
	const lease = 100 * time.Millisecond
	cases := []struct {
		name     string
		cfg      fakeConfig
		opts     []mq.RunOption
		delay    time.Duration
		requeued int
	}{
		{name: "AWaitInsideHalfTheLease", cfg: fakeConfig{lease: time.Second}, delay: 10 * time.Millisecond},
		{name: "AWaitLongerThanHalfTheLease", cfg: fakeConfig{lease: lease}, delay: lease * 3 / 4, requeued: 1},
		{
			name:  "ALeaseTheRunnerExtends",
			cfg:   fakeConfig{lease: lease, extend: true},
			opts:  []mq.RunOption{mq.WithLeaseExtension(lease / 4)},
			delay: lease * 3 / 4,
		},
		{name: "NoLease", cfg: fakeConfig{lease: -1}, delay: lease * 3 / 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFakeWith(t, tc.cfg, "orders")
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			mw, _ := refusing(1, tc.delay)
			h := &collector{}
			run := startRun(t, c, h, append([]mq.RunOption{mq.WithMiddleware(mw)}, tc.opts...)...)
			waitFor(t, "the handler to run after the postponement", func() bool { return h.count() == 1 })
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			if counts := b.counts("orders", "billing"); counts.requeued != tc.requeued || counts.acked != 1 {
				t.Fatalf("counts = %+v, want %d given back and one ack", counts, tc.requeued)
			}
		})
	}
}

// TestRunPostponeWaitsEvenWithNoBackoff is a postponement that names no
// delay on a run configured with no backoff, as Dedup's refusal of a duplicate
// still in flight is under mq.ConstantBackoff(0). Offered again at once, it
// was refused again at once, and its worker spun for as long as the first
// copy stayed in its handler.
func TestRunPostponeWaitsEvenWithNoBackoff(t *testing.T) {
	clk := newFixedClock()
	cl, _ := openFakeClocked(t, fakeConfig{lease: -1}, clk, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	mw, offered := refusing(1, 0)
	h := &collector{}
	run := startRun(t, c, h, mq.WithMiddleware(mw), mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the postponement to wait on the clock, or the handler to run", func() bool {
		return clk.liveTimers() > 0 || h.count() > 0
	})
	if got := offered.Load(); got != 1 || h.count() != 0 {
		t.Fatalf("the chain was offered the delivery %d times before the clock moved, want once", got)
	}
	clk.advance(time.Millisecond)
	waitFor(t, "the handler after the wait", func() bool { return h.count() == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

// TestRunStrictOrderingPostponesInPlaceWithoutCounting is Postpone under
// OrderingStrict: the delivery is held on its lane and the chain is called
// again, so the broker is told nothing, and the postponements leave the
// runner's own count alone. The handler then gets every one of its attempts.
func TestRunStrictOrderingPostponesInPlaceWithoutCounting(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	mw, _ := refusing(10, time.Millisecond)
	h := &collector{fn: func(int, *mq.Delivery) error { return errors.New("no") }}
	run := startRun(t, c, h,
		mq.WithOrderingByKey(),
		mq.WithMiddleware(mw),
		mq.WithMaxAttempts(3),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	var pe *mq.PoisonError
	if err := run.wait(); !errors.As(err, &pe) {
		t.Fatalf("Run = %v, want a *PoisonError once the handler's attempts ran out", err)
	}
	if got := h.count(); got != 3 {
		t.Fatalf("the handler ran %d times, want 3: ten postponements spent none of WithMaxAttempts(3)", got)
	}
	if pe.Attempt != 3 {
		t.Fatalf("PoisonError.Attempt = %d, want 3", pe.Attempt)
	}
	if counts := b.counts("orders", "billing"); counts.requeued != 1 {
		t.Fatalf("requeued = %d, want 1: only the halt gives the message back", counts.requeued)
	}
}

// TestRunStopOnFailureHoldsAPostponementInsteadOfStopping is Postpone under
// OrderingStopOnFailure. A postponement is not a failure, so it does not stop
// the runner, and the delivery is held on its lane so that nothing overtakes
// it.
func TestRunStopOnFailureHoldsAPostponementInsteadOfStopping(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 2)
	mw, _ := refusing(3, time.Millisecond)
	h := &collector{}
	run := startRun(t, c, h,
		mq.WithConcurrency(1),
		mq.WithOrderingByKey(mq.OrderingStopOnFailure),
		mq.WithMiddleware(mw))
	waitFor(t, "both messages", func() bool { return h.count() == 2 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got, want := h.seenIDs(), []string{"acct-0", "acct-1"}; !equalIDs(got, want) {
		t.Fatalf("handled %v, want %v", got, want)
	}
	if counts := b.counts("orders", "billing"); counts.requeued != 0 || counts.acked != 2 {
		t.Fatalf("counts = %+v, want nothing given back and both acked", counts)
	}
}

// TestRunBatchGivesAPostponedDeliveryBack is Postpone from a batch handler. A
// batch runner cannot call its handler again for one delivery of a batch, so
// even under OrderingStopOnFailure the delivery goes back to the broker; it is
// neither left unsettled nor taken for a failure that stops the runner.
func TestRunBatchGivesAPostponedDeliveryBack(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	var calls atomic.Int64
	h := mq.BatchHandlerFunc(func(context.Context, []*mq.Delivery) error {
		if calls.Add(1) == 1 {
			return mq.Postpone(errors.New("not now"), time.Millisecond)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.RunBatch(ctx, h, mq.WithOrderingByKey(mq.OrderingStopOnFailure)) }()
	waitFor(t, "the postponed delivery to be handled again", func() bool { return calls.Load() == 2 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	if counts := b.counts("orders", "billing"); counts.requeued != 1 || counts.acked != 1 {
		t.Fatalf("counts = %+v, want the postponement given back once and the redelivery acked", counts)
	}
}

// TestRunManualSettlementHonoursAPostponement is Postpone under
// SettlementManual. The caller owns the settlement of what its handler is
// given, and a postponed delivery never reached the handler, so the runner
// holds it or gives it back as it does under SettlementAuto. It returned
// without reading the directive, and the delivery stayed in flight until its
// lease ran out, or for ever on a driver with no lease.
func TestRunManualSettlementHonoursAPostponement(t *testing.T) {
	const lease = 100 * time.Millisecond
	cases := []struct {
		name     string
		cfg      fakeConfig
		delay    time.Duration
		requeued int
	}{
		{name: "HeldWhileTheLeaseAllows", cfg: fakeConfig{lease: time.Second}, delay: time.Millisecond},
		{name: "GivenBackWhenItWouldOutliveHalfTheLease", cfg: fakeConfig{lease: lease}, delay: lease * 3 / 4, requeued: 1},
		{name: "HeldWithNoLease", cfg: fakeConfig{lease: -1}, delay: time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFakeWith(t, tc.cfg, "orders")
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			mw, _ := refusing(1, tc.delay)
			h := &collector{fn: func(_ int, d *mq.Delivery) error { return d.Ack() }}
			run := startRun(t, c, h, mq.WithMiddleware(mw), mq.WithSettlement(mq.SettlementManual))
			waitFor(t, "the handler to be given the postponed delivery", func() bool { return h.count() == 1 })
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			if counts := b.counts("orders", "billing"); counts.requeued != tc.requeued || counts.acked != 1 || counts.inflight != 0 {
				t.Fatalf("counts = %+v, want %d given back and the handler's own ack", counts, tc.requeued)
			}
		})
	}
}

// TestRunBatchManualSettlementGivesAPostponedDeliveryBack is Postpone from a
// batch handler under SettlementManual, for one entry of a *BatchError. That
// delivery never reached the handler and goes back to the broker; the ones
// the handler settled are left to it.
func TestRunBatchManualSettlementGivesAPostponedDeliveryBack(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishDistinctKeys(t, cl, 4)
	h := &batchCollector{fn: func(n int, ds []*mq.Delivery) error {
		if n == 1 && len(ds) != 4 {
			return fmt.Errorf("the first batch held %d deliveries, want 4", len(ds))
		}
		errs := make([]error, len(ds))
		for i, d := range ds {
			if n == 1 && i == 1 {
				errs[i] = mq.Postpone(errors.New("not now"), time.Millisecond)
				continue
			}
			errs[i] = d.Ack()
		}
		return &mq.BatchError{Errs: errs}
	}}
	run := startRunBatch(t, c, h,
		mq.WithOrdering(mq.OrderingUnordered),
		mq.WithSettlement(mq.SettlementManual))
	waitFor(t, "every message to be acked, the postponed one after it was given back", func() bool {
		return b.counts("orders", "billing").acked == 4
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunBatch = %v, want context.Canceled", err)
	}
	if counts := b.counts("orders", "billing"); counts.requeued != 1 {
		t.Fatalf("requeued = %d, want exactly the postponed delivery", counts.requeued)
	}
}

// TestRunOrderedPostponementLeavesTheHandlerALiveLease is a postponement
// longer than the lease under OrderingStrict and OrderingStopOnFailure with
// WithLeaseDeadline. Held on the lane, it ended with the lease gone, and the
// handler called afterwards was given a context that had already ended: its
// failure was counted, so a postponement exhausted a message (or stopped the
// runner) that no handler had a chance to handle. The runner gives that
// delivery back to the broker instead, and holds a wait that leaves the
// handler half its lease.
func TestRunOrderedPostponementLeavesTheHandlerALiveLease(t *testing.T) {
	const lease = time.Second
	cases := []struct {
		name     string
		mode     mq.OrderingMode
		delay    time.Duration
		requeued int
	}{
		{name: "StrictGivesBackAWaitLongerThanTheLease", mode: mq.OrderingStrict, delay: lease * 3 / 2, requeued: 1},
		{name: "StopOnFailureGivesBackAWaitLongerThanTheLease", mode: mq.OrderingStopOnFailure, delay: lease * 3 / 2, requeued: 1},
		{name: "StrictHoldsAWaitInsideHalfTheLease", mode: mq.OrderingStrict, delay: 10 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFakeWith(t, fakeConfig{lease: lease}, "orders")
			c := consume(t, cl, mq.ConsumerConfig{})
			publishKeyed(t, cl, "acct", 1)
			mw, _ := refusing(1, tc.delay)
			var handled atomic.Int64
			h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				handled.Add(1)
				return nil
			})
			run := startRun(t, c, h,
				mq.WithOrderingByKey(tc.mode),
				mq.WithLeaseDeadline(),
				mq.WithMiddleware(mw),
				mq.WithMaxAttempts(3),
				mq.WithRetryBackoff(mq.ConstantBackoff(0)))
			waitFor(t, "the handler to run inside a live lease", func() bool { return handled.Load() == 1 || run.returned() })
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v after the handler ran %d times inside a live lease, want context.Canceled", err, handled.Load())
			}
			if counts := b.counts("orders", "billing"); counts.requeued != tc.requeued || counts.acked != 1 {
				t.Fatalf("counts = %+v, want %d given back and one ack", counts, tc.requeued)
			}
		})
	}
}

// --- directives -----------------------------------------------------------

// TestRunDiscardAcksWithoutSuccess pins Discard: the message is acked
// and gone. It is reachable only by a handler asking for it by name, which is
// the whole of "never skip by default".
func TestRunDiscardAcksWithoutSuccess(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := &collector{fn: func(_ int, _ *mq.Delivery) error {
		return mq.Discard(errors.New("not for us"))
	}}
	run := startRun(t, c, h)
	waitFor(t, "the discard", func() bool { return b.counts("orders", "billing").acked == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := h.count(); got != 1 {
		t.Fatalf("handler ran %d times, want 1: a discard is terminal", got)
	}
}

// TestRunHaltStopsTheRunnerAndRequeuesTheMessage pins Halt. The
// handler's own error is what Run reports, and the message goes back first —
// halting must not be a way to lose one.
func TestRunHaltStopsTheRunnerAndRequeuesTheMessage(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	boom := errors.New("stop the line")
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return mq.Halt(boom)
	}))
	err := run.wait()
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want the handler's own error", err)
	}
	if d, _ := mq.DirectiveOf(err); d != mq.DirectiveHalt {
		t.Fatalf("DirectiveOf(Run) = %v, want DirectiveHalt", d)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 0 {
		t.Fatalf("acked = %d, want 0: a halt must not ack", counts.acked)
	}
	if counts.requeued == 0 {
		t.Fatal("the halted message was never requeued, so it is only redeliverable on a lease expiry")
	}
}

// --- the failure policy ---------------------------------------------------

// TestRunHaltsWhenAttemptsRunOutWithNoDeadLetter is ADR-011's default and
// ADR-010's "never skip": a message nobody can dead-letter stops the consumer
// rather than being dropped, and the caller is told which message and why.
func TestRunHaltsWhenAttemptsRunOutWithNoDeadLetter(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	boom := errors.New("cannot process this")
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return boom }),
		mq.WithMaxAttempts(2),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
	)
	err := run.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) {
		t.Fatalf("Run = %v, want ErrPoisonMessage (ADR-011: otherwise halt)", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, does not carry the handler's own error", err)
	}
	if got := mq.CodeOf(err); got != mq.CodePoisonMessage {
		t.Fatalf("CodeOf(Run) = %v, want CodePoisonMessage", got)
	}
	var pe *mq.PoisonError
	if !errors.As(err, &pe) {
		t.Fatalf("Run = %#v, want a *mq.PoisonError naming the delivery", err)
	}
	if pe.Topic != "orders" || pe.Subscription != "billing" || pe.Attempt != 2 {
		t.Fatalf("PoisonError = %+v, want the orders/billing delivery on attempt 2", pe)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 0 || counts.dropped != 0 {
		t.Fatalf("counts = %+v, want the message neither acked nor dropped", counts)
	}
}

// TestRunSkipOnlyWhenAskedForByName pins the one destructive policy. It is
// here so that "never skip by default" has something to be the opposite of:
// the same handler and the same attempts halt without this option.
func TestRunSkipOnlyWhenAskedForByName(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return errors.New("no")
	}),
		mq.WithMaxAttempts(1),
		mq.WithFailurePolicy(mq.FailureSkip),
	)
	waitFor(t, "the skip", func() bool { return b.counts("orders", "billing").acked == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

// --- dead-lettering -------------------------------------------------------

// TestRunDeadLettersWithProvenanceHeaders pins the dead-letter route: the
// message reaches the dead-letter topic with the five provenance headers, and
// the original is acked afterwards.
func TestRunDeadLettersWithProvenanceHeaders(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders", "orders.dlq")
	c := consume(t, cl, mq.ConsumerConfig{})
	dlq := consumeOn(t, cl, "orders.dlq", "audit")
	publishN(t, cl, 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
	)
	waitFor(t, "the original to be acked", func() bool { return b.counts("orders", "billing").acked == 1 })
	d := receiveOne(t, dlq)
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	want := map[string]string{
		mq.HeaderOriginTopic:        "orders",
		mq.HeaderOriginSubscription: "billing",
		mq.HeaderAttempts:           "1",
		mq.HeaderFailure:            "undecodable",
	}
	for k, v := range want {
		got, ok := d.Headers.Get(k)
		if !ok {
			t.Fatalf("the dead-lettered message has no %s header", k)
		}
		if string(got) != v {
			t.Fatalf("%s = %q, want %q", k, got, v)
		}
	}
	if _, ok := d.Headers.Get(mq.HeaderFirstDeliveredAt); !ok {
		t.Fatalf("the dead-lettered message has no %s header on its first attempt", mq.HeaderFirstDeliveredAt)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("acking the dead-lettered message: %v", err)
	}
}

// TestRunLeavesFirstDeliveredAtOffAnUncountedRedelivery is a broker that
// counts no attempts dead-lettering a redelivery. Attempt zero says only
// that nothing was counted, so this receive time is not the first
// delivery's and the header must be absent rather than false; the first
// delivery, which the broker did not flag as a redelivery, still carries it.
func TestRunLeavesFirstDeliveredAtOffAnUncountedRedelivery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failing int
		want    bool
	}{
		{"first delivery", 0, true},
		{"redelivery", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFakeWith(t, fakeConfig{countless: true}, "orders", "orders.dlq")
			c := consume(t, cl, mq.ConsumerConfig{})
			dlq := consumeOn(t, cl, "orders.dlq", "audit")
			publishN(t, cl, 1)
			var calls atomic.Int64
			run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
				if int(calls.Add(1)) <= tc.failing {
					return errors.New("not yet")
				}
				return mq.DeadLetter(errors.New("undecodable"))
			}),
				mq.WithDeadLetter("orders.dlq"),
				mq.WithRetryBackoff(mq.ConstantBackoff(0)),
			)
			waitFor(t, "the original to be acked", func() bool { return b.counts("orders", "billing").acked == 1 })
			d := receiveOne(t, dlq)
			if err := run.stop(); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			if _, ok := d.Headers.Get(mq.HeaderFirstDeliveredAt); ok != tc.want {
				t.Fatalf("%s present = %v, want %v", mq.HeaderFirstDeliveredAt, ok, tc.want)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("acking the dead-lettered message: %v", err)
			}
		})
	}
}

// TestRunNeverAcksAfterAFailedDeadLetterPublish is the single worst defect
// this increment could ship: an ack after a dead-letter publication that did
// not happen destroys the message in exactly the case the dead-letter topic
// exists for. The specification and ADR-011 fix the order — publish, confirm, then settle
// — and this is the negative half of it.
func TestRunNeverAcksAfterAFailedDeadLetterPublish(t *testing.T) {
	testDeadLetterPublishFailure(t, errors.New("the dead-letter broker is down"))
}

// TestRunNeverAcksAfterAnAmbiguousDeadLetterPublish is the same rule for the
// outcome nobody can resolve. An ambiguous publication may have landed, so
// acking would lose the message half the time and keeping it produces a
// duplicate in the dead-letter topic — and the at-least-once boundary makes
// the duplicate the recoverable half.
func TestRunNeverAcksAfterAnAmbiguousDeadLetterPublish(t *testing.T) {
	testDeadLetterPublishFailure(t, mq.ErrSettlementUnknown)
}

// testDeadLetterPublishFailure runs one dead-letter publication failure and
// asserts that nothing was acked and that the message is still the broker's.
func testDeadLetterPublishFailure(t *testing.T, refuse error) {
	t.Helper()
	cl, b := openFakeWith(t, fakeConfig{}, "orders", "orders.dlq")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	var refused atomic.Int64
	b.setRefuse(func(_ int, m *mq.Message) error {
		if m.Topic == "orders.dlq" {
			refused.Add(1)
			return refuse
		}
		return nil
	})
	h := &collector{fn: func(_ int, _ *mq.Delivery) error { return errors.New("undecodable") }}
	run := startRun(t, c, h,
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
	)
	// The failed publication requeues the original, so the handler sees it
	// again; three rounds is enough to show the loop is a retry loop and not
	// a silent ack.
	waitFor(t, "three refused dead-letter publications", func() bool { return refused.Load() >= 3 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 0 {
		t.Fatalf("acked = %d after a failed dead-letter publish, want 0: the specification acks only after a confirmed publish", counts.acked)
	}
	if counts.dropped != 0 || counts.deadLettered != 0 {
		t.Fatalf("counts = %+v, want the message neither dropped nor dead-lettered", counts)
	}
	if counts.ready+counts.inflight != 1 {
		t.Fatalf("counts = %+v, want the one message still redeliverable", counts)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}

	if got := c.Stats().DeadLetterFailures; got < refused.Load() {
		t.Fatalf("DeadLetterFailures = %d after %d refused publications, want at least that many", got, refused.Load())
	}
}

// TestRunDeadLetterDirectiveHaltsWhenThereIsNoDestination pins the other half
// of DeadLetter: a handler may ask for it, and a runner with nowhere
// to put the message halts rather than dropping it.
func TestRunDeadLetterDirectiveHaltsWhenThereIsNoDestination(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return mq.DeadLetter(errors.New("poison"))
	}))
	err := run.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) {
		t.Fatalf("Run = %v, want ErrPoisonMessage", err)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 0 {
		t.Fatalf("acked = %d, want 0", counts.acked)
	}
}

// --- settlement modes and admission --------------------------------------

// TestRunManualSettlementSettlesNothing pins Manual mode: the handler
// owns the delivery and the runner never touches it.
func TestRunManualSettlementSettlesNothing(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 2)
	h := &collector{fn: func(n int, d *mq.Delivery) error {
		if n == 1 {
			return d.Ack()
		}
		return nil
	}}
	run := startRun(t, c, h, mq.WithSettlement(mq.SettlementManual))
	waitFor(t, "both deliveries", func() bool { return h.count() == 2 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	counts := b.counts("orders", "billing")
	if counts.acked != 1 {
		t.Fatalf("acked = %d, want 1: only the delivery the handler settled", counts.acked)
	}
}

// TestRunHonoursMaxInFlight pins the admission bound. The runner takes a slot
// before it fetches and returns it when the delivery is SETTLED, so the number
// of deliveries it is holding unsettled is exactly the bound — not "about" it.
//
// The measure is ConsumerStats.InFlight and not the number of handlers
// running, and the difference is the whole test: with one worker, at most one
// handler runs whatever the bound is, so a test that counted handlers would
// pass for a runner with no admission at all. What a broken bound shows up as
// is deliveries fetched and queued on the lanes with nothing settling them.
func TestRunHonoursMaxInFlight(t *testing.T) {
	const bound = 3
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{Prefetch: 64})
	publishN(t, cl, 64)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	}),
		mq.WithConcurrency(1),
		mq.WithMaxInFlight(bound),
	)
	<-entered
	// Give the fetch loop every chance to overshoot: it is blocked only by
	// the slots, and there are 64 messages waiting for it.
	peak := int64(0)
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n := c.Stats().InFlight; n > peak {
			peak = n
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if peak > bound {
		t.Fatalf("peak unsettled deliveries = %d, want at most WithMaxInFlight(%d)", peak, bound)
	}
	if peak == 0 {
		t.Fatal("nothing was ever in flight, so the bound was not exercised")
	}
}

// TestRunDrainsInFlightWorkBeforeItReturns pins the second lane rule and the drain: Run does not return while a handler is still running, and a
// delivery a lane accepted is always settled.
func TestRunDrainsInFlightWorkBeforeItReturns(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 8)
	var running atomic.Int64
	var finished atomic.Int64
	started := make(chan struct{}, 8)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		running.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		time.Sleep(20 * time.Millisecond)
		finished.Add(1)
		return nil
	}), mq.WithConcurrency(4))
	<-started
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if r, f := running.Load(), finished.Load(); r != f {
		t.Fatalf("Run returned with %d handlers started and %d finished: it abandoned in-flight work", r, f)
	}
	counts := b.counts("orders", "billing")
	if counts.inflight != 0 {
		t.Fatalf("counts = %+v, want nothing left unsettled after the drain", counts)
	}
	if !counts.conserved() {
		t.Fatalf("The specification conservation broken: %+v", counts)
	}
}

// --- middleware and configuration ----------------------------------------

// TestRunComposesTheMiddlewareChainOnce pins rule 6: the chain is built
// when Run starts, not per message.
func TestRunComposesTheMiddlewareChainOnce(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 4)
	var composed atomic.Int64
	var calls atomic.Int64
	h := &collector{}
	run := startRun(t, c, h, mq.WithMiddleware(func(next mq.Handler) mq.Handler {
		composed.Add(1)
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			calls.Add(1)
			return next.Handle(ctx, d)
		})
	}))
	waitFor(t, "four deliveries", func() bool { return h.count() == 4 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := composed.Load(); got != 1 {
		t.Fatalf("the middleware was composed %d times, want once", got)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("the middleware ran %d times, want 4", got)
	}
}

// TestRunRejectsUnusableConfigurations pins the validation. Each row is a
// configuration that cannot do what it says, and each must be refused before
// a single message is fetched.
func TestRunRejectsUnusableConfigurations(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	cases := []struct {
		name string
		opts []mq.RunOption
		code mq.ErrorCode
	}{
		{"zero option", []mq.RunOption{{}}, mq.CodeInvalidConfig},
		{"no concurrency", []mq.RunOption{mq.WithConcurrency(0)}, mq.CodeInvalidConfig},
		{"too much concurrency", []mq.RunOption{mq.WithConcurrency(mq.MaxConcurrency + 1)}, mq.CodeInvalidConfig},
		{"no attempts", []mq.RunOption{mq.WithMaxAttempts(0)}, mq.CodeInvalidConfig},
		{"no in flight", []mq.RunOption{mq.WithMaxInFlight(-1)}, mq.CodeInvalidConfig},
		{"nil backoff", []mq.RunOption{mq.WithRetryBackoff(nil)}, mq.CodeInvalidConfig},
		{"unknown policy", []mq.RunOption{mq.WithFailurePolicy(mq.FailurePolicy(200))}, mq.CodeInvalidConfig},
		{"unknown settlement", []mq.RunOption{mq.WithSettlement(mq.SettlementMode(200))}, mq.CodeInvalidConfig},
		{"dead-letter policy without a topic", []mq.RunOption{mq.WithFailurePolicy(mq.FailureDeadLetter)}, mq.CodeInvalidConfig},
		{"dead-letter option naming no topic", []mq.RunOption{mq.WithDeadLetter("")}, mq.CodeInvalidConfig},
		{"invalid dead-letter topic", []mq.RunOption{mq.WithDeadLetter("orders dlq!")}, mq.CodeInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := c.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }), tc.opts...)
			if err == nil {
				t.Fatal("the configuration was accepted")
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Fatalf("CodeOf = %v, want %v: %v", got, tc.code, err)
			}
		})
	}
	if err := c.Run(context.Background(), nil); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("Run with a nil handler = %v, want CodeInvalidConfig", err)
	}
}

// TestRunFailsWhenTheDeadLetterTopicDoesNotExist pins where the failure is
// discovered: when Run starts, not while it is holding a poison message.
func TestRunFailsWhenTheDeadLetterTopicDoesNotExist(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	// Bounded, so that a runner which opened the publisher lazily — or not
	// at all — fails this test rather than parking in the fetch for ever.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Run(ctx,
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }),
		mq.WithDeadLetter("orders.dlq"))
	if !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("Run = %v, want ErrTopicNotFound from opening the dead-letter publisher", err)
	}
}

// consumeOn opens a consumer on a named topic and subscription.
func consumeOn(tb testing.TB, cl *mq.Client, topic, sub string) *mq.ConsumerHandle {
	tb.Helper()
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		tb.Fatalf("opening the consumer on %s/%s: %v", topic, sub, err)
	}
	return c
}

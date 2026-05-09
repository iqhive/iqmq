package iqmq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// This file is the shared lease-extension scheduler and the handler
// context: what keeps a long handler's delivery alive, and what ends it.

// countingClock is the wall clock plus a count of the timers anything asked
// it for. It is what makes "never one timer per delivery" checkable: the specification
// forbids a timer per delivery and rule 3 forbids one per message, and
// the only way to see the difference from outside is to count what the core
// asked the clock for.
type countingClock struct {
	created atomic.Int64
}

// Now returns the wall time.
func (c *countingClock) Now() time.Time { return time.Now() }

// NewTimer counts one timer and returns it.
func (c *countingClock) NewTimer(d time.Duration) mq.Timer {
	c.created.Add(1)
	t := time.NewTimer(d)
	return &countingTimer{t: t}
}

type countingTimer struct{ t *time.Timer }

func (t *countingTimer) C() <-chan time.Time        { return t.t.C }
func (t *countingTimer) Stop() bool                 { return t.t.Stop() }
func (t *countingTimer) Reset(d time.Duration) bool { return t.t.Reset(d) }

// openFakeClocked builds a client over a fake broker with a counting clock.
func openFakeClocked(tb testing.TB, cfg fakeConfig, clk mq.Clock, topics ...string) (*mq.Client, *fakeBroker) {
	tb.Helper()
	b := newFakeBroker(cfg, topics...)
	cl := mq.NewClient(b, mq.WithClock(clk))
	tb.Cleanup(func() {
		if err := cl.Close(); err != nil {
			tb.Errorf("closing the client: %v", err)
		}
	})
	return cl, b
}

// blockingHandler holds every delivery until it is released.
type blockingHandler struct {
	seen    atomic.Int64
	release chan struct{}
	ctxs    chan context.Context
	once    sync.Once
}

func newBlockingHandler(n int) *blockingHandler {
	return &blockingHandler{release: make(chan struct{}), ctxs: make(chan context.Context, n)}
}

// Handle implements mq.Handler.
func (h *blockingHandler) Handle(ctx context.Context, _ *mq.Delivery) error {
	h.seen.Add(1)
	select {
	case h.ctxs <- ctx:
	default:
	}
	select {
	case <-h.release:
	case <-ctx.Done():
	case <-time.After(runWait):
	}
	return nil
}

// let releases every handler.
func (h *blockingHandler) let() { h.once.Do(func() { close(h.release) }) }

// count is how many deliveries reached the handler.
func (h *blockingHandler) count() int { return int(h.seen.Load()) }

// TestRunExtendsTheLeaseOfAHeldDelivery is the scheduler: while a handler
// holds a delivery, the runner moves its lease before it would expire.
func TestRunExtendsTheLeaseOfAHeldDelivery(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{extend: true, lease: 120 * time.Millisecond}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := newBlockingHandler(1)
	run := startRun(t, c, h, mq.WithLeaseExtension(100*time.Millisecond))
	waitFor(t, "the lease to be extended under the handler", func() bool {
		return b.counts("orders", "billing").extended >= 2
	})
	waitFor(t, "the extensions to be counted", func() bool {
		return c.Stats().LeaseExtensions >= 2
	})
	h.let()
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if got := c.Stats().LeaseExtensionFailures; got != 0 {
		t.Fatalf("LeaseExtensionFailures = %d, want 0", got)
	}
	if got := b.counts("orders", "billing").extendedBy; got <= 0 {
		t.Fatalf("the driver was asked to extend by %v, want a positive duration", got)
	}
}

// TestRunExtendsWithoutATimerPerDelivery is the specification's "never one timer per
// delivery" and rule 3, checked by counting what the runner asked the
// clock for: sixty-four deliveries held at once, all of them extended, and a
// handful of timers between them.
func TestRunExtendsWithoutATimerPerDelivery(t *testing.T) {
	const held = 64
	clk := &countingClock{}
	cl, b := openFakeClocked(t, fakeConfig{extend: true, lease: 120 * time.Millisecond}, clk, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, held)
	h := newBlockingHandler(held)
	run := startRun(t, c, h, mq.WithConcurrency(held), mq.WithLeaseExtension(100*time.Millisecond))
	waitFor(t, "every delivery to reach a handler", func() bool { return h.count() == held })
	waitFor(t, "every held delivery to have its lease extended", func() bool {
		return b.counts("orders", "billing").extended >= held
	})
	timers := clk.created.Load()
	h.let()
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	// One scheduler, one timer, plus whatever the handles armed on their own.
	// The number that matters is that it does not grow with the deliveries.
	if timers >= held {
		t.Fatalf("the runner armed %d timers for %d held deliveries: the specification forbids a timer per delivery", timers, held)
	}
}

// TestRunRefusesLeaseExtensionOnADriverThatCannotExtend is a capability
// the driver does not have is refused, never emulated in silence. A scheduler
// that extended nothing would leave a caller believing its leases were being
// kept alive.
func TestRunRefusesLeaseExtensionOnADriverThatCannotExtend(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	h := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	// Already done, so a runner that ACCEPTED the option would return the
	// context's error at once instead of running: the refusal happens before
	// the context is ever consulted, so this still checks the refusal and a
	// runner that stopped refusing fails in a second rather than in five
	// minutes.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Run(ctx, h, mq.WithLeaseExtension(time.Second))
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Run = %v, want ErrUnsupported", err)
	}
}

// TestExtendCountsBothOutcomes is the iqmq_lease_extensions_total{outcome}
// over the call a caller driving Receive itself makes. The counter has one
// writer, so the runner's scheduler and this call are counted alike.
func TestExtendCountsBothOutcomes(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{extend: true}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	d := receiveOne(t, c)
	if err := d.Extend(time.Minute); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if got := c.Stats(); got.LeaseExtensions != 1 || got.LeaseExtensionFailures != 0 {
		t.Fatalf("stats = %+v, want one successful extension", got)
	}
	if got := c.Stats().SettleFailures; got != 0 {
		t.Fatalf("SettleFailures = %d: an extension is not a settlement ", got)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// A settled delivery has no lease left to move, which is a failed
	// extension and is counted as one.
	if err := d.Extend(time.Minute); err == nil {
		t.Fatal("Extend of a settled delivery = nil, want a failure")
	}
	if got := c.Stats(); got.LeaseExtensions != 1 || got.LeaseExtensionFailures != 1 {
		t.Fatalf("stats = %+v, want one success and one failure", got)
	}
}

// --- the handler's context -------------------------------------------------

// TestRunHandlerContextEndsWithTheLease is the handler's context
// derives from Run's plus the lease deadline.
func TestRunHandlerContextEndsWithTheLease(t *testing.T) {
	const lease = 400 * time.Millisecond
	cl, _ := openFakeWith(t, fakeConfig{lease: lease}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	deadlines := make(chan time.Time, 1)
	h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Error("the handler's context has no deadline: the specification derives it from the lease")
			return nil
		}
		deadlines <- dl
		return nil
	})
	before := time.Now()
	run := startRun(t, c, h, mq.WithLeaseDeadline())
	var dl time.Time
	select {
	case dl = <-deadlines:
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if left := dl.Sub(before); left <= 0 || left > 2*lease {
		t.Fatalf("the handler's deadline is %v away, want about %v (the lease)", left, lease)
	}
}

// TestRunHandlerTimeoutOverridesTheLease is the specification's "WithHandlerTimeout
// overrides": the option means the same thing on every driver, so a lease
// that happens to be longer does not change it and neither does one that
// happens to be shorter.
func TestRunHandlerTimeoutOverridesTheLease(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{lease: time.Hour}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	errs := make(chan error, 1)
	h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		<-ctx.Done()
		errs <- ctx.Err()
		return nil
	})
	run := startRun(t, c, h, mq.WithHandlerTimeout(50*time.Millisecond))
	select {
	case err := <-errs:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the handler's context ended with %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(runWait):
		t.Fatal("the handler's context never ended: the timeout did not override the hour-long lease")
	}
	_ = run.stop()
}

// TestRunHandlerContextHasNoDeadlineByDefault is the half of the
// decision: WithLeaseDeadline is off unless asked for, so the default path
// derives nothing, schedules nothing and allocates nothing even on a driver
// that grants a lease.
func TestRunHandlerContextHasNoDeadlineByDefault(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{lease: time.Hour}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	got := make(chan bool, 1)
	h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		_, ok := ctx.Deadline()
		got <- ok
		return nil
	})
	run := startRun(t, c, h)
	select {
	case ok := <-got:
		if ok {
			t.Fatal("the handler's context has a deadline and nothing asked for one")
		}
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	_ = run.stop()
}

// TestRunHandlerContextHasNoDeadlineWhenNothingNarrowsIt is the other half,
// and it is what keeps the derivation off the path of a driver that grants no
// lease even when the option IS given: nothing is derived, nothing is
// scheduled, nothing is allocated.
func TestRunHandlerContextHasNoDeadlineWhenNothingNarrowsIt(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{lease: -1}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	got := make(chan bool, 1)
	h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		_, ok := ctx.Deadline()
		got <- ok
		return nil
	})
	run := startRun(t, c, h, mq.WithLeaseDeadline())
	select {
	case ok := <-got:
		if ok {
			t.Fatal("the handler's context has a deadline, and no lease and no timeout named one")
		}
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	_ = run.stop()
}

// TestRunHandlerContextIsNotNarrowedWhileTheLeaseIsKeptAlive is a runner whose scheduler is moving the lease has told the broker it
// still wants the delivery, so cancelling the handler at a deadline the
// scheduler has just moved would be a bound on nothing.
func TestRunHandlerContextIsNotNarrowedWhileTheLeaseIsKeptAlive(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{extend: true, lease: 400 * time.Millisecond}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	got := make(chan bool, 1)
	h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		_, ok := ctx.Deadline()
		got <- ok
		return nil
	})
	run := startRun(t, c, h, mq.WithLeaseDeadline(), mq.WithLeaseExtension(100*time.Millisecond))
	select {
	case ok := <-got:
		if ok {
			t.Fatal("the handler's context ends with a lease the runner is extending")
		}
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	_ = run.stop()
}

// TestRunDrainTimeoutCancelsTheHandlersContext is the specification's "wait for in-flight
// handlers within DrainTimeout": when the patience runs out the handlers'
// context is cancelled, which is the only lever anything has over a running
// goroutine.
//
// The consumer closes under a handler that is waiting on its context, so
// nothing else can end it: without the cancellation the runner never returns.
func TestRunDrainTimeoutCancelsTheHandlersContext(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	entered := make(chan struct{}, 1)
	h := mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, h, mq.WithDrainTimeout(50*time.Millisecond)) }()
	select {
	case <-entered:
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("Run = %v, want ErrClosed", err)
		}
	case <-time.After(runWait):
		t.Fatal("Run never returned: the drain timeout did not cancel the handler's context")
	}
}

// TestConsumerDrainStopsReceivingAndLetsTheHandlersFinish is the middle
// verb and Drain: the consumer stops receiving, the handlers already in
// flight finish, and the handle is still open afterwards.
func TestConsumerDrainStopsReceivingAndLetsTheHandlersFinish(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			close(entered)
			<-release
			return nil
		}))
	}()
	select {
	case <-entered:
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	drained := make(chan error, 1)
	go func() { drained <- c.Drain(context.Background()) }()
	select {
	case err := <-drained:
		t.Fatalf("Drain returned %v while a handler still held a delivery", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain = %v, want nil", err)
		}
	case <-time.After(runWait):
		t.Fatal("Drain never returned")
	}
	if err := <-done; !errors.Is(err, context.Canceled) && !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Run = %v, want the drain to have ended it", err)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 1 {
		t.Fatalf("counts = %+v, want the drained delivery acked", counts)
	}
	// The handle is still open: Drain is not Shutdown.
	if err := c.Drain(context.Background()); err != nil {
		t.Fatalf("a second Drain = %v, want nil", err)
	}
	publishN(t, cl, 1)
	dst := make([]mq.Delivery, 1)
	ctx, cancel := context.WithTimeout(context.Background(), runWait)
	defer cancel()
	n, err := c.Receive(ctx, dst)
	if err != nil || n != 1 {
		t.Fatalf("Receive after Drain = (%d, %v), want one delivery: Drain leaves the consumer open", n, err)
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatalf("acking: %v", err)
	}
}

// --- what the scheduler actually buys -------------------------------------

// leaseSweeper is the broker's own visibility sweep, running the way a real
// broker's timer runs it: every interval, every in-flight record whose lease
// deadline has passed goes back to the ready queue.
//
// It exists because the fake broker's Extend used to be a counter, so every
// test of the scheduler could check only that the driver had been CALLED.
// A scheduler that extended by zero, extended the wrong delivery or stopped
// after the first extension passed all of them.
type leaseSweeper struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
	took atomic.Int64
}

// sweepLeases starts the sweep on the named subscription.
func sweepLeases(tb testing.TB, b *fakeBroker, topic, sub string, every time.Duration) *leaseSweeper {
	tb.Helper()
	sw := &leaseSweeper{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(sw.done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-sw.stop:
				return
			case <-t.C:
				sw.took.Add(int64(b.expireLeases(topic, sub)))
			}
		}
	}()
	tb.Cleanup(sw.halt)
	return sw
}

// halt stops the sweep and waits for it, so that no sweep runs after the test
// that owns it has finished with the broker.
func (sw *leaseSweeper) halt() {
	sw.once.Do(func() {
		close(sw.stop)
		<-sw.done
	})
}

// slowFirstHandler holds its FIRST delivery past the lease and returns at
// once for every one after it, so that a redelivery the runner did not expect
// terminates instead of being held past its lease again.
type slowFirstHandler struct {
	hold time.Duration

	calls    atomic.Int64
	mu       sync.Mutex
	attempts []uint32
}

// Handle implements mq.Handler.
func (h *slowFirstHandler) Handle(ctx context.Context, d *mq.Delivery) error {
	n := h.calls.Add(1)
	h.mu.Lock()
	h.attempts = append(h.attempts, d.Info().Attempt)
	h.mu.Unlock()
	if n == 1 {
		select {
		case <-time.After(h.hold):
		case <-ctx.Done():
		}
	}
	return nil
}

// seen is how many deliveries reached the handler.
func (h *slowFirstHandler) seen() int { return int(h.calls.Load()) }

// TestRunLeaseSchedulerIsWhatKeepsALongHandlersDelivery is the scheduler
// stated as its CONSEQUENCE rather than as a call count: a handler that takes
// longer than its lease loses the delivery when nothing is extending it, and
// keeps it when the scheduler is on.
//
// Both arms run against a broker that really takes a delivery back when its
// deadline passes, which is the thing no test in this repository could do
// before: the requirement is that the delivery
// SURVIVES, and "the driver's Extend was called twice" is evidence for that
// only if an extension moves a deadline that something enforces.
func TestRunLeaseSchedulerIsWhatKeepsALongHandlersDelivery(t *testing.T) {
	const (
		lease = 150 * time.Millisecond
		hold  = 3 * lease
		lead  = 100 * time.Millisecond
		sweep = 20 * time.Millisecond
	)

	t.Run("off", func(t *testing.T) {
		cl, b := openFakeWith(t, fakeConfig{extend: true, lease: lease}, "orders")
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		sw := sweepLeases(t, b, "orders", "billing", sweep)
		h := &slowFirstHandler{hold: hold}
		run := startRun(t, c, h)
		// The broker takes the delivery back, and the runner finds out only
		// when its settlement is refused.
		waitFor(t, "the broker to take the lease back", func() bool {
			return b.counts("orders", "billing").requeued >= 1
		})
		waitFor(t, "the settlement of the lost lease to be refused", func() bool {
			return c.Stats().SettleFailures >= 1
		})
		waitFor(t, "the delivery to come back to a handler", func() bool { return h.seen() >= 2 })
		sw.halt()
		if err := run.stop(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
		if got := c.Stats().LeaseExtensions; got != 0 {
			t.Fatalf("LeaseExtensions = %d with no scheduler running, want 0", got)
		}
		h.mu.Lock()
		attempts := append([]uint32(nil), h.attempts...)
		h.mu.Unlock()
		if len(attempts) < 2 || attempts[1] < 2 {
			t.Fatalf("the handler saw attempts %v, want a redelivery of the delivery whose lease expired", attempts)
		}
	})

	t.Run("on", func(t *testing.T) {
		cl, b := openFakeWith(t, fakeConfig{extend: true, lease: lease}, "orders")
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		sw := sweepLeases(t, b, "orders", "billing", sweep)
		h := &slowFirstHandler{hold: hold}
		run := startRun(t, c, h, mq.WithLeaseExtension(lead))
		waitFor(t, "the held delivery to be acked", func() bool {
			return b.counts("orders", "billing").acked == 1
		})
		sw.halt()
		if err := run.stop(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
		counts := b.counts("orders", "billing")
		if counts.requeued != 0 {
			t.Fatalf("the broker took the delivery back %d times while the scheduler was extending its lease", counts.requeued)
		}
		if got := sw.took.Load(); got != 0 {
			t.Fatalf("the sweep expired %d leases the scheduler was keeping alive", got)
		}
		if got := h.seen(); got != 1 {
			t.Fatalf("the handler saw the delivery %d times, want 1: the scheduler did not keep the lease", got)
		}
		if got := c.Stats(); got.LeaseExtensions < 1 || got.SettleFailures != 0 {
			t.Fatalf("stats = %+v, want at least one extension and no failed settlement", got)
		}
		if !counts.conserved() {
			t.Fatalf("counts = %+v, want the conservation", counts)
		}
	})
}

// TestRunLeaseExtensionLeadLongerThanTheLeaseIsNotABusyLoop is the
// arithmetic half. The scheduler schedules the next extension a lead before
// the one it just granted expires, so a lead longer than the lease put that
// moment in the PAST: the run goroutine fired the same ticket immediately,
// for ever, and asked the broker for 828,959 extensions in 300 ms.
//
// A lead longer than the lease is not a misconfiguration the runner can
// refuse, because the lease length is a property of each delivery and is not
// known when Run validates its options: a visibility timeout is per-queue,
// and a driver may report none at all.
//
// The delivery must also still be ALIVE, which is why the sweep runs here
// too. Bounding the rate alone would have been the wrong fix: an interval
// bounded by a constant while the extension still buys less than the lease
// leaves a window the broker can take the message back in.
func TestRunLeaseExtensionLeadLongerThanTheLeaseIsNotABusyLoop(t *testing.T) {
	const window = time.Second
	// The first extension is scheduled at LeaseExpires-lead, so a vast lead
	// puts that moment far in the PAST — an hour before the delivery arrived
	// in the last row. That row is here because a scheduler backed by a
	// hashed timer wheel could plausibly have wrapped such a due time into
	// the future and never extended at all, which would be the same delivery
	// lost by the opposite mistake.
	for _, tc := range []struct {
		name  string
		lease time.Duration
		lead  time.Duration
	}{
		{"a lead four times the lease", 200 * time.Millisecond, 800 * time.Millisecond},
		{"a lead of an hour over a lease of 200ms", 200 * time.Millisecond, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, b := openFakeWith(t, fakeConfig{extend: true, lease: tc.lease}, "orders")
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			sw := sweepLeases(t, b, "orders", "billing", 20*time.Millisecond)
			h := newBlockingHandler(1)
			run := startRun(t, c, h, mq.WithLeaseExtension(tc.lead))
			time.Sleep(window)
			got := b.counts("orders", "billing")
			took := sw.took.Load()
			h.let()
			sw.halt()
			_ = run.stop()
			// The floor is half the lease, so the shape is one extension per
			// half-lease and the expected count is about ten. The ceiling is
			// ten times that, because a loaded host changes the rate and the
			// defect was four orders of magnitude away rather than a factor
			// of two.
			if ceiling := 10 * int(window/(tc.lease/2)); got.extended > ceiling {
				t.Fatalf("the scheduler asked the driver for %d extensions in %v (ceiling %d): a lead longer than the lease spins", got.extended, window, ceiling)
			}
			if got.extended < 1 {
				t.Fatalf("the scheduler asked for %d extensions in %v: a lease shorter than the lead still has to be extended", got.extended, window)
			}
			if took != 0 {
				t.Fatalf("the sweep took back %d leases: bounding the rate is not enough, the extension has to keep the delivery alive", took)
			}
			// And the amount asked for is the lease's own length, never more.
			// The three drivers with a visibility ceiling REFUSE an over-long
			// extension rather than clamping it, so asking for more than the
			// broker granted would turn a kept lease into one that cannot be
			// extended at all.
			if per := got.extendedBy / time.Duration(got.extended); per > tc.lease {
				t.Fatalf("the scheduler asked for %v per extension on a %v lease: a driver with a visibility ceiling refuses that", per, tc.lease)
			}
		})
	}
}

// TestRunLeaseSchedulerStopsExtendingALeaseItHasLost is the failure half of the scheduler, and the answer to "can the handler observe it".
//
// A rebalance takes the delivery away under a running handler, so the
// scheduler's next extension is refused with ErrLeaseLost. Three things must
// then hold, and each is a separate way the scheduler could be wrong:
//
//   - the refusal is COUNTED, as LeaseExtensionFailures — the specification's
//     iqmq_lease_extensions_total{outcome="failure"};
//   - the scheduler does not ask again. A retried extension against a lease
//     the broker has taken back is a busy loop against a broker that is
//     already refusing, which is the same defect from
//     the other direction;
//   - the handler is NOT cancelled  and discovers the loss
//     where the specification puts it, at its own settlement. That is what makes the loss
//     observable at all: the counter, the OnSettle event the extension fires
//     with SettleExtend and an Err, and the settlement's own failure.
func TestRunLeaseSchedulerStopsExtendingALeaseItHasLost(t *testing.T) {
	const lease = 150 * time.Millisecond
	cl, b := openFakeWith(t, fakeConfig{extend: true, lease: lease}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	h := newBlockingHandler(1)
	run := startRun(t, c, h, mq.WithLeaseExtension(100*time.Millisecond))
	waitFor(t, "the lease to be extended at least once", func() bool {
		return c.Stats().LeaseExtensions >= 1
	})
	// The broker takes the delivery back, whatever its lease says.
	b.expire("orders", "billing")
	waitFor(t, "the extension of the lost lease to be refused", func() bool {
		return c.Stats().LeaseExtensionFailures >= 1
	})
	// The handler is still running, which is nothing
	// narrowed its context to a lease.
	if ctxs := len(h.ctxs); ctxs == 0 {
		t.Fatal("the handler never recorded its context")
	}
	failures := c.Stats().LeaseExtensionFailures
	time.Sleep(4 * lease)
	if got := c.Stats().LeaseExtensionFailures; got != failures {
		t.Fatalf("the scheduler asked again after a refusal: %d failures, then %d: a lost lease is not retried", failures, got)
	}
	h.let()
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	// And the handler's own settlement is where the specification reports the loss.
	if got := c.Stats().SettleFailures; got < 1 {
		t.Fatalf("SettleFailures = %d, want the settlement of the lost delivery to have been refused", got)
	}
}

// TestRunLeaseSchedulerHoldsALeaseAcrossAStrictRetryBackoff is the
// interaction the specification names: OrderingStrict holds a failed delivery
// on its lane's worker and calls the handler again after a backoff, so the
// delivery stays in flight at the broker for the whole of it — "a strict
// retry never leaves the process" — and its lease goes on ticking.
//
// With a backoff longer than the lease, the runner is therefore relying on the scheduler for the correctness of the ordering: without it the
// broker takes the message back mid-backoff and the ordered retry is a
// redelivery to whoever gets it next. This is also why DefaultLeaseExtension
// is longer than ADR-011's 30 s backoff cap.
func TestRunLeaseSchedulerHoldsALeaseAcrossAStrictRetryBackoff(t *testing.T) {
	const (
		lease   = 100 * time.Millisecond
		backoff = 250 * time.Millisecond
	)
	cl, b := openFakeWith(t, fakeConfig{extend: true, lease: lease}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishN(t, cl, 1)
	sw := sweepLeases(t, b, "orders", "billing", 20*time.Millisecond)
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			return errors.New("try again")
		}
		return nil
	}}
	run := startRun(t, c, h,
		mq.WithConcurrency(1),
		mq.WithOrderingByKey(),
		mq.WithMaxAttempts(2),
		mq.WithRetryBackoff(mq.ConstantBackoff(backoff)),
		mq.WithLeaseExtension(50*time.Millisecond))
	// Deliberately >=: without the scheduler the count does not stop at two,
	// and a wait for equality would hang on the failure instead of reporting
	// it. The exactness is asserted below, once the runner has stopped.
	waitFor(t, "the retry to be handled", func() bool { return h.count() >= 2 })
	waitFor(t, "the retried delivery to be acked", func() bool {
		return b.counts("orders", "billing").acked == 1
	})
	sw.halt()
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	counts := b.counts("orders", "billing")
	if counts.requeued != 0 {
		t.Fatalf("the broker took the delivery back %d times during a strict retry's backoff ", counts.requeued)
	}
	if got := sw.took.Load(); got != 0 {
		t.Fatalf("the sweep expired %d leases across the backoff: the scheduler did not cover the retry", got)
	}
	if got := c.Stats(); got.LeaseExtensions < 1 || got.SettleFailures != 0 {
		t.Fatalf("stats = %+v, want the lease extended and nothing refused", got)
	}
	// The exactness claims for a strict retry: two attempts,
	// because the process held the delivery and the broker never redelivered
	// it. Without the scheduler this is four — the original, its in-process
	// retry, and two redeliveries of the lease the sweep took back.
	if got := h.count(); got != 2 {
		t.Fatalf("the handler ran %d times for a two-attempt retry: a lease lost mid-backoff makes the attempt count meaningless ", got)
	}
}

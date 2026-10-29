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

// TestRunStrictOrderingRetriesTheMessageAsItWasReceived is the strict retry
// behind a middleware that rewrites the delivery for the rest of the chain, as
// a decrypting or decompressing one does. The retry runs the whole chain
// again, so it has to start from the message the broker delivered: given the
// rewritten one, a decoder that refuses what it cannot decode turned one
// transient failure into a message that could never succeed.
func TestRunStrictOrderingRetriesTheMessageAsItWasReceived(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})
	publishKeyed(t, cl, "acct", 1)
	var mu sync.Mutex
	var bodies []string
	decoding := func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			mu.Lock()
			bodies = append(bodies, string(d.Body))
			mu.Unlock()
			d.Body = []byte("decoded " + string(d.Body))
			return next.Handle(ctx, d)
		})
	}
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			return errors.New("try again")
		}
		return nil
	}}
	run := startRun(t, c, h,
		mq.WithOrderingByKey(),
		mq.WithMiddleware(decoding),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	waitFor(t, "the retry", func() bool { return h.count() == 2 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || bodies[0] != "body" || bodies[1] != "body" {
		t.Fatalf("the chain was given %q, want the received body both times", bodies)
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

// haltWindow is how long TestRunHaltDoesNotFetchBackTheMessageItGaveBack
// gives a broken runner to give the message back while its fetch is held. A
// broken runner does it within microseconds of the halt; a correct one never
// does while the fetch is held, so the window decides how long a passing run
// takes and cannot make one fail.
const haltWindow = 100 * time.Millisecond

// TestRunHaltDoesNotFetchBackTheMessageItGaveBack is the halt against a fetch
// that is already inside the driver when the runner stops.
//
// Cancelling the fetch does not call back a round trip already on its way,
// and a driver's Fetch may answer a cancelled context with whatever is ready:
// the SPI promises that it returns, not that it returns nothing. A runner that
// gave the message back while that fetch was in flight was handed it again,
// gave it back again without handling it, and so charged it one more of the
// broker's attempts for nothing. Under OrderingStopOnFailure that is the
// message that stopped the runner, and it is what made
// TestRunStopOnFailureStopsTheConsumer fail about once in a thousand runs,
// whenever the handler failed between the fetch loop's look at its context
// and the driver's look at its queue. Here the fetch is held in that gap on
// purpose, for both ways a handler halts the runner.
//
// The message's next delivery says what the halt cost it: its second
// attempt, not its third.
func TestRunHaltDoesNotFetchBackTheMessageItGaveBack(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result func(error) error
		opts   []mq.RunOption
	}{
		{"stop on failure", func(err error) error { return err }, []mq.RunOption{mq.WithOrderingByKey(mq.OrderingStopOnFailure)}},
		{"halt directive", mq.Halt, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{}, "orders")
			held := &heldFetch{fakeBroker: b, hold: 2, parked: make(chan struct{}), release: make(chan struct{})}
			cl := mq.NewClient(held)
			t.Cleanup(func() {
				if err := cl.Close(); err != nil {
					t.Errorf("closing the client: %v", err)
				}
			})
			c := consume(t, cl, mq.ConsumerConfig{})
			publishKeyed(t, cl, "acct", 1)
			boom := errors.New("no")
			h := &collector{fn: func(int, *mq.Delivery) error {
				// The failure waits for the next fetch to be inside the
				// driver, so that the halt finds one in flight.
				select {
				case <-held.parked:
				case <-time.After(runWait):
					t.Error("the runner never fetched again")
				}
				return tc.result(boom)
			}}
			run := startRun(t, c, h, tc.opts...)
			// Registered after startRun's cleanup, so it runs before it: a
			// test that fails below still lets the held fetch go, and Run
			// return.
			t.Cleanup(held.let)
			select {
			case <-held.parked:
			case <-time.After(runWait):
				t.Fatal("the runner never fetched again")
			}
			select {
			case <-held.ctx.Done():
			case <-time.After(runWait):
				t.Fatal("the halt never cancelled the fetch")
			}
			deadline := time.Now().Add(haltWindow)
			for time.Now().Before(deadline) {
				if b.counts("orders", "billing").ready > 0 {
					t.Error("the halted runner gave the message back while its own fetch was still in flight")
					break
				}
				time.Sleep(time.Millisecond)
			}
			held.let()
			if err := run.wait(); !errors.Is(err, boom) {
				t.Fatalf("Run = %v, want the halt carrying the handler's error", err)
			}
			if h.count() != 1 {
				t.Fatalf("the handler ran %d times, want 1", h.count())
			}
			if counts := b.counts("orders", "billing"); counts.requeued != 1 || counts.acked != 0 {
				t.Fatalf("counts = %+v, want the message given back once and nothing acked", counts)
			}
			if d := receiveOne(t, c); d.Attempt != 2 {
				t.Fatalf("the message's next delivery is attempt %d, want 2: the halt fetched it back and charged it an attempt", d.Attempt)
			}
		})
	}
}

// heldFetch is the fake broker with one fetch held inside the driver, past
// its look at the context: a round trip already on the wire when the runner
// stops, which comes back with whatever the broker has ready by then.
type heldFetch struct {
	*fakeBroker
	// hold is the call of Fetch that is held, counting from 1.
	hold  int64
	calls atomic.Int64
	// ctx is the held fetch's context, set before parked is closed.
	ctx context.Context
	// parked is closed once the held fetch is inside the driver.
	parked  chan struct{}
	release chan struct{}
	once    sync.Once
}

// OpenSource wraps the fake's source so that its Fetch can be held.
func (b *heldFetch) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	src, err := b.fakeBroker.OpenSource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &heldSource{Source: src, b: b}, nil
}

// let releases the held fetch. It is idempotent.
func (b *heldFetch) let() { b.once.Do(func() { close(b.release) }) }

// heldSource is a fake source whose Fetch the heldFetch holds.
type heldSource struct {
	mq.Source
	b *heldFetch
}

// Fetch holds the chosen call until it is released, whatever its context
// does meanwhile, and then fetches as the fake does.
func (s *heldSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if s.b.calls.Add(1) == s.b.hold {
		s.b.ctx = ctx
		close(s.b.parked)
		<-s.b.release
	}
	return s.Source.Fetch(ctx, dst)
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

// TestRunStrictOrderingPoisonNamesTheAttemptItBoundedBy is the halt's
// *PoisonError after a strictly ordered retry loop. The handler ran three
// times without the message going back, so the broker's count stayed at 1 on
// a broker that counts and at 0 on one that does not; the error used to name
// that count, and so said attempt 1 (or 0) for the delivery that exhausted
// WithMaxAttempts(3). It names the count the bound was applied to, as the
// dead-letter route's iqmq-attempts header does.
func TestRunStrictOrderingPoisonNamesTheAttemptItBoundedBy(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  fakeConfig
	}{
		{"counting", fakeConfig{}},
		{"countless", fakeConfig{countless: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl, _ := openFakeWith(t, tc.cfg, "orders")
			c := consume(t, cl, mq.ConsumerConfig{})
			publishKeyed(t, cl, "acct", 1)
			h := &collector{fn: func(int, *mq.Delivery) error { return errors.New("no") }}
			run := startRun(t, c, h,
				mq.WithOrderingByKey(mq.OrderingStrict),
				mq.WithMaxAttempts(3),
				mq.WithRetryBackoff(mq.ConstantBackoff(0)))
			var pe *mq.PoisonError
			if err := run.wait(); !errors.As(err, &pe) {
				t.Fatalf("Run = %v, want a *PoisonError once the attempts ran out", err)
			}
			if h.count() != 3 {
				t.Fatalf("the handler ran %d times, want 3", h.count())
			}
			if pe.Attempt != 3 {
				t.Fatalf("PoisonError.Attempt = %d, want 3: the handler's third attempt is the one that failed", pe.Attempt)
			}
		})
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

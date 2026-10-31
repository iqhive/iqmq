package iqmq_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
)

// This file is the shutdown order for a client, which is the order the
// whole project keeps getting wrong: "stop accepting publications → stop
// fetching → flush admitted async publications → wait for in-flight handlers
// within DrainTimeout → settle/requeue/let leases expire per policy → commit
// safe offset boundaries → close native clients".
//
// The fake broker's event log is what makes an ORDER checkable at all: a test
// that only asserted the end state would pass for a client that closed the
// backend first and cleaned up afterwards.

// indexOfEvent is the first position of name in the log, or -1.
func indexOfEvent(log []string, name string) int { return slices.Index(log, name) }

// lastIndexOfEvent is the last position of name in the log, or -1.
func lastIndexOfEvent(log []string, name string) int {
	last := -1
	for i, e := range log {
		if e == name {
			last = i
		}
	}
	return last
}

// TestClientShutdownClosesInOrder is end to end: a handler holds a
// delivery while the client is shut down, and every step happens in the order
// the specification names — the settlement leaves the driver, the consumer's
// source closes, the publisher's sink closes, and the backend closes last.
func TestClientShutdownClosesInOrder(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders")
	cl := mq.NewClient(b)
	p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	if err := p.Publish(context.Background(), &mq.Message{ID: "1", Body: []byte("b")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
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

	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), runWait)
		defer cancel()
		shutdown <- cl.Shutdown(ctx)
	}()
	// The shutdown must be waiting for the handler: the specification drains in-flight
	// handlers rather than interrupting them.
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned %v while a handler still held a delivery", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	case <-time.After(runWait):
		t.Fatal("Shutdown never returned")
	}
	if err := <-done; !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Run = %v, want ErrClosed", err)
	}

	log := b.eventLog()
	settle := lastIndexOfEvent(log, "settle-leave")
	sink := indexOfEvent(log, "sink-close")
	source := indexOfEvent(log, "source-close")
	backend := indexOfEvent(log, "backend-close")
	switch {
	case settle < 0 || sink < 0 || source < 0 || backend < 0:
		t.Fatalf("the event log is %v, want a settlement, a sink close, a source close and a backend close", log)
	case settle > source:
		t.Fatalf("the source closed before the settlement left the driver: %v", log)
	case sink < source:
		t.Fatalf("the publisher closed before the consumer drained: a draining handler may still publish: %v", log)
	case backend < source || backend < sink:
		t.Fatalf("the backend closed before its handles: the specification closes native clients last: %v", log)
	}
	if got := c.Stats().InFlight; got != 0 {
		t.Fatalf("InFlight = %d after the shutdown, want 0", got)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 1 || !counts.conserved() {
		t.Fatalf("counts = %+v, want the held delivery acked and conserved", counts)
	}
}

// TestClientCloseClosesTheBackendLast is the prompt half of the same rule:
// Close waits for nothing, but it still closes the backend after the handles
// that are using it.
func TestClientCloseClosesTheBackendLast(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders")
	cl := mq.NewClient(b)
	if _, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"}); err != nil {
		t.Fatalf("opening the publisher: %v", err)
	}
	if _, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	log := b.eventLog()
	sink := indexOfEvent(log, "sink-close")
	source := indexOfEvent(log, "source-close")
	backend := indexOfEvent(log, "backend-close")
	if sink < 0 || source < 0 || backend < 0 {
		t.Fatalf("the event log is %v, want every handle and the backend closed", log)
	}
	if backend < sink || backend < source {
		t.Fatalf("the backend closed before its handles: %v", log)
	}
	// Idempotent, and a second close does not close the backend twice.
	if err := cl.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if n := len(b.eventLog()) - len(log); n != 0 {
		t.Fatalf("a second Close produced %d more driver events, want 0", n)
	}
}

// TestRunnerClosesTheDeadLetterPublisherAfterTheHandlersDrained is the
// runner's own half of the order: a handler still draining may need to
// dead-letter its delivery, so the dead-letter publisher outlives the drain.
func TestRunnerClosesTheDeadLetterPublisherAfterTheHandlersDrained(t *testing.T) {
	cl, b := openFakeWith(t, fakeConfig{}, "orders", "dead")
	c := consume(t, cl, mq.ConsumerConfig{})
	// A subscription on the dead-letter topic, because the fake broker
	// counts a publication against the subscriptions that will receive it.
	consumeOn(t, cl, "dead", "audit")
	publishN(t, cl, 1)
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			close(entered)
			// The runner's context is cancelled while this handler runs, so
			// the dead-letter publication below happens during the drain.
			<-ctx.Done()
			return mq.DeadLetter(errors.New("during the drain"))
		}), mq.WithDeadLetter("dead"))
	}()
	select {
	case <-entered:
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(runWait):
		t.Fatal("Run never returned")
	}
	if got := b.counts("dead", "audit").published; got != 1 {
		t.Fatalf("the dead-letter topic holds %d messages, want 1: the publisher closed before the handler drained", got)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 1 {
		t.Fatalf("counts = %+v, want the dead-lettered original acked", counts)
	}
}

// TestClientShutdownKeepsTheDeadLetterPublisherOpenForTheDrain is the client's
// half of the same rule: the runner's dead-letter publisher is one of the
// client's children, and Client.Shutdown used to close it before the
// consumers drained, so a handler dead-lettering during the drain was refused
// with ErrClosed and its delivery requeued.
func TestClientShutdownKeepsTheDeadLetterPublisherOpenForTheDrain(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders", "dead")
	cl := mq.NewClient(b)
	c := consume(t, cl, mq.ConsumerConfig{})
	// The dead-letter topic's subscription is another process's. The
	// client's own consumers all close as its drain begins, and the fake
	// broker stops counting publications against a subscription whose
	// source has closed.
	audit := mq.NewClient(b)
	defer func() { _ = audit.Close() }()
	consumeOn(t, audit, "dead", "audit")
	publishN(t, cl, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			close(entered)
			<-release
			return mq.DeadLetter(errors.New("during the drain"))
		}), mq.WithDeadLetter("dead"))
	}()
	select {
	case <-entered:
	case <-time.After(runWait):
		t.Fatal("the handler never ran")
	}
	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), runWait)
		defer cancel()
		shutdown <- cl.Shutdown(ctx)
	}()
	// Give the shutdown time to reach the drain, where the bug closed the
	// dead-letter publisher, before the handler dead-letters.
	time.Sleep(100 * time.Millisecond)
	close(release)
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	case <-time.After(runWait):
		t.Fatal("Shutdown never returned")
	}
	if err := <-done; !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Run = %v, want ErrClosed", err)
	}
	if got := b.counts("dead", "audit").published; got != 1 {
		t.Fatalf("the dead-letter topic holds %d messages, want 1: the client closed the dead-letter publisher before the handler drained", got)
	}
	if counts := b.counts("orders", "billing"); counts.acked != 1 {
		t.Fatalf("counts = %+v, want the dead-lettered original acked", counts)
	}
}

// TestClientShutdownIsIdempotentUnderConcurrency is the specification's "idempotent,
// concurrency-safe": four goroutines closing at once observe one outcome and
// the driver is closed once.
func TestClientShutdownIsIdempotentUnderConcurrency(t *testing.T) {
	b := newFakeBroker(fakeConfig{}, "orders")
	cl := mq.NewClient(b)
	if _, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	errs := make(chan error, 4)
	for i := range 4 {
		go func() {
			if i%2 == 0 {
				ctx, cancel := context.WithTimeout(context.Background(), runWait)
				defer cancel()
				errs <- cl.Shutdown(ctx)
				return
			}
			errs <- cl.Close()
		}()
	}
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatalf("a concurrent close reported %v, want nil", err)
		}
	}
	log := b.eventLog()
	n := 0
	for _, e := range log {
		if e == "backend-close" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the backend was closed %d times, want 1: %v", n, log)
	}
}

// hangSink is a driver whose broker never answers: a publication parks until
// its context ends or the sink is closed, which is what a native client does
// when the connection under it is torn down.
type hangSink struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (s *hangSink) Publish(ctx context.Context, _ *mq.Message, _ *mq.PublishOptions) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return mq.ErrClosed
	}
}

func (s *hangSink) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

// TestCloseReturnsWhileAPublishIsParkedInTheDriver is "Close never waits
// indefinitely" for a publisher: a synchronous Publish whose broker never
// answers holds its admission ticket, and a prompt Close must still return
// rather than wait for that ticket before the sink close that would release
// it.
func TestCloseReturnsWhileAPublishIsParkedInTheDriver(t *testing.T) {
	for _, closer := range []string{"publisher", "client"} {
		t.Run(closer, func(t *testing.T) {
			sink := &hangSink{entered: make(chan struct{}, 1), closed: make(chan struct{})}
			b := newAdvBroker(func(*advBroker, mq.SinkConfig) mq.Sink { return sink })
			cl := mq.NewClient(b)
			p, err := cl.Publisher(context.Background(), mq.PublisherConfig{Topic: "orders"})
			if err != nil {
				t.Fatalf("opening the publisher: %v", err)
			}
			published := make(chan error, 1)
			go func() {
				published <- p.Publish(context.Background(), &mq.Message{Body: []byte("x")})
			}()
			select {
			case <-sink.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the publication never reached the driver")
			}
			closed := make(chan error, 1)
			go func() {
				if closer == "client" {
					closed <- cl.Close()
					return
				}
				closed <- p.Close()
			}()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(5 * time.Second):
				// Release the driver so the test's goroutines end.
				_ = sink.Close()
				t.Fatal("Close did not return while a publication was parked in the driver")
			}
			select {
			case err := <-published:
				if err == nil {
					t.Fatal("the parked publication reported success after Close")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the parked publication never returned")
			}
			_ = cl.Close()
		})
	}
}

// --- the drain's bound ---------------------------------------------------
//
// The tests below run in a testing/synctest bubble, because what they check
// is WHEN things happen: when Shutdown returns and when a handler's context
// is cancelled, measured against the run's drain timeout and the deadline of
// the context Shutdown was given.

// drainingShutdowns are the two graceful shutdowns that drain a runner: the
// consumer's own, and the client's, which drains every consumer it opened.
var drainingShutdowns = []struct {
	name     string
	shutdown func(ctx context.Context, cl *mq.Client, c *mq.ConsumerHandle) error
}{
	{"consumer", func(ctx context.Context, _ *mq.Client, c *mq.ConsumerHandle) error { return c.Shutdown(ctx) }},
	{"client", func(ctx context.Context, cl *mq.Client, _ *mq.ConsumerHandle) error { return cl.Shutdown(ctx) }},
}

// cancelledAfter is a handler that waits for its context and reports how
// long after start the context was cancelled. It returns the context's
// error, so the runner gives its delivery back.
func cancelledAfter(start *time.Time, entered chan<- struct{}, after chan<- time.Duration) mq.Handler {
	return mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
		entered <- struct{}{}
		<-ctx.Done()
		after <- time.Since(*start)
		return ctx.Err()
	})
}

// TestShutdownLetsAHandlerFinishWithinTheRunsDrainTimeout is WithDrainTimeout
// as the shutdown's bound: a handler the run allows a minute gets a minute.
// Shutdown used to stop waiting at the lifecycle's own 30 seconds whatever
// the run said, and close the source under the handler, so the ack it made
// at 45 seconds was refused and the message it had processed was delivered
// again.
func TestShutdownLetsAHandlerFinishWithinTheRunsDrainTimeout(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				entered := make(chan struct{})
				acked := make(chan error, 1)
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
						close(entered)
						select {
						case <-time.After(45 * time.Second):
						case <-ctx.Done():
						}
						acked <- d.Ack()
						return nil
					}), mq.WithDrainTimeout(time.Minute))
				}()
				<-entered
				start := time.Now()
				err := s.shutdown(context.Background(), cl, c)
				if elapsed := time.Since(start); err != nil || elapsed != 45*time.Second {
					t.Errorf("Shutdown = %v after %v, want nil once the handler finished at 45s", err, elapsed)
				}
				if err := <-acked; err != nil {
					t.Errorf("the handler's Ack = %v, want nil: the source closed under it", err)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message acked once and never requeued", counts)
				}
			})
		})
	}
}

// TestShutdownCancelsTheHandlersAtTheRunsDrainTimeout is the other edge of
// the same bound: a handler still running when the run's drain timeout
// passes has its context cancelled then, and Shutdown reports the timeout.
// It used to report it at 30 seconds, with the handler still running under a
// live context for another 30.
func TestShutdownCancelsTheHandlersAtTheRunsDrainTimeout(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				var start time.Time
				entered := make(chan struct{}, 1)
				after := make(chan time.Duration, 1)
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), cancelledAfter(&start, entered, after), mq.WithDrainTimeout(time.Minute))
				}()
				<-entered
				start = time.Now()
				err := s.shutdown(context.Background(), cl, c)
				if elapsed := time.Since(start); mq.CodeOf(err) != mq.CodeDeadlineExceeded || elapsed != time.Minute {
					t.Errorf("Shutdown = %v after %v, want the drain timeout reported at 1m0s", err, elapsed)
				}
				if got := <-after; got != time.Minute {
					t.Errorf("the handler's context was cancelled after %v, want 1m0s", got)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				// Shutdown returned at its bound, and the close goes on behind
				// it: the cancelled handler's nack may still have been inside
				// the driver. Close waits for that close to finish.
				_ = c.Close()
				if counts := b.counts("orders", "billing"); counts.ready != 1 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message given back", counts)
				}
			})
		})
	}
}

// TestShutdownAbandonsAHandlerThatIgnoresItsContext is the drain timeout as a
// bound on Shutdown itself: a handler that does not stop when its context is
// cancelled is abandoned there, its delivery goes back to the broker, and
// Shutdown returns rather than wait for it. Run still waits for it.
func TestShutdownAbandonsAHandlerThatIgnoresItsContext(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				entered := make(chan struct{})
				acked := make(chan error, 1)
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
						close(entered)
						time.Sleep(90 * time.Second)
						acked <- d.Ack()
						return nil
					}), mq.WithDrainTimeout(time.Minute))
				}()
				<-entered
				start := time.Now()
				err := s.shutdown(context.Background(), cl, c)
				if elapsed := time.Since(start); mq.CodeOf(err) != mq.CodeDeadlineExceeded || elapsed != time.Minute {
					t.Errorf("Shutdown = %v after %v, want the drain timeout reported at 1m0s", err, elapsed)
				}
				if err := <-acked; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("the abandoned handler's Ack = %v, want ErrClosed", err)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				if elapsed := time.Since(start); elapsed != 90*time.Second {
					t.Errorf("Run returned after %v, want 1m30s, when the handler did", elapsed)
				}
				if counts := b.counts("orders", "billing"); counts.requeued != 1 || counts.ready != 1 || !counts.conserved() {
					t.Errorf("counts = %+v, want the abandoned delivery requeued", counts)
				}
			})
		})
	}
}

// TestShutdownDeadlineCancelsTheHandlersContexts is "or earlier, if you passed
// Shutdown a context with a nearer deadline": when the caller's deadline
// passes first, the source closes and the delivery goes back, so the handler
// must be told then. Its context used to stay live until the run's own drain
// timeout, half a minute later, while it worked on a message already
// requeued.
func TestShutdownDeadlineCancelsTheHandlersContexts(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				var start time.Time
				entered := make(chan struct{}, 1)
				after := make(chan time.Duration, 1)
				done := make(chan error, 1)
				go func() { done <- c.Run(context.Background(), cancelledAfter(&start, entered, after)) }()
				<-entered
				start = time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err := s.shutdown(ctx, cl, c)
				if elapsed := time.Since(start); !errors.Is(err, context.DeadlineExceeded) || elapsed != time.Second {
					t.Errorf("Shutdown = %v after %v, want context.DeadlineExceeded at 1s", err, elapsed)
				}
				if got := <-after; got != time.Second {
					t.Errorf("the handler's context was cancelled after %v, want 1s, when Shutdown gave up on it", got)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				// Shutdown returned at the caller's deadline, and the close
				// goes on behind it: the cancelled handler's nack may still
				// have been inside the driver. Close waits for that close to
				// finish.
				_ = c.Close()
				if counts := b.counts("orders", "billing"); counts.ready != 1 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message given back", counts)
				}
			})
		})
	}
}

// TestClientShutdownStopsEveryConsumerFetchingFirst is the order for a client
// with more than one consumer: every consumer stops fetching as the drain
// begins, before any of them is waited for, so the set to drain is finite.
// The client used to shut its consumers down one at a time, and a consumer
// whose turn had not come kept fetching, here a message that arrived while
// another consumer drained.
func TestClientShutdownStopsEveryConsumerFetchingFirst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker(fakeConfig{lease: -1}, "orders", "refunds")
		cl := mq.NewClient(b)
		defer func() { _ = cl.Close() }()
		orders := consume(t, cl, mq.ConsumerConfig{})
		refunds := consumeOn(t, cl, "refunds", "billing")
		publishN(t, cl, 1)
		entered := make(chan struct{})
		ordersDone := make(chan error, 1)
		go func() {
			ordersDone <- orders.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
				close(entered)
				time.Sleep(25 * time.Second)
				return nil
			}))
		}()
		handled := make(chan string, 1)
		refundsDone := make(chan error, 1)
		go func() {
			refundsDone <- refunds.Run(context.Background(), mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				handled <- d.ID
				return nil
			}))
		}()
		<-entered
		start := time.Now()
		shutdown := make(chan error, 1)
		go func() { shutdown <- cl.Shutdown(context.Background()) }()
		// Another process publishes a refund while the orders consumer
		// drains, through a client of its own: this one is closing.
		time.Sleep(10 * time.Second)
		elsewhere := mq.NewClient(b)
		defer func() { _ = elsewhere.Close() }()
		if err := elsewhere.Publish(context.Background(), "refunds", &mq.Message{ID: "late", Body: []byte("b")}); err != nil {
			t.Fatalf("publishing the refund: %v", err)
		}
		if err := <-shutdown; err != nil {
			t.Errorf("Shutdown = %v, want nil", err)
		}
		if elapsed := time.Since(start); elapsed != 25*time.Second {
			t.Errorf("Shutdown took %v, want 25s, the orders handler's time", elapsed)
		}
		select {
		case id := <-handled:
			t.Errorf("the refunds consumer handled %q after the client began shutting down", id)
		default:
		}
		for name, done := range map[string]chan error{"orders": ordersDone, "refunds": refundsDone} {
			if err := <-done; !errors.Is(err, mq.ErrClosed) {
				t.Errorf("the %s Run = %v, want ErrClosed", name, err)
			}
		}
		if counts := b.counts("refunds", "billing"); counts.acked != 0 || counts.requeued != 0 || counts.inflight != 0 {
			t.Errorf("refunds counts = %+v, want the late refund never fetched", counts)
		}
	})
}

// TestClientShutdownDrainsItsConsumersSideBySide is the bound a client's
// consumers drain under: one, the same for each, so every handler still
// running at its end is cancelled then. Draining them one after another, the
// second consumer's drain began only once the first had used up the bound,
// and the source closed under its handler while the handler's context stayed
// live for a whole drain timeout more.
func TestClientShutdownDrainsItsConsumersSideBySide(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker(fakeConfig{lease: -1}, "orders", "refunds")
		cl := mq.NewClient(b)
		defer func() { _ = cl.Close() }()
		consumers := []*mq.ConsumerHandle{
			consume(t, cl, mq.ConsumerConfig{}),
			consumeOn(t, cl, "refunds", "billing"),
		}
		publishN(t, cl, 1)
		if err := cl.Publish(context.Background(), "refunds", &mq.Message{ID: "r", Body: []byte("b")}); err != nil {
			t.Fatalf("publishing the refund: %v", err)
		}
		var start time.Time
		entered := make(chan struct{}, len(consumers))
		after := make(chan time.Duration, len(consumers))
		done := make(chan error, len(consumers))
		for _, c := range consumers {
			go func() { done <- c.Run(context.Background(), cancelledAfter(&start, entered, after)) }()
		}
		for range consumers {
			<-entered
		}
		start = time.Now()
		err := cl.Shutdown(context.Background())
		if elapsed := time.Since(start); mq.CodeOf(err) != mq.CodeDeadlineExceeded || elapsed != mq.DefaultDrainTimeout {
			t.Errorf("Shutdown = %v after %v, want the drain timeout reported after one DefaultDrainTimeout", err, elapsed)
		}
		for range consumers {
			if got := <-after; got != mq.DefaultDrainTimeout {
				t.Errorf("a handler's context was cancelled after %v, want %v for each consumer", got, mq.DefaultDrainTimeout)
			}
			if err := <-done; !errors.Is(err, mq.ErrClosed) {
				t.Errorf("Run = %v, want ErrClosed", err)
			}
		}
	})
}

// promptCloses are the two prompt closes that close a consumer's source under
// its runner: the consumer's own, and the client's, which closes every
// consumer it opened.
var promptCloses = []struct {
	name  string
	close func(cl *mq.Client, c *mq.ConsumerHandle) error
}{
	{"consumer", func(_ *mq.Client, c *mq.ConsumerHandle) error { return c.Close() }},
	{"client", func(cl *mq.Client, _ *mq.ConsumerHandle) error { return cl.Close() }},
}

// TestCloseCancelsTheHandlersContexts is the prompt close's half of
// TestShutdownDeadlineCancelsTheHandlersContexts: Close waits for no handler,
// so the source closes under every one still running and its delivery goes
// back, and the handler must be told then.
// Its context used to stay live for the run's whole drain timeout, here ten
// minutes, while it worked on a message the broker already had back.
func TestCloseCancelsTheHandlersContexts(t *testing.T) {
	for _, s := range promptCloses {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				var start time.Time
				entered := make(chan struct{}, 1)
				after := make(chan time.Duration, 1)
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), cancelledAfter(&start, entered, after), mq.WithDrainTimeout(10*time.Minute))
				}()
				<-entered
				start = time.Now()
				if err := s.close(cl, c); err != nil {
					t.Errorf("Close = %v, want nil", err)
				}
				if got := <-after; got != 0 {
					t.Errorf("the handler's context was cancelled after %v, want at once, when Close closed the source under it", got)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				if counts := b.counts("orders", "billing"); counts.ready != 1 || counts.requeued != 1 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message given back", counts)
				}
			})
		})
	}
}

// TestCloseDoesNotCountAHandlersCancellationAsAFailure is a handler that
// returns its context's error when Close cancels it, under the options that
// stop a runner on a failure: OrderingStopOnFailure, a delivery on its last
// attempt, and a strict retry with no backoff. The close cut the handler
// short, so the runner gives the delivery back and Run reports ErrClosed. It
// used to count the cancellation against the message and return a
// *PoisonError naming a message nothing was wrong with, and the strict run
// called the handler again, once for each attempt left, on a message the
// broker already had back.
func TestCloseDoesNotCountAHandlersCancellationAsAFailure(t *testing.T) {
	options := []struct {
		name string
		opts []mq.RunOption
	}{
		{"StopOnFailure", []mq.RunOption{mq.WithOrderingByKey(mq.OrderingStopOnFailure)}},
		{"LastAttempt", []mq.RunOption{mq.WithMaxAttempts(1)}},
		{"StrictWithNoBackoff", []mq.RunOption{mq.WithOrderingByKey(), mq.WithRetryBackoff(mq.ConstantBackoff(0)), mq.WithMaxAttempts(5)}},
	}
	for _, s := range promptCloses {
		for _, o := range options {
			t.Run(s.name+"/"+o.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := newFakeBroker(fakeConfig{lease: -1}, "orders")
					cl := mq.NewClient(b)
					defer func() { _ = cl.Close() }()
					c := consume(t, cl, mq.ConsumerConfig{})
					publishN(t, cl, 1)
					var calls atomic.Int64
					entered := make(chan struct{}, 8)
					done := make(chan error, 1)
					go func() {
						done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
							calls.Add(1)
							entered <- struct{}{}
							<-ctx.Done()
							return ctx.Err()
						}), o.opts...)
					}()
					<-entered
					if err := s.close(cl, c); err != nil {
						t.Errorf("Close = %v, want nil", err)
					}
					err := <-done
					var pe *mq.PoisonError
					if errors.As(err, &pe) || !errors.Is(err, mq.ErrClosed) {
						t.Errorf("Run = %v, want ErrClosed: the close cancelled the handler", err)
					}
					if got := calls.Load(); got != 1 {
						t.Errorf("the handler was called %d times, want once", got)
					}
					if counts := b.counts("orders", "billing"); counts.ready != 1 || !counts.conserved() {
						t.Errorf("counts = %+v, want the message given back", counts)
					}
				})
			})
		}
	}
}

// TestShutdownLetsTheSettlementsInFlightFinish is a graceful Shutdown of a
// consumer driven by Receive while the caller's Ack is inside the driver.
// Shutdown waits for it, as its documentation says. It used to cancel the
// context of every settlement in flight before it waited for any, and with no
// runner nothing had waited before that, so the Ack was refused with
// context.Canceled and the message the caller had processed was delivered
// again.
func TestShutdownLetsTheSettlementsInFlightFinish(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				d := receiveOne(t, c)
				g := &settleGate{entered: make(chan uint64, 1), release: make(chan struct{})}
				b.setGate(g)
				acked := make(chan error, 1)
				go func() { acked <- d.Ack() }()
				<-g.entered
				shut := make(chan error, 1)
				go func() { shut <- s.shutdown(context.Background(), cl, c) }()
				// The broker answers the ack a second after Shutdown began.
				time.Sleep(time.Second)
				close(g.release)
				if err := <-acked; err != nil {
					t.Errorf("the Ack in flight as Shutdown began = %v, want nil", err)
				}
				if err := <-shut; err != nil {
					t.Errorf("Shutdown = %v, want nil", err)
				}
				if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message acked once and never requeued", counts)
				}
			})
		})
	}
}

// TestShutdownReportsTheBoundThatCutASettlementShort is a graceful Shutdown
// whose bound passes while a settlement is still inside the driver. The
// settlement is cancelled there and the source closes under it, so the
// shutdown did not finish within its bound, and every caller is told so: the
// Shutdown and a Close after it. The run that finished as the bound passed
// used to report nil, and Shutdown returned that or the bound at random.
func TestShutdownReportsTheBoundThatCutASettlementShort(t *testing.T) {
	bounds := []struct {
		name  string
		ctx   func() (context.Context, context.CancelFunc)
		after time.Duration
	}{
		{"CallersDeadline", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 2*time.Second)
		}, 2 * time.Second},
		{"DrainTimeout", func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}, mq.DefaultDrainTimeout},
	}
	for _, s := range drainingShutdowns {
		for _, bound := range bounds {
			t.Run(s.name+"/"+bound.name, func(t *testing.T) {
				// The run's answer and the caller's bound used to race, so
				// one round proves little either way.
				for round := range 20 {
					synctest.Test(t, func(t *testing.T) {
						b := newFakeBroker(fakeConfig{lease: -1}, "orders")
						cl := mq.NewClient(b)
						defer func() { _ = cl.Close() }()
						c := consume(t, cl, mq.ConsumerConfig{})
						publishN(t, cl, 1)
						d := receiveOne(t, c)
						g := &settleGate{entered: make(chan uint64, 1), release: make(chan struct{})}
						b.setGate(g)
						acked := make(chan error, 1)
						go func() { acked <- d.Ack() }()
						<-g.entered
						ctx, cancel := bound.ctx()
						defer cancel()
						start := time.Now()
						err := s.shutdown(ctx, cl, c)
						if elapsed := time.Since(start); mq.CodeOf(err) != mq.CodeDeadlineExceeded || elapsed != bound.after {
							t.Errorf("round %d: Shutdown = %v after %v, want the bound reported at %v", round, err, elapsed, bound.after)
						}
						if err := <-acked; !errors.Is(err, context.Canceled) {
							t.Errorf("round %d: the Ack still inside the driver at the bound = %v, want context.Canceled", round, err)
						}
						// The consumer's run reports the bound it was given:
						// its own, or under a client's Shutdown the client's
						// context, which the client's bound cancels.
						if err := c.Close(); err == nil {
							t.Errorf("round %d: Close after the Shutdown = nil, want the bound reported", round)
						}
					})
				}
			})
		}
	}
}

// TestCloseBehindAShutdownIsStillPrompt is Close called while a graceful
// Shutdown waits, for a settlement inside the driver or for a handler. Close
// is the prompt one: it cancels both at once and returns, and the Shutdown
// ends with it. It used to join the Shutdown and wait for it, up to the
// Shutdown's whole bound.
func TestCloseBehindAShutdownIsStillPrompt(t *testing.T) {
	t.Run("SettlementInFlight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{lease: -1}, "orders")
			cl := mq.NewClient(b)
			defer func() { _ = cl.Close() }()
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			d := receiveOne(t, c)
			g := &settleGate{entered: make(chan uint64, 1), release: make(chan struct{})}
			b.setGate(g)
			acked := make(chan error, 1)
			go func() { acked <- d.Ack() }()
			<-g.entered
			shut := make(chan error, 1)
			go func() { shut <- c.Shutdown(context.Background()) }()
			time.Sleep(time.Second)
			start := time.Now()
			_ = c.Close()
			if elapsed := time.Since(start); elapsed != 0 {
				t.Errorf("Close behind the Shutdown returned after %v, want at once", elapsed)
			}
			if err := <-acked; !errors.Is(err, context.Canceled) {
				t.Errorf("the Ack inside the driver = %v, want context.Canceled by the Close", err)
			}
			// The Shutdown ends with the Close that overtook it, before its
			// own bound, so it has no bound to report.
			if err := <-shut; err != nil {
				t.Errorf("Shutdown = %v, want nil", err)
			}
			if counts := b.counts("orders", "billing"); counts.ready != 1 || !counts.conserved() {
				t.Errorf("counts = %+v, want the message given back", counts)
			}
		})
	})
	// The handler reports when its context ended, and the test measures that
	// against its own clock reading: nothing the test writes is read by the
	// handler, so a Close that does not cut it fails the assertions and not
	// the race detector.
	cancelledAt := func(entered chan<- struct{}, at chan<- time.Time) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
			entered <- struct{}{}
			<-ctx.Done()
			at <- time.Now()
			return ctx.Err()
		})
	}
	t.Run("HandlerInFlight", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{lease: -1}, "orders")
			cl := mq.NewClient(b)
			defer func() { _ = cl.Close() }()
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			entered := make(chan struct{}, 1)
			at := make(chan time.Time, 1)
			done := make(chan error, 1)
			go func() {
				done <- c.Run(context.Background(), cancelledAt(entered, at), mq.WithDrainTimeout(10*time.Minute))
			}()
			<-entered
			shut := make(chan error, 1)
			go func() { shut <- c.Shutdown(context.Background()) }()
			time.Sleep(time.Second)
			start := time.Now()
			_ = c.Close()
			if elapsed := time.Since(start); elapsed != 0 {
				t.Errorf("Close behind the Shutdown returned after %v, want at once", elapsed)
			}
			if got := (<-at).Sub(start); got != 0 {
				t.Errorf("the handler's context was cancelled %v after the Close, want at once", got)
			}
			if err := <-done; !errors.Is(err, mq.ErrClosed) {
				t.Errorf("Run = %v, want ErrClosed", err)
			}
			if err := <-shut; err != nil {
				t.Errorf("Shutdown = %v, want nil", err)
			}
			if counts := b.counts("orders", "billing"); counts.ready != 1 || !counts.conserved() {
				t.Errorf("counts = %+v, want the message given back", counts)
			}
		})
	})
	// A handler that calls Shutdown while one begun elsewhere is under way
	// only joins it, and that Shutdown has a caller waiting for it, so a
	// Close still cuts it short. Only a Shutdown a handler began makes a
	// Close wait. A handler that merely joined used to be taken for the one
	// that began it, and the Close waited for the whole drain timeout.
	t.Run("HandlerJoinedTheShutdown", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{lease: -1}, "orders")
			cl := mq.NewClient(b)
			defer func() { _ = cl.Close() }()
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			entered := make(chan struct{})
			join := make(chan struct{})
			joined := make(chan error, 1)
			done := make(chan error, 1)
			go func() {
				done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
					close(entered)
					<-join
					joined <- c.Shutdown(ctx)
					<-ctx.Done()
					return ctx.Err()
				}), mq.WithDrainTimeout(10*time.Minute))
			}()
			<-entered
			shut := make(chan error, 1)
			go func() { shut <- c.Shutdown(context.Background()) }()
			time.Sleep(time.Second)
			close(join)
			if err := <-joined; err != nil {
				t.Errorf("the handler's Shutdown = %v, want nil", err)
			}
			start := time.Now()
			_ = c.Close()
			if elapsed := time.Since(start); elapsed != 0 {
				t.Errorf("Close behind a Shutdown the handler only joined returned after %v, want at once", elapsed)
			}
			if err := <-done; !errors.Is(err, mq.ErrClosed) {
				t.Errorf("Run = %v, want ErrClosed", err)
			}
			if err := <-shut; err != nil {
				t.Errorf("Shutdown = %v, want nil", err)
			}
		})
	})
}

// TestCloseAfterACutShutdownNamesTheBound is a Close after a graceful
// Shutdown that the drain timeout cut short while an Ack was inside the
// driver, from a caller that cancels its context as Shutdown returns, as a
// deferred cancel does. The Close reports the drain timeout. It used to
// report that later cancellation about one time in six, because the run
// named its bound only once the Ack had left.
func TestCloseAfterACutShutdownNamesTheBound(t *testing.T) {
	for round := range 40 {
		synctest.Test(t, func(t *testing.T) {
			b := newFakeBroker(fakeConfig{lease: -1}, "orders")
			cl := mq.NewClient(b)
			defer func() { _ = cl.Close() }()
			c := consume(t, cl, mq.ConsumerConfig{})
			publishN(t, cl, 1)
			d := receiveOne(t, c)
			g := &settleGate{entered: make(chan uint64, 1), release: make(chan struct{})}
			b.setGate(g)
			acked := make(chan error, 1)
			go func() { acked <- d.Ack() }()
			<-g.entered
			ctx, cancel := context.WithCancel(context.Background())
			err := c.Shutdown(ctx)
			cancel()
			if mq.CodeOf(err) != mq.CodeDeadlineExceeded {
				t.Errorf("round %d: Shutdown = %v, want the drain timeout", round, err)
			}
			if err := <-acked; !errors.Is(err, context.Canceled) {
				t.Errorf("round %d: the Ack cut short at the bound = %v, want context.Canceled", round, err)
			}
			if err := c.Close(); mq.CodeOf(err) != mq.CodeDeadlineExceeded || errors.Is(err, context.Canceled) {
				t.Errorf("round %d: Close after the cut Shutdown = %v, want the drain timeout", round, err)
			}
		})
	}
}

// TestAJoiningShutdownIsNotToldTheBeginnersCancellation is two Shutdowns of
// a consumer whose handler is in flight. A begins the shutdown with a
// context it cancels while the drain waits for the handler; B joins with a
// live context. The drain step returned A's cancellation, and B was told it
// too: only a barrier's cut was kept from a joiner, and the drain is the
// usual way a beginner's context ends a graceful shutdown. A client's
// Shutdown is the same, through the outcome its consumer reports to it.
func TestAJoiningShutdownIsNotToldTheBeginnersCancellation(t *testing.T) {
	for _, of := range []string{"consumer", "client"} {
		t.Run(of, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				shutdown := c.Shutdown
				if of == "client" {
					shutdown = cl.Shutdown
				}
				publishN(t, cl, 1)
				entered := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
						close(entered)
						<-ctx.Done()
						return ctx.Err()
					}), mq.WithDrainTimeout(10*time.Minute))
				}()
				<-entered
				ctxA, cancelA := context.WithCancel(context.Background())
				began := make(chan error, 1)
				go func() { began <- shutdown(ctxA) }()
				synctest.Wait()
				joined := make(chan error, 1)
				go func() { joined <- shutdown(context.Background()) }()
				synctest.Wait()
				time.Sleep(time.Second)
				cancelA()
				if err := <-began; !errors.Is(err, context.Canceled) {
					t.Errorf("Shutdown A, which began the shutdown and cancelled its context, = %v, want context.Canceled", err)
				}
				if err := <-joined; errors.Is(err, context.Canceled) {
					t.Errorf("Shutdown B, which joined with a live context, = %v: it was told the beginner's cancellation", err)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
			})
		})
	}
}

// cancelWrappingFlushSink is a sink whose driver flush waits on its context
// and answers the way a real driver does when that context ends: an
// *mq.OpError with CodeCanceled wrapping the context's error, as kafka's Flush
// and driverkit's error mapping do.
type cancelWrappingFlushSink struct {
	advSink
	entered chan struct{}
	once    sync.Once
}

func (s *cancelWrappingFlushSink) Flush(ctx context.Context) error {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return &mq.OpError{Driver: "adv", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeCanceled, Err: ctx.Err()}
}

// TestAJoiningPublisherShutdownIsNotToldADriverWrappedCancellation is the
// joiner's rule through a driver: Shutdown A begins a publisher's shutdown
// and is cancelled while the driver flushes, and Shutdown B joined it with a
// live context. The driver's flush answers with the cancellation wrapped in
// an *mq.OpError, and the joiner was told it, because only an unwrapped
// context error was kept from a joiner.
func TestAJoiningPublisherShutdownIsNotToldADriverWrappedCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		b := newAdvBroker(func(b *advBroker, cfg mq.SinkConfig) mq.Sink {
			return &cancelWrappingFlushSink{advSink: advSink{b: b, receipts: cfg.Receipts}, entered: entered}
		})
		p := advPublisher(t, b, mq.PublisherConfig{})
		ctxA, cancelA := context.WithCancel(context.Background())
		began := make(chan error, 1)
		go func() { began <- p.Shutdown(ctxA) }()
		<-entered
		joined := make(chan error, 1)
		go func() { joined <- p.Shutdown(context.Background()) }()
		synctest.Wait()
		cancelA()
		if err := <-began; !errors.Is(err, context.Canceled) {
			t.Errorf("Shutdown A, which began the shutdown and cancelled its context, = %v, want context.Canceled", err)
		}
		if err := <-joined; errors.Is(err, context.Canceled) {
			t.Errorf("Shutdown B, which joined with a live context, = %v: it was told the beginner's cancellation through the driver's flush", err)
		}
	})
}

// selfShuttingRuns are the two runners a handler can shut its own consumer
// down from: Run's, and RunBatch's, whose handler is given its deliveries
// together. Each calls handle on every delivery it is given, with the
// handler's own context, and then returns nil, which acks whatever handle
// left unsettled.
var selfShuttingRuns = []struct {
	name string
	run  func(c *mq.ConsumerHandle, handle func(ctx context.Context, d *mq.Delivery), opts ...mq.RunOption) error
}{
	{"Run", func(c *mq.ConsumerHandle, handle func(ctx context.Context, d *mq.Delivery), opts ...mq.RunOption) error {
		return c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			handle(ctx, d)
			return nil
		}), opts...)
	}},
	{"RunBatch", func(c *mq.ConsumerHandle, handle func(ctx context.Context, d *mq.Delivery), opts ...mq.RunOption) error {
		return c.RunBatch(context.Background(), mq.BatchHandlerFunc(func(ctx context.Context, ds []*mq.Delivery) error {
			for _, d := range ds {
				handle(ctx, d)
			}
			return nil
		}), opts...)
	}},
}

// TestShutdownFromInsideAHandlerLetsItsAckThrough is a handler that shuts down
// its own consumer, or the client the consumer belongs to. The shutdown
// drains the handlers in flight, so called from inside one it waited for
// itself: for the run's whole drain timeout, here an hour. The source then
// closed under the handler, its Ack was refused and the message it had
// processed was delivered again. From there the shutdown begins and returns,
// and the drain waits for the handler instead.
//
// The handler passes Shutdown its own context, as a handler does. That
// context ends as the drain does, a moment before the shutdown sees the
// drain end, and a shutdown it bounded reported the clean drain as
// cancelled to every Shutdown and Close after it.
func TestShutdownFromInsideAHandlerLetsItsAckThrough(t *testing.T) {
	for _, s := range drainingShutdowns {
		for _, run := range selfShuttingRuns {
			t.Run(s.name+"/"+run.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := newFakeBroker(fakeConfig{lease: -1}, "orders")
					cl := mq.NewClient(b)
					defer func() { _ = cl.Close() }()
					c := consume(t, cl, mq.ConsumerConfig{})
					publishN(t, cl, 1)
					type outcome struct {
						shutdown, ack error
						took          time.Duration
					}
					got := make(chan outcome, 1)
					done := make(chan error, 1)
					go func() {
						done <- run.run(c, func(ctx context.Context, d *mq.Delivery) {
							start := time.Now()
							err := s.shutdown(ctx, cl, c)
							took := time.Since(start)
							got <- outcome{shutdown: err, ack: d.Ack(), took: took}
						}, mq.WithDrainTimeout(time.Hour))
					}()
					o := <-got
					if o.shutdown != nil || o.took != 0 {
						t.Errorf("Shutdown from inside the handler = %v after %v, want nil at once", o.shutdown, o.took)
					}
					if o.ack != nil {
						t.Errorf("the handler's Ack = %v, want nil: the source closed under it", o.ack)
					}
					if err := <-done; !errors.Is(err, mq.ErrClosed) {
						t.Errorf("Run = %v, want ErrClosed", err)
					}
					// The shutdown the handler began goes on behind it, and a
					// Shutdown from outside waits for it and reports how it went.
					if err := s.shutdown(context.Background(), cl, c); err != nil {
						t.Errorf("Shutdown afterwards = %v, want nil", err)
					}
					if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
						t.Errorf("counts = %+v, want the message acked once and never requeued", counts)
					}
				})
			})
		}
	}
}

// handlerDeadlines are the two run options that give each handler a context
// of its own, which the runner cancels as the handler returns:
// WithHandlerTimeout, and WithLeaseDeadline on a broker that grants a lease.
var handlerDeadlines = []struct {
	name   string
	broker fakeConfig
	opt    mq.RunOption
}{
	{"WithHandlerTimeout", fakeConfig{lease: -1}, mq.WithHandlerTimeout(time.Hour)},
	{"WithLeaseDeadline", fakeConfig{lease: time.Hour}, mq.WithLeaseDeadline()},
}

// TestShutdownFromInsideAHandlerLetsTheOtherHandlersFinish is a handler that
// shuts down its own consumer, or its client, with its own context while
// another handler is still working. Under WithHandlerTimeout or
// WithLeaseDeadline that context is cancelled as the calling handler returns,
// and the shutdown it began was bounded by it. The drain gave up at once and
// cancelled the other handler's context, the source closed under that
// handler, its Ack was refused and the message it had processed was
// delivered again.
func TestShutdownFromInsideAHandlerLetsTheOtherHandlersFinish(t *testing.T) {
	for _, s := range drainingShutdowns {
		for _, dl := range handlerDeadlines {
			t.Run(s.name+"/"+dl.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					b := newFakeBroker(dl.broker, "orders")
					cl := mq.NewClient(b)
					defer func() { _ = cl.Close() }()
					c := consume(t, cl, mq.ConsumerConfig{})
					publishN(t, cl, 2)
					type outcome struct {
						ctx, ack error
						took     time.Duration
					}
					var handlers atomic.Int32
					working := make(chan struct{})
					other := make(chan outcome, 1)
					begun := make(chan error, 1)
					done := make(chan error, 1)
					go func() {
						done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
							if handlers.Add(1) == 1 {
								close(working)
								start := time.Now()
								select {
								case <-time.After(10 * time.Second):
								case <-ctx.Done():
								}
								other <- outcome{ctx: ctx.Err(), ack: d.Ack(), took: time.Since(start)}
								return nil
							}
							<-working
							begun <- s.shutdown(ctx, cl, c)
							return d.Ack()
						}), mq.WithConcurrency(2), mq.WithDrainTimeout(time.Hour), dl.opt)
					}()
					if err := <-begun; err != nil {
						t.Errorf("Shutdown from inside the handler = %v, want nil", err)
					}
					o := <-other
					if o.ctx != nil || o.took != 10*time.Second {
						t.Errorf("the other handler's context ended with %v after %v, want it live for the handler's 10s of work", o.ctx, o.took)
					}
					if o.ack != nil {
						t.Errorf("the other handler's Ack = %v, want nil: the source closed under it", o.ack)
					}
					if err := <-done; !errors.Is(err, mq.ErrClosed) {
						t.Errorf("Run = %v, want ErrClosed", err)
					}
					if err := s.shutdown(context.Background(), cl, c); err != nil {
						t.Errorf("Shutdown afterwards = %v, want nil", err)
					}
					if counts := b.counts("orders", "billing"); counts.acked != 2 || counts.requeued != 0 || !counts.conserved() {
						t.Errorf("counts = %+v, want both messages acked once and never requeued", counts)
					}
				})
			})
		}
	}
}

// TestClientCloseWaitsForAShutdownBegunInsideAHandler is the client's half of
// a shutdown that a handler began and did not wait for. The client lists the
// consumer until that shutdown has closed the source, so a Close of the
// client waits for it, and the backend still closes last. Had the consumer
// been dropped from the list as the handler's Shutdown returned, the client
// would have closed the backend under a source still draining.
func TestClientCloseWaitsForAShutdownBegunInsideAHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker(fakeConfig{lease: -1}, "orders")
		cl := mq.NewClient(b)
		defer func() { _ = cl.Close() }()
		c := consume(t, cl, mq.ConsumerConfig{})
		publishN(t, cl, 1)
		begun := make(chan error, 1)
		acked := make(chan error, 1)
		done := make(chan error, 1)
		go func() {
			done <- c.Run(context.Background(), mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				begun <- c.Shutdown(context.Background())
				time.Sleep(10 * time.Second)
				acked <- d.Ack()
				return nil
			}))
		}()
		if err := <-begun; err != nil {
			t.Fatalf("Shutdown from inside the handler = %v, want nil", err)
		}
		start := time.Now()
		if err := cl.Close(); err != nil {
			t.Errorf("the client's Close = %v, want nil", err)
		}
		if elapsed := time.Since(start); elapsed != 10*time.Second {
			t.Errorf("the client's Close returned after %v, want 10s, once the handler had finished", elapsed)
		}
		if err := <-acked; err != nil {
			t.Errorf("the handler's Ack = %v, want nil", err)
		}
		if err := <-done; !errors.Is(err, mq.ErrClosed) {
			t.Errorf("Run = %v, want ErrClosed", err)
		}
		log := b.eventLog()
		if source, backend := indexOfEvent(log, "source-close"), indexOfEvent(log, "backend-close"); source < 0 || backend < source {
			t.Errorf("the event log is %v, want the source closed before the backend", log)
		}
		if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
			t.Errorf("counts = %+v, want the message acked once and never requeued", counts)
		}
	})
}

// TestCloseFromInsideAHandlerDoesNotWaitForTheShutdownItBegan is a handler
// that shuts down its own consumer and then closes it, or its client. A
// Close joins the shutdown already under way and waits for it, and that one
// waits for the handler, so the Close waited for itself: for the run's whole
// drain timeout, here an hour. The source then closed under the handler, its
// Ack was refused and the message it had processed was delivered again.
func TestCloseFromInsideAHandlerDoesNotWaitForTheShutdownItBegan(t *testing.T) {
	for _, s := range promptCloses {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				type outcome struct {
					shutdown, close, ack error
					took                 time.Duration
				}
				got := make(chan outcome, 1)
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
						shutdown := c.Shutdown(ctx)
						start := time.Now()
						err := s.close(cl, c)
						took := time.Since(start)
						got <- outcome{shutdown: shutdown, close: err, ack: d.Ack(), took: took}
						return nil
					}), mq.WithDrainTimeout(time.Hour))
				}()
				o := <-got
				if o.shutdown != nil {
					t.Errorf("Shutdown from inside the handler = %v, want nil", o.shutdown)
				}
				if o.close != nil || o.took != 0 {
					t.Errorf("Close from inside the handler = %v after %v, want nil at once", o.close, o.took)
				}
				if o.ack != nil {
					t.Errorf("the handler's Ack = %v, want nil: the source closed under it", o.ack)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				if err := s.close(cl, c); err != nil {
					t.Errorf("Close afterwards = %v, want nil", err)
				}
				if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message acked once and never requeued", counts)
				}
			})
		})
	}
}

// TestShutdownFromInsideAHandlerWaitsForAConsumerWithNoRunner is a handler
// that shuts down another consumer, one driven by Receive. That consumer has
// no handlers for its drain to wait for, so its Shutdown cannot be waiting
// for the caller, and it returns once the consumer's source is closed. It
// used to return at once, as it does for a consumer with a runner, with the
// source still open and the delivery the caller had received still counted
// in flight.
func TestShutdownFromInsideAHandlerWaitsForAConsumerWithNoRunner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newFakeBroker(fakeConfig{lease: -1}, "orders")
		cl := mq.NewClient(b)
		defer func() { _ = cl.Close() }()
		c := consume(t, cl, mq.ConsumerConfig{})
		received := consume(t, cl, mq.ConsumerConfig{Subscription: "audit"})
		publishN(t, cl, 1)
		buf := make([]mq.Delivery, 1)
		if n, err := received.Receive(context.Background(), buf); n != 1 || err != nil {
			t.Fatalf("Receive = %d, %v, want the one message", n, err)
		}
		type outcome struct {
			shutdown error
			closed   bool
			inFlight int64
		}
		got := make(chan outcome, 1)
		done := make(chan error, 1)
		go func() {
			done <- c.Run(context.Background(), mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
				err := received.Shutdown(ctx)
				got <- outcome{
					shutdown: err,
					closed:   slices.Contains(b.eventLog(), "source-close"),
					inFlight: received.Stats().InFlight,
				}
				return d.Ack()
			}))
		}()
		o := <-got
		if o.shutdown != nil {
			t.Errorf("Shutdown of the other consumer = %v, want nil", o.shutdown)
		}
		if !o.closed || o.inFlight != 0 {
			t.Errorf("as Shutdown returned, the source was closed: %v, and InFlight was %d, want closed and 0", o.closed, o.inFlight)
		}
		if err := c.Shutdown(context.Background()); err != nil {
			t.Errorf("the running consumer's Shutdown = %v, want nil", err)
		}
		if err := <-done; !errors.Is(err, mq.ErrClosed) {
			t.Errorf("Run = %v, want ErrClosed", err)
		}
	})
}

// TestDrainFromInsideAHandlerReturnsAtOnce is Drain called from inside a
// handler. Drain waits for the handlers in flight and has no drain timeout of
// its own, so from inside one it waited for itself until its ctx ended, here
// a minute, and for ever under context.Background. From there it stops the
// run and returns, and Run returns once the calling handler has finished.
func TestDrainFromInsideAHandlerReturnsAtOnce(t *testing.T) {
	for _, run := range selfShuttingRuns {
		t.Run(run.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				type outcome struct {
					drain, ack error
					took       time.Duration
				}
				got := make(chan outcome, 1)
				done := make(chan error, 1)
				go func() {
					done <- run.run(c, func(_ context.Context, d *mq.Delivery) {
						ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
						defer cancel()
						start := time.Now()
						err := c.Drain(ctx)
						took := time.Since(start)
						got <- outcome{drain: err, ack: d.Ack(), took: took}
					})
				}()
				o := <-got
				if o.drain != nil || o.took != 0 {
					t.Errorf("Drain from inside the handler = %v after %v, want nil at once", o.drain, o.took)
				}
				if o.ack != nil {
					t.Errorf("the handler's Ack = %v, want nil", o.ack)
				}
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Errorf("Run = %v, want the drain to have ended it", err)
				}
				if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
					t.Errorf("counts = %+v, want the message acked once and never requeued", counts)
				}
			})
		})
	}
}

// --- the bound against a driver that ignores its context ------------------
//
// A graceful Shutdown waits for what is inside the driver, and a driver call
// is bounded only by the native client. The tests below are the shutdown's
// bound holding anyway: Shutdown returns at the run's drain timeout and
// reports it, and the close goes on behind it.

// heldAck is the fake broker with every acknowledgement held inside the
// driver until release is closed, whatever its context does: a native ack
// parked on a connection that has stopped answering.
type heldAck struct {
	*fakeBroker
	release chan struct{}
}

// OpenSource wraps the fake's source so that its acknowledgements are held.
func (b heldAck) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	src, err := b.fakeBroker.OpenSource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return heldAckSource{Source: src, release: b.release}, nil
}

// heldAckSource is a fake source whose settler holds its acknowledgements.
type heldAckSource struct {
	mq.Source
	release chan struct{}
}

// Settler wraps the fake's settler.
func (s heldAckSource) Settler() mq.Settler {
	return heldAckSettler{Settler: s.Source.Settler(), release: s.release}
}

// heldAckSettler is a fake settler whose Ack does not honour its context.
type heldAckSettler struct {
	mq.Settler
	release chan struct{}
}

// Ack waits for the release and then acknowledges as the fake does, under a
// context of its own: the one it was given may have been cancelled
// meanwhile, and this driver does not look.
func (st heldAckSettler) Ack(_ context.Context, d *mq.DriverDelivery) error {
	<-st.release
	return st.Settler.Ack(context.Background(), d)
}

// deafFetch is the fake broker with a Fetch that does not honour its
// context: a native receive that returns with a delivery or when its source
// closes, and not when it is told to stop.
type deafFetch struct{ *fakeBroker }

// OpenSource wraps the fake's source so that its Fetch ignores its context.
func (b deafFetch) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	src, err := b.fakeBroker.OpenSource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return deafSource{Source: src}, nil
}

// deafSource is a fake source whose Fetch does not honour its context.
type deafSource struct{ mq.Source }

// Fetch fetches as the fake does, under a context of its own.
func (s deafSource) Fetch(_ context.Context, dst []mq.DriverDelivery) (int, error) {
	return s.Source.Fetch(context.Background(), dst)
}

// TestShutdownReturnsAtItsBoundWhileAnAckIgnoresItsContext is the bound
// holding behind the drain: the runner's acknowledgement is parked in a
// driver that ignores its context, so the Committing barrier waits for it,
// and Shutdown returns at the run's drain timeout and reports it. The ack
// still lands once the driver lets it go, before the source closes. With no
// bound of the lifecycle's own, the barrier never reported itself and
// Shutdown waited as long as the driver did.
func TestShutdownReturnsAtItsBoundWhileAnAckIgnoresItsContext(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := heldAck{fakeBroker: newFakeBroker(fakeConfig{lease: -1}, "orders"), release: make(chan struct{})}
				cl := mq.NewClient(b)
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				publishN(t, cl, 1)
				entered := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
						close(entered)
						return nil
					}), mq.WithDrainTimeout(time.Minute))
				}()
				<-entered
				// The driver lets the ack go long after any bound, so that a
				// Shutdown that waited for it says how long it waited.
				go func() {
					time.Sleep(10 * time.Minute)
					close(b.release)
				}()
				start := time.Now()
				err := s.shutdown(context.Background(), cl, c)
				if elapsed := time.Since(start); mq.CodeOf(err) != mq.CodeDeadlineExceeded || elapsed != time.Minute {
					t.Errorf("Shutdown = %v after %v, want the drain timeout reported at 1m0s", err, elapsed)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				if counts := b.counts("orders", "billing"); counts.acked != 1 || counts.requeued != 0 || !counts.conserved() {
					t.Errorf("counts = %+v, want the held ack landed before the source closed", counts)
				}
			})
		})
	}
}

// TestShutdownReturnsAtItsBoundWhileAFetchIgnoresItsContext is the bound
// holding in the drain itself: the runner's fetch is parked in a driver that
// ignores its context, on a subscription with nothing to deliver, so the
// fetch loop does not end when it is told to stop and only closing the source
// ends it. Shutdown returns at the run's drain timeout and reports it, and
// the close ends the fetch. With no bound of the lifecycle's own, the drain
// waited for a fetch loop that only the close could end, and the close came
// after the drain.
func TestShutdownReturnsAtItsBoundWhileAFetchIgnoresItsContext(t *testing.T) {
	for _, s := range drainingShutdowns {
		t.Run(s.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newFakeBroker(fakeConfig{lease: -1}, "orders")
				cl := mq.NewClient(deafFetch{b})
				defer func() { _ = cl.Close() }()
				c := consume(t, cl, mq.ConsumerConfig{})
				done := make(chan error, 1)
				go func() {
					done <- c.Run(context.Background(), mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
						return nil
					}), mq.WithDrainTimeout(time.Minute))
				}()
				// The runner is parked in the driver's Fetch.
				synctest.Wait()
				// Long after any bound, another process publishes, which is
				// the one thing besides the close that ends the parked fetch,
				// so that a Shutdown that waited for it says how long it
				// waited.
				elsewhere := mq.NewClient(b)
				defer func() { _ = elsewhere.Close() }()
				woken := make(chan struct{})
				go func() {
					defer close(woken)
					time.Sleep(10 * time.Minute)
					_ = elsewhere.Publish(context.Background(), "orders", &mq.Message{ID: "wake", Body: []byte("b")})
				}()
				start := time.Now()
				err := s.shutdown(context.Background(), cl, c)
				if elapsed := time.Since(start); mq.CodeOf(err) != mq.CodeDeadlineExceeded || elapsed != time.Minute {
					t.Errorf("Shutdown = %v after %v, want the drain timeout reported at 1m0s", err, elapsed)
				}
				if err := <-done; !errors.Is(err, mq.ErrClosed) {
					t.Errorf("Run = %v, want ErrClosed", err)
				}
				<-woken
			})
		})
	}
}

package iqmq_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
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
// the specification names — the settlement leaves the driver, the publisher's
// sink closes, the consumer's source closes, and the backend closes last.
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
	case sink > source:
		t.Fatalf("the consumer drained before the publisher: the specification stops accepting publications first: %v", log)
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

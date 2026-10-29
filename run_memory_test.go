package iqmq_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

//
// Two drivers is the point: the fake is written to the SPI and answers what
// the tests need it to, and the memory driver is an engine with its own
// redelivery accounting, its own delayed-record structure and its own
// dead-letter route. A runner that agrees with only the fake has been checked
// against a mirror.

// memoryConsumer opens a client, a broker and a consumer over a topic.
func memoryConsumer(t *testing.T, topic, sub string, topics ...string) (*mq.Client, *memory.Broker, *mq.ConsumerHandle) {
	t.Helper()
	cl, b := memory.NewForTest(t, memory.TestTopics(append([]string{topic}, topics...)...))
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	return cl, b, c
}

// publishToMemory publishes n messages whose ID is their index.
func publishToMemory(t *testing.T, cl *mq.Client, topic string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		m := &mq.Message{ID: strconv.Itoa(i), Key: []byte("k" + strconv.Itoa(i)), Body: []byte("body")}
		if err := cl.Publish(ctx, topic, m); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
}

// inspect reads one subscription's gauges.
func inspect(t *testing.T, b *memory.Broker, topic, sub string) memory.Snapshot {
	t.Helper()
	s, err := b.Inspect(topic, sub)
	if err != nil {
		t.Fatalf("inspecting %s/%s: %v", topic, sub, err)
	}
	return s
}

// TestRunAgainstTheMemoryDriver is the runner end to end over a real driver:
// everything published is handled once and acked, and the subscription is
// empty afterwards.
func TestRunAgainstTheMemoryDriver(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing")
	publishToMemory(t, cl, "orders", 16)
	h := &collector{}
	run := startRun(t, c, h, mq.WithConcurrency(4))
	waitFor(t, "every message to be handled", func() bool { return h.count() == 16 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	s := inspect(t, b, "orders", "billing")
	if s.Acked != 16 || s.Ready != 0 || s.InFlight != 0 {
		t.Fatalf("snapshot = %+v, want 16 acked and nothing left", s)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

// TestRunAgainstMemoryRequeuesWithTheBrokersOwnDelay pins the retry shape
// against a driver that really has delayed redelivery : the
// nack carries the backoff, the record sits in the broker's DELAYED structure
// rather than in the runner, and it comes back when the broker's clock moves.
//
// This is what "retry by nack, never in place" looks like from outside: while
// the message is waiting, the runner holds nothing.
func TestRunAgainstMemoryRequeuesWithTheBrokersOwnDelay(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing")
	publishToMemory(t, cl, "orders", 1)
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			return errors.New("try again")
		}
		return nil
	}}
	run := startRun(t, c, h, mq.WithRetryBackoff(mq.ConstantBackoff(time.Minute)))
	waitFor(t, "the record to be parked for its delay", func() bool {
		s := inspect(t, b, "orders", "billing")
		return s.Delayed == 1 && s.InFlight == 0
	})
	if h.count() != 1 {
		t.Fatalf("the handler ran %d times while the record was delayed, want 1", h.count())
	}
	if err := b.Faults().Advance(2 * time.Minute); err != nil {
		t.Fatalf("advancing the broker's clock: %v", err)
	}
	waitFor(t, "the redelivery after the delay", func() bool { return h.count() == 2 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	s := inspect(t, b, "orders", "billing")
	if s.Acked != 1 {
		t.Fatalf("snapshot = %+v, want the retry acked", s)
	}
}

// TestRunAgainstMemoryHaltsWithNoDeadLetter is ADR-011's default over a real
// driver: attempts run out, nothing can dead-letter the message, and the
// consumer halts with the message still the broker's.
func TestRunAgainstMemoryHaltsWithNoDeadLetter(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing")
	publishToMemory(t, cl, "orders", 1)
	boom := errors.New("cannot process this")
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return boom }),
		mq.WithMaxAttempts(3),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
	)
	err := run.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) || !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want ErrPoisonMessage carrying the handler's error", err)
	}
	s := inspect(t, b, "orders", "billing")
	if s.Acked != 0 || s.Dropped != 0 || s.DeadLettered != 0 {
		t.Fatalf("snapshot = %+v, want the message neither acked, dropped nor dead-lettered", s)
	}
	if s.Ready+s.InFlight+s.Delayed != 1 {
		t.Fatalf("snapshot = %+v, want the one message still redeliverable", s)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

// TestRunAgainstMemoryDeadLetters is the dead-letter route over a real
// driver: the message lands on the dead-letter topic with its provenance, and
// only then is the original acked.
func TestRunAgainstMemoryDeadLetters(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	dlq, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("opening the dead-letter consumer: %v", err)
	}
	publishToMemory(t, cl, "orders", 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
	)
	waitFor(t, "the original to be acked", func() bool {
		return inspect(t, b, "orders", "billing").Acked == 1
	})
	got, derr := memory.Drain(context.Background(), dlq, 1)
	if derr != nil {
		t.Fatalf("draining the dead-letter topic: %v", derr)
	}
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if len(got) != 1 {
		t.Fatalf("the dead-letter topic holds %d messages, want 1", len(got))
	}
	if v, ok := got[0].Headers.Get(mq.HeaderOriginTopic); !ok || string(v) != "orders" {
		t.Fatalf("%s = %q (present=%v), want \"orders\"", mq.HeaderOriginTopic, v, ok)
	}
	if v, ok := got[0].Headers.Get(mq.HeaderFailure); !ok || string(v) != "undecodable" {
		t.Fatalf("%s = %q (present=%v), want the handler's error", mq.HeaderFailure, v, ok)
	}
	if string(got[0].Body) != "body" {
		t.Fatalf("the dead-lettered body is %q, want the original", got[0].Body)
	}
}

// TestRunAgainstMemoryNeverAcksAfterAFailedDeadLetterPublish is the trap this
// increment exists to avoid, over a real driver: the order is publish,
// confirm, settle, so a refused dead-letter publication leaves the original
// unacked and redeliverable.
//
// memory.Faults.RejectPublish refuses every publication from the moment it is
// armed, which by then is only the runner's dead-letter publications: the
// original message was published before it.
func TestRunAgainstMemoryNeverAcksAfterAFailedDeadLetterPublish(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	publishToMemory(t, cl, "orders", 1)
	b.Faults().RejectPublish(-1)
	var handled atomic.Int64
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		handled.Add(1)
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
		mq.WithRetryBackoff(mq.ConstantBackoff(0)),
	)
	waitFor(t, "three refused dead-letter publications", func() bool {
		return b.Faults().PublishRejections() >= 3
	})
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	s := inspect(t, b, "orders", "billing")
	if s.Acked != 0 {
		t.Fatalf("snapshot = %+v: the original was acked after a dead-letter publication that never landed", s)
	}
	if s.Dropped != 0 || s.DeadLettered != 0 {
		t.Fatalf("snapshot = %+v, want the message neither dropped nor dead-lettered", s)
	}
	if s.Ready+s.InFlight+s.Delayed != 1 {
		t.Fatalf("snapshot = %+v, want the one message still redeliverable", s)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
	if handled.Load() < 2 {
		t.Fatalf("the handler ran %d times, want the message redelivered after the failed publish", handled.Load())
	}
}

// TestRunAgainstMemoryDrainsBeforeItReturns pins Close rule over a
// real driver: nothing a lane accepted is abandoned, so no lease is left
// outstanding when Run returns.
func TestRunAgainstMemoryDrainsBeforeItReturns(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing")
	publishToMemory(t, cl, "orders", 32)
	var started, finished atomic.Int64
	first := make(chan struct{}, 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		started.Add(1)
		select {
		case first <- struct{}{}:
		default:
		}
		time.Sleep(10 * time.Millisecond)
		finished.Add(1)
		return nil
	}), mq.WithConcurrency(8))
	<-first
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if s, f := started.Load(), finished.Load(); s != f {
		t.Fatalf("Run returned with %d handlers started and %d finished: in-flight work was abandoned", s, f)
	}
	s := inspect(t, b, "orders", "billing")
	if s.InFlight != 0 {
		t.Fatalf("snapshot = %+v, want no lease outstanding after the drain", s)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

// TestRunAgainstMemoryKeepsOneKeyInOrder pins the lane routing: deliveries
// that share a key go to one lane, so a concurrent runner does not reorder
// one key's messages.
func TestRunAgainstMemoryKeepsOneKeyInOrder(t *testing.T) {
	cl, _, c := memoryConsumer(t, "orders", "billing")
	const n = 64
	ctx := context.Background()
	for i := range n {
		m := &mq.Message{ID: strconv.Itoa(i), Key: []byte("cust-42"), Body: []byte("b")}
		if err := cl.Publish(ctx, "orders", m); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	h := &collector{}
	run := startRun(t, c, h, mq.WithConcurrency(8))
	waitFor(t, "every message", func() bool { return h.count() == n })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	ids := h.seenIDs()
	for i, id := range ids {
		if id != strconv.Itoa(i) {
			t.Fatalf("one key was handled out of order at %d: %v", i, ids)
		}
	}
}

// rewriting is a middleware that replaces every field of the delivery's
// message for the rest of the chain, the way a decrypting, decompressing or
// claim-checking middleware replaces the body: by assignment, leaving the
// storage the broker delivered alone.
func rewriting(next mq.Handler) mq.Handler {
	return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		d.Body = []byte("opened")
		d.Key = []byte("rewritten")
		d.ID = "rewritten"
		d.Properties.ContentEncoding = ""
		d.Headers = mq.Headers{}
		return next.Handle(ctx, d)
	})
}

// TestRunAgainstMemoryDeadLettersTheMessageAsItWasReceived is the dead-letter
// copy behind a middleware that rewrites the delivery for the rest of the
// chain. The copy is the message the broker delivered and not what the chain
// made of it: a decrypted body in the dead-letter topic is the plaintext the
// publisher sealed, and an expanded one may be more than the broker accepts.
func TestRunAgainstMemoryDeadLettersTheMessageAsItWasReceived(t *testing.T) {
	cl, _, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	ctx := context.Background()
	dlq, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("opening the dead-letter consumer: %v", err)
	}
	m := &mq.Message{ID: "o-1", Key: []byte("k"), Body: []byte("sealed")}
	m.Headers.Set("coding", []byte("v1"))
	m.Properties.ContentEncoding = "x-sealed"
	if err := cl.Publish(ctx, "orders", m); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return errors.New("cannot process")
	}),
		mq.WithMiddleware(rewriting),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
	)
	drainCtx, cancel := context.WithTimeout(ctx, runWait)
	defer cancel()
	got, err := memory.Drain(drainCtx, dlq, 1)
	if err != nil {
		t.Fatalf("draining the dead-letter topic: %v", err)
	}
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	dl := got[0]
	if string(dl.Body) != "sealed" || string(dl.Key) != "k" || dl.ID != "o-1" {
		t.Fatalf("the dead letter is body %q key %q id %q, want the message as it was received", dl.Body, dl.Key, dl.ID)
	}
	if v, ok := dl.Headers.Get("coding"); !ok || string(v) != "v1" {
		t.Fatalf("the dead letter's coding header = %q (present=%v), want the one it was published with", v, ok)
	}
	if dl.Properties.ContentEncoding != "x-sealed" {
		t.Fatalf("the dead letter's content encoding = %q, want the one it was published with", dl.Properties.ContentEncoding)
	}
	if v, ok := dl.Headers.Get(mq.HeaderFailure); !ok || string(v) != "cannot process" {
		t.Fatalf("%s = %q (present=%v), want the handler's error", mq.HeaderFailure, v, ok)
	}
}

// TestRunAgainstMemoryBacksOffAFailedDeadLetterPublish is a dead-letter
// destination that refuses every copy. The original goes back to the broker,
// which is what keeps it alive, and it goes back with the retry backoff: an
// immediate redelivery ran the handler and the refused publication again as
// fast as the broker could hand the message over, for as long as the
// destination stayed broken.
func TestRunAgainstMemoryBacksOffAFailedDeadLetterPublish(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	publishToMemory(t, cl, "orders", 1)
	b.Faults().RejectPublish(-1)
	var handled atomic.Int64
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		handled.Add(1)
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
		mq.WithRetryBackoff(mq.ConstantBackoff(time.Minute)),
	)
	waitFor(t, "the message to be parked or handled again", func() bool {
		return inspect(t, b, "orders", "billing").Delayed == 1 || handled.Load() > 1
	})
	if n := handled.Load(); n != 1 {
		t.Fatalf("the handler ran %d times inside a one-minute backoff, want 1: the refused dead-letter copy was requeued at once", n)
	}
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	s := inspect(t, b, "orders", "billing")
	if s.Acked != 0 || s.Ready+s.InFlight+s.Delayed != 1 {
		t.Fatalf("snapshot = %+v, want the one message unacked and redeliverable", s)
	}
	if got := c.Stats().DeadLetterFailures; got < 1 {
		t.Fatalf("DeadLetterFailures = %d, want the refused publication counted", got)
	}
}

// TestRunAgainstMemoryPostponementsLeaveTheAttemptsToTheHandler is a message
// postponed more often than WithMaxAttempts allows and then failing once for
// real, on a broker that counts every redelivery. When the postponements went
// back to the broker they were counted too, so the first real failure met the
// bound and the runner halted with a *PoisonError after one handler call: a
// backlog behind a rate limit turned any transient error into a stopped
// consumer. Held on the worker, they leave all three attempts to the handler.
func TestRunAgainstMemoryPostponementsLeaveTheAttemptsToTheHandler(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"), memory.TestRealClock())
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	publishToMemory(t, cl, "orders", 1)
	mw, _ := refusing(5, time.Millisecond)
	h := &collector{fn: func(n int, _ *mq.Delivery) error {
		if n == 1 {
			return errors.New("the dependency is busy")
		}
		return nil
	}}
	run := startRun(t, c, h,
		mq.WithMiddleware(mw),
		mq.WithMaxAttempts(3),
		mq.WithRetryBackoff(mq.ConstantBackoff(time.Millisecond)))
	waitFor(t, "the handler's second attempt", func() bool { return h.count() == 2 || run.returned() })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v after %d handler calls, want the message handled on its second attempt", err, h.count())
	}
	if s := inspect(t, b, "orders", "billing"); s.Acked != 1 {
		t.Fatalf("snapshot = %+v, want the message acked", s)
	}
}

// TestRunAgainstMemoryPostponesForTheBackoffWhenNoDelayIsGiven is a
// postponement that names no delay, as Dedup's refusal of a duplicate still in
// flight does. It waits the run's backoff, and a backoff longer than half the
// lease is waited in the broker's delayed structure rather than on a worker
// that would hold the delivery past its lease.
func TestRunAgainstMemoryPostponesForTheBackoffWhenNoDelayIsGiven(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing")
	publishToMemory(t, cl, "orders", 1)
	var refused atomic.Bool
	h := &collector{}
	run := startRun(t, c, mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		if refused.CompareAndSwap(false, true) {
			return mq.Postpone(errors.New("not now"), 0)
		}
		return h.Handle(ctx, d)
	}), mq.WithRetryBackoff(mq.ConstantBackoff(time.Minute)))
	waitFor(t, "the record to be parked for the backoff", func() bool {
		s := inspect(t, b, "orders", "billing")
		return s.Delayed == 1 && s.InFlight == 0
	})
	if h.count() != 0 {
		t.Fatalf("the handler ran %d times while the record was postponed, want 0", h.count())
	}
	if err := b.Faults().Advance(2 * time.Minute); err != nil {
		t.Fatalf("advancing the broker's clock: %v", err)
	}
	waitFor(t, "the redelivery after the backoff", func() bool { return h.count() == 1 })
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

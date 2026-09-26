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

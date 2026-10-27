package middleware_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"
)

// These are the tests for the trap that has no other way of being caught: a
// middleware that refuses a delivery before the handler and returns without
// settling it. Under mq.SettlementAuto the runner settles and the defect is
// invisible; under mq.SettlementManual the runner settles NOTHING, so the
// broker's counters show exactly what the middleware did and nothing else.
// A middleware that forgot to settle leaves the delivery in flight until its
// lease expires, and these tests time out on it.

// manual is the run option set these tests use.
func manual() []mq.RunOption { return []mq.RunOption{mq.WithSettlement(mq.SettlementManual)} }

// settling is the terminal handler under manual settlement: the handler owns
// every delivery it is given, so one that reaches it is acked here. What the
// broker's counters then show for the deliveries that did NOT reach it is the
// middleware's own settlement.
func settling(c *calls) mq.Handler {
	return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		err := c.Handle(ctx, d)
		if err == nil {
			err = d.Ack()
		}
		return err
	})
}

func TestFilterSettlesTheDeliveryItRejects(t *testing.T) {
	h := newHarness(t)
	c := &calls{}
	var dropped int
	chain := middleware.FilterWith(
		func(d *mq.Delivery) bool { return d.ID == "keep" },
		func(context.Context, *mq.Delivery) { dropped++ },
	)
	r := h.run(chain(settling(c)), manual()...)
	h.publish(4, func(i int) string {
		if i == 0 {
			return "keep"
		}
		return "drop-" + strconv.Itoa(i)
	})
	waitFor(t, "every delivery to be settled", func() bool {
		s := h.snapshot()
		return s.Acked == 4 && s.InFlight == 0
	})
	_ = r.stop()
	if c.count() != 1 {
		t.Fatalf("the handler ran %d times, want 1: only the kept delivery reaches it", c.count())
	}
	if dropped != 3 {
		t.Fatalf("the observer saw %d filtered deliveries, want 3", dropped)
	}
	s := h.snapshot()
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

func TestDedupSettlesTheDuplicateItSuppresses(t *testing.T) {
	h := newHarness(t)
	c := &calls{}
	var duplicates int
	chain := middleware.Dedup(middleware.DedupConfig{
		Store: mustNewLRUStore(t, 64),
		OnDuplicate: func(context.Context, *mq.Delivery, string) {
			duplicates++
		},
	})
	r := h.run(chain(settling(c)), manual()...)
	// Four publications, two distinct ids.
	h.publish(4, func(i int) string { return "id-" + strconv.Itoa(i%2) })
	waitFor(t, "every delivery to be settled", func() bool {
		s := h.snapshot()
		return s.Acked == 4 && s.InFlight == 0
	})
	_ = r.stop()
	if c.count() != 2 {
		t.Fatalf("the handler ran %d times, want 2: the two duplicates are suppressed", c.count())
	}
	if duplicates != 2 {
		t.Fatalf("the observer saw %d duplicates, want 2", duplicates)
	}
}

func TestValidateDeadLettersAnInvalidDelivery(t *testing.T) {
	h := newHarness(t,
		memory.TestTopics("dead"),
		memory.TestTopology(memory.TopologyConfig{DeadLetterTopic: "dead"}),
	)
	c := &calls{}
	var refused []error
	chain := middleware.ValidateWith(
		func(d *mq.Delivery) error {
			if d.ID == "good" {
				return nil
			}
			return errBoom
		},
		func(_ context.Context, _ *mq.Delivery, err error) { refused = append(refused, err) },
	)
	// A subscriber on the dead-letter topic, opened before anything is
	// published: StartLatest means a subscription created
	// afterwards would see nothing. It is also the only way to tell a
	// dead-letter from an ack -- Snapshot.DeadLettered counts only the
	// records the ENGINE dead-lettered for exhausting their attempts, and an
	// explicit Nack(NackDeadLetter) settles natively like an ack.
	dlq, err := h.cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "dead", Subscription: "watch"})
	if err != nil {
		t.Fatalf("opening the dead-letter consumer: %v", err)
	}
	defer func() { _ = dlq.Close() }()

	r := h.run(chain(settling(c)), manual()...)
	h.publish(3, func(i int) string {
		if i == 0 {
			return "good"
		}
		return "bad-" + strconv.Itoa(i)
	})
	waitFor(t, "every delivery to be settled", func() bool {
		s := h.snapshot()
		return s.Acked == 3 && s.InFlight == 0
	})
	waitFor(t, "the refused messages to reach the dead-letter topic", func() bool {
		d, err := h.b.Inspect("dead", "watch")
		return err == nil && d.Ready == 2
	})
	_ = r.stop()
	s := h.snapshot()
	dead, err := h.b.Inspect("dead", "watch")
	if err != nil {
		t.Fatalf("inspecting the dead-letter topic: %v", err)
	}
	if dead.Ready != 2 {
		t.Fatalf("the dead-letter topic holds %d messages, want 2: an invalid message goes to the dead-letter route, not back onto the queue", dead.Ready)
	}
	if c.count() != 1 {
		t.Fatalf("the handler ran %d times, want 1", c.count())
	}
	if len(refused) != 2 || !errors.Is(refused[0], errBoom) {
		t.Fatalf("the observer saw %v, want the validator's own reason twice", refused)
	}
	if !s.Conserved() {
		t.Fatalf("The specification conservation broken: %+v", s)
	}
}

func TestValidateHandsTheDecisionToTheRunnerWhenThereIsNoDeadLetterRoute(t *testing.T) {
	// Reject on a backend with no dead-letter destination answers
	// ErrUnsupported and settles NOTHING, which leaves the delivery
	// settleable. Validate then returns mq.DeadLetter(err) so that the
	// runner's own route decides -- and with no route configured, ADR-011
	// says halt rather than skip.
	h := newHarness(t)
	c := &calls{}
	chain := middleware.Validate(func(*mq.Delivery) error { return errBoom })
	r := h.run(chain(c)) // automatic settlement: the runner is the one deciding
	h.publish(1, func(int) string { return "bad" })
	err := r.wait()
	if !errors.Is(err, mq.ErrPoisonMessage) {
		t.Fatalf("Run = %v, want ErrPoisonMessage: an invalid message with no dead-letter route halts (ADR-011)", err)
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run = %v, want the validator's own reason reachable through the halt", err)
	}
	if c.count() != 0 {
		t.Fatalf("the handler ran %d times for an invalid delivery, want 0", c.count())
	}
}

func TestARefusedDeliveryIsNotLeftInFlight(t *testing.T) {
	// The negative control for all three: a middleware that short-circuits
	// and returns nil WITHOUT settling. Under manual settlement nothing
	// settles it, so the delivery stays in flight -- which is the defect the
	// tests above exist to catch, demonstrated here so that their assertions
	// are known to be able to fail.
	h := newHarness(t, memory.TestTopology(memory.TopologyConfig{AckDeadline: -1}))
	c := &calls{}
	silent := func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	}
	r := h.run(silent(c), manual()...)
	h.publish(2, strconv.Itoa)
	waitFor(t, "both deliveries to be in flight", func() bool { return h.snapshot().InFlight == 2 })
	// A whole second later they are still in flight and nothing has been
	// acked: this is what a middleware that forgets to settle looks like.
	time.Sleep(100 * time.Millisecond)
	s := h.snapshot()
	_ = r.stop()
	if s.Acked != 0 || s.InFlight != 2 {
		t.Fatalf("snapshot = %+v, want nothing settled and 2 in flight; if this ever changes, the settlement assertions above are no longer testing anything", s)
	}
	if c.count() != 0 {
		t.Fatal("the short-circuiting middleware called the handler")
	}
}

func TestRetryDoesNotRepeatAHandlerThatSettledItsDelivery(t *testing.T) {
	// A handler that nacked its delivery and then failed has already decided
	// what becomes of the message: it is requeued, and the broker hands it to
	// a handler again. Calling the handler again on the settled delivery runs
	// its work a second time for one message, alongside the redelivery.
	h := newHarness(t)
	var first, redelivered atomic.Int64
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		if d.Attempt > 1 {
			redelivered.Add(1)
			return nil
		}
		first.Add(1)
		if err := d.Nack(); err != nil && !errors.Is(err, mq.ErrAlreadySettled) {
			t.Errorf("Nack: %v", err)
		}
		return mq.ErrBackpressure
	})
	chain := mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Clock: newFakeClock()})
	r := h.run(chain(handler))
	h.publish(1, nil)
	waitFor(t, "the requeued message to be handled again", func() bool { return redelivered.Load() == 1 })
	_ = r.stop()
	if n := first.Load(); n != 1 {
		t.Fatalf("the handler ran %d times on the delivery it had nacked, want 1: Retry repeated a settled delivery", n)
	}
}

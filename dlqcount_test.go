package iqmq_test

import (
	"context"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestTheTwoDeadLetterRoutesAreCountedSeparately pins the finding and
// the decision that closed it.
//
// the specification's own route publishes a copy and then ACKS the delivery; the native
// route nacks it with NackDeadLetter and the broker moves the message. One
// counter cannot say both, because the two settle in opposite directions: so
// DeadLettered stays the native route's (and stays a subset of Nacked) and
// DeadLetterPublished is the published route's (a subset of Acked). Before
// this, a service on the route read zero however many messages it
// dead-lettered.
func TestTheTwoDeadLetterRoutesAreCountedSeparately(t *testing.T) {
	cl, b, c := memoryConsumer(t, "orders", "billing", "orders.dlq")
	_ = b
	publishToMemory(t, cl, "orders", 1)
	run := startRun(t, c, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		return errors.New("undecodable")
	}),
		mq.WithMaxAttempts(1),
		mq.WithDeadLetter("orders.dlq"),
	)
	waitFor(t, "the message to be dead-lettered through the route", func() bool {
		return c.Stats().DeadLetterPublished == 1
	})
	got := c.Stats()
	if err := run.stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}

	if got.DeadLetterPublished != 1 {
		t.Errorf("DeadLetterPublished = %d, want 1: the route published a copy of the message", got.DeadLetterPublished)
	}
	if got.DeadLettered != 0 {
		t.Errorf("DeadLettered = %d, want 0: that counter is the NATIVE route, and this run took the specification's",
			got.DeadLettered)
	}
	// The published route settles positively, so its message is in Acked and
	// not in Nacked. This is the reconciliation the second counter exists to
	// keep: widening DeadLettered would have put a dead-letter in Nacked's
	// subset while the delivery was acked.
	if got.Acked != 1 {
		t.Errorf("Acked = %d, want 1: the route acks the delivery once the copy is published", got.Acked)
	}
	if got.DeadLetterFailures != 0 {
		t.Errorf("DeadLetterFailures = %d, want 0: the publication succeeded", got.DeadLetterFailures)
	}
	if total := got.DeadLettered + got.DeadLetterPublished; total != 1 {
		t.Errorf("DeadLettered + DeadLetterPublished = %d, want 1: the sum is the total dead-lettered", total)
	}
}

package main

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// TestChargeWorkflowInABubble runs the workflow with an HOUR of retry backoff
// and asserts how much time passed.
//
// The assertion is the reason the test exists. Anybody can run a retrying
// consumer under synctest and watch it finish quickly; what a deterministic
// test buys is that the SCHEDULE is checkable. Two failures and a doubling
// backoff based at an hour is one hour then two, so exactly three hours of
// bubble time separate the publication from the capture — and if the runner
// stopped waiting between attempts, or capped the delay, or drew it out of a
// jitter envelope, this number would change and nothing else in the test
// would.
func TestChargeWorkflowInABubble(t *testing.T) {
	const base = time.Hour
	wall := time.Now()
	synctest.Test(t, func(t *testing.T) {
		client, broker := memory.NewForTest(t, memory.TestRealClock())
		if err := broker.Advance(time.Second); err == nil {
			t.Fatal("Broker.Advance succeeded on a broker left on the wall clock; TestRealClock hands time to the bubble and Advance has no clock of its own to move")
		}
		var out bytes.Buffer
		start := time.Now()
		if err := chargeWorkflow(t.Context(), &out, client, base); err != nil {
			t.Fatalf("chargeWorkflow: %v", err)
		}
		if got, want := time.Since(start), base+2*base; got != want {
			t.Errorf("the workflow took %v of bubble time, want exactly %v (%d failures at base %v, doubling)",
				got, want, attemptsBeforeSuccess, base)
		}
		const want = "attempt 1 for charge ch-1 (broker attempt 1)\n" +
			"attempt 2 for charge ch-1 (broker attempt 2)\n" +
			"attempt 3 for charge ch-1 (broker attempt 3)\n" +
			"charge ch-1 captured\n"
		if got := out.String(); got != want {
			t.Errorf("the workflow printed\n%q\nwant\n%q", got, want)
		}
	})
	// Three hours of backoff cost no real time at all, which is the whole
	// claim. A second is three orders of magnitude of headroom over what this
	// test needs and still four orders below what it would cost awake.
	if slept := time.Since(wall); slept > time.Second {
		t.Errorf("three hours of bubble time cost %v of real time: something in the workflow is waiting outside the bubble", slept)
	}
}

// TestLeaseExpiryWithAdvance is the other half of the story: the broker's own
// clock, moved by hand.
//
// A lease that expires is not something a bubble can produce on its own — the
// bubble moves time when every goroutine is durably blocked, and a test holding
// an unsettled delivery is not blocked on anything. So the deterministic
// TestClock is what this needs: Broker.Advance moves it AND runs the sweeps the
// move implies (lease expiry among them) before it returns, which is why the
// redelivery is already waiting when Advance comes back.
func TestLeaseExpiryWithAdvance(t *testing.T) {
	const lease = 30 * time.Second
	client, broker := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{AckDeadline: lease}))
	ctx := t.Context()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "charges", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "charges", &mq.Message{ID: "ch-2", Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Every Receive is bounded. A check that hangs instead of failing is
	// worse than no check: the CI log says "timed out after 10m" and names no
	// assertion, which is the failure mode the contributor notes records.
	dst := make([]mq.Delivery, 1)
	n, err := receive(ctx, consumer, dst)
	if n != 1 || err != nil {
		t.Fatalf("Receive = %d, %v", n, err)
	}
	if got := dst[0].Info().Attempt; got != 1 {
		t.Fatalf("the first delivery reports attempt %d, want 1", got)
	}
	// Nothing is settled, and nothing is slept on: the lease runs out because
	// the test moved the broker's clock past it.
	if err := broker.Advance(lease + time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	again, err := receive(ctx, consumer, dst)
	if again != 1 || err != nil {
		t.Fatalf("after advancing past a %v lease the message did not come back: Receive = %d, %v", lease, again, err)
	}
	if got := dst[0].Info().Attempt; got != 2 {
		t.Errorf("the redelivery reports attempt %d, want 2", got)
	}
	if !dst[0].Info().Redelivered {
		t.Error("the redelivery is not marked as one")
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// receive is Receive with a bound, so that a message that never arrives fails
// the test instead of hanging it.
func receive(ctx context.Context, c *mq.ConsumerHandle, dst []mq.Delivery) (int, error) {
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.Receive(rctx, dst)
}

package idempotency_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/idempotency"
)

// TestAnOpenClaimDoesNotUseUpTheAttempts is another worker's open claim met
// through a real runner. The refusal came back to the runner as a failed
// attempt, so a claim held for longer than the attempts added up to halted the
// runner with a *PoisonError, or dead-lettered or skipped a message nobody had
// handled. Postponed, the delivery waits until the claim is released and is
// then handled.
func TestAnOpenClaimDoesNotUseUpTheAttempts(t *testing.T) {
	cl, b := memory.NewForTest(t, memory.TestTopics("orders"), memory.TestRealClock())
	c, err := cl.Consumer(context.Background(), mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	store, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	held, err := store.Claim(context.Background(), "order-1", idempotency.DefaultInFlightLease)
	if err != nil {
		t.Fatalf("taking the other worker's claim: %v", err)
	}
	ic, err := idempotency.New(idempotency.Config{Store: store, Key: idempotency.KeyFromID(), Window: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	h := ic.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		calls.Add(1)
		return nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx, h, mq.WithMaxAttempts(2), mq.WithRetryBackoff(mq.ConstantBackoff(time.Millisecond)))
	}()
	t.Cleanup(cancel)
	if err := cl.Publish(context.Background(), "orders", &mq.Message{ID: "order-1", Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	var stopped error
	returned := func() bool {
		select {
		case stopped = <-done:
			return true
		default:
			return false
		}
	}
	waitUntil(t, "the delivery to be refused more often than WithMaxAttempts allows", func() bool {
		return ic.Counts().Deferred >= 5 || returned()
	})
	if stopped != nil {
		t.Fatalf("Run = %v after %d refusals, want it still running: the refusals spent the attempts", stopped, ic.Counts().Deferred)
	}
	if err := store.Abort(context.Background(), held); err != nil {
		t.Fatalf("releasing the other worker's claim: %v", err)
	}
	waitUntil(t, "the delivery to be handled", func() bool {
		s, err := b.Inspect("orders", "billing")
		return err == nil && s.Acked == 1 || returned()
	})
	if stopped != nil {
		t.Fatalf("Run = %v, want it still running", stopped)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("the handler ran %d times, want 1", n)
	}
}

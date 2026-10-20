package claimcheck_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/claimcheck"
)

// These two tests are the decision the specification records, proved through
// a real runner over the memory driver rather than asserted on an error value:
// what a consumer SEES when the payload a claim names has gone.
//
// The store answered, definitively, that the object does not exist -- here a
// FileStore whose TTL has passed, which is the ordinary way it happens -- and
// no retry can change that. So the consumer sees:
//
//   - its handler never runs;
//   - with a dead-letter topic, the message is dead-lettered on its FIRST
//     attempt, with no backoff, carrying the original reference as its body
//     (so which payload went missing is diagnosable) and ErrPayloadGone's text
//     as its failure header;
//   - without one, the runner halts with a *PoisonError wrapping
//     ErrPayloadGone, and the message goes back to the broker rather than
//     being dropped (ADR-011).

func TestAGonePayloadIsDeadLetteredOnItsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	var c clock
	store := fileStore(t, claimcheck.FileOptions{TTL: time.Hour, Now: c.options()})
	cfg := claimcheck.Config{Store: store, Threshold: 16, RequireDigest: true}
	pm, err := claimcheck.PublishCheckIn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkOut, err := claimcheck.CheckOut(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()
	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	dead, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatal(err)
	}

	var wire []byte
	publish := pm(func(ctx context.Context, topic string, ms []*mq.Message, _ []mq.PublishOption) error {
		wire = bytes.Clone(ms[0].Body)
		return client.Publish(ctx, topic, ms[0])
	})
	if err := publish(ctx, "orders", []*mq.Message{{ID: "o-1", Body: bytes.Repeat([]byte("a large order "), 64)}}, nil); err != nil {
		t.Fatal(err)
	}
	// The lifecycle policy expires the payload before anybody consumes it.
	c.advance(2 * time.Hour)

	var attempts, handled atomic.Int32
	counting := func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			attempts.Add(1)
			return next.Handle(ctx, d)
		})
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			handled.Add(1)
			return nil
		}),
			mq.WithMiddleware(counting, checkOut),
			mq.WithMaxAttempts(5),
			// A backoff long enough that a retried message could not reach
			// the dead-letter topic inside the drain's deadline: a pass
			// here therefore cannot be five fast retries.
			mq.WithRetryBackoff(mq.ConstantBackoff(time.Minute)),
			mq.WithDeadLetter("orders.dlq"))
	}()

	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ms, err := memory.Drain(drainCtx, dead, 1)
	if err != nil {
		t.Fatalf("nothing reached the dead-letter topic: %v (the chain ran %d time(s))", err, attempts.Load())
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("the chain ran %d times, want 1: a payload that is gone cannot come back, so a retry is a wasted round trip that holds the lane", got)
	}
	if handled.Load() != 0 {
		t.Error("the handler ran for a message whose payload is gone")
	}
	dl := ms[0]
	if !bytes.Equal(dl.Body, wire) {
		t.Errorf("the dead letter's body is %d bytes, want the %d-byte reference that was published", len(dl.Body), len(wire))
	}
	ref, err := claimcheck.ParseRef(dl.Body)
	if err != nil {
		t.Fatalf("the dead letter's body is not a parseable reference, so which payload went missing is lost: %v", err)
	}
	if string(ref.Store) != store.Name() {
		t.Errorf("the dead letter's reference names store %q", ref.Store)
	}
	failure, _ := dl.Headers.Get(mq.HeaderFailure)
	if !strings.Contains(string(failure), "no payload") {
		t.Errorf("the failure header is %q, want ErrPayloadGone's text", failure)
	}
}

func TestAGonePayloadHaltsARunnerWithNoDeadLetterTopic(t *testing.T) {
	ctx := context.Background()
	var c clock
	store := fileStore(t, claimcheck.FileOptions{TTL: time.Hour, Now: c.options()})
	cfg := claimcheck.Config{Store: store, Threshold: 16}
	pm, err := claimcheck.PublishCheckIn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkOut, err := claimcheck.CheckOut(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Shutdown(ctx) }()
	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	publish := pm(func(ctx context.Context, topic string, ms []*mq.Message, _ []mq.PublishOption) error {
		return client.Publish(ctx, topic, ms[0])
	})
	if err := publish(ctx, "orders", []*mq.Message{{ID: "o-1", Body: bytes.Repeat([]byte("a large order "), 64)}}, nil); err != nil {
		t.Fatal(err)
	}
	c.advance(2 * time.Hour)

	var attempts atomic.Int32
	counting := func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			attempts.Add(1)
			return next.Handle(ctx, d)
		})
	}
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	runErr := consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		t.Error("the handler ran for a message whose payload is gone")
		return nil
	}),
		mq.WithMiddleware(counting, checkOut),
		mq.WithMaxAttempts(5),
		mq.WithRetryBackoff(mq.ConstantBackoff(time.Minute)))

	var poison *mq.PoisonError
	if !errors.As(runErr, &poison) {
		t.Fatalf("Run = %v, want a *PoisonError: with no dead-letter topic a message that can never succeed halts the runner (ADR-011)", runErr)
	}
	if !errors.Is(runErr, claimcheck.ErrPayloadGone) {
		t.Errorf("the halt does not carry ErrPayloadGone: %v", runErr)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("the chain ran %d times before the halt, want 1", got)
	}
}

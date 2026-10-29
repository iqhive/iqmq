package claimcheck_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/claimcheck"
)

// TestADeadLetteredClaimCarriesTheReference is a message whose handler fails
// for good, on a broker whose size limit is the reason the payload was checked
// in at all. The runner dead-letters the message it RECEIVED, which is the
// reference: the payload CheckOut fetched for the handler is larger than the
// broker accepts, so a copy carrying it was refused, the original requeued,
// and the handler and the refused publication ran again as fast as the broker
// could redeliver.
func TestADeadLetteredClaimCarriesTheReference(t *testing.T) {
	ctx := context.Background()
	store, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := claimcheck.Config{Store: store, Threshold: 512, RequireDigest: true}
	pm, err := claimcheck.PublishCheckIn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	checkOut, err := claimcheck.CheckOut(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true, DefaultTopology: memory.TopologyConfig{MaxMessageSize: 1024}})
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
	if err := publish(ctx, "orders", []*mq.Message{{ID: "o-1", Body: bytes.Repeat([]byte("x"), 8<<10)}}, nil); err != nil {
		t.Fatal(err)
	}

	var handled atomic.Int32
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			handled.Add(1)
			return errors.New("cannot process")
		}), mq.WithMiddleware(checkOut), mq.WithMaxAttempts(1), mq.WithDeadLetter("orders.dlq"))
	}()
	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ms, err := memory.Drain(drainCtx, dead, 1)
	if err != nil {
		t.Fatalf("nothing reached the dead-letter topic: %v (the handler ran %d times)", err, handled.Load())
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if !bytes.Equal(ms[0].Body, wire) {
		t.Fatalf("the dead letter's body is %d bytes, want the %d-byte reference that was published", len(ms[0].Body), len(wire))
	}
	if _, err := claimcheck.ParseRef(ms[0].Body); err != nil {
		t.Fatalf("the dead letter's body is not a reference: %v", err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("the handler ran %d times, want 1", got)
	}
	if s := consumer.Stats(); s.DeadLetterFailures != 0 {
		t.Fatalf("DeadLetterFailures = %d, want 0: the copy fits the broker", s.DeadLetterFailures)
	}
}

package compress_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/compress"
)

// These tests run Decompress through a real runner over the memory driver,
// because what they pin is what the runner does with a delivery Decompress has
// unframed: it dead-letters and retries the framed message it RECEIVED, and
// never the expanded body Decompress put in its place for the handler.

var payload = bytes.Repeat([]byte("a large order line "), 256)

// framedTopic opens a memory client with a consumer on "orders" and one on
// "orders.dlq", and publishes one framed message to "orders".
func framedTopic(t *testing.T, cfg compress.Config) (consumer, dead *mq.ConsumerHandle, wire []byte) {
	t.Helper()
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Shutdown(ctx) })
	consumer, err = client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatal(err)
	}
	dead, err = client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders.dlq", Subscription: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	pm, err := compress.PublishCompress(cfg)
	if err != nil {
		t.Fatal(err)
	}
	publish := pm(func(ctx context.Context, topic string, ms []*mq.Message, _ []mq.PublishOption) error {
		wire = bytes.Clone(ms[0].Body)
		return client.Publish(ctx, topic, ms[0])
	})
	m := &mq.Message{ID: "o-1", Key: []byte("acct"), Body: bytes.Clone(payload)}
	if err := publish(ctx, "orders", []*mq.Message{m}, nil); err != nil {
		t.Fatal(err)
	}
	return consumer, dead, wire
}

func TestADeadLetteredMessageStaysFramed(t *testing.T) {
	cfg := compress.Config{}
	consumer, dead, wire := framedTopic(t, cfg)
	mw, err := compress.Decompress(cfg, compress.UnframedFail)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			return errors.New("cannot process")
		}), mq.WithMiddleware(mw), mq.WithMaxAttempts(1), mq.WithDeadLetter("orders.dlq"))
	}()
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ms, err := memory.Drain(drainCtx, dead, 1)
	if err != nil {
		t.Fatalf("nothing reached the dead-letter topic: %v", err)
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if !bytes.Equal(ms[0].Body, wire) {
		t.Fatalf("the dead-letter copy is %d bytes, want the %d-byte frame that was published", len(ms[0].Body), len(wire))
	}
}

func TestAStrictRetryUnframesTheBodyAgain(t *testing.T) {
	cfg := compress.Config{}
	consumer, _, _ := framedTopic(t, cfg)
	mw, err := compress.Decompress(cfg, compress.UnframedFail)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var bodies [][]byte
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			mu.Lock()
			defer mu.Unlock()
			bodies = append(bodies, bytes.Clone(d.Body))
			if len(bodies) == 1 {
				return errors.New("the database blinked")
			}
			return nil
		}),
			mq.WithMiddleware(mw),
			mq.WithOrderingByKey(),
			mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(bodies)
		mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("the runner returned after one transient failure: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the retry never reached the handler")
		}
		time.Sleep(time.Millisecond)
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(bodies[0], payload) || !bytes.Equal(bodies[1], payload) {
		t.Fatalf("the handler was given %d and %d bytes, want the %d-byte payload on both attempts", len(bodies[0]), len(bodies[1]), len(payload))
	}
}

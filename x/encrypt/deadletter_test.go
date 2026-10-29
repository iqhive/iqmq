package encrypt_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/x/encrypt"
)

// These tests run Decrypt through a real runner over the memory driver,
// because what they pin is what the runner does with a delivery Decrypt has
// opened: it dead-letters and retries the message it RECEIVED, sealed, and
// never the plaintext Decrypt put in its place for the handler.

const secret = "card=4111111111111111;cvv=123"

// sealedTopic opens a memory client with a consumer on "orders" and one on
// "orders.dlq", and publishes one sealed message to "orders".
func sealedTopic(t *testing.T, ring *encrypt.StaticKeyring) (consumer, dead *mq.ConsumerHandle) {
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
	pm, err := encrypt.PublishEncrypt(encrypt.Config{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	publish := pm(func(ctx context.Context, topic string, ms []*mq.Message, _ []mq.PublishOption) error {
		return client.Publish(ctx, topic, ms[0])
	})
	m := &mq.Message{ID: "o-1", Key: []byte("acct"), Body: []byte(secret)}
	if err := publish(ctx, "orders", []*mq.Message{m}, nil); err != nil {
		t.Fatal(err)
	}
	return consumer, dead
}

func TestADeadLetteredMessageStaysSealed(t *testing.T) {
	ring := mustRing(t)
	consumer, dead := sealedTopic(t, ring)
	dec, err := encrypt.Decrypt(encrypt.Config{Keys: ring, RequireSealed: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			return mq.DeadLetter(errors.New("cannot process"))
		}), mq.WithMiddleware(dec), mq.WithDeadLetter("orders.dlq"))
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
	body := ms[0].Body
	if !encrypt.Sealed(body) || bytes.Contains(body, []byte(secret)) {
		t.Fatalf("the dead-letter copy is not sealed: %q", body)
	}
	o, err := encrypt.NewOpener(encrypt.Config{Keys: ring})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := o.Open(context.Background(), body)
	if err != nil || string(plain) != secret {
		t.Fatalf("opening the dead-letter copy = %q, %v; want the plaintext", plain, err)
	}
}

func TestAStrictRetryOpensTheEnvelopeAgain(t *testing.T) {
	ring := mustRing(t)
	consumer, _ := sealedTopic(t, ring)
	dec, err := encrypt.Decrypt(encrypt.Config{Keys: ring, RequireSealed: true})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var bodies []string
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			mu.Lock()
			defer mu.Unlock()
			bodies = append(bodies, string(d.Body))
			if len(bodies) == 1 {
				return errors.New("the database blinked")
			}
			return nil
		}),
			mq.WithMiddleware(dec),
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
	if bodies[0] != secret || bodies[1] != secret {
		t.Fatalf("the handler was given %q, want the plaintext on both attempts", bodies)
	}
}

// mustRing is a one-key ring.
func mustRing(t *testing.T) *encrypt.StaticKeyring {
	t.Helper()
	ring, err := encrypt.NewStaticKeyring(bytes.Repeat([]byte{0x2a}, 32), "k1")
	if err != nil {
		t.Fatal(err)
	}
	return ring
}

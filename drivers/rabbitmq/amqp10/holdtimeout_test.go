package amqp10_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rmq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/dsn"
)

// TestAHoldIsCutInsideTheConsumerTimeoutOnABroker is a delivery Run holds for
// longer than the broker's consumer timeout, over AMQP 1.0 on RabbitMQ 4.3: a
// `consumer-timeout` policy of 10 s on a quorum queue, Config.ConsumerTimeout
// to match, and a handler that asks for 60 s, by a postponement or by a
// failure under a 60 s backoff. This profile has no delayed redelivery, so Run
// keeps the delivery itself.
//
// Measured before deliveries reported the timeout as their lease (the root
// reviewer's probe, and this test): RabbitMQ took the delivery back at the
// timeout and sent the consumer nothing more, while Health stayed Ready and
// Run returned nil. Now Run gives the delivery back within half the lease, it
// comes back about 5 s after it was first handled or sooner, and a message
// published afterwards is handled.
//
// IQMQ_RABBITMQ_ADDR is the broker and IQMQ_RABBITMQ_CONTAINER its container,
// for `rabbitmqadmin` (the topology: this profile declares nothing) and
// `rabbitmqctl` (the policy).
func TestAHoldIsCutInsideTheConsumerTimeoutOnABroker(t *testing.T) {
	addr, container := os.Getenv("IQMQ_RABBITMQ_ADDR"), os.Getenv("IQMQ_RABBITMQ_CONTAINER")
	if addr == "" || container == "" {
		t.Skip("set IQMQ_RABBITMQ_ADDR and IQMQ_RABBITMQ_CONTAINER to run this against RabbitMQ 4.3")
	}
	const timeout = 10 * time.Second
	for i, tc := range []struct {
		name  string
		first func() error
	}{
		{"postponement", func() error { return mq.Postpone(nil, 60*time.Second) }},
		{"retry", func() error { return errors.New("handler failed") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := "iqmq-test-hold10-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(i) + "."
			exchange, queue := prefix+"orders", prefix+"orders.workers"
			docker(t, container, "rabbitmqadmin", "declare", "exchange", "--name", exchange, "--type", "topic", "--durable", "true")
			docker(t, container, "rabbitmqadmin", "declare", "queue", "--name", queue, "--type", "quorum", "--durable", "true")
			docker(t, container, "rabbitmqadmin", "declare", "binding", "--source", exchange,
				"--destination-type", "queue", "--destination", queue, "--routing-key", "orders")
			policy := prefix + "consumer-timeout"
			docker(t, container, "rabbitmqctl", "set_policy", policy, "^"+queue+"$",
				`{"consumer-timeout":10000}`, "--apply-to", "queues")
			t.Cleanup(func() {
				docker(t, container, "rabbitmqctl", "clear_policy", policy)
				docker(t, container, "rabbitmqadmin", "delete", "queue", "--name", queue)
				docker(t, container, "rabbitmqadmin", "delete", "exchange", "--name", exchange)
			})
			cl, err := rmq10.Open(t.Context(), rmq10.Config{
				Addrs: []string{addr}, Username: "guest", Password: dsn.Secret("guest"),
				InsecurePlainSASL: true, Prefix: prefix, ConsumerTimeout: timeout,
			})
			if err != nil {
				t.Fatalf("opening a client: %v", err)
			}
			t.Cleanup(func() { _ = cl.Close() })
			cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
			if err != nil {
				t.Fatal(err)
			}
			pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pub.Close() })
			var mu sync.Mutex
			var calls []time.Time
			called := make(chan struct{}, 8)
			handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				mu.Lock()
				calls = append(calls, time.Now())
				n := len(calls)
				mu.Unlock()
				called <- struct{}{}
				if n == 1 {
					return tc.first()
				}
				return nil
			})
			ctx, cancel := context.WithCancel(t.Context())
			ran := make(chan error, 1)
			go func() {
				ran <- cons.Run(ctx, handler, mq.WithRetryBackoff(mq.ConstantBackoff(60*time.Second)))
			}()
			defer func() {
				cancel()
				if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Run: %v", err)
				}
			}()
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
				t.Fatal(err)
			}
			wait(t, called, 10*time.Second, "the first delivery")
			wait(t, called, 30*time.Second, "the held delivery again")
			mu.Lock()
			back := calls[1].Sub(calls[0])
			mu.Unlock()
			t.Logf("the held delivery came back %v after it was first handled", back.Round(10*time.Millisecond))
			if back > timeout/2+3*time.Second {
				t.Errorf("the held delivery came back %v after it was first handled, want within half the %v "+
					"consumer timeout", back, timeout)
			}
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
				t.Fatal(err)
			}
			wait(t, called, 10*time.Second, "a message published afterwards")
		})
	}
}

// docker runs a command in the broker's container and fails the test if it
// fails.
func docker(t *testing.T, container string, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", append([]string{"exec", container}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %v: %s", args, err, out)
	}
}

// wait waits for one handler call for at most within.
func wait(t *testing.T, called <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-called:
	case <-time.After(within):
		t.Fatalf("%s was not handled within %v", what, within)
	}
}

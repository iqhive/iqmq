package stomp_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/testkit"
)

// A requeue on this driver is a subscription reset ([stomp.Source] doc), and
// the reset swaps the source's buffer, which is what wakes a fetch parked on
// the old one. The parked fetch read that wake as the connection's death and
// returned `Unavailable: connection closed unexpectedly`. Run returns the
// error a fetch returns, so Run over STOMP ended at the first requeue its own
// handler caused, and the client still reported itself Ready. The tests here
// hold a fetch parked across a reset, on the in-process server and on both
// brokers.

// parkSignal is a context that says when a fetch parks on it.
//
// A fetch on a live subscription reads ctx.Done() for the first time in the
// buffer's own select, after it has registered for the buffer's wake-up and
// let the buffer's lock go, so the first call is the moment it parks: a reset
// after it wakes that fetch and no other.
type parkSignal struct {
	context.Context
	once   sync.Once
	parked chan struct{}
}

// Done reports the park once, then answers for the context it wraps.
func (c *parkSignal) Done() <-chan struct{} {
	c.once.Do(func() { close(c.parked) })
	return c.Context.Done()
}

// TestARequeueDoesNotEndAParkedFetch parks a fetch, requeues a delivery the
// caller holds under it, and expects the same fetch to go on waiting and to
// return the delivery the broker sends to the new subscription.
func TestARequeueDoesNotEndAParkedFetch(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		t.Run(broker.String(), func(t *testing.T) {
			t.Parallel()
			srv := brokerServer(t, broker)
			cfg := settings(broker)
			cfg.Addrs = []string{srv.Addr()}
			src, consume := openSource(t, openBackend(t, cfg), cfg, srv, "orders", "workers", 8)
			deliver(t, srv, consume, body("one"))
			held := fetchN(t, src, 1)

			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			park := &parkSignal{Context: ctx, parked: make(chan struct{})}
			type answer struct {
				body []byte
				err  error
			}
			fetched := make(chan answer, 1)
			go func() {
				dst := make([]mq.DriverDelivery, 1)
				n, err := src.Fetch(park, dst)
				var b []byte
				if n > 0 {
					b = append(b, dst[0].Message.Body...)
				}
				fetched <- answer{b, err}
			}()
			<-park.parked
			if err := src.Settler().Nack(ctx, &held[0], nackOptions(t, mq.NackRequeue())); err != nil {
				t.Fatalf("Nack(NackRequeue()): %v", err)
			}
			if err := srv.WaitSubscribed(consume, 2, budget); err != nil {
				t.Fatalf("the requeue did not subscribe again: %v", err)
			}
			deliver(t, srv, consume, body("again"))
			got := <-fetched
			if got.err != nil {
				t.Fatalf("the fetch parked across the requeue returned %v (code %v): a requeue is this driver's own "+
					"subscription reset, not a failure, and Run ends on the error a fetch returns", got.err,
					mq.CodeOf(got.err))
			}
			if string(got.body) != "again" {
				t.Fatalf("the parked fetch returned %q, want the delivery sent after the reset", got.body)
			}
		})
	}
}

// TestRunSurvivesARequeue is the same through the client: a handler fails a
// delivery once, Run requeues it, and Run goes on to handle the next message
// rather than returning the parked fetch's error.
func TestRunSurvivesARequeue(t *testing.T) {
	t.Parallel()
	for _, broker := range activemq.Brokers() {
		t.Run(broker.String(), func(t *testing.T) {
			t.Parallel()
			srv := brokerServer(t, broker)
			cfg := settings(broker)
			cfg.Addrs = []string{srv.Addr()}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			c, err := stomp.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			_, consume := destinations(t, cfg, "orders", "workers")
			if err := srv.WaitSubscribed(consume, 1, budget); err != nil {
				t.Fatalf("the consumer never subscribed: %v", err)
			}
			var failed atomic.Bool
			handled := make(chan string, 4)
			h := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				if string(d.Body) == "poison" && failed.CompareAndSwap(false, true) {
					// Long enough for Run's fetch loop to park again before
					// the requeue resets the subscription under it; the
					// fetch-level test above lands the reset on a parked
					// fetch without a clock.
					time.Sleep(50 * time.Millisecond)
					return errors.New("the first attempt fails")
				}
				handled <- string(d.Body)
				return nil
			})
			runCtx, stop := context.WithCancel(ctx)
			ran := make(chan error, 1)
			go func() { ran <- cons.Run(runCtx, h) }()
			deliver(t, srv, consume, body("poison"))
			if err := srv.WaitSubscribed(consume, 2, budget); err != nil {
				t.Fatalf("the requeue did not subscribe again: %v (Run: %v)", err, runResult(ran))
			}
			deliver(t, srv, consume, body("after"))
			select {
			case got := <-handled:
				if got != "after" {
					t.Fatalf("Run handled %q, want the message sent after the requeue", got)
				}
			case err := <-ran:
				t.Fatalf("Run returned %v (code %v) after its handler's requeue: the reset of the subscription "+
					"is not a failure", err, mq.CodeOf(err))
			case <-ctx.Done():
				t.Fatalf("Run handled nothing after the requeue (Run: %v)", runResult(ran))
			}
			stop()
			if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v once stopped, want context.Canceled or nil", err)
			}
			if h := c.Health(); h.State != mq.HealthReady {
				t.Errorf("health is %v after a requeue, want Ready", h.State)
			}
		})
	}
}

// runResult is what Run returned, or that it is still running.
func runResult(ran chan error) string {
	select {
	case err := <-ran:
		ran <- err
		if err == nil {
			return "returned nil"
		}
		return "returned " + err.Error()
	default:
		return "still running"
	}
}

// TestARequeueDoesNotEndAParkedReceiveOnABroker parks a Receive on a real
// broker, requeues one of two deliveries the caller holds, and expects the
// parked Receive to return a redelivery rather than an error; then Run, whose
// handler fails a message once, handles the next one.
//
// The reset gives back every unacknowledged delivery of the subscription, so
// both come back, and the one the caller still held loses its lease.
func TestARequeueDoesNotEndAParkedReceiveOnABroker(t *testing.T) {
	for name, cfg := range brokersAt(t) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := stomp.Provision(ctx, cfg, testkit.Need{Topic: "requeue", Subscriptions: []string{"workers"}}); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			c, err := stomp.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: "requeue"})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: "requeue", Subscription: "workers"})
			if err != nil {
				t.Fatalf("Consumer: %v", err)
			}
			for _, b := range []string{"a", "b"} {
				if err := pub.Publish(ctx, &mq.Message{Body: []byte(b)}); err != nil {
					t.Fatalf("Publish(%s): %v", b, err)
				}
			}
			receive := func(ctx context.Context) (*mq.Delivery, error) {
				dst := make([]mq.Delivery, 1)
				if _, err := cons.Receive(ctx, dst); err != nil {
					return nil, err
				}
				return &dst[0], nil
			}
			var first []*mq.Delivery
			for len(first) < 2 {
				d, err := receive(ctx)
				if err != nil {
					t.Fatalf("Receive: %v", err)
				}
				first = append(first, d)
			}
			parked := make(chan error, 1)
			again := make(chan *mq.Delivery, 1)
			go func() {
				d, err := receive(ctx)
				parked <- err
				again <- d
			}()
			// No clock can say the Receive has parked on a real broker; it
			// has nothing to read, so a moment is enough, and the in-process
			// tests above land the reset on a parked fetch exactly.
			time.Sleep(200 * time.Millisecond)
			if err := first[0].Nack(); err != nil {
				t.Fatalf("Nack (a requeue): %v", err)
			}
			if err := <-parked; err != nil {
				t.Fatalf("the Receive parked across the requeue returned %v (code %v)", err, mq.CodeOf(err))
			}
			// Artemis marks neither copy `redelivered` after an UNSUBSCRIBE
			// (measured), so the body is what says this is a redelivery.
			d := <-again
			if b := string(d.Body); b != "a" && b != "b" {
				t.Errorf("the parked Receive returned %q, want a or b back", b)
			}
			if err := d.Ack(); err != nil {
				t.Errorf("Ack: %v", err)
			}
			if d, err := receive(ctx); err != nil {
				t.Errorf("the second redelivery: %v", err)
			} else if err := d.Ack(); err != nil {
				t.Errorf("Ack: %v", err)
			}

			// Run, whose handler fails "poison" once.
			var failed atomic.Bool
			handled := make(chan string, 4)
			h := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				if string(d.Body) == "poison" && failed.CompareAndSwap(false, true) {
					time.Sleep(200 * time.Millisecond)
					return errors.New("the first attempt fails")
				}
				handled <- string(d.Body)
				return nil
			})
			runCtx, stop := context.WithCancel(ctx)
			ran := make(chan error, 1)
			go func() { ran <- cons.Run(runCtx, h) }()
			if err := pub.Publish(ctx, &mq.Message{Body: []byte("poison")}); err != nil {
				t.Fatalf("Publish(poison): %v", err)
			}
			seen := map[string]bool{}
			published := false
			for !seen["after"] {
				select {
				case got := <-handled:
					seen[got] = true
					if got == "poison" && !published {
						published = true
						if err := pub.Publish(ctx, &mq.Message{Body: []byte("after")}); err != nil {
							t.Fatalf("Publish(after): %v", err)
						}
					}
				case err := <-ran:
					t.Fatalf("Run returned %v (code %v) after its handler's requeue", err, mq.CodeOf(err))
				case <-ctx.Done():
					t.Fatalf("Run handled %v and then nothing", seen)
				}
			}
			stop()
			if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v once stopped", err)
			}
		})
	}
}

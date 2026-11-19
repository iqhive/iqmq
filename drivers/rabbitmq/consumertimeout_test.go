package rabbitmq_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/testkit"
)

// TestTheConsumerTimeoutIsADSNSetting is Config.ConsumerTimeout through the
// DSN: `consumertimeout` sets it, a negative value (the broker has none)
// included, and both render back so that they round-trip.
func TestTheConsumerTimeoutIsADSNSetting(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want time.Duration
	}{
		{"rabbitmq://host", 0},
		{"rabbitmq://host?consumertimeout=10m", 10 * time.Minute},
		{"rabbitmq://host?consumertimeout=-1s", -time.Second},
	} {
		cfg, err := rq.ParseDSN(tc.in)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", tc.in, err)
		}
		if cfg.ConsumerTimeout != tc.want {
			t.Errorf("ParseDSN(%q).ConsumerTimeout = %v, want %v", tc.in, cfg.ConsumerTimeout, tc.want)
		}
		again, err := rq.ParseDSN(cfg.DSN())
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", cfg.DSN(), err)
		}
		if again.ConsumerTimeout != tc.want {
			t.Errorf("%q rendered as %q, which parses to %v", tc.in, cfg.DSN(), again.ConsumerTimeout)
		}
		if tc.want == 0 && strings.Contains(cfg.DSN(), "consumertimeout") {
			t.Errorf("the default rendered as %q", cfg.DSN())
		}
	}
}

// TestADeliveryReportsTheConsumerTimeoutAsItsLease is the lease a delivery
// reports: its ReceivedAt plus Config.ConsumerTimeout, 30 minutes when that is
// zero, and none when it is negative or when the queue is a classic one on
// RabbitMQ 4.3 or later, which times out no classic queue's consumer.
func TestADeliveryReportsTheConsumerTimeoutAsItsLease(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version string
		queue   rq.QueueType
		timeout time.Duration
		want    time.Duration
	}{
		{"quorum, the default", "4.3.5", rq.QueueQuorum, 0, 30 * time.Minute},
		{"quorum, configured", "4.3.5", rq.QueueQuorum, 10 * time.Minute, 10 * time.Minute},
		{"quorum, none", "4.3.5", rq.QueueQuorum, -1, 0},
		{"classic on 4.2", "4.2.9", rq.QueueClassic, 10 * time.Minute, 10 * time.Minute},
		{"classic on 4.3", "4.3.5", rq.QueueClassic, 10 * time.Minute, 0},
		{"classic on 4.10", "4.10.0", rq.QueueClassic, 0, 0},
		{"quorum on 3.13", "3.13.7", rq.QueueQuorum, 0, 30 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, cl := openFakeWith(t, rabbitfake.Config{Version: tc.version}, func(c *rq.Config) {
				c.Declare = true
				c.QueueType = tc.queue
				c.ConsumerTimeout = tc.timeout
			})
			cons := consumeOn(t, cl, "orders", "workers")
			if _, err := broker.Publish("", "orders.workers", amqpBody("m")); err != nil {
				t.Fatal(err)
			}
			d := receiveOne(t, cons)
			defer func() { _ = d.Ack() }()
			info := d.Info()
			switch got := info.LeaseExpires; {
			case tc.want == 0 && !got.IsZero():
				t.Errorf("LeaseExpires = %v, want none", got)
			case tc.want != 0 && got.Sub(info.ReceivedAt) != tc.want:
				t.Errorf("LeaseExpires is %v after ReceivedAt (%v), want %v", got.Sub(info.ReceivedAt), got, tc.want)
			}
		})
	}
}

// TestAHoldOnAQuorumQueueIsCutInsideTheConsumerTimeout is a delivery Run holds
// for longer than the broker's consumer timeout, on a quorum queue: by a
// postponement, and by a retry whose delay the driver refuses (30 hours, past
// the delay ladder's top) and which Run, from iqmq v1.1.3, holds itself.
//
// RabbitMQ takes a delivery back once its consumer has held it for the
// timeout: 4.3 cancels the consumer, 3.13 to 4.2 close the channel and take
// back every delivery on it, and over AMQP 1.0 a 4.3 consumer stops receiving
// for good. With no lease reported, Run held the delivery for the whole delay
// and met that. The delivery now reports the timeout as its lease, so Run
// gives it back within half of it and the redelivery is handled long before
// the broker would act; the consumer then goes on receiving. The broker and
// Run share one clock that only the test moves.
func TestAHoldOnAQuorumQueueIsCutInsideTheConsumerTimeout(t *testing.T) {
	const timeout = 10 * time.Minute
	for _, tc := range []struct {
		name  string
		first func() error
	}{
		{"postponement", func() error { return mq.Postpone(nil, 30*time.Hour) }},
		{"retry the driver cannot delay", func() error { return errors.New("handler failed") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := testkit.NewClock(time.Time{})
			broker, cl := openFakeWith(t, rabbitfake.Config{Clock: clk, Version: "4.3.5"}, func(c *rq.Config) {
				c.Declare = true
				c.Clock = clk
				c.ConsumerTimeout = timeout
			})
			cons := consumeOn(t, cl, "orders", "workers")
			start := clk.Now()
			var mu sync.Mutex
			var calls []time.Duration
			called := make(chan struct{}, 8)
			handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				mu.Lock()
				calls = append(calls, clk.Now().Sub(start))
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
				ran <- cons.Run(ctx, handler, mq.WithRetryBackoff(mq.ConstantBackoff(30*time.Hour)))
			}()
			defer func() {
				cancel()
				if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Run: %v", err)
				}
			}()
			if _, err := broker.Publish("", "orders.workers", amqpBody("held")); err != nil {
				t.Fatal(err)
			}
			waitCall(t, called)
			// Move the shared clock to the consumer timeout, a step at a
			// time, giving Run the chance to act at each step.
			for clk.Now().Sub(start) < timeout {
				select {
				case <-called:
				case <-time.After(50 * time.Millisecond):
					clk.Advance(15 * time.Second)
					continue
				}
				break
			}
			mu.Lock()
			got := append([]time.Duration(nil), calls...)
			mu.Unlock()
			if len(got) < 2 {
				t.Fatalf("the held delivery was not handled again before the consumer timeout, %v after it was "+
					"first handled: Run held it past the lease (calls at %v)", timeout, got)
			}
			t.Logf("handled at %v after the first call", got)
			if got[1] > timeout/2+15*time.Second {
				t.Errorf("the held delivery came back %v after it was first handled, want within half the "+
					"%v lease", got[1], timeout)
			}
			if _, err := broker.Publish("", "orders.workers", amqpBody("after")); err != nil {
				t.Fatal(err)
			}
			waitCall(t, called)
		})
	}
}

// amqpBody is a plain publication of body, as another client would make it.
func amqpBody(body string) amqp.Publishing { return amqp.Publishing{Body: []byte(body)} }

// waitCall waits for one handler call.
func waitCall(t *testing.T, called <-chan struct{}) {
	t.Helper()
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was not called within 5s")
	}
}

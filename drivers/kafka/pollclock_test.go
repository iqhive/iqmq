package kafka_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
	"github.com/iqhive/iqmq/internal/clock"
)

// pollClock is the internal fake clock as an mq.Clock.
type pollClock struct{ *clock.Fake }

func (c pollClock) NewTimer(d time.Duration) mq.Timer { return c.Fake.NewTimer(d) }

// TestAHeldRedeliveryWakesFetchOnTheConfiguredClock: a nack's delay is due on
// Config.Clock, and a Fetch with nothing else to do waits until it is. The
// poll's bound was a wall-clock deadline at the due time, so with the
// configured clock ahead of the wall clock a redelivery the configured clock
// had made due was not handed out until the Fetch's own context ended.
// kafkago's test of the same name is this test for that driver.
func TestAHeldRedeliveryWakesFetchOnTheConfiguredClock(t *testing.T) {
	fake := clock.NewFake(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
	topic := newTopic("pollclock")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.Clock = pollClock{fake} })
	publish(t, c, topic, &mq.Message{Body: []byte("held")})
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: topic, Subscription: "g-pollclock", Prefetch: 16, Start: mq.StartEarliest(),
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	d := fetchN(t, t.Context(), src, 1)[0]
	if err := src.Settler().Nack(t.Context(), &d, mq.NackOptions{Action: mq.NackActionRequeue, Delay: time.Minute}); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	type result struct {
		n   int
		err error
		at  time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
		defer cancel()
		n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1))
		done <- result{n, err, time.Since(start)}
	}()
	time.Sleep(300 * time.Millisecond)
	fake.Advance(2 * time.Minute)
	r := <-done
	if r.n != 1 || r.at > 3*time.Second {
		t.Fatalf("the held redelivery was not handed out when Config.Clock passed its due time: Fetch = (%d, %v) after %v", r.n, r.err, r.at)
	}
}

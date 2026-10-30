package kafkago

import (
	"context"
	"sync"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/clock"
)

// fakeClock is the internal fake clock as an mq.Clock.
type fakeClock struct{ *clock.Fake }

func (c fakeClock) NewTimer(d time.Duration) mq.Timer { return c.Fake.NewTimer(d) }

// advanceUntil advances the fake clock by step until done is closed, or fails
// once the wall-clock budget runs out.
func advanceUntil(t *testing.T, fake *clock.Fake, step time.Duration, done <-chan struct{}, what string) {
	t.Helper()
	budget := time.Now().Add(10 * time.Second)
	for time.Now().Before(budget) {
		fake.Advance(step)
		select {
		case <-done:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("%s did not end when the configured clock passed it", what)
}

// TestTheGenerationWaitRunsOnTheConfiguredClock: Config.Clock is the time
// source the driver reads, and RevokeWait is a time it waits. The wait read
// its deadline from the configured clock and woke on a wall-clock timer, so a
// generation whose clock had passed RevokeWait still waited for a settlement
// that never came.
func TestTheGenerationWaitRunsOnTheConfiguredClock(t *testing.T) {
	fake := clock.NewFake(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	p := &partState{partition: 0, inflight: 1}
	b := &Backend{cfg: Config{RevokeWait: time.Minute}, clock: fakeClock{fake}}
	s := &source{b: b, topic: "t", parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1}
	s.cond = sync.NewCond(&s.mu)
	s.released = make(chan struct{}, 1)

	var settled bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		settled = s.waitInflight(map[int]bool{0: true})
	}()
	t.Cleanup(func() {
		// A wait that never ended is released, so it does not outlive the
		// test.
		s.mu.Lock()
		s.closed = true
		s.signalReleased()
		s.mu.Unlock()
		<-done
	})
	advanceUntil(t, fake, time.Minute, done, "the generation's wait")
	if settled {
		t.Fatal("the wait reported a delivery that never settled as settled")
	}
}

// TestAHeldRedeliveryWakesFetchOnTheConfiguredClock: a nack's delay is due on
// the configured clock, and a Fetch with nothing else to do waits until it is.
// The wait was a wall-clock timer for the time until the due time, so a
// redelivery the configured clock had made due was not handed out until a
// record arrived or the Fetch's context ended.
func TestAHeldRedeliveryWakesFetchOnTheConfiguredClock(t *testing.T) {
	// A clock ahead of the wall clock: the wall-clock timer waits for the
	// years between them.
	fake := clock.NewFake(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
	held := heldRecord{msg: &kgo.Message{Partition: 0, Offset: 10}, due: fake.Now().Add(time.Minute)}
	p := &partState{partition: 0, held: []heldRecord{held}}
	b := &Backend{clock: fakeClock{fake}}
	s := &source{
		b: b, topic: "t", parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1,
		recs: newRecQueue(1), wake: make(chan struct{}, 1),
	}
	s.cond = sync.NewCond(&s.mu)
	s.ctx, s.cancel = context.WithCancel(context.Background())

	var (
		n   int
		err error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		n, err = s.poll(t.Context(), make([]mq.DriverDelivery, 1))
	}()
	t.Cleanup(func() {
		// A wait that never ended is released, so it does not outlive the
		// test.
		s.cancel()
		<-done
	})
	advanceUntil(t, fake, time.Minute, done, "the wait for a held redelivery")
	if n != 0 || err != nil {
		t.Fatalf("the wait for a held redelivery = (%d, %v), want (0, nil)", n, err)
	}
}

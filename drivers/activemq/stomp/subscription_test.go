package stomp_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// never is an unsubscribe receipt delay no test outlives: the server closes
// first, which is what ends its wait.
const never = time.Hour

// slowUnsubscribeServer is an Artemis server that answers an UNSUBSCRIBE's
// receipt only after delay.
func slowUnsubscribeServer(t *testing.T, delay time.Duration) *stompsrv.Server {
	t.Helper()
	return newServer(t, stompsrv.Options{
		ServerHeader:            activemq.ServerArtemis + "/2.44.0",
		UnsubscribeReceiptDelay: delay,
	})
}

// TestARequeueRacingAFetchLeavesOneSubscription is the reset/fetch race. A
// requeue's reset cleared the live subscription and then waited for its
// UNSUBSCRIBE receipt; a fetch in that window saw no subscription and sent a
// SUBSCRIBE of its own, and the reset then sent another. The connection carried
// two subscriptions, and the one the source no longer read had its deliveries
// dropped unacknowledged until the socket went.
func TestARequeueRacingAFetchLeavesOneSubscription(t *testing.T) {
	t.Parallel()
	srv := slowUnsubscribeServer(t, 300*time.Millisecond)
	cfg := artemisConfig(srv)
	cfg.ReceiptTimeout = budget
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	nacked := make(chan error, 1)
	go func() { nacked <- src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())) }()
	if err := srv.WaitFrames(frame.UNSUBSCRIBE, 1, budget); err != nil {
		t.Fatalf("the requeue never unsubscribed: %v", err)
	}
	fctx, fcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer fcancel()
	_, _ = src.Fetch(fctx, make([]mq.DriverDelivery, 1))
	if err := <-nacked; err != nil {
		t.Fatalf("Nack(requeue): %v", err)
	}
	if err := srv.WaitSubscribed(consume, 2, budget); err != nil {
		t.Fatalf("the reset never re-subscribed: %v", err)
	}
	if got := srv.Live(consume); got != 1 {
		t.Errorf("%d live subscriptions on %s after a requeue raced a fetch, want 1.\nframes:%s",
			got, consume, frameLog(srv))
	}
}

// TestARequeueWhoseUnsubscribeIsNeverAnsweredKeepsConsuming is the reset's
// early return. An UNSUBSCRIBE whose receipt timed out ended the reset with no
// new subscription, the old one possibly still on the socket, and the requeue
// reported failed; the consumer stopped receiving.
func TestARequeueWhoseUnsubscribeIsNeverAnsweredKeepsConsuming(t *testing.T) {
	t.Parallel()
	srv := slowUnsubscribeServer(t, never)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())); err != nil {
		t.Fatalf("Nack(requeue) whose unsubscribe went unanswered: %v\nframes:%s", err, frameLog(srv))
	}
	if err := srv.WaitLive(consume, 1, budget); err != nil {
		t.Fatalf("no live subscription after the requeue: %v\nframes:%s", err, frameLog(srv))
	}
	deliver(t, srv, consume, body("two"))
	got := fetchN(t, src, 1)
	if string(got[0].Message.Body) != "two" {
		t.Errorf("fetched %q after the requeue, want %q", got[0].Message.Body, "two")
	}
}

// TestCloseReturnsWhenTheBrokerNeverAnswersTheUnsubscribe is Close's wait. It
// waited for the pumps before it disconnected, and a pump ends only when its
// channel closes -- which go-stomp does, for a subscription whose UNSUBSCRIBE
// timed out, only when the connection goes. So Close never returned.
func TestCloseReturnsWhenTheBrokerNeverAnswersTheUnsubscribe(t *testing.T) {
	t.Parallel()
	srv := slowUnsubscribeServer(t, never)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)
	closesWithin(t, src)
}

// TestCloseReturnsWithAPumpParkedOnAFullBuffer is Close's order. It
// unsubscribed before it closed the buffer, so a pump parked on a full buffer
// read nothing, go-stomp's channel filled, its single process loop stopped,
// the RECEIPT was never read, and the library's own send of the timeout into
// the full channel blocked Close for ever.
func TestCloseReturnsWithAPumpParkedOnAFullBuffer(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 1)
	msgs := make([]stompsrv.Message, 40)
	for i := range msgs {
		msgs[i] = body(fmt.Sprint(i))
	}
	deliver(t, srv, consume, msgs...)
	// Nothing the source exposes says its pump is parked and every frame
	// read, so the test gives forty loopback frames time to arrive.
	time.Sleep(200 * time.Millisecond)
	closesWithin(t, src)
}

// closesWithin closes src and fails the test if Close does not return within
// the budget. Close's error is not the point: an unanswered UNSUBSCRIBE is
// reported, and that is correct.
func closesWithin(t *testing.T, src mq.Source) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = src.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(budget):
		t.Fatal("Close did not return")
	}
}

// pausingClock is the wall clock, except that the first Now after Pause blocks
// until Resume.
//
// It is how a test holds the pump at one exact point: [stomp.Source] reads the
// clock once per delivery, for `ReceivedAt`, after the delivery has been
// decoded and has claimed its token slot and before it is pushed into the
// buffer -- which is the window between the pump's "am I still the current
// subscription?" and the push.
type pausingClock struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newPausingClock() *pausingClock {
	return &pausingClock{entered: make(chan struct{}), release: make(chan struct{})}
}

// Pause makes the next Now block.
func (c *pausingClock) Pause() { c.armed.Store(true) }

// Resume lets the blocked Now return. It must be called exactly once.
func (c *pausingClock) Resume() { close(c.release) }

// Now is the current time, after the pause when one is armed.
func (c *pausingClock) Now() time.Time {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return time.Now()
}

// NewTimer is a timer on the wall clock.
func (c *pausingClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{t: time.NewTimer(d)} }

// wallTimer adapts *time.Timer to mq.Timer.
type wallTimer struct{ t *time.Timer }

func (w wallTimer) C() <-chan time.Time        { return w.t.C }
func (w wallTimer) Stop() bool                 { return w.t.Stop() }
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// TestADeliveryReadBeforeARequeueIsNotHandedOverAfterIt is the pump's window
// between its supersession check and its push.
//
// The pump asked "is my subscription still the current one?", then decoded the
// delivery and claimed it a slot, and then pushed it into whatever buffer the
// source had BY THEN. A requeue landing in between bumps the generation,
// invalidates the table and swaps in a fresh buffer -- so the superseded
// subscription's delivery went into the new one, and the caller fetched it
// after its Nack had returned: already unsettleable (its Ack reports a lost
// lease), and redelivered by the broker on the new subscription, so one
// message was processed twice. The delivery must go to the buffer the reset
// closed, and be dropped there.
func TestADeliveryReadBeforeARequeueIsNotHandedOverAfterIt(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	clock := newPausingClock()
	cfg := artemisConfig(srv)
	cfg.Clock = clock
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deliver(t, srv, consume, body("first"))
	first := fetchN(t, src, 1)[0]

	// The pump reads "stale" while its subscription is current and stops
	// inside the window.
	clock.Pause()
	deliver(t, srv, consume, body("stale"))
	select {
	case <-clock.entered:
	case <-time.After(budget):
		t.Fatal("the pump never read the clock for the second delivery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	nackErr := src.Settler().Nack(ctx, &first, nackOptions(t, mq.NackRequeue()))
	clock.Resume()
	if nackErr != nil {
		t.Fatalf("Nack(NackRequeue()): %v", nackErr)
	}

	fctx, fcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer fcancel()
	dst := make([]mq.DriverDelivery, 4)
	n, _ := src.Fetch(fctx, dst)
	for i := range n {
		if string(dst[i].Message.Body) == "stale" {
			t.Fatalf("the fetch after the requeue returned %q, which the superseded subscription read before the "+
				"reset. It cannot be settled -- its subscription is gone -- and the broker redelivers it on the new "+
				"one, so the caller processes it twice", dst[i].Message.Body)
		}
	}
}

// TestAFetchAfterARequeueWhoseUnsubscribeWentUnansweredIsRetryable is what the
// requeue leaves behind for the next empty fetch.
//
// An UNSUBSCRIBE whose receipt never arrives makes go-stomp push "channel
// unsubscribe receipt timeout" into the old subscription's channel, the
// superseded pump records it, and the requeue carries on: it drops that
// connection and subscribes on a new one. The next fetch with nothing to hand
// over then reported the recorded sentence as CodeUnknown, not retryable -- so
// a consumer loop stopped on a subscription that had already been re-established.
func TestAFetchAfterARequeueWhoseUnsubscribeWentUnansweredIsRetryable(t *testing.T) {
	t.Parallel()
	srv := slowUnsubscribeServer(t, never)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())); err != nil {
		t.Fatalf("Nack(requeue) whose unsubscribe went unanswered: %v\nframes:%s", err, frameLog(srv))
	}
	if err := srv.WaitLive(consume, 1, budget); err != nil {
		t.Fatalf("no live subscription after the requeue: %v\nframes:%s", err, frameLog(srv))
	}
	err := fetchError(t, src, 300*time.Millisecond)
	if err == nil {
		t.Fatal("the empty fetch after the requeue returned no error, so the fixture no longer records the " +
			"retired subscription's terminal message and this test proves nothing")
	}
	if !mq.IsRetryable(err) || mq.CodeOf(err) != mq.CodeUnavailable {
		t.Errorf("the empty fetch after a requeue reports %v (code %v, retryable %v), want a retryable "+
			"Unavailable: the subscription it is about is the one the requeue retired, and the new one is live",
			err, mq.CodeOf(err), mq.IsRetryable(err))
	}
}

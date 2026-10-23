package stomp_test

import (
	"context"
	"fmt"
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
	// Nothing the source exposes says its pump is parked and the library's
	// channel full, so the test gives forty loopback frames time to arrive.
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

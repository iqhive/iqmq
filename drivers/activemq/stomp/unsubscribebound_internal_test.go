package stomp

import (
	"context"
	"fmt"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// The wait for an UNSUBSCRIBE's receipt, which this driver bounds itself.
//
// go-stomp v3.2.1's Subscription.Unsubscribe, given a receipt timeout of its
// own, sends the timeout into Subscription.C when its timer fires, while the
// library's subscription reader closes that same channel when the connection
// ends; nothing orders the two. A requeue's retire drops the connection when
// its context ends ([Source.retire]), and a context that ended about when the
// library's timer fired made the race detector report the pair and, when the
// close came first, panicked with "send on closed channel" on the goroutine
// that called Nack.

// TestARequeueWhoseDeadlineMeetsTheReceiptTimeoutEndsItsOwnWait is a requeue
// on a broker that never answers the UNSUBSCRIBE, whose deadline is swept
// across Config.ReceiptTimeout in 2.5 microsecond steps, with a deadline of
// twice ReceiptTimeout now and then.
//
// Every round has to be ended by the driver's own drop, which is the one end
// of the wait that does not send on the channel: a round ended by go-stomp's
// timer is the library's send, racing the close. And none may panic. The
// panic is recovered here so that the rounds can be counted; in a program it
// unwound through Settler.Nack into the caller.
func TestARequeueWhoseDeadlineMeetsTheReceiptTimeoutEndsItsOwnWait(t *testing.T) {
	t.Parallel()
	const receipt = 20 * time.Millisecond
	srv, _, s := requeueFixtureWith(t, stompsrv.Options{UnsubscribeReceiptDelay: time.Hour}, func(c *Config) {
		c.ReceiptTimeout = receipt
		// A round that panicked left the source's closed connection in place,
		// and the next subscribe met it closed, which is a lost connection
		// that a source re-dials only when reconnection is on.
		c.NoReconnect = false
		c.ReconnectBase = time.Millisecond
		c.ReconnectMax = 2 * time.Millisecond
	})
	const rounds = 120
	var panics, undropped []string
	for i := range rounds {
		subscribeAgain(t, srv, s, i)
		if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")}); err != nil {
			t.Fatalf("round %d: Deliver: %v", i, err)
		}
		held := fetchAll(t, s, 1)
		// go-stomp's own timer, where there is one, starts a few
		// microseconds after this context, inside Unsubscribe.
		d := receipt + time.Duration(i)*2500*time.Nanosecond
		if i%30 == 29 {
			d = 2 * receipt
		}
		s.mu.Lock()
		before := s.dropped
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), d)
		p, err := nackRecovering(ctx, s, &held[0], requeueOptions(t))
		cancel()
		if p != nil {
			panics = append(panics, fmt.Sprintf("round %d (deadline %v): %v", i, d, p))
			continue
		}
		if err != nil {
			t.Fatalf("round %d: requeue: %v", i, err)
		}
		s.mu.Lock()
		dropped := s.dropped != before
		s.mu.Unlock()
		if !dropped {
			undropped = append(undropped, fmt.Sprintf("round %d (deadline %v)", i, d))
		}
	}
	if len(panics) > 0 {
		t.Errorf("%d of %d requeues panicked inside go-stomp's Unsubscribe:\n%v", len(panics), rounds, panics)
	}
	if len(undropped) > 0 {
		t.Errorf("%d of %d requeues on a broker that never answers the UNSUBSCRIBE were not ended by the driver's "+
			"own drop, so go-stomp's receipt timer ended them, and that timer's send races the channel's close: %v",
			len(undropped), rounds, undropped)
	}
}

// nackRecovering is Nack, with a panic from inside it returned.
func nackRecovering(ctx context.Context, s *Source, d *mq.DriverDelivery, o mq.NackOptions) (p any, err error) {
	defer func() { p = recover() }()
	return nil, s.set.Nack(ctx, d, o)
}

// subscribeAgain makes sure the source has a live subscription again.
func subscribeAgain(t *testing.T, srv *stompsrv.Server, s *Source, round int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	var err error
	for range 20 {
		if err = s.ensureSubscribed(ctx); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("round %d: subscribing: %v", round, err)
	}
	if err := srv.WaitLive(s.Destination(), 1, requeueBudget); err != nil {
		t.Fatalf("round %d: no live subscription: %v", round, err)
	}
}

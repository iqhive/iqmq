package sqs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// One ReceiveMessage on a FIFO queue may return several messages of one
// group, in order, and the service holds the group against later receives
// while ANY of them is in flight. So when the first of them is requeued, the
// later ones the source still holds from that answer keep the group held: the
// requeued message cannot come back until they are settled, and handing them
// out first is exactly the overtaking a requeue under an ordering mode exists
// to prevent.

// publishGroup publishes bodies, in order, into one message group.
func publishGroup(h *harness, name string, bodies ...string) {
	for _, b := range bodies {
		h.publish(name, &mq.Message{Key: []byte("G"), Body: []byte(b)})
	}
}

// fifoHarness is a FIFO client whose single poller has taken the whole group
// into the ring in one receive by the time it returns.
func fifoHarness(t *testing.T) (*harness, string, *mq.ConsumerHandle) {
	t.Helper()
	h := open(t, func(c *sq.Config) {
		c.FIFO = true
		c.Pollers = 1
		c.AckDeadline = time.Hour
	})
	name := topic(t, "fifo-order")
	publishGroup(h, name, "m1", "m2", "m3")
	cons := h.consumer(name, 4)
	return h, name, cons
}

// bodiesOf is the bodies of ds, in order.
func bodiesOf(ds []mq.Delivery) []string {
	out := make([]string, len(ds))
	for i := range ds {
		out[i] = string(ds[i].Body)
	}
	return out
}

// TestARequeuedFIFOMessageComesBackBeforeTheRestOfItsGroup: the consumer took
// m1 of a group whose m2 and m3 came in the same answer, and requeued it. The
// source handed out m2 and m3 next, from its ring, and m1 came back only once
// they were settled. A requeue now gives the later messages of the group back
// to the service as well, without handing them out, so the group comes back
// in its own order. A delayed requeue holds them back for the delay too.
func TestARequeuedFIFOMessageComesBackBeforeTheRestOfItsGroup(t *testing.T) {
	t.Run("at once", func(t *testing.T) {
		h, _, cons := fifoHarness(t)
		first := h.receiveOne(cons, 5*time.Second)
		if string(first.Body) != "m1" {
			t.Fatalf("the first delivery is %q, want m1", first.Body)
		}
		if err := first.Nack(); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		next := h.receive(cons, 3, 5*time.Second)
		if got := bodiesOf(next); got[0] != "m1" || got[1] != "m2" || got[2] != "m3" {
			t.Fatalf("after m1 was requeued the consumer was handed %v, want [m1 m2 m3]: the rest of its group "+
				"overtook it", got)
		}
		ackBodies(t, next, "m1", "m2", "m3")
	})

	t.Run("after a delay", func(t *testing.T) {
		h, _, cons := fifoHarness(t)
		first := h.receiveOne(cons, 5*time.Second)
		if err := first.Nack(mq.NackDelay(30 * time.Second)); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		h.expectNothing(cons, 300*time.Millisecond, "later message of the group of a message whose requeue was delayed")
		h.server.Advance(31 * time.Second)
		next := h.receive(cons, 3, 5*time.Second)
		if got := bodiesOf(next); got[0] != "m1" || got[1] != "m2" || got[2] != "m3" {
			t.Fatalf("after m1's delay the consumer was handed %v, want [m1 m2 m3]", got)
		}
		ackBodies(t, next, "m1", "m2", "m3")
	})

	t.Run("another group is not held", func(t *testing.T) {
		h, name, cons := fifoHarness(t)
		h.publish(name, &mq.Message{Key: []byte("H"), Body: []byte("x1")})
		first := h.receiveOne(cons, 5*time.Second)
		if err := first.Nack(mq.NackDelay(30 * time.Second)); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		if got := bodiesOf(h.receive(cons, 1, 5*time.Second)); got[0] != "x1" {
			t.Fatalf("the consumer was handed %v, want [x1]: only m1's own group waits for it", got)
		}
	})
}

// TestAMessageLeftOnTheQueueHoldsBackTheRestOfItsGroup: the source leaves a
// received message on the queue when its digest is wrong (or when it cannot
// decode it and has no dead-letter queue), and the later messages of its
// group from the same answer were handed out ahead of it. They are left on
// the queue with it, and the group comes back in order when the window
// passes.
func TestAMessageLeftOnTheQueueHoldsBackTheRestOfItsGroup(t *testing.T) {
	api := &skewedDigests{Server: sqsfake.New(sqsfake.Options{StaleHandleDeletes: true}), received: map[string]bool{"m1": true}}
	h := openOver(t, api, api.Server, func(c *sq.Config) {
		c.FIFO = true
		c.Pollers = 1
		c.AckDeadline = 30 * time.Second
	})
	name := topic(t, "fifo-left")
	publishGroup(h, name, "m1", "m2", "m3")
	h.publish(name, &mq.Message{Key: []byte("H"), Body: []byte("x1")})
	cons := h.consumer(name, 4)
	ackBodies(t, h.receive(cons, 1, 5*time.Second), "x1")
	h.expectNothing(cons, 300*time.Millisecond, "later message of the group of a message left on the queue")

	api.mended.Store(true)
	api.Advance(31 * time.Second)
	next := h.receive(cons, 3, 5*time.Second)
	if got := bodiesOf(next); got[0] != "m1" || got[1] != "m2" || got[2] != "m3" {
		t.Fatalf("once the window passed the consumer was handed %v, want [m1 m2 m3]", got)
	}
	ackBodies(t, next, "m1", "m2", "m3")
}

// TestStopOnFailureOnAFIFOQueueLetsNothingOvertakeTheFailedMessage is the
// reviewer's reproduction, which ElasticMQ confirmed: under
// OrderingStopOnFailure with one message in flight, m1 fails, Run requeues it
// and stops, and a second Run on the same consumer handled m2 and m3 before
// m1 came back (m1 m2 m3 m1). The mode promises that nothing overtakes the
// failed message.
func TestStopOnFailureOnAFIFOQueueLetsNothingOvertakeTheFailedMessage(t *testing.T) {
	_, _, cons := fifoHarness(t)
	var mu sync.Mutex
	var order []string
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, string(d.Body))
		if string(d.Body) == "m1" && len(order) == 1 {
			return errors.New("m1 fails once")
		}
		return nil
	})
	opts := []mq.RunOption{mq.WithOrderingByKey(mq.OrderingStopOnFailure), mq.WithMaxInFlight(1)}
	if err := cons.Run(t.Context(), handler, opts...); err == nil {
		t.Fatal("the first Run returned nil, want the failure that stopped it")
	}
	runCtx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- cons.Run(runCtx, handler, opts...) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled once stopped", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 || order[1] != "m1" || order[2] != "m2" || order[3] != "m3" {
		t.Fatalf("the handler saw %v, want [m1 m1 m2 m3]: a later message of the group overtook the failed one", order)
	}
}

// TestAStrictRunsGiveBackOnAFIFOQueueKeepsTheGroupInOrder is OrderingStrict's
// give-back, the same requeue: m1 fails and the runner holds it through a
// backoff on its worker, the run stops during the backoff and gives m1 back,
// and the next Run must see m1 before m2 and m3, which came in the same
// answer.
func TestAStrictRunsGiveBackOnAFIFOQueueKeepsTheGroupInOrder(t *testing.T) {
	_, _, cons := fifoHarness(t)
	var mu sync.Mutex
	var order []string
	failed := make(chan struct{})
	handler := mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, string(d.Body))
		if len(order) == 1 {
			close(failed)
			return errors.New("m1 fails once")
		}
		return nil
	})
	opts := []mq.RunOption{
		mq.WithOrderingByKey(mq.OrderingStrict), mq.WithMaxInFlight(1),
		mq.WithRetryBackoff(mq.ConstantBackoff(time.Hour)),
	}
	runCtx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- cons.Run(runCtx, handler, opts...) }()
	<-failed
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled once stopped", err)
	}

	runCtx, stop = context.WithCancel(t.Context())
	defer stop()
	done = make(chan error, 1)
	go func() { done <- cons.Run(runCtx, handler, opts...) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Run = %v, want context.Canceled once stopped", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 || order[1] != "m1" || order[2] != "m2" || order[3] != "m3" {
		t.Fatalf("the handler saw %v, want [m1 m1 m2 m3]: a later message of the group overtook the one the "+
			"strict run gave back", order)
	}
}

package sqs_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// TestTheCoreDeadLetterRouteTakesARejectedMessage: a message this driver's
// Reject route dead-lettered carries iqmq-dlq-origin and iqmq-dlq-reason, and
// decode hands both to the dead-letter queue's consumer. The core's
// dead-letter copy kept them beside its own five provenance headers, so a
// message published with an identifier, a key and one header of its own
// needed more than the ten message attributes the service holds, and
// mq.WithDeadLetter over the dead-letter queue was refused before any I/O and
// the message requeued on every delivery. The copy now leaves the earlier
// hop's provenance out and lands.
func TestTheCoreDeadLetterRouteTakesARejectedMessage(t *testing.T) {
	if v, ok := linkedRootRelease(t); ok && releaseBefore(v, "v1.1.4") {
		t.Skipf("built with root module %s: the dead-letter copy leaves an earlier hop's provenance out from v1.1.4", v)
	}
	dead := topic(t, "dead")
	h := open(t, func(c *sq.Config) {
		c.DeadLetterQueue = dead
		c.MaxReceiveCount = 5
	})
	if q := h.queue(dead); q != dead {
		t.Fatalf("topic %q maps to queue %q, so it cannot stand for the dead-letter queue", dead, q)
	}
	name := topic(t, "rejected")
	routed := topic(t, "routed")
	m := &mq.Message{ID: "id-1", Key: []byte("k"), Body: []byte("poison")}
	m.Headers.Set("trace-id", []byte("t"))
	h.publish(name, m)
	d := h.receiveOne(h.consumer(name, 1), 5*time.Second)
	if err := d.Nack(mq.NackDeadLetter(), mq.NackReason("unparsable")); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	cons := h.consumer(dead, 1)
	audit := h.consumer(routed, 1)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	rejected := make(chan bool, 1)
	done := make(chan error, 1)
	go func() {
		done <- cons.Run(ctx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			_, origin := d.Headers.Get("iqmq-dlq-origin")
			_, reason := d.Headers.Get("iqmq-dlq-reason")
			select {
			case rejected <- origin && reason:
			default:
			}
			return errors.New("still poison")
		}), mq.WithDeadLetter(routed), mq.WithMaxAttempts(1))
	}()
	rctx, rcancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer rcancel()
	var buf [1]mq.Delivery
	n, rerr := audit.Receive(rctx, buf[:])
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run: %v", err)
	}
	st := cons.Stats()
	if n != 1 {
		t.Fatalf("the route's dead-letter queue received nothing (%v); DeadLetterFailures = %d: "+
			"the copy carried this driver's own dlq-origin and dlq-reason and needed more than the ten message attributes",
			rerr, st.DeadLetterFailures)
	}
	select {
	case carried := <-rejected:
		if !carried {
			t.Error("the rejected message reached the handler without its dlq-origin and dlq-reason: nothing here tests the copy")
		}
	default:
		t.Error("the handler never ran")
	}
	got := &buf[0]
	for _, k := range []string{"iqmq-dlq-origin", "iqmq-dlq-reason"} {
		if v, ok := got.Headers.Get(k); ok {
			t.Errorf("the dead-letter copy carries the earlier hop's %s=%q", k, v)
		}
	}
	if got.ID != "id-1" || string(got.Key) != "k" {
		t.Errorf("the copy's identifier and key are %q and %q, want the message's", got.ID, got.Key)
	}
	if v, _ := got.Headers.Get(mq.HeaderOriginTopic); string(v) != dead {
		t.Errorf("%s = %q, want the dead-letter queue the route consumed it from, %q", mq.HeaderOriginTopic, v, dead)
	}
	// A copy is published again only if the delivery's visibility runs out
	// before the ack, which a slow runner can make happen; one or more is
	// the route working, and DeadLetterFailures is the refusal.
	if st.DeadLetterFailures != 0 || st.DeadLetterPublished < 1 {
		t.Errorf("DeadLetterFailures = %d and DeadLetterPublished = %d, want 0 and at least 1", st.DeadLetterFailures, st.DeadLetterPublished)
	}
	_ = got.Ack()
}

// TestTheCoreDeadLetterRouteFitsTheAttributeLimit: this driver advertises a
// MaxHeaderCount of nine, and the core's dead-letter copy adds up to five
// provenance headers to the message's own, so the copy of a message with six
// headers of its own was refused before any I/O and the message requeued on
// every delivery. The route now gives up its provenance, the last written
// first, until the copy fits, and keeps the message's own headers.
func TestTheCoreDeadLetterRouteFitsTheAttributeLimit(t *testing.T) {
	if v, ok := linkedRootRelease(t); ok && releaseBefore(v, "v1.1.4") {
		t.Skipf("built with root module %s: the dead-letter copy fits the header limit from v1.1.4", v)
	}
	h := open(t)
	name := topic(t, "crowded")
	routed := topic(t, "routed")
	const own = 6
	m := &mq.Message{Body: []byte("poison")}
	for i := range own {
		m.Headers.Set("h"+strconv.Itoa(i), []byte("v"))
	}
	h.publish(name, m)
	cons := h.consumer(name, 1)
	audit := h.consumer(routed, 1)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- cons.Run(ctx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			return errors.New("still poison")
		}), mq.WithDeadLetter(routed), mq.WithMaxAttempts(1))
	}()
	rctx, rcancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer rcancel()
	var buf [1]mq.Delivery
	n, rerr := audit.Receive(rctx, buf[:])
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run: %v", err)
	}
	st := cons.Stats()
	if n != 1 {
		t.Fatalf("the route's dead-letter queue received nothing (%v); DeadLetterFailures = %d: "+
			"the copy carried more headers than the nine this driver advertises", rerr, st.DeadLetterFailures)
	}
	got := &buf[0]
	for i := range own {
		if _, ok := got.Headers.Get("h" + strconv.Itoa(i)); !ok {
			t.Errorf("the copy lost the message's own header h%d", i)
		}
	}
	for _, k := range []string{mq.HeaderOriginTopic, mq.HeaderOriginSubscription, mq.HeaderAttempts} {
		if _, ok := got.Headers.Get(k); !ok {
			t.Errorf("the copy carries no %s, which fits", k)
		}
	}
	for _, k := range []string{mq.HeaderFailure, mq.HeaderFirstDeliveredAt} {
		if v, ok := got.Headers.Get(k); ok {
			t.Errorf("the copy carries %s=%q, past the header limit", k, v)
		}
	}
	if st.DeadLetterFailures != 0 || st.DeadLetterPublished < 1 {
		t.Errorf("DeadLetterFailures = %d and DeadLetterPublished = %d, want 0 and at least 1", st.DeadLetterFailures, st.DeadLetterPublished)
	}
	_ = got.Ack()
}

package sqs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
	sq "github.com/iqhive/iqmq/drivers/sqs"
)

// The specification and mq.Delivery.Extend promise that an extension moves the deadline "at
// least by further into the future". The service's ChangeMessageVisibility
// measures its VisibilityTimeout from the moment of the CALL, so a driver that
// sends `by` as the timeout sets the window to `by` from now — and an
// extension shorter than what was left SHORTENS the lease, which is the worst
// direction for the defect to point (closed by).
//
// The window is ten seconds and the extension one, so a single second cannot
// be confused with the service's whole-second rounding.
const (
	leaseWindow    = 10 * time.Second
	shortExtension = time.Second
)

// extendHarness opens a driver whose lease is leaseWindow long and whose
// settlements leave immediately, so that an Ack's outcome is the service's
// answer and not a coalescing boundary.
func extendHarness(t *testing.T) *harness {
	t.Helper()
	return open(t, func(c *sq.Config) {
		c.AckDeadline = leaseWindow
		c.Settle = flush.Immediate
	})
}

// TestExtendDoesNotShortenARemainingWindow is the test, and it
// is the one to watch fail: before the fix the Ack below reported ErrLeaseLost
// because Extend(1s) replaced the ten seconds the delivery had with one.
//
// The wait is half a second SHORT of the deadline the specification promises, rather than just
// past the extension: an Ack that succeeds a second and a half in proves only
// that the lease outlived the extension, and what is being pinned here is the
// lower end of the new deadline itself.
func TestExtendDoesNotShortenARemainingWindow(t *testing.T) {
	h := extendHarness(t)
	name := topic(t, "extend-short")
	h.publish(name, &mq.Message{Body: []byte("shorten")})
	d := h.receiveOne(h.consumer(name, 1), 10*time.Second)
	if err := d.Extend(shortExtension); err != nil {
		t.Fatalf("Extend(%v): %v", shortExtension, err)
	}
	// Just inside the deadline the extension bought: the window the delivery
	// started with, plus the extension, less half a second.
	const wait = leaseWindow + shortExtension - 500*time.Millisecond
	h.server.Advance(wait)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack %v after Extend(%v) on a %v window: %v; the specification makes an extension move the deadline at least %v BEYOND the one it had, so the lease runs to %v and this settlement is inside it ",
			wait, shortExtension, leaseWindow, err, shortExtension, leaseWindow+shortExtension)
	}
}

// TestExtendMovesTheDeadlineByTheExtensionAndNoFurther is the other side of the
// bracket: an extension is not a new lease of its own length either, so the
// window really ends leaseWindow+shortExtension after the receive.
//
// Together the two tests bracket the deadline inside one second of the 11 s the specification
// promises — settleable at 10.5 s, gone at 11.5 s — which is what makes the fix
// a statement about the deadline rather than about one settlement that happened
// to succeed.
func TestExtendMovesTheDeadlineByTheExtensionAndNoFurther(t *testing.T) {
	h := extendHarness(t)
	name := topic(t, "extend-bound")
	h.publish(name, &mq.Message{Body: []byte("bounded")})
	d := h.receiveOne(h.consumer(name, 1), 10*time.Second)
	if err := d.Extend(shortExtension); err != nil {
		t.Fatalf("Extend(%v): %v", shortExtension, err)
	}
	h.server.Advance(leaseWindow + shortExtension + 500*time.Millisecond)
	err := d.Ack()
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("Ack %v after a %v window extended by %v reported %v, want mq.ErrLeaseLost: the window has closed and the driver's local check is what stops the delete leaving the process",
			leaseWindow+shortExtension+500*time.Millisecond, leaseWindow, shortExtension, err)
	}
}

// TestExtendRefusesWhatWouldPassTheServicesCeiling records the one case the fix
// has to answer for: `remaining + by` can exceed the service's twelve hours
// where `by` alone does not, and there is no timeout that grants it.
//
// Clamping to twelve hours and reporting success would be a lie — the deadline
// would move less than `by` — so the extension is refused with
// CodeInvalidArgument and the delivery is left exactly as settleable as it was.
func TestExtendRefusesWhatWouldPassTheServicesCeiling(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.AckDeadline = sq.MaxVisibility
		c.Settle = flush.Immediate
	})
	name := topic(t, "extend-ceiling")
	h.publish(name, &mq.Message{Body: []byte("ceiling")})
	d := h.receiveOne(h.consumer(name, 1), 10*time.Second)
	err := d.Extend(time.Minute)
	if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
		t.Fatalf("Extend(%v) on a delivery holding the whole %v window reported %v (code %v), want %v: no visibility timeout moves the deadline a minute past the service's ceiling",
			time.Minute, sq.MaxVisibility, err, code, mq.CodeInvalidArgument)
	}
	// A refused extension changes nothing: the delivery still settles.
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack after a refused extension: %v; a failed extension leaves the delivery exactly as settleable as it was", err)
	}
}

// TestAFifoGroupWhoseWindowLapsedIsDeliveredAgain is the portable-API half, through the whole driver rather than against the fake's
// front door.
//
// It exists because the stranded group is reachable from ordinary use — a
// handler that takes longer than its lease — and because the fake is what four
// of this driver's critical scenarios rest on: a queue that can never be
// drained again is a test suite that deadlocks against its own server. The
// failure is a REPORT rather than a timeout: the receive is bounded, and when
// nothing arrives the queue's own depth and invisible count are what the
// message names.
func TestAFifoGroupWhoseWindowLapsedIsDeliveredAgain(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.FIFO = true
		c.AckDeadline = 5 * time.Second
		c.Settle = flush.Immediate
	})
	name := topic(t, "fifo-lapse")
	for _, body := range []string{"one", "two"} {
		h.publish(name, &mq.Message{Key: []byte("g"), Body: []byte(body)})
	}
	cons := h.consumer(name, 2)
	first := h.receiveOne(cons, 10*time.Second)
	if got := string(first.Body); got != "one" {
		t.Fatalf("the first delivery was %q, want \"one\"", got)
	}
	// Nothing is settled and the window lapses by the clock alone, which is
	// what a handler slower than its lease does.
	h.server.Advance(6 * time.Second)
	if err := first.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("an Ack after the window reported %v, want mq.ErrLeaseLost", err)
	}
	// The group must be deliverable again: its only in-flight message became
	// visible, so nothing holds it.
	ds, err := receiveWithin(h, cons, 2*time.Second)
	if err != nil || len(ds) == 0 {
		t.Fatalf("no delivery arrived after the window lapsed (%v): the queue holds %d message(s) with %d invisible, so the FIFO group is held by a message that is VISIBLE and this consumer can never make progress again ",
			err, h.depth(name), h.invisible(name))
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack of the redelivery: %v", err)
	}
}

// TestABlockedFifoGroupIsVisibleInTheQueueStats is.
//
// A receive that produces nothing says nothing about WHY: SQS delivers nothing
// else from a message group while one of its messages is in flight — which is
// what makes the order hold — and a blocked group therefore looks exactly like
// an empty queue through the portable API, with no error and no code.
// The only thing that can tell them apart is Admin.Stats,
// and that only works if a blocked group always has a message IN FLIGHT for
// Stats to report.
//
// The stranded group broke precisely that. A group held by a message that was
// VISIBLE made the two answers contradict each other: Stats said depth 1 with
// nothing in flight, which reads as "a message is available", while every
// receive returned nothing for ever. So the limitation of the specification is a
// limitation a caller can work with, and this test is what says so.
func TestABlockedFifoGroupIsVisibleInTheQueueStats(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.FIFO = true
		c.AckDeadline = 5 * time.Second
		c.Settle = flush.Immediate
	})
	name := topic(t, "fifo-blocked")
	for _, body := range []string{"one", "two"} {
		h.publish(name, &mq.Message{Key: []byte("g"), Body: []byte(body)})
	}
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("this driver has no Admin (%v), so nothing at all can tell a blocked queue from an empty one", err)
	}
	cons := h.consumer(name, 1)
	held := h.receiveOne(cons, 10*time.Second)

	// Blocked by a message in flight: the poll is empty and Stats says why.
	if ds, err := receiveWithin(h, cons, 500*time.Millisecond); err != nil || len(ds) != 0 {
		t.Fatalf("a receive produced %d delivery(ies) (%v) while %q is in flight; a FIFO group holds its later messages", len(ds), err, held.Body)
	}
	stats, err := admin.Stats(t.Context(), name, "s")
	if err != nil {
		t.Fatalf("Admin.Stats: %v", err)
	}
	switch {
	case !stats.Depth.Known || !stats.InFlight.Known:
		t.Fatalf("Admin.Stats reports depth known=%v and in-flight known=%v; a caller with neither number cannot tell a blocked queue from an empty one",
			stats.Depth.Known, stats.InFlight.Known)
	case stats.InFlight.Value == 0:
		t.Fatalf("a receive produced nothing and Admin.Stats reports depth %d with %d in flight: the two answers contradict each other, so this queue is indistinguishable from one holding an available message ",
			stats.Depth.Value, stats.InFlight.Value)
	}

	// And the window lapses with nothing settled. The group is released, so
	// the poll is no longer empty — which is the state that used to report
	// depth 1, in flight 0 and deliver nothing at all.
	h.server.Advance(6 * time.Second)
	if err := held.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("an Ack after the window reported %v, want mq.ErrLeaseLost", err)
	}
	ds, err := receiveWithin(h, cons, 2*time.Second)
	if err != nil || len(ds) == 0 {
		after, serr := admin.Stats(t.Context(), name, "s")
		t.Fatalf("no delivery arrived after the window lapsed (%v): Admin.Stats reports depth %d with %d in flight (%v), so the queue says a message is available and the receive says there is none ",
			err, after.Depth.Value, after.InFlight.Value, serr)
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack of the redelivery: %v", err)
	}
}

// receiveWithin takes whatever arrives inside the budget, without failing the
// test when nothing does: a test about an empty poll has to be able to observe
// one.
func receiveWithin(h *harness, c *mq.ConsumerHandle, budget time.Duration) ([]mq.Delivery, error) {
	ctx, cancel := contextWithTimeout(h.t, budget)
	defer cancel()
	dst := make([]mq.Delivery, 4)
	n, err := c.Receive(ctx, dst)
	if n > 0 {
		return dst[:n], nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil, nil
	}
	return nil, err
}

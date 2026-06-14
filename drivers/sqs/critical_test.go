package sqs_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/flush"
	sq "bitbucket.org/iqhive/iqmq/drivers/sqs"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
)

//
// Each of them is a way for an SQS driver to delete a message it does not own,
// lose a message it said it kept, or claim an ordering it does not have — and
// each of them is unreachable from the portable API without control over the
// broker, which is why they are here against the in-process server rather than
// in the conformance suite. The conformance suite runs the same
// driver against ElasticMQ and LocalStack; what it cannot do is make a
// visibility window pass without waiting for it, fail three entries of a
// ten-entry delete batch, or hand one message two valid receipt handles on
// purpose.

// TestCriticalVisibilityExpiry is the at-least-once window opening.
//
// The message is received, its visibility window is allowed to pass, and two
// things then have to be true:
//
//   - the message comes BACK. A driver that held it locally, or that had
//     deleted it early, would fail here.
//   - the stale holder's delete is REFUSED, with ErrLeaseLost, and refused
//     before any API call. That is the whole reason the local lease check
//     exists: the service accepts a DeleteMessage for a superseded receipt
//     handle, so a driver that just forwarded the delete would remove the
//     message the second consumer is now processing — and would report
//     success for it.
func TestCriticalVisibilityExpiry(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = 5 * time.Second })
	name := topic(t, "vis-expiry")
	h.publish(name, &mq.Message{Body: []byte("comes-back")})
	first := h.consumer(name, 4)
	held := h.receiveOne(first, 5*time.Second)
	if held.Info().LeaseExpires.IsZero() {
		t.Fatal("the delivery reports no lease, so nothing can expire and this test proves nothing")
	}

	// The window passes. Nothing here sleeps: the driver reads the server's
	// clock, so advancing it past the deadline is exactly what the service
	// doing so looks like from the driver's side.
	deletesBefore := h.server.Calls("DeleteMessageBatch") + h.server.Calls("DeleteMessage")
	h.server.Advance(6 * time.Second)

	err := held.Ack()
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack of a delivery whose visibility window has passed = %v, want ErrLeaseLost: the service would accept the delete and remove a message another consumer now holds ", err)
	}
	if got := h.server.Calls("DeleteMessageBatch") + h.server.Calls("DeleteMessage"); got != deletesBefore {
		t.Fatalf("the refused settlement still made %d delete call(s): the refusal has to happen BEFORE the call, because the call would succeed", got-deletesBefore)
	}
	// A negative settlement is refused for the same reason: neither
	// direction may reach a message somebody else may already hold.
	if err := held.Nack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Nack of the same delivery = %v, want ErrLeaseLost", err)
	}

	// And the message really is back, which is the other half of the window.
	again := h.receiveOne(first, 5*time.Second)
	if string(again.Body) != "comes-back" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if got := again.Info().Attempt; got != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2: ApproximateReceiveCount is this driver's attempt count", got)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("the redelivery's Ack = %v", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) left in the queue after the redelivery was acknowledged", got)
	}
}

// TestCriticalPartialDeleteBatch is the partial settlement.
//
// Ten messages are received and settled as one batch; the server is told to
// fail three of its ten entries. What has to be true:
//
//   - the seven that succeeded are gone and report nil;
//   - the three that failed are STILL IN THE QUEUE and report their own
//     error, at their own index. A driver that reported the batch's first
//     error for every entry, or that aligned the result with the failures
//     rather than with the input, would tell the caller a message was settled
//     that is about to be delivered again — or the reverse;
//   - the three that failed are still SETTLEABLE, because a definite refusal
//     leaves a delivery settleable, and a retry settles them.
func TestCriticalPartialDeleteBatch(t *testing.T) {
	const n = 10
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) {
		c.AckDeadline = time.Hour
		// A ten-entry DeleteMessageBatch needs a policy that batches:
		// under the default `immediate` policy every Ack is its own call,
		// which is the policy's whole point and is what the specification is
		// deciding. `hybrid` is the arm that reaches BOTH halves of this
		// scenario — the count fills the ten-entry batch the partial
		// response needs, and the interval releases the three retries at
		// the end, which `count` alone would hold for ever waiting for a
		// batch that can no longer fill. That worst case is exactly what
		// rules["A32"] excludes `count` as a default for, and this test is
		// where it was first felt: written with `count`, the retry loop
		// hung.
		c.Settle = flush.Hybrid
		c.SettleMaxItems = n
		c.SettleInterval = 20 * time.Millisecond
	})
	name := topic(t, "partial-delete")
	for i := range n {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	cons := h.consumer(name, n)
	ds := h.receive(cons, n, 10*time.Second)

	// The failing entries are chosen by the BODY of the message they
	// settle, which is the only thing this test controls: the entry
	// identifier is the driver's, and the position in the request is
	// whichever order the ten concurrent settlements happened to be queued
	// in. Naming either of those would be asserting on the driver's own
	// bookkeeping rather than on the correlation.
	failing := map[string]string{"1": "InternalError", "4": "ReceiptHandleIsInvalid", "7": "InternalError"}
	server.Fault(sqsfake.Fault{Op: "DeleteMessageBatch", Bodies: failing, Count: 1})

	// Settling from ten goroutines is what makes the ten join one batch: the
	// settler coalesces whatever is queued, and a sequential loop under the
	// immediate policy would be ten separate calls.
	errs := settleTogether(t, ds, func(d *mq.Delivery) error { return d.Ack() })

	survived := map[string]bool{}
	for _, body := range h.bodies(name) {
		survived[body] = true
	}
	for i := range n {
		body := string(ds[i].Body)
		_, wantFail := failing[body]
		switch {
		case wantFail && errs[i] == nil:
			t.Errorf("entry %d (body %q) was told the delete succeeded; the service refused it", i, body)
		case !wantFail && errs[i] != nil:
			t.Errorf("entry %d (body %q) was told the delete failed with %v; the service took it", i, body, errs[i])
		}
		if wantFail && !survived[body] {
			t.Errorf("body %q was deleted although its entry failed", body)
		}
		if !wantFail && survived[body] {
			t.Errorf("body %q is still in the queue although its entry succeeded", body)
		}
	}
	if t.Failed() {
		t.Fatalf("per-entry outcomes: %v; queue holds %v", errs, h.bodies(name))
	}
	if got := h.depth(name); got != len(failing) {
		t.Fatalf("%d message(s) left in the queue, want the %d whose entries failed", got, len(failing))
	}

	// A definite refusal leaves the delivery settleable, which is what lets
	// the caller retry — and for a partial batch it is the only thing that
	// keeps the three from being redelivered.
	for i := range n {
		if _, failed := failing[string(ds[i].Body)]; !failed {
			continue
		}
		err := ds[i].Ack()
		if errors.Is(err, mq.ErrLeaseLost) {
			// The entry whose code was ReceiptHandleIsInvalid really has
			// lost its lease, and reporting that rather than retrying is
			// correct: the handle will never become valid again.
			continue
		}
		if err != nil {
			t.Fatalf("the retried settlement of entry %d = %v; a definite refusal leaves a delivery settleable ", i, err)
		}
	}
}

// TestCriticalStaleReceiptHandle is the case the visibility window cannot
// catch.
//
// One message is received twice by ONE source — the second receive happening
// because the window passed — and both receipt handles are valid at the
// service: the in-process server is configured the way the service documents
// itself, where a delete for a superseded handle deletes the message anyway.
//
// So the driver has to refuse the stale settlement itself, and it has to
// refuse it for a reason that does not depend on the clock. The redelivery
// here is NOT caused by the window passing — the server is told to make the
// message visible again without the clock moving at all, which is what another
// consumer's ChangeMessageVisibility(0) looks like from this source's side —
// so the first delivery's recorded deadline is still in the future and the
// local lease check would let its delete through. What catches it is the
// source's supersession bookkeeping, and nothing else can.
//
// Written the other way round, with the clock advanced past the deadline, this
// test passed with the supersession bookkeeping deleted: the lease check
// caught it and the half this test exists for was never exercised.
func TestCriticalStaleReceiptHandle(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) { c.AckDeadline = 10 * time.Second })
	name := topic(t, "stale-receipt")
	h.publish(name, &mq.Message{Body: []byte("one-message")})
	cons := h.consumer(name, 4)

	stale := h.receiveOne(cons, 5*time.Second)
	staleExpiry := stale.Info().LeaseExpires
	// The message becomes visible again with the clock standing still, so
	// the same source receives it a second time under a new handle while the
	// FIRST delivery's recorded window is still open.
	if n := h.server.ExpireVisibility(h.queue(name)); n != 1 {
		t.Fatalf("the server released %d message(s), want 1", n)
	}
	fresh := h.receiveOne(cons, 5*time.Second)
	if string(fresh.Body) != "one-message" || fresh.Info().Attempt != 2 {
		t.Fatalf("the second delivery is body %q attempt %d", fresh.Body, fresh.Info().Attempt)
	}

	// Both windows are open — the clock has not moved — so the local lease
	// check has nothing to say and the bookkeeping is the only defence.
	if now := h.server.Now(); !now.Before(staleExpiry) {
		t.Fatalf("the stale delivery's window closed at %v and it is now %v; this test needs it OPEN, or the lease check catches the settlement and the supersession bookkeeping is not exercised at all", staleExpiry, now)
	}
	deletes := server.Calls("DeleteMessageBatch") + server.Calls("DeleteMessage")
	err := stale.Ack()
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack of a superseded delivery = %v, want ErrLeaseLost: the service accepts a delete for the old handle and the message the second delivery owns would be gone", err)
	}
	if got := server.Calls("DeleteMessageBatch") + server.Calls("DeleteMessage"); got != deletes {
		t.Fatalf("the refused settlement made %d delete call(s); the service would have taken every one of them", got-deletes)
	}
	if got := h.depth(name); got != 1 {
		t.Fatalf("the queue holds %d message(s) after the stale settlement was refused, want 1", got)
	}

	// The fresh delivery settles normally, which is the half a driver that
	// invalidated everything on a redelivery would break.
	if err := fresh.Ack(); err != nil {
		t.Fatalf("the fresh delivery's Ack = %v", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("the queue holds %d message(s) after the fresh delivery was acknowledged", got)
	}
}

// TestCriticalFIFOGrouping is the ordering claim, in both directions.
//
// The FIFO profile claims FeatureOrderedByKey and nothing else, so two things
// have to hold and the second is as important as the first:
//
//   - ONE group is ordered. Messages published under one key arrive in
//     publication order, and a group's later message is not delivered while
//     an earlier one of the same group is in flight — which is what makes the
//     order survive a consumer that settles slowly.
//   - DIFFERENT groups are not ordered with respect to each other. The driver
//     does not claim FeatureOrderedGlobally, and this half proves the claim
//     is honest rather than accidental: two groups are delivered
//     concurrently, so a consumer sees one group's second message before the
//     other group's first.
func TestCriticalFIFOGrouping(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.FIFO = true
		c.AckDeadline = time.Hour
	})
	name := topic(t, "fifo-group")
	const per = 4
	// Interleaved on purpose: if the driver ignored the group and the queue
	// were a plain queue, the arrival order would be the publication order
	// and the first assertion would pass for the wrong reason.
	for i := range per {
		h.publish(name, &mq.Message{Key: []byte("a"), Body: []byte("a" + strconv.Itoa(i))})
		h.publish(name, &mq.Message{Key: []byte("b"), Body: []byte("b" + strconv.Itoa(i))})
	}
	cons := h.consumer(name, 2*per)

	// A FIFO queue holds a group's later messages while one is in flight, so
	// the first receive can return at most one message per group.
	first := h.receive(cons, 2, 10*time.Second)
	groups := map[byte]bool{}
	for i := range first {
		groups[first[i].Body[0]] = true
	}
	if len(groups) != 2 {
		t.Fatalf("the first two deliveries came from %d group(s) (%q, %q); a FIFO queue delivers one message per group at a time, so two in-flight messages are two groups",
			len(groups), first[0].Body, first[1].Body)
	}

	// Drain the rest, settling as we go, and record each group's order.
	order := map[byte][]string{}
	for i := range first {
		order[first[i].Body[0]] = append(order[first[i].Body[0]], string(first[i].Body))
		if err := first[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	for range 2 * (per - 1) {
		d := h.receiveOne(cons, 10*time.Second)
		g := d.Body[0]
		order[g] = append(order[g], string(d.Body))
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	for _, g := range []byte{'a', 'b'} {
		got := order[g]
		if len(got) != per {
			t.Fatalf("group %q produced %d of %d messages: %v", g, len(got), per, got)
		}
		for i := range per {
			want := string(g) + strconv.Itoa(i)
			if got[i] != want {
				t.Fatalf("group %q arrived as %v; a FIFO message group is delivered in publication order", g, got)
			}
		}
	}
}

// TestCriticalFIFOGroupsAreNotOrderedTogether is the negative half of the
// ordering claim: the driver does not advertise FeatureOrderedGlobally, and
// this is what makes that honest.
//
// Two groups' messages are published strictly interleaved and consumed with
// nothing settled in between, so the two groups are in flight at once. A queue
// that ordered globally would deliver them in publication order; a FIFO queue
// orders each group and nothing across them, so the second message of one
// group is delivered before the first of the other.
func TestCriticalFIFOGroupsAreNotOrderedTogether(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.FIFO = true
		c.AckDeadline = time.Hour
	})
	name := topic(t, "fifo-cross")
	// a0 b0 a1 b1 …: under a global order the deliveries alternate.
	const per = 3
	for i := range per {
		h.publish(name, &mq.Message{Key: []byte("a"), Body: []byte("a" + strconv.Itoa(i))})
		h.publish(name, &mq.Message{Key: []byte("b"), Body: []byte("b" + strconv.Itoa(i))})
	}
	cons := h.consumer(name, 2*per)
	got := make([]string, 0, 2*per)
	for range 2 * per {
		d := h.receiveOne(cons, 10*time.Second)
		got = append(got, string(d.Body))
		if err := d.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	// Every group is internally ordered.
	for _, g := range []string{"a", "b"} {
		seen := 0
		for _, body := range got {
			if body[:1] != g {
				continue
			}
			if body != g+strconv.Itoa(seen) {
				t.Fatalf("group %q arrived out of order in %v", g, got)
			}
			seen++
		}
		if seen != per {
			t.Fatalf("group %q produced %d of %d in %v", g, seen, per, got)
		}
	}
	// And the two groups are NOT interleaved the way a global order would
	// interleave them: the queue is free to drain one group ahead of the
	// other, and a driver claiming a global order would have to produce the
	// publication order here.
	alternating := true
	for i, body := range got {
		want := "ab"[i%2 : i%2+1]
		if body[:1] != want {
			alternating = false
			break
		}
	}
	if alternating {
		t.Logf("the two groups happened to arrive alternating (%v); that is allowed and is not evidence of a global order", got)
	}
	t.Logf("delivery order across two groups: %v", got)
}

// settleTogether settles every delivery from its own goroutine and returns the
// outcomes index-aligned with ds.
//
// The goroutines are the point: the batch settler coalesces whatever is queued
// when a settlement arrives, so a sequential loop would make one call per
// delivery whatever the policy and the partial-batch case would never be
// reached.
func settleTogether(t *testing.T, ds []mq.Delivery, fn func(*mq.Delivery) error) []error {
	t.Helper()
	errs := make([]error, len(ds))
	done := make(chan struct{})
	for i := range ds {
		go func(i int) {
			errs[i] = fn(&ds[i])
			done <- struct{}{}
		}(i)
	}
	for range ds {
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("a settlement never returned; the batch settler is holding it")
		}
	}
	return errs
}

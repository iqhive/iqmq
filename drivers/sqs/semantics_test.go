package sqs_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// TestRoundTrip is the driver's whole job, once.
func TestRoundTrip(t *testing.T) {
	h := open(t)
	name := topic(t, "roundtrip")
	m := &mq.Message{Key: []byte("k-1"), Body: []byte("payload"), ID: "id-1"}
	m.Headers.Add("content-kind", []byte("order"))
	h.publish(name, m)
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	if string(d.Body) != "payload" || string(d.Key) != "k-1" || d.ID != "id-1" {
		t.Fatalf("delivery = body %q key %q id %q", d.Body, d.Key, d.ID)
	}
	if v, ok := d.Headers.Get("content-kind"); !ok || string(v) != "order" {
		t.Fatalf("header = %q, %v", v, ok)
	}
	info := d.Info()
	switch {
	case info.Topic != name:
		t.Errorf("Info.Topic = %q", info.Topic)
	case info.Attempt != 1:
		t.Errorf("Info.Attempt = %d on a first delivery", info.Attempt)
	case info.Redelivered:
		t.Error("Info.Redelivered on a first delivery")
	case info.LeaseExpires.IsZero():
		t.Error("Info.LeaseExpires is zero; every SQS delivery has a visibility window")
	case info.ReceivedAt.IsZero():
		t.Error("Info.ReceivedAt is zero")
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) left after the Ack", got)
	}
	// The specification: a second terminal settlement is refused above the driver.
	if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("the second Ack = %v, want ErrAlreadySettled", err)
	}
}

// TestReceiptCarriesTheServiceAnswer is a publisher configured with
// receipts is told what the broker said, in storage it owns.
func TestReceiptCarriesTheServiceAnswer(t *testing.T) {
	h := open(t)
	name := topic(t, "receipt")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name, Receipts: true})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() {
		if err := pub.Close(); err != nil {
			t.Errorf("closing the publisher: %v", err)
		}
	}()
	var receipt mq.PublishReceipt
	m := &mq.Message{Body: []byte("b"), Receipt: &receipt}
	if err := pub.Publish(t.Context(), m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.MessageID == "" {
		t.Fatal("the receipt carries no message identifier; the service assigns one")
	}
	if receipt.Confirmation != mq.ConfirmAccepted {
		t.Fatalf("the receipt reports the %v boundary", receipt.Confirmation)
	}
	if receipt.ConfirmedAt.IsZero() {
		t.Fatal("the receipt carries no confirmation time")
	}
}

// TestDurableConfirmIsTheSameBoundary is the claim the capability table makes:
// the service stores the message redundantly before it answers, so
// ConfirmDurable costs no extra round trip and is not refused.
func TestDurableConfirmIsTheSameBoundary(t *testing.T) {
	h := open(t)
	name := topic(t, "durable")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{
		Topic: name, Confirmation: mq.ConfirmDurable,
	})
	if err != nil {
		t.Fatalf("a ConfirmDurable publisher was refused: %v", err)
	}
	defer func() { _ = pub.Close() }()
	before := h.server.Calls("SendMessage")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("b")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := h.server.Calls("SendMessage") - before; got != 1 {
		t.Fatalf("a durable publication cost %d request(s), want one: the response IS the durable boundary", got)
	}
}

// TestPublishBatchChunksAtTen is the service's own limit, applied by the
// driver rather than discovered at the service.
func TestPublishBatchChunksAtTen(t *testing.T) {
	h := open(t)
	name := topic(t, "batch-chunk")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()
	const n = 25
	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte(strconv.Itoa(i))}
	}
	before := h.server.Calls("SendMessageBatch")
	if err := pub.PublishBatch(t.Context(), ms); err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	calls := h.server.Calls("SendMessageBatch") - before
	if calls != 3 {
		t.Fatalf("%d messages took %d SendMessageBatch call(s), want 3 at ten entries each", n, calls)
	}
	if got := h.depth(name); got != n {
		t.Fatalf("the queue holds %d of %d messages", got, n)
	}
}

// TestPublishBatchPartialResultsAreIndexAligned is the alignment contract, and
// it is the one a batch publish loses a message by getting wrong.
//
// Three entries of a twelve-message batch fail: one because the encoding
// refuses it before any I/O, and two because the service refuses them. All
// three errors have to be at THEIR index in the caller's slice — not the
// chunk's, not the failure list's — and the nine that succeeded have to report
// nil and be on the queue.
func TestPublishBatchPartialResultsAreIndexAligned(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	name := topic(t, "batch-partial")
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	const n = 12
	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte("m" + strconv.Itoa(i))}
	}
	// Index 3 is refused by the ENCODER: a time to live the service has no
	// answer for. It never reaches the service at all, which is the case a
	// driver that aligned its result with what it sent would misreport.
	ms[3].Headers.Add("bad/name", []byte("1"))
	// Indexes 7 and 11 are refused by the service. 11 is deliberately in the
	// second chunk.
	server.Fault(sqsfake.Fault{
		Op:     "SendMessageBatch",
		Bodies: map[string]string{"m7": "InternalError", "m11": "InvalidAttributeValue"},
		Count:  -1,
	})

	err = pub.PublishBatch(t.Context(), ms)
	var be *mq.BatchError
	if !errors.As(err, &be) {
		t.Fatalf("PublishBatch = %v, want a *BatchError", err)
	}
	if len(be.Errs) != n {
		t.Fatalf("the *BatchError carries %d outcome(s) for %d messages; the Errs are index-aligned with the input", len(be.Errs), n)
	}
	wantFail := map[int]bool{3: true, 7: true, 11: true}
	for i := range n {
		switch {
		case wantFail[i] && be.Errs[i] == nil:
			t.Errorf("message %d (%q) was reported accepted; it was not", i, ms[i].Body)
		case !wantFail[i] && be.Errs[i] != nil:
			t.Errorf("message %d (%q) was reported refused with %v; it was accepted", i, ms[i].Body, be.Errs[i])
		}
	}
	on := map[string]bool{}
	for _, body := range h.bodies(name) {
		on[body] = true
	}
	for i := range n {
		body := string(ms[i].Body)
		if wantFail[i] && on[body] {
			t.Errorf("%q reached the queue although its entry was refused", body)
		}
		if !wantFail[i] && !on[body] {
			t.Errorf("%q did not reach the queue although its entry was accepted", body)
		}
	}
	if t.Failed() {
		t.Fatalf("outcomes: %v; queue holds %v", be.Errs, h.bodies(name))
	}
}

// TestNackMakesTheMessageVisibleAgain is the specification's "Nack→ChangeMessageVisibility
// (0|delay)", the immediate half.
func TestNackMakesTheMessageVisibleAgain(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "nack")
	h.publish(name, &mq.Message{Body: []byte("again")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	again := h.receiveOne(cons, 5*time.Second)
	if string(again.Body) != "again" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if got := again.Info().Attempt; got != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", got)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	// A settlement for the nacked delivery is refused: it is back in the
	// queue and this delivery is not the one that owns it.
	if err := d.Ack(); err == nil {
		t.Fatal("the nacked delivery was acknowledged afterwards; it no longer owns the message")
	}
}

// TestNackWithDelayHoldsTheMessage is the delayed half, and the thing that
// makes it a NATIVE claim rather than an emulation: the queue holds the delay,
// so it survives this process.
func TestNackWithDelayHoldsTheMessage(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "nack-delay")
	h.publish(name, &mq.Message{Body: []byte("later")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Nack(mq.NackDelay(30 * time.Second)); err != nil {
		t.Fatalf("Nack with a delay: %v", err)
	}
	h.expectNothing(cons, 300*time.Millisecond, "a message whose redelivery was delayed")
	h.server.Advance(31 * time.Second)
	again := h.receiveOne(cons, 5*time.Second)
	if string(again.Body) != "later" {
		t.Fatalf("the delayed redelivery carries %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestRejectRoutesToTheDeadLetterQueue is the dead-letter route, and the
// order that makes it safe.
func TestRejectRoutesToTheDeadLetterQueue(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.AckDeadline = time.Hour
		c.DeadLetterQueue = "dead"
		c.MaxReceiveCount = 5
	})
	name := topic(t, "reject")
	h.publish(name, &mq.Message{Body: []byte("poison")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("the source queue holds %d message(s) after the Reject", got)
	}
	if got := h.server.Bodies("dead"); len(got) != 1 || got[0] != "poison" {
		t.Fatalf("the dead-letter queue holds %v", got)
	}
}

// TestADeadLetterReasonIsCarriedInTheServicesCharacterSet holds the reason
// attribute to what the service accepts. A String attribute is held to the
// body's character set, and the reason is the caller's own text: one with a
// control character or invalid UTF-8 in it went out as it was, and the service
// refuses the whole SendMessage for it, so the message could never be
// dead-lettered with that reason. This server does not check the character
// set, so the test reads what was sent instead.
func TestADeadLetterReasonIsCarriedInTheServicesCharacterSet(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.AckDeadline = time.Hour
		c.DeadLetterQueue = "dead"
		c.MaxReceiveCount = 5
	})
	name := topic(t, "reject-reason")
	h.publish(name, &mq.Message{Body: []byte("poison")})
	d := h.receiveOne(h.consumer(name, 4), 5*time.Second)
	const reason = "handler failed: \x00\x1b[31mred\x1b[0m \xff end￿"
	if err := d.Nack(mq.NackDeadLetter(), mq.NackReason(reason)); err != nil {
		t.Fatalf("Nack(dead-letter): %v", err)
	}
	out, err := h.server.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(h.server.QueueURL("dead")),
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil || len(out.Messages) != 1 {
		t.Fatalf("reading the dead-letter queue: %d message(s), %v", len(out.Messages), err)
	}
	const want = "handler failed: ��[31mred�[0m � end�"
	if got := aws.ToString(out.Messages[0].MessageAttributes["iqmq-dlq-reason"].StringValue); got != want {
		t.Fatalf("the copy's iqmq-dlq-reason is %q, want %q: every character outside the service's set replaced, and the rest kept", got, want)
	}
}

// TestRejectWithNoDestinationIsRefused is the other half: only an explicit
// destination authorises a route off the queue, and the refusal leaves the
// delivery settleable.
func TestRejectWithNoDestinationIsRefused(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "reject-none")
	h.publish(name, &mq.Message{Body: []byte("poison")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Reject(); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Reject with no destination = %v, want ErrUnsupported", err)
	}
	if got := h.depth(name); got != 1 {
		t.Fatalf("the queue holds %d message(s); a refused Reject destroys nothing", got)
	}
	// A definite refusal leaves the delivery settleable.
	if err := d.Nack(); err != nil {
		t.Fatalf("the delivery is no longer settleable after a refused Reject: %v", err)
	}
}

// TestFailedDeadLetterSendNeverAcksTheOriginal is the specification stated as a rule and
// tested as one: "a failed or ambiguous DLQ publish never acks the original".
//
// A driver that deleted first, or that deleted after a send it could not
// confirm, would acknowledge a message it had written nowhere — which is
// silent loss on the one path that exists to make a poison message safe.
func TestFailedDeadLetterSendNeverAcksTheOriginal(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) {
		c.AckDeadline = time.Hour
		c.DeadLetterQueue = "dead"
		c.MaxReceiveCount = 5
	})
	name := topic(t, "dlq-fail")
	h.publish(name, &mq.Message{Body: []byte("must-not-vanish")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)

	// Every send to the dead-letter queue is refused for as long as the
	// fault is installed.
	f := server.Fault(sqsfake.Fault{Op: "SendMessage", Queue: "dead", Err: errFaulted, Count: -1})
	if err := d.Reject(); err == nil {
		t.Fatal("the settlement succeeded while the dead-letter send was refused: the message has been acknowledged on the source queue and written nowhere ")
	}
	server.Remove(f)
	if got := h.depth(name); got != 1 {
		t.Fatalf("the source queue holds %d message(s) after the refused route", got)
	}
	if got := h.server.Bodies("dead"); len(got) != 0 {
		t.Fatalf("the dead-letter queue holds %v after a send that was refused", got)
	}

	// The delivery is still settleable, which is the whole point.
	if err := d.Reject(); err != nil {
		t.Fatalf("the retried Reject = %v; a definite refusal leaves the delivery settleable", err)
	}
	if got := h.server.Bodies("dead"); len(got) != 1 || got[0] != "must-not-vanish" {
		t.Fatalf("the dead-letter queue holds %v", got)
	}
	// And exactly one copy arrived: the refused attempt wrote nothing.
	if got := h.depth(name); got != 0 {
		t.Fatalf("the source queue holds %d message(s) after the route succeeded", got)
	}
}

// TestNativeRedriveMovesAnUnsettledMessage is the OTHER dead-letter route: the
// queue's own redrive policy, which needs no settlement from the consumer at
// all and is what makes a message nobody ever settles safe.
func TestNativeRedriveMovesAnUnsettledMessage(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.AckDeadline = time.Second
		c.DeadLetterQueue = "dead"
		c.MaxReceiveCount = 2
	})
	name := topic(t, "redrive")
	h.publish(name, &mq.Message{Body: []byte("never-settled")})
	cons := h.consumer(name, 4)
	// Two deliveries, neither settled, and the window passes after each.
	for range 2 {
		d := h.receiveOne(cons, 5*time.Second)
		if string(d.Body) != "never-settled" {
			t.Fatalf("delivery = %q", d.Body)
		}
		h.server.Advance(2 * time.Second)
	}
	// The third receive finds the message past its receive count, and the
	// service moves it rather than delivering it.
	h.expectNothing(cons, 500*time.Millisecond, "a message the redrive policy has moved")
	if got := h.server.Bodies("dead"); len(got) != 1 || got[0] != "never-settled" {
		t.Fatalf("the dead-letter queue holds %v", got)
	}
}

// TestExtendMovesTheWindow is DeadlineExtender, and the half a driver
// that forgot to move its LOCAL view would break: the next settlement would be
// refused against a deadline that is no longer the service's.
func TestExtendMovesTheWindow(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = 10 * time.Second })
	name := topic(t, "extend")
	h.publish(name, &mq.Message{Body: []byte("slow")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	before := d.Info().LeaseExpires
	if err := d.Extend(time.Hour); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	// Past the original deadline and inside the new one.
	h.server.Advance(11 * time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("the Ack after an extension = %v; the extension moved the window and the local lease check has to move with it (original deadline %v)", err, before)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) left after the extended Ack", got)
	}
}

// TestExtendRefusesZero is the refusal the specification asks for: a timeout of zero makes
// the message immediately visible, which is what a Nack means and the opposite
// of an extension.
func TestExtendRefusesZero(t *testing.T) {
	h := open(t)
	name := topic(t, "extend-zero")
	h.publish(name, &mq.Message{Body: []byte("b")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Extend(0); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Extend(0) = %v, want CodeInvalidArgument", err)
	}
	if err := d.Extend(13 * time.Hour); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("Extend past the service's twelve hours = %v, want CodeInvalidArgument", err)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("the delivery is no longer settleable after a refused extension: %v", err)
	}
}

// TestSettlementPoliciesCostWhatTheyClaim drives the four kinds
// through the DRIVER and counts the requests each one makes.
//
// The family's own tests measure the policy; this is the end-to-end check that
// the binding works, and it is what would catch a driver that took the kind
// and then handed the kit something else.
func TestSettlementPoliciesCostWhatTheyClaim(t *testing.T) {
	for _, tc := range []struct {
		kind       flush.Kind
		concurrent bool
		wantCalls  int
	}{
		// `immediate` is settled SEQUENTIALLY, and that is not a
		// convenience. driverkit.BatchSettler.submit appends to the queue
		// and then flushes everything in it, so two settlements that arrive
		// before either flush runs travel together even under the immediate
		// policy: written with ten concurrent goroutines this row asserted
		// ten requests and saw nine. What `immediate` guarantees is that
		// nothing is HELD waiting for a batch, not that every settlement is
		// its own request — concurrency can only reduce the count, never
		// raise it.
		{flush.Immediate, false, 10},
		{flush.Count, true, 1},
		{flush.Hybrid, true, 1},
	} {
		t.Run(tc.kind.String(), func(t *testing.T) {
			h := open(t, func(c *sq.Config) {
				c.AckDeadline = time.Hour
				c.Settle = tc.kind
				c.SettleMaxItems = 10
				c.SettleInterval = 20 * time.Millisecond
			})
			name := topic(t, "settle-"+tc.kind.String())
			const n = 10
			for i := range n {
				h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
			}
			cons := h.consumer(name, n)
			ds := h.receive(cons, n, 10*time.Second)
			before := h.server.Calls("DeleteMessageBatch") + h.server.Calls("DeleteMessage")
			if tc.concurrent {
				for _, err := range settleTogether(t, ds, func(d *mq.Delivery) error { return d.Ack() }) {
					if err != nil {
						t.Fatalf("Ack: %v", err)
					}
				}
			} else {
				for i := range ds {
					if err := ds[i].Ack(); err != nil {
						t.Fatalf("Ack: %v", err)
					}
				}
			}
			calls := h.server.Calls("DeleteMessageBatch") + h.server.Calls("DeleteMessage") - before
			if calls != tc.wantCalls {
				t.Fatalf("%v settled %d deliveries in %d request(s), want %d", tc.kind, n, calls, tc.wantCalls)
			}
			if got := h.depth(name); got != 0 {
				t.Fatalf("%d message(s) left", got)
			}
		})
	}
}

// TestTimePolicyFlushesOnItsTicker is the fourth kind, which cannot be driven
// by the count at all: nothing sends until the shared ticker fires.
func TestTimePolicyFlushesOnItsTicker(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.AckDeadline = time.Hour
		c.Settle = flush.Time
		c.SettleMaxItems = 10
		c.SettleInterval = 20 * time.Millisecond
	})
	name := topic(t, "settle-time")
	const n = 3
	for i := range n {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	cons := h.consumer(name, n)
	ds := h.receive(cons, n, 10*time.Second)
	before := h.server.Calls("DeleteMessageBatch")
	for _, err := range settleTogether(t, ds, func(d *mq.Delivery) error { return d.Ack() }) {
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if got := h.server.Calls("DeleteMessageBatch") - before; got == 0 || got > n {
		t.Fatalf("three settlements under the time policy cost %d batch call(s)", got)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) left", got)
	}
}

// TestCloseReleasesUnfetchedDeliveries is the specification's "Close requeues or abandons
// everything it has handed out".
//
// A delivery the application never saw must not wait out its visibility
// window: the source makes it visible again, so the next consumer gets it at
// once rather than in thirty seconds.
func TestCloseReleasesUnfetchedDeliveries(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "close-release")
	const n = 5
	for i := range n {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	cons := h.consumer(name, n)
	// One fetch takes one delivery; the poller has pushed the rest into the
	// ring, where nobody has asked for them.
	d := h.receiveOne(cons, 5*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The release is asserted DIRECTLY, before anything tries to receive: the specification
	// says Close makes what it handed out visible again, and "a second
	// consumer got them" is a different claim that also needs the poller, the
	// ring and the fetch loop to work. Conflating the two is why the first
	// GitHub Actions run of this repository reported only "took 2 of 4" and
	// named neither half.
	//
	// Not a poll with a deadline: Close returned, and the requeue is part of
	// what it returns FROM.
	if inv := h.invisible(name); inv != 0 {
		t.Fatalf("%d of the %d unfetched message(s) are still invisible after Close returned: the source "+
			"did not make them visible again, so they wait out the %v deadline this source was opened with "+
			"(queue depth %d, bodies %v)", inv, n-1, time.Hour, h.depth(name), h.bodies(name))
	}

	next := h.consumer(name, n)
	// Every remaining message is available at once. Without the release they
	// would be invisible for the hour this source's deadline names.
	got := h.receive(next, n-1, 10*time.Second)
	for i := range got {
		if err := got[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if left := h.depth(name); left != 0 {
		t.Fatalf("%d message(s) left after the second consumer drained the queue", left)
	}
}

// TestASourcesCloseReleasesItsRingWithADeliveryUnsettled holds the source's
// Close to what its documentation promises, which used to say more: that Close
// made everything handed out and never settled visible again. It promises the
// release of what the ring still holds, which nobody was handed, with a
// delivery handed out and unsettled beside it, and nothing about that
// delivery, which the specification lets Close requeue or abandon.
func TestASourcesCloseReleasesItsRingWithADeliveryUnsettled(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "close-ring")
	const n = 3
	for i := range n {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	src, err := h.back.OpenSource(t.Context(), mq.SourceConfig{Topic: name, Prefetch: n})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	if got, err := src.Fetch(ctx, dst); got != 1 {
		t.Fatalf("Fetch = %d, %v; want one delivery", got, err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inv := h.invisible(name); inv > 1 {
		t.Fatalf("%d message(s) invisible after Close, want no more than the one handed out: what the ring held "+
			"was not made visible again", inv)
	}
}

// TestCloseReleasesTheRestOfTheAnswerItsPollerHeld is the defect behind
// TestCloseReleasesUnfetchedDeliveries' failures on GitHub ("took 2 of 4", and
// here "2 of the 4 unfetched message(s) are still invisible after Close"),
// with the race taken out of it.
//
// A poller admits a ReceiveMessage answer one message at a time. When Close
// lands part-way through, the push of the next message fails and that message
// was released -- and the poller used to return there, abandoning the rest of
// the answer: received, so invisible at the service, and in neither the ring
// nor anything Close drains. The flaky test reached that
// only when the consumer fetched, acked and closed faster than the poller
// admitted five messages: 20 and 21 runs in 2000 at GOMAXPROCS=2 on go1.26.5
// and go1.27.1, about one in a hundred.
//
// Here the poller is PARKED in that state before Close is called, not raced
// into it. One poller, ten messages an answer and a prefetch of one make the
// ring exactly one answer deep; nothing fetches, so the first answer fills it
// and the poller parks pushing the first message of the second, holding the
// other nine. The wait is for that parked push, observed through the ring
// (sq.ParkedPushesForTest), and the precondition that both answers were
// received is checked before Close, so the test cannot pass by never
// reaching the state it is about.
func TestCloseReleasesTheRestOfTheAnswerItsPollerHeld(t *testing.T) {
	const answer = 10
	h := open(t, func(c *sq.Config) {
		c.AckDeadline = time.Hour
		c.Pollers = 1
		c.MaxMessages = answer
	})
	name := topic(t, "close-mid-answer")
	for i := range 2 * answer {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	cons := h.consumer(name, 1)
	srcs := sq.SourcesForTest(h.back)
	if len(srcs) != 1 {
		t.Fatalf("the backend has %d sources open, want the consumer's one", len(srcs))
	}
	bound := time.Now().Add(10 * time.Second)
	for {
		parked, ok := sq.ParkedPushesForTest(srcs[0])
		if !ok {
			t.Fatalf("SourcesForTest returned a %T, not a source", srcs[0])
		}
		if parked == 1 {
			break
		}
		if time.Now().After(bound) {
			t.Fatalf("the poller never parked pushing its second answer (%d parked, %d invisible); the ring is "+
				"not one answer deep, so this test cannot reach the state it is about", parked, h.invisible(name))
		}
		runtime.Gosched()
	}
	if inv := h.invisible(name); inv != 2*answer {
		t.Fatalf("%d messages invisible before Close, want %d: both answers received, one in the ring and one "+
			"in the poller's hands", inv, 2*answer)
	}

	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inv := h.invisible(name); inv != 0 {
		t.Fatalf("%d of %d received messages are still invisible after Close returned: the rest of the answer "+
			"the poller was admitting was abandoned rather than released, so it waits out the %v deadline "+
			"(queue depth %d)", inv, 2*answer, time.Hour, h.depth(name))
	}
}

// TestSeekIsRefused is the rule for an option the driver cannot honour: it
// is refused before any I/O, and with ErrUnsupported so that the negative
// direction is provable.
func TestSeekIsRefused(t *testing.T) {
	h := open(t)
	name := topic(t, "seek")
	for _, start := range []mq.StartPosition{
		mq.StartAt(time.Unix(0, 0)),
		mq.StartAtSequence(1),
	} {
		_, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{
			Topic: name, Subscription: "s", Start: start,
		})
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("a consumer at %v = %v, want ErrUnsupported", start, err)
		}
	}
	// The two positions the driver DOES accept are the same position, which
	// is what a queue with no history means.
	for _, start := range []mq.StartPosition{mq.StartLatest(), mq.StartEarliest()} {
		c, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{
			Topic: name, Subscription: "s", Start: start,
		})
		if err != nil {
			t.Fatalf("a consumer at %v: %v", start, err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestFIFODeduplicates is the conditional claim: a DedupKey becomes a
// MessageDeduplicationId and the service discards the second publication.
func TestFIFODeduplicates(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.FIFO = true })
	name := topic(t, "dedup")
	for range 2 {
		h.publish(name, &mq.Message{Key: []byte("k"), Body: []byte("once")}, mq.DedupKey("d-1"))
	}
	if got := h.depth(name); got != 1 {
		t.Fatalf("the queue holds %d message(s) after two publications of one deduplication identifier", got)
	}
}

// TestFIFOWithoutADedupKeyIsNotDeduplicated is, and it is
// the conservative half: a publication with no DedupKey is given a unique
// identifier, so two identical bodies are two messages rather than one.
//
// The alternative — content-based deduplication on by default — would discard
// the second publication of an identical body inside a five-minute window,
// which is a message loss disqualifies.
func TestFIFOWithoutADedupKeyIsNotDeduplicated(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.FIFO = true })
	name := topic(t, "nodedup")
	for range 2 {
		h.publish(name, &mq.Message{Key: []byte("k"), Body: []byte("twice")})
	}
	if got := h.depth(name); got != 2 {
		t.Fatalf("the queue holds %d message(s) after two identical publications with no DedupKey, want 2: a driver that let the service deduplicate them by content would lose one", got)
	}
}

// TestPollersRunConcurrently is the specification's "N pollers": each one is a request in
// flight, so N pollers really is N concurrent receives.
func TestPollersRunConcurrently(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.Pollers = 4
		c.AckDeadline = time.Hour
	})
	name := topic(t, "pollers")
	const n = 20
	for i := range n {
		h.publish(name, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	cons := h.consumer(name, n)
	ds := h.receive(cons, n, 15*time.Second)
	seen := map[string]bool{}
	for i := range ds {
		body := string(ds[i].Body)
		if seen[body] {
			t.Fatalf("body %q was delivered twice by four pollers of one queue; the visibility timeout is what keeps them apart", body)
		}
		seen[body] = true
		if err := ds[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if got := h.server.Calls("ReceiveMessage"); got < 2 {
		t.Fatalf("four pollers made %d receive call(s)", got)
	}
}

// TestASubSecondWaitTimeIsStillALongPoll holds that a WaitTime under a second
// is rounded up to the service's whole second rather than down to zero. Zero
// is a SHORT poll: the service answers an empty queue at once, and the poller
// went round as fast as the round trip allowed, over a million receives a
// second against this server. The server waits at most a few milliseconds of
// any long poll, so one poller sitting on an empty queue for a fifth of a
// second makes a few dozen calls at the most; the short poll made hundreds of
// thousands.
func TestASubSecondWaitTimeIsStillALongPoll(t *testing.T) {
	h := open(t, func(c *sq.Config) {
		c.Pollers = 1
		c.WaitTime = 500 * time.Millisecond
	})
	name := topic(t, "subsecond-wait")
	h.consumer(name, 1)
	before := h.server.Calls("ReceiveMessage")
	time.Sleep(200 * time.Millisecond)
	if got := h.server.Calls("ReceiveMessage") - before; got > 100 {
		t.Fatalf("one poller on an empty queue made %d receive calls in 200ms with a 500ms WaitTime: "+
			"the long poll was truncated to a short one", got)
	}
}

// TestAdmin is the portable topology surface, including the one operation
// this driver refuses and the reason it refuses rather than answering "done".
func TestAdmin(t *testing.T) {
	h := open(t)
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	name := topic(t, "admin")
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: name}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	// Idempotent: the specification requires it, and the service's CreateQueue is.
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: name}); err != nil {
		t.Fatalf("the second EnsureTopic: %v", err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: name, Name: "s"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	// A subscription IS the queue, so deleting one would delete the topic.
	// Answering "done" would be worse than refusing.
	if err := admin.DeleteSubscription(t.Context(), name, "s"); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("DeleteSubscription = %v, want ErrUnsupported", err)
	}
	h.publish(name, &mq.Message{Body: []byte("b")})
	stats, err := admin.Stats(t.Context(), name, "s")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Depth.Known || stats.Depth.Value != 1 {
		t.Fatalf("Stats.Depth = %+v", stats.Depth)
	}
	if !stats.Lag.Known {
		t.Fatal("Stats.Lag is unknown; the service reports the counts it is built from")
	}
	found := false
	for got, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if got == h.queue(name) {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListTopics did not report %q", h.queue(name))
	}
	if err := admin.Purge(t.Context(), name, "s"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) survived the purge", got)
	}
	if err := admin.DeleteTopic(t.Context(), name); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := h.depth(name); got != -1 {
		t.Fatalf("the queue still exists after DeleteTopic (depth %d)", got)
	}
}

// TestTypedAdminRedrive is the specification's "DLQ native via redrive (typed Admin)".
func TestTypedAdminRedrive(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Hour })
	admin, err := h.back.TypedAdmin()
	if err != nil {
		t.Fatalf("TypedAdmin: %v", err)
	}
	source := topic(t, "redrive-src")
	dead := topic(t, "redrive-dead")
	h.publish(source, &mq.Message{Body: []byte("seed")})
	h.publish(dead, &mq.Message{Body: []byte("stuck")})

	// The redrive policy is a queue attribute, and reading it back is what
	// says the typed surface really wrote it.
	if err := admin.SetRedrivePolicy(t.Context(), source, sq.RedrivePolicy{
		DeadLetterQueue: h.queue(dead), MaxReceiveCount: 3,
	}); err != nil {
		t.Fatalf("SetRedrivePolicy: %v", err)
	}
	attrs, err := admin.Attributes(t.Context(), source)
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	policy := attrs["RedrivePolicy"]
	if policy == "" || !bytes.Contains([]byte(policy), []byte(h.queue(dead))) {
		t.Fatalf("RedrivePolicy = %q", policy)
	}

	// And the move task drains the dead-letter queue back.
	handle, err := admin.Redrive(t.Context(), sq.RedriveRequest{Source: dead, Destination: source})
	if err != nil {
		t.Fatalf("Redrive: %v", err)
	}
	if handle == "" {
		t.Fatal("Redrive returned no task handle; the move is asynchronous and the handle is how a caller follows it")
	}
	if got := h.depth(dead); got != 0 {
		t.Fatalf("the dead-letter queue holds %d message(s) after the redrive", got)
	}
	if got := h.bodies(source); len(got) != 2 {
		t.Fatalf("the source queue holds %v after the redrive", got)
	}
	if _, err := admin.Redrive(t.Context(), sq.RedriveRequest{}); err == nil {
		t.Fatal("Redrive with no source was accepted")
	}
}

// TestClosedBackendRefusesEverything is a closed client answers
// ErrClosed rather than reaching for a client that is gone.
func TestClosedBackendRefusesEverything(t *testing.T) {
	h := open(t)
	name := topic(t, "closed")
	h.publish(name, &mq.Message{Body: []byte("b")})
	if err := h.client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Idempotent, and the second Close answers the first one's error.
	if err := h.client.Close(); err != nil {
		t.Fatalf("the second Close: %v", err)
	}
	if err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("b")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("publishing after Close = %v, want ErrClosed", err)
	}
	if _, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: name, Subscription: "s"}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("opening a consumer after Close = %v, want ErrClosed", err)
	}
	if err := h.client.Ping(t.Context()); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Ping after Close = %v, want ErrClosed", err)
	}
	if got := h.client.Health().State; got != mq.HealthClosed {
		t.Fatalf("Health after Close = %v; Closed is terminal", got)
	}
}

// TestFetchSkipsSupersededDeliveries is, and it is the shape
// the lease/expiry-redelivery scenario has.
//
// A poller keeps polling while the application is doing something else, so a
// message whose visibility window passes is received again and pushed into the
// ring behind the delivery nobody has fetched yet. A Fetch that handed out the
// older one would hand the caller a delivery whose settlement is refused —
// while the message sits one slot along, perfectly settleable.
func TestFetchSkipsSupersededDeliveries(t *testing.T) {
	h := open(t, func(c *sq.Config) { c.AckDeadline = time.Second })
	name := topic(t, "superseded")
	h.publish(name, &mq.Message{Body: []byte("abandoned")})
	cons := h.consumer(name, 4)
	held := h.receiveOne(cons, 5*time.Second)

	// Two windows pass with the delivery held, so the pollers receive the
	// message twice more and the ring holds two deliveries the newest of
	// which supersedes both.
	for range 2 {
		if n := h.server.ExpireVisibility(h.queue(name)); n == 0 {
			t.Fatal("the server released nothing; the message is not invisible")
		}
		waitFor(t, 5*time.Second, "the poller to receive the message again", func() bool {
			return h.invisible(name) == 1
		})
	}
	if err := held.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the first delivery's Ack = %v, want ErrLeaseLost: it has been superseded twice", err)
	}

	// And the delivery the caller is handed next is one it can settle.
	again := h.receiveOne(cons, 10*time.Second)
	if string(again.Body) != "abandoned" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("the redelivery's Ack = %v; Fetch handed out a delivery nobody can settle", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) left", got)
	}
	stale := h.stale()
	if stale == 0 {
		t.Fatal("Fetch dropped no superseded delivery, so this test never reached the case it is about")
	}
	t.Logf("Fetch dropped %d superseded deliver(ies)", stale)
}

// TestSecretsNeverRendered is over the four surfaces the
// conformance kit's `secrets/redaction` scenario probes, without a container.
//
// SQS is the first driver whose configuration carries a real credential, so it
// is the first driver on which that scenario runs rather than skips — and this
// is the same check as a unit test, so a regression is caught by `go test`
// rather than by an integration run somebody has to remember.
func TestSecretsNeverRendered(t *testing.T) {
	const secret = "iqmq-b08-secret-not-a-real-credential"
	const token = "iqmq-b08-token-not-a-real-credential"
	h := open(t, func(c *sq.Config) {
		c.SecretAccessKey = dsnSecret(secret)
		c.SessionToken = dsnSecret(token)
	})
	var rendered []string
	probe := func(what, s string) {
		for _, cred := range []string{secret, token} {
			if strings.Contains(s, cred) {
				rendered = append(rendered, what+": "+s)
			}
		}
	}
	info := h.client.Info()
	probe("Info", info.Name+" "+info.Profile+" "+info.ClientName+" "+info.ClientVersion+" "+info.BrokerVersion)
	if err := h.client.Publish(t.Context(), "\x00 invalid", &mq.Message{Body: []byte("x")}); err != nil {
		probe("a refused publication", err.Error())
	}
	if _, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{}); err != nil {
		probe("a refused consumer", err.Error())
	}
	if snap := h.client.Health(); snap.Err != nil {
		probe("Health", snap.Err.Error())
	}
	// Two more surfaces the kit does not reach and the specification covers: the
	// configuration's own renderings.
	probe("Config.String", h.cfg.String())
	probe("Config.LogValue", h.cfg.LogValue().String())
	// And the refusals the DRIVER builds rather than the core's validator.
	// Without these the check cannot fail: every probe above is answered
	// either by the core (an invalid topic, an empty consumer config) or by a
	// preallocated sentinel, so a driver that rendered its configuration
	// into an error would pass. A credential leaking into an error is only
	// reachable where the error is BUILT, which is the error mapper.
	name := topic(t, "secret")
	if err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("x")}, mq.TTL(time.Minute)); err != nil {
		probe("a refused option", err.Error())
	}
	strict := open(t, func(c *sq.Config) {
		c.SecretAccessKey = dsnSecret(secret)
		c.SessionToken = dsnSecret(token)
		// No auto-creation, so a topic that names no queue is refused by
		// the driver's own error mapper with the service's answer inside it.
		c.AutoCreate = false
	})
	missing := topic(t, "secret-missing")
	if _, err := strict.client.Consumer(t.Context(), mq.ConsumerConfig{
		Topic: missing, Subscription: "s",
	}); err != nil {
		probe("a consumer on a queue that does not exist", err.Error())
	} else {
		t.Fatal("a consumer opened on a queue that does not exist and may not be created")
	}
	if err := strict.client.Publish(t.Context(), missing, &mq.Message{Body: []byte("x")}); err != nil {
		probe("a publication to a queue that does not exist", err.Error())
	} else {
		t.Fatal("a publication reached a queue that does not exist and may not be created")
	}
	if snap := strict.client.Health(); snap.Err != nil {
		probe("Health after a failure", snap.Err.Error())
	}
	if len(rendered) > 0 {
		t.Fatalf("a credential appeared in %d rendered string(s), the first being %q: forbid it", len(rendered), rendered[0])
	}
}

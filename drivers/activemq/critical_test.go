package activemq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

//
// Each one is a scenario whose failure would be a silent data defect rather
// than a visible error, and each one is unreachable from a broker: the peer
// here is the shared core's scripted one, which can detach a link while the
// connection lives, hold a transfer's disposition for ever, and report every
// disposition it was sent.

// TestCriticalStaleDispositionIsRefusedOnAReattachedLink is the specification's
// disqualifier in AMQP's clothing.
//
// The specification: "a settlement must never apply to a delivery other than the one it
// names". A receiving link that detaches and re-attaches to the same address
// is the case where that can go wrong invisibly: the delivery ids start again
// from zero on the new link, so a disposition built for the old link's
// delivery 0 names the NEW link's delivery 0 if it is sent on the new link.
// The message that gets settled is then somebody else's, the caller's own
// message is redelivered, and nothing anywhere reports an error.
//
// What must happen instead is that the settlement is refused: the delivery is
// gone with its link, so the answer is a lost lease, and the reattached
// link must have received no disposition at all.
func TestCriticalStaleDispositionIsRefusedOnAReattachedLink(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, p := openPeer(t, cfg, artemisProps())
	_, source := addresses(t, cfg, "orders", "workers")
	src := openSource(t, b, "orders", "workers", 4)
	warm(t, b)

	// One delivery on the first link.
	if err := p.Deliver(source, tagged("first", "one")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	d := fetchOne(t, src, 5*time.Second)
	if got := string(d.Message.Body); got != "one" {
		t.Fatalf("the first delivery carries %q, want %q", got, "one")
	}

	// The link dies with the connection still alive, which is what a broker
	// does when a queue is deleted or a link is stolen.
	if err := p.DetachReceiver(source, &amqp.Error{Condition: amqp.ErrCondDetachForced, Description: "AMQ119021"}); err != nil {
		t.Fatalf("DetachReceiver: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	// One fetch reports the detached link's own error, which is the specification's "one
	// failed operation per link failure", and drops the dead link.
	if _, err := src.Fetch(ctx, dst); err == nil {
		t.Fatal("a fetch on a detached link succeeded")
	}
	// The re-attach is what puts a SECOND link on the same address for a
	// stale disposition to land on, and it has to happen through the
	// PRODUCTION path: a caller only ever calls Fetch, so a test that
	// re-attached by warming the link itself would pass against a driver
	// that never recovers.
	dst = fetchWhileDelivering(t, src, p, source, tagged("second", "two"))
	if got := string(dst[0].Message.Body); got != "two" {
		t.Fatalf("the delivery after the re-attach carries %q, want %q", got, "two")
	}
	if p.Receivers(source) < 2 {
		t.Fatalf("the peer saw %d receiving links on %s, want at least 2: the test cannot be about a reattached link if nothing reattached",
			p.Receivers(source), source)
	}

	// Now settle the FIRST delivery, whose link is gone.
	err := src.Settler().Ack(ctx, &d)
	if err == nil {
		t.Fatalf("acknowledging a delivery whose link detached reported success; the specification requires the settlement to be refused rather than applied to whatever delivery now holds that id")
	}
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("the refusal is %v (code %v), want a lost lease: the delivery went with its link and the specification makes that a lease loss rather than a failure the caller should retry",
			err, mq.CodeOf(err))
	}

	// The whole point: nothing reached the new link.
	if got := p.Dispositions(source); len(got) != 0 {
		t.Fatalf("the reattached link received %d dispositions (%+v); a disposition built for the old link's delivery would settle whatever delivery now holds that id, which is the disqualifier",
			len(got), got)
	}
	// And the second delivery is still settleable, which is the other half:
	// refusing the stale one must not have poisoned the live one.
	if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
		t.Errorf("acknowledging the live delivery after the stale one was refused: %v", err)
	}
}

// A broker detaches a link without closing the connection when the
// destination is deleted, when a link is stolen by another client of the same
// name, or when an address's settings change under it. Artemis does all three.
// The connection is alive afterwards, so the driver's reconnect loop never
// runs -- and a source that keys re-attachment on the CONNECTION generation
// alone would hold a dead link for ever and fail every fetch with a retryable
// error, which the core's engine would then retry for ever.
func TestCriticalLinkDetachIsRecovered(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, p := openPeer(t, cfg, artemisProps())
	_, source := addresses(t, cfg, "orders", "workers")
	src := openSource(t, b, "orders", "workers", 4)
	warm(t, b)

	if err := p.Deliver(source, tagged("a", "a")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	first := fetchOne(t, src, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := src.Settler().Ack(ctx, &first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	genBefore := b.Generation()

	if err := p.DetachReceiver(source, &amqp.Error{Condition: amqp.ErrCondDetachForced, Description: "AMQ119021: queue deleted"}); err != nil {
		t.Fatalf("DetachReceiver: %v", err)
	}

	// The specification allows the failure to surface once: the parked fetch wakes with the
	// link's own error. What it may not do is keep failing.
	dst := make([]mq.DriverDelivery, 1)
	if _, err := src.Fetch(ctx, dst); err == nil {
		t.Log("the fetch after the detach succeeded, which is allowed: the driver may reattach before it reads")
	} else if mq.CodeOf(err) != mq.CodeUnavailable && mq.CodeOf(err) != mq.CodeNotFound {
		t.Errorf("the fetch after a forced detach reports %v (code %v), want Unavailable or NotFound", err, mq.CodeOf(err))
	}

	// Recovery has to happen through the PRODUCTION path -- a caller only
	// ever calls Fetch -- so the fetch runs while the message is delivered to
	// whatever link it attaches.
	dst = fetchWhileDelivering(t, src, p, source, tagged("b", "b"))
	if string(dst[0].Message.Body) != "b" {
		t.Fatalf("after recovering, Fetch returned a delivery carrying %q, want \"b\"", dst[0].Message.Body)
	}
	if got := b.Generation(); got != genBefore {
		t.Errorf("the connection generation moved from %d to %d for a LINK detach; the generation is the connection's and bumping it would invalidate every other link's deliveries as well",
			genBefore, got)
	}
	if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
		t.Errorf("Ack on the reattached link: %v", err)
	}
}

// TestCriticalSenderLinkDetachIsAmbiguousAndRecovers is the publish side of
// the same thing.
//
// A transfer that has left the socket and whose disposition never arrived may
// have been accepted, so the failure must be AMBIGUOUS: a caller told
// "failed" republishes and the broker has the message twice. And the sink must
// come back, for the reason the source must.
func TestCriticalSenderLinkDetachIsAmbiguousAndRecovers(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, p := openPeer(t, cfg, artemisProps())
	sink, _ := addresses(t, cfg, "orders", "")
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The peer's transfer script is positional from its first transfer, so
	// the first publication is scripted as well as the second.
	p.PlanSendStates(&amqp.StateAccepted{})
	p.HoldSends(1)
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("one")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- snk.Publish(ctx, &mq.Message{Body: []byte("two")}, nil) }()
	// The publish is parked on a disposition that will not come; detaching
	// the link is what a broker restart or an address deletion does to it.
	deadline := time.After(3 * time.Second)
	for p.Held(sink) < 1 {
		select {
		case <-deadline:
			t.Fatalf("the peer never received the held transfer (Held = %d)", p.Held(sink))
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := p.DetachSender(sink, &amqp.Error{Condition: amqp.ErrCondDetachForced, Description: "AMQ119021"}); err != nil {
		t.Fatalf("DetachSender: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("a publication whose link detached while its disposition was outstanding reported SUCCESS; the specification makes that window ambiguous and reporting success loses the message")
		}
		if !mq.IsAmbiguous(err) {
			t.Errorf("the failure is %v (code %v), ambiguous=%v; a transfer that left the socket may have been accepted, so a caller told \"failed\" republishes and the broker has it twice",
				err, mq.CodeOf(err), mq.IsAmbiguous(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the publication never returned after its link detached")
	}

	// And the sink recovers.
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("three")}, nil); err != nil {
		t.Fatalf("the sink never recovered from a link detach: Publish = %v", err)
	}
}

// The specification: "Close unblocks blocked operations". A fetch parked on a link with no
// credit and no messages is the case that matters, because there is nothing to
// interrupt it with: the peer will not send, the context is not done, and the
// only thing that can wake it is the close. A driver that waited for the
// context would hang for as long as the caller's deadline, and a caller with
// no deadline would hang for ever.
func TestCriticalConsumerCancelUnblocksAParkedFetch(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, p := openPeer(t, cfg, artemisProps())
	_, source := addresses(t, cfg, "orders", "workers")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 2})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}

	parked := make(chan error, 1)
	go func() {
		dst := make([]mq.DriverDelivery, 4)
		// No deadline of its own: the close is the only thing that can end
		// this, which is the whole point.
		_, err := src.Fetch(context.Background(), dst)
		parked <- err
	}()
	// Wait until the peer has the link and its credit, so that the fetch is
	// really parked rather than still attaching.
	deadline := time.After(5 * time.Second)
	for p.Granted(source) <= 0 {
		select {
		case <-deadline:
			t.Fatalf("the fetch never issued credit (granted = %d), so it was not parked on the peer", p.Granted(source))
		default:
			time.Sleep(time.Millisecond)
		}
	}

	start := time.Now()
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-parked:
		if !errors.Is(err, mq.ErrClosed) {
			t.Errorf("the parked fetch woke with %v (code %v), want ErrClosed: the specification says a Close unblocks a blocked operation, and the error a caller sees must say the source closed rather than repeating whatever the close did to the link",
				err, mq.CodeOf(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Close returned after %v and the parked fetch is still parked; nothing else can wake it", time.Since(start))
	}
	// A second close is a no-op, which is the idempotence.
	if err := src.Close(); err != nil {
		t.Errorf("the second Close reported %v, want nil: the specification makes Close idempotent", err)
	}
}

// TestCriticalConsumerCancelReleasesUnsettledDeliveries is the other half of a
// cancel.
//
// A consumer that goes away holding unsettled deliveries must give them back,
// or they sit in the broker's delivering set until the link's own timeout and
// the next consumer waits for them. AMQP's answer is `released`, which
// explicitly does NOT count the attempt -- and that distinction is the reason
// the core has a release path separate from Nack: the driver is giving back
// what it never handed out, not reporting a failure.
func TestCriticalConsumerCancelReleasesUnsettledDeliveries(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, p := openPeer(t, cfg, artemisProps())
	_, source := addresses(t, cfg, "orders", "workers")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	warm(t, b)
	if err := p.Deliver(source, tagged("keep", "keep"), tagged("drop", "drop")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	dst := make([]mq.DriverDelivery, 2)
	n, err := src.Fetch(ctx, dst)
	if err != nil || n == 0 {
		t.Fatalf("Fetch: %d, %v", n, err)
	}
	if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := p.Dispositions(source)
	var accepted, released int
	for _, d := range got {
		switch d.Outcome {
		case core.OutcomeAccepted:
			accepted++
		case core.OutcomeReleased:
			released++
			if d.DeliveryFailed {
				t.Errorf("the close released %q with delivery-failed set; a release on the close path is the driver giving back what it never handed out, and counting it as a failed attempt would make MaxAttempts fire on a message nobody tried", d.Tag)
			}
		}
	}
	if accepted != 1 {
		t.Errorf("the peer saw %d accepted dispositions, want 1: %+v", accepted, got)
	}
	if n > 1 && released != n-1 {
		t.Errorf("the peer saw %d released dispositions for %d unsettled deliveries; a delivery a closing consumer keeps sits in the broker's delivering set until the link times out: %+v",
			released, n-1, got)
	}
}

// TestCriticalLinkDetachWhileAFetchIsParkedIsRecovered is the state in which
// the link-loss guard on the READ is the only thing that can recover.
//
// A source whose credit is already with the peer issues no more: the window's
// invariant is `issued + held <= max`, so a full window's top-up is a no-op
// and cannot fail. So a link that dies while a fetch is parked on it is
// discovered by the READ and by nothing else — and a driver that dropped the
// dead link only when a flow frame failed would hold this one for ever, with
// every fetch failing on a link the window thinks is fine.
func TestCriticalLinkDetachWhileAFetchIsParkedIsRecovered(t *testing.T) {
	t.Parallel()
	cfg := artemisConfig()
	b, p := openPeer(t, cfg, artemisProps())
	_, source := addresses(t, cfg, "orders", "workers")
	src := openSource(t, b, "orders", "workers", 2)
	// Warming issues the whole prefetch, so the window is full of credit the
	// peer holds and a further top-up has nothing to send.
	warm(t, b)
	if got := p.Granted(source); got != 2 {
		t.Fatalf("the peer was granted %d credit, want the whole prefetch of 2: this test is about a window with no room to issue more", got)
	}

	parked := make(chan error, 1)
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err := src.Fetch(ctx, dst)
		parked <- err
	}()
	// Give the fetch time to park on the link rather than on the window.
	time.Sleep(50 * time.Millisecond)
	if err := p.DetachReceiver(source, &amqp.Error{Condition: amqp.ErrCondDetachForced, Description: "AMQ119021: address removed"}); err != nil {
		t.Fatalf("DetachReceiver: %v", err)
	}
	select {
	case err := <-parked:
		if err == nil {
			t.Fatal("the parked fetch returned no error after its link was detached")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the parked fetch never woke after its link was detached")
	}

	dst := fetchWhileDelivering(t, src, p, source, tagged("after", "after"))
	if got := string(dst[0].Message.Body); got != "after" {
		t.Fatalf("the delivery after the re-attach carries %q, want \"after\"", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := src.Settler().Ack(ctx, &dst[0]); err != nil {
		t.Errorf("Ack on the reattached link: %v", err)
	}
}

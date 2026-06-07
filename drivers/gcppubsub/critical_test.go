package gcppubsub_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mq "bitbucket.org/iqhive/iqmq"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// The driver-specific critical tests (the last fixed section). Each one is
// a failure mode this broker has and most brokers do not, and each was watched
// failing against a deliberate mutation of the production code before it was
// trusted.

// TestCriticalOrderingKeyIsResumedAfterAFailedPublication is the most
// consequential test in this package.
//
// The client library puts an ordering key into a permanently failed state when
// a publication carrying it fails, and refuses every LATER publication of that
// key for the life of the publisher — reporting the pause only to those later
// publications, never to the one that caused it. A driver that does not call
// ResumePublish therefore has a publisher that has silently stopped carrying
// one key while every other key still works, which is the message loss in slow motion rather than an error anybody sees.
//
// This runs against the REAL client library over its own in-process fake
// service, because the behaviour under test is the library's and a model of it
// would prove nothing. pstest's publish-response queue is what makes the first
// publication fail: the code is PermissionDenied, which the generated client's
// retry policy does not retry, so exactly one response is consumed.
func TestCriticalOrderingKeyIsResumedAfterAFailedPublication(t *testing.T) {
	l := newLive(t)
	c := l.open(t, func(cfg *gps.Config) { cfg.Ordering = true })
	topic := newTopic("resume")
	provision(t, c, topic, "s")
	ctx := context.Background()
	pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	l.srv.SetAutoPublishResponse(false)
	l.srv.ResetPublishResponses(4)
	l.srv.AddPublishResponse(nil, status.Error(codes.PermissionDenied, "injected: the publication is refused"))
	l.srv.AddPublishResponse(&pubsubpb.PublishResponse{MessageIds: []string{"second"}}, nil)

	const key = "customer-7"
	first := pub.Publish(ctx, &mq.Message{Key: []byte(key), Body: []byte("first")})
	if first == nil {
		t.Fatal("the injected refusal was not reported to the publication that caused it")
	}
	// The second publication of the SAME key is the whole test: without the
	// driver's ResumePublish the library refuses it locally, having never
	// sent it, and the queued success is never consumed.
	if err := pub.Publish(ctx, &mq.Message{Key: []byte(key), Body: []byte("second")}); err != nil {
		t.Fatalf("the second publication of ordering key %q failed: %v\n"+
			"the client library pauses a key whose publication failed and this driver did not resume it, "+
			"so the publisher has silently stopped carrying that key ", key, err)
	}
}

// TestCriticalOrderingKeyPauseIsRealInTheFake holds the scripted fake to the
// same contract, which is what makes the fake usable as an oracle: a fake that
// did not pause would let the test above pass for the wrong reason if it ever
// moved onto it.
func TestCriticalOrderingKeyPauseIsRealInTheFake(t *testing.T) {
	srv := psfake.New("iqmq-test")
	srv.CreateTopic("t")
	pub, paused := pausedPublisher(t, srv, "t", true)
	srv.PublishErr = func(string, *psapi.Outgoing) error {
		return errors.New("injected")
	}
	ctx := context.Background()
	if _, err := pub.Publish(ctx, &psapi.Outgoing{Data: []byte("a"), OrderingKey: "k"}).Get(ctx); err == nil {
		t.Fatal("the injected failure was not reported")
	}
	if !paused.Paused("k") {
		t.Fatal("the fake did not pause the ordering key of a failed publication, so it cannot prove the driver resumes one")
	}
	srv.PublishErr = nil
	if _, err := pub.Publish(ctx, &psapi.Outgoing{Data: []byte("b"), OrderingKey: "k"}).Get(ctx); !errors.Is(err, psapi.ErrOrderingPaused) {
		t.Fatalf("a publication of a paused key reported %v, want ErrOrderingPaused", err)
	}
	pub.ResumePublish("k")
	if _, err := pub.Publish(ctx, &psapi.Outgoing{Data: []byte("c"), OrderingKey: "k"}).Get(ctx); err != nil {
		t.Fatalf("a publication after ResumePublish failed: %v", err)
	}
}

// TestCriticalDelayedNackIsAnInProcessHold is the other half of the specification's
// emulation claim.
//
// Pub/Sub has no nack delay: a nack is a ModifyAckDeadline of zero and the
// message becomes deliverable at once. So a delayed negative settlement has to
// hold the DELIVERY, and this test proves both directions of that — the
// message is NOT deliverable while the hold runs, and it IS once the hold
// ends. A driver that ignored the delay would pass the second assertion and
// fail the first.
func TestCriticalDelayedNackIsAnInProcessHold(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.AckDeadline = time.Minute })
	topic := newTopic("hold")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)
	sub, err := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	const delay = 700 * time.Millisecond
	start := time.Now()
	if err := d.Nack(mq.NackDelay(delay)); err != nil {
		t.Fatalf("Nack(NackDelay(%v)): %v", delay, err)
	}
	// While the hold runs the message is outstanding at the service — not
	// settled, not deliverable — and the consumer sees nothing. A driver that
	// nacked immediately would deliver it within milliseconds.
	if got := srv.Outstanding(sub); got != 1 {
		t.Fatalf("the held message is not outstanding at the service (outstanding %d); "+
			"a hold that settled the message immediately is not a delay", got)
	}
	receiveNone(t, cons, delay/2)
	// And once the hold ends the message really comes back.
	again := receiveOne(t, cons, 5*time.Second)
	if held := time.Since(start); held < delay {
		t.Fatalf("the redelivery arrived after %v, which is less than the %v delay asked for", held, delay)
	}
	if string(again.Body) != "held" {
		t.Fatalf("the redelivery carries %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestCriticalDelayedNackHoldEndsOnClose races a delayed negative settlement
// against Close, for thousands of rounds.
//
// It is the playbook's recurring defect class one layer out: a record placed
// somewhere the driver holds it while the driver is closing. The hold is
// in-process, so a hold that outlives its source is TWO failures at once — a
// goroutine and a timer nobody can stop, and a message held unacknowledged by
// a source that no longer exists until its deadline expires, which turns a
// three-hundred-millisecond delay into the acknowledgement deadline. The
// established fix is to re-read the closing flag after registering, under the
// mutex Close takes, and that is what this exercises.
//
// Thousands of rounds, because the known instances of this class appeared at
// five to twenty failures per two or three thousand: a hundred rounds proves
// nothing.
func TestCriticalDelayedNackHoldEndsOnClose(t *testing.T) {
	rounds := 3000
	if testing.Short() {
		rounds = 200
	}
	for round := range rounds {
		c, srv := scripted(t, func(cfg *gps.Config) { cfg.AckDeadline = time.Minute })
		topic := newTopic("holdclose")
		provision(t, c, topic, "s")
		ctx := context.Background()
		if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("held")}); err != nil {
			t.Fatalf("round %d: Publish: %v", round, err)
		}
		cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
		if err != nil {
			t.Fatalf("round %d: Consumer: %v", round, err)
		}
		d := receiveOne(t, cons, 5*time.Second)
		sub, err := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
		if err != nil {
			t.Fatalf("round %d: SubscriptionID: %v", round, err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		var nackErr error
		go func() {
			defer wg.Done()
			// A delay long enough that the hold is certainly still running
			// when Close arrives.
			nackErr = d.Nack(mq.NackDelay(30 * time.Second))
		}()
		go func() {
			defer wg.Done()
			_ = cons.Close()
		}()
		wg.Wait()
		_ = c.Close()
		// Whatever the interleaving, the message must not be left held: it is
		// either back in the backlog (nacked) or the settlement was refused
		// outright, and never outstanding with nobody holding it.
		if srv.Outstanding(sub) != 0 {
			t.Fatalf("round %d: the message is still outstanding after Close (nack err %v); "+
				"a hold that outlived its source holds the message until its deadline expires", round, nackErr)
		}
	}
}

// TestCriticalStaleSettlementAfterRedeliveryIsRefused is the local lease
// check.
//
// The service accepts an acknowledgement for an identifier whose message
// another delivery now owns, and there is no API that asks "do I still hold
// this?". So the refusal has to happen in the driver, before the call, and
// this is the shape that needs it: one source receives one message twice
// because its deadline expired, and a settlement of the FIRST delivery would
// acknowledge the message the second one is working on.
func TestCriticalStaleSettlementAfterRedeliveryIsRefused(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) { cfg.AckDeadline = time.Minute })
	topic := newTopic("stale")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("twice")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	first := receiveOne(t, cons, 5*time.Second)
	// The service's own redelivery: the deadline passed and the message came
	// back with a NEW acknowledgement identifier.
	if n := srv.Expire(); n != 1 {
		t.Fatalf("Expire redelivered %d messages, want 1", n)
	}
	second := receiveOne(t, cons, 5*time.Second)
	if err := first.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acknowledging the superseded first delivery reported %v (code %v), want ErrLeaseLost: "+
			"it acknowledges a message the second delivery now owns ", err, mq.CodeOf(err))
	}
	if err := second.Ack(); err != nil {
		t.Fatalf("acknowledging the current delivery failed: %v", err)
	}
}

// TestCriticalCompletionsDoNotCostAGoroutinePerMessage holds the asynchronous
// publish path rule 3.
//
// The obvious implementation of PublishAsync on this client library is
// `go func() { res.Get(ctx); done(err) }()`, which is a goroutine per message.
// The pool is what makes it a fixed number, and this is what notices if the
// pool is bypassed: the goroutine count is sampled while a thousand
// publications are outstanding and compared against the count with sixteen
// outstanding.
func TestCriticalCompletionsDoNotCostAGoroutinePerMessage(t *testing.T) {
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.CompletionWorkers = 4
		cfg.CompletionQueue = 4096
	})
	topic := newTopic("pool")
	provision(t, c, topic, "s")
	ctx := context.Background()
	pub, err := c.Publisher(ctx, mq.PublisherConfig{Topic: topic, MaxPendingMessages: 8192})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// The publications are held OUTSTANDING, which is the whole point: with
	// results that resolve immediately a goroutine per message finishes
	// before the next publication starts and the count never grows, so the
	// measurement could not tell the two shapes apart.
	measure := func(n int) int {
		srv.GatePublishResults()
		var wg sync.WaitGroup
		wg.Add(n)
		for i := range n {
			if err := pub.PublishAsync(ctx, &mq.Message{Body: []byte("x")}, func(mq.PublishResult) {
				wg.Done()
			}); err != nil {
				t.Fatalf("PublishAsync(%d): %v", i, err)
			}
		}
		// Every publication is now outstanding and nothing can complete, so
		// the count is whatever the completion path costs for n of them.
		peak := runtime.NumGoroutine()
		srv.ReleasePublishResults()
		wg.Wait()
		return peak
	}
	small := measure(16)
	large := measure(1000)
	// The pool is four workers wide, so the difference between sixteen and a
	// thousand outstanding publications must be a handful of goroutines and
	// not a thousand of them. The bound is deliberately generous: what it has
	// to catch is linear growth.
	if large > small+64 {
		t.Fatalf("a thousand outstanding publications held %d goroutines against %d for sixteen: "+
			"the completion path is creating a goroutine per message, which is what the fixed pool exists to avoid (rule 3)",
			large, small)
	}
}

// TestCriticalRejectReachesTheDeadLetterTopicBeforeAcknowledging is the specification's
// Reject on a broker whose native dead-letter policy cannot serve it.
//
// The subscription's own policy moves a message the SERVICE has delivered at
// least five times, which is not what a caller rejecting a message once is
// asking for. So a Reject publishes to the dead-letter topic and acknowledges
// the original afterwards — and the ORDER is what makes it safe: this test
// makes the republication fail and asserts that the original is still
// settleable, because a route that acknowledged first would have destroyed it.
func TestCriticalRejectReachesTheDeadLetterTopicBeforeAcknowledging(t *testing.T) {
	dlq := newTopic("dlq")
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = 5
		cfg.AckDeadline = time.Minute
	})
	topic := newTopic("reject")
	provision(t, c, topic, "s")
	provision(t, c, dlq, "d")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)

	// First: the republication fails. The original must NOT be acknowledged.
	srv.PublishErr = func(topic string, _ *psapi.Outgoing) error {
		if topic == dlq {
			return status.Error(codes.Unavailable, "injected: the dead-letter topic is unreachable")
		}
		return nil
	}
	if err := d.Reject(); err == nil {
		t.Fatal("Reject succeeded while the dead-letter publication failed")
	}
	sub, err := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	if got := srv.Outstanding(sub); got != 1 {
		t.Fatalf("the original is no longer outstanding after a failed dead-letter publication (outstanding %d): "+
			"a Reject that acknowledged before the republication succeeded would have destroyed the message", got)
	}

	// Then: the republication succeeds, and the message reaches the
	// destination and leaves the origin.
	srv.PublishErr = nil
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	dcons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: dlq, Subscription: "d"})
	if err != nil {
		t.Fatalf("Consumer(dlq): %v", err)
	}
	defer func() { _ = dcons.Close() }()
	got := receiveOne(t, dcons, 5*time.Second)
	if string(got.Body) != "poison" {
		t.Fatalf("the dead-letter topic holds %q", got.Body)
	}
	if err := got.Ack(); err != nil {
		t.Fatalf("Ack in the dead-letter topic: %v", err)
	}
}

// TestCriticalRejectWithNoDestinationIsRefused is the negative half, and it is
// the scenario the specification runs as `dlq/failed-publish-never-acks`: a Reject with
// nowhere to send the message must refuse, must not settle, and must leave the
// delivery settleable.
func TestCriticalRejectWithNoDestinationIsRefused(t *testing.T) {
	c, _ := scripted(t, func(cfg *gps.Config) { cfg.AckDeadline = time.Minute })
	topic := newTopic("nodlq")
	provision(t, c, topic, "s")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)
	if err := d.Reject(); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Reject with no dead-letter topic reported %v (code %v), want ErrUnsupported", err, mq.CodeOf(err))
	}
	if d.Settled() {
		t.Fatal("a refused Reject settled the delivery")
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("the delivery is not settleable after a refused Reject: %v", err)
	}
}

// TestCriticalCloseWithUnsettledDeliveriesDoesNotHang is the specification's "Close
// completes promptly" against the one thing on this broker that can stop it.
//
// The client library's shutdown WAITS for every message it has handed to the
// callback to be acknowledged or negatively settled — messageIterator.stop
// blocks on exactly that — so a source whose Close waited for Receive to
// return BEFORE settling what it had handed out would deadlock, and a caller
// would see Close hang for as long as the automatic extension keeps those
// messages alive: ten minutes on the default configuration.
//
// It is only reachable against the real client library, which is why this test
// uses it: the scripted fake's Receive returns as soon as its context is done
// and would pass either way.
//
// A regression shows up as the whole PACKAGE timing out rather than as this
// message, because the test's own cleanup closes the client and deadlocks in
// the same place. That is worth knowing when it happens: `panic: test timed
// out` with this function on the stack is this defect.
func TestCriticalCloseWithUnsettledDeliveriesDoesNotHang(t *testing.T) {
	l := newLive(t)
	c := l.open(t)
	topic := newTopic("closehang")
	provision(t, c, topic, "s")
	ctx := context.Background()
	const n = 8
	for i := range n {
		if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("unsettled")}); err != nil {
			t.Fatalf("Publish(%d): %v", i, err)
		}
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s", Prefetch: 16})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	// Deliberately unsettled: the whole question is what Close does with the
	// deliveries a handler is still holding.
	d := receiveOne(t, cons, 10*time.Second)
	_ = d
	done := make(chan error, 1)
	go func() { done <- cons.Close() }()
	select {
	case cerr := <-done:
		if cerr != nil {
			t.Fatalf("Close: %v", cerr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return within ten seconds while a delivery was unsettled: " +
			"the client library's shutdown waits for every message it handed to the callback to be settled, " +
			"so a source that waits for Receive before settling what it handed out deadlocks ")
	}
}

// TestCriticalRejectRefusesADelay is the rule about an option a driver
// cannot honour, on the one combination where both halves of the request are
// individually supported.
//
// A delay asks for a redelivery held back; a dead-letter settlement produces
// no redelivery at all — the message goes to another topic and the original is
// acknowledged. So `Nack(NackDeadLetter(), NackDelay(d))` is a request with no
// meaning, and the two available answers were ignoring the delay and refusing
// the settlement. Ignoring it is what the specification forbids, and it is the quiet kind of
// wrong: the caller's message really does reach the dead-letter topic, just
// immediately.
func TestCriticalRejectRefusesADelay(t *testing.T) {
	dlq := newTopic("dlqdelay")
	c, _ := scripted(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = 5
		cfg.AckDeadline = time.Minute
	})
	topic := newTopic("rejectdelay")
	provision(t, c, topic, "s")
	provision(t, c, dlq, "d")
	ctx := context.Background()
	if err := c.Publish(ctx, topic, &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cons, err := c.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "s"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	d := receiveOne(t, cons, 5*time.Second)
	err = d.Nack(mq.NackDeadLetter(), mq.NackDelay(time.Second))
	if err == nil {
		t.Fatal("a dead-letter settlement with a delay was accepted; the specification refuses an option a driver cannot honour rather than ignoring it")
	}
	if code := mq.CodeOf(err); code != mq.CodeInvalidArgument {
		t.Fatalf("the refusal reported code %v: %v", code, err)
	}
	if d.Settled() {
		t.Fatal("a refused settlement settled the delivery")
	}
	// And the delivery is still settleable, which is what makes the refusal a
	// refusal rather than a loss.
	if err := d.Reject(); err != nil {
		t.Fatalf("Reject after the refusal: %v", err)
	}
}

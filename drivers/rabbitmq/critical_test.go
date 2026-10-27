package rabbitmq_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
)

//
// Each of them is a way for this driver to lose a message it said it kept,
// acknowledge a delivery it does not own, or report a definite outcome for one
// it cannot know — and each is unreachable from the portable API without
// control over the broker, which is why they are here against the scripted
// fake rather than in the conformance suite. A conformance run against a
// healthy container cannot close a channel mid-confirm, cannot make the broker
// cancel a consumer, and cannot hand a driver a confirm frame for a channel
// that has gone.

// TestCriticalChannelCloseDuringConfirm is the ambiguity and the specification's
// settlement disqualifier, on the publish side.
//
// A channel that closes with confirms outstanding leaves those publications
// unresolved: the broker may have taken every one of them, and it may have
// taken none. Reporting that as a definite FAILURE invites a retry that
// duplicates the message; reporting it as a SUCCESS loses it. The only honest
// answer is Ambiguous.
//
// It is the sharpest case on this broker because the client library's own
// answer is the wrong one: `deferredConfirmations.Close` resolves every
// pending confirmation with `ack=false`, which is a definite negative. This
// driver therefore does not use that path at all — it reads its confirms from
// `NotifyPublish` and detaches its own tracker — and this test is what says
// so.
func TestCriticalChannelCloseDuringConfirm(t *testing.T) {
	broker, client := openFakeWith(t,
		// The broker confirms nothing on its own, so the publications stay
		// outstanding until the test closes the channel under them.
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		func(c *rq.Config) {
			c.Declare = true
			c.ConfirmKind, c.ConfirmWindow = confirm.Async, 8
		})
	_ = consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	const n = 4
	outcomes := make(chan error, n)
	for i := range n {
		if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte{byte('a' + i)}},
			func(r mq.PublishResult) { outcomes <- r.Err }); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	broker.Sync()
	// Every publication is outstanding, which is what makes the close
	// ambiguous rather than merely unlucky.
	if got := rq.OutstandingForTest(backendOf(t, client)); sum(got) != n {
		t.Fatalf("%v publications are outstanding, want %d in one sink", got, n)
	}

	// The channel the publisher is using goes, with the broker's own reason.
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.ResourceError, Reason: "the broker ran out of memory", Server: true,
	})

	for i := range n {
		select {
		case err := <-outcomes:
			if err == nil {
				t.Fatalf("publication %d was reported SUCCESSFUL although its confirm never arrived: the broker may have taken it and may not, and reporting success loses the message when it did not", i)
			}
			var oe *mq.OpError
			if !errors.As(err, &oe) {
				t.Fatalf("publication %d: %T is not an *OpError: %v", i, err, err)
			}
			if !oe.Ambiguous {
				t.Fatalf("publication %d was reported as a DEFINITE failure (%v, ambiguous=%v): the broker may have taken it, so a retry can duplicate it -- the message contract forbids this and the specification names it a settlement disqualifier",
					i, oe.Code, oe.Ambiguous)
			}
			if oe.Code != mq.CodeAmbiguous {
				t.Errorf("publication %d carries %v, want %v", i, oe.Code, mq.CodeAmbiguous)
			}
			if oe.Retryable {
				t.Errorf("publication %d is marked retryable: an ambiguous publication must not be retried blindly", i)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("publication %d never completed after the channel closed: a close has to resolve everything it was holding", i)
		}
	}
}

// TestCriticalMandatoryReturnIsAttributedToTheRightPublication is the specification's
// "Mandatory + NotifyReturn = Rejected", in the case that makes it hard.
//
// `basic.return` carries the returned message and NOT its delivery tag, so
// with more than one publication outstanding there is nothing in the frame
// that says which one came back. This driver puts a correlation token on every
// `mandatory` publication, and
// this is what the token is for: one of three publications is unroutable, and
// the other two must be reported CONFIRMED while that one is reported
// Rejected.
//
// Getting it wrong is worse than not attributing the return at all: it reports
// one message Rejected and another Confirmed when the truth is the other way
// round, so a caller retries a message that arrived and discards one that did
// not.
func TestCriticalMandatoryReturnIsAttributedToTheRightPublication(t *testing.T) {
	broker, client := openFakeWith(t,
		// The broker confirms nothing on its own, so all three publications
		// are outstanding on ONE tracker when the return arrives -- which is
		// the whole difficulty. Two publishers would give each sink one
		// outstanding publication, and the token would not be needed.
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		func(c *rq.Config) {
			c.Declare = true
			// `mandatory` is opt-in (see TestMandatoryIsOffByDefault), and
			// this whole test is about the frame it is the only way to make
			// the broker send at all.
			c.Mandatory = true
			c.ConfirmKind, c.ConfirmWindow = confirm.Async, 8
		})
	_ = consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	type result struct {
		which string
		err   error
	}
	outcomes := make(chan result, 3)
	publish := func(which string) {
		t.Helper()
		if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte(which)},
			func(r mq.PublishResult) { outcomes <- result{which, r.Err} }); err != nil {
			t.Fatalf("publishing %s: %v", which, err)
		}
	}

	publish("good-1")
	// The binding goes away for exactly one publication, which is what makes
	// the second one unroutable on the same channel as the other two.
	if !broker.Unbind("orders", "orders.workers", "orders") {
		t.Fatal("the binding was not there to remove, so this test proves nothing")
	}
	publish("unroutable")
	if !broker.Bind("orders", "orders.workers", "orders") {
		t.Fatal("the binding could not be restored")
	}
	publish("good-2")
	broker.Sync()

	// All three are outstanding, and the return for the middle one has
	// already arrived: RabbitMQ sends `basic.return` before the ack.
	ch := publisherChannel(t, broker)
	if got := ch.PendingConfirms(); len(got) != 3 {
		t.Fatalf("%d publications are outstanding, want 3: the return has to arrive while several are in flight or the token is not needed", len(got))
	}
	// Now the broker confirms all three, cumulatively, which is what makes
	// the library expand them into three confirmations in tag order.
	ch.AckPublicationsThrough(3)
	broker.Sync()

	got := map[string]error{}
	for range 3 {
		select {
		case r := <-outcomes:
			got[r.which] = r.err
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 3 publications completed: %v", len(got), got)
		}
	}
	for _, which := range []string{"good-1", "good-2"} {
		if err := got[which]; err != nil {
			t.Errorf("the routable publication %q was reported %v: the return belonged to another publication, so one message is reported Rejected and another Confirmed with the truth the other way round", which, err)
		}
	}
	err := got["unroutable"]
	if err == nil {
		t.Fatal("the unroutable publication was reported SUCCESSFUL: the broker returned it and then acked it, and reporting that ack as a success tells the caller a message is in a queue that never received it")
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Errorf("the unroutable publication carries %v, want %v: %v", code, mq.CodeNotFound, err)
	}
	if dropped := broker.Dropped(); len(dropped) != 1 {
		t.Errorf("the broker recorded %d drops, want 1", len(dropped))
	}
}

// TestCriticalMandatoryReturnUnderPerMessageWithConcurrentPublishers is the
// same attribution under the default per-message strategy, which keeps one
// publication outstanding per CALLER and not per channel: `Publish` is safe
// for concurrent use, so two goroutines on one publisher leave two
// publications outstanding on its channel, and a `basic.return` without a
// token has two candidates. It used to be dropped, and the unroutable
// publication was then reported successful when its ack arrived.
func TestCriticalMandatoryReturnUnderPerMessageWithConcurrentPublishers(t *testing.T) {
	broker, client := openFakeWith(t,
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		mandating)
	_ = consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")

	type result struct {
		which string
		err   error
	}
	outcomes := make(chan result, 2)
	publish := func(which string) {
		go func() { outcomes <- result{which, pub.Publish(t.Context(), &mq.Message{Body: []byte(which)})} }()
	}

	ch := publisherChannel(t, broker)
	publish("good")
	waitFor(t, 5*time.Second, func() bool { return len(ch.PendingConfirms()) == 1 })
	if !broker.Unbind("orders", "orders.workers", "orders") {
		t.Fatal("the binding was not there to remove, so this test proves nothing")
	}
	publish("unroutable")
	waitFor(t, 5*time.Second, func() bool { return len(ch.PendingConfirms()) == 2 })
	broker.Sync()
	ch.AckPublicationsThrough(2)
	broker.Sync()

	got := map[string]error{}
	for range 2 {
		select {
		case r := <-outcomes:
			got[r.which] = r.err
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of 2 publications completed: %v", len(got), got)
		}
	}
	if err := got["good"]; err != nil {
		t.Errorf("the routable publication was reported %v", err)
	}
	err := got["unroutable"]
	if err == nil {
		t.Fatal("the unroutable publication was reported SUCCESSFUL: its return arrived while another publication was outstanding and was dropped")
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Errorf("the unroutable publication carries %v, want %v: %v", code, mq.CodeNotFound, err)
	}
}

// TestCriticalDelayedNackDoesNotLoseTheMessageWhenTheParkFails is the order
// that decides what a crash costs, and it is the half a happy-path test
// cannot see.
//
// A delayed redelivery re-publishes the message into a delay queue and then
// acknowledges the original. The order is publish, wait for the confirm, THEN
// ack — because a crash between a publish and an ack leaves both copies (a
// duplicate, which the at-least-once allows) and a crash between an ack and
// a publish loses the message entirely (which no guarantee allows).
//
// Here the park is REFUSED by the broker. The original must still be in its
// queue afterwards, and the settlement must say it did not happen.
func TestCriticalDelayedNackDoesNotLoseTheMessageWhenTheParkFails(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("retry-me")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)

	// The next publication on the CONSUMER's channel -- which is the park --
	// is nacked by the broker.
	consumerChannel(t, broker).NackNextPublishes(1)

	err := d.Nack(mq.NackDelay(3 * time.Second))
	if err == nil {
		t.Fatal("a delayed nack whose park the broker refused reported success: the original would have been acknowledged and the message would be gone")
	}
	broker.Sync()

	// The original is still unacknowledged, so the broker will redeliver it.
	// That is the duplicate-over-loss choice, and it is the whole reason the
	// park comes first.
	for _, s := range broker.Settlements() {
		if s.Kind == rabbitfake.SettleAck {
			t.Fatalf("the original was acknowledged although its parked copy was refused: %+v", s)
		}
	}
	// The parked copy may or may not be in the delay queue: the fake routes
	// a publication and then nacks its confirm, which is one of the two
	// things a real broker's `basic.nack` can mean. What matters is that the
	// driver did not believe it, and the assertion above is that.
	// And the message comes back, which is what "not lost" means.
	if err := d.Nack(); err != nil {
		t.Fatalf("the delivery was not settleable after the refused delayed nack: %v", err)
	}
	again := receiveOne(t, cons)
	if string(again.Body) != "retry-me" {
		t.Fatalf("the redelivered body is %q", again.Body)
	}
	if err := again.Ack(); err != nil {
		t.Fatal(err)
	}
}

// The BROKER cancels a consumer when the queue it is reading is deleted. The
// caller closed nothing, so reporting ErrClosed would tell them they had; the
// queue is gone, so the honest answer is CodeNotFound. And every delivery the
// consumer was holding is unsettleable from that moment, so a settlement for
// one must report ErrLeaseLost rather than being sent as a frame the broker
// will refuse with a channel exception.
func TestCriticalConsumerCancel(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	held := receiveOne(t, cons)

	// The broker cancels the consumer, which is what a `queue.delete` under
	// a live consumer produces.
	cancelled := false
	for _, ch := range broker.Channels() {
		for _, tag := range ch.Consumers() {
			if ch.CancelConsumer(tag) {
				cancelled = true
			}
		}
	}
	if !cancelled {
		t.Fatal("no consumer was cancelled, so this test proves nothing")
	}
	broker.Sync()

	// The next receive is what the driver's own notice of the cancel becomes
	// visible through, so it comes first: a settlement asserted before the
	// driver had seen the cancel would be racing the notification rather
	// than testing the guard.
	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	_, err := cons.Receive(ctx, dst)
	if err == nil {
		t.Fatal("a receive on a cancelled consumer returned deliveries")
	}
	if errors.Is(err, mq.ErrClosed) {
		t.Fatalf("a receive on a BROKER-cancelled consumer reported ErrClosed: the caller closed nothing, and telling them they did hides a deleted queue: %v", err)
	}
	if code := mq.CodeOf(err); code != mq.CodeNotFound {
		t.Fatalf("the receive carries %v, want %v: %v", code, mq.CodeNotFound, err)
	}

	// And the settlement the caller already holds is refused rather than
	// sent. A `basic.ack` for a tag the broker has forgotten is a channel
	// exception, which would take every publication on that channel with it.
	before := settlementCount(broker)
	if err := held.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("acknowledging a delivery whose consumer the broker cancelled = %v, want ErrLeaseLost", err)
	}
	if after := settlementCount(broker); after != before {
		t.Errorf("the refused settlement sent %d frame(s) to the broker", after-before)
	}
}

// TestCriticalStaleTagNeverSettlesOnANewChannel is the settlement
// disqualifier in AMQP's clothing, and it is the one the SPI cares about most.
//
// Delivery tags are per CHANNEL and restart at 1 on a new one. So a
// settlement carrying a tag from a channel that has gone names, on the
// replacement channel, whatever delivery holds that tag there — and sending it
// would acknowledge a message the caller never saw and never processed. It is
// the exact shape of the defect the generation exists to prevent, and
// `FuzzRabbitTagSettlement` is the fuzz target for it.
//
// The test proves three things, and the third is the one a weaker test would
// miss: the stale settlement is refused, a FRESH delivery on the new channel
// still settles, and NO FRAME for the stale tag reached the broker. A driver
// that sent the ack and mapped the broker's refusal would pass the first two
// and would have acknowledged another delivery on a broker that did not
// refuse.
func TestCriticalStaleTagNeverSettlesOnANewChannel(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	for _, body := range []string{"first", "second"} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	stale := receiveOne(t, cons)
	if string(stale.Body) != "first" {
		t.Fatalf("the first delivery is %q", stale.Body)
	}

	// The consumer's channel goes. The broker requeues what it was holding,
	// the driver re-attaches, and the requeued message is delivered again on
	// the NEW channel -- under a delivery tag that starts at 1, which is the
	// tag the stale settlement names.
	consumerChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.ResourceError, Reason: "the node is under pressure", Server: true,
	})
	broker.Sync()
	waitFor(t, 5*time.Second, func() bool { return len(broker.Channels()) > 0 && consumerTags(broker) > 0 })

	before := settlementCount(broker)
	if err := stale.Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("acknowledging a delivery from the OLD channel = %v, want ErrLeaseLost: its tag names a different delivery on the new channel", err)
	}
	if after := settlementCount(broker); after != before {
		t.Fatalf("the stale settlement sent %d frame(s) to the broker: the guard has to refuse it BEFORE the frame, because a broker that did not refuse it would acknowledge a delivery the caller never saw",
			after-before)
	}

	// A fresh delivery on the new channel still settles, so the guard is not
	// simply refusing everything.
	fresh := receiveOne(t, cons)
	if err := fresh.Ack(); err != nil {
		t.Fatalf("a delivery from the NEW channel was refused: %v", err)
	}
	broker.Sync()
	if got := settlementCount(broker); got <= before {
		t.Fatalf("the fresh settlement reached the broker %d times, want more than %d", got, before)
	}
}

// TestCriticalAReattachDoesNotOrphanALiveChannel is a
// source that is already attached is left alone by a second attach, rather
// than given a second channel while the first keeps its consumer.
//
// After a reconnection the dial loop installs the new connection, the
// caller's first Fetch attaches the source on it, and the dial loop's own
// re-attach reaches the source afterwards. The second attach used to install
// another channel over the live one without closing it, so the broker kept a
// consumer nobody reads: it filled that consumer's prefetch with messages that
// were never delivered and never redelivered, and the generation bump made the
// delivery the caller was holding from the live channel unsettleable.
// `FuzzRabbitTagSettlement` saw it as a "stale" settlement that succeeded,
// because the channel its fault closed was the orphan and not the delivery's.
func TestCriticalAReattachDoesNotOrphanALiveChannel(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 2 })
	pub := publishOn(t, client, "orders")
	const n = 6
	for i := range n {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("m" + string(rune('0'+i)))}); err != nil {
			t.Fatal(err)
		}
	}
	held := receiveOne(t, cons)
	broker.Sync()

	// The dial loop's re-attach, reaching a source the caller's Fetch has
	// already attached on the current connection.
	if err := rq.ReattachForTest(backendOf(t, client)); err != nil {
		t.Fatalf("the re-attach step: %v", err)
	}
	broker.Sync()

	if got := consumerTags(broker); got != 1 {
		t.Fatalf("the broker holds %d consumers for one subscription after a re-attach of an attached source: "+
			"the extra one is on a channel nothing reads, and its prefetch is messages nobody will receive until the "+
			"connection dies", got)
	}
	if err := held.Ack(); err != nil {
		t.Fatalf("acknowledging a delivery whose channel nothing replaced = %v: a re-attach that changed nothing "+
			"must not make what the caller holds stale", err)
	}
	// And every message reaches the caller. With an orphan, the ones in its
	// prefetch never would.
	seen := map[string]bool{string(held.Body): true}
	for len(seen) < n {
		d := receiveOne(t, cons)
		seen[string(d.Body)] = true
		if err := d.Ack(); err != nil {
			t.Fatalf("acknowledging %s: %v", d.Body, err)
		}
	}
}

// TestCriticalAFullTokenTableWaitsRatherThanStrands is a
// delivery that finds every token slot in use waits for one, instead of being
// left unsettled on a live channel.
//
// The table is prefetch-sized and the broker never has more than the prefetch
// unacknowledged, but a settlement writes its frame BEFORE it frees its slot,
// and the broker answers the frame by sending the next delivery at once. The
// pump used to meet that delivery with the table still full and leave it
// unsettled -- and on a live channel RabbitMQ does not give it back, so it sat
// in the consumer's prefetch, never handed to anybody, until the channel died.
// The window is microseconds wide, so the test holds it open with a slot of
// its own rather than asserting that a race went one way.
func TestCriticalAFullTokenTableWaitsRatherThanStrands(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 2 })
	pub := publishOn(t, client, "orders")
	publish := func(body string) {
		t.Helper()
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	// The first Fetch is what starts the consumer.
	publish("m0")
	zeroth := receiveOne(t, cons)
	if err := zeroth.Ack(); err != nil {
		t.Fatal(err)
	}
	b := backendOf(t, client)

	// One slot belongs to a settlement that has written its frame and not yet
	// freed it; the broker, holding nothing unacknowledged, sends two.
	release, err := rq.OccupySlotForTest(b)
	if err != nil {
		t.Fatal(err)
	}
	publish("m1")
	publish("m2")
	broker.Sync()
	first := receiveOne(t, cons)
	// The pump meets the second delivery with the table full. The wait is for
	// it to have done so, so that what follows is about its answer and not
	// about whether it got there before this goroutine did.
	waitFor(t, 5*time.Second, func() bool { return rq.SlotWaitsForTest(b) > 0 || rq.StaleForTest(b) > 0 })
	if n := rq.StaleForTest(b); n != 0 {
		t.Fatalf("the source left %d delivery(ies) unsettled because its token table was full: on a live channel the "+
			"broker does not give one back, so it is never delivered while the channel lives", n)
	}

	release()
	second := receiveOne(t, cons)
	if got := string(first.Body) + "," + string(second.Body); got != "m1,m2" {
		t.Fatalf("the deliveries were %s, want m1,m2", got)
	}
	for _, d := range []mq.Delivery{first, second} {
		if err := d.Ack(); err != nil {
			t.Fatalf("acknowledging %s: %v", d.Body, err)
		}
	}
}

// TestCriticalEveryChannelExceptionIsRepaired is a consumer
// whose channel the broker closes gets a new one, every time.
//
// The pump reads the channel's delivery, confirm and cancel channels and its
// close notification in one `select`, and on a shutdown all four become ready
// together. It used to return when it had seen the first three close, before
// reading the close notification that triggers the repair, so an unlucky
// `select` left the source holding the dead channel for ever -- no consumer on
// the broker, the generation unchanged, a Fetch that waits for nothing. Which
// case `select` takes is the runtime's choice and cannot be arranged, so the
// test takes many exceptions in a row: the defect lost about one in a hundred
// and fifty, so a thousand fail it essentially always, and a correct driver
// passes every one.
func TestCriticalEveryChannelExceptionIsRepaired(t *testing.T) {
	broker, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	b := backendOf(t, client)
	const rounds = 1000
	for i := range rounds {
		before := rq.GenerationsForTest(b)
		consumerChannel(t, broker).CloseWith(&amqp.Error{
			Code: amqp.ResourceError, Reason: "the node is under pressure", Server: true,
		})
		deadline := time.Now().Add(5 * time.Second)
		for !repaired(b, broker, before) {
			if time.Now().After(deadline) {
				t.Fatalf("channel exception %d of %d was never answered: generation %v -> %v, %d source(s) attached, "+
					"%d consumer(s) on the broker -- the source holds a dead channel and nothing will ever repair it",
					i+1, rounds, before, rq.GenerationsForTest(b), rq.AttachedForTest(b), consumerTags(broker))
			}
			time.Sleep(50 * time.Microsecond)
		}
	}
	// And the consumer still delivers after all of them.
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); string(d.Body) != "after" {
		t.Fatalf("the delivery after the exceptions is %q", d.Body)
	}
}

// repaired reports whether the one source has moved past the generation it
// had and is attached, with one consumer on the broker again.
func repaired(b *rq.Backend, broker *rabbitfake.Broker, before []uint32) bool {
	after := rq.GenerationsForTest(b)
	return len(after) == 1 && after[0] > before[0] && rq.AttachedForTest(b) == 1 && consumerTags(broker) == 1
}

// waitRepaired fails the test unless the one source is repaired within five
// seconds of a fault that left it at generation `before`.
func waitRepaired(t *testing.T, b *rq.Backend, broker *rabbitfake.Broker, before []uint32, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !repaired(b, broker, before) {
		if time.Now().After(deadline) {
			t.Fatalf("%s was never answered: generation %v -> %v, %d source(s) attached, %d consumer(s) on the "+
				"broker -- the source holds a dead channel and nothing will ever repair it",
				what, before, rq.GenerationsForTest(b), rq.AttachedForTest(b), consumerTags(broker))
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// TestCriticalAPoisonDeliveryDuringAChannelExceptionIsRepaired is the second exit: a delivery the consumer cannot map, met while its
// channel is taking an exception.
//
// With a dead-letter route the pump rejects such a delivery itself
// (`rejectUnmappable`), and on a channel that has just closed that
// `basic.nack` fails. The failure used to STOP the pump -- `admit`'s error was
// read as "the source is closing" -- before it had read the close
// notification that repairs the source, so the source kept the dead channel,
// the broker had no consumer and a Fetch waited for ever. Which of the pump's
// cases runs first is the runtime's choice, so the test takes the exception
// many times with poison already queued: the defect lost the race within the
// first few rounds, and a correct pump survives every one.
func TestCriticalAPoisonDeliveryDuringAChannelExceptionIsRepaired(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) { c.Declare, c.DeadLetter = true, true })
	// An enveloping consumer, so that a plain body is a delivery it cannot
	// map; and an enveloping publisher, so that it has something it can.
	cons := consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) {
		c.Metadata = mq.MetadataPortableEnvelope
	})
	pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	b := backendOf(t, client)
	const rounds, poison = 200, 20
	for i := range rounds {
		for range poison {
			if _, err := broker.Publish("", "orders.workers", amqp.Publishing{Body: []byte("not an envelope")}); err != nil {
				t.Fatal(err)
			}
		}
		before := rq.GenerationsForTest(b)
		consumerChannel(t, broker).CloseWith(&amqp.Error{
			Code: amqp.ResourceError, Reason: "the node is under pressure", Server: true,
		})
		waitRepaired(t, b, broker, before, "a channel exception with poison in flight (round "+strconv.Itoa(i+1)+")")
	}
	// And the consumer still delivers what it can map.
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); string(d.Body) != "after" {
		t.Fatalf("the delivery after the exceptions is %q", d.Body)
	}
}

// slotWaitFixture is a consumer with a prefetch of two whose pump is waiting
// for a token slot: one slot belongs to a settlement that has not freed it
// (OccupySlotForTest), one to the delivery the test holds, and the broker has
// sent a third. It is the state the wait exists for, held open
// so that a test can do something to the source in the middle of it.
type slotWaitFixture struct {
	broker  *rabbitfake.Broker
	client  *mq.Client
	cons    *mq.ConsumerHandle
	pub     *mq.PublisherHandle
	b       *rq.Backend
	release func()
}

func newSlotWait(t *testing.T) *slotWaitFixture {
	t.Helper()
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase = time.Millisecond
	})
	f := &slotWaitFixture{
		broker: broker,
		client: client,
		cons:   consumeOnWith(t, client, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 2 }),
		pub:    publishOn(t, client, "orders"),
	}
	f.publish(t, "m0")
	if d := receiveOne(t, f.cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	f.b = backendOf(t, client)
	release, err := rq.OccupySlotForTest(f.b)
	if err != nil {
		t.Fatal(err)
	}
	f.release = release
	f.publish(t, "m1")
	f.publish(t, "m2")
	broker.Sync()
	_ = receiveOne(t, f.cons) // m1, held and never settled
	waitFor(t, 5*time.Second, func() bool { return rq.SlotWaitsForTest(f.b) > 0 })
	return f
}

func (f *slotWaitFixture) publish(t *testing.T, body string) {
	t.Helper()
	if err := f.pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
		t.Fatal(err)
	}
}

// TestCriticalAChannelExceptionDuringASlotWaitIsAnswered pins the slot wait's
// exit on channel death.
//
// Only the pump reads its channel's close notification, and while it waits for
// a slot it is not reading. The wait used to end only when a slot freed, the
// source closed or the channel was REPLACED -- and replacing it is what the
// close notification the pump was not reading would have caused. So an
// exception during the wait was not answered until a caller happened to settle
// something, and a caller holding a full prefetch while it waited for one more
// delivery waited for ever. Nothing frees the occupied slot in this test, so a
// wait that did not notice the channel dying would never end.
func TestCriticalAChannelExceptionDuringASlotWaitIsAnswered(t *testing.T) {
	f := newSlotWait(t)
	before := rq.GenerationsForTest(f.b)
	consumerChannel(t, f.broker).CloseWith(&amqp.Error{
		Code: amqp.ResourceError, Reason: "the node is under pressure", Server: true,
	})
	waitRepaired(t, f.b, f.broker, before, "a channel exception during a slot wait")
	// The broker took m1 and m2 back with the channel, and the new channel
	// delivers them.
	seen := map[string]bool{}
	for len(seen) < 2 {
		d := receiveOne(t, f.cons)
		seen[string(d.Body)] = true
		if err := d.Ack(); err != nil {
			t.Fatalf("acknowledging %s on the new channel: %v", d.Body, err)
		}
	}
	if !seen["m1"] || !seen["m2"] {
		t.Fatalf("the new channel delivered %v, want m1 and m2", seen)
	}
	f.release()
}

// TestCriticalACloseDuringASlotWaitReturns pins the slot wait's exit on Close:
// the close returns, and the deliveries the source held are the broker's
// again.
//
// The exit is one of two things that end the wait on a close -- `Close` also
// invalidates every token, which frees a slot -- so this test fails only when
// both are gone. It is here so that the pair is pinned as a pair.
func TestCriticalACloseDuringASlotWaitReturns(t *testing.T) {
	f := newSlotWait(t)
	closed := make(chan error, 1)
	go func() { closed <- f.cons.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("closing the consumer during a slot wait: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing a consumer whose pump was waiting for a token slot did not return")
	}
	f.broker.Sync()
	q, ok := f.broker.Queue("orders.workers")
	if !ok || q.Depth != 2 || q.Unacked != 0 {
		t.Fatalf("after the close the queue holds %+v, want m1 and m2 back and ready", q)
	}
	f.release()
}

// TestCriticalAConnectionLossDuringASlotWaitRecovers pins the slot wait's exit
// on a replacement: the connection goes while the pump waits, the dial loop
// re-attaches the consumer, and both messages the old channel held are
// delivered on the new one.
func TestCriticalAConnectionLossDuringASlotWaitRecovers(t *testing.T) {
	f := newSlotWait(t)
	restores := rq.RestoresForTest(f.b)
	f.broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 5*time.Second, func() bool { return rq.RestoresForTest(f.b) > restores })
	seen := map[string]bool{}
	for len(seen) < 2 {
		d := receiveOne(t, f.cons)
		seen[string(d.Body)] = true
		if err := d.Ack(); err != nil {
			t.Fatalf("acknowledging %s after the reconnection: %v", d.Body, err)
		}
	}
	if !seen["m1"] || !seen["m2"] {
		t.Fatalf("after the reconnection the consumer delivered %v, want m1 and m2", seen)
	}
	f.release()
}

// The distinction from TestCriticalChannelCloseDuringConfirm is the level. A
// CHANNEL close is answered by the library, which resolves the deferred
// confirmations itself; a CONNECTION loss is answered by nothing unless the
// driver answers it, so a driver that only reconnected would leave every
// outstanding publication waiting for a confirm that can never arrive and
// every delivery settleable against a tag the broker has reused.
//
// It is here rather than in `driverkit` because the ORDER inside the
// invalidation is the driver's own — sinks before sources, for the reasons
// `Backend.detachChildren` gives — and because only a driver can be made to
// lose a connection.
func TestCriticalConnectionLossResolvesWhatItWasHolding(t *testing.T) {
	broker, client := openFakeWith(t,
		// Nothing is confirmed on its own, so the publications are still
		// outstanding when the connection goes.
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		func(c *rq.Config) {
			c.Declare = true
			c.ConfirmKind, c.ConfirmWindow = confirm.Async, 8
			c.ReconnectBase = time.Millisecond
		})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	b := backendOf(t, client)
	restores := rq.RestoresForTest(b)

	const n = 4
	outcomes := make(chan error, n)
	for i := range n {
		if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte{byte('a' + i)}},
			func(r mq.PublishResult) { outcomes <- r.Err }); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	broker.Sync()
	if got := rq.OutstandingForTest(b); sum(got) != n {
		t.Fatalf("%v publications are outstanding, want %d in one sink", got, n)
	}
	// One delivery in hand as well, so that the sources' half of the
	// invalidation has something to invalidate.
	held := receiveOne(t, cons)

	// The CONNECTION goes, which is what the supervision loop notices and
	// answers with `Reconnector.Lost`.
	conns := broker.Connections()
	if len(conns) != 1 {
		t.Fatalf("the client holds %d connections, want 1", len(conns))
	}
	conns[0].CloseWith(&amqp.Error{
		Code: amqp.ConnectionForced, Reason: "the broker is going away", Server: true,
	})

	for i := range n {
		select {
		case err := <-outcomes:
			var oe *mq.OpError
			switch {
			case err == nil:
				t.Fatalf("publication %d was reported SUCCESSFUL although its confirm never arrived and the connection died under it", i)
			case !errors.As(err, &oe):
				t.Fatalf("publication %d: %T is not an *OpError: %v", i, err, err)
			case !oe.Ambiguous:
				t.Fatalf("publication %d was reported as a DEFINITE failure (%v): the transfer had left and its confirm had not arrived, so the broker may hold it -- the specification forbids this and names it a settlement disqualifier",
					i, oe.Code)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("publication %d never completed after the connection died: a lost connection has to resolve everything the client was holding, and nothing else will ", i)
		}
	}

	// And the delivery from the dead connection cannot be settled: its tag
	// belongs to a channel that no longer exists, and the broker will reuse
	// the number.
	if err := held.Ack(); err == nil {
		t.Error("a delivery from the lost connection was acknowledged: the settlement would land on whatever took its tag ")
	} else if !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("settling a delivery from the lost connection reported %v, want ErrLeaseLost", err)
	}

	// The client comes back, which is what says the invalidation did not
	// break the reconnection it precedes.
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
}

// TestCriticalReconnectIsNotRestoredMidReplay is the invariant the test below
// used to get wrong, held deterministically.
//
// A replay declares one thing at a time, so "the broker holds a queue again" is
// true from the FIRST declaration and is not a statement about the topology. A
// test that waited on it and then asserted the whole topology was asserting
// that a replay is atomic, which it is not -- and it passed for months because
// on a quiet host the replay finishes between two one-millisecond polls. Under
// load it failed as
//
//	"orders.workers" was not restored; the broker holds queues [orders.workers.dlq]
//
// So this test stands in the window rather than hoping to miss it:
// [rabbitfake.Broker.HoldDeclarations] parks the replay after its first
// declaration, and at that point the broker holds SOME of the topology, the old
// predicate is satisfied, and nothing says the client is restored. There is no
// deadline in the decisive part and no sleep anywhere: the state is created, not
// waited for.
func TestCriticalReconnectIsNotRestoredMidReplay(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		c.ReconnectBase = time.Millisecond
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	// A delayed nack, so that a delay queue exists and has to be replayed --
	// which is what makes the replay long enough to have a middle.
	if err := d.Nack(mq.NackDelay(time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}
	broker.Sync()

	b := backendOf(t, client)
	declared := rq.TopologyForTest(b)
	queues := 0
	for _, d := range declared {
		if strings.HasPrefix(d, "q:") {
			queues++
		}
	}
	if queues < 3 {
		t.Fatalf("the client declared %v; this test needs at least three QUEUES to have a mid-replay state at all", declared)
	}
	restores := rq.RestoresForTest(b)

	gate := broker.HoldDeclarations()
	defer gate.Release()
	broker.Restart(&amqp.Error{
		Code: amqp.ConnectionForced, Reason: "broker restarting", Server: true,
	})

	// The replay is stepped one declaration at a time until the broker holds a
	// queue again, which is the first instant the OLD wait was satisfied. Both
	// waits are for a goroutine to reach the gate, which is an event and not a
	// duration; `Parked() == 1` with `Passed() == step` says every declaration
	// up to `step` has been applied, because the replay issues them one at a
	// time from one goroutine and the one that is parked is the next.
	//
	// The first declaration is an EXCHANGE (`x:orders`), so the step count
	// matters and is not one: the recorded flake saw the broker holding
	// `[orders.workers.dlq]`, which is this replay's third declaration.
	waitFor(t, 10*time.Second, func() bool { return gate.Parked() == 1 })
	var held []string
	for step := 1; step < len(declared); step++ {
		gate.Step()
		waitFor(t, 10*time.Second, func() bool { return gate.Passed() == step && gate.Parked() == 1 })
		if held = queueNames(broker); len(held) > 0 {
			break
		}
	}

	// This is the state the flake ran its assertions in. Everything below is
	// about THIS instant.
	if len(held) == 0 {
		t.Fatalf("the replay was stepped through %d of %v declarations and the broker holds no queue: the gate is not gating what this test thinks it is", gate.Passed(), declared)
	}
	if len(held) >= queues {
		t.Fatalf("the broker already holds %v of %d declared queues after %d declarations: the replay is not being stepped", held, queues, gate.Passed())
	}
	// The old wait was satisfied here -- `len(broker.Queues()) > 0` -- and the
	// topology is incomplete, which is the whole defect in one assertion.
	if got := rq.RestoresForTest(b); got != restores {
		t.Errorf("the dial loop reported %d restores mid-replay, want %d: Dial must mean the client can work again, and a client whose topology is half replayed cannot ", got, restores)
	}
	if got := client.Health().State; got == mq.HealthReady {
		t.Errorf("health is %v with the topology half replayed: a client reported ready that cannot publish is the failure mode exists to prevent", got)
	}

	// Released, the replay completes and the client really is restored.
	gate.Release()
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	if got, want := len(queueNames(broker)), queues; got < want {
		t.Errorf("the restored broker holds %d queues, want at least %d: %v", got, want, queueNames(broker))
	}
}

// TestCriticalReconnectRestoresTheTopology is the specification's "driver owns bounded
// reconnection and topology restoration", and the restoration is the half a
// driver most easily leaves out.
//
// A durable exchange and a quorum queue survive a broker restart, so a driver
// that did not replay would usually seem to work — and the delay queues do
// NOT survive, because `x-expires` deletes an unused one, so a client that did
// not replay would come back unable to delay a redelivery. The test therefore
// compares what was declared before with what exists after.
func TestCriticalReconnectRestoresTheTopology(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.DeadLetter = true, true
		c.ReconnectBase = time.Millisecond
	})
	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	// A delayed nack, so that a delay queue exists and has to be replayed.
	if err := d.Nack(mq.NackDelay(time.Second)); err != nil {
		t.Fatalf("a delayed nack: %v", err)
	}
	broker.Sync()

	b := backendOf(t, client)
	declared := rq.TopologyForTest(b)
	if len(declared) < 5 {
		t.Fatalf("the client declared only %v; the test needs an exchange, a queue, a binding, a dead-letter route and a delay queue", declared)
	}

	// The connection goes and the broker forgets everything, which is what a
	// broker whose data directory was lost looks like.
	//
	// One call rather than a close and a forget, because the two are a RACE
	// otherwise: the driver's dial loop starts the moment the close lands, so
	// a replay that reached the broker before the forget would have its own
	// declarations deleted and nothing would recreate them. `Restart` closes
	// every connection before it forgets anything, which is what the real
	// event does too.
	restores := rq.RestoresForTest(b)
	broker.Restart(&amqp.Error{
		Code: amqp.ConnectionForced, Reason: "broker restarting", Server: true,
	})
	// The wait is for the RECONNECTION to finish, and not for the first sign
	// of life. It used to be `len(broker.Queues()) > 0`, which is true from
	// the FIRST declaration the replay makes and says nothing about the other
	// five -- so on a loaded host, where the dial goroutine is descheduled
	// between two declarations, the assertions below ran against a
	// half-restored broker and reported
	//
	//	"orders.workers" was not restored; the broker holds queues [orders.workers.dlq]
	//
	// which is a defect in the test and not in the driver. It is reproducible
	// to the character by making the fake's declarations slow, and
	// TestCriticalReconnectIsNotRestoredMidReplay holds the invariant this
	// wait now reads.
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })

	for _, want := range []string{"orders", "orders.workers", "orders.workers.dlq"} {
		if _, ok := broker.Queue(want); !ok {
			if _, ok := broker.Exchange(want); !ok {
				t.Errorf("%q was not restored; the broker holds queues %v", want, queueNames(broker))
			}
		}
	}
	// A one-second delay is granted the 1.6-second bucket: the default
	// ladder doubles from a hundred milliseconds, so the first bucket at or
	// above a second is 1600 ms. The specification makes a delay a minimum, so rounding
	// up is what the ladder does.
	if _, ok := broker.Queue("orders.workers.delay.1600"); !ok {
		t.Errorf("the delay queue was not restored; the broker holds %v: a client that did not replay would come back unable to delay a redelivery", queueNames(broker))
	}
	// And the client works again. The publication is RETRIED, because that is
	// what the specification asks of a caller: a publication attempted while the dial
	// loop is still running is refused with CodeUnavailable and Retryable
	// set, and a test that demanded success on the first attempt would be
	// asserting that a reconnection is instantaneous rather than that it
	// happens.
	var last error
	for range 200 {
		last = pub.Publish(t.Context(), &mq.Message{Body: []byte("after")})
		if last == nil {
			break
		}
		var oe *mq.OpError
		if !errors.As(last, &oe) || !oe.Retryable {
			t.Fatalf("publishing after the reconnection failed with an error a caller cannot act on: %v", last)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if last != nil {
		t.Fatalf("publishing after the reconnection never succeeded: %v", last)
	}
}

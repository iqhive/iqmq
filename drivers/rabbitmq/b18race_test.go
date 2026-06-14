package rabbitmq_test

import (
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestRedialDoesNotAttachASinkTheCoreIsClosing is the specification's
// reproduction, and it is DETERMINISTIC where the failure that found it was
// not.
//
// The defect was a data race that `make ci` hit about once in twenty-five runs
// through FuzzRabbitTagSettlement's seed corpus: the core's
// `PublisherHandle.closeStep` calls `sink.Close`, which waits on the sink's
// goroutine `sync.WaitGroup`, while the supervise goroutine's `redial` is
// inside `sink.attach` for the same sink, about to `Add` to it. `sync` models
// exactly that pair as a write and a read of the WaitGroup's semaphore
// precisely because it is the one documented misuse ("calls with a positive
// delta that occur when the counter is zero must happen before a Wait"), and
// it may also answer it with "WaitGroup misuse: Add called concurrently with
// Wait" or "WaitGroup is reused before previous Wait has returned".
//
// The race detector is not what this test reads, because a probabilistic
// reproducer is not a check: the SAME window leaves an observable behind. An
// attach that lands on a closed sink installs a channel and starts a loop
// goroutine on a handle that `Backend.forget` has already dropped, so nothing
// -- not the sink's own Close, which has returned, and not the backend's
// teardown, which iterates `b.sinks` -- ever closes that channel again. The
// assertion is therefore that the broker has no channel left open, which fails
// without the interlock and passes with it.
//
// The window is pinned rather than raced. `rabbitfake.Config.OnChannel` runs
// synchronously inside the driver's `Channel()` call, so holding it there
// parks `redial` INSIDE `sink.attach`, past the early-out check at its top,
// for as long as the test likes. The first channel opened after the fault is
// `redial`'s own throwaway one for the topology replay and is let through; the
// test asserts it has been closed again before it accepts that the next one
// belongs to an attach, so the test cannot pass by pinning the wrong channel.
func TestRedialDoesNotAttachASinkTheCoreIsClosing(t *testing.T) {
	var (
		mu    sync.Mutex
		armed bool
		opens []*rabbitfake.ChannelHandle
		held  int
	)
	gate := make(chan struct{})

	bcfg := rabbitfake.Config{OnChannel: func(h *rabbitfake.ChannelHandle) {
		mu.Lock()
		if !armed {
			mu.Unlock()
			return
		}
		opens = append(opens, h)
		n := len(opens)
		if n > 1 {
			held++
		}
		mu.Unlock()
		if n == 1 {
			// `redial`'s throwaway channel for the topology replay.
			return
		}
		<-gate
	}}

	broker, client := openFakeWith(t, bcfg, func(c *rq.Config) {
		c.Declare = true
		// A microsecond so that a second dial attempt, if the driver ever
		// needs one, does not make this test a hundred milliseconds slower.
		c.ReconnectBase = time.Microsecond
	})
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("m1")}); err != nil {
		t.Fatalf("publishing before the fault: %v", err)
	}

	mu.Lock()
	armed = true
	mu.Unlock()

	cn := broker.LastConnection()
	if cn == nil {
		t.Fatal("the broker has no connection to close")
	}
	cn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})

	// Wait until the re-attach is parked inside `sink.attach`.
	waitUntil(t, "the re-attach to open a channel", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return held > 0
	})
	mu.Lock()
	replay := opens[0]
	mu.Unlock()
	if !replay.IsClosed() {
		t.Fatal("the channel this test pinned is the topology replay's and not an attach: the replay channel is still open, so `redial` has not reached its re-attach step and the window below is not the one the specification closes")
	}

	// The close runs to completion while the re-attach is pinned, which is
	// the whole of the window: without the interlock the attach below then
	// installs a channel on a sink whose Close has already returned.
	if err := pub.Close(); err != nil {
		t.Fatalf("closing the publisher while its sink is being re-attached: %v", err)
	}
	close(gate)

	// The dial has to finish, and it has to finish SUCCESSFULLY: a handle
	// that closed mid-dial is not a reason to throw away a good connection.
	waitUntil(t, "the reconnection to report ready", func() bool {
		return client.Health().State == mq.HealthReady
	})

	// Nothing the driver opened may be left open. Every channel here was
	// opened by the driver, the client has no handles left, and both the
	// replay channel and the sink's own are closed by whoever opened them --
	// unless an attach installed one on a sink nothing owns any more.
	if n := settledOpenChannels(broker); n != 0 {
		t.Fatalf("%d channel(s) are still open after the publisher closed and the reconnection finished: `redial` re-attached a sink whose Close had already returned, so the channel it installed and the loop goroutine it started belong to a handle `Backend.forget` has dropped and nothing will ever close them ", n)
	}
}

// TestRedialDoesNotAttachASourceTheCoreIsClosing is
// [TestRedialDoesNotAttachASinkTheCoreIsClosing] for a consumer, whose
// `source.attachLocked` had the same shape: an early-out check at the top, and
// `s.wg.Add(1)` for the pump goroutine well after it.
//
// It is a separate test rather than a table row because the two handles reach
// the window differently: a source is attached LAZILY, so it has to have
// fetched once before a reconnection has anything to re-attach.
func TestRedialDoesNotAttachASourceTheCoreIsClosing(t *testing.T) {
	var (
		mu    sync.Mutex
		armed bool
		opens []*rabbitfake.ChannelHandle
		held  int
	)
	gate := make(chan struct{})

	bcfg := rabbitfake.Config{OnChannel: func(h *rabbitfake.ChannelHandle) {
		mu.Lock()
		if !armed {
			mu.Unlock()
			return
		}
		opens = append(opens, h)
		n := len(opens)
		if n > 1 {
			held++
		}
		mu.Unlock()
		if n == 1 {
			return
		}
		<-gate
	}}

	broker, client := openFakeWith(t, bcfg, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase = time.Microsecond
	})
	cons := consumeOn(t, client, "orders", "workers")
	// The first Fetch is what starts the consumer, and therefore what gives
	// the dial loop something to re-attach at all.
	ctx, cancel := contextWithTimeout(t, 200*time.Millisecond)
	defer cancel()
	if _, err := cons.Receive(ctx, make([]mq.Delivery, 1)); err == nil {
		t.Fatal("the empty queue produced a delivery")
	}

	mu.Lock()
	armed = true
	mu.Unlock()

	cn := broker.LastConnection()
	if cn == nil {
		t.Fatal("the broker has no connection to close")
	}
	cn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})

	waitUntil(t, "the re-attach to open a channel", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return held > 0
	})
	mu.Lock()
	replay := opens[0]
	mu.Unlock()
	if !replay.IsClosed() {
		t.Fatal("the channel this test pinned is the topology replay's and not an attach's ")
	}

	if err := cons.Close(); err != nil {
		t.Fatalf("closing the consumer while its source is being re-attached: %v", err)
	}
	close(gate)

	waitUntil(t, "the reconnection to report ready", func() bool {
		return client.Health().State == mq.HealthReady
	})
	if n := settledOpenChannels(broker); n != 0 {
		t.Fatalf("%d channel(s) are still open after the consumer closed and the reconnection finished: `redial` re-attached a source whose Close had already returned ", n)
	}
}

// openChannels is how many channels the broker still holds open.
func openChannels(b *rabbitfake.Broker) int {
	n := 0
	for _, ch := range b.Channels() {
		if !ch.IsClosed() {
			n++
		}
	}
	return n
}

// settledOpenChannels is [openChannels] once it has stopped falling.
//
// The count is settled by the time the reconnection reports ready, so the
// short wait buys nothing except immunity to a close that is one scheduling
// hop behind — and it reports the COUNT rather than timing out, so the
// assertion that reads it is the one that speaks when it fails.
func settledOpenChannels(b *rabbitfake.Broker) int {
	deadline := time.Now().Add(2 * time.Second)
	for {
		n := openChannels(b)
		if n == 0 || time.Now().After(deadline) {
			return n
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// waitUntil polls cond until it holds, or fails the test saying what it was
// waiting for.
//
// The budget is generous because the thing being waited for is a goroutine
// hand-off on a fake broker: it takes microseconds when it happens at all, so
// a wait that runs out is a hang rather than a slow machine.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// TestAReconnectionDoesNotRereadTheBrokersFacts is, and it
// reads the consequence rather than the race.
//
// `Backend.readFacts` used to run on every dial, from inside `redial`. That
// was a data race — `b.fx` is a plain struct with no lock and, once handles
// exist, `source.admit` reads `b.fx.quorum` on the pump goroutine for every
// delivery it maps — and it also undid the one thing about `b.fx` a
// reconnection may NOT change. `Backend.probe` runs once, at open, and
// UPGRADES `b.fx.delayKind` to the plugin arm when it detects the plugin;
// re-reading the facts put it back to `b.cfg.DelayKind`, so a client that had
// detected the plugin silently fell back to TTL+DLX after its first outage.
// That is the exact failure `probe`'s own comment says must not happen, and
// nothing in a deployment would report it: the redeliveries just start being
// approximate.
func TestAReconnectionDoesNotRereadTheBrokersFacts(t *testing.T) {
	broker, client := openFakeWith(t,
		rabbitfake.Config{DelayedMessagePlugin: true},
		func(c *rq.Config) {
			c.Declare, c.DetectDelayPlugin = true, true
			c.ReconnectBase = time.Microsecond
		})
	b, ok := rq.BackendForTest(client)
	if !ok {
		t.Fatal("the client's Native is not this driver's backend")
	}
	pub := publishOn(t, client, "orders")
	before := rq.FactsForTest(b)
	if !contains(before, "delaykind=plugin") {
		t.Fatalf("the probe did not resolve to the plugin arm, so this test cannot see the regression: %s", before)
	}

	cn := broker.LastConnection()
	if cn == nil {
		t.Fatal("the broker has no connection to close")
	}
	cn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})

	// A publication that SUCCEEDS is the wait, and a weaker one is not enough:
	// the broker having a second connection only says `Dial` returned, which
	// is before the facts were re-read, so a test that waited on that read the
	// old facts about half the time. A publication succeeds only once `redial`
	// has re-attached this sink, which is the last of its four steps.
	waitUntil(t, "a publication to succeed on the new connection", func() bool {
		return pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}) == nil
	})

	if after := rq.FactsForTest(b); after != before {
		t.Errorf("the reconnection changed the broker facts:\n before %s\n after %s\nthe capability table is built from these once, at open, so a fact that moves afterwards is a client whose claims and whose behaviour have stopped agreeing ", before, after)
	}
}

// TestFetchNeverHandsOutADeliveryFromAReplacedChannel is.
//
// `source.admit` reads the generation, then claims a token, decodes the
// delivery, unwraps its envelope and only then pushes it into the ring. The
// channel can be replaced anywhere in between, and `source.claimLost`'s answer
// -- cancel the pump, then drain -- cannot reach a push that is already past
// the check. So the ring can end up holding a delivery from a channel the
// broker has already taken everything back on, and handing that to a caller is
// handing out something nobody can settle. It happened: the seed
// `52404cfc9f059630` fetched exactly such a delivery and then fetched the
// broker's redelivery of the same message, with nothing in between that could
// have invalidated the first, which is a message delivered twice while it was
// still settleable.
//
// The guard is on the way OUT, in [source.Fetch], because that is the one
// place a delivery leaves this driver and the only check a late admission
// cannot get in front of.
func TestFetchNeverHandsOutADeliveryFromAReplacedChannel(t *testing.T) {
	_, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	// The first Fetch attaches the consumer, which is what gives the source a
	// generation to supersede.
	ctx, cancel := contextWithTimeout(t, 200*time.Millisecond)
	defer cancel()
	if _, err := cons.Receive(ctx, make([]mq.Delivery, 4)); err == nil {
		t.Fatal("the empty queue produced a delivery")
	}
	b, ok := rq.BackendForTest(client)
	if !ok {
		t.Fatal("the client's Native is not this driver's backend")
	}
	before := rq.StaleForTest(b)

	if err := rq.LateAdmissionForTest(b, "late"); err != nil {
		t.Fatalf("staging a late admission: %v", err)
	}

	fctx, fcancel := contextWithTimeout(t, 200*time.Millisecond)
	defer fcancel()
	buf := make([]mq.Delivery, 4)
	n, _ := cons.Receive(fctx, buf)
	for i := range n {
		if body := string(buf[i].Body); body == "late" {
			t.Fatalf("Fetch handed out %q, whose channel had been replaced before it reached the ring: its tag names a different delivery on the replacement channel and its token is already invalid, so the caller has a message it cannot settle and the broker will deliver again ", body)
		}
	}
	if got := rq.StaleForTest(b); got != before+1 {
		t.Errorf("the source counted %d stale deliveries, want %d: a delivery dropped on the way out has to be counted, or a source that never reached the case is indistinguishable from one that dropped silently", got-before, 1)
	}
}

// TestTwoAttachmentsDoNotShareOneDeliverysStorage is.
//
// `driverkit.TokenTable.InvalidateAll` frees every slot at once, which is what
// a reconnection needs: every delivery the broker lent is gone. The
// replacement channel's pump then claims slot zero while the delivery that had
// slot zero may still be in a caller's hands — and while the per-slot storage
// belonged to the SOURCE rather than to the attachment, those two deliveries
// were one `mq.Message`. The race detector found the two pumps writing it
// through `FuzzRabbitTagSettlement`; the damage it does not report is that
// the borrowed body is rewritten under a caller who is reading it.
func TestTwoAttachmentsDoNotShareOneDeliverysStorage(t *testing.T) {
	_, client := openFake(t, declaring)
	cons := consumeOn(t, client, "orders", "workers")
	ctx, cancel := contextWithTimeout(t, 200*time.Millisecond)
	defer cancel()
	if _, err := cons.Receive(ctx, make([]mq.Delivery, 4)); err == nil {
		t.Fatal("the empty queue produced a delivery")
	}
	b, ok := rq.BackendForTest(client)
	if !ok {
		t.Fatal("the client's Native is not this driver's backend")
	}

	first, second, err := rq.SharedSlotForTest(b, "before", "after")
	if err != nil {
		t.Fatalf("driving two attachments through one slot: %v", err)
	}
	if first != "before" || second != "after" {
		t.Fatalf("the two deliveries report %q and %q, want \"before\" and \"after\": the replacement attachment decoded into the storage the first delivery is still using, so a body the specification says stays borrowed until settlement was rewritten under its holder ", first, second)
	}
}

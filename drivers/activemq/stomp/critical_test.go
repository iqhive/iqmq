package stomp_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"bitbucket.org/iqhive/iqmq/internal/activemq"
)

//
// Each one is a scenario whose failure would be a silent DATA defect rather
// than a visible error, and not one of them can be provoked from a container:
// a NACK frame that must not be written, a receipt that never arrives, a
// subscription reset that invalidates its own deliveries, an ERROR frame at a
// chosen moment, and a socket that dies without a DISCONNECT. The seam is the
// socket, for the reason `internal/stompsrv`'s package comment gives.

// TestCriticalRequeueSendsNoNackFrame is this increment's headline finding as
// a test.
//
// the specification's `Nack(NackRequeue())` promises the message becomes deliverable again.
// STOMP has a NACK frame, so the obvious mapping is Nack to NACK — and on
// BOTH brokers that mapping destroys the message while reporting a requeue.
// Measured (doc.go carries the numbers): on Apache ActiveMQ Artemis a NACK is
// treated exactly as an ACK, so one NACK and one ACK took
// `getMessagesAcknowledged` from 0 to 2, the queue emptied and the dead-letter
// queue did not grow; on ActiveMQ Classic a NACK is a POISON_ACK, so the
// message goes to the broker's dead-letter strategy and not back to the queue.
// Neither returns it. Nothing in either product's documentation says so.
//
// So the assertion is positive and not an absence nobody looked for: what the
// server must see after a requeue is an UNSUBSCRIBE and a SUBSCRIBE — the
// protocol's own recovery, which IS measured to return the message with
// `redelivered: true` — and it must see no NACK frame and no ACK frame at all.
func TestCriticalRequeueSendsNoNackFrame(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)

	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]
	if got := string(d.Message.Body); got != "one" {
		t.Fatalf("the delivery carries %q, want %q", got, "one")
	}

	// Everything from here is the requeue's own doing, which is what makes
	// the frame assertion below about the requeue rather than about the
	// source's whole life.
	mark := srv.Next()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())); err != nil {
		t.Fatalf("Nack(NackRequeue()): %v", err)
	}
	// The wait is satisfied by EITHER outcome -- the reset's two frames, or a
	// NACK frame -- so that the headline assertion below is the one that
	// speaks when a driver sends the wrong thing, rather than a timeout on a
	// re-subscribe that was never going to happen.
	await(t, "the requeue's frames reaching the server", func() bool {
		return srv.Subscribes(consume) >= 2 || countOf(srv, frame.NACK) > 0
	})

	if got := countOf(srv, frame.NACK); got != 0 {
		t.Errorf("the driver sent %d NACK frame(s) for a REQUEUE. A STOMP NACK returns no message on either "+
			"broker -- on Artemis it is an acknowledgement and on Classic a poison-ack -- so this destroys the "+
			"message while reporting a requeue, which is the silent-loss disqualifier.\nframes:%s",
			got, frameLog(srv))
	}
	if got := countOf(srv, frame.ACK); got != 0 {
		t.Errorf("the driver sent %d ACK frame(s) for a requeue, which acknowledges the message the caller asked "+
			"to have back.\nframes:%s", got, frameLog(srv))
	}
	if got := srv.Commands(mark); len(got) != 2 || got[0] != frame.UNSUBSCRIBE || got[1] != frame.SUBSCRIBE {
		t.Errorf("the requeue sent %v; want exactly [%s %s]. The mechanism is the protocol's own recovery: an "+
			"unacknowledged client-individual delivery comes back when its SUBSCRIPTION goes away, measured on "+
			"both brokers and marked `redelivered`.\nframes:%s",
			got, frame.UNSUBSCRIBE, frame.SUBSCRIBE, frameLog(srv))
	}
	if got := src.Resets(); got != 1 {
		t.Errorf("Source.Resets() is %d after one requeue, want 1: the reset is what a requeue COSTS on this "+
			"protocol, and a deployment that cannot see the number cannot see what its nack rate is buying",
			got)
	}

	// The other half of the mechanism: the broker gives it back, marked.
	// This is the server doing what both brokers were measured doing, and
	// the driver has to read `redelivered` as the attempt 2.
	deliver(t, srv, consume, stompsrv.Message{Body: []byte("one"), Redelivered: true})
	again := fetchN(t, src, 1)[0]
	if got := string(again.Message.Body); got != "one" {
		t.Fatalf("the redelivery carries %q, want %q", got, "one")
	}
	if !again.Info.Redelivered {
		t.Error("the redelivery does not report Redelivered; `redelivered` is the whole of what a STOMP delivery " +
			"says about its history on either broker")
	}
	// Attempt is ZERO, which is its own documented answer -- "the
	// broker does not count attempts" -- and it is the truthful one here
	// rather than 2 derived from the boolean. Measured on Artemis, a
	// redelivery after an UNSUBSCRIBE arrives with `redelivered: false` and
	// only one after a reconnection arrives with true, so the boolean is
	// not even a reliable "at least 2": the nack/immediate-redelivery
	// read "attempt 1 after a first delivery at 1" against the real broker
	// when this driver derived a count from it. A zero makes a MaxAttempts
	// policy above this driver refuse to act rather than act on a wrong
	// number.
	if got := again.Info.Attempt; got != 0 {
		t.Errorf("the redelivery reports attempt %d, want 0: neither broker sends a count over STOMP and the "+
			"`redelivered` boolean is not a reliable substitute, so the \"the broker does not count attempts\" "+
			"is the honest answer", got)
	}
}

// TestCriticalSettlementAfterAResetIsRefused is the rule in this
// protocol's terms.
//
// The specification: "a settlement must never apply to a delivery other than the one it
// names". A STOMP 1.2 ACK names a delivery by the `ack` header the broker sent
// with it, and that identifier belongs to the SUBSCRIPTION it came from. A
// requeue takes the subscription away, so every delivery it had handed out is
// the broker's again — and a settlement carrying one of their identifiers must
// be refused rather than written, because on a broker that reuses identifiers
// it would settle whoever now holds that one.
//
// That is also the COST the capability note states: a reset returns every
// unacknowledged delivery of the subscription, not just the one that was
// nacked. So this holds two deliveries, requeues one, and settles the other.
func TestCriticalSettlementAfterAResetIsRefused(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 8)

	ids := deliver(t, srv, consume, body("one"), body("two"))
	held := fetchN(t, src, 2)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	if err := src.Settler().Nack(ctx, &held[0], nackOptions(t, mq.NackRequeue())); err != nil {
		t.Fatalf("Nack(NackRequeue()) on the first delivery: %v", err)
	}
	if err := srv.WaitSubscribed(consume, 2, budget); err != nil {
		t.Fatalf("the driver did not re-subscribe after a requeue: %v", err)
	}

	err := src.Settler().Ack(ctx, &held[1])
	if err == nil {
		t.Fatalf("acknowledging a delivery whose subscription was reset reported success. Its `ack` identifier "+
			"belongs to a subscription the broker has taken back, so sending it would either be refused or -- on a "+
			"broker that reuses identifiers -- settle somebody else's delivery, which is the specification's "+
			"disqualifier.\nframes:%s", frameLog(srv))
	}
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("the refusal is %v (code %v), want a lost lease: the delivery went with its subscription, and "+
			"The specification makes that a lease loss rather than a failure the caller should retry", err, mq.CodeOf(err))
	}
	if got := srv.SettledIDs(frame.ACK); len(got) != 0 {
		t.Errorf("the server received ACK frames %v after a reset; none of those identifiers is this "+
			"subscription's any more, and the delivery the broker applies one to is whichever now holds it. The "+
			"reset returned the deliveries %v.\nframes:%s", got, ids, frameLog(srv))
	}
	if got := src.StaleSettlements(); got < 1 {
		t.Errorf("Source.StaleSettlements() is %d, want at least 1: a refusal nobody counts is a refusal nobody "+
			"can alert on", got)
	}
}

// TestCriticalCancelUnblocksAParkedFetch is.
//
// The specification: "Close unblocks everything parked". A push-protocol driver buffers,
// and a fetch that found the buffer empty parks on it — so the ONLY thing that
// can wake it is the buffer being closed, because `driverkit.PushAdapter` has
// no other signal. A driver that recorded the close and left the buffer alone
// would leave a consumer parked for ever on a buffer nothing will ever push
// to, and the caller would see a hang rather than an error.
//
// The subscription here is one the server sends nothing to, which is the only
// way to park a fetch with certainty.
func TestCriticalCancelUnblocksAParkedFetch(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	parked := make(chan struct{})
	go func() {
		close(parked)
		// The context is far longer than the test's own budget on purpose:
		// a Close that did not wake the fetch must show up as a hang this
		// test reports, not as a context deadline that looks like a pass.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		dst := make([]mq.DriverDelivery, 4)
		n, err := src.Fetch(ctx, dst)
		done <- result{n: n, err: err}
	}()
	<-parked

	start := time.Now()
	// Not a bare Close: go-stomp can miss its own Broadcast on the
	// UNSUBSCRIBE receipt and wait out a thirty-second timeout this driver
	// does not shorten (see closeSource). What this test is about is the
	// buffer waking the parked fetch, which happens BEFORE the unsubscribe
	// either way.
	closeSource(t, src)
	select {
	case r := <-done:
		if r.n != 0 {
			t.Errorf("the fetch that a Close woke returned %d deliveries; a cancelled consumer must hand over "+
				"nothing it cannot be settled for", r.n)
		}
		if !errors.Is(r.err, mq.ErrClosed) {
			t.Errorf("the woken fetch reports %v (code %v), want ErrClosed: the specification makes a cancel a definite end "+
				"and not a retryable failure the core would spin on", r.err, mq.CodeOf(r.err))
		}
		if took := time.Since(start); took > budget {
			t.Errorf("the fetch took %v to wake, which is not promptly: the cancel is synchronous with the "+
				"caller's Close", took)
		}
	case <-time.After(budget):
		t.Fatalf("Close did not unblock the parked fetch within %v. `driverkit.PushAdapter` wakes a parked Fetch "+
			"when it is CLOSED and has no other signal, so a driver that only recorded the close leaves the "+
			"consumer parked for ever", budget)
	}

	// The specification again: a second Close is a no-op and not an error. It really is
	// a bare Close: the first one retired the subscription, so there is
	// nothing left to unsubscribe and nothing to wait for.
	if err := src.Close(); err != nil {
		t.Errorf("the second Close reported %v; the specification makes it idempotent, and a shutdown path that closes twice "+
			"is the normal case rather than a mistake", err)
	}
}

// TestCriticalPublicationWithNoReceiptIsAmbiguous is.
//
// A STOMP SEND frame has no answer of its own. What the protocol offers is the
// `receipt` header, and a RECEIPT frame is therefore the whole of
// `ConfirmAccepted`. So a receipt that never arrives is the one case the specification
// names AMBIGUOUS: the frame left the socket, so the broker MAY have taken the
// message, and a publisher told "failed" republishes it — after which the
// broker holds two and nothing anywhere reports a duplicate.
//
// No broker will hold a receipt on request, which is why this needs the
// in-process server at all.
func TestCriticalPublicationWithNoReceiptIsAmbiguous(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)

	srv.HoldReceipts(1)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := snk.Publish(ctx, &mq.Message{Body: []byte("one")}, nil)
	if err == nil {
		t.Fatalf("a publication whose RECEIPT never arrived reported success. Nothing came back from the broker, "+
			"so ConfirmAccepted's boundary was never reached.\nframes:%s", frameLog(srv))
	}
	if !mq.IsAmbiguous(err) {
		t.Errorf("the failure is %v (code %v, ambiguous=%v), want AMBIGUOUS. The SEND frame left the socket, so "+
			"the broker may have the message: a caller told \"failed\" republishes it and the broker then holds "+
			"two, which is the whole reason for having an ambiguous outcome at all",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	if got := mq.CodeOf(err); got != mq.CodeSettlementUnknown {
		t.Errorf("the failure's code is %v, want %v: the outcome is unknown rather than failed",
			got, mq.CodeSettlementUnknown)
	}
	if got := b.Ambiguous(); got != 1 {
		t.Errorf("Backend.Ambiguous() is %d, want 1: the window is a number a deployment has to be able to "+
			"read, and one nobody counts is one nobody can alert on", got)
	}
	// The frame really did leave: this is what makes the ambiguity real
	// rather than a label.
	if got := countOf(srv, frame.SEND); got != 1 {
		t.Errorf("the server received %d SEND frames, want 1. If nothing was sent the outcome is not ambiguous, "+
			"it is a clean failure, and calling it ambiguous would make every caller republish for nothing", got)
	}

	// And the next publication, whose receipt IS answered, succeeds: a sink
	// that had poisoned itself on the ambiguous one would pass the test
	// above and be useless.
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("two")}, nil); err != nil {
		t.Errorf("the publication after an ambiguous one failed: %v", err)
	}
}

// TestCriticalFailedDeadLetterPublishDoesNotAcknowledge is.
//
// The specification: "a failed or ambiguous dead-letter publish never acknowledges the
// original". The ORDER is the whole contract. This driver's dead-letter route
// on Artemis is a PUBLICATION to another of its own topics followed by an ACK,
// because a NACK frame on Artemis acknowledges the delivery rather than
// dead-lettering it — so if the ACK went first, or went anyway, the message
// would be gone from the original queue and absent from the dead-letter topic.
// That is silent loss with every step individually plausible, which is the
// shape the specification calls a disqualifier.
//
// The failure injected here is the AMBIGUOUS half of "failed or ambiguous":
// the dead-letter publication's RECEIPT never arrives, so the driver does not
// know whether the dead-letter topic has the message — and must therefore not
// destroy the only other copy. No broker will hold a receipt on request.
func TestCriticalFailedDeadLetterPublishDoesNotAcknowledge(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	cfg.DeadLetterTopic = "orders-dead"
	// The strict default, and it is worth saying that it works, because for
	// most of this increment it did not: `settler.deadLetter` carried
	// NackOptions.Reason under `x-iqmq-dead-letter-reason`, and the shared
	// mapping refuses any application header under that prefix outright --
	// the prefix is how it carries the four slots neither protocol has, and
	// it cannot tell this driver's header from a caller's. So every
	// dead-letter publication was refused before any I/O, no SEND frame was
	// ever written, and this test would have been green about a publication
	// that was never attempted. The header names left the prefix; a
	// publication now happens and the ordering below is observable.
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deadLetter, _ := destinations(t, cfg, "orders-dead", "")

	deliver(t, srv, consume, body("poison"))
	d := fetchN(t, src, 1)[0]

	srv.HoldReceiptsOn(deadLetter, 1)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter(), mq.NackReason("poison")))
	if err == nil {
		t.Fatalf("a dead-letter settlement whose publication was never confirmed reported success.\nframes:%s",
			frameLog(srv))
	}
	sends := srv.FramesOf(frame.SEND)
	if len(sends) != 1 {
		t.Fatalf("the server received %d SEND frames, want 1 (the dead-letter publication). Without one there is "+
			"nothing to be about, and every assertion below would pass for the wrong "+
			"reason.\nframes:%s", len(sends), frameLog(srv))
	}
	if got := sends[0].Get(frame.Destination); got != deadLetter {
		t.Errorf("the dead-letter publication went to %q, want %q", got, deadLetter)
	}
	if !sends[0].Has(frame.Receipt) {
		t.Errorf("the dead-letter publication carries no `receipt` header, so nothing about it was ever confirmed: "+
			"the boundary is the broker having taken the dead-lettered copy, and a SEND with no receipt is "+
			"fire-and-forget.\nframes:%s", frameLog(srv))
	}
	if got := srv.SettledIDs(frame.ACK); len(got) != 0 {
		t.Errorf("the original was ACKNOWLEDGED (%v) after its dead-letter publication went unconfirmed. The "+
			"broker then has no copy and the dead-letter topic may have none either, which is the exact "+
			"prohibition.\nframes:%s", got, frameLog(srv))
	}
	if got := countOf(srv, frame.NACK); got != 0 {
		t.Errorf("the driver sent %d NACK frame(s) on Artemis, where a NACK is measured to ACKNOWLEDGE the "+
			"delivery: that is the same loss by another route.\nframes:%s", got, frameLog(srv))
	}

	// A second attempt must not acknowledge it either. The specification does not stop
	// applying because the caller tried twice.
	if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackDeadLetter())); err == nil {
		t.Error("the second dead-letter attempt reported success without a publication the broker confirmed")
	}
	if got := srv.SettledIDs(frame.ACK); len(got) != 0 {
		t.Errorf("a retry of the failed dead-letter settlement acknowledged the original (%v)", got)
	}
}

// TestCriticalRefusedSubscribeReportsNotFound pins where a broker's refusal of
// a SUBSCRIBE surfaces.
//
// Two facts decide the answer and both are measured. Artemis with
// auto-creation disabled answers a SUBSCRIBE to a destination that does not
// exist with `AMQ339001 Destination does not exist <name>`, as an ERROR frame,
// and then closes the connection. And a `receipt` header on a SUBSCRIBE frame
// cannot be used at all: go-stomp v3.1.5 registers the subscription's own
// frame channel under the receipt identifier, the RECEIPT closes that channel,
// and the library panics with "send on closed channel" from inside its own
// `processLoop` (source.go records this as). So a
// subscription cannot be confirmed synchronously and the refusal has nowhere
// to surface but the first FETCH. That is the contract, and the alternative —
// `Unavailable` with the broker's own sentence dropped — leaves an operator
// with no diagnosis at all, because an ERROR frame's `message` header is the
// only one STOMP offers.
//
// The ERROR frame is written at a moment this test picks rather than when the
// SUBSCRIBE arrives, and that is deliberate: the two orderings take different
// paths through the driver, and a test that could not choose would be green
// about whichever one it happened to get. The automatic
// refusal is exercised first, for the claims that do not depend on the order.
func TestCriticalRefusedSubscribeReportsNotFound(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	b := openBackend(t, cfg)
	_, consume := destinations(t, cfg, "orders", "workers")
	refusal := "AMQ339001 Destination does not exist " + consume

	// Phase one: Artemis's own answer, on the SUBSCRIBE itself. What is
	// asserted here is only what does not depend on the ordering -- that the
	// SUBSCRIBE was sent, that the ERROR was fatal to the connection, and
	// that the driver came back.
	srv.RefuseSubscribe(consume, refusal)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)
	// The SOURCE's own re-dial and not the backend's: a source owns its
	// connection, because a STOMP connection multiplexes deliveries and
	// receipts and a consumer that stops reading would otherwise block a
	// publisher's receipts on the same socket. So the
	// backend's Reconnects counter stays at zero here and the source's
	// Redials is what moves.
	awaitRedial(t, src, 1, func() { fetchQuietly(src, 100*time.Millisecond) })
	if got := srv.Accepted(); got < 2 {
		t.Fatalf("the server took %d connections, want at least 2: an ERROR frame is fatal to the connection on "+
			"both brokers -- measured, the broker closes the socket after it -- so a driver that read the refusal "+
			"and kept the connection would be holding a dead one", got)
	}
	await(t, "the driver re-sending its SUBSCRIBE on the new connection", func() bool {
		fetchQuietly(src, 100*time.Millisecond)
		return srv.Subscribes(consume) >= 2
	})

	// Phase two: the same refusal, written while a fetch can see it.
	if err := srv.ErrorFrame(refusal); err != nil {
		t.Fatalf("ErrorFrame: %v", err)
	}
	// No redial is awaited here, and that is the point of phase two: the
	// fetch itself is what must report the refusal. Nothing may CONSUME the
	// broker's sentence and discard it -- a terminal error is reported once
	// and then forgotten, because the sentence is the only diagnosis STOMP
	// offers and repeating it for ever would make a recovered subscription
	// look broken -- so the throwaway fetches phase one pokes with are wrong
	// here.
	//
	// # This bound cannot be converted to a wait on an event, and that was
	// # established the hard way
	//
	// Its two-second version went red on another branch's gate with
	// `DeadlineExceeded` instead of `NotFound`, under five concurrent
	// `make ci` runs, while the server log showed the broker had answered
	// `AMQ339001` three times. The obvious repair -- fetch repeatedly until
	// an answer that is not the caller's own deadline -- was written, and it
	// is WRONG, because polling PERTURBS this subject. Each extra fetch
	// drives another re-subscribe, and a re-subscribe reports through the
	// OPEN path rather than the pump: 200 runs produced one
	// `open ... Unavailable: connection closed unexpectedly (code
	// Unavailable), want NotFound` -- the right answer displaced by a
	// symptom of the polling -- and then a later run parked in
	// `PushAdapter.Fetch` for twenty-one minutes, because that iteration had
	// left the source subscribed with no delivery and no sentence to report.
	// A single attempt that waits INSIDE itself is the only shape that both
	// reads the pump's answer and leaves the source alone.
	//
	// So the clock stays, and what changed is only that it is the package's
	// shared `budget` rather than a bespoke two seconds -- the same bound
	// the other forty-nine waits in this package use.
	err := fetchError(t, src, budget)
	if err == nil {
		t.Fatalf("the fetch after a refused SUBSCRIBE reported success. The subscription does not exist, so a "+
			"consumer that was told nothing waits for messages that can never arrive.\nframes:%s", frameLog(srv))
	}
	if got := mq.CodeOf(err); got != mq.CodeNotFound {
		t.Errorf("the fetch reports %v (code %v), want %v. `AMQ339001` is the only diagnosis STOMP offers -- there "+
			"is no code and no structured field, just the ERROR frame's `message` header -- so a driver that maps "+
			"it to Unavailable leaves an operator with a retryable failure and no cause",
			err, got, mq.CodeNotFound)
	}
	if !strings.Contains(err.Error(), "AMQ339001") {
		t.Errorf("the failure does not carry the broker's own sentence: %v. It is the whole of what STOMP says "+
			"about a refusal, and a driver that dropped it would report \"not found\" about nothing in particular",
			err)
	}
	if !errors.Is(err, mq.ErrTopicNotFound) {
		t.Errorf("the failure does not match mq.ErrTopicNotFound: %v", err)
	}
}

// TestCriticalConnectionDeathIsRecoveredAndStaleSettlementsRefused is the specification's
// reconnection and the staleness, which are one test because the second is
// what makes the first safe.
//
// go-stomp does not reconnect: a connection whose socket died is a `*Conn`
// whose every method reports it, and nothing re-establishes one. So the driver
// owns the loop — and the order is the contract. The generation is bumped
// FIRST, so that from the instant the connection died every delivery a source
// handed out is stale; only then is the socket re-dialled and the SUBSCRIBE
// re-sent. A driver that re-dialled first would have a live subscription and a
// pile of `ack` identifiers belonging to a subscription the broker has
// forgotten, and every one of them would settle whatever now holds it.
func TestCriticalConnectionDeathIsRecoveredAndStaleSettlementsRefused(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)

	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	// The socket dies with no DISCONNECT, which is what a broker restart, a
	// network partition or an operator's kill does to a client. Closing the
	// STOMP session instead would be an orderly shutdown, which is a
	// different thing entirely.
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	// The source's own re-dial; see the note in
	// TestCriticalRefusedSubscribeReportsNotFound.
	awaitRedial(t, src, 1, func() { fetchQuietly(src, 100*time.Millisecond) })
	if got := srv.Accepted(); got < 2 {
		t.Fatalf("the server took %d connections, want at least 2", got)
	}
	await(t, "the driver re-sending its SUBSCRIBE on the new connection", func() bool {
		fetchQuietly(src, 100*time.Millisecond)
		return srv.Subscribes(consume) >= 2
	})
	subs := srv.FramesOf(frame.SUBSCRIBE)
	if len(subs) < 2 {
		t.Fatalf("the server received %d SUBSCRIBE frames, want at least 2", len(subs))
	}
	if subs[0].Conn == subs[len(subs)-1].Conn {
		t.Errorf("both SUBSCRIBE frames arrived on connection %d: the subscription has to be re-sent on the NEW "+
			"connection, and a driver that re-used the old one is holding a socket that is gone", subs[0].Conn)
	}
	if got := subs[len(subs)-1].Get("subscription-type"); got != "MULTICAST" {
		t.Errorf("the re-sent SUBSCRIBE carries subscription-type %q, want MULTICAST: without it a SUBSCRIBE to a "+
			"fully qualified queue name is read as an ANYCAST consumer, which is a different routing type on the "+
			"same address -- so a recovered subscription would consume from somewhere else", got)
	}

	// The delivery handed out before the death is the broker's again: the
	// connection took the subscription with it.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := src.Settler().Ack(ctx, &d)
	if err == nil {
		t.Fatalf("acknowledging a delivery from a connection that died reported success.\nframes:%s", frameLog(srv))
	}
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Errorf("the refusal is %v (code %v), want a lost lease: the settlement never reached the broker, so the "+
			"broker still holds the delivery and will hand it to the next subscription -- which the specification makes a lease "+
			"loss rather than an unknown outcome, and the difference matters because an unknown outcome is "+
			"permanent", err, mq.CodeOf(err))
	}
	if got := srv.SettledIDs(frame.ACK); len(got) != 0 {
		t.Errorf("the server received ACK frames %v after the connection was replaced. Those identifiers belong "+
			"to a subscription the broker has forgotten, so each would settle whichever delivery now holds "+
			"it.\nframes:%s", got, frameLog(srv))
	}
	if got := src.StaleSettlements(); got < 1 {
		t.Errorf("Source.StaleSettlements() is %d, want at least 1", got)
	}

	// And the recovered subscription works, which is the other half: a
	// driver that invalidated everything and never came back would pass
	// every assertion above.
	deliver(t, srv, consume, stompsrv.Message{Body: []byte("two"), Redelivered: true})
	again := fetchN(t, src, 1)[0]
	if got := string(again.Message.Body); got != "two" {
		t.Fatalf("the delivery after the reconnection carries %q, want %q", got, "two")
	}
	if err := src.Settler().Ack(ctx, &again); err != nil {
		t.Errorf("acknowledging a delivery from the recovered subscription: %v", err)
	}
	if err := srv.WaitFrames(frame.ACK, 1, budget); err != nil {
		t.Errorf("the live delivery's ACK never reached the server: %v\nframes:%s", err, frameLog(srv))
	}
}

// compile-time proof that the broker constants these tests name are
// `internal/activemq`'s own rather than strings this file invented: a test
// that spelled the broker itself would still pass against a driver that
// mapped the other one.
var _ = [...]activemq.Broker{activemq.Artemis, activemq.Classic}

// TestCriticalSinkRecoversAfterTheConnectionDies is the BACKEND's dial loop,
// which a source no longer exercises.
//
// A source owns its own connection , so a source's socket
// dying is that source's repair and the backend's `Reconnects` counter never
// moves for it. What still uses the backend's connection is every SINK -- a
// publisher never stops reading, so there is nothing for it to deadlock
// against -- and this is what holds that loop to the specification's "reconnect with backoff"
// now that nothing else does.
//
// It is here rather than in settlement_test.go because the failure it guards
// against is the one the specification is about: a publication after a connection death
// that is reported as succeeding without the broker having seen it.
func TestCriticalSinkRecoversAfterTheConnectionDies(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	b := openBackend(t, cfg)
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	if err := snk.Publish(ctx, &mq.Message{Body: []byte("one")}, nil); err != nil {
		t.Fatalf("the first publication: %v", err)
	}
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	// The publication that meets the death has two honest outcomes and the
	// socket watcher is what decides which: if it ran first the sink finds no
	// connection at all and the frame provably never left, which is a
	// DEFINITE failure; if the publication got there first the frame may have
	// gone and the specification makes the outcome ambiguous. Either way the caller must
	// be able to act on it, which is what is asserted here -- the ambiguity
	// itself is pinned deterministically by
	// TestCriticalPublicationWithNoReceiptIsAmbiguous, which holds the receipt
	// back rather than racing a socket.
	err := snk.Publish(ctx, &mq.Message{Body: []byte("two")}, nil)
	if err != nil && !mq.IsRetryable(err) && !mq.IsAmbiguous(err) {
		t.Errorf("the publication that met the connection's death reports %v (code %v), retryable=%v "+
			"ambiguous=%v; a transport failure while the dial loop is running is one or the other, because "+
			"a caller that can neither retry nor reconcile has lost the message",
			err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err))
	}
	awaitReconnect(t, b, 1)
	if got := srv.Accepted(); got < 2 {
		t.Fatalf("the server took %d connections, want at least 2: the backend's dial loop is what re-establishes "+
			"the connection a sink publishes on", got)
	}
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("three")}, nil); err != nil {
		t.Fatalf("the sink never recovered: %v", err)
	}
}

// TestCriticalAnIdleConnectionsDeathIsNoticed is watch.go, and it is the one
// test that cannot be written as an operation meeting a failure.
//
// A broker drops a connection that nothing is using: a network partition, an
// idle timeout, a broker restart. Nothing in this client is in a call, so
// there is no error for a rule table to classify and no caller to report it
// to — and go-stomp offers no event. What the library does with the same
// socket is not enough on its own: its reader closes its frame channel and
// its process loop answers pending receipts on an unbuffered channel, so a
// connection whose last publication had already timed out is never even
// marked closed (watch.go has the measurement).
//
// So the driver watches the socket, and this asserts the consequence a
// deployment sees: the dial loop runs and health comes back on its own, with
// no operation to prompt it. Before the socket was watched this test found
// `reconnects=0` and a client that still called itself Ready.
func TestCriticalAnIdleConnectionsDeathIsNoticed(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	b := openBackend(t, reconnecting(artemisConfig(srv)))
	// One publication, so that there is a connection with something on it,
	// and then nothing at all: no sink, no source, no call in flight.
	snk := openSink(t, b, "orders", mq.ConfirmAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("one")}, nil); err != nil {
		t.Fatalf("the first publication: %v", err)
	}
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}

	awaitReconnect(t, b, 1)
	if got := srv.Accepted(); got < 2 {
		t.Fatalf("the server took %d connections, want at least 2: nothing but the socket's own death can have "+
			"started the second one, because no operation ran", got)
	}
	await(t, "health returning to Ready with no operation to prompt it", func() bool {
		return b.Health().State == mq.HealthReady
	})
	// And the connection that came back works.
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("two")}, nil); err != nil {
		t.Fatalf("publishing on the repaired connection: %v", err)
	}
}

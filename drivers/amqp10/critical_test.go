package amqp10_test

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

//
// Each of them is a way for an AMQP 1.0 driver to settle a delivery it does not
// own, report a publication that may have been accepted as a definite failure,
// lose a callback, or stop making progress — and each is unreachable from the
// portable API without control over the peer, which is why they are here against
// the scripted peer of internal/client rather than in the conformance suite.

// TestCriticalStaleDispositionAfterDetach is the disqualifier: "lets a
// stale ack settle a later redelivery".
//
// The sequence is the one a broker restart produces. A delivery is handed out,
// the connection dies, the dial loop replaces it, and the delivery the caller is
// still holding is settled. Detaching an AMQP 1.0 link returns everything
// unsettled on it to the node, so by that point the message is the node's again
// and may already be another consumer's — and the delivery id the disposition
// would name is one the NEW link may reuse for a different message.
//
// What has to be true:
//
//   - the settlement reports [mq.ErrLeaseLost] rather than succeeding;
//   - NOTHING is written to the peer for it. That is the half a driver gets
//     wrong: a driver that sent the disposition and mapped the resulting link
//     error would answer ErrLeaseLost too, and on a peer that reuses delivery
//     ids it would have settled the delivery that took the slot. The
//     disposition count is what tells the two apart.
func TestCriticalStaleDispositionAfterDetach(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	set := src.Settler()

	rcv, held := deliverAndFetch(t, f, src, "tag-1", "first")
	gen := f.b.Generation()
	before := len(rcv.Dispositions())

	// The connection dies. The supervisor bumps the generation, invalidates
	// the token table and re-dials.
	f.peer.LastConn().Kill(&amqp.ConnError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondConnectionForced}})
	if !aq.AwaitGenerationForTest(f.b, gen, budget) {
		t.Fatalf("the generation did not move past %d after the connection died", gen)
	}
	if !aq.AwaitReconnectForTest(f.b, 1, budget) {
		t.Fatal("the dial loop did not re-establish the connection")
	}

	err := set.Ack(context.Background(), &held)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack of a delivery whose link was detached = %v, want ErrLeaseLost: "+
			"detaching a link returns everything unsettled on it to the node, so this delivery is not ours ", err)
	}
	if got := len(rcv.Dispositions()); got != before {
		t.Fatalf("the refused settlement wrote %d disposition(s) to the peer; a stale disposition must never reach the "+
			"wire, because a peer that reuses delivery ids across an attach would apply it to whatever took this "+
			"delivery's place", got-before)
	}
	// The same rule for the negative direction: neither may reach a delivery
	// somebody else owns.
	if err := set.Nack(context.Background(), &held, mq.NackOptions{}); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Nack of the same delivery = %v, want ErrLeaseLost", err)
	}
	if n := src.StaleSettlements(); n == 0 {
		t.Fatal("the source counted no stale settlement, so a deployment could not see that this happened")
	}
	// And a delivery from the NEW link settles normally: a driver that
	// invalidated everything for ever would turn every reconnection into a
	// consumer that can no longer acknowledge anything.
	rcv2, fresh := deliverAndFetch(t, f, src, "tag-2", "second")
	if err := set.Ack(context.Background(), &fresh); err != nil {
		t.Fatalf("the Ack of a delivery from the NEW link = %v, want nil", err)
	}
	if got := rcv2.Dispositions(); len(got) != 1 || got[0].Tag != "tag-2" {
		t.Fatalf("the peer received %v, want one accepted disposition for tag-2", got)
	}
}

// TestCriticalStaleDeliveryGenerationIsRefused is the same disqualifier reached
// through the one window the token table alone does not close.
//
// A Fetch that is already in flight when the connection dies holds the OLD
// link. The supervisor invalidates the token table and resets the window before
// anything else — that is step 2 of reconnect.go's sequence — and the fetch then
// claims a FRESH slot for a message it took from the dead link, recording that
// link's generation on it. The token is valid, because the slot is new; the
// delivery is not, because the link it came from is gone and everything
// unsettled on it has already returned to the node.
//
// So the generation carried on the delivery is a second guard and not a
// duplicate of the table's, and this is the case that shows the difference. It
// is driven through [mq.DriverDelivery.Gen], which the specification makes the driver's own
// bookkeeping for exactly this — "a driver that redelivers a record hands out a
// new generation, and a settlement carrying the old one is answered with
// ErrLeaseLost rather than applied to the new delivery".
func TestCriticalStaleDeliveryGenerationIsRefused(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	set := src.Settler()
	rcv, fresh := deliverAndFetch(t, f, src, "tag-1", "x")
	before := len(rcv.Dispositions())

	// A delivery whose token the table still accepts and whose link belongs to
	// an older generation, which is what the fetch race produces.
	stale := fresh
	stale.Gen = fresh.Gen - 1

	err := set.Ack(context.Background(), &stale)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the Ack of a delivery from an older link generation = %v, want ErrLeaseLost: the link it came "+
			"from is gone, so everything unsettled on it has already returned to the node and may be another "+
			"consumer's ", err)
	}
	if got := len(rcv.Dispositions()); got != before {
		t.Fatalf("the refused settlement wrote %d disposition(s) to the peer", got-before)
	}
	if n := src.StaleSettlements(); n == 0 {
		t.Fatal("the source counted no stale settlement")
	}
	// And the refusal freed the slot rather than leaving it settling for ever,
	// which is what makes the delivery's own generation the thing that was
	// refused rather than the token.
	if err := set.Ack(context.Background(), &fresh); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the delivery whose generation was stale can still be settled under its own token: %v", err)
	}
}

// TestCriticalAmbiguousPublishWhenDispositionNeverArrives is Ambiguous
// outcome and the other disqualifier: "misclassifies an ambiguous publish".
//
// The transfer leaves the socket and the peer never answers. The publication may
// have been accepted — the peer may hold the message and have lost the
// disposition, or the disposition may be in a socket buffer that died — so
// nobody knows. Reporting it as a definite failure is what makes a caller retry
// and the broker hold two copies of a message it believes it published once.
func TestCriticalAmbiguousPublishWhenDispositionNeverArrives(t *testing.T) {
	peer := client.NewFake(testFacts())
	// The peer takes the transfer and answers nothing.
	peer.PlanSends(client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := snk.Publish(ctx, &mq.Message{Body: []byte("may-or-may-not-be-there")}, nil)
	if err == nil {
		t.Fatal("the publication succeeded although no disposition ever arrived")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("the publication's error is not ambiguous: %v (code %v). A transfer that has left the socket and whose "+
			"disposition never arrived may have been accepted, so the specification makes the outcome Ambiguous and makes "+
			"reporting it as a definite failure a disqualifying defect", err, mq.CodeOf(err))
	}
	// The native cause is still reachable, so a caller who wants to know WHY
	// the outcome is unknown can find out.
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the native cause is not reachable through the mapped error: %v", err)
	}
	if n := f.b.Stats().AmbiguousPublications; n != 1 {
		t.Fatalf("the backend counted %d ambiguous publication(s), want 1", n)
	}
}

// TestCriticalDefinitePublishFailureIsNotAmbiguous is the other half of the
// same distinction, and it matters as much: a peer that ANSWERS has given a
// definite answer, and reporting that as ambiguous would make every refusal
// something a caller has to reconcile.
func TestCriticalDefinitePublishFailureIsNotAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state amqp.DeliveryState
		code  mq.ErrorCode
	}{
		{
			name:  "rejected with a condition",
			state: &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondNotAllowed, Description: "no"}},
			code:  mq.CodeForbidden,
		},
		{
			name:  "rejected as unauthorized",
			state: &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}},
			code:  mq.CodeUnauthorized,
		},
		{
			name:  "released",
			state: &amqp.StateReleased{},
			code:  mq.CodeUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := client.NewFake(testFacts())
			peer.PlanSends(client.SendPlan{State: tc.state})
			f := openWith(t, testProfile(), aq.Config{}, peer)
			snk := f.sink(mq.SinkConfig{Topic: "topic"})
			err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
			if err == nil {
				t.Fatal("the publication succeeded through the peer's refusal")
			}
			if mq.IsAmbiguous(err) {
				t.Fatalf("a %s outcome the peer ANSWERED with is reported as ambiguous: %v", tc.name, err)
			}
			assertCode(t, err, tc.code, "the peer's "+tc.name)
		})
	}
}

// TestCriticalCloseWhileSendInFlight is the specification's "Close unblocks blocked
// operations" and the specification's "the callback runs exactly once, close included",
// together.
//
// A publication is in flight — the transfer has left and its disposition has not
// arrived — when the sink closes. Three things have to be true, and each of them
// is a way a driver loses work:
//
//   - Close RETURNS. A close that waited for a disposition the peer is never
//     going to send would hang for as long as the broker stayed silent, which
//     The specification forbids outright.
//   - the callback runs, exactly once, with an outcome that says the
//     publication's fate is unknown. A driver that dropped it would leave a
//     caller's pooled message unreleased for ever, and one that reported
//     success would be inventing an acknowledgement.
//   - a Publish afterwards reports ErrClosed rather than attaching a new link.
func TestCriticalCloseWhileSendInFlight(t *testing.T) {
	peer := client.NewFake(testFacts())
	peer.PlanSends(client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	outcomes := make(chan error, 4)
	msg := &mq.Message{Body: []byte("in-flight")}
	if err := snk.PublishAsync(context.Background(), msg, nil, func(err error) { outcomes <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	snd := f.sender(budget)
	waitFor(t, budget, "the transfer to reach the peer", func() bool { return snd.Held() == 1 })

	done := make(chan error, 1)
	go func() { done <- snk.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(budget):
		t.Fatal("Close did not return while a publication was in flight: the specification forbids a close that waits indefinitely")
	}

	select {
	case err := <-outcomes:
		if err == nil {
			t.Fatal("the in-flight publication's callback reported success; the transfer had left and no disposition " +
				"arrived, so nobody knows whether the peer holds it")
		}
		if !mq.IsAmbiguous(err) && mq.CodeOf(err) != mq.CodeSettlementUnknown {
			t.Fatalf("the in-flight publication's callback reported %v (code %v); the outcome is unknown",
				err, mq.CodeOf(err))
		}
	case <-time.After(budget):
		t.Fatal("the in-flight publication's callback never ran: the specification requires it to run exactly once, close included")
	}
	select {
	case err := <-outcomes:
		t.Fatalf("the callback ran twice; the second outcome was %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("after")}, nil); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("a publication after Close = %v, want ErrClosed", err)
	}
	if err := snk.Close(); err != nil {
		t.Fatalf("a second Close = %v, want nil: Close is idempotent", err)
	}
}

// TestCriticalCreditExhaustionDoesNotDeadlockFetch is the credit accounting's
// whole purpose.
//
// A source holds its whole prefetch unsettled. The peer has no credit, so it
// will send nothing, and a Fetch that blocked on the link would block for ever —
// a deadlock with no error and no diagnosis, which is the worst shape a defect
// can take.
//
// What has to be true:
//
//   - the Fetch parks rather than returning nothing, because the specification says Fetch
//     blocks until at least one delivery is available;
//   - a SETTLEMENT wakes it, because a settlement is what frees the room;
//   - the link never held more credit than the prefetch, which is the bound
//     The specification gives a meaning to.
func TestCriticalCreditExhaustionDoesNotDeadlockFetch(t *testing.T) {
	const prefetch = 2
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: prefetch})
	set := src.Settler()

	rcv, first := deliverAndFetch(t, f, src, "tag-1", "one")
	// Two more at the peer: one fills the window, the other cannot be
	// released because the credit is spent.
	rcv.Deliver(message("tag-2", "two"), message("tag-3", "three"))
	second := fetchOne(t, src, budget)

	if got := src.CreditStats(); got.Held != prefetch {
		t.Fatalf("the window holds %d deliveries, want the whole prefetch of %d (%+v)", got.Held, prefetch, got)
	}
	if got := rcv.Credit(); got != 0 {
		t.Fatalf("the peer still holds %d link credit with the window full; the prefetch is the bound and it is spent", got)
	}
	if got := rcv.Granted(); got > prefetch {
		t.Fatalf("the source issued %d credit for a prefetch of %d: a source must never let the peer send more "+
			"unsettled deliveries than its prefetch ", got, prefetch)
	}
	if got := rcv.Queued(); got != 1 {
		t.Fatalf("the peer holds %d undelivered message(s), want the one the spent credit is holding back", got)
	}

	// Park a fetch. It cannot produce anything: the peer has no credit.
	type result struct {
		n   int
		err error
	}
	parked := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		n, err := src.Fetch(ctx, dst)
		parked <- result{n: n, err: err}
	}()
	select {
	case r := <-parked:
		t.Fatalf("Fetch returned (%d, %v) with the credit window full; the specification says it blocks until a delivery is available",
			r.n, r.err)
	case <-time.After(100 * time.Millisecond):
	}

	// A settlement frees a slot, which is the only thing that can.
	if err := set.Ack(context.Background(), &first); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	select {
	case r := <-parked:
		if r.err != nil {
			t.Fatalf("the woken Fetch failed: %v", r.err)
		}
		if r.n != 1 {
			t.Fatalf("the woken Fetch returned %d deliveries, want 1", r.n)
		}
	case <-time.After(budget):
		t.Fatal("a settlement did not wake the parked Fetch: a full credit window must be a wait on a settlement and " +
			"never a wait on a link the peer has been given no credit for")
	}
	if err := set.Ack(context.Background(), &second); err != nil {
		t.Fatalf("Ack of the second delivery: %v", err)
	}
}

// TestCriticalCloseUnblocksAParkedFetch is the companion of the credit test:
// the OTHER thing that must wake a parked fetch is a close.
func TestCriticalCloseUnblocksAParkedFetch(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 1})
	set := src.Settler()
	// Attach the link and spend its credit, so the fetch below has nothing to
	// take and parks.
	_, held := deliverAndFetch(t, f, src, "tag-1", "one")

	parked := make(chan error, 1)
	go func() {
		dst := make([]mq.DriverDelivery, 1)
		_, err := src.Fetch(context.Background(), dst)
		parked <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-parked:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("the parked Fetch was woken with %v, want ErrClosed", err)
		}
	case <-time.After(budget):
		t.Fatal("Close did not unblock the parked Fetch ")
	}
	// And the delivery the source was holding went back to the broker rather
	// than sitting unsettled until the link expired.
	if err := set.Ack(context.Background(), &held); err == nil {
		t.Fatal("a delivery of a closed source was acknowledged; the close had already released it")
	}
}

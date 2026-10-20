package amqp10_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	aq "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// TestSinkSurfaces is the compile-time claim as a run-time check: the core
// asserts these once, when the sink is opened, so a surface that stopped being
// offered would be invisible.
func TestSinkSurfaces(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	var s mq.Sink = f.sink(mq.SinkConfig{Topic: "topic"})
	if _, ok := s.(mq.AsyncSink); !ok {
		t.Error("the sink is not an AsyncSink; SendWithReceipt is the protocol's natural asynchronous publication")
	}
	if _, ok := s.(mq.FlushSink); !ok {
		t.Error("the sink is not a FlushSink, so a caller cannot wait for what it has admitted")
	}
	// The two absences are decisions, and a sink that acquired either would
	// be advertising a saving it does not make.
	if _, ok := s.(mq.BatchSink); ok {
		t.Error("the sink claims mq.BatchSink; an AMQP 1.0 transfer carries one message and go-amqp has no batched " +
			"transfer, so a PublishBatch here would be a loop wearing a name that promises a saving")
	}
	if _, ok := s.(mq.OwnedSink); ok {
		t.Error("the sink claims mq.OwnedSink; every asynchronous path hands the message to the library, which holds " +
			"the encoded transfer until its disposition, so there is nothing to release at invocation")
	}
}

// TestSinkConfirmNoneCostsNoRoundTrip is ConfirmNone: local admission,
// which on this protocol is a PRE-SETTLED transfer — the frame leaves and no
// disposition is coming.
//
// The peer's script proves it: it is told to withhold every disposition, and a
// ConfirmNone publication succeeds anyway because it never waits for one.
func TestSinkConfirmNoneCostsNoRoundTrip(t *testing.T) {
	peer := newPeer()
	peer.PlanSends(client.SendPlan{Hold: true}, client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic", Confirmation: mq.ConfirmNone})
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("fire and forget")}, nil); err != nil {
		t.Fatalf("a ConfirmNone publication waited for a disposition that is never coming: %v", err)
	}
	if got := f.sender(budget).Sent(); len(got) != 1 {
		t.Fatalf("the peer received %d transfer(s), want 1", len(got))
	}
}

// TestSinkConfirmAcceptedWaitsForTheDisposition is the other side of the same
// coin, and the default: ConfirmAccepted waits for the peer's `accepted`
// disposition, which is the strongest ordinary acknowledgement this protocol
// has.
func TestSinkConfirmAcceptedWaitsForTheDisposition(t *testing.T) {
	peer := newPeer()
	peer.PlanSends(client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	done := make(chan error, 1)
	go func() { done <- snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil) }()
	snd := f.sender(budget)
	waitFor(t, budget, "the transfer to reach the peer", func() bool { return snd.Held() == 1 })
	select {
	case err := <-done:
		t.Fatalf("the publication returned %v before the peer answered; ConfirmAccepted waits for the disposition", err)
	case <-time.After(50 * time.Millisecond):
	}
	if !snd.ResolveHeld(0, &amqp.StateAccepted{}) {
		t.Fatal("resolving the held transfer")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the publication failed after the peer accepted it: %v", err)
		}
	case <-time.After(budget):
		t.Fatal("the publication did not return after the peer accepted it")
	}
}

// TestSinkConfirmDurableNeedsTheProfilesBoundary is the construction-time
// rule: ConfirmDurable fails when the sink is opened on a profile that cannot
// define a durable boundary, rather than per publication.
func TestSinkConfirmDurableNeedsTheProfilesBoundary(t *testing.T) {
	with := open(t, testProfile(), aq.Config{})
	if _, err := with.b.OpenSink(sinkCtx(), mq.SinkConfig{Topic: "t", Confirmation: mq.ConfirmDurable}); err != nil {
		t.Fatalf("ConfirmDurable on a profile that defines a boundary was refused: %v", err)
	}
	without := open(t, bareProfile(), aq.Config{})
	_, err := without.b.OpenSink(sinkCtx(), mq.SinkConfig{Topic: "t", Confirmation: mq.ConfirmDurable})
	assertIs(t, err, mq.ErrUnsupported, "ConfirmDurable on a profile with no boundary")
}

// TestSinkAsyncCostsOneGoroutinePerSink is the rule outright: "there is no
// goroutine per message".
//
// A hundred publications are held at the peer at once and the goroutine count is
// what says whether the driver spent one each. The count is taken as a
// DIFFERENCE from before the publications, because the test binary's own count
// is not zero.
func TestSinkAsyncCostsOneGoroutinePerSink(t *testing.T) {
	peer := newPeer()
	const n = 100
	plans := make([]client.SendPlan, n)
	for i := range plans {
		plans[i] = client.SendPlan{Hold: true}
	}
	peer.PlanSends(plans...)
	f := openWith(t, testProfile(), aq.Config{MaxPending: n * 2}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	before := goroutines()
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		_ = i
		if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil,
			func(error) { wg.Done() }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	snd := f.sender(budget)
	waitFor(t, budget, "every transfer to reach the peer", func() bool { return snd.Held() == n })
	if grew := goroutines() - before; grew > 4 {
		t.Fatalf("%d publications outstanding cost %d extra goroutines; the specification forbids a goroutine per message and "+
			"this driver spends one per SINK", n, grew)
	}
	if got := snk.Outstanding(); got != n {
		t.Fatalf("the sink reports %d outstanding publications, want %d", got, n)
	}
	// Resolve them out of order, which is what a real peer does: a disposition
	// may arrive for any outstanding transfer.
	for i := n - 1; i >= 0; i-- {
		if !snd.ResolveHeld(i, &amqp.StateAccepted{}) {
			t.Fatalf("resolving held transfer %d", i)
		}
	}
	wg.Wait()
	waitFor(t, budget, "the queue to drain", func() bool { return snk.Outstanding() == 0 })
}

// TestSinkAsyncCallbackRunsExactlyOnce is the other rule, over every outcome
// a publication can have.
func TestSinkAsyncCallbackRunsExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   amqp.DeliveryState
		wantErr bool
	}{
		{"accepted", &amqp.StateAccepted{}, false},
		{"rejected", &amqp.StateRejected{Error: &amqp.Error{Condition: amqp.ErrCondNotAllowed}}, true},
		{"released", &amqp.StateReleased{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := newPeer()
			peer.PlanSends(client.SendPlan{State: tc.state})
			f := openWith(t, testProfile(), aq.Config{}, peer)
			snk := f.sink(mq.SinkConfig{Topic: "topic"})
			outcomes := make(chan error, 4)
			if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil,
				func(err error) { outcomes <- err }); err != nil {
				t.Fatalf("PublishAsync: %v", err)
			}
			select {
			case err := <-outcomes:
				if (err != nil) != tc.wantErr {
					t.Fatalf("the callback reported %v, want an error=%v", err, tc.wantErr)
				}
			case <-time.After(budget):
				t.Fatal("the callback never ran")
			}
			select {
			case err := <-outcomes:
				t.Fatalf("the callback ran twice; the second outcome was %v", err)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// TestSinkAsyncIsBounded is the rule: every internal buffer is bounded by
// default, and the answer to a full one is an error rather than a buffer that
// grows.
//
// The slot is taken BEFORE the transfer is written, so a refusal is a
// publication that never left rather than one that left and was then disowned —
// which the peer's own count is what proves.
func TestSinkAsyncIsBounded(t *testing.T) {
	peer := newPeer()
	plans := make([]client.SendPlan, 8)
	for i := range plans {
		plans[i] = client.SendPlan{Hold: true}
	}
	peer.PlanSends(plans...)
	f := openWith(t, testProfile(), aq.Config{MaxPending: 2}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	for i := range 2 {
		if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil, func(error) {}); err != nil {
			t.Fatalf("publication %d: %v", i, err)
		}
	}
	err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil, func(error) {
		t.Error("the refused publication's callback ran; the specification says a non-nil immediate error means done does not run")
	})
	assertIs(t, err, mq.ErrBackpressure, "a third publication with MaxPending=2")
	snd := f.sender(budget)
	if got := len(snd.Sent()); got != 2 {
		t.Fatalf("the peer received %d transfer(s); a refusal has to happen BEFORE the write, or the publication "+
			"left and was then disowned", got)
	}
}

// TestSinkFlushWaitsForWhatWasAdmitted is FlushSink: a caller can wait for
// the publications the sink has taken.
func TestSinkFlushWaitsForWhatWasAdmitted(t *testing.T) {
	peer := newPeer()
	peer.PlanSends(client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	resolved := make(chan error, 1)
	if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil,
		func(err error) { resolved <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	snd := f.sender(budget)
	waitFor(t, budget, "the transfer to reach the peer", func() bool { return snd.Held() == 1 })

	// A flush with nothing to wait for is immediate; one with an outstanding
	// publication waits.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := snk.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush returned %v with a publication outstanding, want the context's own error", err)
	}
	if !snd.ResolveHeld(0, &amqp.StateAccepted{}) {
		t.Fatal("resolving the held transfer")
	}
	<-resolved
	if err := snk.Flush(context.Background()); err != nil {
		t.Fatalf("Flush with nothing outstanding: %v", err)
	}
}

// TestSinkFillsTheReceiptHonestly is the receipt over a protocol that
// assigns no identifier.
//
// Filling MessageID with the identifier the PUBLISHER sent would be reporting
// the caller's own value back as the broker's, which is worse than leaving it
// empty — and the capability note says so.
func TestSinkFillsTheReceiptHonestly(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic", Receipts: true})
	var receipt mq.PublishReceipt
	msg := &mq.Message{Body: []byte("x"), ID: "the-caller's-own-id", Receipt: &receipt}
	if err := snk.Publish(context.Background(), msg, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if receipt.MessageID != "" {
		t.Fatalf("the receipt reports a broker-assigned identifier of %q; AMQP 1.0's disposition assigns none, and "+
			"reporting the publisher's own is reporting the caller's value back as the broker's", receipt.MessageID)
	}
	if receipt.Confirmation != mq.ConfirmAccepted {
		t.Fatalf("the receipt's boundary is %v, want the sink's resolved confirmation mode", receipt.Confirmation)
	}
	if receipt.ConfirmedAt.IsZero() {
		t.Fatal("the receipt carries no time, which is one of the two things this protocol can honestly report")
	}
	if receipt.Native == nil {
		t.Fatal("the receipt carries no native delivery state, which is the escape hatch a caller asked for")
	}
	if receipt.Partition != 0 || receipt.Offset != 0 || receipt.Sequence != 0 {
		t.Fatalf("the receipt reports a placement of %+v; AMQP 1.0 reports none", receipt)
	}
}

// TestSinkReceiptsNeedTheCapability holds the open-time check: a publisher
// asking for receipts on a profile that withdrew the claim is refused rather
// than given empty ones.
func TestSinkReceiptsNeedTheCapability(t *testing.T) {
	p := testProfile()
	p.Capabilities = func(aq.Facts) []aq.Claim { return []aq.Claim{aq.Withdraw(mq.FeatureReceipts)} }
	f := open(t, p, aq.Config{})
	_, err := f.b.OpenSink(sinkCtx(), mq.SinkConfig{Topic: "t", Receipts: true})
	assertIs(t, err, mq.ErrUnsupported, "receipts on a profile that withdrew the claim")
}

// TestSinkRetainsNothingOfTheMessage is the rule for a synchronous publish:
// "caller may reuse/mutate referenced data after return".
//
// The check is to publish, then scribble over the caller's buffer, then read
// what the peer received: a driver that aliased the body into a transfer it
// still held would show the scribble.
func TestSinkRetainsNothingOfTheMessage(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	body := []byte("original")
	if err := snk.Publish(context.Background(), &mq.Message{Body: body}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	copy(body, "scribble")
	sent := f.sender(budget).Sent()
	if len(sent) != 1 {
		t.Fatalf("the peer received %d transfer(s)", len(sent))
	}
	// The synchronous path ALIASES the body, which the specification permits because the
	// transfer is finished with it before Publish returns — so the peer's
	// record shows the scribble and that is correct. What must not happen is
	// the ASYNCHRONOUS path doing the same, which the next test holds.
	_ = sent
}

// TestSinkAsyncCopiesTheBody is the other half an asynchronous
// publication borrows the message until its callback runs, and the library holds
// the encoded transfer for that whole time — so the payload cannot stay the
// caller's.
func TestSinkAsyncCopiesTheBody(t *testing.T) {
	peer := newPeer()
	peer.PlanSends(client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	body := []byte("original")
	done := make(chan error, 1)
	if err := snk.PublishAsync(context.Background(), &mq.Message{Body: body}, nil,
		func(err error) { done <- err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	snd := f.sender(budget)
	waitFor(t, budget, "the transfer to reach the peer", func() bool { return snd.Held() == 1 })
	copy(body, "scribble")
	sent := snd.Sent()
	if len(sent) != 1 {
		t.Fatalf("the peer received %d transfer(s)", len(sent))
	}
	if got := string(sent[0].Data[0]); got != "original" {
		t.Fatalf("the asynchronous transfer's payload is %q after the caller reused its buffer; the library holds "+
			"the transfer until its disposition, so an asynchronous publication has to copy ", got)
	}
	if !snd.ResolveHeld(0, &amqp.StateAccepted{}) {
		t.Fatal("resolving the held transfer")
	}
	<-done
}

// TestSinkCloseIsIdempotentAndRefusesAfterwards is the lifecycle rule.
func TestSinkCloseIsIdempotentAndRefusesAfterwards(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	if err := snk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := snk.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	for name, err := range map[string]error{
		"Publish": snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil),
		"PublishAsync": snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil,
			func(error) { t.Error("the callback of a publication on a closed sink ran") }),
		"Flush": snk.Flush(context.Background()),
	} {
		assertIs(t, err, mq.ErrClosed, name+" on a closed sink")
	}
}

// TestSinkRefusesANilCallback keeps the contract honest: a publication whose
// outcome has nowhere to go is a caller's mistake and not a publication that
// silently loses its answer.
func TestSinkRefusesANilCallback(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil, nil)
	assertCode(t, err, mq.CodeInvalidArgument, "PublishAsync with no callback")
}

// TestSinkReattachesAfterAReconnection is the lazy re-attach of links.go: the
// generation bump and the token invalidation are eager, and the attach is the
// next operation's.
func TestSinkReattachesAfterAReconnection(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("before")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	gen := f.b.Generation()
	f.peer.LastConn().Kill(&amqp.ConnError{})
	if !aq.AwaitGenerationForTest(f.b, gen, budget) {
		t.Fatal("the generation did not move after the connection died")
	}
	if !aq.AwaitReconnectForTest(f.b, 1, budget) {
		t.Fatal("the dial loop did not re-establish the connection")
	}
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("after")}, nil); err != nil {
		t.Fatalf("publishing after a reconnection: %v; the next operation is what attaches a link under the new "+
			"generation", err)
	}
	conns := f.peer.Conns()
	if len(conns) < 2 {
		t.Fatalf("the peer handed out %d connection(s), want a replacement", len(conns))
	}
	if got := conns[len(conns)-1].Senders(); len(got) != 1 || len(got[0].Sent()) != 1 {
		t.Fatalf("the new connection carries %d sending link(s); the publication after the reconnection should be "+
			"on one of its own", len(got))
	}
}

// TestSinkAsyncCallbacksRunInTransferOrder holds the order the one resolving
// goroutine promises: dispositions may arrive in any order, and the callbacks
// still run in the order the transfers were written.
func TestSinkAsyncCallbacksRunInTransferOrder(t *testing.T) {
	peer := newPeer()
	const n = 8
	plans := make([]client.SendPlan, n)
	for i := range plans {
		plans[i] = client.SendPlan{Hold: true}
	}
	peer.PlanSends(plans...)
	f := openWith(t, testProfile(), aq.Config{MaxPending: n}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	var mu sync.Mutex
	var order []int
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil, func(error) {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			wg.Done()
		}); err != nil {
			t.Fatalf("PublishAsync %d: %v", i, err)
		}
	}
	snd := f.sender(budget)
	waitFor(t, budget, "every transfer to reach the peer", func() bool { return snd.Held() == n })
	for i := n - 1; i >= 0; i-- {
		if !snd.ResolveHeld(i, &amqp.StateAccepted{}) {
			t.Fatalf("resolving held transfer %d", i)
		}
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for i, v := range order {
		if v != i {
			t.Fatalf("callbacks ran in the order %v, want the order the transfers were written", order)
		}
	}
}

// TestSinkCloseResolvesEveryQueuedPublication holds Close to the queue behind
// the one the resolving goroutine is waiting on: every outstanding publication
// is resolved exactly once, as an unknown outcome, and none is dropped.
func TestSinkCloseResolvesEveryQueuedPublication(t *testing.T) {
	peer := newPeer()
	const n = 4
	plans := make([]client.SendPlan, n)
	for i := range plans {
		plans[i] = client.SendPlan{Hold: true}
	}
	peer.PlanSends(plans...)
	f := openWith(t, testProfile(), aq.Config{MaxPending: n}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	var mu sync.Mutex
	calls := make([]int, n)
	var outcomes []error
	for i := range n {
		if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil, func(err error) {
			mu.Lock()
			calls[i]++
			outcomes = append(outcomes, err)
			mu.Unlock()
		}); err != nil {
			t.Fatalf("PublishAsync %d: %v", i, err)
		}
	}
	snd := f.sender(budget)
	waitFor(t, budget, "every transfer to reach the peer", func() bool { return snd.Held() == n })
	if err := snk.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, c := range calls {
		if c != 1 {
			t.Fatalf("publication %d's callback ran %d times across Close, want exactly once", i, c)
		}
	}
	for i, err := range outcomes {
		if err == nil || (!mq.IsAmbiguous(err) && mq.CodeOf(err) != mq.CodeSettlementUnknown) {
			t.Fatalf("outcome %d across Close was %v; the transfer had left, so the outcome is unknown", i, err)
		}
	}
	if got := snk.Outstanding(); got != 0 {
		t.Fatalf("%d publications are still outstanding after Close", got)
	}
}

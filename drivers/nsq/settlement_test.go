package nsq_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nsq"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
)

// publishOne publishes one body and returns the delivery it becomes.
func publishOne(t *testing.T, b *nq.Backend, src mq.Source, topic, body string) mq.DriverDelivery {
	t.Helper()
	snk := sinkOf(t, b, topic, mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte(body)}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return fetchOne(t, src)
}

// TestAckIsFinishAndPreventsRedelivery is the positive settlement: FIN, and
// then the message is gone.
func TestAckIsFinishAndPreventsRedelivery(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	d := publishOne(t, b, src, "orders", "one")
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	st := server.Stats("orders", "workers")
	if st.Finished != 1 || st.InFlight != 0 || st.Depth != 0 {
		t.Fatalf("after the acknowledgement the broker reports %+v", st)
	}
	// A lease that has run out changes nothing: the message is finished.
	server.Advance(time.Hour)
	if st := server.Stats("orders", "workers"); st.Depth != 0 || st.InFlight != 0 {
		t.Fatalf("an acknowledged message came back: %+v", st)
	}
}

// TestASecondSettlementIsRefused is the idempotency rule as one source sees
// it, and the reason the state machine exists at all.
func TestASecondSettlementIsRefused(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	d := publishOne(t, b, src, "orders", "one")
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("the second Ack answered %v, want ErrAlreadySettled", err)
	}
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{}); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("a Nack after an Ack answered %v, want ErrAlreadySettled", err)
	}
	if n := server.Stats("orders", "workers").Finished; n != 1 {
		t.Fatalf("the broker saw %d finishes for one message; a refused settlement must write nothing", n)
	}
}

// TestNackRequeuesWithNSQsOwnDelay is the negative settlement: REQ, with
// nsqd's own delayed redelivery and not an in-process timer.
func TestNackRequeuesWithNSQsOwnDelay(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	d := publishOne(t, b, src, "orders", "one")
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: time.Minute}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	st := server.Stats("orders", "workers")
	if st.Deferred != 1 || st.Depth != 0 {
		t.Fatalf("a delayed requeue is %+v; the delay must be nsqd's, not a local timer", st)
	}
	if st.BackoffAsked != 0 {
		t.Fatalf("the requeue asked go-nsq for its own backoff %d time(s); that sets RDY to zero for the "+
			"whole subscription, which makes the core's retry schedule a fiction ", st.BackoffAsked)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a delayed requeue came back early: n=%d err=%v", n, err)
	}
	server.Advance(2 * time.Minute)
	again := fetchOne(t, src)
	if again.Info.Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d", again.Info.Attempt)
	}
	if err := src.Settler().Ack(context.Background(), &again); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestNackDropIsFinishAndNackDeadLetterIsRefused holds the three actions to
// what nsqd has.
//
// Drop is FIN, which really does destroy the message — nsqd has no other way
// to make one go away. DeadLetter has no native route at all, so it is refused
// with ErrUnsupported rather than emulated inside the driver: the specification forbids the
// silent emulation, and the core's own mq.WithDeadLetter publishes and then
// acknowledges, which needs nothing from here.
func TestNackDropIsFinishAndNackDeadLetterIsRefused(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)

	d := publishOne(t, b, src, "orders", "drop me")
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDrop}); err != nil {
		t.Fatalf("Nack(drop): %v", err)
	}
	if st := server.Stats("orders", "workers"); st.Finished != 1 || st.Depth != 0 {
		t.Fatalf("a dropped message left the broker at %+v", st)
	}

	d2 := publishOne(t, b, src, "orders", "dead letter me")
	err := src.Settler().Nack(context.Background(), &d2, mq.NackOptions{Action: mq.NackActionDeadLetter})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Nack(dead letter) answered %v, want ErrUnsupported", err)
	}
	if st := server.Stats("orders", "workers"); st.InFlight != 1 {
		t.Fatalf("a refused settlement changed the broker's state: %+v; the delivery must stay settleable", st)
	}
	if err := src.Settler().Ack(context.Background(), &d2); err != nil {
		t.Fatalf("a refused Nack left the delivery unsettleable: %v", err)
	}
}

// TestTouchExtendsTheLeaseAndIsNotASettlement is the rule that a lease
// extension is not a settlement.
//
// A TOUCH moves the deadline and leaves the delivery settleable, which is why
// the settler has a peek beside its claim. A driver whose Extend took the
// delivery's state would make the acknowledgement that follows it fail with
// ErrAlreadySettled — the message would then be redelivered and processed
// twice, for no reason but the extension that was supposed to prevent it.
func TestTouchExtendsTheLeaseAndIsNotASettlement(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	ext, ok := src.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the settler does not implement DeadlineExtender; TOUCH is NSQ's lease extension")
	}
	d := publishOne(t, b, src, "orders", "slow")

	// Two thirds of the way through the lease, extend it.
	server.Advance(20 * time.Second)
	if err := ext.Extend(context.Background(), &d, 30*time.Second); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if st := server.Stats("orders", "workers"); st.Touched != 1 {
		t.Fatalf("the broker saw %d touches", st.Touched)
	}
	// Past the ORIGINAL deadline, and still ours.
	server.Advance(20 * time.Second)
	if st := server.Stats("orders", "workers"); st.InFlight != 1 || st.Depth != 0 {
		t.Fatalf("the extension did not hold: %+v", st)
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack after an Extend answered %v; an extension is not a settlement ", err)
	}
}

// TestATouchOnALeaseAnotherConsumerNowHoldsIsRefused is this increment's
// second trap, and the disqualifier in NSQ's clothing.
//
// nsqd's TOUCH names a message IDENTIFIER and is answered by nothing. After a
// redelivery that identifier belongs to somebody else's delivery, and nsqd
// applies the TOUCH to it — so a driver that wrote the frame would extend
// another consumer's lease and report success to a caller whose own lease is
// long gone. There is no verb that asks "do I still hold this?", so the refusal
// has to happen locally, before the frame is written, and NOTHING may be
// written when it does.
//
// The model counts the crime as well as refusing to let the driver commit it:
// CrossTouched is how many touches landed on a later delivery than the one that
// sent them, and it has to be zero.
func TestATouchOnALeaseAnotherConsumerNowHoldsIsRefused(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	ext := src.Settler().(mq.DeadlineExtender)
	first := publishOne(t, b, src, "orders", "slow")

	// The lease runs out and nsqd hands the message to somebody else. The
	// handle this caller holds is still perfectly valid as far as nsqd is
	// concerned, which is the whole problem.
	server.Advance(45 * time.Second)
	second := fetchOne(t, src)
	if second.Info.Attempt != 2 {
		t.Fatalf("the second delivery reports attempt %d; the test needs a real redelivery", second.Info.Attempt)
	}

	err := ext.Extend(context.Background(), &first, 30*time.Second)
	if err == nil {
		t.Fatal("a Touch for a lease another consumer now holds succeeded silently; " +
			"it extended the OTHER delivery's lease and told this caller its own was safe")
	}
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("the refusal is %v, want ErrLeaseLost", err)
	}
	st := server.Stats("orders", "workers")
	if st.CrossTouched != 0 {
		t.Fatalf("%d touch(es) landed on a later delivery than the one that sent them; "+
			"the refusal must write NOTHING", st.CrossTouched)
	}
	if st.Touched != 0 {
		t.Fatalf("the broker saw %d touches; a refused extension writes no frame", st.Touched)
	}
	if n := b.Stale(); n == 0 {
		t.Fatal("the driver's stale counter is zero, so the local lease check is not what refused")
	}
	if err := src.Settler().Ack(context.Background(), &second); err != nil {
		t.Fatalf("Ack of the live delivery: %v", err)
	}
}

// TestAnAckForALeaseAnotherConsumerNowHoldsIsRefused is the same disqualifier
// through the settlement verb rather than the extension.
//
// nsqd accepts a FIN by message identifier, so a stale FIN finishes whatever
// is now under it — the specification's "lets a stale ack settle a later redelivery",
// which is a disqualifier and not a rough edge. CrossSettled is the model's
// count of it having happened, and it has to be zero.
func TestAnAckForALeaseAnotherConsumerNowHoldsIsRefused(t *testing.T) {
	for name, lose := range map[string]func(*nsqfake.Server){
		"the lease expired":        func(s *nsqfake.Server) { s.Advance(45 * time.Second) },
		"the connection went away": func(s *nsqfake.Server) { s.Redeliver("orders", "workers") },
	} {
		t.Run(name, func(t *testing.T) {
			b, server := newBackendRig(t)
			src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
			first := publishOne(t, b, src, "orders", "one")
			lose(server)
			second := fetchOne(t, src)

			for verb, settle := range map[string]func(*mq.DriverDelivery) error{
				"Ack":  func(d *mq.DriverDelivery) error { return src.Settler().Ack(context.Background(), d) },
				"Nack": func(d *mq.DriverDelivery) error { return src.Settler().Nack(context.Background(), d, mq.NackOptions{}) },
			} {
				if err := settle(&first); !errors.Is(err, mq.ErrLeaseLost) {
					t.Fatalf("%s of a superseded delivery answered %v, want ErrLeaseLost", verb, err)
				}
			}
			st := server.Stats("orders", "workers")
			if st.CrossSettled != 0 {
				t.Fatalf("%d settlement(s) landed on a later delivery than the one that sent them: "+
					"the specification's 'lets a stale ack settle a later redelivery'", st.CrossSettled)
			}
			if err := src.Settler().Ack(context.Background(), &second); err != nil {
				t.Fatalf("Ack of the live delivery: %v", err)
			}
			if st := server.Stats("orders", "workers"); st.Finished != 1 {
				t.Fatalf("the broker finished %d messages for one publication", st.Finished)
			}
		})
	}
}

// TestASettlementFromAnotherSourceIsNotSettleable is the answer for a
// delivery that never came from here — another source's, or a zero value.
func TestASettlementFromAnotherSourceIsNotSettleable(t *testing.T) {
	b, _ := newBackendRig(t)
	a := sourceOf(t, b, "orders", "a", mq.MetadataNativeStrict, 4)
	other := sourceOf(t, b, "orders", "b", mq.MetadataNativeStrict, 4)
	d := publishOne(t, b, a, "orders", "one")

	if err := other.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrNotSettleable) {
		t.Fatalf("another source's Ack answered %v, want ErrNotSettleable", err)
	}
	var zero mq.DriverDelivery
	if err := a.Settler().Ack(context.Background(), &zero); !errors.Is(err, mq.ErrNotSettleable) {
		t.Fatalf("a zero delivery answered %v, want ErrNotSettleable", err)
	}
	if err := a.Settler().Ack(context.Background(), nil); !errors.Is(err, mq.ErrNotSettleable) {
		t.Fatalf("a nil delivery answered %v, want ErrNotSettleable", err)
	}
	// A delivery whose generation was tampered with is one this source
	// cannot vouch for: the specification makes Gen what detects it.
	tampered := d
	tampered.Gen++
	if err := a.Settler().Ack(context.Background(), &tampered); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a tampered generation answered %v, want ErrLeaseLost", err)
	}
	if err := a.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("the real Ack: %v", err)
	}
}

// TestExtendRefusesWhatNSQsTouchCannotDo holds the extension to nsqd's own
// shape.
//
// nsqd's TOUCH resets a message's timeout to the CHANNEL's negotiated value —
// it is a reset and not an addition — and nsqd caps a message's total in-flight
// time at --max-msg-timeout from its first delivery. So the bound on `by` is
// that cap and not the channel's timeout: an extension shorter than the cap is
// granted, and what it BUYS is the channel's timeout from now rather than the
// amount asked for. An extension of nothing is a mistake
// rather than a request, and the specification's own probe checks it.
func TestExtendRefusesWhatNSQsTouchCannotDo(t *testing.T) {
	b, _ := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	ext := src.Settler().(mq.DeadlineExtender)
	d := publishOne(t, b, src, "orders", "one")
	// An extension longer than the channel's own timeout is GRANTED: nsqd
	// takes the TOUCH and moves the deadline by what it can.
	if err := ext.Extend(context.Background(), &d, 10*nq.DefaultAckDeadline); err != nil {
		t.Fatalf("Extend past the channel timeout answered %v; nsqd takes the TOUCH and the "+
			"capability table is what says how much it buys", err)
	}
	for name, by := range map[string]time.Duration{
		"zero":                       0,
		"negative":                   -time.Second,
		"longer than nsqd's maximum": 2 * nq.DefaultMaxAckDeadline,
	} {
		t.Run(name, func(t *testing.T) {
			if err := ext.Extend(context.Background(), &d, by); mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Fatalf("Extend(%v) answered %v: %v", by, mq.CodeOf(err), err)
			}
		})
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("a refused extension left the delivery unsettleable: %v", err)
	}
}

// TestConcurrentSettlementsSerialise is the specification's "concurrent ack/nack
// serialisation": exactly one of them reaches the broker, and the rest are
// refused rather than racing.
func TestConcurrentSettlementsSerialise(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	d := publishOne(t, b, src, "orders", "one")

	const racers = 16
	var wg sync.WaitGroup
	errs := make([]error, racers)
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				errs[i] = src.Settler().Ack(context.Background(), &d)
				return
			}
			errs[i] = src.Settler().Nack(context.Background(), &d, mq.NackOptions{})
		}()
	}
	wg.Wait()
	won := 0
	for i, err := range errs {
		switch {
		case err == nil:
			won++
		case errors.Is(err, mq.ErrAlreadySettled):
		default:
			t.Errorf("racer %d answered %v, want nil or ErrAlreadySettled", i, err)
		}
	}
	if won != 1 {
		t.Fatalf("%d of %d concurrent settlements succeeded; the specification allows exactly one", won, racers)
	}
	st := server.Stats("orders", "workers")
	if st.Finished+st.Requeued != 1 {
		t.Fatalf("the broker saw %d finishes and %d requeues for one message", st.Finished, st.Requeued)
	}
}

// TestCloseRequeuesWhatItWasHolding is the specification's "Close requeues or abandons
// everything it has handed out".
//
// On this broker it is also what keeps Close prompt: go-nsq's own teardown
// drains a connection that still has messages in flight, for up to thirty
// seconds, so a driver that forgot to requeue would turn every Close into a
// hang the specification forbids.
func TestCloseRequeuesWhatItWasHolding(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 8)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	for i := range 6 {
		if err := snk.Publish(context.Background(), &mq.Message{Body: []byte{byte('a' + i)}}, nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	// Two are handed out and never settled; the rest stay in the ring.
	held := fetchN(t, src, 2)
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	st := server.Stats("orders", "workers")
	if st.InFlight != 0 {
		t.Fatalf("%d message(s) are still in flight at the broker after Close; go-nsq's own teardown "+
			"waits thirty seconds for each of them", st.InFlight)
	}
	if st.Depth+st.Deferred != 6 {
		t.Fatalf("the broker holds %d pending and %d deferred of 6; Close must requeue and not discard",
			st.Depth, st.Deferred)
	}
	// A settlement after Close is refused rather than applied to a message
	// somebody else may now hold.
	if err := src.Settler().Ack(context.Background(), &held[0]); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("an Ack after Close answered %v, want ErrLeaseLost", err)
	}
	if n := server.Stats("orders", "workers").CrossSettled; n != 0 {
		t.Fatalf("%d settlement(s) after Close landed on a later delivery", n)
	}
}

// TestFetchDropsADeliveryThatWentDeadInTheRing is why Fetch filters.
//
// A delivery whose lease ran out while it sat in the ring is one no settlement
// can succeed on. Handing it to a handler would make the work certainly wasted
// and the acknowledgement certainly refused — and nsqd has already given the
// message to somebody else.
func TestFetchDropsADeliveryThatWentDeadInTheRing(t *testing.T) {
	b, server := newBackendRig(t)
	src := sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "orders", mq.MetadataNativeStrict)
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("stale")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Wait until the delivery is in the ring AND the handler has finished
	// recording it. Waiting only for the broker's in-flight count is not
	// enough: the broker counts a message before the handler runs, so an
	// Advance between the two makes the driver record a lease measured from
	// the ADVANCED clock — a live delivery where this test needs a dead one.
	//
	// The wait fails rather than falling through: a test that advanced the
	// clock without having seen the delivery recorded would be racing for
	// the state it asserts on.
	deadline := time.Now().Add(10 * time.Second)
	for server.Stats("orders", "workers").InFlight != 1 || !server.Idle() {
		if time.Now().After(deadline) {
			t.Fatal("the delivery was never in flight with the handler finished, so this test cannot " +
				"make it go dead in the ring")
		}
		time.Sleep(time.Millisecond)
	}
	server.Advance(45 * time.Second)
	second := fetchOne(t, src)
	// At least the second attempt, and possibly a later one: the lease
	// sweeper gives a dead delivery back as well, so the message may have
	// gone round more than once by the time a Fetch returns a live one. What
	// matters is that the DEAD one was never handed out.
	if second.Info.Attempt < 2 {
		t.Fatalf("the delivery Fetch returned reports attempt %d, want a redelivery: "+
			"a dead delivery was handed out", second.Info.Attempt)
	}
	if n := b.Stale(); n == 0 {
		t.Fatal("no delivery was counted stale, so Fetch did not drop anything")
	}
	if err := src.Settler().Ack(context.Background(), &second); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

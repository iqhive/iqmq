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

// TestSinkLateLinkErrorKeepsTheReplacement is a dead link's receipt failing
// after a publication has already attached its replacement.
//
// The resolve loop hears about a detached link's receipts one at a time, and a
// callback that takes a while holds the rest back, so a synchronous
// publication can attach a new link in between. The late failure names the OLD
// link; dropping the new one for it would orphan a healthy link and its
// session, and the next publication would attach a third.
func TestSinkLateLinkErrorKeepsTheReplacement(t *testing.T) {
	peer := newPeer()
	peer.PlanSends(client.SendPlan{Hold: true}, client.SendPlan{Hold: true})
	f := openWith(t, testProfile(), aq.Config{}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})

	firstRan := make(chan struct{})
	letFirstFinish := make(chan struct{})
	second := make(chan error, 1)
	if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("a")}, nil, func(error) {
		close(firstRan)
		<-letFirstFinish
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("b")}, nil, func(err error) {
		second <- err
	}); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	old := f.sender(budget)
	old.Detach(&amqp.LinkError{})
	select {
	case <-firstRan:
	case <-time.After(budget):
		t.Fatal("the first callback never ran")
	}

	// The worker is parked in the first callback with the second receipt's
	// failure still to hear about. A publication now attaches the replacement.
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("c")}, nil); err != nil {
		t.Fatalf("Publish on the replacement link: %v", err)
	}
	close(letFirstFinish)
	select {
	case err := <-second:
		if err == nil {
			t.Fatal("the second publication succeeded on a link that was detached under it")
		}
	case <-time.After(budget):
		t.Fatal("the second callback never ran")
	}

	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("d")}, nil); err != nil {
		t.Fatalf("Publish after the late failure: %v", err)
	}
	conn := f.peer.LastConn()
	senders := conn.Senders()
	if len(senders) != 2 {
		t.Fatalf("the sink attached %d sending links, want 2: the dead link's late failure dropped its healthy "+
			"replacement, which is orphaned with its session", len(senders))
	}
	if got := len(senders[1].Sent()); got != 2 || senders[1].Detached() {
		t.Fatalf("the replacement link carried %d publication(s) and detached=%v, want both later publications "+
			"on a link that is still attached", got, senders[1].Detached())
	}
	if !senderSession(conn, old).Ended() {
		t.Fatal("the detached link's session is still begun; a link detach leaves its session on a connection " +
			"this client keeps, so dropping the link without ending it leaks a channel for the connection's life")
	}
}

// TestSourceLinkLossEndsTheDeadSessionAndKeepsTheDeadLetterLink is a forced
// detach of a source's receiving link on a connection that stays up.
//
// The dead link's session stays begun until somebody ends it, and the
// dead-letter sender is on a session of its own that the detach did not
// touch. Forgetting either without detaching it leaks it for the connection's
// life, and forgetting a working dead-letter sender costs an attach as well.
func TestSourceLinkLossEndsTheDeadSessionAndKeepsTheDeadLetterLink(t *testing.T) {
	p := testProfile()
	p.DeadLetterAddress = func(topic, sub string) (string, error) { return "DLQ." + topic, nil }
	f := open(t, p, aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	dead := mq.NackOptions{Action: mq.NackActionDeadLetter}

	rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")
	if err := src.Settler().Nack(context.Background(), &d, dead); err != nil {
		t.Fatalf("the first dead-letter: %v", err)
	}
	rcv.Detach(&amqp.LinkError{})
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); err == nil {
		t.Fatal("a Fetch on a detached link succeeded")
	}
	conn := f.peer.LastConn()
	if !receiverSession(conn, rcv).Ended() {
		t.Fatal("the detached receiving link's session is still begun after the source dropped the link")
	}

	_, d2 := deliverAndFetch(t, f, src, "tag-2", "y")
	if err := src.Settler().Nack(context.Background(), &d2, dead); err != nil {
		t.Fatalf("the second dead-letter: %v", err)
	}
	var dlq []*client.FakeSender
	for _, s := range conn.Senders() {
		if s.Address() == "DLQ.topic" {
			dlq = append(dlq, s)
		}
	}
	if len(dlq) != 1 || dlq[0].Detached() || len(dlq[0].Sent()) != 2 {
		t.Fatalf("the source used %d dead-letter link(s); want the one working link it had, carrying both copies",
			len(dlq))
	}
}

// TestSettlementInFlightAcrossALinkLossLeavesTheNewWindowAlone is a
// settlement that passed the guard on the old link and failed after the link
// was replaced.
//
// The window was reset when the link went, so the delivery is no longer in
// it. Counting the failure anyway lands on the NEW link's window: under
// `first` it frees a slot a new delivery holds, and the window issues credit
// beyond the prefetch; under `second` it records an unknown outcome the new
// link never had, and enough of them make a healthy source report
// CodeResourceExhausted.
func TestSettlementInFlightAcrossALinkLossLeavesTheNewWindowAlone(t *testing.T) {
	for _, mode := range []aq.ReceiverSettleMode{aq.ReceiverSettleFirst, aq.ReceiverSettleSecond} {
		t.Run(mode.String(), func(t *testing.T) {
			f := open(t, testProfile(), aq.Config{ReceiverSettle: mode})
			src := f.source(mq.SourceConfig{Prefetch: 2})
			rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")

			release := rcv.HoldDispositions()
			defer release()
			acked := make(chan error, 1)
			go func() { acked <- src.Settler().Ack(context.Background(), &d) }()
			waitFor(t, budget, "the settlement to be in flight", func() bool { return rcv.ParkedDispositions() == 1 })

			rcv.Detach(&amqp.LinkError{})
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			if _, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); err == nil {
				t.Fatal("a Fetch on a detached link succeeded")
			}
			_, fresh := deliverAndFetch(t, f, src, "tag-2", "y")

			release()
			if err := <-acked; err == nil {
				t.Fatal("a settlement written to a detached link succeeded")
			}
			stats := src.CreditStats()
			if stats.Held != 1 || stats.Unknown != 0 {
				t.Fatalf("the new link's window is %+v, want the one delivery it holds and no unknown outcome: the "+
					"old link's settlement was counted against it", stats)
			}
			if err := src.Settler().Ack(context.Background(), &fresh); err != nil {
				t.Fatalf("Ack of the new link's delivery: %v", err)
			}
		})
	}
}

// TestSourceHeadersSurviveALinkLoss is a handler still reading a delivery
// when its link is lost.
//
// The loss frees every slot while the handler runs -- its settlement will be
// refused, but nothing stops it reading -- and the core's copy of the message
// shares the slot's header storage. The next delivery decoded into that slot
// must not write through it.
func TestSourceHeadersSurviveALinkLoss(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	withHeader := func(tag, v string) *amqp.Message {
		m := message(tag, "x")
		m.ApplicationProperties = map[string]any{"k": v}
		return m
	}

	var seen []*client.FakeReceiver
	if c := f.peer.LastConn(); c != nil {
		seen = c.Receivers()
	}
	go func() { f.receiverExcept(seen, budget).Deliver(withHeader("tag-1", "first")) }()
	d1 := fetchOne(t, src, budget)
	// What the core hands a handler: the message VALUE, whose header slice is
	// the slot's.
	inHandler := *d1.Message
	if v, ok := inHandler.Headers.Get("k"); !ok || string(v) != "first" {
		t.Fatalf("the first delivery's header is %q, %v", v, ok)
	}

	rcv := f.receiver(budget)
	rcv.Detach(&amqp.LinkError{})
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); err == nil {
		t.Fatal("a Fetch on a detached link succeeded")
	}
	seen = f.peer.LastConn().Receivers()
	go func() { f.receiverExcept(seen, budget).Deliver(withHeader("tag-2", "second")) }()
	d2 := fetchOne(t, src, budget)
	if uint32(d2.Token) != uint32(d1.Token) {
		t.Fatalf("the second delivery took slot %d and the first had %d; the test needs them to share one",
			uint32(d2.Token), uint32(d1.Token))
	}
	if v, ok := inHandler.Headers.Get("k"); !ok || string(v) != "first" {
		t.Fatalf("the first delivery's header now reads %q: the next delivery decoded into the slot overwrote "+
			"storage a handler was still reading", v)
	}
}

// senderSession is the session a sending link was attached on.
func senderSession(conn *client.FakeConn, snd *client.FakeSender) *client.FakeSession {
	for _, sess := range conn.Sessions() {
		for _, s := range sess.Senders() {
			if s == snd {
				return sess
			}
		}
	}
	return nil
}

// receiverSession is the session a receiving link was attached on.
func receiverSession(conn *client.FakeConn, rcv *client.FakeReceiver) *client.FakeSession {
	for _, sess := range conn.Sessions() {
		for _, r := range sess.Receivers() {
			if r == rcv {
				return sess
			}
		}
	}
	return nil
}

// TestSourceReattachesAfterThePeerEndsItsSession is the peer ending the
// SESSION a receiving link is on while the connection stays up.
//
// Every link this driver attaches has a session of its own, so the session's
// end is the link's, and go-amqp reports it with an *amqp.SessionError rather
// than an *amqp.LinkError. Only the second used to drop the link, and the
// connection generation does not move for a session, so the source handed
// back the dead link for ever: every Fetch failed Unavailable until the
// connection happened to drop.
func TestSourceReattachesAfterThePeerEndsItsSession(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	src := f.source(mq.SourceConfig{Prefetch: 4})
	rcv, _ := deliverAndFetch(t, f, src, "tag-1", "x")
	rcv.Detach(&amqp.SessionError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondErrantLink}})
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1)); err == nil {
		t.Fatal("a Fetch on a link whose session ended succeeded")
	}
	if !receiverSession(f.peer.LastConn(), rcv).Ended() {
		t.Error("the dead link's session is still begun after the source dropped the link")
	}
	// deliverAndFetch delivers only on a link the peer did not already have,
	// so it returning at all is the reattachment.
	if again, _ := deliverAndFetch(t, f, src, "tag-2", "y"); again == rcv {
		t.Fatal("the source fetched from the link whose session ended")
	}
}

// TestSinkReattachesAfterThePeerEndsItsSession is the same session end under
// a sending link: the publication that hears of it fails, and the next one
// attaches a new link rather than failing on the dead one.
func TestSinkReattachesAfterThePeerEndsItsSession(t *testing.T) {
	f := open(t, testProfile(), aq.Config{})
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	if err := snk.Publish(sinkCtx(), &mq.Message{Body: []byte("1")}, nil); err != nil {
		t.Fatalf("the first publication: %v", err)
	}
	f.sender(budget).Detach(&amqp.SessionError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondErrantLink}})
	if err := snk.Publish(sinkCtx(), &mq.Message{Body: []byte("2")}, nil); err == nil {
		t.Fatal("a publication on a link whose session ended succeeded")
	}
	if err := snk.Publish(sinkCtx(), &mq.Message{Body: []byte("3")}, nil); err != nil {
		t.Fatalf("the sink never reattached after its session ended: %v", err)
	}
	if n := len(f.peer.LastConn().Senders()); n != 2 {
		t.Fatalf("the peer saw %d sending link(s), want the dead one and its replacement", n)
	}
}

// TestAConditionalDetachWithAFullWindowReattachesTheSource is the peer
// detaching a source's link with a condition while every credit slot is held.
//
// A Fetch with a full window parks on the window until a settlement frees a
// slot, and never reaches the link to find it gone, so the settlements are the
// only calls that hear of the detach. The condition says why the link went (a
// queue deleted, an address not found, a permission withdrawn) and not what
// became of the disposition. Read as the peer's answer to it, it made each
// settlement a definite NotFound or Unauthorized that kept its slot as if the
// delivery could be settled again, and the parked Fetch waited for ever. The
// delivery went back to the node with its link: that is a lost lease under
// `first` and an unknown outcome under `second`, and either way the source
// has to drop the link so that the parked Fetch attaches a new one.
//
// A detach with no condition is the same loss. Under `second` it stalled too:
// each settlement was an unknown outcome that kept its slot, until the window
// held nothing else and every Fetch reported it exhausted.
func TestAConditionalDetachWithAFullWindowReattachesTheSource(t *testing.T) {
	verbs := []struct {
		name   string
		settle func(mq.Settler, *mq.DriverDelivery) error
	}{
		{"ack", func(s mq.Settler, d *mq.DriverDelivery) error { return s.Ack(context.Background(), d) }},
		{"nack", func(s mq.Settler, d *mq.DriverDelivery) error {
			return s.Nack(context.Background(), d, mq.NackOptions{Action: mq.NackActionRequeue})
		}},
	}
	// The empty condition is a detach that carries none.
	conds := []amqp.ErrCond{amqp.ErrCondResourceDeleted, amqp.ErrCondNotFound, amqp.ErrCondUnauthorizedAccess, ""}
	for _, mode := range []aq.ReceiverSettleMode{aq.ReceiverSettleFirst, aq.ReceiverSettleSecond} {
		for _, cond := range conds {
			detach := &amqp.LinkError{}
			name := "no-condition"
			if cond != "" {
				detach.RemoteErr = &amqp.Error{Condition: cond}
				name = string(cond)
			}
			for _, verb := range verbs {
				t.Run(mode.String()+"/"+name+"/"+verb.name, func(t *testing.T) {
					f := open(t, testProfile(), aq.Config{ReceiverSettle: mode})
					src := f.source(mq.SourceConfig{Prefetch: 2})
					rcv, d1 := deliverAndFetch(t, f, src, "tag-1", "x")
					rcv.Deliver(message("tag-2", "y"))
					d2 := fetchOne(t, src, budget)

					// The window is full, so this Fetch parks on it.
					type fetched struct {
						n   int
						d   mq.DriverDelivery
						err error
					}
					parked := make(chan fetched, 1)
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), budget)
						defer cancel()
						dst := make([]mq.DriverDelivery, 1)
						n, err := src.Fetch(ctx, dst)
						parked <- fetched{n, dst[0], err}
					}()

					rcv.Detach(detach)
					err := verb.settle(src.Settler(), &d1)
					if mode == aq.ReceiverSettleSecond {
						assertCode(t, err, mq.CodeSettlementUnknown,
							"a settle-second disposition on a link the peer detached with a condition")
						if !mq.IsAmbiguous(err) {
							t.Fatalf("the unknown settlement is not reported as ambiguous: %v", err)
						}
					}
					if mode == aq.ReceiverSettleFirst {
						assertCode(t, err, mq.CodeLeaseLost,
							"a settle-first disposition on a link the peer detached with a condition")
						if mq.IsAmbiguous(err) {
							t.Fatalf("the lost lease is reported as ambiguous: %v", err)
						}
					}
					if got, ok := aq.Condition(err); cond != "" && (!ok || got != string(cond)) {
						t.Fatalf("the settlement's error carries condition %q (%v), want %q: the reason the "+
							"link went is the caller's diagnosis", got, ok, cond)
					}
					assertIs(t, verb.settle(src.Settler(), &d2), mq.ErrLeaseLost,
						"the other delivery's settlement after its link went")

					f.receiverExcept([]*client.FakeReceiver{rcv}, budget).Deliver(message("tag-3", "z"))
					select {
					case got := <-parked:
						if got.err != nil || got.n != 1 {
							t.Fatalf("the parked Fetch returned %d deliveries and %v, want the delivery on "+
								"the reattached link", got.n, got.err)
						}
						if body := string(got.d.Message.Body); body != "z" {
							t.Fatalf("the parked Fetch returned %q, want the delivery on the reattached link", body)
						}
					case <-time.After(2 * budget):
						t.Fatal("the parked Fetch never returned")
					}
					if stats := src.CreditStats(); stats.Held != 1 || stats.Unknown != 0 {
						t.Fatalf("the reattached link's window is %+v, want the one delivery it holds and no "+
							"unknown outcome", stats)
					}
				})
			}
		}
	}
}

// TestALinkLossFinishesItsInvalidationBeforeAFetchReattaches is a settlement
// dropping a detached link while a Fetch waits on the full credit window.
//
// The invalidation resets the window, and the reset wakes the parked Fetch,
// which attaches a new link and takes a delivery from it. A settlement does not
// hold the lock every fetch runs under, so when it dropped the link and only
// then invalidated, a Fetch woken by another settlement could attach and take a
// delivery in between. The late invalidation then made that live delivery
// stale: its Ack was refused with ErrLeaseLost while the peer kept it
// unsettled, Close did not hand it back, and the window counted nothing held
// while the peer counted one. To an attach, dropping the link and invalidating
// its deliveries have to be one step.
//
// The test stops the settlement inside its invalidation by holding the slot
// records, which go last, and checks that no new link attaches until it lets
// them go.
func TestALinkLossFinishesItsInvalidationBeforeAFetchReattaches(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleFirst})
	src := f.source(mq.SourceConfig{Prefetch: 2})
	rcv, d1 := deliverAndFetch(t, f, src, "tag-1", "x")
	rcv.Deliver(message("tag-2", "y"))
	d2 := fetchOne(t, src, budget)

	// The window is full, so this Fetch parks on it.
	type fetched struct {
		n   int
		d   mq.DriverDelivery
		err error
	}
	parked := make(chan fetched, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		dst := make([]mq.DriverDelivery, 1)
		n, err := src.Fetch(ctx, dst)
		parked <- fetched{n, dst[0], err}
	}()

	letGo := sync.OnceFunc(aq.HoldSlotRecordsForTest(src))
	defer letGo()
	rcv.Detach(&amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}})
	settled := make(chan error, 1)
	go func() { settled <- src.Settler().Ack(context.Background(), &d1) }()

	// The window is reset before the slot records go, so an empty window is a
	// settlement part way through the invalidation, and its reset has woken the
	// parked Fetch.
	waitFor(t, budget, "the settlement to reset the credit window", func() bool {
		return src.CreditStats().Held == 0
	})
	time.Sleep(100 * time.Millisecond)
	for _, r := range f.peer.LastConn().Receivers() {
		if r != rcv {
			t.Fatal("the parked Fetch attached a new link while the settlement that dropped the old one was " +
				"still invalidating its deliveries; the rest of the invalidation would make the new link's " +
				"deliveries stale")
		}
	}
	letGo()

	select {
	case err := <-settled:
		assertCode(t, err, mq.CodeLeaseLost, "the settlement that dropped the link")
	case <-time.After(budget):
		t.Fatal("the settlement that dropped the link never returned")
	}
	assertIs(t, src.Settler().Ack(context.Background(), &d2), mq.ErrLeaseLost,
		"the other delivery's settlement after its link went")

	f.receiverExcept([]*client.FakeReceiver{rcv}, budget).Deliver(message("tag-3", "z"))
	var got fetched
	select {
	case got = <-parked:
	case <-time.After(2 * budget):
		t.Fatal("the parked Fetch never returned")
	}
	if got.err != nil || got.n != 1 || string(got.d.Message.Body) != "z" {
		t.Fatalf("the parked Fetch returned %d deliveries and %v, want the delivery on the reattached link",
			got.n, got.err)
	}
	if stats := src.CreditStats(); stats.Held != 1 {
		t.Fatalf("the reattached link's window is %+v, want the one delivery it holds", stats)
	}
	if err := src.Settler().Ack(context.Background(), &got.d); err != nil {
		t.Fatalf("the Ack of the reattached link's delivery = %v, want nil", err)
	}
}

// TestATopUpForADroppedLinkLeavesTheNewLinkItsCredit is a settlement dropping
// a detached link after a Fetch found the link and before it issued the
// link's credit.
//
// go-amqp's IssueCredit reports nil on a detached link and sends nothing, so
// the resumed top-up booked a whole window of credit on the window the drop
// had reset a moment before. Its fetch then failed on the dead link without
// resetting the window again, because the link was no longer this source's,
// and the reattached link was issued none of the prefetch: every Fetch after
// it waited for a delivery the peer had no credit to send.
func TestATopUpForADroppedLinkLeavesTheNewLinkItsCredit(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleFirst})
	const prefetch = 2
	src := f.source(mq.SourceConfig{Prefetch: prefetch})
	rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")

	topUp, err := aq.TopUpLaterForTest(context.Background(), src, nil)
	if err != nil {
		t.Fatalf("finding the link: %v", err)
	}
	rcv.Detach(&amqp.LinkError{})
	assertCode(t, src.Settler().Ack(context.Background(), &d), mq.CodeLeaseLost,
		"the settlement that dropped the link")
	if err := topUp(); err != nil {
		t.Fatalf("the top-up for the dropped link: %v", err)
	}
	if stats := src.CreditStats(); stats.Issued != 0 {
		t.Fatalf("the window is %+v after a top-up for the link the settlement dropped, want no credit "+
			"outstanding: the dead link will never spend it, and the new link is issued only what is left", stats)
	}

	next, fresh := deliverAndFetch(t, f, src, "tag-2", "y")
	if next == rcv {
		t.Fatal("the source fetched from the link the settlement dropped")
	}
	if got := next.Granted(); int(got) != prefetch {
		t.Errorf("the reattached link was granted %d credit, want the whole prefetch of %d", got, prefetch)
	}
	if err := src.Settler().Ack(context.Background(), &fresh); err != nil {
		t.Fatalf("Ack on the reattached link: %v", err)
	}
}

// TestAFailedTopUpAcrossALinkLossLeavesTheNewWindowAlone is an IssueCredit
// that fails after a settlement dropped the link under it.
//
// A top-up books its credit before it issues it and gives the credit back when
// the issue fails. The drop in between had already reset the window, so the
// give-back took the new window's count below zero, and the reattached link
// was issued that much beyond the prefetch.
func TestAFailedTopUpAcrossALinkLossLeavesTheNewWindowAlone(t *testing.T) {
	f := open(t, testProfile(), aq.Config{ReceiverSettle: aq.ReceiverSettleFirst})
	const prefetch = 3
	src := f.source(mq.SourceConfig{Prefetch: prefetch})
	rcv, d1 := deliverAndFetch(t, f, src, "tag-1", "x")
	rcv.Deliver(message("tag-2", "x"))
	d2 := fetchOne(t, src, budget)
	// One slot freed, so the next top-up has one credit to issue.
	if err := src.Settler().Ack(context.Background(), &d1); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	var dropped error
	topUp, err := aq.TopUpLaterForTest(context.Background(), src, func(uint32) error {
		rcv.Detach(&amqp.LinkError{})
		dropped = src.Settler().Ack(context.Background(), &d2)
		return errors.New("the flow frame was not sent")
	})
	if err != nil {
		t.Fatalf("finding the link: %v", err)
	}
	if err := topUp(); err == nil {
		t.Fatal("the top-up reported success for credit it could not issue")
	}
	assertCode(t, dropped, mq.CodeLeaseLost, "the settlement that dropped the link")
	if stats := src.CreditStats(); stats.Issued != 0 || stats.Held != 0 {
		t.Fatalf("the window is %+v after the failed top-up, want it as the reset left it: the reset had "+
			"already given back the credit the top-up booked", stats)
	}

	next, fresh := deliverAndFetch(t, f, src, "tag-3", "y")
	if got := next.Granted(); int(got) != prefetch {
		t.Errorf("the reattached link was granted %d credit, want the prefetch of %d", got, prefetch)
	}
	if err := src.Settler().Ack(context.Background(), &fresh); err != nil {
		t.Fatalf("Ack on the reattached link: %v", err)
	}
}

// TestABufferedTransferFromADroppedLinkStaysOutOfTheNewWindow is a Fetch
// holding a transfer its link had buffered when a settlement drops the link.
//
// go-amqp hands over what a link buffered even after the link detached, and
// the drop had already reset the window for the link that comes next, so the
// dead link's transfer was counted in the new window and handed out. Its
// settlement could only fail: under `second` that is an unknown outcome, which
// holds its slot in the new window for good, and the reattached link was
// issued one credit short of the prefetch for the rest of its life.
func TestABufferedTransferFromADroppedLinkStaysOutOfTheNewWindow(t *testing.T) {
	for _, mode := range []aq.ReceiverSettleMode{aq.ReceiverSettleFirst, aq.ReceiverSettleSecond} {
		t.Run(mode.String(), func(t *testing.T) {
			peer := newPeer()
			dialer := &pausingDialer{inner: peer}
			b, err := aq.BackendWithDialer(context.Background(), testProfile(), aq.Config{ReceiverSettle: mode}, dialer)
			if err != nil {
				t.Fatalf("opening the backend: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			f := &fixture{tb: t, peer: peer, b: b}
			const prefetch = 2
			src := f.source(mq.SourceConfig{Prefetch: prefetch})
			rcv, d := deliverAndFetch(t, f, src, "tag-1", "x")

			// The next Fetch stops with the link's next transfer in hand and
			// not yet counted.
			paused, resume := dialer.pauseNextReceive()
			defer resume()
			type fetched struct {
				n   int
				d   mq.DriverDelivery
				err error
			}
			out := make(chan fetched, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*budget)
				defer cancel()
				dst := make([]mq.DriverDelivery, 1)
				n, err := src.Fetch(ctx, dst)
				out <- fetched{n, dst[0], err}
			}()
			rcv.Deliver(message("tag-2", "y"))
			select {
			case <-paused:
			case <-time.After(budget):
				t.Fatal("the Fetch never received the buffered transfer")
			}

			rcv.Detach(&amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondResourceDeleted}})
			want := mq.CodeLeaseLost
			if mode == aq.ReceiverSettleSecond {
				want = mq.CodeSettlementUnknown
			}
			assertCode(t, src.Settler().Ack(context.Background(), &d), want, "the settlement that dropped the link")
			resume()

			var next *client.FakeReceiver
			waitFor(t, budget, "the resumed Fetch to return or attach a new link", func() bool {
				if len(out) > 0 {
					return true
				}
				for _, r := range peer.LastConn().Receivers() {
					if r != rcv {
						next = r
						return true
					}
				}
				return false
			})
			if next == nil {
				got := <-out
				if got.err != nil || got.n != 1 {
					t.Fatalf("the resumed Fetch returned %d deliveries and %v without attaching a new link",
						got.n, got.err)
				}
				err := src.Settler().Ack(context.Background(), &got.d)
				t.Fatalf("the resumed Fetch handed out %q from the link the settlement dropped; its Ack reports %v "+
					"and the window is %+v: the reset had sent that delivery back to the node, and counting it "+
					"held a slot of the next link's window", got.d.Message.Body, err, src.CreditStats())
			}
			next.Deliver(message("tag-3", "z"))
			var got fetched
			select {
			case got = <-out:
			case <-time.After(2 * budget):
				t.Fatal("the resumed Fetch never returned")
			}
			if got.err != nil || got.n != 1 || string(got.d.Message.Body) != "z" {
				t.Fatalf("the resumed Fetch returned %d deliveries and %v, want the delivery on the reattached link",
					got.n, got.err)
			}
			if stats := src.CreditStats(); stats.Held != 1 || stats.Unknown != 0 {
				t.Fatalf("the reattached link's window is %+v, want the one delivery it holds and no unknown "+
					"outcome", stats)
			}
			if got := next.Granted(); int(got) != prefetch {
				t.Errorf("the reattached link was granted %d credit, want the whole prefetch of %d", got, prefetch)
			}
			if err := src.Settler().Ack(context.Background(), &got.d); err != nil {
				t.Fatalf("Ack on the reattached link: %v", err)
			}
		})
	}
}

// pausingDialer is a dialler whose receiving links can stop a Receive straight
// after it hands a transfer back, which is where a Fetch holds a delivery the
// credit window has not counted yet.
type pausingDialer struct {
	inner *client.Fake

	mu     sync.Mutex
	paused chan struct{}
	resume chan struct{}
}

// pauseNextReceive arms the pause for the next Receive that returns a
// transfer. It returns a channel that is closed once that Receive has stopped,
// and the function that lets it go, which is harmless to call twice.
func (d *pausingDialer) pauseNextReceive() (<-chan struct{}, func()) {
	paused, resume := make(chan struct{}), make(chan struct{})
	d.mu.Lock()
	d.paused, d.resume = paused, resume
	d.mu.Unlock()
	return paused, sync.OnceFunc(func() { close(resume) })
}

// park stops the calling Receive when the pause is armed, and disarms it.
func (d *pausingDialer) park() {
	d.mu.Lock()
	paused, resume := d.paused, d.resume
	d.paused, d.resume = nil, nil
	d.mu.Unlock()
	if paused == nil {
		return
	}
	close(paused)
	<-resume
}

// Dial dials the scripted peer and wraps the connection.
func (d *pausingDialer) Dial(ctx context.Context, addr string, opts *amqp.ConnOptions) (client.Conn, error) {
	conn, err := d.inner.Dial(ctx, addr, opts)
	if err != nil {
		return nil, err
	}
	return &pausingConn{Conn: conn, d: d}, nil
}

// pausingConn is a connection whose sessions are wrapped.
type pausingConn struct {
	client.Conn
	d *pausingDialer
}

// NewSession begins a session on the peer and wraps it.
func (c *pausingConn) NewSession(ctx context.Context, opts *amqp.SessionOptions) (client.Session, error) {
	sess, err := c.Conn.NewSession(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pausingSession{Session: sess, d: c.d}, nil
}

// pausingSession is a session whose receiving links are wrapped.
type pausingSession struct {
	client.Session
	d *pausingDialer
}

// NewReceiver attaches a receiving link on the peer and wraps it.
func (s *pausingSession) NewReceiver(ctx context.Context, source string, opts *amqp.ReceiverOptions) (client.Receiver, error) {
	rcv, err := s.Session.NewReceiver(ctx, source, opts)
	if err != nil {
		return nil, err
	}
	return &pausingReceiver{Receiver: rcv, d: s.d}, nil
}

// pausingReceiver is a receiving link whose Receive can be stopped.
type pausingReceiver struct {
	client.Receiver
	d *pausingDialer
}

// Receive receives from the peer, then stops if the pause is armed.
func (r *pausingReceiver) Receive(ctx context.Context, opts *amqp.ReceiveOptions) (*amqp.Message, error) {
	m, err := r.Receiver.Receive(ctx, opts)
	if err == nil {
		r.d.park()
	}
	return m, err
}

// compile-time proof that the pausing dialler is the seam's own.
var _ client.Dialer = (*pausingDialer)(nil)

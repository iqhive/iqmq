package amqp10_test

import (
	"context"
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

package stomp

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A requeue resets the subscription: it moves `subGen` on, invalidates every
// token, and sends UNSUBSCRIBE and then a new SUBSCRIBE on the same socket.
// Anything that names a delivery of the old subscription has to be on one side
// of that line. The tests here hold a goroutine at the line it has to be on,
// which the package's external tests cannot reach without a clock.

// requeueBudget is how long a test here waits for another goroutine.
const requeueBudget = 10 * time.Second

// requeueFixture is a server, a backend and one subscribed source, with the
// configuration adjusted by tune. The server's protocol violations fail the
// test in its cleanup, as the external tests' `newServer` does: a settlement
// naming a delivery the server has taken back is one of them.
func requeueFixture(t *testing.T, tune func(*Config)) (*stompsrv.Server, *Backend, *Source) {
	t.Helper()
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() {
		_ = srv.Close()
		for _, v := range srv.Violations() {
			t.Errorf("the driver committed a STOMP protocol violation: %v", v)
		}
	})
	cfg := Config{
		Broker:         activemq.Artemis,
		Addrs:          []string{srv.Addr()},
		ReceiptTimeout: requeueBudget,
		DialTimeout:    requeueBudget,
		NoReconnect:    true,
	}
	if tune != nil {
		tune(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	opened, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	s, ok := opened.(*Source)
	if !ok {
		t.Fatalf("OpenSource returned %T", opened)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := srv.WaitSubscribed(s.Destination(), 1, requeueBudget); err != nil {
		t.Fatalf("the driver never subscribed: %v", err)
	}
	return srv, b, s
}

// fetchAll fetches exactly n deliveries.
func fetchAll(t *testing.T, s *Source, n int) []mq.DriverDelivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	dst := make([]mq.DriverDelivery, n)
	for got := 0; got < n; {
		k, err := s.Fetch(ctx, dst[got:])
		if err != nil {
			t.Fatalf("Fetch after %d of %d deliveries: %v", got, n, err)
		}
		got += k
	}
	return dst
}

// requeueOptions is Nack(NackRequeue()).
func requeueOptions(t *testing.T) mq.NackOptions {
	t.Helper()
	var o mq.NackOptions
	if err := o.Apply(mq.NackRequeue()); err != nil {
		t.Fatalf("NackOptions.Apply: %v", err)
	}
	return o
}

// ackedBeforeUnsubscribe reports whether the server read an ACK naming id
// before its first UNSUBSCRIBE.
func ackedBeforeUnsubscribe(srv *stompsrv.Server, id string) bool {
	ack, unsub := -1, -1
	for i, f := range srv.Frames() {
		switch {
		case f.Command == frame.ACK && f.Get(frame.Id) == id && ack < 0:
			ack = i
		case f.Command == frame.UNSUBSCRIBE && unsub < 0:
			unsub = i
		}
	}
	return ack >= 0 && (unsub < 0 || ack < unsub)
}

// requeueFirst is the dead-letter sink with a requeue landed inside its
// publication, which is the copy's broker round trip that [Source.giveBack]
// waits on before it acknowledges the original.
type requeueFirst struct {
	mq.Sink
	requeue   func()
	published chan struct{}
}

// Publish runs the requeue and then publishes.
func (r *requeueFirst) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	r.requeue()
	defer close(r.published)
	return r.Sink.Publish(ctx, m, o)
}

// TestAGiveBackARequeueOvertakesSendsNoAck is the pump's own settlement of a
// delivery it cannot decode, with a requeue of another delivery landing while
// the dead-letter copy is on its way.
//
// [Source.giveBack] publishes the copy, waits for its receipt and then
// acknowledges the original. That ACK skipped the check [settler.write] makes,
// so it went out after the requeue's UNSUBSCRIBE and new SUBSCRIBE, on the same
// socket, naming a delivery of a subscription the broker had already taken
// back. No broker's answer to that frame has been measured: it may be
// dropped, refused with an ERROR that costs the connection, or applied to the
// redelivered copy. Refused here, the original comes back with the reset and
// is dead-lettered again, which is the cost the settler's own dead-letter
// route already states, and the refusal counts as a stale settlement.
func TestAGiveBackARequeueOvertakesSendsNoAck(t *testing.T) {
	t.Parallel()
	srv, b, s := requeueFixture(t, func(cfg *Config) {
		cfg.Metadata = mq.MetadataPortableEnvelope
		cfg.DeadLetterTopic = "poison"
	})
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()

	// The handler's delivery has to decode, and under the portable envelope
	// only an envelope does, so its body is one this client's own sink wrote.
	snk, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "elsewhere", Confirmation: mq.ConfirmAccepted})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("one")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	sends := srv.FramesOf(frame.SEND)
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: sends[len(sends)-1].Body}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	held := fetchAll(t, s, 1)

	dl, err := s.deadLetterSink(ctx)
	if err != nil {
		t.Fatalf("opening the dead-letter sink: %v", err)
	}
	requeue := requeueOptions(t)
	requeued := make(chan error, 1)
	hook := &requeueFirst{Sink: dl, published: make(chan struct{}), requeue: func() {
		requeued <- s.set.Nack(ctx, &held[0], requeue)
	}}
	s.dlmu.Lock()
	s.dl = hook
	s.dlmu.Unlock()
	// The pump that will read the undecodable delivery. It stops once its
	// subscription has gone, which is after giveBack has returned.
	s.mu.Lock()
	pumpDone := s.pumpDone
	s.mu.Unlock()

	ids, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("IQMQ\x01\xff\xff\xff\xff")})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	select {
	case <-hook.published:
	case <-time.After(requeueBudget):
		t.Fatalf("the undecodable delivery's dead-letter copy was not published within %v\nframes:%s",
			requeueBudget, frameLogOf(srv))
	}
	if err := <-requeued; err != nil {
		t.Fatalf("Nack(NackRequeue()) of the other delivery: %v", err)
	}
	select {
	case <-pumpDone:
	case <-time.After(requeueBudget):
		t.Fatalf("the retired subscription's pump did not stop within %v\nframes:%s", requeueBudget, frameLogOf(srv))
	}
	// Close sends its own frames after every one the pump sent.
	_ = s.Close()

	if got := s.Resets(); got != 1 {
		t.Fatalf("the requeue ran %d time(s), want once", got)
	}
	if slices.Contains(srv.SettledIDs(frame.ACK), ids[0]) {
		t.Errorf("the undecodable delivery %s was acknowledged after the requeue had reset the subscription it "+
			"came from: the ACK names a delivery the broker has taken back\nframes:%s", ids[0], frameLogOf(srv))
	}
	if got := s.StaleSettlements(); got != 1 {
		t.Errorf("Source.StaleSettlements() is %d, want 1: the give-back's ACK was a settlement refused because "+
			"its subscription had gone", got)
	}
}

// TestOnlyTheFirstOfTwoConcurrentRequeuesResets is two handlers requeueing two
// deliveries of one subscription at the same moment.
//
// Both were checked against the subscription before either reset it, and the
// reset did not ask again: the second reset the subscription the first had
// just made, so every delivery handed out in between lost its lease after its
// handler had run and was processed a second time, and both requeues reported
// success. The first reset is the requeue of both deliveries, so the second is
// refused with a lost lease, which is what a settlement the reset overtook
// gets everywhere else.
//
// It reproduced in about one run in four with the two requeues racing. Here
// the test holds subMu, the first lock a reset takes, until both requeues have
// claimed their slots, which is the interleaving the race found, every time.
func TestOnlyTheFirstOfTwoConcurrentRequeuesResets(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixture(t, nil)
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	held := fetchAll(t, s, 2)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()

	requeue := requeueOptions(t)
	s.subMu.Lock()
	errs := make(chan error, len(held))
	for i := range held {
		go func() { errs <- s.set.Nack(ctx, &held[i], requeue) }()
	}
	// A claimed slot is a requeue past its check.
	deadline := time.Now().Add(requeueBudget)
	for !settling(s.table, held) {
		if time.Now().After(deadline) {
			s.subMu.Unlock()
			t.Fatal("the two requeues never both claimed their slots")
		}
		time.Sleep(time.Millisecond)
	}
	s.subMu.Unlock()

	ok, lost := 0, 0
	for range held {
		switch err := <-errs; {
		case err == nil:
			ok++
		case errors.Is(err, mq.ErrLeaseLost):
			lost++
		default:
			t.Errorf("a requeue returned %v", err)
		}
	}
	if ok != 1 || lost != 1 || s.Resets() != 1 {
		t.Errorf("two concurrent requeues reported %d success(es) and %d lost lease(s) and ran %d reset(s), want "+
			"one of each and one reset: a second reset retires the subscription the first one made\nframes:%s",
			ok, lost, s.Resets(), frameLogOf(srv))
	}
	if got := s.StaleSettlements(); got != 1 {
		t.Errorf("Source.StaleSettlements() is %d, want 1 for the refused requeue", got)
	}
}

// settling reports whether every delivery's slot is claimed by a settlement.
func settling(table *driverkit.TokenTable, held []mq.DriverDelivery) bool {
	for i := range held {
		if st, err := table.State(held[i].Token); err != nil || st != driverkit.StateSettling {
			return false
		}
	}
	return true
}

// TestARequeueCannotMoveTheSubscriptionWhileASettlementFrameIsWritten is the
// half of the staleness guard that the external tests cannot reach: the lock
// [settler.write] holds from its check to its frame.
//
// The check without the lock is not enough: a requeue that lands after the
// check and before the frame puts its UNSUBSCRIBE and new SUBSCRIBE ahead of
// the ACK, and nothing failed when the lock was taken out. The frame here asks
// for settleMu, the lock a reset takes before it moves the subscription on. A
// requeue that could take it now runs to the end before the frame, which is
// the order the lock exists to prevent; one that cannot waits, and runs after.
func TestARequeueCannotMoveTheSubscriptionWhileASettlementFrameIsWritten(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixture(t, nil)
	ids, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	held := fetchAll(t, s, 2)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()

	requeue := requeueOptions(t)
	h, err := s.set.begin(&held[0])
	if err != nil {
		t.Fatalf("claiming the slot: %v", err)
	}
	conn, err := s.set.write(ctx, h, func(c *gostomp.Conn, m *gostomp.Message) error {
		if s.settleMu.TryLock() {
			s.settleMu.Unlock()
			if err := s.set.Nack(ctx, &held[1], requeue); err != nil {
				t.Errorf("Nack(NackRequeue()) inside the frame: %v", err)
			}
		}
		return c.Ack(m)
	})
	_ = s.set.finish(&held[0], conn, err, mq.OpAck)
	if s.Resets() == 0 {
		if err := s.set.Nack(ctx, &held[1], requeue); err != nil {
			t.Fatalf("Nack(NackRequeue()) after the frame: %v", err)
		}
	}
	if err := srv.WaitFrames(frame.UNSUBSCRIBE, 1, requeueBudget); err != nil {
		t.Fatalf("the requeue sent no UNSUBSCRIBE: %v", err)
	}
	if err := srv.WaitFrames(frame.ACK, 1, requeueBudget); err != nil {
		t.Fatalf("the settlement sent no ACK: %v", err)
	}
	if !ackedBeforeUnsubscribe(srv, ids[0]) {
		t.Errorf("the ACK of %s, checked before the requeue, reached the server after the requeue's "+
			"UNSUBSCRIBE: the subscription it names was retired first\nframes:%s", ids[0], frameLogOf(srv))
	}
}

// frameLogOf renders the server's frame log for a failure message.
func frameLogOf(srv *stompsrv.Server) string {
	log := ""
	for _, f := range srv.Frames() {
		log += "\n\t" + f.String()
	}
	return log
}

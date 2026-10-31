package stomp

import (
	"context"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// subMu is held across network I/O on purpose: a requeue's UNSUBSCRIBE round
// trip, and a re-subscribe's dial. The tests here put a caller with a short
// deadline behind that I/O and hold the I/O open for longer than the deadline.

// subWaitHold is how long the server holds the I/O a test waits behind, and
// subWaitLimit is how long a caller with a 100 ms deadline may take before it
// counts as having waited for the holder.
const (
	subWaitHold  = 2 * time.Second
	subWaitLimit = time.Second
)

// TestAFetchDuringARequeueKeepsItsDeadline is a fetch made while another
// handler's requeue waits for its UNSUBSCRIBE receipt. The requeue clears the
// live subscription before it unsubscribes, so the fetch waits for subMu, and
// it waited out the broker's answer (up to ReceiptTimeout, and DialTimeout
// more when the receipt never came) whatever its own context said.
func TestAFetchDuringARequeueKeepsItsDeadline(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixtureWith(t, stompsrv.Options{UnsubscribeReceiptDelay: subWaitHold}, nil)
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	held := fetchAll(t, s, 1)
	requeued := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
		defer cancel()
		requeued <- s.set.Nack(ctx, &held[0], requeueOptions(t))
	}()
	if err := srv.WaitFrames(frame.UNSUBSCRIBE, 1, requeueBudget); err != nil {
		t.Fatalf("the requeue sent no UNSUBSCRIBE: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.Fetch(ctx, make([]mq.DriverDelivery, 1))
	took := time.Since(start)
	if rerr := <-requeued; rerr != nil {
		t.Errorf("the requeue: %v", rerr)
	}
	if took > subWaitLimit {
		t.Errorf("a Fetch with a 100ms deadline took %v (returned %v): it waited for the requeue's UNSUBSCRIBE "+
			"receipt", took.Round(time.Millisecond), err)
	}
	if code := mq.CodeOf(err); code != mq.CodeDeadlineExceeded {
		t.Errorf("the Fetch returned %v (code %v), want its own DeadlineExceeded", err, code)
	}
}

// TestAFetchBehindAnotherFetchsDialKeepsItsDeadline is the same wait with no
// requeue at all: two consumers fetch from one source after its socket died,
// the first re-dials a node that is slow to answer CONNECT, and the second
// waited for that dial, which DialTimeout bounds at 15 s by default.
func TestAFetchBehindAnotherFetchsDialKeepsItsDeadline(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixture(t, func(c *Config) { c.NoReconnect = false })
	s.mu.Lock()
	pumpDone := s.pumpDone
	s.mu.Unlock()
	if err := srv.DropConnection(); err != nil { // the source's socket is the newest
		t.Fatalf("DropConnection: %v", err)
	}
	select {
	case <-pumpDone:
	case <-time.After(requeueBudget):
		t.Fatal("the source's pump never saw its socket die")
	}
	_ = s.takeErr()
	srv.GateConnects(true)
	release := time.AfterFunc(subWaitHold, func() { srv.GateConnects(false) })
	defer release.Stop()
	connects := srv.Count(frame.CONNECT)
	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
		defer cancel()
		_, err := s.Fetch(ctx, make([]mq.DriverDelivery, 1))
		first <- err
	}()
	if err := srv.WaitFrames(frame.CONNECT, connects+1, requeueBudget); err != nil {
		srv.GateConnects(false)
		t.Fatalf("the first fetch never re-dialled: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.Fetch(ctx, make([]mq.DriverDelivery, 1))
	took := time.Since(start)
	srv.GateConnects(false)
	if took > subWaitLimit {
		t.Errorf("a Fetch with a 100ms deadline took %v (returned %v): it waited for another fetch's dial",
			took.Round(time.Millisecond), err)
	}
	if code := mq.CodeOf(err); code != mq.CodeDeadlineExceeded {
		t.Errorf("the Fetch returned %v (code %v), want its own DeadlineExceeded", err, code)
	}
	if err := srv.WaitSubscribed(s.Destination(), 2, requeueBudget); err != nil {
		t.Fatalf("the first fetch never subscribed again: %v", err)
	}
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if err := <-first; err != nil {
		t.Errorf("the first fetch: %v", err)
	}
}

// TestASecondRequeueKeepsItsDeadline is two requeues of one subscription's
// deliveries that both pass their staleness check before either resets, the
// race the reset's own check answers with a lost lease. The second waited for
// subMu while the first held it across its UNSUBSCRIBE round trip, so a
// handler with a short deadline was held for the broker's answer. It now ends
// with its own context, having done nothing; the first reset has already
// given its delivery back to the broker.
//
// The test holds subMu until both requeues have claimed their slots, which is
// the interleaving, and lets the first requeue queue for the lock before the
// second starts, so that the first is the one that resets.
func TestASecondRequeueKeepsItsDeadline(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixtureWith(t, stompsrv.Options{UnsubscribeReceiptDelay: subWaitHold}, nil)
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")},
		stompsrv.Message{Body: []byte("two")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	held := fetchAll(t, s, 2)
	bg, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	if err := s.lockSub(bg); err != nil {
		t.Fatalf("taking subMu: %v", err)
	}
	locked := true
	unlock := func() {
		if locked {
			locked = false
			s.unlockSub()
		}
	}
	defer unlock()
	requeued := make(chan error, 1)
	go func() { requeued <- s.set.Nack(bg, &held[0], requeueOptions(t)) }()
	waitSettling(t, s, held[:1])
	time.Sleep(50 * time.Millisecond)
	type answer struct {
		err  error
		took time.Duration
	}
	second := make(chan answer, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := s.set.Nack(ctx, &held[1], requeueOptions(t))
		second <- answer{err, time.Since(start)}
	}()
	waitSettling(t, s, held)
	unlock()
	got := <-second
	if rerr := <-requeued; rerr != nil {
		t.Errorf("the first requeue: %v", rerr)
	}
	if srv.Count(frame.UNSUBSCRIBE) == 0 {
		t.Fatalf("no requeue reset the subscription\nframes:%s", frameLogOf(srv))
	}
	if got.took > subWaitLimit {
		t.Errorf("a second requeue with a 100ms deadline took %v (returned %v): it waited for the first one's "+
			"UNSUBSCRIBE receipt", got.took.Round(time.Millisecond), got.err)
	}
	if code := mq.CodeOf(got.err); code != mq.CodeDeadlineExceeded {
		t.Errorf("the second requeue returned %v (code %v), want its own DeadlineExceeded", got.err, code)
	}
	if mq.IsAmbiguous(got.err) {
		t.Errorf("the second requeue's refusal %v is ambiguous, and it wrote nothing", got.err)
	}
	if s.Resets() != 1 {
		t.Errorf("Source.Resets() is %d, want 1: the second requeue must not reset", s.Resets())
	}
}

// TestARequeueThatGaveUpWaitingLeavesItsDeliverySettleable is a requeue whose
// context ends while subMu is held elsewhere (here by the test, standing in
// for any holder). It wrote nothing and reset nothing, so its delivery is
// still the caller's: a settlement it left claimed would answer every later
// Ack or Nack with "settlement in progress" until the next reset.
func TestARequeueThatGaveUpWaitingLeavesItsDeliverySettleable(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixture(t, nil)
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	held := fetchAll(t, s, 1)
	bg, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	if err := s.lockSub(bg); err != nil {
		t.Fatalf("taking subMu: %v", err)
	}
	ctx, cancelShort := context.WithTimeout(bg, 100*time.Millisecond)
	defer cancelShort()
	answered := make(chan error, 1)
	go func() { answered <- s.set.Nack(ctx, &held[0], requeueOptions(t)) }()
	var err error
	select {
	case err = <-answered:
		s.unlockSub()
	case <-time.After(subWaitLimit):
		// Let the parked requeue go so that the test ends.
		s.unlockSub()
		late := <-answered
		t.Fatalf("a requeue with a 100ms deadline was still waiting for subMu after %v (it returned %v once "+
			"subMu was let go)", subWaitLimit, late)
	}
	if code := mq.CodeOf(err); code != mq.CodeDeadlineExceeded {
		t.Fatalf("the requeue returned %v (code %v), want its own DeadlineExceeded", err, code)
	}
	if s.Resets() != 0 || srv.Count(frame.UNSUBSCRIBE) != 0 {
		t.Fatalf("a requeue that never took subMu reset the subscription (%d resets)", s.Resets())
	}
	if err := s.set.Ack(bg, &held[0]); err != nil {
		t.Errorf("Ack after a requeue that gave up waiting: %v; the delivery was never given back, so it is "+
			"still the caller's to settle", err)
	}
}

// waitSettling waits until every delivery's slot is claimed by a settlement.
func waitSettling(t *testing.T, s *Source, held []mq.DriverDelivery) {
	t.Helper()
	for deadline := time.Now().Add(requeueBudget); !settling(s.table, held); {
		if time.Now().After(deadline) {
			t.Fatal("the requeues never claimed their slots")
		}
		time.Sleep(time.Millisecond)
	}
}

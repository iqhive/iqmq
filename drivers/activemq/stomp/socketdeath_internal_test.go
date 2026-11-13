package stomp

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A source whose own socket dies forgets the connection and leaves go-stomp to
// close it ([Source.socketDied]); its pump reads the subscription's terminal
// message once go-stomp has handed out what it read. The tests here start from
// a consumer that is behind: the buffer is full and the pump is parked pushing
// one more delivery into it, so it cannot read that message until something
// closes the buffer, and go-stomp's process loop for the connection is parked
// handing the backlog over.

// parkedBehindAFullBuffer is a source that may re-dial, with n deliveries
// against its prefetch of four and nothing fetched, so that its pump is parked
// pushing the fifth into a full buffer. The consumer window is one byte, so
// the drain's overflow holds one delivery and go-stomp's process loop is
// parked handing over the next.
func parkedBehindAFullBuffer(t *testing.T, n int) (*stompsrv.Server, *Source) {
	t.Helper()
	srv, _, s := requeueFixtureWith(t, stompsrv.Options{}, func(c *Config) {
		c.NoReconnect = false
		c.ConsumerWindow = 1
	})
	for i := range n {
		if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte(fmt.Sprint("before-", i))}); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	eventually(t, "the pump to park on the full buffer", func() bool { return s.buffer().Blocked() == 1 })
	return srv, s
}

// eventually waits up to requeueBudget for pred, or fails.
func eventually(t *testing.T, what string, pred func() bool) {
	t.Helper()
	for deadline := time.Now().Add(requeueBudget); !pred(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("waited %v for %s", requeueBudget, what)
		}
	}
}

// socketForgotten waits for the source to have noticed its socket's death.
func socketForgotten(t *testing.T, s *Source) {
	t.Helper()
	eventually(t, "the source to notice its socket's death", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.conn == nil
	})
}

// TestAFetchAfterTheSocketDiesBehindAFullBufferHandsOutNothingThatCameOverIt is
// the consumer that is behind losing its socket, with no fetch between the
// death and the next. The death notice invalidates every token and forgets the
// connection, and the next fetch re-dials and re-subscribes. That fetch used to
// read the old buffer and hand out what the dead connection had buffered: the
// Ack of each reported a lost lease, and the broker redelivered each on the new
// subscription after its handler had run. The parked pump then pushed the
// delivery it was holding into the same buffer, for the fetch after.
func TestAFetchAfterTheSocketDiesBehindAFullBufferHandsOutNothingThatCameOverIt(t *testing.T) {
	t.Parallel()
	srv, s := parkedBehindAFullBuffer(t, 6)
	if err := srv.DropConnection(); err != nil { // the source's socket is the newest
		t.Fatalf("DropConnection: %v", err)
	}
	socketForgotten(t, s)
	handsOutOnlyTheNewSubscriptions(t, srv, s, "the source's socket died")
}

// handsOutOnlyTheNewSubscriptions fetches from s after its connection went
// with a full buffer, acknowledging everything handed out, until a delivery
// made on the subscription the fetches make again comes through. Nothing from
// before the death may be handed out and no Ack may be refused.
func handsOutOnlyTheNewSubscriptions(t *testing.T, srv *stompsrv.Server, s *Source, death string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	var handed, refused []string
	fetch := func() {
		fctx, fcancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer fcancel()
		dst := make([]mq.DriverDelivery, 8)
		n, _ := s.Fetch(fctx, dst)
		for i := range n {
			body := string(dst[i].Message.Body)
			handed = append(handed, body)
			if err := s.Settler().Ack(ctx, &dst[i]); err != nil {
				refused = append(refused, fmt.Sprintf("%s: %v", body, err))
			}
		}
	}
	// The fetches re-subscribe, the first of them at once. The one after is
	// where the parked pump's delivery used to land.
	for deadline := time.Now().Add(requeueBudget); srv.Subscribes(s.Destination()) < 2; {
		if time.Now().After(deadline) {
			t.Fatalf("the source never subscribed again; handed out %v, Acks refused %v", handed, refused)
		}
		fetch()
	}
	fetch()
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("Deliver on the new subscription: %v", err)
	}
	for deadline := time.Now().Add(requeueBudget); !slices.Contains(handed, "after") && time.Now().Before(deadline); {
		fetch()
	}

	stale := slices.DeleteFunc(slices.Clone(handed), func(b string) bool { return !strings.HasPrefix(b, "before-") })
	if len(stale) > 0 || len(refused) > 0 {
		t.Errorf("after %s with a full buffer, the fetches that re-subscribed handed out %v, which came over the "+
			"connection that went, and refused the Ack of %v: the handler ran for each and the broker redelivers "+
			"every one on the new subscription", death, stale, refused)
	}
	if !slices.Contains(handed, "after") {
		t.Errorf("the delivery made on the new subscription was never handed out; handed out: %v", handed)
	}
}

// TestAConnectionGoStompClosesBehindAFullBufferHandsOutNothingThatCameOverIt
// is the consumer that is behind losing its connection to go-stomp itself: a
// heart-beat the broker promised did not arrive, so go-stomp's read timer
// fired, handed the subscription "read timeout" and closed the connection. A
// source ignored the close's notice, on the reasoning that its pump reads the
// terminal message and repairs the source, but that pump was parked on the full
// buffer. The source kept the closed connection and its subscription as the
// live ones, and the next fetch handed out what the closed connection had
// buffered: the Ack of each reported a lost lease, and the broker redelivered
// each after its handler had run. A broker's ERROR frame does the same when
// go-stomp handles it before its reader meets the socket's end.
func TestAConnectionGoStompClosesBehindAFullBufferHandsOutNothingThatCameOverIt(t *testing.T) {
	t.Parallel()
	srv, s := closedByGoStompBehindAFullBuffer(t)
	handsOutOnlyTheNewSubscriptions(t, srv, s, "go-stomp closed the source's connection")
}

// TestCloseAfterGoStompClosesTheConnectionReportsNothing is that state with no
// fetch before Close. Close took the subscription as live and unsubscribed it
// on the closed connection, and go-stomp, which had already ended the
// subscription, answered ErrCompletedSubscription: Close, and Client.Close with
// it, reported "Unknown: subscription is unsubscribed" for a source that had
// lost its connection and nothing else.
func TestCloseAfterGoStompClosesTheConnectionReportsNothing(t *testing.T) {
	t.Parallel()
	_, s := closedByGoStompBehindAFullBuffer(t)
	if err := s.Close(); err != nil {
		t.Errorf("Source.Close after go-stomp closed the source's connection reports %v (code %v), want nil: the "+
			"subscription went with the connection", err, mq.CodeOf(err))
	}
}

// closedByGoStompBehindAFullBuffer is a consumer that is behind whose
// connection go-stomp closes itself ([heartBeatsStop]). It has six deliveries
// against its prefetch of four with nothing fetched, so its pump is parked
// pushing the fifth into a full buffer. It returns once go-stomp has closed the
// source's socket, which takes about five seconds.
func closedByGoStompBehindAFullBuffer(t *testing.T) (*stompsrv.Server, *Source) {
	t.Helper()
	srv, b, s := heartBeatsStop(t)
	for i := range 6 {
		if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte(fmt.Sprint("before-", i))}); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	eventually(t, "the pump to park on the full buffer", func() bool { return s.buffer().Blocked() == 1 })
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	reconnects := b.Reconnects()
	for deadline := time.Now().Add(2 * requeueBudget); b.writerOf(conn) != nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("go-stomp did not close the source's connection within %v", 2*requeueBudget)
		}
	}
	if got := b.Reconnects(); got != reconnects {
		t.Fatalf("the backend reconnected (%d to %d) while go-stomp timed out the source's connection, and its "+
			"repair reaches every source, so this run cannot show what the source does on its own", reconnects, got)
	}
	return srv, s
}

// heartBeatsStop is a source that may re-dial and asks for the broker's
// heart-beats, against a server that promises them and sends none, so
// go-stomp's read timer fires on the source's connection and go-stomp closes
// it itself. The timeout is the promised interval plus go-stomp's fixed
// five-second allowance, counted from the last frame the connection read. A
// publication every interval keeps the backend's own connection reading
// receipts, so that its timer does not fire first and its repair does not
// reach the source.
func heartBeatsStop(t *testing.T) (*stompsrv.Server, *Backend, *Source) {
	t.Helper()
	const interval = 100 * time.Millisecond
	srv, b, s := requeueFixtureWith(t, stompsrv.Options{PromiseHeartBeats: interval}, func(c *Config) {
		c.NoReconnect = false
		c.HeartbeatReceive = interval
	})
	snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: "keepalive", Confirmation: mq.ConfirmDurable})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(interval):
			}
			ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
			_ = snk.Publish(ctx, &mq.Message{Body: []byte("keepalive")}, nil)
			cancel()
		}
	})
	t.Cleanup(func() {
		close(stop)
		wg.Wait()
		_ = snk.Close()
	})
	return srv, b, s
}

// TestARequeueAsGoStompEndsTheConnectionDoesNotSubscribeOnIt is a requeue in
// the moment go-stomp has ended the live subscription and not yet closed its
// connection: its process loop has handed the subscription "read timeout" and
// is still handing the error to the source's other subscriptions, ahead of its
// deferred disconnect, whose close notice is what tells the source. The
// UNSUBSCRIBE then answers ErrCompletedSubscription. A requeue that took that
// as nothing left to unsubscribe subscribed again on the same connection,
// which the source still held: go-stomp queued the SUBSCRIBE and never sent
// it, the source took the subscription as the live one, go-stomp's reader for
// it stayed blocked for good, and Close waited the whole libraryCloseWait for
// its pump. Measured:
//
//	the requeue subscribed again on the connection go-stomp was ending;
//	Close took 1.001s, and go-stomp's reader for that subscription is still
//	running after it
//
// The moment is held open the way go-stomp can hold it: a second subscription
// on the source's connection, its channel full and unread, blocks the process
// loop as it hands that subscription the error. Which subscription the loop
// hands the error to first is its map's order, so an attempt where the held one
// comes first is tried again.
func TestARequeueAsGoStompEndsTheConnectionDoesNotSubscribeOnIt(t *testing.T) {
	t.Parallel()
	for attempt := 1; attempt <= 4; attempt++ {
		if requeueAsGoStompEndsTheConnection(t) {
			return
		}
		t.Logf("attempt %d: go-stomp handed the held subscription its error first", attempt)
	}
	t.Skip("go-stomp handed the held subscription its error first in every attempt, so the moment never came")
}

// requeueAsGoStompEndsTheConnection is one attempt of
// [TestARequeueAsGoStompEndsTheConnectionDoesNotSubscribeOnIt], and reports
// whether the moment it needs came.
func requeueAsGoStompEndsTheConnection(t *testing.T) bool {
	t.Helper()
	srv, b, s := heartBeatsStop(t)
	s.mu.Lock()
	conn, live := s.conn, s.sub2
	s.mu.Unlock()
	// The held subscription takes 16 deliveries in its channel and one more
	// in its reader, so the next frame the process loop hands it blocks the
	// loop. It is drained once the test is past the moment; the source is
	// closed first in the cleanup, so that the drain always ends.
	held, err := conn.Subscribe("held", gostomp.AckAuto)
	if err != nil {
		t.Fatalf("subscribing the held subscription: %v", err)
	}
	var drain sync.WaitGroup
	release := sync.OnceFunc(func() {
		drain.Go(func() {
			for range held.C {
			}
		})
	})
	t.Cleanup(func() {
		release()
		_ = s.Close()
		drain.Wait()
	})
	if err := srv.WaitSubscribed("held", 1, requeueBudget); err != nil {
		t.Fatalf("the held subscription: %v", err)
	}
	for i := range 17 {
		if _, err := srv.Deliver("held", stompsrv.Message{Body: []byte(fmt.Sprint("held-", i))}); err != nil {
			t.Fatalf("Deliver to the held subscription: %v", err)
		}
	}
	// One delivery handed out for the requeue, and the buffer filled behind
	// it, so that the pump is parked and cannot read the terminal message
	// and repair the source first.
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("requeued")}); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	d := fetchAll(t, s, 1)[0]
	for i := range 5 {
		if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte(fmt.Sprint("before-", i))}); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	eventually(t, "the pump to park on the full buffer", func() bool { return s.buffer().Blocked() == 1 })
	for deadline := time.Now().Add(requeueBudget); live.Active(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			release()
			return false
		}
	}
	s.mu.Lock()
	holds := s.conn == conn
	s.mu.Unlock()
	if !holds || b.writerOf(conn) == nil {
		t.Fatalf("go-stomp ended the live subscription, and the source holds its connection=%v with its socket "+
			"open=%v: the held subscription did not keep the process loop from closing it", holds, b.writerOf(conn) != nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	if err := s.Settler().Nack(ctx, &d, requeueOptions(t)); err != nil {
		t.Errorf("the requeue: %v", err)
	}
	s.mu.Lock()
	after, sub := s.conn, s.sub2
	s.mu.Unlock()
	release()
	eventually(t, "go-stomp to close the ending connection", func() bool { return b.writerOf(conn) == nil })
	start := time.Now()
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	took := time.Since(start)
	if after == conn {
		_, leaked := libraryGoroutines(conn, sub)
		t.Errorf("the requeue subscribed again on the connection go-stomp was ending, whose SUBSCRIBE go-stomp "+
			"never sends; Close took %v, and go-stomp's reader for that subscription is still running after it: %v",
			took.Round(time.Millisecond), leaked)
	} else if took >= libraryCloseWait/2 {
		t.Errorf("Close took %v after the requeue: a pump on the ending connection held it", took.Round(time.Millisecond))
	}
	return true
}

// TestCloseOfASubscriptionGoStompHasEndedReportsNothing is Close finding the
// live subscription already ended by go-stomp. go-stomp ends a subscription
// when its connection ends, on a goroutine of its own, and the source learns of
// that through the connection's death notice a moment later: a Close between
// the two unsubscribed, and go-stomp answered ErrCompletedSubscription, which
// Close reported as "Unknown: subscription is unsubscribed". The test holds
// that moment open by ending the subscription through go-stomp itself, with
// the connection still up.
func TestCloseOfASubscriptionGoStompHasEndedReportsNothing(t *testing.T) {
	t.Parallel()
	_, _, s := requeueFixture(t, nil)
	s.mu.Lock()
	sub := s.sub2
	s.mu.Unlock()
	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("ending the subscription through go-stomp: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Source.Close of a subscription go-stomp had ended reports %v (code %v), want nil: there was "+
			"nothing left to unsubscribe", err, mq.CodeOf(err))
	}
}

// TestTheDeadSubscriptionsLastMessageLeavesTheReplacementConnection is the pump
// of a subscription whose socket died reading the subscription's terminal
// message after the source has dialled a replacement and before it has
// subscribed on it. Under Config.VerifyQueueAddress a question sits between
// the two, and a pump that was busy until then (on a dead-letter copy waiting
// for its receipt, or parked on a full buffer until the re-subscribe closes it)
// reads the message there. The pump was the live subscription's by generation
// alone, and its repair dropped the connection the fetch had just dialled: the
// fetch's SUBSCRIBE met a closed connection, the fetch failed, and the next one
// had to dial again.
//
// The test makes that order with the two steps the re-subscribing fetch takes
// before its SUBSCRIBE: the dial ([Source.connection]) and the buffer swap that
// frees the pump.
func TestTheDeadSubscriptionsLastMessageLeavesTheReplacementConnection(t *testing.T) {
	t.Parallel()
	srv, s := parkedBehindAFullBuffer(t, 6)
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	socketForgotten(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	replacement, err := s.connection(ctx)
	if err != nil {
		t.Fatalf("dialling the replacement: %v", err)
	}
	s.swapBuffer()
	eventually(t, "the dead subscription's pump to read its terminal message", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.lastErr != nil
	})
	s.mu.Lock()
	held := s.conn
	s.mu.Unlock()
	if held != replacement {
		t.Errorf("the dead subscription's terminal message dropped the connection dialled to replace it (the "+
			"source holds %p, the replacement is %p): the fetch about to subscribe on it meets a closed connection",
			held, replacement)
	}

	fctx, fcancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, _ = s.Fetch(fctx, make([]mq.DriverDelivery, 1))
	fcancel()
	if err := srv.WaitSubscribed(s.Destination(), 2, requeueBudget); err != nil {
		t.Fatalf("the source never subscribed again: %v", err)
	}
	if got := s.Redials(); got != 1 {
		t.Errorf("the source dialled %d connections beyond its first, want 1: the replacement went unused", got)
	}
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("Deliver on the new subscription: %v", err)
	}
	d := fetchAll(t, s, 1)[0]
	if err := s.Settler().Ack(ctx, &d); err != nil {
		t.Errorf("acknowledging the delivery made on the replacement: %v", err)
	}
}

// TestASecondSocketDeathNeitherWaitsForTheFirstNorLosesIt is a replacement
// connection that dies while go-stomp is still finishing with the first: the
// first's loop is parked handing over its backlog, behind a pump parked on a
// full buffer, and a replacement is dialled and dies before anything reads the
// buffer.
//
// The replacement's death notice used to wait libraryCloseWait for the first
// connection on go-stomp's reader goroutine, where nothing could free it, and
// then drop it where Close no longer knew of it. When Close later freed the
// pump and let the drains go, the first loop was cut off from its drain halfway
// through the backlog and stayed blocked for good.
//
// It is not parallel because it reads every goroutine's stack.
func TestASecondSocketDeathNeitherWaitsForTheFirstNorLosesIt(t *testing.T) {
	srv, s, first, sub := dyingBehindAFullBuffer(t)

	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	second, err := s.connection(ctx)
	if err != nil {
		t.Fatalf("dialling the replacement: %v", err)
	}
	if err := srv.DropConnection(); err != nil { // the replacement is now the newest
		t.Fatalf("DropConnection: %v", err)
	}
	dropped := time.Now()
	// go-stomp closes the replacement's socket once its reader has returned
	// from the death notice, so this is how long the notice held that reader.
	eventually(t, "the replacement's socket to close", func() bool { return s.b.writerOf(second) == nil })
	if took := time.Since(dropped); took > libraryCloseWait/2 {
		t.Errorf("the replacement's death notice held go-stomp's reader for %v: it waited for the first "+
			"connection, whose loop only a fetch or Close can free", took.Round(time.Millisecond))
	}
	if _, running := libraryGoroutines(first, sub); !running {
		t.Fatal("go-stomp finished with the first connection before Close, so this run cannot show what Close does")
	}
	closesLeavingNothing(t, s, first, sub)
}

// TestCloseLeavesNothingOfADyingConnectionRunning is one dying connection at
// Close, the first connection above with no second death. Close waited for
// the connection's socket to close and then let the drains go. go-stomp closes
// the socket when its process loop returns, and the subscription's reader may
// still have to push the last message into the subscription's channel, behind
// deliveries the drain has not read yet: once the drain had gone, that reader
// stayed blocked for good, in 11 of 200 runs at GOMAXPROCS=2 under -cover.
//
// It is not parallel because it reads every goroutine's stack.
func TestCloseLeavesNothingOfADyingConnectionRunning(t *testing.T) {
	_, s, first, sub := dyingBehindAFullBuffer(t)
	closesLeavingNothing(t, s, first, sub)
}

// dyingBehindAFullBuffer is [parkedBehindAFullBuffer] with 35 deliveries and
// the source's socket dropped: the source has forgotten the connection, and
// go-stomp is still handing that connection's backlog to the subscription. It
// returns that connection and subscription.
func dyingBehindAFullBuffer(t *testing.T) (*stompsrv.Server, *Source, *gostomp.Conn, *gostomp.Subscription) {
	t.Helper()
	srv, s := parkedBehindAFullBuffer(t, 35)
	s.mu.Lock()
	sub := s.sub2
	s.mu.Unlock()
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	var first *gostomp.Conn
	eventually(t, "the source to notice its socket's death", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.dying != nil {
			first = s.dying.conn
		}
		return first != nil
	})
	if _, running := libraryGoroutines(first, sub); !running {
		t.Fatal("go-stomp finished with the first connection, so the state this test needs did not arise")
	}
	return srv, s, first, sub
}

// closesLeavingNothing closes s and asserts that go-stomp's process loop for
// conn and the reader of sub have both ended within twice libraryCloseWait.
func closesLeavingNothing(t *testing.T, s *Source, conn *gostomp.Conn, sub *gostomp.Subscription) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Logf("Close: %v", err)
	}
	var stacks string
	running := true
	for deadline := time.Now().Add(2 * libraryCloseWait); running && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
		stacks, running = libraryGoroutines(conn, sub)
	}
	if running {
		t.Errorf("go-stomp is still running %v after Close for the dying connection or the subscription made on "+
			"it: Close let the drains go before that connection had handed out its backlog\n%s",
			2*libraryCloseWait, stacks)
	}
}

// libraryGoroutines reports whether go-stomp's process loop for conn, or the
// reader of subscription sub, is still running, and their stacks. Each takes
// its connection or subscription as its first argument, and a goroutine's
// stack prints it.
func libraryGoroutines(conn *gostomp.Conn, sub *gostomp.Subscription) (string, bool) {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	loop := fmt.Sprintf("go-stomp/stomp/v3.processLoop(%p", conn)
	reader := fmt.Sprintf("go-stomp/stomp/v3.(*Subscription).readLoop(%p", sub)
	var found []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, loop) || strings.Contains(g, reader) {
			found = append(found, g)
		}
	}
	return strings.Join(found, "\n\n"), len(found) > 0
}

// TestARepairThatLostTheRaceToAReplacementLeavesItsDeliveriesSettleable is the
// dead subscription's pump reaching its repair after a re-subscribing fetch has
// dialled a replacement, subscribed on it and handed out a delivery. The pump
// read its terminal message while the source held no connection and the
// generation was still its own, so it was the live subscription's then, and
// the fetch got in before the repair took the lock. The repair invalidated
// every token and dropped what the new subscription had buffered while that
// subscription stayed live: the handed-out delivery's Ack reported a lost
// lease, and the broker kept the buffered ones as delivered until a reset.
//
// The window is a few instructions wide, so the test calls the repair itself
// in the state the window leaves.
func TestARepairThatLostTheRaceToAReplacementLeavesItsDeliveriesSettleable(t *testing.T) {
	t.Parallel()
	srv, _, s := requeueFixtureWith(t, stompsrv.Options{}, func(c *Config) { c.NoReconnect = false })
	s.mu.Lock()
	old, oldGen := s.conn, s.subGen
	s.mu.Unlock()
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	socketForgotten(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	fctx, fcancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, _ = s.Fetch(fctx, make([]mq.DriverDelivery, 1))
	fcancel()
	if err := srv.WaitSubscribed(s.Destination(), 2, requeueBudget); err != nil {
		t.Fatalf("the source never subscribed again: %v", err)
	}
	for i := range 3 {
		if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte(fmt.Sprint("new-", i))}); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	eventually(t, "the new subscription to buffer three deliveries", func() bool { return s.buffer().Len() == 3 })
	d := fetchAll(t, s, 1)[0]

	if s.fail(oldGen, old, gostomp.ErrClosedUnexpectedly) {
		t.Error("the dead subscription's repair ran on a source that had subscribed again since")
	}
	if !s.subscribed() {
		t.Error("the dead subscription's repair retired the subscription made since")
	}
	if err := s.Settler().Ack(ctx, &d); err != nil {
		t.Errorf("acknowledging a delivery of the subscription made after the death: %v", err)
	}
	_ = s.takeErr()
	rest := fetchAll(t, s, 2)
	for i := range rest {
		if err := s.Settler().Ack(ctx, &rest[i]); err != nil {
			t.Errorf("acknowledging %s, buffered before the late repair: %v", rest[i].Message.Body, err)
		}
	}
}

// TestASubscriptionMadeOnAConnectionTheSourceHasForgottenIsNotTheLiveOne is a
// subscribe whose connection the source lets go while its SUBSCRIBE is going
// out. Under Config.VerifyQueueAddress the question sits between the dial and
// the SUBSCRIBE, and here the death notice for the new connection runs during
// it. The subscription was then installed as the live one, so the source
// counted as subscribed until that subscription's terminal message, and
// go-stomp sends none for a SUBSCRIBE its process loop did not take before it
// ended: every fetch would return nothing and no error. The test runs the
// notice by hand on a connection that is still up, which leaves the same
// state, a subscription on a connection the source no longer holds with no
// terminal message coming.
func TestASubscriptionMadeOnAConnectionTheSourceHasForgottenIsNotTheLiveOne(t *testing.T) {
	t.Parallel()
	srv, s := verifiedSource(t)
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	socketForgotten(t, s)

	fetching(t, s, func() {
		reply := awaitQuestion(t, srv, 2)
		s.mu.Lock()
		replacement := s.conn
		s.mu.Unlock()
		if replacement == nil {
			t.Fatal("the re-subscribing fetch asked its question with no connection of its own")
		}
		s.socketDied(replacement)
		answerQuestion(t, srv, reply)
		if err := srv.WaitSubscribed(s.Destination(), 2, requeueBudget); err != nil {
			t.Fatalf("the fetch never subscribed on the forgotten connection: %v", err)
		}
	})
	if s.subscribed() {
		t.Error("a subscription made on a connection the source has forgotten is the live one, so no fetch " +
			"subscribes again until its terminal message, which may never come")
	}

	fetching(t, s, func() {
		answerQuestion(t, srv, awaitQuestion(t, srv, 3))
		if err := srv.WaitSubscribed(s.Destination(), 3, requeueBudget); err != nil {
			t.Fatalf("the next fetch never subscribed again: %v", err)
		}
	})
	if _, err := srv.Deliver(s.Destination(), stompsrv.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("Deliver on the new subscription: %v", err)
	}
	d := fetchAll(t, s, 1)[0]
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	if err := s.Settler().Ack(ctx, &d); err != nil {
		t.Errorf("acknowledging the delivery made on the new subscription: %v", err)
	}
}

// verifiedSource is a source that may re-dial and asks Artemis which address
// its queue is bound to before every subscribe, opened with its first question
// answered.
func verifiedSource(t *testing.T) (*stompsrv.Server, *Source) {
	t.Helper()
	srv, err := stompsrv.New(stompsrv.Options{ServerHeader: activemq.ServerArtemis + "/2.44.0"})
	if err != nil {
		t.Fatalf("starting the in-process STOMP server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	defer cancel()
	b, err := OpenBackend(ctx, Config{
		Broker:             activemq.Artemis,
		Addrs:              []string{srv.Addr()},
		ReceiptTimeout:     requeueBudget,
		DialTimeout:        requeueBudget,
		VerifyQueueAddress: true,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	type result struct {
		src mq.Source
		err error
	}
	opened := make(chan result, 1)
	go func() {
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers", Prefetch: 4})
		opened <- result{src, err}
	}()
	answerQuestion(t, srv, awaitQuestion(t, srv, 1))
	got := <-opened
	if got.err != nil {
		t.Fatalf("OpenSource: %v", got.err)
	}
	s, ok := got.src.(*Source)
	if !ok {
		t.Fatalf("OpenSource returned %T", got.src)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := srv.WaitSubscribed(s.Destination(), 1, requeueBudget); err != nil {
		t.Fatalf("the driver never subscribed: %v", err)
	}
	return srv, s
}

// fetching runs one fetch on a goroutine of its own while step runs, then
// cancels that fetch and waits for it.
func fetching(t *testing.T, s *Source, step func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), requeueBudget)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.Fetch(ctx, make([]mq.DriverDelivery, 1))
	}()
	defer func() {
		cancel()
		<-done
	}()
	step()
}

// awaitQuestion waits for the n-th `getAddress` request the server receives
// and returns the reply queue it names, once the driver is subscribed to it.
func awaitQuestion(t *testing.T, srv *stompsrv.Server, n int) string {
	t.Helper()
	var reply string
	eventually(t, fmt.Sprint("getAddress question ", n), func() bool {
		seen := 0
		for _, f := range srv.FramesOf(frame.SEND) {
			if f.Get(frame.Destination) != managementAddress || f.Get("_AMQ_OperationName") != "getAddress" {
				continue
			}
			if seen++; seen == n {
				reply = f.Get("reply-to")
				return true
			}
		}
		return false
	})
	if err := srv.WaitSubscribed(reply, 1, requeueBudget); err != nil {
		t.Fatalf("the driver never subscribed to its reply queue %s: %v", reply, err)
	}
	return reply
}

// answerQuestion answers a `getAddress` request on reply the way Artemis does
// for a queue bound to the address "orders".
func answerQuestion(t *testing.T, srv *stompsrv.Server, reply string) {
	t.Helper()
	if _, err := srv.Deliver(reply, stompsrv.Message{
		Body:    []byte(`["orders"]`),
		Headers: [][2]string{{"_AMQ_OperationSucceeded", "true"}},
	}); err != nil {
		t.Fatalf("answering the question on %s: %v", reply, err)
	}
}

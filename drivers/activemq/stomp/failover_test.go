package stomp_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
	"github.com/iqhive/iqmq/internal/activemq"
)

// A failover list, and who walks it.
//
// The client's dial loop and every source dial connections of their own from
// Config.Addrs. They used to take the next address from one counter between
// them, so each dialler's attempts were interleaved with everybody else's and
// none of them walked the list the way the documentation says: round-robin
// across its own attempts.

// deadNode is a failover node that is not taking clients: every CONNECT is
// answered with an ERROR frame and then the socket, which is what a dial to a
// node that is down costs a dialler.
func deadNode(t *testing.T) *stompsrv.Server {
	t.Helper()
	return newServer(t, stompsrv.Options{
		ServerHeader:  activemq.ServerArtemis + "/2.44.0",
		RefuseConnect: "AMQ229119: this node is not live",
	})
}

// TestEverySourceDialsTheNodeTheClientReached is a failover list of a live
// node and a dead one, with the client connected to the live one.
//
// The client's own dial took the first address, so the first source drew the
// second and failed, the next drew the first and opened, and so on: every
// second OpenSource was refused although the client was connected and the live
// node was taking connections. A source starts its walk at the node the
// client's connection reached.
func TestEverySourceDialsTheNodeTheClientReached(t *testing.T) {
	t.Parallel()
	live, dead := artemisServer(t), deadNode(t)
	cfg := artemisConfig(live)
	cfg.Addrs = []string{live.Addr(), dead.Addr()}
	b := openBackend(t, cfg)

	for i := range 4 {
		sub := "workers" + strconv.Itoa(i)
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		s, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: sub, Prefetch: 4})
		cancel()
		if err != nil {
			t.Fatalf("source %d of 4 failed to open on a client connected to a live node: %v (the dead node took "+
				"%d connection(s))", i+1, err, dead.Accepted())
		}
		t.Cleanup(func() { closeSource(t, s) })
	}
	if got := dead.Accepted(); got != 0 {
		t.Errorf("the dead node was dialled %d time(s) while four sources opened, want 0: each source starts at the "+
			"node the client reached", got)
	}
}

// TestARequeueReplacesItsConnectionOnTheNodeItLeft is the same list, with a
// requeue whose UNSUBSCRIBE the live node never answers.
//
// The requeue drops the source's own connection, which was healthy, and dials
// a replacement as part of the requeue. That dial took the next address of the
// source's walk, the dead node, so the requeue failed. Under
// ReconnectAttempts=1 the failed dial also spent the source's whole re-dial
// budget, and every fetch after it refused for good while the live node was
// taking connections. A replacement is not a reconnection: it goes back to the
// node the source just left.
func TestARequeueReplacesItsConnectionOnTheNodeItLeft(t *testing.T) {
	t.Parallel()
	live, dead := slowUnsubscribeServer(t, never), deadNode(t)
	cfg := reconnecting(artemisConfig(live))
	cfg.Addrs = []string{live.Addr(), dead.Addr()}
	cfg.ReconnectAttempts = 1
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, live, "orders", "workers", 4)
	deliver(t, live, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())); err != nil {
		t.Fatalf("Nack(NackRequeue()) whose unsubscribe went unanswered: %v (the dead node took %d connection(s))",
			err, dead.Accepted())
	}
	if got := dead.Accepted(); got != 0 {
		t.Errorf("the requeue's replacement connection was dialled at the dead node %d time(s), want 0: the "+
			"source had just left the live node, which was up", got)
	}
	if err := live.WaitLive(consume, 1, budget); err != nil {
		t.Fatalf("no live subscription after the requeue: %v\nframes:%s", err, frameLog(live))
	}
	deliver(t, live, consume, body("two"))
	if got := fetchN(t, src, 1); string(got[0].Message.Body) != "two" {
		t.Errorf("fetched %q after the requeue, want %q", got[0].Message.Body, "two")
	}
}

// TestAReplacementItsCallerCutShortStillGoesToTheNodeItLeft is that requeue
// with its context ending while the replacement dial waits for CONNECTED.
//
// A dial the caller's context ends says nothing about the node, so it is not
// counted, and the next fetch dials again. That dial took the next node of the
// walk, the dead one, so the failure the replacement exists to avoid came one
// fetch later instead. The next dial goes back to the node the source left,
// once, as an ordinary dial (TestAReplacementOwedToAHungNodeIsOwedOnce says
// why only once).
func TestAReplacementItsCallerCutShortStillGoesToTheNodeItLeft(t *testing.T) {
	t.Parallel()
	live, dead := slowUnsubscribeServer(t, never), deadNode(t)
	cfg := reconnecting(artemisConfig(live))
	cfg.Addrs = []string{live.Addr(), dead.Addr()}
	cfg.ReconnectAttempts = 1
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, live, "orders", "workers", 4)
	deliver(t, live, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	live.GateConnects(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requeue := nackOptions(t, mq.NackRequeue())
	nacked := make(chan error, 1)
	go func() { nacked <- src.Settler().Nack(ctx, &d, requeue) }()
	// The client's CONNECT, the source's, and the replacement's.
	if err := live.WaitFrames(frame.CONNECT, 3, budget); err != nil {
		t.Fatalf("the requeue never dialled its replacement: %v\nframes:%s", err, frameLog(live))
	}
	cancel()
	if err := <-nacked; err == nil {
		t.Fatal("Nack(NackRequeue()) succeeded although its context ended during the replacement dial")
	}
	live.GateConnects(false)

	deadline := time.Now().Add(budget)
	for live.Subscribes(consume) < 2 && dead.Accepted() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the source never subscribed again: the next fetch reports %v\nframes:%s",
				fetchError(t, src, 10*time.Millisecond), frameLog(live))
		}
		fetchQuietly(src, 10*time.Millisecond)
	}
	if got := dead.Accepted(); got != 0 {
		t.Errorf("the dial after a replacement its caller cut short went to the dead node (%d connection(s)), "+
			"want the live node the source had left", got)
	}
}

// TestAReplacementOwedToAHungNodeIsOwedOnce is a node that takes the socket
// and never answers CONNECT, with a working standby and fetches whose
// deadlines are shorter than DialTimeout.
//
// The replacement the requeue's context cut short was owed again every time
// the owed dial was cut short too, and a fetch's deadline cuts every dial to
// that node short. None of those dials is counted, so the source went back to
// the hung node on every fetch and never tried the standby: measured, 30
// fetches in six seconds, all of them at the hung node. The owed dial is an
// ordinary one, so once the next fetch cuts it short as well, the walk moves
// on.
func TestAReplacementOwedToAHungNodeIsOwedOnce(t *testing.T) {
	t.Parallel()
	live, standby := slowUnsubscribeServer(t, never), artemisServer(t)
	cfg := reconnecting(artemisConfig(live))
	cfg.Addrs = []string{live.Addr(), standby.Addr()}
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, live, "orders", "workers", 4)
	deliver(t, live, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	live.HoldConnects(true)
	t.Cleanup(func() { live.HoldConnects(false) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requeue := nackOptions(t, mq.NackRequeue())
	nacked := make(chan error, 1)
	go func() { nacked <- src.Settler().Nack(ctx, &d, requeue) }()
	// The client's CONNECT, the source's, and the replacement's.
	if err := live.WaitFrames(frame.CONNECT, 3, budget); err != nil {
		t.Fatalf("the requeue never dialled its replacement: %v\nframes:%s", err, frameLog(live))
	}
	cancel()
	if err := <-nacked; err == nil {
		t.Fatal("Nack(NackRequeue()) succeeded although its context ended during the replacement dial")
	}

	deadline := time.Now().Add(budget)
	fetches := 0
	for standby.Accepted() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("after %d fetches the source had never dialled the standby: every dial went back to the node "+
				"that takes the socket and never answers CONNECT (it took %d connection(s))", fetches, live.Accepted())
		}
		fetchQuietly(src, 50*time.Millisecond)
		fetches++
	}
	if err := standby.WaitSubscribed(consume, 1, budget); err != nil {
		t.Fatalf("the source never subscribed on the standby: %v\nframes:%s", err, frameLog(standby))
	}
}

// TestARequeueOnAHungNodeFailsAtDialTimeoutAndTheNextFetchReachesTheStandby is
// the same hung node with a Nack and a fetch whose deadlines outlast
// DialTimeout, as a consumer loop's do.
//
// The replacement goes back to the node the source left, so here it waits out
// DialTimeout and the requeue fails; the next fetch dials the standby. That is
// the cost of going back: before, this requeue reached the standby at once.
// The failure is a retryable Unavailable. DialTimeout ended the dial while the
// caller's own deadline had not passed, and it came back as DeadlineExceeded,
// which told the caller its own deadline was the problem.
func TestARequeueOnAHungNodeFailsAtDialTimeoutAndTheNextFetchReachesTheStandby(t *testing.T) {
	t.Parallel()
	live, standby := slowUnsubscribeServer(t, never), artemisServer(t)
	cfg := reconnecting(artemisConfig(live))
	cfg.Addrs = []string{live.Addr(), standby.Addr()}
	cfg.DialTimeout = time.Second
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, live, "orders", "workers", 4)
	deliver(t, live, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	live.HoldConnects(true)
	t.Cleanup(func() { live.HoldConnects(false) })
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue()))
	if err == nil {
		t.Fatal("Nack(NackRequeue()) succeeded although the node it left took no new connections")
	}
	if mq.CodeOf(err) != mq.CodeUnavailable || !mq.IsRetryable(err) {
		t.Errorf("a requeue whose replacement DialTimeout ended, under a context with time left, reports %v "+
			"(code %v, retryable %v), want a retryable Unavailable", err, mq.CodeOf(err), mq.IsRetryable(err))
	}
	if got := standby.Accepted(); got != 0 {
		t.Errorf("the requeue's replacement went to the standby (%d connection(s)), want the node it left", got)
	}

	fetchQuietly(src, 2*cfg.DialTimeout)
	if err := standby.WaitSubscribed(consume, 1, budget); err != nil {
		t.Fatalf("the fetch after the requeue never subscribed on the standby: %v (the hung node took %d "+
			"connection(s))", err, live.Accepted())
	}
}

// TestAFailedReplacementSpendsNoReconnectAttempt is a requeue whose
// replacement connection the node refuses for a moment.
//
// The refused dial was counted as a failed re-dial, so under
// ReconnectAttempts=1 the source gave up for good on the requeue's own
// replacement, and every fetch after the node took connections again was
// refused as a spent budget. The replacement is not a reconnection and is not
// counted: the next fetch dials again, and it is that dial the budget counts.
func TestAFailedReplacementSpendsNoReconnectAttempt(t *testing.T) {
	t.Parallel()
	srv := slowUnsubscribeServer(t, never)
	cfg := reconnecting(artemisConfig(srv))
	cfg.ReconnectAttempts = 1
	b := openBackend(t, cfg)
	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]

	srv.RefuseConnects("AMQ229119: this node is not live")
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Nack(ctx, &d, nackOptions(t, mq.NackRequeue())); err == nil {
		t.Fatal("Nack(NackRequeue()) succeeded while the node refused every connection, so the test never " +
			"reached a failed replacement")
	}
	srv.RefuseConnects("")

	deadline := time.Now().Add(budget)
	for srv.Subscribes(consume) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the source never subscribed again once the node took connections: the next fetch "+
				"reports %v\nframes:%s", fetchError(t, src, 10*time.Millisecond), frameLog(srv))
		}
		fetchQuietly(src, 10*time.Millisecond)
	}
	deliver(t, srv, consume, body("two"))
	if got := fetchN(t, src, 1); string(got[0].Message.Body) != "two" {
		t.Errorf("fetched %q after the source re-dialled, want %q", got[0].Message.Body, "two")
	}
}

// TestSourcesRedialAcrossAFailoverListIndependently is the same list with two
// sources whose connections both died.
//
// Re-dialling through one counter, two sources fetching in turn draw
// alternate addresses, so one of them can draw the dead node on every attempt
// and never come back. Each source walks the list on its own, so each reaches
// the live node within one pass.
func TestSourcesRedialAcrossAFailoverListIndependently(t *testing.T) {
	t.Parallel()
	live, dead := artemisServer(t), deadNode(t)
	cfg := reconnecting(artemisConfig(live))
	cfg.Addrs = []string{live.Addr(), dead.Addr()}
	b := openBackend(t, cfg)
	first, _ := openSource(t, b, cfg, live, "orders", "workers", 4)
	second, _ := openSource(t, b, cfg, live, "orders", "billing", 4)

	// The newest connection goes first, so this is the second source's and
	// then the first's; the client's own connection stays up.
	dropConnection(t, live)
	dropConnection(t, live)

	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) && (first.Redials() < 1 || second.Redials() < 1) {
		fetchQuietly(first, 10*time.Millisecond)
		fetchQuietly(second, 10*time.Millisecond)
	}
	for name, src := range map[string]*stomp.Source{"first": first, "second": second} {
		if src.Redials() < 1 {
			t.Errorf("the %s source never re-dialled the live node within %v (the dead node took %d connection(s))",
				name, budget, dead.Accepted())
		}
	}
}

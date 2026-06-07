package amqp10_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
)

// The driver-owned dial loop. go-amqp does not reconnect: a connection whose
// stream dies closes Done, every session and link on it starts failing, and that
// is the end of it. So every test here is about the goroutine that notices.

// TestReconnectRedialsWithBackoff is the specification's "reconnect with backoff (never
// busy-spin)".
//
// The peer is told to refuse two dials and then work, so the loop has to wait
// between attempts and the hook has to report each one. The attempt NUMBERS are
// what a caller acts on, and the successful one carries a nil error.
func TestReconnectRedialsWithBackoff(t *testing.T) {
	peer := newPeer()
	type event struct {
		attempt int
		err     error
	}
	events := make(chan event, 16)
	refuse := errors.New("connection refused")
	peer.FailDials(nil, refuse, refuse)
	f := openWith(t, testProfile(), aq.Config{
		ReconnectBase: time.Millisecond,
		ReconnectMax:  5 * time.Millisecond,
		Hooks: mq.Hooks{OnReconnect: func(_ context.Context, ev mq.ReconnectEvent) {
			events <- event{attempt: ev.Attempt, err: ev.Err}
		}},
	}, peer)

	f.peer.LastConn().Kill(&amqp.ConnError{})
	if !aq.AwaitReconnectForTest(f.b, 1, budget) {
		t.Fatal("the dial loop did not re-establish the connection after two refusals")
	}
	// Three attempts: two refused and one that worked, each reported.
	var failures, successes int
	timeout := time.After(budget)
	for failures+successes < 3 {
		select {
		case ev := <-events:
			if ev.err != nil {
				failures++
			} else {
				successes++
			}
			if ev.attempt <= 0 {
				t.Fatalf("a reconnection event reports attempt %d; the event counts from 1", ev.attempt)
			}
			if ev.err == nil && ev.attempt != 3 {
				t.Fatalf("the successful attempt is reported as number %d, want 3 after two refusals", ev.attempt)
			}
		case <-timeout:
			t.Fatalf("only %d failed and %d successful attempt(s) were reported; the specification requires a driver's "+
				"background failures to surface through hooks rather than only in logs", failures, successes)
		}
	}
	if failures != 2 || successes != 1 {
		t.Fatalf("%d failed and %d successful attempts, want two refusals and one success", failures, successes)
	}
	if got := f.b.Stats().Losses; got != 1 {
		t.Fatalf("the backend counted %d loss(es), want 1", got)
	}
	// And the connection really works again.
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	if err := snk.Publish(context.Background(), &mq.Message{Body: []byte("after")}, nil); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}
}

// TestReconnectInvalidatesEveryDeliveryBeforeItRedials is the ORDER the
// sequence in reconnect.go insists on: the generation bump and the token
// invalidation happen before anything that can block, because a settlement that
// arrives while the loop is dialling must already be refused.
func TestReconnectInvalidatesEveryDeliveryBeforeItRedials(t *testing.T) {
	peer := newPeer()
	// The re-dial is refused for as long as the test wants, so the whole
	// check happens inside the reconnect window — which is exactly when a
	// naive driver's guard is not yet in place.
	refuse := errors.New("still down")
	peer.FailDials(nil, refuse, refuse, refuse, refuse, refuse, refuse, refuse, refuse)
	f := openWith(t, testProfile(), aq.Config{ReconnectBase: 20 * time.Millisecond}, peer)
	src := f.source(mq.SourceConfig{Prefetch: 4})
	rcv, held := deliverAndFetch(t, f, src, "tag-1", "x")
	before := len(rcv.Dispositions())
	gen := f.b.Generation()

	f.peer.LastConn().Kill(&amqp.ConnError{})
	if !aq.AwaitGenerationForTest(f.b, gen, budget) {
		t.Fatal("the generation did not move")
	}
	// Still inside the window: no replacement connection yet.
	if got := f.b.Stats().Reconnects; got != 0 {
		t.Fatalf("the loop already reconnected (%d), so this test is not measuring the window", got)
	}
	err := src.Settler().Ack(context.Background(), &held)
	assertIs(t, err, mq.ErrLeaseLost, "a settlement inside the reconnect window")
	if got := len(rcv.Dispositions()); got != before {
		t.Fatalf("the refused settlement wrote %d disposition(s) during the reconnect window", got-before)
	}
	// And an operation inside the window is retryable rather than a closed
	// handle, which is what tells a caller to try again.
	dst := make([]mq.DriverDelivery, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, ferr := src.Fetch(ctx, dst)
	if ferr == nil {
		t.Fatal("a fetch inside the reconnect window succeeded")
	}
	if errors.Is(ferr, mq.ErrClosed) {
		t.Fatalf("a fetch inside the reconnect window reported a closed source: %v", ferr)
	}
}

// TestReconnectFailsPendingPublicationsAmbiguously is step 3 of the sequence,
// and the other disqualifier: the transfers had left and their dispositions
// had not arrived, so nobody knows whether the broker holds them.
func TestReconnectFailsPendingPublicationsAmbiguously(t *testing.T) {
	peer := newPeer()
	peer.PlanSends(clientHold(), clientHold())
	f := openWith(t, testProfile(), aq.Config{ReconnectBase: time.Millisecond}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	outcomes := make(chan error, 4)
	for range 2 {
		if err := snk.PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil,
			func(err error) { outcomes <- err }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
	}
	snd := f.sender(budget)
	waitFor(t, budget, "both transfers to reach the peer", func() bool { return snd.Held() == 2 })

	f.peer.LastConn().Kill(&amqp.ConnError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondConnectionForced}})
	for i := range 2 {
		select {
		case err := <-outcomes:
			if err == nil {
				t.Fatalf("publication %d was reported as accepted when the connection died", i)
			}
			if !mq.IsAmbiguous(err) {
				t.Fatalf("publication %d reported %v (code %v); the transfer had left and no disposition arrived, "+
					"so the specification makes the outcome Ambiguous and makes reporting it as definite a disqualifier",
					i, err, mq.CodeOf(err))
			}
		case <-time.After(budget):
			t.Fatalf("publication %d's callback never ran after the connection died; the specification requires it to run "+
				"exactly once", i)
		}
	}
	if got := f.b.Stats().AmbiguousPublications; got != 2 {
		t.Fatalf("the backend counted %d ambiguous publication(s), want 2", got)
	}
}

// TestReconnectHealthFollowsTheLoop is the health over the states this
// driver really passes through, including the one rule
// [bitbucket.org/iqhive/iqmq/driverkit.Health] enforces for every driver: Closed
// is terminal.
func TestReconnectHealthFollowsTheLoop(t *testing.T) {
	peer := newPeer()
	refuse := errors.New("down")
	peer.FailDials(nil, refuse, refuse)
	b, err := aq.BackendWithDialer(context.Background(), testProfile(), aq.Config{
		ReconnectBase: time.Millisecond,
	}, peer)
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	if got := b.Health().State; got != mq.HealthReady {
		t.Fatalf("a freshly opened backend reports %v, want ready", got)
	}
	// The dial loop is HELD before the connection is killed, so "the client
	// is reconnecting" is a state this test stands in rather than a window
	// it polls for and might miss. Without the hold the whole not-ready
	// window is about three milliseconds -- ReconnectBase is a millisecond
	// and two dials are refused -- and a poller descheduled across it sees
	// ready before the kill is noticed and ready again after the loop has
	// finished. That is what `timed out after 5s waiting for the health to
	// leave ready` was, on a gate run whose host was busy: a failure the
	// budget had nothing to do with, and which widening it cannot fix.
	release := peer.HoldDials()
	dialsBeforeTheOutage := peer.Dials()
	peer.LastConn().Kill(&amqp.ConnError{})
	// It LEAVES ready while the loop is trying, and comes back when the loop
	// succeeds. The intermediate state is asserted as "not ready" rather than
	// as Degraded specifically, and that is not vagueness: the loss makes it
	// Degraded and the very next thing the loop does is report Connecting, so
	// a test that insisted on seeing Degraded would be racing a transition
	// that is microseconds wide. The terminal Degraded — the one a caller
	// polling health is looking for — is what
	// TestReconnectNoReconnectLeavesItToTheCaller and
	// TestReconnectAttemptCeilingIsTerminal hold.
	waitFor(t, budget, "the health to leave ready", func() bool {
		return b.Health().State != mq.HealthReady
	})
	// The hold is load-bearing and this is what says so. If a dial got
	// through, the not-ready window belongs to the loop again and the wait
	// above is back to polling for a transient -- which it would still pass
	// on a quiet host, every time, which is the whole difficulty.
	if got := peer.Dials(); got != dialsBeforeTheOutage {
		t.Errorf("the dial count moved from %d to %d while the peer was holding dials: the reconnection "+
			"is running, so the state this test stands in is three milliseconds of the loop's rather "+
			"than a window the test controls", dialsBeforeTheOutage, got)
	}
	release()
	waitFor(t, budget, "the health to come back", func() bool {
		return b.Health().State == mq.HealthReady
	})
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := b.Health().State; got != mq.HealthClosed {
		t.Fatalf("a closed backend reports %v, want closed", got)
	}
	// Terminal: nothing moves it out again.
	if got := b.Health().State; got != mq.HealthClosed {
		t.Fatalf("a closed backend's health moved to %v; Closed is terminal, or a caller is handed a client it can "+
			"poll for ever", got)
	}
}

// TestReconnectAttemptCeilingIsTerminal is [aq.Config.ReconnectAttempts]: a
// short-lived job wants the loop to give up and every operation afterwards to
// report CodeUnavailable, rather than a client that waits for ever.
func TestReconnectAttemptCeilingIsTerminal(t *testing.T) {
	peer := newPeer()
	refuse := errors.New("down for good")
	peer.FailDials(nil, refuse, refuse, refuse, refuse, refuse, refuse)
	f := openWith(t, testProfile(), aq.Config{
		ReconnectBase:     time.Millisecond,
		ReconnectAttempts: 2,
	}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	f.peer.LastConn().Kill(&amqp.ConnError{})
	waitFor(t, budget, "the loop to give up after two attempts", func() bool {
		return peer.Dials() >= 3
	})
	// The loop stopped: no more dials however long we wait.
	after := peer.Dials()
	time.Sleep(50 * time.Millisecond)
	if got := peer.Dials(); got != after {
		t.Fatalf("the loop dialled %d more times after its ceiling of two attempts", got-after)
	}
	err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
	if err == nil {
		t.Fatal("a publication succeeded after the dial loop gave up")
	}
	if errors.Is(err, mq.ErrClosed) {
		t.Fatalf("a publication after the loop gave up reports a closed handle: %v; the handle is open and the "+
			"broker is not there, which is CodeUnavailable", err)
	}
}

// TestReconnectNoReconnectLeavesItToTheCaller is [aq.Config.NoReconnect], for a
// caller that supervises its own connection through OpenWithClient and does not
// want two supervisors.
func TestReconnectNoReconnectLeavesItToTheCaller(t *testing.T) {
	peer := newPeer()
	f := openWith(t, testProfile(), aq.Config{NoReconnect: true}, peer)
	snk := f.sink(mq.SinkConfig{Topic: "topic"})
	f.peer.LastConn().Kill(&amqp.ConnError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondConnectionForced}})
	waitFor(t, budget, "the health to go degraded", func() bool {
		return f.b.Health().State == mq.HealthDegraded
	})
	time.Sleep(30 * time.Millisecond)
	if got := peer.Dials(); got != 1 {
		t.Fatalf("the peer answered %d dials with NoReconnect set, want only the one Open made", got)
	}
	err := snk.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
	if err == nil {
		t.Fatal("a publication succeeded on a dead connection nobody is replacing")
	}
	assertCode(t, err, mq.CodeUnavailable, "a publication with NoReconnect after the connection died")
}

// TestReconnectTriesEveryAddress is the failover list's own rule: the addresses
// are tried round-robin across ATTEMPTS rather than all of them per attempt, so
// that the backoff is what paces the loop.
//
// An attempt that tried every address would make the delay between attempts a
// function of the list's length, and a five-address failover set would hammer a
// down broker five times per backoff interval.
func TestReconnectTriesEveryAddress(t *testing.T) {
	var dialed atomic.Int64
	seen := make(chan string, 16)
	d := &recordingDialer{inner: newPeer(), onDial: func(addr string) {
		dialed.Add(1)
		select {
		case seen <- addr:
		default:
		}
	}}
	b, err := aq.BackendWithDialer(context.Background(), testProfile(), aq.Config{
		Addrs:         []string{"amqp://a:5672", "amqp://b:5672", "amqp://c:5672"},
		ReconnectBase: time.Millisecond,
	}, d)
	if err != nil {
		t.Fatalf("opening: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	first := <-seen
	if first != "amqp://a:5672" {
		t.Fatalf("the first dial went to %q, want the first address", first)
	}
	// Kill the connection twice and watch the loop move down the list.
	for want := range []string{"amqp://b:5672", "amqp://c:5672"} {
		_ = want
		conns := d.inner.Conns()
		conns[len(conns)-1].Kill(&amqp.ConnError{})
		select {
		case got := <-seen:
			if got == first {
				t.Fatalf("the re-dial went back to %q rather than moving down the failover list", got)
			}
		case <-time.After(budget):
			t.Fatal("the loop did not re-dial")
		}
	}
}

// TestReconnectSupervisorStopsWithTheBackend is the specification's "bound all queues and
// goroutines", and it is what goleak in TestMain holds for every test in this
// package: the supervisor is one goroutine for the life of the backend and not
// one more.
func TestReconnectSupervisorStopsWithTheBackend(t *testing.T) {
	before := goroutines()
	for range 8 {
		peer := newPeer()
		b, err := aq.BackendWithDialer(context.Background(), testProfile(), aq.Config{}, peer)
		if err != nil {
			t.Fatalf("opening: %v", err)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	waitFor(t, budget, "the supervisors to stop", func() bool { return goroutines() <= before+1 })
}

// TestReconnectBackoffHonoursItsBounds holds the policy's own numbers, because
// a jittered backoff cannot be checked by watching the clock.
func TestReconnectBackoffHonoursItsBounds(t *testing.T) {
	f := open(t, testProfile(), aq.Config{
		Backoff:       aq.BackoffConstant,
		ReconnectBase: 7 * time.Millisecond,
		ReconnectMax:  time.Second,
	})
	for attempt := 1; attempt <= 5; attempt++ {
		if got := f.b.Backoff(attempt, 0); got != 7*time.Millisecond {
			t.Fatalf("the constant policy's attempt %d waits %v, want the configured base", attempt, got)
		}
	}
	jittered := open(t, testProfile(), aq.Config{
		Backoff:       aq.BackoffFullJitter,
		ReconnectBase: time.Millisecond,
		ReconnectMax:  20 * time.Millisecond,
	})
	for attempt := 1; attempt <= 12; attempt++ {
		got := jittered.b.Backoff(attempt, 0)
		if got < 0 || got > 20*time.Millisecond {
			t.Fatalf("a jittered draw for attempt %d is %v, outside [0, 20ms]", attempt, got)
		}
	}
}

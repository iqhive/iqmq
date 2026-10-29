package stomp_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// What OpenSource and OpenSink refuse, and what they do when the broker
// refuses them.
//
// Every test in this package opens against a server that says yes, so the
// guards before the SUBSCRIBE frame and the failures after it ran nowhere --
// and those are the paths where a driver leaves a subscription behind: the
// open has sent the frame, the broker has answered ERROR, and the only thing
// that cleans up is the code being tested.

// TestOpeningASourceWithNoSubscriptionIsRefused.
//
// STOMP has no anonymous consumer: a subscription is the id on the frame, so
// a driver that defaulted it would have two consumers of one destination
// share an id and settle each other's messages.
//
// A destination with no subscription is also a queue every consumer competes
// for, which is not the fan-out -- which is what the guard's own sentence
// says, and why the test asserts on it rather than on the code alone.
func TestOpeningASourceWithNoSubscriptionIsRefused(t *testing.T) {
	srv := artemisServer(t)
	b := openBackend(t, artemisConfig(srv))

	_, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: "orders"})
	if err == nil {
		t.Fatal("a source opened with no subscription")
	}
	if got := mq.CodeOf(err); got != mq.CodeInvalidArgument {
		t.Errorf("a source with no subscription is code %v, want CodeInvalidArgument", got)
	}
	// The MESSAGE as well as the code, because the naming layer refuses an
	// empty subscription too -- with the same code and a sentence about a
	// name set, which tells the caller nothing about what they left out.
	if !strings.Contains(err.Error(), "a source needs a subscription") {
		t.Errorf("a source with no subscription is %v; the guard's own sentence says what is missing, and "+
			"the naming layer's does not", err)
	}
}

// TestOpeningASourceOnATopicTheNamingRefusesFailsBeforeTheFrame.
//
// The destination is derived from the topic, so a topic the derivation
// refuses cannot produce one -- and the refusal belongs before the SUBSCRIBE
// rather than after a broker rejects a destination nobody meant to name.
func TestOpeningASourceOnATopicTheNamingRefusesFailsBeforeTheFrame(t *testing.T) {
	srv := artemisServer(t)
	b := openBackend(t, artemisConfig(srv))

	before := srv.Count("SUBSCRIBE")
	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: "orders.*", Subscription: "workers",
	}); err == nil {
		t.Fatal("a source opened on a wildcard topic")
	}
	if got := srv.Count("SUBSCRIBE"); got != before {
		t.Errorf("the refused open sent %d SUBSCRIBE frame(s); the naming refusal has to come first",
			got-before)
	}
}

// TestASourceWhoseSubscribeIsRefusedReportsItOnTheFetch.
//
// The SUBSCRIBE is sent by the OPEN  and yet the open
// succeeds, because the frame cannot be confirmed: a `receipt` header on a
// SUBSCRIBE makes go-stomp v3.1.5 panic from inside its own process loop, so
// this driver sends none and nothing synchronous can hear a refusal.
// A destination the broker refuses -- Artemis answers
// `AMQ339001 Destination does not exist` and then closes the socket, which is
// what a deployment meets when the topology was never provisioned -- therefore
// surfaces on the first Fetch, where the pump has read the ERROR frame.
//
// It has to surface at all: a driver that swallowed the ERROR would return
// zero deliveries and no error, which a consumer loop reads as an empty
// topic and spins on for ever.
func TestASourceWhoseSubscribeIsRefusedReportsItOnTheFetch(t *testing.T) {
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	_, consume := destinations(t, cfg, "orders", "workers")
	srv.RefuseSubscribe(consume, "AMQ339001 Destination does not exist")
	b := openBackend(t, cfg)

	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatalf("OpenSource: %v; the SUBSCRIBE carries no receipt, so the open itself should succeed and "+
			"the refusal should reach the caller through the first Fetch", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	dst := make([]mq.DriverDelivery, 1)
	n, ferr := src.Fetch(ctx, dst)
	if ferr == nil {
		t.Errorf("Fetch returned %d deliveries and no error against a destination the broker says does not "+
			"exist; a consumer loop reads that as an empty topic and spins on it for ever", n)
	}
}

// TestTheOpenSendsOneSubscribeAndTheFirstFetchSendsNoMore is the other half of
// the same decision, stated the way the driver actually behaves.
//
// The SUBSCRIBE is EAGER. On ActiveMQ Classic it is the
// only topology operation this protocol has -- a consumer queue comes into
// existence when a subscription attaches to it -- so a lazy subscribe makes
// `OpenSource` create nothing and a deployment that opened its consumers and
// then published loses everything published before the first fetch. The lazy
// alternative was written and reverted. What is asserted here is therefore
// ONE subscription taken by the open, and no second one taken by the fetch
// that reads it: a driver that re-subscribed per fetch would reset the
// subscription on every call, which on this protocol is what a requeue is.
//
// # Why neither count is read straight off a return
//
// This replaced `TestNoSubscribeIsSentUntilSomethingFetches`, which asserted
// ZERO SUBSCRIBE frames immediately after `OpenSource` -- the opposite of the
// driver's decision, passing only while the writer goroutine had not run yet.
// `Conn.Subscribe` returns when the frame is QUEUED and the server counts it
// when it has READ it, so the old assertion was a race the test did not
// control: it lost about four times in three thousand runs under
// `GOMAXPROCS=2 -race` and reported "1 SUBSCRIBE frame(s) were sent by the
// open alone".
//
// The same hazard points the other way here, so neither count is taken on a
// bare return. The first waits for the frame ([openSource] does it). The
// second is taken after an ACK the server has been seen to read: every frame
// this source writes goes out on the one socket it owns ,
// so a SUBSCRIBE the fetch had sent would have been read BEFORE that ACK.
func TestTheOpenSendsOneSubscribeAndTheFirstFetchSendsNoMore(t *testing.T) {
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)

	src, consume := openSource(t, b, cfg, srv, "orders", "workers", 4)
	if got := srv.Count(frame.SUBSCRIBE); got != 1 {
		t.Fatalf("the open sent %d SUBSCRIBE frame(s), want exactly 1: on Classic the SUBSCRIBE is what creates "+
			"the consumer queue, so an open that sent none would lose every message published before the first "+
			"fetch.\nframes:%s", got, frameLog(srv))
	}

	deliver(t, srv, consume, body("one"))
	d := fetchN(t, src, 1)[0]
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := src.Settler().Ack(ctx, &d); err != nil {
		t.Fatalf("acknowledging the delivery: %v", err)
	}
	if err := srv.WaitFrames(frame.ACK, 1, budget); err != nil {
		t.Fatalf("the ACK never reached the server, so nothing orders the count below: %v\nframes:%s",
			err, frameLog(srv))
	}
	if got := srv.Count(frame.SUBSCRIBE); got != 1 {
		t.Errorf("the source had sent %d SUBSCRIBE frame(s) by the time its first delivery was acknowledged, "+
			"want 1: the fetch reads the subscription the open took, and re-subscribing is a requeue on this "+
			"protocol.\nframes:%s", got, frameLog(srv))
	}
}

// TestOpeningAHandleOnAClosedBackendIsRefused.
//
// The refusal is asserted rather than located: the layers below CheckOpen
// refuse a closed backend too, so deleting the check here changes nothing a
// test can see. Both refusing is the point, since a handle reaches a backend
// from more than one place.
func TestOpeningAHandleOnAClosedBackendIsRefused(t *testing.T) {
	srv := artemisServer(t)
	b := openBackend(t, artemisConfig(srv))
	if err := b.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}

	if _, err := b.OpenSource(t.Context(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers",
	}); err == nil {
		t.Error("a source opened on a closed backend")
	}
	if _, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: "orders"}); err == nil {
		t.Error("a sink opened on a closed backend")
	}
}

// TestASourceOpenedWhileTheBackendClosesDoesNotOutliveIt is a backend closed
// while an OpenSource is dialling the source's own connection.
//
// The backend's teardown closes the sources it holds, and a source joined that
// set only once it had subscribed, with nothing checking the backend again. A
// teardown that ran in between closed every source but that one, which then
// kept its connection, its subscription and its pump after Close had returned.
// The client's own adopt step refuses a handle from a closed backend, so this
// reaches a caller of the SPI.
func TestASourceOpenedWhileTheBackendClosesDoesNotOutliveIt(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv)
	b := openBackend(t, cfg)
	_, consume := destinations(t, cfg, "orders", "workers")

	srv.GateConnects(true)
	type opened struct {
		src mq.Source
		err error
	}
	out := make(chan opened, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		src, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "workers"})
		out <- opened{src, err}
	}()
	// The backend's CONNECT and the source's.
	if err := srv.WaitFrames(frame.CONNECT, 2, budget); err != nil {
		t.Fatalf("the source never dialled: %v", err)
	}
	if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("closing the backend: %v", err)
	}
	srv.GateConnects(false)
	got := <-out
	if got.err == nil {
		closeSource(t, got.src)
		t.Fatal("OpenSource handed back an open source from a backend whose Close had already returned: nothing " +
			"will close it, so its connection and its pump outlive the client")
	}
	if !errors.Is(got.err, mq.ErrClosed) {
		t.Errorf("OpenSource on a backend closed under it reports %v, want ErrClosed", got.err)
	}
	await(t, "the server to see the source's subscription end", func() bool { return srv.Live(consume) == 0 })
}

// TestOpeningAHandleRespectsACancelledContext.
func TestOpeningAHandleRespectsACancelledContext(t *testing.T) {
	srv := artemisServer(t)
	b := openBackend(t, artemisConfig(srv))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: "orders", Subscription: "workers",
	}); err == nil {
		t.Error("a source opened with a cancelled context")
	}
	if _, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"}); err == nil {
		t.Error("a sink opened with a cancelled context")
	}
}

// TestASourceReportsWhichDestinationItIsOn.
//
// It is what a failure message prints, and the two brokers derive it
// differently -- so a source that reported the topic instead would make
// every Artemis failure read as though it were Classic.
func TestASourceReportsWhichDestinationItIsOn(t *testing.T) {
	srv := artemisServer(t)
	b := openBackend(t, artemisConfig(srv))

	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { closeSource(t, src) })

	typed, ok := src.(*stomp.Source)
	if !ok {
		t.Fatalf("OpenSource returned %T, not this driver's source", src)
	}
	_, consume := destinations(t, artemisConfig(srv), "orders", "workers")
	if got := typed.Destination(); got != consume {
		t.Errorf("the source reports the destination %q, want %q: it is what a failure message prints, and "+
			"the two brokers derive it differently", got, consume)
	}
}

var _ = stompsrv.Options{}

// TestAFetchAfterTheSocketDiesIsRetryable is what a consumer loop reads when
// the source's own connection dies under a parked fetch.
//
// The source's death notice disconnects on go-stomp's reader goroutine before
// the reader closes its own channel, so the library answers the subscription
// with its synthetic "write channel closed" -- every time. The error table did
// not know the sentence, the fetch reported it as CodeUnknown and not
// retryable, and a loop that stops on a non-retryable error gave up on a
// subscription the next fetch would have re-established.
func TestAFetchAfterTheSocketDiesIsRetryable(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)

	type result struct {
		n   int
		err error
	}
	fetched := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), budget)
		defer cancel()
		n, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1))
		fetched <- result{n, err}
	}()
	// Only so that the death usually meets a PARKED fetch and wakes it; a
	// fetch that arrives later reads the same error at its deadline.
	time.Sleep(50 * time.Millisecond)
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	r := <-fetched
	if r.err == nil {
		t.Fatalf("the fetch across the socket's death returned %d deliveries and no error", r.n)
	}
	if !mq.IsRetryable(r.err) || mq.CodeOf(r.err) != mq.CodeUnavailable {
		t.Errorf("the fetch across the socket's death reports %v (code %v, retryable %v), want a retryable "+
			"Unavailable: the next fetch re-dials and re-subscribes, and a caller told otherwise stops consuming",
			r.err, mq.CodeOf(r.err), mq.IsRetryable(r.err))
	}
}

// TestASourceHonoursNoReconnect is the source's own re-dial under
// Config.NoReconnect, which makes a lost connection terminal.
//
// The backend's dial loop honoured it and a source's did not: a source owns its
// own connection and re-dialled on the next fetch whatever the configuration
// said, so a client told not to reconnect reconnected every consumer.
func TestASourceHonoursNoReconnect(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := artemisConfig(srv) // NoReconnect
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}

	// The first fetches may report what the pump read when the socket died,
	// which is retryable and is reported once; what has to follow, and stay,
	// is the refusal to dial. Three in a row is "stays".
	var last error
	for run, deadline := 0, time.Now().Add(budget); run < 3 && time.Now().Before(deadline); {
		last = fetchError(t, src, 100*time.Millisecond)
		run++
		if last == nil || mq.IsRetryable(last) {
			run = 0
		}
	}
	if got := srv.Accepted(); got != 2 {
		t.Errorf("the server took %d connections, want 2 (the backend's and the source's first): the source "+
			"re-dialled %d time(s) under NoReconnect", got, src.Redials())
	}
	if last == nil || mq.IsRetryable(last) || mq.CodeOf(last) != mq.CodeUnavailable {
		t.Errorf("a fetch on a source whose connection is gone for good reports %v (code %v, retryable %v), want a "+
			"non-retryable Unavailable: a retryable answer is a consumer loop retrying a source that will never "+
			"come back", last, mq.CodeOf(last), mq.IsRetryable(last))
	}
}

// TestASourceStopsAfterItsReconnectAttempts is the same promise for the
// attempt budget: ReconnectAttempts consecutive failed dials end an outage, for
// the backend's loop and now for a source's.
func TestASourceStopsAfterItsReconnectAttempts(t *testing.T) {
	t.Parallel()
	const attempts = 2
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	cfg.ReconnectAttempts = attempts
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)
	connects := len(srv.FramesOf(frame.CONNECT))

	srv.RefuseConnects("stompsrv: refusing every new connection")
	if err := srv.DropConnection(); err != nil {
		t.Fatalf("DropConnection: %v", err)
	}
	// Fetch until the budget's refusal has come back three times in a row:
	// each of those fetches would have been another dial, and what the pump
	// read when the socket died may be reported among the first of them.
	var last error
	for run, deadline := 0, time.Now().Add(budget); run < 3 && time.Now().Before(deadline); {
		last = fetchError(t, src, 20*time.Millisecond)
		run++
		if last == nil || !strings.Contains(last.Error(), "ReconnectAttempts") {
			run = 0
		}
	}
	if got := len(srv.FramesOf(frame.CONNECT)) - connects; got != attempts {
		t.Errorf("the source made %d dial(s) after its connection died, want %d (Config.ReconnectAttempts)",
			got, attempts)
	}
	if last == nil || mq.IsRetryable(last) || !strings.Contains(last.Error(), "ReconnectAttempts") {
		t.Errorf("a fetch on a source that has spent its re-dial budget reports %v (retryable %v), want the "+
			"budget's own non-retryable refusal", last, mq.IsRetryable(last))
	}
}

// TestARedialTheCallerCutShortSpendsNoReconnectAttempts is a source
// re-dialling inside fetches whose deadlines end before the broker answers.
//
// The dial runs inside the fetch that needs it and is bounded by that fetch's
// context, so a caller polling with a short deadline cuts short a dial that was
// on its way to working. Each one counted against ReconnectAttempts: with a
// budget of one, the first short fetch ended the source for good, against a
// broker that was up and answering slowly.
func TestARedialTheCallerCutShortSpendsNoReconnectAttempts(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	cfg.ReconnectAttempts = 1
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)

	srv.HoldConnects(true)
	dropConnection(t, srv)
	before := srv.Accepted()
	var last error
	for deadline := time.Now().Add(budget); srv.Accepted()-before < 3 && time.Now().Before(deadline); {
		last = fetchError(t, src, 50*time.Millisecond)
	}
	if got := srv.Accepted() - before; got < 3 {
		t.Fatalf("the source dialled %d time(s) across fetches whose own deadlines ended the dials, then stopped "+
			"dialling: %v (code %v, retryable %v). A dial the caller cut short says nothing about the broker and "+
			"must not spend Config.ReconnectAttempts", got, last, mq.CodeOf(last), mq.IsRetryable(last))
	}
	srv.HoldConnects(false)
	awaitRedial(t, src, 1, func() { fetchQuietly(src, 100*time.Millisecond) })
}

// TestARedialTheCallerCancelledIsNotTheNextFetchsAnswer is a fetch cancelled
// while its re-dial waits for the broker, and the fetch after it.
//
// A failed dial is remembered, so that a fetch before the backoff has passed
// reports it rather than dialling again. A cancelled one was remembered too, so
// the next fetch, with a context nobody had cancelled, was answered with the
// previous caller's context.Canceled and reported CodeCanceled. The clock
// stands still here, so a remembered failure would answer every fetch that
// follows.
func TestARedialTheCallerCancelledIsNotTheNextFetchsAnswer(t *testing.T) {
	t.Parallel()
	srv := artemisServer(t)
	cfg := reconnecting(artemisConfig(srv))
	cfg.Clock = stillClock{at: time.Now()}
	b := openBackend(t, cfg)
	src, _ := openSource(t, b, cfg, srv, "orders", "workers", 4)

	srv.HoldConnects(true)
	dropConnection(t, srv)
	before := srv.Accepted()
	for deadline := time.Now().Add(budget); srv.Accepted() == before && time.Now().Before(deadline); {
		ctx, cancel := context.WithCancel(context.Background())
		stop := time.AfterFunc(50*time.Millisecond, cancel)
		_, _ = src.Fetch(ctx, make([]mq.DriverDelivery, 1))
		stop.Stop()
		cancel()
	}
	if srv.Accepted() == before {
		t.Fatalf("no cancelled fetch re-dialled within %v", budget)
	}
	srv.HoldConnects(false)

	for deadline := time.Now().Add(budget); src.Redials() < 1 && time.Now().Before(deadline); {
		err := fetchError(t, src, 100*time.Millisecond)
		if mq.CodeOf(err) == mq.CodeCanceled || errors.Is(err, context.Canceled) {
			t.Fatalf("a fetch with a live context reports %v (code %v): that is an earlier fetch's cancellation, "+
				"remembered from the dial it cut short", err, mq.CodeOf(err))
		}
	}
	if src.Redials() < 1 {
		t.Fatalf("the source never re-dialled within %v once the broker answered again", budget)
	}
}

// stillClock is a clock whose time never moves, so a source's re-dial backoff
// never passes and a remembered dial failure answers every later fetch.
type stillClock struct{ at time.Time }

// Now is the fixed instant.
func (c stillClock) Now() time.Time { return c.at }

// NewTimer is a timer on the wall clock.
func (stillClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{t: time.NewTimer(d)} }

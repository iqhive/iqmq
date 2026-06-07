package gcppubsub_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/pstest"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"bitbucket.org/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// The two test vehicles, and what each is for.
//
// `live` is the REAL client library against cloud.google.com/go/pubsub/v2's
// own in-process fake service (pstest). It exercises the library's bundler,
// its streaming pull, its flow control and its ordering-key pause — which is
// the only way to prove the driver's interaction with the library rather than
// with a model of it. pstest also has an injection point for a failed
// publication, which is what the ordering test turns on.
//
// `scripted` is [psfake], this driver's own implementation of the client
// interface. It is for what pstest cannot produce on demand: an
// acknowledgement the service refuses, a streaming pull that dies, a delivery
// whose deadline has just expired.

// liveServer is a pstest server and the connection to it.
type liveServer struct {
	srv  *pstest.Server
	conn *grpc.ClientConn
}

// newLive starts a pstest server. It is closed by the test's cleanup.
func newLive(tb testing.TB) *liveServer {
	tb.Helper()
	srv := pstest.NewServer()
	conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = srv.Close()
		tb.Fatalf("dialling pstest: %v", err)
	}
	l := &liveServer{srv: srv, conn: conn}
	tb.Cleanup(func() {
		_ = conn.Close()
		_ = srv.Close()
	})
	return l
}

// open opens a driver client over the pstest server.
func (l *liveServer) open(tb testing.TB, tweak ...func(*gps.Config)) *mq.Client {
	tb.Helper()
	cfg := gps.Config{
		ProjectID:  "iqmq-test",
		AutoCreate: true,
		// The bundler is pinned to one message and a millisecond for the
		// tests, so a publication's confirmation boundary is the service's
		// answer rather than the bundler's timer.
		PublishCountThreshold: 1,
		PublishDelayThreshold: time.Millisecond,
	}
	for _, f := range tweak {
		f(&cfg)
	}
	client, err := pubsub.NewClient(context.Background(), cfg.ProjectID, option.WithGRPCConn(l.conn))
	if err != nil {
		tb.Fatalf("pubsub.NewClient: %v", err)
	}
	c, err := gps.OpenWithClient(context.Background(), client, driverkit.ClientOwned, cfg)
	if err != nil {
		_ = client.Close()
		tb.Fatalf("OpenWithClient: %v", err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// scripted opens a driver client over [psfake].
func scripted(tb testing.TB, tweak ...func(*gps.Config)) (*mq.Client, *psfake.Server) {
	tb.Helper()
	cfg := gps.Config{ProjectID: "iqmq-test", AutoCreate: true}
	for _, f := range tweak {
		f(&cfg)
	}
	srv := psfake.New(cfg.ProjectID)
	b, err := gps.BackendWithAPI(context.Background(), srv, driverkit.ClientOwned, cfg)
	if err != nil {
		tb.Fatalf("BackendWithAPI: %v", err)
	}
	c := gps.ClientFor(b, cfg)
	tb.Cleanup(func() { _ = c.Close() })
	return c, srv
}

// topics are unique per test run, because pstest keeps everything in one
// process and a topic reused across subtests would carry the previous
// subtest's backlog.
var topicSeq atomic.Uint64

// newTopic returns a fresh topic name.
func newTopic(purpose string) string {
	return "t-" + purpose + "-" + strconv.FormatUint(topicSeq.Add(1), 36)
}

// provision creates a topic and a subscription through the portable admin.
func provision(tb testing.TB, c *mq.Client, topic, sub string) {
	tb.Helper()
	admin, err := c.Admin()
	if err != nil {
		tb.Fatalf("Admin: %v", err)
	}
	ctx := context.Background()
	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: topic}); err != nil {
		tb.Fatalf("EnsureTopic(%s): %v", topic, err)
	}
	if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
		tb.Fatalf("EnsureSubscription(%s/%s): %v", topic, sub, err)
	}
}

// receiveOne fetches exactly one delivery, failing the test if none arrives.
func receiveOne(tb testing.TB, c *mq.ConsumerHandle, within time.Duration) mq.Delivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, dst)
	if n != 1 {
		tb.Fatalf("Receive returned %d, %v", n, err)
	}
	return dst[0]
}

// receiveNone asserts that nothing arrives within d, which is what a hold, a
// filter and a settled message each have to prove.
func receiveNone(tb testing.TB, c *mq.ConsumerHandle, d time.Duration) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, dst)
	if n != 0 {
		tb.Fatalf("a delivery arrived when none should have: body %q", dst[0].Body)
	}
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		tb.Fatalf("Receive: %v", err)
	}
}

// fakePublisher returns the fake's publisher for a topic, so that a test can
// ask whether an ordering key is paused.
type pausable interface{ Paused(key string) bool }

// pausedPublisher is the fake publisher a test asserts the pause on.
func pausedPublisher(tb testing.TB, srv *psfake.Server, topic string, ordering bool) (psapi.Publisher, pausable) {
	tb.Helper()
	pub, err := srv.Publisher(psapi.PublisherConfig{Topic: topic, Ordering: ordering})
	if err != nil {
		tb.Fatalf("Publisher: %v", err)
	}
	p, ok := pub.(pausable)
	if !ok {
		tb.Fatalf("the fake's publisher does not report whether a key is paused")
	}
	return pub, p
}

// testClock is a manual [bitbucket.org/iqhive/iqmq.Clock], for the tests about
// the local lease check.
//
// The lease check is the one piece of this driver that depends on time
// passing, and waiting for a real second is how a test becomes flaky on a
// loaded host: the driver takes an injected clock precisely so that "the lease
// has gone" is a statement rather than a sleep.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

// set fixes the clock.
func (c *testClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// advance moves the clock forward.
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Now is the current time.
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer returns a real timer: nothing in these tests drives a timer through
// this clock, and a manual timer nobody fires would hang a Close rather than
// making a test deterministic.
func (c *testClock) NewTimer(d time.Duration) mq.Timer { return realTimer{time.NewTimer(d)} }

// realTimer is the wall-clock timer this clock hands out.
type realTimer struct{ t *time.Timer }

// C is the expiry channel.
func (r realTimer) C() <-chan time.Time { return r.t.C }

// Stop prevents the timer from firing.
func (r realTimer) Stop() bool { return r.t.Stop() }

// Reset re-arms the timer.
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

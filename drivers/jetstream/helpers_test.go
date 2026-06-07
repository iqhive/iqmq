package jetstream_test

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "bitbucket.org/iqhive/iqmq"
	nq "bitbucket.org/iqhive/iqmq/drivers/jetstream"
)

// freshServer starts an embedded nats-server with JetStream enabled and returns
// its client URL, for a test that needs a server nobody else is using.
//
// The embedded server is what makes this module's own tests need no container:
// nats-server is a Go library, so a unit test gets a real broker — the real
// protocol, the real stream store, the real redelivery machinery — for the cost
// of a goroutine and a temporary directory. The container run is still the
// conformance evidence (the conformance suite), because the specification asks
// what a driver does against the shipped image and not against a library the
// test compiled in.
func freshServer(tb testing.TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "iqmq-js-*")
	if err != nil {
		tb.Fatalf("store dir: %v", err)
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: dir, MaxPayload: 1 << 20,
	})
	if err != nil {
		tb.Fatalf("embedded nats-server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		srv.Shutdown()
		tb.Fatal("embedded nats-server did not become ready")
	}
	tb.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		_ = os.RemoveAll(dir)
	})
	return srv.ClientURL()
}

// streamSeq names a fresh stream and topic per call, so nothing in one test
// reaches another even though they share a server.
var streamSeq atomic.Uint64

// streamName is a stream name derived from the test's own name, which is what
// makes a failure readable in `nats stream ls`.
func streamName(tb testing.TB) string {
	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, tb.Name())
	if len(name) > 180 {
		name = name[:180]
	}
	return name + "_" + strconv.FormatUint(streamSeq.Add(1), 36)
}

// baseConfig is the configuration every test starts from: the shared server, a
// stream of this test's own that the driver manages, and a subject tree that
// captures everything.
func baseConfig(tb testing.TB) nq.Config {
	tb.Helper()
	name := streamName(tb)
	return nq.Config{
		Addrs:         []string{sharedAddr},
		Stream:        name,
		SubjectPrefix: strings.ToLower(name),
		ManageStream:  true,
		AckWait:       5 * time.Second,
	}
}

// openClient opens a client against the shared server, closed on cleanup.
func openClient(tb testing.TB, mutate ...func(*nq.Config)) *mq.Client {
	tb.Helper()
	cfg := baseConfig(tb)
	for _, m := range mutate {
		m(&cfg)
	}
	c, err := nq.Open(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("opening a client: %v", err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// openBackend is openClient for a test that needs the driver's own surfaces.
func openBackend(tb testing.TB, mutate ...func(*nq.Config)) *nq.Backend {
	tb.Helper()
	b, _ := openBackendCfg(tb, mutate...)
	return b
}

// openBackendCfg is openBackend for a test that also needs the resolved
// configuration — the stream name and the subject prefix above all, which a test
// that reads the server's own view of a consumer cannot do without.
func openBackendCfg(tb testing.TB, mutate ...func(*nq.Config)) (*nq.Backend, nq.Config) {
	tb.Helper()
	cfg := baseConfig(tb)
	for _, m := range mutate {
		m(&cfg)
	}
	b, err := nq.OpenBackend(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("opening a backend: %v", err)
	}
	tb.Cleanup(func() { _ = b.Close() })
	return b, cfg
}

// topicSeq names a fresh topic per call.
var topicSeq atomic.Uint64

func newTopic(prefix string) string {
	return prefix + "." + strconv.FormatUint(topicSeq.Add(1), 36)
}

// receiveOne waits for one delivery, or fails.
func receiveOne(tb testing.TB, c *mq.ConsumerHandle, budget time.Duration) mq.Delivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := c.Receive(ctx, dst)
	if n != 1 {
		tb.Fatalf("Receive returned %d deliveries, err %v", n, err)
	}
	return dst[0]
}

// tryReceive waits for one delivery and reports whether it arrived, for a test
// whose point is that nothing does.
func tryReceive(tb testing.TB, c *mq.ConsumerHandle, budget time.Duration) (mq.Delivery, bool) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, _ := c.Receive(ctx, dst)
	if n != 1 {
		return mq.Delivery{}, false
	}
	return dst[0], true
}

// consumerOn opens a consumer, closed on cleanup.
func consumerOn(tb testing.TB, c *mq.Client, topic, sub string) *mq.ConsumerHandle {
	tb.Helper()
	return consumerWith(tb, c, mq.ConsumerConfig{Topic: topic, Subscription: sub})
}

// consumerWith opens a consumer from a full configuration, closed on cleanup.
func consumerWith(tb testing.TB, c *mq.Client, cfg mq.ConsumerConfig) *mq.ConsumerHandle {
	tb.Helper()
	h, err := c.Consumer(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("opening a consumer on %s/%s: %v", cfg.Topic, cfg.Subscription, err)
	}
	tb.Cleanup(func() { _ = h.Close() })
	return h
}

// publish is Client.Publish with the test's context and a fatal on failure.
func publish(tb testing.TB, c *mq.Client, topic string, m *mq.Message, opts ...mq.PublishOption) {
	tb.Helper()
	if err := c.Publish(tb.Context(), topic, m, opts...); err != nil {
		tb.Fatalf("publishing to %s: %v", topic, err)
	}
}

// natsConn dials the shared server directly, for a test that needs the client
// library rather than iqmq: the stale-ack evidence, and the streams a test
// provisions itself.
func natsConn(tb testing.TB) *natsgo.Conn {
	tb.Helper()
	nc, err := natsgo.Connect(sharedAddr)
	if err != nil {
		tb.Fatalf("dialling %s: %v", sharedAddr, err)
	}
	tb.Cleanup(nc.Close)
	return nc
}

// provisionStream creates a stream with exactly the configuration the test asks
// for and returns its name, so that a test can open a client against a stream
// the DRIVER did not create.
//
// It is how the version matrix is reachable from a single server: a 2.10 server
// ignores allow_msg_ttl and allow_msg_schedules and produces a stream without
// them, and a stream created here without those flags is the same stream. The
// container matrix (conformance/matrix) runs the real versions; this proves the
// driver reads the flags off the stream rather than off a version string.
func provisionStream(tb testing.TB, mutate func(*sjs.StreamConfig)) string {
	tb.Helper()
	name := streamName(tb)
	nc := natsConn(tb)
	jsc, err := sjs.New(nc)
	if err != nil {
		tb.Fatalf("jetstream context: %v", err)
	}
	cfg := sjs.StreamConfig{
		Name:     name,
		Subjects: []string{strings.ToLower(name) + ".>"},
		Storage:  sjs.FileStorage,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	if _, err := jsc.CreateStream(tb.Context(), cfg); err != nil {
		tb.Fatalf("creating stream %s: %v", name, err)
	}
	return name
}

// openOnStream opens a client bound to a stream the test provisioned, with the
// driver managing nothing.
func openOnStream(tb testing.TB, stream string, mutate ...func(*nq.Config)) *mq.Client {
	tb.Helper()
	cfg := nq.Config{
		Addrs:         []string{sharedAddr},
		Stream:        stream,
		SubjectPrefix: strings.ToLower(stream),
		AckWait:       5 * time.Second,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	c, err := nq.Open(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("opening a client on stream %s: %v", stream, err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// goroutines is the current goroutine count, which is how a test asserts that an
// asynchronous path did not spawn one per message.
func goroutines() int { return runtime.NumGoroutine() }

// shortCtx is an already-cancelled context, for a test that measures a call's
// own machinery rather than what it waits for.
func shortCtx(tb testing.TB) (context.Context, context.CancelFunc) {
	tb.Helper()
	ctx, cancel := context.WithCancel(tb.Context())
	cancel()
	return ctx, func() {}
}

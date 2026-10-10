package nats_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nats"
)

// serverURL starts an embedded nats-server and returns its client URL.
//
// The embedded server is what makes this module's own tests need no container:
// nats-server is a Go library, so a unit test gets a real broker — the real
// protocol, the real header framing, the real slow-consumer machinery — for the
// cost of a goroutine. The container run is still the conformance evidence
// (conformance/suites/nats_test.go), because the specification asks what a driver does
// against the shipped image and not against a library the test compiled in.
func serverURL(tb testing.TB) string {
	tb.Helper()
	opts := &server.Options{
		Host:       "127.0.0.1",
		Port:       -1, // an ephemeral port, so parallel tests never collide
		NoLog:      true,
		NoSigs:     true,
		MaxPayload: 1 << 20,
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		tb.Fatalf("embedded nats-server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		tb.Fatal("embedded nats-server did not become ready")
	}
	tb.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv.ClientURL()
}

// openClient opens a client against a fresh embedded server, closed on cleanup.
func openClient(tb testing.TB, mutate ...func(*nq.Config)) *mq.Client {
	tb.Helper()
	cfg := nq.Config{Addrs: []string{serverURL(tb)}}
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
	cfg := nq.Config{Addrs: []string{serverURL(tb)}}
	for _, m := range mutate {
		m(&cfg)
	}
	b, err := nq.OpenBackend(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("opening a backend: %v", err)
	}
	tb.Cleanup(func() { _ = b.Close() })
	return b
}

// topicSeq names a fresh topic per call, so nothing in one test reaches another
// even when they share a server.
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

// consumerOn opens a consumer, closed on cleanup.
func consumerOn(tb testing.TB, c *mq.Client, topic, sub string) *mq.ConsumerHandle {
	tb.Helper()
	h, err := c.Consumer(tb.Context(), mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		tb.Fatalf("opening a consumer on %s/%s: %v", topic, sub, err)
	}
	tb.Cleanup(func() { _ = h.Close() })
	return h
}

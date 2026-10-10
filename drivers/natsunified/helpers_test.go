package natsunified_test

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/jetstream"
	"github.com/iqhive/iqmq/drivers/nats"
	"github.com/iqhive/iqmq/drivers/natsunified"
)

// Both profiles run against real embedded nats-servers, and both are shared for
// the package.
//
// One server with JetStream enabled would serve both profiles — that is the
// whole reason the two drivers exist side by side — but the facade's own
// question is whether a Core client behaves like a Core client, and a Core
// client on a JetStream-enabled server can only be told apart from one on a
// plain server by what it does NOT do. So there are two: a plain one, on which
// a stream cannot even be asked for, and a JetStream one. Starting them once
// costs two goroutines and a temporary directory for the whole package.
var (
	coreAddr   string
	jsAddr     string
	coreServer *server.Server
	jsServer   *server.Server
	jsStoreDir string
)

// startServers starts the two shared servers. TestMain calls it before m.Run so
// that the runnable examples, which have no *testing.T to start one with, have
// an address to connect to.
func startServers() error {
	var err error
	if coreServer, coreAddr, err = startServer(false); err != nil {
		return err
	}
	jsServer, jsAddr, err = startServer(true)
	return err
}

// coreURL is the plain server's client URL.
func coreURL(tb testing.TB) string {
	tb.Helper()
	if coreAddr == "" {
		tb.Fatal("the shared plain server did not start")
	}
	return coreAddr
}

// jsURL is the JetStream server's client URL.
func jsURL(tb testing.TB) string {
	tb.Helper()
	if jsAddr == "" {
		tb.Fatal("the shared JetStream server did not start")
	}
	return jsAddr
}

// startServer starts an embedded nats-server, with JetStream when asked.
//
// The embedded server is what makes this module's tests need no container:
// nats-server is a Go library, so a unit test gets the real protocol, the real
// header framing and the real stream store for the cost of a goroutine. The
// container run is still the evidence
// (conformance/suites/natsunified_test.go).
func startServer(js bool) (*server.Server, string, error) {
	opts := &server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, MaxPayload: 1 << 20,
	}
	if js {
		dir, err := os.MkdirTemp("", "iqmq-unified-js-*")
		if err != nil {
			return nil, "", err
		}
		jsStoreDir = dir
		opts.JetStream, opts.StoreDir = true, dir
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, "", err
	}
	go srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		srv.Shutdown()
		return nil, "", errNotReady
	}
	url := srv.ClientURL()
	return srv, url, nil
}

// errNotReady is an embedded server that never came up.
var errNotReady = errors.New("embedded nats-server did not become ready")

// stopServers tears the shared servers down. TestMain calls it before goleak
// looks, because a server that is still running keeps goroutines goleak would
// otherwise blame on the facade.
func stopServers() {
	for _, s := range []*server.Server{coreServer, jsServer} {
		if s != nil {
			s.Shutdown()
			s.WaitForShutdown()
		}
	}
	if jsStoreDir != "" {
		_ = os.RemoveAll(jsStoreDir)
	}
}

// waitForNoClients waits until the server has no connections left.
//
// It is what proves that a client the facade refused was CLOSED rather than
// leaked: goleak catches a goroutine, and a *nats.Conn nobody closed is a
// goroutine — but the server's own count is the observation that does not
// depend on which goroutines the client library happens to park.
func waitForNoClients(tb testing.TB, srv *server.Server) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.NumClients() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	tb.Fatalf("the server still has %d client connection(s); a client the facade refused was not closed", srv.NumClients())
}

// nameSeq names a fresh stream, subject tree and subscription per call, so
// nothing in one test reaches another even though they share a server.
var nameSeq atomic.Uint64

// names returns a fresh (stream, subject root) pair derived from the test's
// name, which is what makes a failure readable in `nats stream ls`.
func names(tb testing.TB) (stream, root string) {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, tb.Name())
	if len(safe) > 120 {
		safe = safe[:120]
	}
	safe += "_" + strconv.FormatUint(nameSeq.Add(1), 36)
	return strings.ToUpper(safe), strings.ToLower(safe)
}

// coreConfig is a facade configuration for the Core profile against the shared
// plain server.
func coreConfig(tb testing.TB) natsunified.Config {
	tb.Helper()
	return natsunified.Config{
		Profile: natsunified.ProfileCore,
		Core:    natsCoreConfig(coreURL(tb)),
	}
}

// natsCoreConfig is the NATS Core configuration every Core test starts from.
func natsCoreConfig(addr string) nats.Config {
	return nats.Config{Addrs: []string{addr}, FlushTimeout: 5 * time.Second}
}

// jsConfig is a facade configuration for the JetStream profile against the
// shared JetStream server, with a stream of this test's own that the driver
// manages.
func jsConfig(tb testing.TB) natsunified.Config {
	tb.Helper()
	stream, root := names(tb)
	return natsunified.Config{
		Profile: natsunified.ProfileJetStream,
		JetStream: jetstream.Config{
			Addrs:         []string{jsURL(tb)},
			Stream:        stream,
			SubjectPrefix: root,
			ManageStream:  true,
			AckWait:       5 * time.Second,
		},
	}
}

// coreTopic is a fresh subject for a Core test. NATS Core has no topology, so
// a subject exists because somebody published to it and there is nothing to
// create.
func coreTopic(tb testing.TB) string {
	_, root := names(tb)
	return root + ".t"
}

// openFacade opens a client through the facade, closed on cleanup.
func openFacade(tb testing.TB, cfg natsunified.Config) *mq.Client {
	tb.Helper()
	c, err := natsunified.Open(tb.Context(), cfg)
	if err != nil {
		tb.Fatalf("opening a %s client through the facade: %v", cfg.Profile, err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// receiveOne waits for one delivery, or fails.
func receiveOne(tb testing.TB, h *mq.ConsumerHandle, budget time.Duration) mq.Delivery {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), budget)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := h.Receive(ctx, dst)
	if n != 1 {
		tb.Fatalf("Receive returned %d deliveries, err %v", n, err)
	}
	return dst[0]
}

// exampleCoreConfig and exampleJetStreamConfig are what the runnable examples
// connect with.
//
// They are here rather than in example_test.go because an Example has no
// *testing.T to start a server with; TestMain starts the two shared servers
// before m.Run, and these read the addresses it recorded.
func exampleCoreConfig() nats.Config {
	return natsCoreConfig(coreAddr)
}

func exampleJetStreamConfig() natsunified.Config {
	return natsunified.Config{
		Profile: natsunified.ProfileJetStream,
		JetStream: jetstream.Config{
			Addrs:         []string{jsAddr},
			Stream:        "IQMQ_EXAMPLE",
			SubjectPrefix: "iqmqexample",
			ManageStream:  true,
			AckWait:       5 * time.Second,
		},
	}
}

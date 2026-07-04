package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// wantOutput is what the example prints, byte for byte.
//
// Every line is a fact about the broker rather than about this program: which
// path the requester took, what a subscribed-but-silent responder produces,
// what a cancellation produces, and — the line the whole example is for — that
// the native path answers "nobody is listening" while the emulated path can
// only time out. Comparing the whole block is what makes a change to any of
// them a failure instead of a diff nobody reads.
const wantOutput = "native requester: native=true, reply topology=none\n" +
	"native reply: SKU-1 costs 1299 cents\n" +
	"native timeout: a subscribed responder that does not answer -> the caller's deadline (iqmq code DeadlineExceeded)\n" +
	"native cancelled: a request the caller gave up on -> context canceled\n" +
	"native nobody: no subscriber on rpc.nobody -> iqmq code NotFound, before the deadline\n" +
	"emulated requester: native=false, reply topology=rpc.replies\n" +
	"emulated reply: SKU-2 costs 1499 cents\n" +
	"emulated nobody: no subscriber on rpc.nobody -> the caller's deadline (iqmq code DeadlineExceeded), indistinguishable from slow\n" +
	"emulated pending after the timeout: 0 correlation slots\n"

// TestRunInProcess is the arm that never skips.
//
// nats-server is a Go library, so this is a REAL broker — the real protocol,
// the real header framing, the real "no responders" reply from the server
// itself — for the cost of a goroutine. It is what lets `make ci` on a machine
// with no Docker still hold this example to its output, and it is the reason
// the container arm below is allowed to skip.
func TestRunInProcess(t *testing.T) {
	addr := embeddedServer(t)
	reportArm(t, armInProcess, statusRan, "embedded nats-server "+server.VERSION)
	runExample(t, addr)
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_NATS_ADDR.
//
// It skips when no broker is configured and FAILS when one is: everything
// below the gate is the same assertion the in-process arm makes, so a broker
// that behaves differently from the embedded server is a failure here and not
// a skip. That is the whole point of the gate being an address rather than a
// build tag — the code below is compiled, vetted and linted on every run.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_NATS_ADDR")
	if !ok {
		t.Skip("IQMQ_NATS_ADDR is unset: start a NATS container and set it to host:port to run this example against a broker (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_NATS_ADDR="+addr)
	runExample(t, addr)
}

// runExample opens a client against addr and holds the example to its output.
//
// It is one body for both arms on purpose: an arm that asserted less than the
// other would make "it passed against the broker" mean less than it looks.
func runExample(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	client, backend, err := open(ctx, addr)
	if err != nil {
		t.Fatalf("open(%q): %v", addr, err)
	}
	defer func() {
		if err := client.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	var out bytes.Buffer
	if err := run(ctx, &out, client, backend); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	if got := out.String(); got != wantOutput {
		t.Errorf("the example printed\n%s\nwant\n%s", got, wantOutput)
	}
}

// TestOpenAcceptsBothSpellings pins the one piece of parsing this program does.
//
// The harness convention is a bare host:port and a reader who already has a
// nats:// URL should not have to strip it, so open accepts either. A helper
// that silently mangled one of the two would produce a connection error a
// reader would read as "the broker is down".
func TestOpenAcceptsBothSpellings(t *testing.T) {
	addr := embeddedServer(t)
	for _, spelling := range []string{addr, "nats://" + addr} {
		client, _, err := open(t.Context(), spelling)
		if err != nil {
			t.Fatalf("open(%q): %v", spelling, err)
		}
		if err := client.Ping(t.Context()); err != nil {
			t.Errorf("open(%q) produced a client that cannot ping: %v", spelling, err)
		}
		if err := client.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
	if hasScheme("127.0.0.1:4222") {
		t.Error(`hasScheme("127.0.0.1:4222") is true; a bare host:port would be given a second scheme`)
	}
	if !hasScheme("nats://x") {
		t.Error(`hasScheme("nats://x") is false; a URL would be given a second scheme`)
	}
}

// TestTheSilentBodyIsNotAPrice is the check that says the timeout half of the
// example is about what it claims.
//
// The two timeout lines would pass just as well if bodySilent happened to be a
// SKU the responder does not know — but then the request would be ANSWERED
// ("unknown sku") and the timeout would have to come from somewhere else. This
// is the assertion a well-meaning edit to bodySilent or to prices would break.
func TestTheSilentBodyIsNotAPrice(t *testing.T) {
	if _, ok := prices[bodySilent]; ok {
		t.Fatalf("prices knows %q, so the responder would answer it and the timeout lines would be measuring something else", bodySilent)
	}
	if !strings.HasPrefix(wantOutput, "native requester:") {
		t.Fatal("wantOutput no longer starts with the line the native path prints first")
	}
}

// embeddedServer starts an in-process nats-server and returns its host:port.
//
// Port -1 is an ephemeral port, so two tests in this package never collide,
// and the address is returned bare rather than as a URL so that both arms hand
// open the same shape of string.
func embeddedServer(tb testing.TB) string {
	tb.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		tb.Fatalf("embedded nats-server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		tb.Fatal("the embedded nats-server did not become ready")
	}
	tb.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})
	return srv.Addr().String()
}

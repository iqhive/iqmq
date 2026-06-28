package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// wantOutput is what the example prints, byte for byte.
//
// Every line is a fact about the seam rather than about this program. The last
// four are the whole reason the example exists: a message that was confirmed
// on the destination and not settled on the source is REDELIVERED, published
// again, and ends up on the destination twice. Comparing the whole block is
// what makes a change to any of it a failure instead of a diff nobody reads --
// and in particular it is what would catch a "fix" that quietly settled the
// source before publishing, which would make the duplicate disappear and the
// data loss invisible.
const wantOutput = "settlement: at-least-once across the seam -- publish, confirm, " +
	"then settle the source\n" +
	"moved 3 of 3\n" +
	"destination holds: order-1 order-2 order-3\n" +
	"interrupted transfer: confirmed on the destination, source delivery NOT settled\n" +
	"after the source's ack deadline: redelivered attempt=2, and published again\n" +
	"destination now holds: order-4 order-4\n" +
	"duplicate on the destination: order-4 twice, from one publication and one redelivery\n" +
	"the seam cannot do better without a transaction spanning both brokers, " +
	"which the specification does not offer\n"

// TestRunInProcess is the arm that never skips.
//
// nats-server is a Go library, so this is a REAL JetStream source -- the real
// protocol, the real stream, the real ack deadline and the real redelivery --
// for the cost of a goroutine and a temporary directory. It is what lets
// `make ci` on a machine with no Docker hold this example to its output, and
// it is the reason the container arm below is allowed to skip.
func TestRunInProcess(t *testing.T) {
	addr := embeddedServer(t)
	reportArm(t, armInProcess, statusRan, "embedded nats-server "+server.VERSION+" with JetStream")
	runExample(t, addr)
}

// TestRunAgainstABroker is the container arm, gated on IQMQ_NATS_ADDR.
//
// It skips when no broker is configured and FAILS when one is: everything
// below the gate is the same assertion the in-process arm makes, so a server
// that behaves differently from the embedded one is a failure here and not a
// skip. That is the whole point of the gate being an address rather than a
// build tag -- the code below is compiled, vetted and linted on every run.
//
// The broker must have JetStream enabled (`nats -js`); a plain NATS server
// answers the stream creation with "jetstream not enabled", which is a
// failure and not a skip, because a run that MEANT to reach a broker should
// not read as green.
func TestRunAgainstABroker(t *testing.T) {
	addr, ok := brokerAddr(t, "IQMQ_NATS_ADDR")
	if !ok {
		t.Skip("IQMQ_NATS_ADDR is unset: start a NATS container with JetStream enabled " +
			"(docker run -p 4222:4222 nats -js) and set it to host:port to run this " +
			"example against a broker (see README.md)")
	}
	reportArm(t, armBroker, statusRan, "IQMQ_NATS_ADDR="+addr)
	runExample(t, addr)
}

// runExample opens both ends and holds the example to its output.
//
// It is one body for both arms on purpose: an arm that asserted less than the
// other would make "it passed against the broker" mean less than it looks.
func runExample(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	src, dst, closeAll, err := open(ctx, addr)
	if err != nil {
		t.Fatalf("open(%q): %v", addr, err)
	}
	defer closeAll(ctx)

	var out bytes.Buffer
	if err := run(ctx, &out, src, dst); err != nil {
		t.Fatalf("run: %v\noutput so far:\n%s", err, out.String())
	}
	if got := out.String(); got != wantOutput {
		t.Errorf("the example printed\n%s\nwant\n%s", got, wantOutput)
	}
}

// TestTheDuplicateIsReal is the assertion the golden block above cannot make
// on its own: that the two "order-4" lines are two SEPARATE arrivals rather
// than one message printed twice.
//
// It reads the destination's own count. If the second transfer had settled the
// source before publishing -- the lossy order -- there would be no redelivery
// and exactly one copy, and the golden output would simply have been updated
// to match. This is what stops that.
func TestTheDuplicateIsReal(t *testing.T) {
	addr := embeddedServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	src, dst, closeAll, err := open(ctx, addr)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer closeAll(ctx)

	var out bytes.Buffer
	if err := run(ctx, &out, src, dst); err != nil {
		t.Fatalf("run: %v\noutput:\n%s", err, out.String())
	}
	line := ""
	for _, l := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(l, "destination now holds: ") {
			line = strings.TrimPrefix(l, "destination now holds: ")
		}
	}
	if line == "" {
		t.Fatalf("the example printed no \"destination now holds\" line:\n%s", out.String())
	}
	bodies := strings.Fields(line)
	if len(bodies) != 2 {
		t.Fatalf("the destination received %d cop(y/ies) of the interrupted message (%v); "+
			"the seam is documented to produce two", len(bodies), bodies)
	}
	if bodies[0] != bodies[1] {
		t.Errorf("the two arrivals are %q and %q; they are meant to be the same message",
			bodies[0], bodies[1])
	}
}

// TestNormaliseAcceptsBothSpellings pins the one piece of parsing this program
// does.
//
// The harness convention is a bare host:port, and a reader who already has a
// nats:// URL should not have to strip it.
func TestNormaliseAcceptsBothSpellings(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"127.0.0.1:4222", "nats://127.0.0.1:4222"},
		{"nats://127.0.0.1:4222", "nats://127.0.0.1:4222"},
		{"tls://broker.example:4222", "tls://broker.example:4222"},
	} {
		if got := normalise(c.in); got != c.want {
			t.Errorf("normalise(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// embeddedServer starts an in-process nats-server with JetStream enabled and
// returns its client URL.
//
// The store is a temporary directory and the stream this example creates is
// memory-backed, so nothing survives the test and nothing is left on disk.
func embeddedServer(tb testing.TB) string {
	tb.Helper()
	dir, err := os.MkdirTemp("", "iqmq-bridge-*")
	if err != nil {
		tb.Fatalf("store dir: %v", err)
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: dir,
	})
	if err != nil {
		tb.Fatalf("embedded nats-server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(30 * time.Second) {
		srv.Shutdown()
		tb.Fatal("the embedded nats-server did not become ready")
	}
	tb.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
		_ = os.RemoveAll(dir)
	})
	return srv.ClientURL()
}

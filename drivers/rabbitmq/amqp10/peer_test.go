package amqp10_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	core "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	rmq10 "bitbucket.org/iqhive/iqmq/drivers/rabbitmq/amqp10"
)

// The scripted peer, and why it is go-amqp talking to itself.
//
// Everything this module decides is decided from what the peer said in its
// OPEN frame: the version gate, the product gate, the capability table, the
// driver information and the error mapping. So a test needs a peer that
// completes an AMQP 1.0 connection and says it is a particular RabbitMQ, and
// needs nothing else — no queue, no transfer, no disposition.
//
// The AMQP 1.0 connection handshake is SYMMETRIC: both peers send the protocol
// header and both send an OPEN performative. go-amqp implements that half, so
// an [amqp.NewConn] on the accepted side of a loopback socket is a complete
// and correct peer for it, and [amqp.ConnOptions.Properties] is exactly the
// `product` and `version` a broker announces itself with. Fifty other
// people's containers run on this host; nothing here starts one.
//
// # What it cannot do, and what that costs
//
// It cannot attach a link, transfer a message or take a disposition: go-amqp
// implements the CLIENT role of a link, so the accepted side can dial and
// cannot serve. The shared core has a scripted peer that can — the Fake in
// `drivers/amqp10/internal/client` — and it is unreachable from this module:
// `internal` is importable only from within `drivers/amqp10`, and the core's
// [bitbucket.org/iqhive/iqmq/drivers/amqp10.BackendWithDialer] names that internal
// interface in its signature, so no package outside that subtree can call it.
// Go's own message for the attempt is
// "use of internal package bitbucket.org/iqhive/iqmq/drivers/amqp10/internal/client
// not allowed".
//
// What that costs is stated rather than worked around: the delivery-level half
// of critical_test.go drives the staleness guard through a fabricated delivery
// token instead of a real transfer (see TestCriticalStaleDispositionIsRefused),
// and a link-level test of this profile waits on the core exporting its
// transport seam. Everything else — the gate, the table, the addressing, the
// reconnection — is reachable here.

// peer is a scripted AMQP 1.0 peer on a loopback socket.
type peer struct {
	tb testing.TB
	ln net.Listener

	mu    sync.Mutex
	conns []*amqp.Conn
	nets  []net.Conn
	// killed is how many of nets killAll has already closed, so that a
	// second kill closes only what has been accepted since the first and
	// stop can close everything exactly once more.
	killed int
	done   chan struct{}
	props  map[string]any

	// total counts every connection the peer has ever taken, and is
	// separate from the slices because killAll empties those: a test that
	// read the slice length would see a re-dial as no connection at all.
	total atomic.Int64
}

// rabbitProps are the connection properties RabbitMQ announces itself with.
func rabbitProps(version string) map[string]any {
	return map[string]any{
		"product":  "RabbitMQ",
		"version":  version,
		"platform": "Erlang/OTP 27.0",
	}
}

// newPeer starts a peer that announces props, and stops it when the test ends.
func newPeer(tb testing.TB, props map[string]any) *peer {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listening for the scripted peer: %v", err)
	}
	p := &peer{tb: tb, ln: ln, done: make(chan struct{}), props: props}
	go p.serve()
	tb.Cleanup(p.stop)
	return p
}

// addr is the AMQP URL a client dials.
func (p *peer) addr() string { return "amqp://" + p.ln.Addr().String() }

// serve accepts connections and completes the AMQP 1.0 handshake on each.
//
// A connection whose handshake fails is not a test failure: the version gate's
// own test closes the client's connection as soon as it has read the peer's
// properties, which the peer sees as a connection that went away mid-exchange.
//
// The socket is recorded BEFORE the handshake and that ordering is what makes
// [peer.killAll] reliable. The client's own Open cannot return until this side
// has written its OPEN frame, so recording the socket first puts the record
// before the client is ready — whereas recording it afterwards is a race the
// client wins about one run in twenty, and killAll then finds nothing to close
// and the connection this test needs to die stays alive.
func (p *peer) serve() {
	defer close(p.done)
	for {
		nc, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.total.Add(1)
		p.mu.Lock()
		p.nets = append(p.nets, nc)
		p.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := amqp.NewConn(ctx, nc, &amqp.ConnOptions{
			ContainerID: "scripted-peer",
			Properties:  p.props,
		})
		cancel()
		if err != nil {
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, conn)
		p.mu.Unlock()
	}
}

// stop closes the listener, every connection the peer accepted and every raw
// socket behind them, then waits for the accept loop to finish.
//
// All three are needed for goleak to be satisfied: closing the listener ends
// the loop, closing the *amqp.Conn ends the library's reader and writer
// goroutines, and closing the raw socket covers a connection whose handshake
// never completed and which therefore has no *amqp.Conn to close.
func (p *peer) stop() {
	_ = p.ln.Close()
	<-p.done
	p.mu.Lock()
	conns, nets := p.conns, p.nets
	p.conns, p.nets, p.killed = nil, nil, 0
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	for _, nc := range nets {
		_ = nc.Close()
	}
}

// killAll drops every socket the peer holds without a close performative,
// which is what a broker restart looks like to a client.
//
// It closes the raw socket rather than the *amqp.Conn on purpose: closing the
// connection sends a close performative, which is an orderly shutdown, and
// what this has to reproduce is a connection that simply went away. go-amqp's
// reader and writer then exit on the terminal error and close the connection
// themselves, so [peer.stop]'s later Close is idempotent.
func (p *peer) killAll() {
	p.mu.Lock()
	nets := p.nets[p.killed:]
	p.killed = len(p.nets)
	p.mu.Unlock()
	for _, nc := range nets {
		_ = nc.Close()
	}
}

// accepted is how many connections the peer has taken since it started, which
// is how a test tells a re-dial from a client that gave up.
func (p *peer) accepted() int { return int(p.total.Load()) }

// testConfig is a configuration pointed at the scripted peer.
//
// SASL is NONE because the peer is go-amqp, which implements the client half
// of the SASL exchange and not the server's: a client that offered a SASL
// layer would wait for a mechanisms frame the peer cannot send. It is the one
// setting in these tests that a real RabbitMQ deployment would not use, and
// [rmq10.Config.SASL] documents why the value exists.
func testConfig(p *peer) rmq10.Config {
	return rmq10.Config{
		Addrs: []string{p.addr()},
		SASL:  core.SASLNone,
	}
}

// openClient opens a client against the peer and closes it when the test ends.
func openClient(tb testing.TB, cfg rmq10.Config) *mq.Client {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := rmq10.Open(ctx, cfg)
	if err != nil {
		tb.Fatalf("opening the client: %v", err)
	}
	tb.Cleanup(func() {
		if err := c.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			tb.Errorf("closing the client: %v", err)
		}
	})
	return c
}

// openBackend is [openClient] for a test that needs the driver's own surface.
func openBackend(tb testing.TB, cfg rmq10.Config) *core.Backend {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, err := rmq10.OpenBackend(ctx, cfg)
	if err != nil {
		tb.Fatalf("opening the backend: %v", err)
	}
	tb.Cleanup(func() {
		if err := b.Close(); err != nil && !errors.Is(err, mq.ErrClosed) {
			tb.Errorf("closing the backend: %v", err)
		}
	})
	return b
}

// compile-time proof that the ownership constants a borrowed-connection test
// needs are the kit's own, so that a test cannot pass one this driver invented.
var _ = driverkit.ClientBorrowed

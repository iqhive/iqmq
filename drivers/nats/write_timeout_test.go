package nats_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	mq "github.com/iqhive/iqmq"
	nq "github.com/iqhive/iqmq/drivers/nats"
)

// stallingProxy forwards each connection to a server until stall is called,
// and from then on stops reading what the client sends while it keeps every
// socket open, until resume. To the client that is a server that stays
// connected and stops reading: a wedged broker, or a partition that sends no
// reset.
type stallingProxy struct {
	ln         net.Listener
	target     string
	stalled    chan struct{}
	resumed    chan struct{}
	stop       chan struct{}
	stallOnce  sync.Once
	resumeOnce sync.Once
	stopOnce   sync.Once
	wg         sync.WaitGroup
	mu         sync.Mutex
	closed     bool
	conns      []net.Conn
}

// newStallingProxy starts a proxy in front of the server at url, closed on
// cleanup.
func newStallingProxy(tb testing.TB, url string) *stallingProxy {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	p := &stallingProxy{
		ln:      ln,
		target:  strings.TrimPrefix(url, "nats://"),
		stalled: make(chan struct{}),
		resumed: make(chan struct{}),
		stop:    make(chan struct{}),
	}
	p.wg.Add(1)
	go p.accept()
	tb.Cleanup(p.cut)
	return p
}

// addr is the address a client dials to reach the server through the proxy.
func (p *stallingProxy) addr() string { return "nats://" + p.ln.Addr().String() }

// stall stops reading what every client sends, now and on every connection
// accepted later.
func (p *stallingProxy) stall() { p.stallOnce.Do(func() { close(p.stalled) }) }

// resume reads what every client sends again, the server having caught up.
func (p *stallingProxy) resume() { p.resumeOnce.Do(func() { close(p.resumed) }) }

// cut closes every socket, which fails whatever the client is writing, and
// waits for the proxy's goroutines.
func (p *stallingProxy) cut() {
	p.stopOnce.Do(func() {
		_ = p.ln.Close()
		p.mu.Lock()
		p.closed = true
		conns := p.conns
		p.mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
		close(p.stop)
	})
	p.wg.Wait()
}

func (p *stallingProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		if !p.track(client, server) {
			return
		}
		p.wg.Add(2)
		go func() {
			defer p.wg.Done()
			_, _ = io.Copy(client, server)
		}()
		go func() {
			defer p.wg.Done()
			p.forward(server, client)
		}()
	}
}

// track records a pair of sockets for cut, and closes them instead once the
// proxy has been cut.
func (p *stallingProxy) track(client, server net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = client.Close()
		_ = server.Close()
		return false
	}
	p.conns = append(p.conns, client, server)
	return true
}

// forward copies what the client sends to the server, and while the proxy is
// stalled reads nothing until it resumes or is cut.
func (p *stallingProxy) forward(server, client net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		select {
		case <-p.stalled:
			select {
			case <-p.resumed:
			case <-p.stop:
				return
			}
		default:
		}
		n, err := client.Read(buf)
		if n > 0 {
			if _, werr := server.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// TestAServerThatStopsReadingFailsTheWriteAfterWriteTimeout is a publication
// to a server that stays connected but has stopped reading.
//
// Once nats.go's 32 KiB write buffer is full, PublishMsg writes to the socket
// while it holds the connection's lock, and no context reaches that write.
// The driver set no write bound, so nats.go's own minute applied: a ConfirmNone
// Publish, and every caller queued behind the lock, waited up to a minute
// whatever their contexts said. Config.WriteTimeout is that bound now, and the
// write fails after it. Nothing of the publication that failed was delivered,
// so the failure is definite.
func TestAServerThatStopsReadingFailsTheWriteAfterWriteTimeout(t *testing.T) {
	const writeTimeout = 300 * time.Millisecond
	p := newStallingProxy(t, serverURL(t))
	c, err := nq.Open(t.Context(), nq.Config{Addrs: []string{p.addr()}, WriteTimeout: writeTimeout})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: newTopic("stalled"), Confirmation: mq.ConfirmNone})
	if err != nil {
		_ = c.Close()
		t.Fatalf("publisher: %v", err)
	}
	p.stall()

	type outcome struct {
		n       int
		longest time.Duration
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		body := bytes.Repeat([]byte("x"), 512<<10)
		var o outcome
		for o.n = 1; o.n <= 256; o.n++ {
			start := time.Now()
			o.err = pub.Publish(context.Background(), &mq.Message{Body: body})
			o.longest = max(o.longest, time.Since(start))
			if o.err != nil {
				break
			}
		}
		done <- o
	}()
	var o outcome
	const budget = 15 * time.Second
	select {
	case o = <-done:
	case <-time.After(budget):
		// Cutting the link fails the write, so the goroutine returns.
		p.cut()
		o = <-done
		_ = c.Close()
		t.Fatalf("a Publish to a server that stopped reading was still writing %v later, with WriteTimeout %v; "+
			"cutting the link released it after %v (err %v)", budget, writeTimeout, o.longest.Round(time.Millisecond), o.err)
	}
	p.cut()
	_ = pub.Close()
	_ = c.Close()

	if o.err == nil {
		t.Fatalf("%d publications of 512 KiB to a server that stopped reading all succeeded", o.n-1)
	}
	if o.longest > writeTimeout+2*time.Second {
		t.Errorf("a Publish to a server that stopped reading took %v, want about WriteTimeout (%v)",
			o.longest.Round(time.Millisecond), writeTimeout)
	}
	if mq.IsAmbiguous(o.err) {
		t.Errorf("the write that timed out = %v (code %v), ambiguous; the publication was never delivered", o.err, mq.CodeOf(o.err))
	}
	t.Logf("publication %d failed after %v: %v (code %v, retryable %v)",
		o.n, o.longest.Round(time.Millisecond), o.err, mq.CodeOf(o.err), mq.IsRetryable(o.err))
}

// TestAWriteThatTimesOutOnTLSDoesNotWedgeTheConnection is a write that times
// out on a TLS connection before it has finished one TLS record.
//
// crypto/tls fails every write after one that timed out, and counts only
// whole records as written, so such a write tells nats.go that nothing was
// written. nats.go closes the socket only after a write that sent part of the
// buffer, so it kept this one: it read on, and every later write failed. Once
// the server read again the connection stayed unusable until the ping check
// noticed, minutes later by default. The driver asks nats.go to reconnect when
// the flusher's write fails, so the next write reconnects it.
//
// Each publication is written on its own and is smaller than a TLS record, so
// the write that times out is always one record, unfinished.
func TestAWriteThatTimesOutOnTLSDoesNotWedgeTheConnection(t *testing.T) {
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		TLS: true, TLSConfig: selfSignedTLS(t), TLSTimeout: 2,
	})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	p := newStallingProxy(t, "127.0.0.1:"+strconv.Itoa(srv.Addr().(*net.TCPAddr).Port))
	b, err := nq.OpenBackend(t.Context(), nq.Config{
		Addrs:        []string{p.addr()},
		TLS:          &tls.Config{MinVersion: tls.VersionTLS12},
		WriteTimeout: 300 * time.Millisecond, ConnectTimeout: time.Second, ReconnectWait: 50 * time.Millisecond,
		InsecureSkipVerify: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	c := mq.NewClient(b)
	defer func() { _ = c.Close() }()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: newTopic("tls"), Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	nc := b.Conn()
	p.stall()
	stalled := time.Now()
	body := bytes.Repeat([]byte("t"), 12<<10)
	deadline := time.Now().Add(30 * time.Second)
	for nc.LastError() == nil && nc.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("no write timed out within 30s of the server ceasing to read")
		}
		if err := pub.Publish(t.Context(), &mq.Message{Body: body}); err != nil {
			break
		}
		// Wait for the flusher to write it, so that the next one is
		// written on its own.
		for {
			n, err := nc.Buffered()
			if err != nil || n == 0 || !nc.IsConnected() {
				break
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	failed, status, reconnects := nc.LastError(), nc.Status(), nc.Stats().Reconnects
	p.resume()
	resumed := time.Now()

	var ping error
	for time.Since(resumed) < 5*time.Second {
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		ping = b.Ping(ctx)
		cancel()
		if ping == nil {
			break
		}
	}
	if ping != nil {
		t.Fatalf("the connection was still unusable 5s after the server read again (last ping %v); "+
			"when the write failed (%v) it was %v with %d reconnections, and it is now %v with %d",
			ping, failed, status, reconnects, nc.Status(), nc.Stats().Reconnects)
	}
	t.Logf("the write failed %v after the server stopped reading (%v), with the connection %v; usable %v after the server read again, after %d reconnections",
		resumed.Sub(stalled).Round(time.Millisecond), failed, status, time.Since(resumed).Round(time.Millisecond), nc.Stats().Reconnects)
}

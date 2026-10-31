package jetstream_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	sjs "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// TestASynchronousPublicationTheConnectionCutIsAmbiguous is a Publish waiting
// for its PubAck when the connection is cut under it. A synchronous
// publication is a request, and nats.go fails a request still waiting for its
// reply with ErrDisconnected when it is told to reconnect and with
// ErrConnectionClosed when it closes. The driver read the first as a definite,
// retryable Unavailable and the second as a definite Closed, as it does for a
// write refused before anything left, so the core retried a message the stream
// already held.
//
// The stream is created with NoAck and holds the message before the cut, so
// nothing but the cut answers the publication, and the PubAck timeout is a
// minute. A reconnection is the drop the asynchronous paths already answer
// SettlementUnknown. A close stays Closed, because the connection is not
// coming back, and is ambiguous too.
func TestASynchronousPublicationTheConnectionCutIsAmbiguous(t *testing.T) {
	cuts := []struct {
		name string
		cut  func(*natsgo.Conn) error
		code mq.ErrorCode
	}{
		{"a forced reconnection", (*natsgo.Conn).ForceReconnect, mq.CodeSettlementUnknown},
		{"the connection closes", func(nc *natsgo.Conn) error { nc.Close(); return nil }, mq.CodeClosed},
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			stream := provisionStream(t, func(cfg *sjs.StreamConfig) { cfg.NoAck = true })
			b := openBackend(t, func(cfg *nq.Config) {
				cfg.Stream, cfg.SubjectPrefix, cfg.ManageStream = stream, strings.ToLower(stream), false
				cfg.PublishTimeout = time.Minute
			})
			snk := sinkOn(t, b, newTopic("cut"), mq.ConfirmAccepted)
			res := make(chan error, 1)
			go func() { res <- snk.Publish(t.Context(), &mq.Message{Body: []byte("cut")}, nil) }()
			waitForStored(t, stream, 1, 10*time.Second)
			if err := cut.cut(b.Conn()); err != nil {
				t.Fatalf("cutting the connection: %v", err)
			}
			var err error
			select {
			case err = <-res:
			case <-time.After(10 * time.Second):
				t.Fatal("the publication did not return after the connection was cut")
			}
			if !mq.IsAmbiguous(err) || mq.CodeOf(err) != cut.code {
				t.Errorf("a publication the stream holds and %s cut reported %v (code %v, ambiguous %v, retryable %v), "+
					"want an ambiguous %v: a retry duplicates the message",
					cut.name, err, mq.CodeOf(err), mq.IsAmbiguous(err), mq.IsRetryable(err), cut.code)
			}
			// The flag is read off the *mq.OpError a close answers with,
			// because the ambiguity hides it from mq.IsRetryable. A closed
			// connection is not coming back, so it stays false.
			var oe *mq.OpError
			if errors.As(err, &oe) && oe.Retryable {
				t.Errorf("a publication %s cut reported %v with the retryable flag set; the connection is not coming back",
					cut.name, err)
			}
		})
	}
}

// TestASynchronousPublicationOnAClosedConnectionIsDefinite is the other side
// of the close. nats.go refuses a request on a connection that has already
// closed before it writes anything, so nothing reached the stream and the
// answer stays a definite Closed.
func TestASynchronousPublicationOnAClosedConnectionIsDefinite(t *testing.T) {
	b := openBackend(t)
	snk := sinkOn(t, b, newTopic("closed"), mq.ConfirmAccepted)
	b.Conn().Close()
	err := snk.Publish(t.Context(), &mq.Message{Body: []byte("refused")}, nil)
	var oe *mq.OpError
	if !errors.As(err, &oe) || oe.Code != mq.CodeClosed || mq.IsAmbiguous(err) {
		t.Fatalf("a publication on a closed connection reported %v (code %v, ambiguous %v), want a definite Closed: nothing was written",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
}

// withholdingDialer dials the server for real and hands nats.go connections
// that, once held, keep back every byte the server sends until the link is
// cut: a server that has stopped answering while the TCP connection stays up.
type withholdingDialer struct {
	mu    sync.Mutex
	conns []*withholdingConn
}

func (d *withholdingDialer) Dial(network, address string) (net.Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	wc := &withholdingConn{Conn: c, cut: make(chan struct{})}
	d.mu.Lock()
	d.conns = append(d.conns, wc)
	d.mu.Unlock()
	return wc, nil
}

// current is the connection the client dialled last.
func (d *withholdingDialer) current() *withholdingConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conns[len(d.conns)-1]
}

// withholdingConn withholds what the server sends once hold is called, until
// the link is cut, and counts the publications and PINGs the client writes
// after that.
type withholdingConn struct {
	net.Conn
	mu      sync.Mutex
	holding bool
	writes  atomic.Int64
	cut     chan struct{}
	cutOnce sync.Once
}

// wrote waits up to five seconds for n publications or PINGs to have been
// written since hold.
func (c *withholdingConn) wrote(n int64) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if c.writes.Load() >= n {
			return true
		}
	}
	return c.writes.Load() >= n
}

func (c *withholdingConn) hold() {
	c.mu.Lock()
	c.holding = true
	c.mu.Unlock()
}

func (c *withholdingConn) isHolding() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.holding
}

// drop cuts the link.
func (c *withholdingConn) drop() { c.cutOnce.Do(func() { close(c.cut) }) }

func (c *withholdingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.isHolding() {
		<-c.cut
		return 0, errors.New("the test cut the link")
	}
	return n, err
}

func (c *withholdingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if c.isHolding() {
		c.writes.Add(int64(bytes.Count(p[:n], []byte("PUB ")) + bytes.Count(p[:n], []byte("PING\r\n"))))
	}
	return n, err
}

func (c *withholdingConn) Close() error {
	c.drop()
	return c.Conn.Close()
}

// TestBackendCloseEndsAPublicationInFlight holds that Backend.Close ends a
// publication still waiting for its PubAck, and a Flush or a Ping still
// waiting for the server, on a borrowed connection and on an owned one. On a
// borrowed connection nothing woke a Publish or a Ping, and on an owned one
// closing the connection woke neither a PublishBatch nor a Flush, so with a
// server that had stopped answering they outlived Close by up to
// Config.PublishTimeout. The stream may hold a publication that had left, so
// it is SettlementUnknown, as an asynchronous one outstanding at Close is; a
// Ping is a definite Closed.
func TestBackendCloseEndsAPublicationInFlight(t *testing.T) {
	// writes is how many publications or PINGs a path writes. Close waits
	// for the last: one closed before it was written is refused before
	// anything leaves, which is a definite Closed and not what is tested.
	paths := []struct {
		name   string
		wait   func(*nq.Backend, mq.Sink) error
		writes int64
		code   mq.ErrorCode
	}{
		{"Publish", func(_ *nq.Backend, s mq.Sink) error {
			return s.Publish(context.Background(), &mq.Message{Body: []byte("x")}, nil)
		}, 1, mq.CodeSettlementUnknown},
		{"PublishBatch", func(_ *nq.Backend, s mq.Sink) error {
			return s.(mq.BatchSink).PublishBatch(context.Background(),
				[]*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}, nil)
		}, 2, mq.CodeSettlementUnknown},
		{"Flush", func(_ *nq.Backend, s mq.Sink) error {
			if err := s.(mq.AsyncSink).PublishAsync(context.Background(), &mq.Message{Body: []byte("x")}, nil, func(error) {}); err != nil {
				return err
			}
			return s.(mq.FlushSink).Flush(context.Background())
		}, 1, mq.CodeSettlementUnknown},
		{"Ping", func(b *nq.Backend, _ mq.Sink) error { return b.Ping(context.Background()) }, 1, mq.CodeClosed},
	}
	for _, own := range []driverkit.Ownership{driverkit.ClientBorrowed, driverkit.ClientOwned} {
		for _, path := range paths {
			t.Run(own.String()+"/"+path.name, func(t *testing.T) { closeEnds(t, own, path.wait, path.writes, path.code) })
		}
	}
}

// closeEnds starts wait on a backend whose server has stopped answering,
// closes the backend once wait has written writes publications or PINGs, and
// checks that wait returns at once with code.
func closeEnds(t *testing.T, own driverkit.Ownership, wait func(*nq.Backend, mq.Sink) error, writes int64, code mq.ErrorCode) {
	dialer := &withholdingDialer{}
	nc, err := natsgo.Connect(sharedAddr, natsgo.SetCustomDialer(dialer), natsgo.MaxReconnects(0))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	cfg := baseConfig(t)
	cfg.PublishTimeout = 8 * time.Second
	b, err := nq.BackendWithClient(t.Context(), nc, own, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	snk, err := b.OpenSink(t.Context(), mq.SinkConfig{Topic: newTopic("orders"), Confirmation: mq.ConfirmDurable})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	conn := dialer.current()
	t.Cleanup(conn.drop)
	conn.hold()
	res := make(chan error, 1)
	go func() { res <- wait(b, snk) }()
	if !conn.wrote(writes) {
		t.Fatalf("%d of %d publications or PINGs were written", conn.writes.Load(), writes)
	}
	closed := time.Now()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var perr error
	select {
	case perr = <-res:
	case <-time.After(2 * time.Second):
		perr = <-res
		t.Fatalf("Backend.Close returned, but the wait on the %s connection returned %v later, with %v (code %v)",
			own, time.Since(closed).Round(time.Millisecond), perr, mq.CodeOf(perr))
	}
	outcomes := []error{perr}
	var be *mq.BatchError
	if errors.As(perr, &be) {
		outcomes = be.Errs
	}
	for i, err := range outcomes {
		if mq.CodeOf(err) != code || mq.IsAmbiguous(err) != (code == mq.CodeSettlementUnknown) {
			t.Errorf("outcome %d of a wait Close ended on the %s connection is %v (code %v, ambiguous %v), want %v",
				i, own, err, mq.CodeOf(err), mq.IsAmbiguous(err), code)
		}
	}
	if !own.Owned() && nc.IsClosed() {
		t.Fatal("Close closed the borrowed connection")
	}
}

// sinkOn opens an SPI sink on topic with confirm, closed on cleanup.
func sinkOn(tb testing.TB, b *nq.Backend, topic string, confirm mq.ConfirmationMode) mq.Sink {
	tb.Helper()
	snk, err := b.OpenSink(tb.Context(), mq.SinkConfig{Topic: topic, Confirmation: confirm})
	if err != nil {
		tb.Fatalf("OpenSink: %v", err)
	}
	tb.Cleanup(func() { _ = snk.Close() })
	return snk
}

// waitForStored waits until stream holds at least n messages. On a NoAck
// stream it is the only sign that a publication has reached the stream, since
// nothing answers it.
func waitForStored(tb testing.TB, stream string, n uint64, budget time.Duration) {
	tb.Helper()
	jsc, err := sjs.New(natsConn(tb))
	if err != nil {
		tb.Fatalf("jetstream context: %v", err)
	}
	for deadline := time.Now().Add(budget); ; time.Sleep(time.Millisecond) {
		st, err := jsc.Stream(tb.Context(), stream)
		if err != nil {
			tb.Fatalf("looking up stream %s: %v", stream, err)
		}
		info, err := st.Info(tb.Context())
		if err != nil {
			tb.Fatalf("reading stream %s: %v", stream, err)
		}
		if info.State.Msgs >= n {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("stream %s held %d messages after %v, want %d", stream, info.State.Msgs, budget, n)
		}
	}
}

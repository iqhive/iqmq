package jetstream_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
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
	sent    bytes.Buffer // what the client wrote since hold, under mu
	writes  atomic.Int64
	cut     chan struct{}
	cutOnce sync.Once
}

// wroteTo waits up to five seconds for a publication to subject to have been
// written since hold. A source's consume loop writes pull requests of its
// own, so a count of publications cannot say which one left.
func (c *withholdingConn) wroteTo(subject string) bool {
	want := []byte("PUB " + subject + " ")
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		c.mu.Lock()
		ok := bytes.Contains(c.sent.Bytes(), want)
		c.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
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
	c.mu.Lock()
	if c.holding {
		c.sent.Write(p[:n])
		c.writes.Add(int64(bytes.Count(p[:n], []byte("PUB ")) + bytes.Count(p[:n], []byte("PING\r\n"))))
	}
	c.mu.Unlock()
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
// Ping is a definite Closed, and an OpenSource waiting for its durable's
// create fails with Closed.
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
		// The durable's create is a JetStream API request.
		{"OpenSource", func(b *nq.Backend, _ mq.Sink) error {
			_, err := b.OpenSource(context.Background(), mq.SourceConfig{Topic: newTopic("open"), Subscription: "workers", Prefetch: 1})
			return err
		}, 1, mq.CodeClosed},
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

// TestBackendCloseOnASilentServerDrainsTheSourcesTogether holds that
// Backend.Close on a server that has stopped answering waits for the drains of
// all its sources together, up to one Config.DrainTimeout, on a borrowed
// connection and on an owned one. It closed them one after another and each
// waited for its own drain, so a process shutting down through a partition
// with 20 consumers waited out 20 drains.
func TestBackendCloseOnASilentServerDrainsTheSourcesTogether(t *testing.T) {
	const sources, drain = 4, 2 * time.Second
	for _, own := range []driverkit.Ownership{driverkit.ClientBorrowed, driverkit.ClientOwned} {
		t.Run(own.String(), func(t *testing.T) {
			dialer := &withholdingDialer{}
			nc, err := natsgo.Connect(sharedAddr, natsgo.SetCustomDialer(dialer), natsgo.MaxReconnects(0))
			if err != nil {
				t.Fatalf("dialling: %v", err)
			}
			t.Cleanup(nc.Close)
			cfg := baseConfig(t)
			cfg.DrainTimeout = drain
			b, err := nq.BackendWithClient(t.Context(), nc, own, cfg)
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			for i := range sources {
				src := mq.SourceConfig{Topic: newTopic("stall"), Subscription: "workers" + strconv.Itoa(i), Prefetch: 1}
				if _, err := b.OpenSource(t.Context(), src); err != nil {
					t.Fatalf("OpenSource: %v", err)
				}
			}
			conn := dialer.current()
			t.Cleanup(conn.drop)
			conn.hold()
			start := time.Now()
			if err := b.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if took := time.Since(start); took > drain+drain/2 {
				t.Fatalf("Backend.Close of %d sources on a server that stopped answering took %v with a DrainTimeout of %v, want one DrainTimeout for them all",
					sources, took.Round(time.Millisecond), drain)
			}
		})
	}
}

// TestBackendCloseEndsADeadLetterCopyInFlight holds that Backend.Close ends a
// Nack(DeadLetter) whose copy is waiting for its PubAck, on a borrowed
// connection and on an owned one, with SettlementUnknown: the copy had left,
// so the dead-letter subject's stream may hold it.
func TestBackendCloseEndsADeadLetterCopyInFlight(t *testing.T) {
	for _, own := range []driverkit.Ownership{driverkit.ClientBorrowed, driverkit.ClientOwned} {
		t.Run(own.String(), func(t *testing.T) {
			b, _, res := deadLetterInFlight(t, own)
			closed := time.Now()
			if err := b.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			var nerr error
			select {
			case nerr = <-res:
			case <-time.After(2 * time.Second):
				nerr = <-res
				t.Fatalf("Backend.Close returned, but the dead-letter Nack on the %s connection returned %v later, with %v (code %v)",
					own, time.Since(closed).Round(time.Millisecond), nerr, mq.CodeOf(nerr))
			}
			if mq.CodeOf(nerr) != mq.CodeSettlementUnknown || !mq.IsAmbiguous(nerr) {
				t.Fatalf("a dead-letter Nack Close ended on the %s connection = %v (code %v, ambiguous %v), want an ambiguous SettlementUnknown",
					own, nerr, mq.CodeOf(nerr), mq.IsAmbiguous(nerr))
			}
		})
	}
}

// TestADeadLetterCopyTheConnectionCutIsAmbiguous holds that a
// Nack(DeadLetter) whose copy was waiting for its PubAck when the connection
// was cut under it is ambiguous, as a synchronous publication is
// (TestASynchronousPublicationTheConnectionCutIsAmbiguous): the copy had left,
// so the dead-letter subject's stream may hold it. nats.go fails the wait with
// ErrDisconnected when the client is told to reconnect and with
// ErrConnectionClosed when the connection closes, and the driver read them as
// a definite, retryable Unavailable and a definite Closed, which left the
// delivery settleable for a second copy.
func TestADeadLetterCopyTheConnectionCutIsAmbiguous(t *testing.T) {
	for _, cut := range []struct {
		name string
		cut  func(*natsgo.Conn) error
		code mq.ErrorCode
	}{
		{"a forced reconnection", (*natsgo.Conn).ForceReconnect, mq.CodeSettlementUnknown},
		{"the connection closes", func(nc *natsgo.Conn) error { nc.Close(); return nil }, mq.CodeClosed},
	} {
		t.Run(cut.name, func(t *testing.T) {
			_, nc, res := deadLetterInFlight(t, driverkit.ClientBorrowed)
			if err := cut.cut(nc); err != nil {
				t.Fatalf("cutting the connection: %v", err)
			}
			var err error
			select {
			case err = <-res:
			case <-time.After(5 * time.Second):
				t.Fatal("the dead-letter Nack did not return after the connection was cut")
			}
			if !mq.IsAmbiguous(err) || mq.CodeOf(err) != cut.code {
				t.Fatalf("a dead-letter Nack whose copy had left when %s cut it = %v (code %v, ambiguous %v), want an ambiguous %v",
					cut.name, err, mq.CodeOf(err), mq.IsAmbiguous(err), cut.code)
			}
		})
	}
}

// TestADeadLetterCopyOnAClosedConnectionIsDefinite is the other side of the
// close. nats.go refuses a request on a connection that has already closed
// before it writes anything, so no copy was stored and the Nack stays a
// definite Closed.
func TestADeadLetterCopyOnAClosedConnectionIsDefinite(t *testing.T) {
	r := newDeadLetterRig(t, driverkit.ClientBorrowed)
	r.nc.Close()
	if err := <-r.nack(); mq.CodeOf(err) != mq.CodeClosed || mq.IsAmbiguous(err) {
		t.Fatalf("a dead-letter Nack on a closed connection = %v (code %v, ambiguous %v), want a definite Closed: nothing was written",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
}

// deadLetterRig is a source with a dead-letter topic, on a connection the
// test dialled through a withholdingDialer and the backend has as own, and
// one delivery fetched from it.
type deadLetterRig struct {
	b    *nq.Backend
	nc   *natsgo.Conn
	conn *withholdingConn
	src  mq.Source
	d    mq.DriverDelivery
	dead string // the dead-letter subject
}

func newDeadLetterRig(t *testing.T, own driverkit.Ownership) *deadLetterRig {
	t.Helper()
	dialer := &withholdingDialer{}
	nc, err := natsgo.Connect(sharedAddr, natsgo.SetCustomDialer(dialer))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	cfg := baseConfig(t)
	// The server stops answering, so the source's drain waits out
	// DrainTimeout, which is not what is tested.
	cfg.PublishTimeout, cfg.DeadLetterTopic, cfg.DrainTimeout = 8*time.Second, "dead", 100*time.Millisecond
	b, err := nq.BackendWithClient(t.Context(), nc, own, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	topic := newTopic("orders")
	src, err := b.OpenSource(t.Context(), mq.SourceConfig{Topic: topic, Subscription: "workers", Prefetch: 1})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	if err := sinkOn(t, b, topic, mq.ConfirmDurable).Publish(t.Context(), &mq.Message{Body: []byte("x")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	dst := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(t.Context(), dst); n != 1 {
		t.Fatalf("Fetch = %d, %v", n, err)
	}
	conn := dialer.current()
	t.Cleanup(conn.drop)
	dead, err := nq.Subject(cfg.SubjectPrefix, cfg.DeadLetterTopic)
	if err != nil {
		t.Fatalf("the dead-letter subject: %v", err)
	}
	return &deadLetterRig{b: b, nc: nc, conn: conn, src: src, d: dst[0], dead: dead}
}

// nack starts a Nack(DeadLetter) of the rig's delivery; its answer arrives
// on the channel.
func (r *deadLetterRig) nack() <-chan error {
	res := make(chan error, 1)
	go func() {
		res <- r.src.Settler().Nack(context.Background(), &r.d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	}()
	return res
}

// deadLetterInFlight starts a Nack(DeadLetter) through a server that has
// stopped answering, and returns once the copy has been written.
func deadLetterInFlight(t *testing.T, own driverkit.Ownership) (*nq.Backend, *natsgo.Conn, <-chan error) {
	t.Helper()
	r := newDeadLetterRig(t, own)
	r.conn.hold()
	res := r.nack()
	if !r.conn.wroteTo(r.dead) {
		t.Fatal("the dead-letter copy was not written")
	}
	return r.b, r.nc, res
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

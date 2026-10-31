package nats_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/nats"
)

// withholdingDialer dials the server for real and hands nats.go connections
// that can hold back what the server sends. It is how a test keeps a flush
// waiting for a PONG the server has already answered with, so the flush is
// still pending when the test cuts the link.
type withholdingDialer struct {
	mu    sync.Mutex
	conns []*withholdingConn
}

func (d *withholdingDialer) Dial(network, address string) (net.Conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	wc := &withholdingConn{Conn: c, pinged: make(chan struct{}), cut: make(chan struct{})}
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

// withholdingConn withholds every byte the server sends once hold is called,
// until the link is cut, and reports the first PING the client writes after
// that.
type withholdingConn struct {
	net.Conn
	mu       sync.Mutex
	holding  bool
	pinged   chan struct{}
	cut      chan struct{}
	cutOnce  sync.Once
	pingOnce sync.Once
}

// errLinkCut is what a read on a cut link returns.
var errLinkCut = errors.New("the test cut the link")

// hold starts withholding what the server sends.
func (c *withholdingConn) hold() {
	c.mu.Lock()
	c.holding = true
	c.mu.Unlock()
}

// drop cuts the link, as a network failure does: the client's next read fails
// and it starts to reconnect.
func (c *withholdingConn) drop() { c.cutOnce.Do(func() { close(c.cut) }) }

func (c *withholdingConn) isHolding() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.holding
}

func (c *withholdingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.isHolding() {
		<-c.cut
		return 0, errLinkCut
	}
	return n, err
}

func (c *withholdingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if c.isHolding() && bytes.Contains(p[:n], []byte("PING\r\n")) {
		c.pingOnce.Do(func() { close(c.pinged) })
	}
	return n, err
}

func (c *withholdingConn) Close() error {
	c.drop()
	return c.Conn.Close()
}

// The three ways a pending flush is cut short.
func dropLink(_ *natsgo.Conn, c *withholdingConn) error { c.drop(); return nil }

func forceReconnect(nc *natsgo.Conn, _ *withholdingConn) error { return nc.ForceReconnect() }

func closeConn(nc *natsgo.Conn, _ *withholdingConn) error { nc.Close(); return nil }

// TestAConfirmationFlushTheLinkCutIsAmbiguous is a publication under
// ConfirmAccepted whose flush the link cuts. nats.go clears every pending flush
// when the link drops or the client is told to reconnect, and the flush returns
// ErrConnectionClosed although the connection is only reconnecting. The driver
// read that as a definite Closed, as it does for a write the closed connection
// refused, so a message the server may already have routed was reported as
// never sent and a caller's retry duplicated it. Publish, PublishBatch and
// Respond all confirm through that flush.
//
// While the client reconnects the answer is an ambiguous Unavailable, which
// the driver calls retryable: the link is coming back. A connection closed for
// good under the flush stays Closed, and is ambiguous too.
func TestAConfirmationFlushTheLinkCutIsAmbiguous(t *testing.T) {
	cuts := []struct {
		name      string
		cut       func(*natsgo.Conn, *withholdingConn) error
		code      mq.ErrorCode
		retryable bool
	}{
		{"the link drops", dropLink, mq.CodeUnavailable, true},
		{"a forced reconnection", forceReconnect, mq.CodeUnavailable, true},
		{"the connection closes", closeConn, mq.CodeClosed, false},
	}
	paths := []struct {
		name    string
		publish func(*nq.Backend, mq.Sink) error
	}{
		{"Publish", func(_ *nq.Backend, s mq.Sink) error {
			return s.Publish(context.Background(), &mq.Message{Body: []byte("cut")}, nil)
		}},
		{"PublishBatch", func(_ *nq.Backend, s mq.Sink) error {
			return s.(mq.BatchSink).PublishBatch(context.Background(),
				[]*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}, nil)
		}},
		{"Respond", func(b *nq.Backend, _ mq.Sink) error {
			return b.Respond(context.Background(), natsgo.NewInbox(), &mq.Message{Body: []byte("cut")})
		}},
	}
	url := serverURL(t)
	for _, cut := range cuts {
		for _, path := range paths {
			t.Run(cut.name+"/"+path.name, func(t *testing.T) {
				dialer := &withholdingDialer{}
				nc, err := natsgo.Connect(url, natsgo.SetCustomDialer(dialer), natsgo.ReconnectWait(10*time.Millisecond))
				if err != nil {
					t.Fatalf("dialling: %v", err)
				}
				b, err := nq.BackendWithClient(context.Background(), nc, driverkit.ClientOwned, nq.Config{Addrs: []string{url}})
				if err != nil {
					nc.Close()
					t.Fatalf("BackendWithClient: %v", err)
				}
				t.Cleanup(func() { _ = b.Close() })
				snk, err := b.OpenSink(context.Background(), mq.SinkConfig{Topic: newTopic("cut"), Confirmation: mq.ConfirmAccepted})
				if err != nil {
					t.Fatalf("OpenSink: %v", err)
				}
				t.Cleanup(func() { _ = snk.Close() })

				conn := dialer.current()
				conn.hold()
				res := make(chan error, 1)
				go func() { res <- path.publish(b, snk) }()
				select {
				case <-conn.pinged:
				case <-time.After(10 * time.Second):
					t.Fatal("the publication never sent its flush")
				}
				if err := cut.cut(nc, conn); err != nil {
					t.Fatalf("cutting the link: %v", err)
				}
				var perr error
				select {
				case perr = <-res:
				case <-time.After(10 * time.Second):
					t.Fatal("the publication did not return after its flush was cut")
				}
				outcomes := []error{perr}
				var be *mq.BatchError
				if errors.As(perr, &be) {
					outcomes = be.Errs
				}
				for i, err := range outcomes {
					var oe *mq.OpError
					if !errors.As(err, &oe) {
						t.Fatalf("outcome %d is %v (%T), want an *mq.OpError", i, err, err)
					}
					if !mq.IsAmbiguous(err) || oe.Code != cut.code || oe.Retryable != cut.retryable {
						t.Errorf("outcome %d of a publication whose flush %s cut is %v (code %v, ambiguous %v, retryable flag %v), "+
							"want an ambiguous %v with the retryable flag %v: the server may already have the message",
							i, cut.name, err, oe.Code, mq.IsAmbiguous(err), oe.Retryable, cut.code, cut.retryable)
					}
				}
			})
		}
	}
}

// TestARequestTheConnectionCutIsAmbiguous is the same cut on a request that has
// reached its responder. nats.go fails every request still waiting for its
// reply when it is told to reconnect, with ErrDisconnected, and when it closes,
// with ErrConnectionClosed, and the responder may already have acted on the
// request.
func TestARequestTheConnectionCutIsAmbiguous(t *testing.T) {
	cuts := []struct {
		name      string
		cut       func(*natsgo.Conn) error
		code      mq.ErrorCode
		retryable bool
	}{
		{"a forced reconnection", (*natsgo.Conn).ForceReconnect, mq.CodeUnavailable, true},
		{"the connection closes", func(nc *natsgo.Conn) error { nc.Close(); return nil }, mq.CodeClosed, false},
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			b := openBackend(t)
			topic := newTopic("unanswered")
			cons := consumerOn(t, mq.NewClient(b), topic, "responders")
			res := make(chan error, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, err := b.Request(ctx, topic, &mq.Message{Body: []byte("act on this")})
				res <- err
			}()
			receiveOne(t, cons, 10*time.Second)
			if err := cut.cut(b.Conn()); err != nil {
				t.Fatalf("cutting the connection: %v", err)
			}
			var err error
			select {
			case err = <-res:
			case <-time.After(10 * time.Second):
				t.Fatal("the request did not return after the connection was cut")
			}
			var oe *mq.OpError
			if !errors.As(err, &oe) || !mq.IsAmbiguous(err) || oe.Code != cut.code || oe.Retryable != cut.retryable {
				t.Fatalf("a request the responder received and %s cut reported %v (code %v, ambiguous %v), "+
					"want an ambiguous %v with the retryable flag %v: the responder may have acted on it",
					cut.name, err, mq.CodeOf(err), mq.IsAmbiguous(err), cut.code, cut.retryable)
			}
		})
	}
}

// TestARequestOnAClosedConnectionIsDefinite is the other side of the close.
// nats.go refuses a request on a connection that has already closed before it
// writes anything, so no responder can have acted on it and the answer stays a
// definite Closed.
func TestARequestOnAClosedConnectionIsDefinite(t *testing.T) {
	b := openBackend(t)
	b.Conn().Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := b.Request(ctx, newTopic("refused"), &mq.Message{Body: []byte("never sent")})
	var oe *mq.OpError
	if !errors.As(err, &oe) || oe.Code != mq.CodeClosed || mq.IsAmbiguous(err) {
		t.Fatalf("a request on a closed connection reported %v (code %v, ambiguous %v), want a definite Closed: nothing was written",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
}

// TestBackendCloseEndsARequestOnABorrowedConnection holds that Backend.Close
// ends a Request still waiting for its reply when the connection is borrowed.
// Only closing the connection woke one, so on a borrowed connection the
// Request outlived Close until its own deadline. The responder has the
// request, so the answer is an ambiguous Closed.
func TestBackendCloseEndsARequestOnABorrowedConnection(t *testing.T) {
	url := serverURL(t)
	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	topic := newTopic("silent")
	got := make(chan struct{}, 1)
	sub, err := nc.Subscribe(topic, func(*natsgo.Msg) { got <- struct{}{} })
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("flushing the subscription: %v", err)
	}
	b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	res := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := b.Request(ctx, topic, &mq.Message{Body: []byte("act on this")})
		res <- err
	}()
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("the responder never saw the request")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err = <-res:
	case <-time.After(5 * time.Second):
		t.Fatal("Backend.Close did not end a Request in flight on a borrowed connection")
	}
	var oe *mq.OpError
	if !errors.As(err, &oe) || oe.Code != mq.CodeClosed || !mq.IsAmbiguous(err) || oe.Operation != mq.OpPublish {
		t.Fatalf("a request Close cut off reported %v (code %v, ambiguous %v), want an ambiguous Closed: the responder has it",
			err, mq.CodeOf(err), mq.IsAmbiguous(err))
	}
	if nc.IsClosed() {
		t.Fatal("Close closed the borrowed connection")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := b.Request(ctx, topic, &mq.Message{Body: []byte("after")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("a Request after Close = %v, want ErrClosed", err)
	}
}

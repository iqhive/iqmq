package client

import (
	"context"
	"errors"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Real is the [Dialer] over github.com/rabbitmq/amqp091-go.
//
// It is a thin adapter and deliberately so: everything below it is the
// library's, and the driver's own behaviour is above it. The one piece of
// judgement in this file is the endpoint loop — a DSN may name several hosts
// and the driver tries them in order — and the one decision is that automatic
// recovery is NOT enabled; see [Real.Dial].
type Real struct{}

var _ Dialer = Real{}

// Dial tries each URL in order and returns the first connection that opens.
//
// amqp091-go v1.14 can reconnect and restore topology by itself
// (`amqp091.Config.Recovery`, which the library marks experimental), and this
// driver deliberately does not use it. Three reasons, in order of weight:
//
//  1. The specification gives the obligation to the driver: "driver owns bounded
//     reconnection and topology restoration". A driver that delegated it would
//     be claiming a behaviour it does not implement and cannot bound.
//  2. A reconnection invalidates every delivery tag the driver has handed out,
//     because tags restart at 1 on a new channel. The driver has to bump its
//     own generation at exactly that moment or a settlement for an old
//     delivery lands on whatever took its tag -- the disqualifier. With
//     the library reconnecting underneath, the driver learns about it from a
//     notification that races the first delivery on the new channel.
//  3. A reconnection also has to resolve the publications the old channel was
//     holding, and resolve them as AMBIGUOUS: the broker may have taken them.
//     The library's own answer is to nack them, which is a definite negative
//     and the wrong one.
//
// The library's recovery is nil by default, so nothing here has to switch it
// off; this comment is the record of the decision.
func (Real) Dial(ctx context.Context, urls []string, cfg DialConfig) (Conn, error) {
	if len(urls) == 0 {
		return nil, errNoEndpoint
	}
	acfg := amqp.Config{
		Vhost:           cfg.Vhost,
		Heartbeat:       cfg.Heartbeat,
		ChannelMax:      cfg.ChannelMax,
		FrameSize:       cfg.FrameSize,
		TLSClientConfig: cfg.TLS,
		Properties:      cfg.Properties,
		Locale:          "en_US",
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultDialTimeout
	}
	var first error
	for _, url := range urls {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := dialWithin(ctx, url, acfg, timeout)
		if err == nil {
			return &realConn{conn: conn}, nil
		}
		if first == nil {
			first = err
		}
	}
	return nil, first
}

// dialWithin is one amqp091 dial of url whose TCP connect and AMQP handshake
// both end with ctx, and end at timeout at the latest.
//
// The library's own Dial hook is where the context reaches the socket:
// amqp091.Config has no context parameter. The connect took it, and the
// handshake after it did not: a broker that completes the TCP handshake and
// then says nothing, which is a paused container, held the dial for the
// socket deadline, Config.DialTimeout's 15 s, whatever the caller's context
// said (TestOpenEndsAtTheCallersDeadline). So the socket is closed when the
// context ends ([context.AfterFunc]), which fails the handshake at once, and
// this returns the context's error. The close is waited for before this
// returns, and it is not made at all when the handshake finishes first.
func dialWithin(ctx context.Context, url string, acfg amqp.Config, timeout time.Duration) (*amqp.Connection, error) {
	stop := func() bool { return true }
	closed := make(chan struct{})
	acfg.Dial = func(network, addr string) (net.Conn, error) {
		d := net.Dialer{Timeout: timeout}
		conn, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		// The library's own default sets a read deadline for the handshake
		// and then clears it; matching that is what keeps a silent server
		// from parking the handshake for ever.
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			_ = conn.Close()
			return nil, err
		}
		stop = context.AfterFunc(ctx, func() {
			defer close(closed)
			_ = conn.Close()
		})
		return conn, nil
	}
	conn, err := amqp.DialConfig(url, acfg)
	if stop() {
		return conn, err
	}
	// The context ended during the handshake, or as it finished: the socket
	// is closed, so what the library made of it is no connection.
	<-closed
	if conn != nil {
		_ = conn.CloseDeadline(time.Now())
	}
	return nil, ctx.Err()
}

// DefaultDialTimeout bounds one dial attempt when the configuration names no
// timeout. It is the library's own default, so that a caller who sets nothing
// gets the behaviour the library documents.
const DefaultDialTimeout = 30 * time.Second

// errNoEndpoint is a dial with no address, which is a configuration that
// Validate should already have refused.
var errNoEndpoint = errors.New("iqmq/rabbitmq: no endpoint to dial")

// realConn adapts *amqp091.Connection.
type realConn struct{ conn *amqp.Connection }

var _ Conn = (*realConn)(nil)

// Channel opens a channel.
func (c *realConn) Channel() (Channel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// NotifyClose registers a connection-close listener.
func (c *realConn) NotifyClose(ch chan *amqp.Error) chan *amqp.Error { return c.conn.NotifyClose(ch) }

// NotifyBlocked registers a `connection.blocked` listener.
func (c *realConn) NotifyBlocked(ch chan amqp.Blocking) chan amqp.Blocking {
	return c.conn.NotifyBlocked(ch)
}

// IsClosed reports whether the connection has closed.
func (c *realConn) IsClosed() bool { return c.conn.IsClosed() }

// Close closes the connection.
func (c *realConn) Close() error { return c.conn.Close() }

// CloseDeadline closes the connection with the handshake bounded by deadline.
func (c *realConn) CloseDeadline(deadline time.Time) error { return c.conn.CloseDeadline(deadline) }

// ServerProperties is the broker's `connection.start` table.
func (c *realConn) ServerProperties() amqp.Table { return c.conn.Properties }

// FrameSize is the frame size the connection negotiated. The library writes it
// into its Config during the handshake, before the connection is handed out,
// and never again.
func (c *realConn) FrameSize() int { return c.conn.Config.FrameSize }

// Native returns the library's own connection, for a caller that needs the
// escape hatch.
func (c *realConn) Native() *amqp.Connection { return c.conn }

// Wrap adapts a connection the caller already made, for
// `rabbitmq.OpenWithClient`.
//
// It is here rather than in the driver so that the adapter exists once: the
// driver's borrowed-connection dialler and the real one both hand back the
// same type, and a second adapter would be a second place for the
// notification registrations to drift.
func Wrap(conn *amqp.Connection) Conn { return &realConn{conn: conn} }

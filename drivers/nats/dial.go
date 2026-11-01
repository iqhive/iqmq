package nats

import (
	"context"
	"net"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

// connectWithin is [connectServers] held to ctx's deadline, when it has one.
//
// nats.go's connect takes no context. Its dial waits up to ConnectTimeout for
// each server, and once a server accepts the connection nats.go puts a
// deadline of the same length on the socket for the INFO it waits for, so a
// server that accepted and never answered held Open that long whatever the
// context said. With a deadline, the dial goes through an [openDialer] for as
// long as nats.go connects, and then that dialer is plain again.
//
// A context with no deadline keeps nats.go's own dialer. Ending the INFO wait
// at a cancellation would take a goroutine to set a deadline when the context
// ends, so a cancellation without a deadline still waits up to ConnectTimeout.
func connectWithin(ctx context.Context, addrs []string, timeout time.Duration, opts []natsgo.Option) (*natsgo.Conn, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return connectServers(addrs, opts)
	}
	d := &openDialer{timeout: timeout}
	d.open.Store(&openBound{ctx: ctx, deadline: deadline})
	defer d.open.Store(nil)
	return connectServers(addrs, append(opts, natsgo.SetCustomDialer(d)))
}

// openDialer is the dialer of a connection opened under a context with a
// deadline.
//
// While Open connects, it dials with that context and hands nats.go a socket
// that caps every deadline nats.go sets on it at the context's. Once Open has
// connected it dials as nats.go's own dialer would, with ConnectTimeout, and
// the socket it dialled caps nothing, so no later write or reconnection is
// held to a deadline that has passed. nats.go divides its own dialer's
// timeout between the addresses a host name resolves to, and this one cannot
// see how many there are, so a reconnection to a host with several dead
// addresses waits up to ConnectTimeout on each.
type openDialer struct {
	timeout time.Duration
	open    atomic.Pointer[openBound]
}

// openBound is the context Open connects under, and its deadline.
type openBound struct {
	ctx      context.Context
	deadline time.Time
}

// Dial implements nats.CustomDialer.
func (d *openDialer) Dial(network, address string) (net.Conn, error) {
	nd := net.Dialer{Timeout: d.timeout}
	b := d.open.Load()
	if b == nil {
		return nd.Dial(network, address)
	}
	c, err := nd.DialContext(b.ctx, network, address)
	if err != nil {
		return nil, err
	}
	// nats.go sets no deadline of its own before the TLS handshake of a
	// websocket address, so the socket starts with the context's. Its
	// connect clears every deadline once the connection is up.
	if err := c.SetDeadline(b.deadline); err != nil {
		_ = c.Close()
		return nil, err
	}
	return &openConn{Conn: c, d: d, deadline: b.deadline}, nil
}

// openConn is a socket dialled while Open connects.
type openConn struct {
	net.Conn
	d        *openDialer
	deadline time.Time
}

// capped is t, or the deadline of the Open c was dialled for when that comes
// first and Open is still connecting. A zero t clears a deadline and passes
// through, so nothing of the cap is left on the socket once nats.go clears
// its own.
func capped(c *openConn, t time.Time) time.Time {
	if t.IsZero() || c.d.open.Load() == nil || t.Before(c.deadline) {
		return t
	}
	return c.deadline
}

func (c *openConn) SetDeadline(t time.Time) error { return c.Conn.SetDeadline(capped(c, t)) }

func (c *openConn) SetReadDeadline(t time.Time) error { return c.Conn.SetReadDeadline(capped(c, t)) }

func (c *openConn) SetWriteDeadline(t time.Time) error { return c.Conn.SetWriteDeadline(capped(c, t)) }

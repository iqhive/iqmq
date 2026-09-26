package client

import (
	"context"

	amqp "github.com/Azure/go-amqp"
)

// Compile-time proof that go-amqp satisfies the seam. Asserting it here is what
// makes a go-amqp release that changes one of these six signatures a build
// failure in this package rather than a surprise inside the driver.
var (
	_ Dialer   = RealDialer{}
	_ Conn     = (*realConn)(nil)
	_ Session  = (*realSession)(nil)
	_ Sender   = (*realSender)(nil)
	_ Receiver = (*realReceiver)(nil)
	_ Receipt  = (*realReceipt)(nil)
)

// RealDialer dials with go-amqp.
type RealDialer struct{}

// Dial connects to addr with go-amqp.
func (RealDialer) Dial(ctx context.Context, addr string, opts *amqp.ConnOptions) (Conn, error) {
	c, err := amqp.Dial(ctx, addr, opts)
	if err != nil {
		return nil, err
	}
	return &realConn{c: c}, nil
}

// WrapConn adapts a *amqp.Conn the caller already has, which is what
// OpenWithClient is built on: the caller dialled it, so iqmq does not, and
// [github.com/iqhive/iqmq/driverkit.Ownership] decides who closes it.
func WrapConn(c *amqp.Conn) Conn { return &realConn{c: c} }

// Native returns the go-amqp connection behind a Conn, or nil for a connection
// this package did not wrap. It is the escape hatch
// [github.com/iqhive/iqmq.Backend.Native] answers with.
func Native(c Conn) *amqp.Conn {
	if rc, ok := c.(*realConn); ok {
		return rc.c
	}
	return nil
}

// realConn is a go-amqp connection under the seam.
type realConn struct{ c *amqp.Conn }

// NewSession begins a session.
func (r *realConn) NewSession(ctx context.Context, opts *amqp.SessionOptions) (Session, error) {
	s, err := r.c.NewSession(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &realSession{s: s}, nil
}

// Properties is the peer's connection-properties map.
func (r *realConn) Properties() map[string]any { return r.c.Properties() }

// Done is closed when the connection has died.
func (r *realConn) Done() <-chan struct{} { return r.c.Done() }

// Err is why the connection died.
func (r *realConn) Err() error { return r.c.Err() }

// Close closes the connection.
func (r *realConn) Close() error { return r.c.Close() }

// realSession is a go-amqp session under the seam.
type realSession struct{ s *amqp.Session }

// NewSender attaches a sending link.
func (r *realSession) NewSender(ctx context.Context, target string, opts *amqp.SenderOptions) (Sender, error) {
	s, err := r.s.NewSender(ctx, target, opts)
	if err != nil {
		return nil, err
	}
	return &realSender{s: s}, nil
}

// NewReceiver attaches a receiving link.
func (r *realSession) NewReceiver(ctx context.Context, source string, opts *amqp.ReceiverOptions) (Receiver, error) {
	rcv, err := r.s.NewReceiver(ctx, source, opts)
	if err != nil {
		return nil, err
	}
	return &realReceiver{r: rcv}, nil
}

// Close ends the session.
func (r *realSession) Close(ctx context.Context) error { return r.s.Close(ctx) }

// realSender is a go-amqp sending link under the seam.
type realSender struct{ s *amqp.Sender }

// Send transfers a message.
func (r *realSender) Send(ctx context.Context, m *amqp.Message, opts *amqp.SendOptions) error {
	return r.s.Send(ctx, m, opts)
}

// SendWithReceipt transfers a message and defers its disposition.
func (r *realSender) SendWithReceipt(ctx context.Context, m *amqp.Message, opts *amqp.SendWithReceiptOptions) (Receipt, error) {
	rec, err := r.s.SendWithReceipt(ctx, m, opts)
	if err != nil {
		return nil, err
	}
	// The receipt is returned by value and Wait is on the pointer, so it is
	// moved to the heap here once rather than copied per Wait: a copy whose
	// Wait had already resolved would answer from the copy's cache and a
	// second copy would block again on a channel nothing will send to.
	out := rec
	return &realReceipt{r: &out}, nil
}

// MaxMessageSize is the peer's declared ceiling, or zero when it declared none.
func (r *realSender) MaxMessageSize() uint64 { return r.s.MaxMessageSize() }

// Address is the link's target.
func (r *realSender) Address() string { return r.s.Address() }

// Close detaches the link.
func (r *realSender) Close(ctx context.Context) error { return r.s.Close(ctx) }

// realReceipt is a go-amqp send receipt under the seam.
type realReceipt struct{ r *amqp.SendReceipt }

// Wait blocks until the peer's disposition arrives.
func (r *realReceipt) Wait(ctx context.Context) (amqp.DeliveryState, error) { return r.r.Wait(ctx) }

// DeliveryTag correlates the receipt with its transfer.
func (r *realReceipt) DeliveryTag() []byte { return r.r.DeliveryTag() }

// realReceiver is a go-amqp receiving link under the seam.
type realReceiver struct{ r *amqp.Receiver }

// Receive blocks until a message arrives.
func (r *realReceiver) Receive(ctx context.Context, opts *amqp.ReceiveOptions) (*amqp.Message, error) {
	return r.r.Receive(ctx, opts)
}

// Prefetched returns a message the library already holds, or nil.
func (r *realReceiver) Prefetched() *amqp.Message { return r.r.Prefetched() }

// IssueCredit adds link credit.
func (r *realReceiver) IssueCredit(n uint32) error { return r.r.IssueCredit(n) }

// DrainCredit asks the peer to consume or return every outstanding credit.
func (r *realReceiver) DrainCredit(ctx context.Context, opts *amqp.DrainCreditOptions) error {
	return r.r.DrainCredit(ctx, opts)
}

// Dispose settles one delivery.
//
// go-amqp routes every disposition through the message's OWN receiver
// (msg.rcv), so the four calls below are equivalent to one another and this
// switch is about the outcome rather than about the link. That is also why a
// disposition for a delivery from a detached link fails here with a
// *amqp.LinkError rather than landing somewhere: the driver's guard still comes
// first, because a broker that accepted a stale disposition would apply it to
// whatever took the delivery's place.
func (r *realReceiver) Dispose(ctx context.Context, m *amqp.Message, d Disposition) error {
	switch d.Outcome {
	case OutcomeAccepted:
		return r.r.AcceptMessage(ctx, m)
	case OutcomeReleased:
		return r.r.ReleaseMessage(ctx, m)
	case OutcomeRejected:
		return r.r.RejectMessage(ctx, m, d.Error)
	case OutcomeModified:
		return r.r.ModifyMessage(ctx, m, &amqp.ModifyMessageOptions{
			DeliveryFailed:    d.DeliveryFailed,
			UndeliverableHere: d.UndeliverableHere,
			Annotations:       d.Annotations,
		})
	case outcomeCount:
	}
	return errUnknownOutcome
}

// Address is the link's source.
func (r *realReceiver) Address() string { return r.r.Address() }

// Close detaches the link.
func (r *realReceiver) Close(ctx context.Context) error { return r.r.Close(ctx) }

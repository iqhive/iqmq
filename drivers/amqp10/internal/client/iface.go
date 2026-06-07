// Package client is the narrow AMQP 1.0 surface the driver is written against,
// and the scripted fake that makes every one of the driver's own tests runnable
// with no broker (the test suite level 1).
//
// The interfaces below are deliberately smaller than go-amqp's. What the driver
// needs of a client library is six things — dial, session, sender, receiver,
// disposition, close — and naming exactly those is what lets [Fake] be a
// complete implementation rather than a stub with holes in it. The types that
// cross the boundary are go-amqp's own ([github.com/Azure/go-amqp.Message],
// [github.com/Azure/go-amqp.DeliveryState], the option structs and the error
// types), because re-declaring them would mean maintaining a second copy of the
// protocol's data model and would stop the driver's error table from being
// driven by the values the client library really produces — which is increment
// the finding: a driver's error table is usually tested by feeding it the text
// a SERVER sends, and what reaches the mapper is what the CLIENT LIBRARY hands
// over.
//
// # Why a seam at all
//
// Three of the four scenarios this driver has to get right are unreachable from
// a real broker in a unit test: a disposition that arrives after its link
// detached, a send whose disposition never arrives, and a credit window that
// runs dry. A broker can be made to do the first two with a proxy and the third
// with a slow consumer, and all three then depend on timing. Through this seam
// each is three lines of script, deterministic, and runs in microseconds with
// no container — which matters on a shared host where about fifty other
// people's containers are already running.
//
// # Ownership
//
// Nothing here owns a message's storage. A [Receiver] hands out the
// *amqp.Message the library decoded, and the delivery's settlement goes back
// through the same Receiver, so the driver's token table is what decides whether
// a disposition may be applied at all. This package has no opinion about
// staleness: it is the transport, and the guard is the driver's.
package client

import (
	"context"

	amqp "github.com/Azure/go-amqp"
)

// Dialer establishes connections. It is the one seam the driver's dial loop
// needs, and it is an interface rather than a function so that a fake can
// answer several dials differently — the reconnect tests script a dial that
// fails twice and then succeeds.
type Dialer interface {
	// Dial connects to addr. It must respect ctx: a dial that ignores
	// cancellation is a goroutine a closing client cannot get rid of.
	Dial(ctx context.Context, addr string, opts *amqp.ConnOptions) (Conn, error)
}

// Conn is one AMQP 1.0 connection.
//
// Done and Err are the whole reason this driver owns a dial loop: go-amqp does
// not reconnect, so the connection publishes its own death on Done and the
// driver's supervisor is what notices.
type Conn interface {
	// NewSession begins a session, which is the scope a link lives in.
	NewSession(ctx context.Context, opts *amqp.SessionOptions) (Session, error)
	// Properties is the peer's connection-properties map, which is how every
	// AMQP 1.0 broker says what it is. It may be nil.
	Properties() map[string]any
	// Done is closed when the connection has died or been closed.
	Done() <-chan struct{}
	// Err is why the connection died, and nil while it is alive.
	Err() error
	// Close closes the connection. It is idempotent.
	Close() error
}

// Session is one AMQP 1.0 session: the window a link's transfers are counted
// in.
//
// This driver puts one link on one session, which is the decision recorded in
// links.go: a session's window is shared by every link on it, so a stalled
// receiver on a shared session throttles an unrelated sender.
type Session interface {
	// NewSender attaches a sending link to target.
	NewSender(ctx context.Context, target string, opts *amqp.SenderOptions) (Sender, error)
	// NewReceiver attaches a receiving link to source.
	NewReceiver(ctx context.Context, source string, opts *amqp.ReceiverOptions) (Receiver, error)
	// Close ends the session and detaches its links.
	Close(ctx context.Context) error
}

// Sender is one sending link.
type Sender interface {
	// Send transfers a message and waits for the peer's disposition, or
	// returns as soon as the frame has been written when opts.Settled is set.
	Send(ctx context.Context, m *amqp.Message, opts *amqp.SendOptions) error
	// SendWithReceipt transfers a message and returns a receipt that resolves
	// to the peer's disposition. It is what makes an asynchronous publication
	// cost no goroutine per message: one goroutine per sink waits on the
	// receipts in the order they were handed out.
	SendWithReceipt(ctx context.Context, m *amqp.Message, opts *amqp.SendWithReceiptOptions) (Receipt, error)
	// MaxMessageSize is the ceiling the peer declared on attach, or zero when
	// it declared none. The specification forbids reading an unadvertised limit as zero, so
	// zero here means unknown and never "no messages allowed".
	MaxMessageSize() uint64
	// Address is the target the link attached to.
	Address() string
	// Close detaches the link.
	Close(ctx context.Context) error
}

// Receipt is a transfer whose disposition has not arrived yet.
//
// It is an interface and not go-amqp's struct because the struct's only useful
// method takes a pointer receiver and its zero value is unusable, so a fake
// cannot produce one. Wait's contract is the one that matters: a Wait that
// returns an error has NOT told the caller the transfer failed — the frame has
// already left the socket, so the outcome is ambiguous.
type Receipt interface {
	// Wait blocks until the peer's disposition arrives, ctx is done or the
	// link dies.
	Wait(ctx context.Context) (amqp.DeliveryState, error)
	// DeliveryTag correlates the receipt with its transfer.
	DeliveryTag() []byte
}

// Receiver is one receiving link.
//
// The split between Receive and Prefetched is what a [bitbucket.org/iqhive/iqmq.Source.Fetch]
// is built out of: one blocking wait followed by a non-blocking drain, so that a
// fetch of a batch costs one wait rather than one per message.
type Receiver interface {
	// Receive blocks until a message arrives, ctx is done or the link dies.
	Receive(ctx context.Context, opts *amqp.ReceiveOptions) (*amqp.Message, error)
	// Prefetched returns a message the library already holds, or nil. It never
	// blocks and never waits on the peer.
	Prefetched() *amqp.Message
	// IssueCredit adds link credit, which is how a manually credited receiver
	// asks the peer for more messages.
	IssueCredit(n uint32) error
	// DrainCredit asks the peer to consume or return every outstanding credit
	// and blocks until it answers.
	DrainCredit(ctx context.Context, opts *amqp.DrainCreditOptions) error
	// Dispose settles one delivery with the outcome d names.
	Dispose(ctx context.Context, m *amqp.Message, d Disposition) error
	// Address is the source the link attached to.
	Address() string
	// Close detaches the link.
	Close(ctx context.Context) error
}

// Outcome is the AMQP 1.0 terminal delivery outcome a disposition carries.
//
// The four are the protocol's, and the mapping from iqmq's settlement verbs to
// them is settler.go's: Ack is accepted, a requeue is released, a delayed or
// poisoned requeue is modified, and a dead-letter or a drop is rejected.
type Outcome uint8

// The outcomes, in the order the verbs reach them.
const (
	// OutcomeAccepted is `accepted`: the receiver processed the message and
	// the sender may forget it.
	OutcomeAccepted Outcome = iota
	// OutcomeReleased is `released`: the message was not acted upon and is
	// available again, with the delivery count unchanged.
	OutcomeReleased
	// OutcomeRejected is `rejected`: the message is invalid and unprocessable.
	// A broker with a dead-letter address routes it there.
	OutcomeRejected
	// OutcomeModified is `modified`: the message was not acted upon and should
	// be changed at the node — the delivery count incremented
	// (delivery-failed), this link skipped (undeliverable-here), or an
	// annotation merged in.
	OutcomeModified

	outcomeCount
)

var outcomeNames = [outcomeCount]string{"accepted", "released", "rejected", "modified"}

// String returns the outcome's protocol name.
func (o Outcome) String() string {
	if o < outcomeCount {
		return outcomeNames[o]
	}
	return "Outcome(?)"
}

// Valid reports whether o names an outcome.
func (o Outcome) Valid() bool { return o < outcomeCount }

// Disposition is one terminal settlement as the transport takes it.
type Disposition struct {
	// Outcome is which of the four terminal outcomes this is.
	Outcome Outcome
	// Error is the rejection's condition and description, for
	// OutcomeRejected. It may be nil.
	Error *amqp.Error
	// DeliveryFailed asks the node to count this as a failed delivery
	// attempt, for OutcomeModified.
	DeliveryFailed bool
	// UndeliverableHere asks the node not to redeliver to this link, for
	// OutcomeModified.
	UndeliverableHere bool
	// Annotations are merged into the message's annotations, for
	// OutcomeModified. It is how a broker-specific redelivery delay is
	// expressed.
	Annotations amqp.Annotations
}

// State renders the disposition as the go-amqp delivery state it stands for,
// which is what a fake compares against and what a real receiver sends.
func (d Disposition) State() amqp.DeliveryState {
	switch d.Outcome {
	case OutcomeAccepted:
		return &amqp.StateAccepted{}
	case OutcomeReleased:
		return &amqp.StateReleased{}
	case OutcomeRejected:
		return &amqp.StateRejected{Error: d.Error}
	case OutcomeModified:
		return &amqp.StateModified{
			DeliveryFailed:     d.DeliveryFailed,
			UndeliverableHere:  d.UndeliverableHere,
			MessageAnnotations: d.Annotations,
		}
	case outcomeCount:
	}
	return nil
}

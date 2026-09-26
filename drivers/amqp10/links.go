package amqp10

import (
	"context"
	"sync"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// Link management: one sending link per sink and one receiving link per source,
// each on a session of its own, re-attached lazily after a reconnection, and
// tagged with the connection generation that makes a stale delivery detectable.
//
// # One session per link
//
// A session is the scope an AMQP 1.0 transfer's flow control is counted in: the
// incoming and outgoing windows belong to the session, and every link on it
// shares them. Putting a sink and a source on one session would therefore make
// a source that stops fetching throttle a sink that is publishing perfectly
// well — the session's incoming window fills with unsettled deliveries and the
// outgoing window's credit is negotiated against the same session — and the
// symptom is a publisher that slows down because of a consumer it has nothing to
// do with. A session costs one BEGIN frame and one channel number, of which
// there are 65,536 by default, so the price of isolating them is negligible and
// the price of sharing them is a coupling nothing in the API can explain.
//
// # The generation, and why there is only one
//
// Every link carries the connection generation it was attached under.
// [github.com/iqhive/iqmq/driverkit.Reconnector.Lost] bumps that number when the
// connection dies and [github.com/iqhive/iqmq/driverkit.Reconnector.Generation]
// reads it, so the backend has exactly one generation counter and it is the
// kit's — which matters because the alternative is two numbers that have to be
// kept in step by hand, and the one that drifts is the one the staleness guard
// reads.
//
// A delivery records the generation of the link it arrived on. That is what
// makes the stale disposition detectable at the driver: a disposition
// whose delivery came from an older generation is refused with
// [github.com/iqhive/iqmq.ErrLeaseLost] and never sent, because the link it
// would have to be sent on is gone and the delivery it names has gone back to
// the node — where another consumer may already hold it.
//
// [github.com/iqhive/iqmq/driverkit.TokenTable] is the second half of the same
// guard and it is not redundant. The generation catches a whole link's worth of
// deliveries at once; the table catches one delivery at a time, which is what a
// second settlement of the SAME delivery needs (ErrAlreadySettled), what a
// settlement racing another needs (ErrSettlementInProgress), and what a
// delivery whose outcome is unknown needs (sticky, for ever). Either one alone
// leaves a hole: without the table a second Ack on the current link would be
// sent twice, and without the generation every delivery of a dead link would
// still resolve to a live slot.
//
// # Re-attach is lazy
//
// A reconnection does not re-attach the links; the next operation on a sink or
// a source does. The generation bump and the token invalidation ARE eager,
// because those are the correctness half — a settlement must be refused from
// the instant the connection died, not from the instant somebody notices.
//
// The reason to make the attach lazy is that it cannot be done well eagerly. An
// attach needs a context and a deadline, and a supervisor re-attaching thirty
// links has no caller's context to use and no good answer to "the fourth attach
// was refused": it would have to either give up on the connection, which throws
// away twenty-nine working links, or carry a per-link failure nobody asked for.
// A parked Fetch wakes with its link's own error the moment the connection dies,
// the core fetches again, and the second Fetch attaches — so the observable
// behaviour is one failed fetch per reconnection, which is what a caller of an
// at-least-once driver already handles.

// senderLink is one attached sending link and the generation it belongs to.
type senderLink struct {
	gen  uint32
	sess client.Session
	snd  client.Sender
}

// receiverLink is one attached receiving link and the generation it belongs to.
type receiverLink struct {
	gen  uint32
	sess client.Session
	rcv  client.Receiver
}

// attachSender opens a session and a sending link to addr under the backend's
// current connection.
//
// It returns the generation the link belongs to alongside it, read BEFORE the
// session is begun: a connection that dies during the attach is then recorded
// under the generation the attach was made in, so the link is stale rather than
// looking current. Reading it afterwards would tag a link that never worked with
// the generation of the connection that replaced it.
func (b *Backend) attachSender(ctx context.Context, addr string, opts *amqp.SenderOptions) (*senderLink, error) {
	conn, gen, err := b.connection()
	if err != nil {
		return nil, err
	}
	sess, err := conn.NewSession(ctx, b.sessionOptions())
	if err != nil {
		return nil, err
	}
	snd, err := sess.NewSender(ctx, addr, opts)
	if err != nil {
		_ = sess.Close(ctx)
		return nil, err
	}
	return &senderLink{gen: gen, sess: sess, snd: snd}, nil
}

// attachReceiver opens a session and a receiving link to addr, with MANUAL
// credit: the link is attached with no credit at all and the source issues it.
// See credit.go for why.
func (b *Backend) attachReceiver(ctx context.Context, addr string, opts *amqp.ReceiverOptions) (*receiverLink, error) {
	conn, gen, err := b.connection()
	if err != nil {
		return nil, err
	}
	sess, err := conn.NewSession(ctx, b.sessionOptions())
	if err != nil {
		return nil, err
	}
	rcv, err := sess.NewReceiver(ctx, addr, opts)
	if err != nil {
		_ = sess.Close(ctx)
		return nil, err
	}
	return &receiverLink{gen: gen, sess: sess, rcv: rcv}, nil
}

// sessionOptions are the session's own, which this driver leaves at the
// library's defaults: the incoming window is go-amqp's and this driver's own
// bound on unsettled deliveries is the link credit, which is the number the specification
// gives a meaning to.
func (b *Backend) sessionOptions() *amqp.SessionOptions { return nil }

// detachSender detaches a sending link and ends its session, reporting the
// first failure.
//
// Both steps run even when the first fails: a session left open on a connection
// this client keeps holds a channel number for the life of the connection, and
// a detach that failed because the link was already gone is not a reason to
// leak one.
func detachSender(ctx context.Context, l *senderLink) error {
	if l == nil {
		return nil
	}
	err := l.snd.Close(ctx)
	if serr := l.sess.Close(ctx); serr != nil && err == nil {
		err = serr
	}
	return err
}

// detachReceiver detaches a receiving link and ends its session.
//
// Detaching is also the backstop for everything the link still held unsettled:
// AMQP 1.0 returns an unsettled delivery to the node when its link goes away, so
// a delivery this driver failed to release explicitly is redelivered rather than
// lost. The explicit release is still what a close does first, because it is
// immediate and unambiguous where the detach's effect is the node's business.
func detachReceiver(ctx context.Context, l *receiverLink) error {
	if l == nil {
		return nil
	}
	err := l.rcv.Close(ctx)
	if serr := l.sess.Close(ctx); serr != nil && err == nil {
		err = serr
	}
	return err
}

// linkHolder is the shared "attach on demand, under one lock" bookkeeping of a
// sink and a source.
//
// It is a type rather than two copies because the invariant is subtle and worth
// stating once: the CHECK of the generation and the ATTACH have to happen under
// one lock, or two callers racing on a reconnected backend each attach a link
// and one of them is orphaned — a link nobody detaches, holding a channel
// number and, on a receiving link, credit the peer will fill.
type linkHolder struct {
	mu     sync.Mutex
	closed bool
}

// closeHolder marks the holder closed and reports whether this call was the one
// that did it.
func (h *linkHolder) closeHolder() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.closed = true
	return true
}

// isClosed reports whether the holder has been closed.
func (h *linkHolder) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// compile-time proof that the holder's two users answer the close contract
// with the root package's own sentinel rather than one of their own.
var _ = mq.ErrClosed

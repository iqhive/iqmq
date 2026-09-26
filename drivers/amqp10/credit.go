package amqp10

import (
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Link credit: the source's prefetch, accounted by the driver.
//
// # Automatic and manual credit, and which this driver uses
//
// go-amqp offers two modes. With AUTOMATIC credit
// ([github.com/Azure/go-amqp.ReceiverOptions.Credit] set to a positive number)
// the library issues that much on attach and re-issues as messages are SETTLED,
// counting settlement itself; the driver never writes a flow frame. With MANUAL
// credit (Credit set to -1) the library issues nothing and
// [github.com/Azure/go-amqp.Receiver.IssueCredit] and DrainCredit are the
// driver's.
//
// This driver uses MANUAL credit, and the reason is the sticky unknown
// rather than a preference for control.
//
// A delivery whose settlement outcome was ambiguous never becomes settled.
// Nothing later turns it into a success — that is the rule, and
// [github.com/iqhive/iqmq/driverkit.TokenTable.Complete] enforces it by keeping
// the slot for ever. Under automatic credit the library's counter and the
// driver's would then disagree permanently and in opposite directions: the
// library re-issues credit when IT considers a delivery settled, which for an
// unknown outcome is never, so the window silently shrinks by one for every
// ambiguous settlement and NOTHING can report it. The prefetch would drift
// downwards over a long-lived consumer's life, the symptom would be a consumer
// that gets slower, and the cause would be invisible from both sides.
//
// With manual credit the driver owns the one number the prefetch means, so the
// drift is a value this file holds and a counter a deployment can read
// ([Source.CreditStats]). It also makes the bound the specification asks for real: a source
// never holds more unsettled deliveries than its prefetch, because the credit it
// issues is exactly the room it has.
//
// # The invariant
//
//	issued + held <= max
//
// where `max` is the prefetch, `issued` is credit the peer holds and has not
// spent, and `held` is deliveries this source has taken and not released. A
// delivery arriving moves one from issued to held; a terminal settlement, a
// release or a close moves one out of held and frees room; and a top-up issues
// whatever the invariant allows.
//
// # Why a Fetch with a full window does not deadlock
//
// A Fetch that finds the window full cannot produce a delivery: the peer has no
// credit and will send nothing. Blocking on the receiver would then be a
// deadlock, so [creditWindow.waitForRoom] blocks on a SETTLEMENT instead — the
// caller's own settlements are what free room, and one of them wakes the parked
// fetch. That is the fourth critical scenario, and it is the reason this is a
// condition and not a channel of deliveries.
//
// The one state that cannot be waited out is a window every one of whose slots
// holds a delivery whose outcome is UNKNOWN. Nothing will ever free those, so a
// Fetch that waited would wait for ever; it reports CodeResourceExhausted naming
// the condition instead, which is a diagnosis rather than a hang. It is not a
// spin: the state is terminal for that source, and a source in it is one whose
// deployment has a settlement problem to look at.

// creditWindow is one source's credit accounting.
type creditWindow struct {
	mu      sync.Mutex
	max     int
	issued  int
	held    int
	unknown int
	// room is signalled, non-blockingly, whenever a slot frees. It is
	// buffered so that a settlement never waits for a fetch to be listening.
	room   chan struct{}
	closed bool
}

// newCreditWindow returns the window for a source with the given prefetch.
func newCreditWindow(prefetch int) (*creditWindow, error) {
	if prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	return &creditWindow{max: prefetch, room: make(chan struct{}, 1)}, nil
}

// Max is the prefetch this window bounds.
func (w *creditWindow) Max() int { return w.max }

// topUp issues whatever credit the invariant allows, on the receiver given.
//
// The credit is computed under the lock and issued OUTSIDE it, which is not a
// micro-optimisation: IssueCredit reaches into the library's link mux, and a
// library that called back into this window — which a future release could, for
// a credit callback — would deadlock on a mutex its own caller holds. That is
// the shape [github.com/iqhive/iqmq/driverkit.BaseBackend.Close] exists for, one
// level down.
func (w *creditWindow) topUp(rcv client.Receiver) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return mq.ErrClosed
	}
	want := w.max - w.issued - w.held
	if want <= 0 {
		w.mu.Unlock()
		return nil
	}
	w.issued += want
	w.mu.Unlock()
	if err := rcv.IssueCredit(uint32(want)); err != nil {
		// The flow frame did not go out, so the peer does not hold the
		// credit: giving it back is what keeps the invariant true, and a
		// window that counted credit the peer never got would shrink by that
		// much for ever.
		w.mu.Lock()
		w.issued -= want
		w.mu.Unlock()
		return err
	}
	return nil
}

// took records that a delivery arrived: one credit becomes one held delivery.
//
// It reports false when the window is already full, which means the peer sent
// more than its credit — a protocol violation this driver does not trust away:
// the delivery is released rather than handed out, because handing out a
// delivery the token table has no slot for is how a settlement comes to name
// somebody else's.
func (w *creditWindow) took() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.held >= w.max {
		return false
	}
	if w.issued > 0 {
		w.issued--
	}
	w.held++
	return true
}

// freed records that a delivery left this source's hands: a terminal
// settlement, an explicit release, or a close.
func (w *creditWindow) freed() {
	w.mu.Lock()
	if w.held > 0 {
		w.held--
	}
	w.mu.Unlock()
	w.signal()
}

// stuck records a delivery whose settlement outcome is unknown.
//
// It does NOT free the slot, and that is the stickiness reaching the credit
// accounting: nobody knows whether the broker took the settlement, so nobody
// may reuse the room. The count is kept separately so that the condition has a
// name and a number rather than being an unexplained loss of throughput.
func (w *creditWindow) stuck() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.unknown++
}

// signal wakes a parked fetch, without ever blocking a settlement.
func (w *creditWindow) signal() {
	select {
	case w.room <- struct{}{}:
	default:
	}
}

// full reports whether the window has no room for another delivery.
func (w *creditWindow) full() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.held >= w.max
}

// exhausted reports whether every slot in the window holds a delivery whose
// outcome is unknown, which is the one state waiting cannot resolve.
func (w *creditWindow) exhausted() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.unknown >= w.max
}

// stats is the window's own numbers, for a diagnosis and for a test.
func (w *creditWindow) stats() CreditStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return CreditStats{Prefetch: w.max, Issued: w.issued, Held: w.held, Unknown: w.unknown}
}

// waitForRoom blocks until a settlement frees a slot, the context is done or
// the source closes.
func (w *creditWindow) waitForRoom(ctx context.Context, closing <-chan struct{}) error {
	for {
		if !w.full() {
			return nil
		}
		if w.exhausted() {
			return errWindowUnknown
		}
		select {
		case <-w.room:
		case <-closing:
			return mq.ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// reset forgets everything the window was holding, for a connection whose
// deliveries have all gone back to the node.
//
// It is called when the connection dies, and it has to reset every number and
// not merely `held`: the credit the peer was holding died with the link too, so
// a window that kept `issued` would issue less than the prefetch on the new link
// and would go on doing so for the client's whole life. The unknown count goes
// with it, which is the one place the stickiness ends — a delivery whose
// settlement was ambiguous on a link that no longer exists has been returned to
// the node by the detach, so its outcome is no longer unknown: it is a
// redelivery.
func (w *creditWindow) reset() {
	w.mu.Lock()
	w.issued, w.held, w.unknown = 0, 0, 0
	w.mu.Unlock()
	w.signal()
}

// close stops the window and wakes anything parked on it.
func (w *creditWindow) close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.signal()
}

// CreditStats is a source's credit accounting, as [Source.CreditStats] reports
// it.
//
// It is exported because the property it describes is invisible from the
// portable API and matters: a consumer whose Unknown count is rising is one
// whose settlements are timing out, and the throughput it loses to that is
// otherwise unexplained.
type CreditStats struct {
	// Prefetch is the window's size: the most unsettled deliveries this
	// source may hold.
	Prefetch int
	// Issued is link credit the peer holds and has not spent.
	Issued int
	// Held is deliveries this source has taken and not released.
	Held int
	// Unknown is how many of Held have a settlement outcome nobody knows.
	// They never free their slot, so a rising number is a shrinking
	// effective prefetch.
	Unknown int
}

// The credit window's refusals.
var (
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errWindowUnknown = errcode.New(errcode.ResourceExhausted,
		"iqmq/amqp10: every delivery this source holds has a settlement outcome nobody knows, so no link credit can ever be returned; an ambiguous settlement is sticky (the message contract) and this source has to be reopened")
)

package iqmq

import (
	"context"
	"time"
)

// Hooks are the core's observation points. They are stdlib-only
// function fields and not an interface, so that a caller can set one without
// implementing five.
//
// A nil hook costs nothing: no event is built, the clock is not read and
// nothing is formatted. That is the rule and the specification's "disabled
// hooks/observer path: 0 allocs, ≤ 1 predictable branch", and it is why every
// event is a value rather than a pointer — a hook that is not set never
// causes one to exist.
//
// Every hook takes the CONTEXT of the operation it observes, as a parameter
// rather than a field of the event. That is what lets an integration put a
// span where it belongs: `integrations/otel` recorded two of the four
// spans as ROOT spans -- right in time and nowhere in the trace -- because
// the event it was handed carried no context. A parameter
// and not a field, because the specification requires 0 allocations on the disabled path
// and the core would have to populate a field before it knows whether the
// hook is nil.
//
// A hook runs on the goroutine that reached the point it observes, inside the
// operation. It must not block: a hook on the settlement path holds up the
// settlement, and the specification says callbacks must never block driver progress.
// Hooks are set once, when the client is built (WithHooks), and never
// afterwards.
type Hooks struct {
	// OnPublishStart runs before a publication is handed to the driver.
	OnPublishStart func(context.Context, PublishStartEvent)
	// OnPublishDone runs when a publication has succeeded or failed.
	OnPublishDone func(context.Context, PublishDoneEvent)
	// OnReceive runs when a fetch returns, whether or not it returned
	// messages.
	OnReceive func(context.Context, ReceiveEvent)
	// OnSettle runs when a settlement has succeeded or failed.
	//
	// Its context is context.Background(). A settlement and a lease
	// extension have no context of their own: Delivery.Ack takes none, the
	// runner settles through the same methods, and its lease scheduler
	// extends every delivery from one goroutine. A span started from it is
	// therefore a root.
	OnSettle func(context.Context, SettleEvent)
	// OnReconnect runs when a driver reports a reconnection. It is the
	// driver's event: the specification requires background failures to surface through
	// hooks and health rather than only in logs.
	//
	// Nothing in the core fires it, and no part of the SPI carries it at v0:
	// a Backend is built before the Client that holds these hooks, so the
	// increment that gives a driver access to a client's options is the one
	// that will. The field is frozen here because the specification names all five.
	OnReconnect func(context.Context, ReconnectEvent)
}

// PublishStartEvent is a publication about to be handed to the driver.
type PublishStartEvent struct {
	// Seq pairs this event with the PublishDoneEvent of the same
	// publication. It counts from 1 per publisher handle, so two concurrent
	// publications to one topic are two pairs rather than four unmatchable
	// events. It is a counter and not an identifier
	// because the disabled-hook path must stay free of allocation.
	Seq uint64
	// Topic is the destination.
	Topic string
	// Messages is how many messages the publication carries: 1 for a single
	// publish, the batch size for a batch.
	Messages int
	// Bytes is the total body length of those messages.
	Bytes int64
}

// PublishDoneEvent is a publication that has finished.
type PublishDoneEvent struct {
	// Seq is the Seq of this publication's PublishStartEvent. A done event
	// with no start -- a publication the SINK refused to open, which never
	// reached the driver -- carries 0.
	Seq uint64
	// Topic is the destination.
	Topic string
	// Messages is how many messages the publication carried.
	Messages int
	// Bytes is the total body length of those messages.
	Bytes int64
	// Duration is how long the publication took, measured on the client's
	// clock.
	Duration time.Duration
	// Err is the outcome: nil on success, ErrPartial's *BatchError for a
	// batch in which some messages failed, or the failure.
	Err error
}

// ReceiveEvent is a fetch that has returned.
type ReceiveEvent struct {
	// Topic is the topic the consumer subscribed to.
	Topic string
	// Subscription is the subscription.
	Subscription string
	// Messages is how many deliveries the fetch returned.
	Messages int
	// Duration is how long the fetch took, measured on the client's clock.
	Duration time.Duration
	// Err is the fetch's error, which may be non-nil alongside a non-zero
	// Messages.
	Err error
}

// SettleKind is which settlement an OnSettle event is about, and it is the
// `kind` label of the iqmq_settle_duration_seconds.
type SettleKind uint8

// The settlement kinds.
const (
	// SettleAck is a terminal positive settlement.
	SettleAck SettleKind = iota
	// SettleNack is a negative settlement; the event's Action says what was
	// asked of the broker.
	SettleNack
	// SettleExtend is a lease extension.
	SettleExtend

	settleKindCount
)

var settleKindNames = [settleKindCount]string{"ack", "nack", "extend"}

// String returns the kind's lower-case name, as used in metric labels.
func (k SettleKind) String() string {
	if k < settleKindCount {
		return settleKindNames[k]
	}
	return "SettleKind(?)"
}

// Valid reports whether k names a settlement kind.
func (k SettleKind) Valid() bool { return k < settleKindCount }

// SettleEvent is a settlement that has finished.
//
// A settlement that never reached its delivery — one of a delivery that was
// already settled, of a source that cannot settle, of a closing consumer, or
// the loser of the exactly-one race — has no delivery metadata to report:
// the core may only read a delivery's own while it holds it. Such an event carries the consumer's own topic and subscription,
// which is what ReceiveEvent carries, and attempt 0. Err says which case it
// was.
type SettleEvent struct {
	// Topic is the delivery's resolved topic, or the consumer's own topic
	// when the settlement never reached the delivery.
	Topic string
	// Subscription is the subscription the delivery came from, which for a
	// settlement that never reached it is the consumer's own.
	Subscription string
	// Kind is which settlement this was. SettleExtend is a lease
	// extension, which is not a settlement at all and reports itself here
	// because the iqmq_settle_duration_seconds{kind} has a label for
	// it.
	Kind SettleKind
	// Action is what a negative settlement asked the broker to do. It is
	// meaningful only when Kind is SettleNack.
	Action NackAction
	// Attempt is the delivery's attempt number, and zero when the
	// settlement never reached the delivery.
	Attempt int
	// Duration is how long the settlement took, measured on the client's
	// clock.
	Duration time.Duration
	// Err is the outcome: nil on success, and otherwise the failure the specification
	// names — ErrAlreadySettled, ErrLeaseLost, ErrClosed, ErrNotSettleable,
	// ErrSettlementUnknown or the driver's own.
	Err error
}

// ReconnectEvent is a driver's report that it reconnected, or tried to.
type ReconnectEvent struct {
	// Driver is the driver's name.
	Driver string
	// Attempt is which reconnection attempt this was, counting from 1.
	Attempt int
	// Err is nil when the connection was re-established and the failure
	// otherwise.
	Err error
}

package iqmq

import "time"

// PublishReceipt is what the broker said about one accepted publication
// the identifier it assigned, the boundary it confirmed at, and
// wherever it placed the message.
//
// Every field is the driver's to fill and every one is optional: a broker
// that assigns no identifier leaves MessageID empty, and one that reports no
// placement leaves Partition, Offset and Sequence at zero. A receipt is
// therefore read against the driver page that says which fields its broker
// populates.
//
// A receipt is never allocated by the publish path. The specification requires "no result
// struct allocation on success", so the storage is the caller's: a caller
// that wants a receipt points Message.Receipt at a struct it owns and the
// publisher fills it in place.
type PublishReceipt struct {
	// MessageID is the identifier the broker assigned, when it assigns one.
	MessageID string
	// Confirmation is the boundary this publication was confirmed at. It is
	// the publisher's resolved ConfirmationMode unless the driver reports a
	// weaker one — a driver that had to fall back says so here.
	Confirmation ConfirmationMode
	// ConfirmedAt is when the broker confirmed, on whatever clock the driver
	// read. It is the zero time when the driver reports no time.
	ConfirmedAt time.Time
	// Partition is the partition the message was written to.
	Partition int32
	// Offset is the message's offset within its partition.
	Offset int64
	// Sequence is the message's broker-assigned sequence.
	Sequence uint64
	// Native is the driver's own acknowledgement object, for callers that
	// need the escape hatch. It is nil unless the driver sets it, and a
	// driver that sets it costs the publication one interface boxing —
	// which is why nothing in the core ever writes it.
	Native any
}

// reset empties the receipt so that a publication cannot read the previous
// one's answer out of its own slot. It is what the publish path does before
// anything can refuse the publication, and it is why a failed publication,
// one refused before any I/O included, leaves no receipt behind rather than a
// stale one.
func (r *PublishReceipt) reset() { *r = PublishReceipt{} }

// PublishResult is one publication's outcome, as the asynchronous callback
// receives it.
//
// It is passed by value: the callback's parameter is the whole of the result,
// so an asynchronous publication allocates no result object either. Message
// is the message that was published — the same pointer the caller handed to
// PublishAsync — and ownership returns to the caller when the callback
// returns.
type PublishResult struct {
	// Message is the message the outcome is about.
	Message *Message
	// Receipt is what the broker said, when the publisher is configured with
	// Receipts and the message carried a receipt slot. It is the zero
	// receipt otherwise.
	Receipt PublishReceipt
	// Err is the outcome: nil when the broker accepted the publication,
	// ErrBackpressure for one a drop mode discarded, and the failure
	// otherwise. A publication still outstanding when the publisher closed
	// reports an error whose code is CodeSettlementUnknown: it may or may
	// not have reached the broker.
	Err error
}

// IDGenerator produces the identifiers PublisherConfig.AutoID stamps on
// messages that carry none.
//
// It is an interface with one method so that a caller can supply any
// generator — its own, or one of the four formats internal/id implements — and so that the root package does not bind v0 to a format
// whose bake-off is provisional.
//
// NextID is called once per publication, from whatever goroutine published,
// so an implementation must be safe for concurrent use. It is called only
// for a message whose ID is empty, and never at all by a publisher that was
// not configured with one: the specification's "Publish never generates IDs or timestamps
// on the hot path unless configured".
type IDGenerator interface {
	// NextID returns the identifier for the next message. It must be unique
	// within whatever scope the caller needs uniqueness in.
	NextID() string
}

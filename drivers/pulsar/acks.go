package pulsar

import (
	"strconv"

	"github.com/iqhive/iqmq/internal/errcode"
)

// BatchPosition is where one message sits in a Pulsar topic: the entry that
// holds it, and its index inside that entry when the entry holds a batch.
//
// It is the driver's own value rather than a pulsar.MessageID because the rules
// below have to be decidable without a client, without a broker and without
// allocation — they are what FuzzPulsarAckRules fuzzes and what the settler
// checks before it writes anything.
type BatchPosition struct {
	// Ledger is the BookKeeper ledger the entry is in.
	Ledger int64
	// Entry is the entry's index in the ledger.
	Entry int64
	// Partition is the partition index, or -1 for a non-partitioned topic.
	Partition int32
	// Index is the message's index inside the entry, or -1 when the entry
	// holds one message.
	Index int32
	// Size is how many messages the entry holds, or 0 when the client does
	// not know — which is the case for an identifier that was serialised
	// and read back.
	Size int32
}

// Batched reports whether this position names one message of a batch, so that
// an acknowledgement of it could settle a sibling.
//
// An entry of size one is not a batch: acknowledging it settles exactly the
// message it names, whatever route the acknowledgement takes.
func (p BatchPosition) Batched() bool { return p.Index >= 0 && p.Size > 1 }

// Valid reports whether the position is internally consistent. A message index
// at or past the batch size is a position no entry has.
func (p BatchPosition) Valid() bool {
	switch {
	case p.Ledger < 0 || p.Entry < 0:
		return false
	case p.Index < -1:
		return false
	case p.Size < 0:
		return false
	case p.Size > 0 && p.Index >= p.Size:
		return false
	}
	return true
}

// Entryf is the entry's identity, which is what a Pulsar acknowledgement frame
// carries: the ledger, the entry and the partition, and never the batch index
// unless the frame carries an acknowledgement set as well.
func (p BatchPosition) Entryf() string {
	return strconv.FormatInt(p.Ledger, 10) + ":" + strconv.FormatInt(p.Entry, 10) +
		":" + strconv.FormatInt(int64(p.Partition), 10)
}

// String renders the position the way pulsar-client-go renders a message
// identifier.
func (p BatchPosition) String() string {
	return p.Entryf() + ":" + strconv.FormatInt(int64(p.Index), 10)
}

// AckScope is what one acknowledgement settles at the broker.
type AckScope uint8

// The acknowledgement scopes.
const (
	// AckScopeMessage settles the one message the acknowledgement names.
	// It is what a batched entry needs and what an acknowledgement set
	// achieves.
	AckScopeMessage AckScope = iota
	// AckScopeEntry settles the WHOLE entry — every message in it. For an
	// entry holding one message that is the same thing as AckScopeMessage;
	// for a batch it settles siblings the application never completed,
	// which is the specification's "can lose an accepted message outside a documented
	// failure boundary".
	AckScopeEntry

	ackScopeCount
)

var ackScopeNames = [ackScopeCount]string{"message", "entry"}

// String returns the scope's lower-case name.
func (a AckScope) String() string {
	if a < ackScopeCount {
		return ackScopeNames[a]
	}
	return "AckScope(?)"
}

// Valid reports whether a names a scope.
func (a AckScope) Valid() bool { return a < ackScopeCount }

// AckRules are the three conditions that decide what ONE acknowledgement of
// one message settles at the broker.
//
// They are a statement about Apache Pulsar and pulsar-client-go rather than a
// choice this driver makes, and they are written down because two of the five
// combinations lose messages — which is the specification's "can lose an accepted message
// outside a documented failure boundary" and is the subject of
// TestCriticalBatchIndexAckDoesNotSettleASibling.
type AckRules struct {
	// Tracked reports whether the identifier being acknowledged still
	// carries the client's own batch bookkeeping.
	//
	// A `*trackingMessageID` — the value a consumer hands out — knows its
	// entry's batch and clears one bit of it per acknowledgement. An
	// identifier that LOST that tracker, because it was put through
	// MessageID.Serialize and DeserializeMessageID or built with
	// NewMessageID, has no batch to clear: pulsar-client-go's
	// `trackingMessageID.ack()` then answers "the whole batch is done" and
	// the frame acknowledges the entry. It is why this driver keeps the
	// identifier object the consumer gave it, inside the delivery, for the
	// delivery's whole life, and never rebuilds one.
	Tracked bool
	// ClientBatchIndexAck reports whether the client will put an
	// acknowledgement SET in the frame, which is
	// ConsumerOptions.EnableBatchIndexAcknowledgment.
	//
	// With it off, pulsar-client-go holds an individual acknowledgement
	// back until every index of the entry has been acknowledged, and then
	// sends one frame for the entry. Nothing is settled early.
	ClientBatchIndexAck bool
	// BrokerBatchIndexAck reports whether the BROKER honours an
	// acknowledgement set, which is its own
	// `acknowledgmentAtBatchIndexLevelEnabled` and is FALSE by default in
	// Apache Pulsar.
	//
	// This is the combination that loses messages and the reason the flag
	// is not simply passed through: a client that sends an acknowledgement
	// set to a broker that ignores it has sent an acknowledgement for the
	// ENTRY, and every sibling in the batch is marked deleted. The client
	// cannot tell — there is no error — so the driver refuses the
	// configuration at open instead, which is why Config.EnableBatchIndexAck
	// requires an administrative endpoint to check it against.
	BrokerBatchIndexAck bool
}

// Scope is what acknowledging p settles under these rules.
func (r AckRules) Scope(p BatchPosition) AckScope {
	if !p.Batched() {
		// The entry holds this message and nothing else, so the frame's
		// scope and the message's are the same thing.
		return AckScopeEntry
	}
	switch {
	case !r.Tracked:
		return AckScopeEntry
	case !r.ClientBatchIndexAck:
		// The client holds the frame back until every index is clear, so
		// nothing is settled early — which is message scope as far as
		// any sibling is concerned.
		return AckScopeMessage
	case r.BrokerBatchIndexAck:
		return AckScopeMessage
	default:
		// The frame carries an acknowledgement set the broker will
		// ignore, so the broker settles the entry.
		return AckScopeEntry
	}
}

// Check refuses an acknowledgement that would settle a message the application
// never completed.
//
// It is the guard, and it is a refusal rather than a best effort: a
// delivery whose acknowledgement cannot name one message of its batch is a
// delivery this driver cannot settle safely, and answering ErrNotSettleable
// leaves the message to be redelivered rather than destroying its siblings.
func (r AckRules) Check(p BatchPosition) error {
	if !p.Valid() {
		return errInvalidPosition
	}
	if !p.Batched() {
		return nil
	}
	if r.Scope(p) == AckScopeEntry {
		if !r.Tracked {
			return errUntrackedBatchAck
		}
		return errBrokerBatchAck
	}
	return nil
}

// EntryAcks is one batched entry's acknowledgement bookkeeping: which of its
// messages have been settled, and whether the entry as a whole may be.
//
// It is the model pulsar-client-go keeps inside the identifiers it hands out,
// reproduced here so that the rule can be stated, tested and fuzzed without a
// client — and so that the driver's fake can behave the way the library does.
// A zero EntryAcks is not usable; build one with [NewEntryAcks].
type EntryAcks struct {
	size  int32
	bits  []uint64
	acked int32
}

// NewEntryAcks is the bookkeeping for an entry of size messages. A size of one
// or less is an entry that is not a batch, and every operation on it settles
// the entry.
func NewEntryAcks(size int32) *EntryAcks {
	if size < 1 {
		size = 1
	}
	return &EntryAcks{size: size, bits: make([]uint64, (int(size)+63)/64)}
}

// Size is how many messages the entry holds.
func (e *EntryAcks) Size() int32 { return e.size }

// Acked reports whether index i has been settled.
func (e *EntryAcks) Acked(i int32) bool {
	if i < 0 || i >= e.size {
		return false
	}
	return e.bits[i/64]&(1<<uint(i%64)) != 0
}

// Ack settles index i, reporting whether this call settled it (false for a
// repeat or an index the entry does not have) and whether the entry is now
// complete.
//
// An index of -1 means the entry is not a batch, which settles it whole.
func (e *EntryAcks) Ack(i int32) (settled, complete bool) {
	if i < 0 {
		// Not a batch, or an identifier that has no index: the entry is
		// settled outright. Every index is marked so that Complete is
		// true and a second call is a repeat.
		if e.acked == e.size {
			return false, true
		}
		for j := range e.bits {
			e.bits[j] = ^uint64(0)
		}
		e.acked = e.size
		return true, true
	}
	if i >= e.size {
		return false, e.acked == e.size
	}
	if e.Acked(i) {
		return false, e.acked == e.size
	}
	e.bits[i/64] |= 1 << uint(i%64)
	e.acked++
	return true, e.acked == e.size
}

// Complete reports whether every message in the entry has been settled, which
// is when a client without batch-index acknowledgement may send the entry's
// acknowledgement frame.
func (e *EntryAcks) Complete() bool { return e.acked == e.size }

// Pending is how many of the entry's messages are still unsettled.
func (e *EntryAcks) Pending() int { return int(e.size - e.acked) }

// Settled is how many of the entry's messages have been settled.
func (e *EntryAcks) Settled() int { return int(e.acked) }

// The acknowledgement rules' refusals.
var (
	errInvalidPosition = errcode.New(errcode.InvalidArgument,
		"iqmq/pulsar: the delivery's position is not one any Pulsar entry has")
	errUntrackedBatchAck = errcode.New(errcode.NotSettleable,
		"iqmq/pulsar: this delivery is one message of a batched entry and its identifier no longer carries the batch, so acknowledging it would settle every sibling in the entry the driver refuses rather than destroying work it did not complete")
	errBrokerBatchAck = errcode.New(errcode.NotSettleable,
		"iqmq/pulsar: this delivery is one message of a batched entry, the acknowledgement would carry an acknowledgement set, and the broker does not honour one (acknowledgmentAtBatchIndexLevelEnabled is false) — so the broker would settle the whole entry and every sibling in it ")
)

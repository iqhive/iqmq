package pulsarfake

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// The compile-time proof that the model's message and identifier are the
// library's.
var (
	_ pulsar.Message        = (*fmessage)(nil)
	_ pulsar.MessageID      = (*msgID)(nil)
	_ pulsar.TopicMessageID = (*msgID)(nil)
)

// entryAcks is the per-entry bitmap pulsar-client-go keeps inside the
// identifiers it hands out.
//
// It is the client's, not the broker's, and it is why this fake sits above the
// library: an acknowledgement for one index clears one bit, and the frame for
// the ENTRY is sent only when every bit is clear. An identifier that lost this
// value acknowledges the entry outright.
type entryAcks struct {
	mu    sync.Mutex
	size  int
	bits  []bool
	count int
}

// newEntryAcks is the bitmap for an entry of size messages, with the indices in
// done already clear.
func newEntryAcks(size int, done func(int) bool) *entryAcks {
	e := &entryAcks{size: size, bits: make([]bool, size)}
	for i := range size {
		if done != nil && done(i) {
			e.bits[i] = true
			e.count++
		}
	}
	return e
}

// cleared reports whether index i's bit is clear, which is the client's record
// of the application having acknowledged that message.
//
// It is what keeps the model's accounting honest: when the client finally sends
// the ENTRY's frame, the messages whose bits are clear were acknowledged by
// somebody and the rest were not, and only the rest are cross-settled.
func (e *entryAcks) cleared(i int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i < 0 || i >= e.size {
		return false
	}
	return e.bits[i]
}

// ack clears one bit, reporting whether it changed anything and whether every
// bit is now clear.
func (e *entryAcks) ack(i int) (changed, complete bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i < 0 || i >= e.size {
		return false, e.count == e.size
	}
	if e.bits[i] {
		return false, e.count == e.size
	}
	e.bits[i] = true
	e.count++
	return true, e.count == e.size
}

// msgID is one message's position, and the settlement handle.
type msgID struct {
	topic                          string
	ledger, entry                  int64
	partition, batchIdx, batchSize int32
	sl                             slot
	// gen is the delivery generation, which is how this model recognises a
	// settlement for a delivery a later one superseded. A real broker
	// cannot: an acknowledgement names a position and nothing else.
	gen uint32

	// tracker is the client's per-entry bitmap, or nil for an identifier
	// that has lost it. Nil is the whole hazard: see
	// pulsarapi.BatchTracker.
	tracker *entryAcks
}

// Serialize encodes the identifier the way the library does. The TRACKER is not
// part of the encoding, which is exactly why a serialised identifier cannot
// settle one message of a batch.
func (m *msgID) Serialize() []byte {
	return []byte(strconv.FormatInt(m.ledger, 10) + ":" + strconv.FormatInt(m.entry, 10) + ":" +
		strconv.FormatInt(int64(m.partition), 10) + ":" + strconv.FormatInt(int64(m.batchIdx), 10))
}

// LedgerID is the entry's ledger.
func (m *msgID) LedgerID() int64 { return m.ledger }

// EntryID is the entry's index in its ledger.
func (m *msgID) EntryID() int64 { return m.entry }

// BatchIdx is the message's index in its entry, or -1 when the entry holds one.
func (m *msgID) BatchIdx() int32 { return m.batchIdx }

// PartitionIdx is the partition, or -1 for a non-partitioned topic.
func (m *msgID) PartitionIdx() int32 { return m.partition }

// BatchSize is how many messages the entry holds.
func (m *msgID) BatchSize() int32 { return m.batchSize }

// Topic is the concrete topic the message came from.
func (m *msgID) Topic() string { return m.topic }

// String renders the identifier the way the library does.
func (m *msgID) String() string {
	s := strconv.FormatInt(m.ledger, 10) + ":" + strconv.FormatInt(m.entry, 10) + ":" +
		strconv.FormatInt(int64(m.partition), 10)
	if m.batchIdx > -1 {
		s += ":" + strconv.FormatInt(int64(m.batchIdx), 10)
	}
	return s
}

// UntrackedID returns a copy of an identifier with the client's batch
// bookkeeping REMOVED, which is what Serialize followed by
// DeserializeMessageID produces.
//
// It exists so that a test can hand the driver's settler the shape that loses
// messages, and observe that the driver refuses it rather than settling every
// sibling in the entry.
func UntrackedID(id pulsar.MessageID) pulsar.MessageID {
	f, ok := id.(*msgID)
	if !ok {
		return id
	}
	cp := *f
	cp.tracker = nil
	return &cp
}

// fmessage is one delivered message.
type fmessage struct {
	t            *ftopic
	m            *fmsg
	id           *msgID
	redeliveries uint32
}

// Topic is the concrete topic.
func (f *fmessage) Topic() string { return f.t.name }

// ProducerName is the producer that published the message.
func (f *fmessage) ProducerName() string { return f.m.producer }

// Properties are the message's properties.
func (f *fmessage) Properties() map[string]string { return f.m.props }

// Payload is the message body, nil for a null value.
func (f *fmessage) Payload() []byte {
	if f.m.nullValue {
		return nil
	}
	return f.m.payload
}

// IsNullValue reports Pulsar's null-value flag, which is what keeps the specification's
// nil-versus-empty distinction alive.
func (f *fmessage) IsNullValue() bool { return f.m.nullValue }

// ID is the settlement handle.
func (f *fmessage) ID() pulsar.MessageID { return f.id }

// PublishTime is when the broker stored the message.
func (f *fmessage) PublishTime() time.Time { return f.m.publishTime }

// EventTime is the publisher's event time, or the Unix epoch when it set none —
// which is what pulsar-client-go produces for an absent field, and the reason
// the driver carries a marker property.
func (f *fmessage) EventTime() time.Time {
	if f.m.eventTime.IsZero() {
		return time.Unix(0, 0)
	}
	return f.m.eventTime
}

// Key is the partition key.
func (f *fmessage) Key() string { return f.m.key }

// OrderingKey is the ordering key.
func (f *fmessage) OrderingKey() string { return f.m.orderingKey }

// RedeliveryCount is how many times this message has been delivered again.
func (f *fmessage) RedeliveryCount() uint32 { return f.redeliveries }

// IsReplicated reports whether the message came from another cluster.
func (f *fmessage) IsReplicated() bool { return false }

// GetReplicatedFrom is the cluster the message came from.
func (f *fmessage) GetReplicatedFrom() string { return "" }

// GetSchemaValue decodes the message through its schema, which this model does
// not have.
func (f *fmessage) GetSchemaValue(any) error { return errors.New("pulsarfake: no schema") }

// SchemaVersion is the schema version.
func (f *fmessage) SchemaVersion() []byte { return nil }

// GetEncryptionContext is the message's encryption context.
func (f *fmessage) GetEncryptionContext() *pulsar.EncryptionContext { return nil }

// Index is the broker-assigned index, which this model does not assign.
func (f *fmessage) Index() *uint64 { return nil }

// BrokerPublishTime is when the broker published the message.
func (f *fmessage) BrokerPublishTime() *time.Time { return &f.m.publishTime }

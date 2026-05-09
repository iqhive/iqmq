package outbox

import (
	"encoding/binary"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Row is one outbox row: a message plus the bookkeeping the relay needs.
type Row struct {
	// ID is the row's primary key, and is what [Store.MarkSent] and
	// [Store.Release] take. A string rather than an int64 so that a caller
	// whose key is a UUID does not have to map it.
	ID string
	// Topic is where the message goes. A row with no topic is
	// [ErrNoTopic] rather than published to a default, because a default
	// topic is how a misconfigured relay delivers somebody's orders to the
	// wrong place quietly.
	Topic string
	// Key is the partition or ordering key.
	Key []byte
	// Body is the payload.
	Body []byte
	// Headers are the message headers, round-tripped through
	// [EncodeHeaders] and [DecodeHeaders].
	Headers mq.Headers
	// MessageID is the message's application identifier, which for an
	// outbox row should be the BUSINESS operation's id and not a
	// per-publication one: a row republished after a relay died carries the
	// same id, which is what lets the consumer's inbox recognise it.
	MessageID string
	// Attempts is how many times this row has been claimed, including this
	// one. It is what tells a relay that a row is poison rather than merely
	// unlucky, and [Config.MaxAttempts] is what it is compared with.
	Attempts int
}

// The errors this package reports.
var (
	// ErrNoTopic (CodeInvalidArgument) reports a row with an empty topic.
	ErrNoTopic error = errcode.New(errcode.InvalidArgument, "iqmq/x/outbox: the row names no topic")
	// ErrNoStore (CodeInvalidConfig) reports a nil [Config.Store].
	ErrNoStore error = errcode.New(errcode.InvalidConfig, "iqmq/x/outbox: no store")
	// ErrNoPublisher (CodeInvalidConfig) reports a nil [Config.Publish].
	ErrNoPublisher error = errcode.New(errcode.InvalidConfig, "iqmq/x/outbox: no publish function")
	// ErrInvalidConfig (CodeInvalidConfig) reports a configuration that
	// cannot work.
	ErrInvalidConfig error = errcode.New(errcode.InvalidConfig, "iqmq/x/outbox: invalid configuration")
	// ErrBadHeaders (CodeCorruptMessage) reports a header blob
	// [DecodeHeaders] cannot read. It is a corrupt row rather than a bad
	// configuration: the bytes came out of a database column that something
	// else wrote.
	ErrBadHeaders error = errcode.New(errcode.CorruptMessage, "iqmq/x/outbox: the row's header blob is malformed")
	// ErrPoisonRow (CodePoisonMessage) reports a row over
	// [Config.MaxAttempts].
	//
	// It is what stops a relay from spending every claim on the same
	// unpublishable row for ever — a row whose topic does not exist, or
	// whose body exceeds the broker's limit, will fail identically on every
	// attempt, and a relay with no bound makes no progress on anything
	// behind it.
	ErrPoisonRow error = errcode.New(errcode.PoisonMessage, "iqmq/x/outbox: the row has exceeded MaxAttempts")
	// ErrClosed (CodeClosed) reports a call on a closed store or relay.
	ErrClosed error = errcode.New(errcode.Closed, "iqmq/x/outbox: closed")
)

// Message returns the row as an [mq.Message].
//
// The slices are shared with the row rather than copied: the row came out of
// a database and the relay is about to publish it and drop it, so a copy
// would be pure cost. A caller who keeps the row after publishing must not
// mutate these.
func (r Row) Message() (*mq.Message, error) {
	if r.Topic == "" {
		return nil, ErrNoTopic
	}
	return &mq.Message{
		Topic:   r.Topic,
		Key:     r.Key,
		Body:    r.Body,
		Headers: r.Headers,
		ID:      r.MessageID,
	}, nil
}

// EncodeHeaders renders headers as one opaque blob for a BYTEA or BLOB
// column.
//
// The format is a count-prefixed sequence of length-prefixed key/value pairs.
// It is deliberately NOT JSON: headers are ordered and duplicate-preserving
// and a JSON object is neither — round-tripping them through one
// would silently drop a repeated header and reorder the rest, which for
// `traceparent` and a multi-valued application header is a real loss. It is
// also not the wire envelope, which carries a body and a version this column
// does not need.
//
//	uint32  pair count
//	then per pair: uint32 key length, key, uint32 value length, value
func EncodeHeaders(h mq.Headers) []byte {
	if h.Len() == 0 {
		return nil
	}
	out := binary.BigEndian.AppendUint32(make([]byte, 0, 4+h.Len()*16), uint32(h.Len()))
	for k, v := range h.All() {
		out = binary.BigEndian.AppendUint32(out, uint32(len(k)))
		out = append(out, k...)
		out = binary.BigEndian.AppendUint32(out, uint32(len(v)))
		out = append(out, v...)
	}
	return out
}

// DecodeHeaders reads what [EncodeHeaders] wrote.
//
// Every length is checked against what remains before it is used, so no blob
// can make it read past the end, and the declared pair count is a bound to
// compare rather than a size to allocate — the same rule the rest of this
// increment applies to a declared size: a four-byte blob claiming four
// billion pairs must cost nothing.
func DecodeHeaders(b []byte) (mq.Headers, error) {
	var h mq.Headers
	if len(b) == 0 {
		return h, nil
	}
	if len(b) < 4 {
		return h, ErrBadHeaders
	}
	count := binary.BigEndian.Uint32(b[:4])
	// The cheapest possible pair is eight bytes (two empty
	// length-prefixed fields), so a blob shorter than 8*count cannot hold
	// what it claims. This is the check that makes the count safe without
	// allocating on it.
	if remaining := len(b) - 4; uint64(count) > uint64(remaining)/8 {
		return h, ErrBadHeaders
	}
	off := 4
	for range count {
		key, n, err := takeField(b, off)
		if err != nil {
			return mq.Headers{}, err
		}
		off = n
		val, n, err := takeField(b, off)
		if err != nil {
			return mq.Headers{}, err
		}
		off = n
		// Add and not Set: duplicates are preserved, which is the reason
		// this format exists rather than JSON.
		h.Add(string(key), val)
	}
	if off != len(b) {
		// Trailing bytes mean the blob is not what it says it is.
		return mq.Headers{}, ErrBadHeaders
	}
	return h, nil
}

// takeField reads one length-prefixed field at off and returns it with the
// new offset.
func takeField(b []byte, off int) ([]byte, int, error) {
	if off < 0 || off+4 > len(b) {
		return nil, 0, ErrBadHeaders
	}
	n := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	// n is compared with what REMAINS, so a length field of four billion
	// fails here rather than becoming a slice bound.
	if n < 0 || n > len(b)-off {
		return nil, 0, ErrBadHeaders
	}
	return b[off : off+n], off + n, nil
}

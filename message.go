package iqmq

import (
	"bytes"
	"time"
)

// Properties are the typed properties that map to native fields on backends
// that have them (AMQP, Pulsar) and to reserved iqmq- headers elsewhere
type Properties struct {
	// ContentType is the media type of Body.
	ContentType string
	// ContentEncoding is the content coding applied to Body.
	ContentEncoding string
	// CorrelationID ties a reply to its request.
	CorrelationID string
	// CausationID names the message that caused this one.
	CausationID string
	// ReplyTo is the topic a reply should be published to.
	ReplyTo string
	// CreatedAt is when the application created the message. It is filled
	// only when the publisher is configured to: the publish path never
	// reads the clock on its own.
	CreatedAt time.Time
}

// IsZero reports whether every property is unset.
func (p *Properties) IsZero() bool {
	return p.ContentType == "" && p.ContentEncoding == "" && p.CorrelationID == "" &&
		p.CausationID == "" && p.ReplyTo == "" && p.CreatedAt.IsZero()
}

// Message is a message to publish, and the message half of a received
// delivery. It carries no settlement state: settlement belongs to a Delivery.
//
// Key is a []byte and not a string, which is the winner (ADR-006,
// KeyBytes): a key is converted at the driver boundary and not before, so a
// received key costs no allocation and a published one none either.
//
// Key and Body distinguish nil from empty. A nil Body is "no body" and an
// empty non-nil Body is "an empty body"; both are valid, and Clone preserves
// the distinction.
//
// Ownership follows. A synchronous Publish never retains Key, Body or
// the header storage after it returns, so a caller may reuse them; a received
// Message's Body is borrowed until the delivery is settled unless the consumer
// asked for a copy. Clone is the independent copy.
type Message struct {
	// Topic is the destination. A bound publisher ignores it; Client.Publish
	// and an unbound publisher read it.
	Topic string
	// Key is the partition or ordering key. A nil key lets the broker choose.
	Key []byte
	// Body is the payload. It is read-only after Publish and on receive.
	Body []byte
	// Headers are the message headers, ordered and duplicate-preserving.
	Headers Headers
	// ID is the application identifier. It is empty unless the caller set it
	// or the publisher is configured with an IDGenerator.
	ID string
	// Properties are the typed properties.
	Properties Properties
	// Receipt is where a publisher configured with Receipts writes what the
	// broker said about this publication, in place and on success.
	//
	// The storage is the caller: the specification requires "no result struct
	// allocation on success", so a caller that wants a receipt points this
	// at a PublishReceipt it owns — one per message, or one it reuses across
	// a loop — and reads it once the publication has succeeded. A nil
	// Receipt means this message's receipt is not wanted, and a publisher
	// that was not configured with Receipts never reads or writes it. A
	// publication that fails, one refused before any I/O included, leaves the
	// receipt zeroed.
	//
	// It is publish-only. A received delivery's Receipt is nil: what a
	// broker said about a delivery is DeliveryInfo, not a publication
	// receipt.
	Receipt *PublishReceipt
}

// Clone returns an independent deep copy: Key, Body and every header key and
// value are copied, so neither message can be observed through the other.
// The nil-versus-empty distinction of Key and Body is preserved.
func (m *Message) Clone() *Message {
	out := &Message{
		Topic:      m.Topic,
		Key:        bytes.Clone(m.Key),
		Body:       bytes.Clone(m.Body),
		Headers:    m.Headers.Clone(),
		ID:         m.ID,
		Properties: m.Properties,
	}
	if m.Receipt != nil {
		// A receipt slot is storage, so a clone gets its own: sharing the
		// pointer would let each message be observed through the other,
		// which is the one thing Clone promises does not happen.
		r := *m.Receipt
		out.Receipt = &r
	}
	return out
}

// Reset empties the message while keeping its header storage for reuse. It is
// what a pooled message is put back into circulation with: every field
// is cleared, so a reused message holds no reference to the one before it.
func (m *Message) Reset() {
	m.Topic = ""
	m.Key = nil
	m.Body = nil
	m.Headers.Reset()
	m.ID = ""
	m.Properties = Properties{}
	m.Receipt = nil
}

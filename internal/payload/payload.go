// Package payload defines the canonical, API-independent form of a message
// as stored by the memory engine and encoded by the wire envelope.
//
// The root package's Message type either aliases these types directly or
// converts at the API boundary, depending on the outcome of the Phase-0
// header-representation bake-off. Either way, the engine and the wire
// codec never depend on the public API.
package payload

import "time"

// KV is one header: an ordered, duplicate-preserving key/value pair with a
// binary-capable value. Keys are case-sensitive.
type KV struct {
	Key   string
	Value []byte
}

// Props are the typed properties that map to native fields on backends that
// have them (AMQP, Pulsar) and to reserved iqmq-* headers elsewhere.
type Props struct {
	ContentType     string
	ContentEncoding string
	CorrelationID   string
	CausationID     string
	ReplyTo         string
	CreatedAt       time.Time
}

// IsZero reports whether every property is unset.
func (p *Props) IsZero() bool {
	return p.ContentType == "" && p.ContentEncoding == "" && p.CorrelationID == "" &&
		p.CausationID == "" && p.ReplyTo == "" && p.CreatedAt.IsZero()
}

// Payload is the storage form of a message.
//
// Key and Body distinguish nil from empty: a nil Body is "no body" and an
// empty non-nil Body is "an empty body". Clone and CloneInto preserve that
// distinction.
type Payload struct {
	Key     []byte
	Body    []byte
	Headers []KV
	ID      string
	Props   Props
}

// Size returns the number of bytes of variable-length content: key, body,
// header keys and values, ID and string properties. It is the figure used
// for byte-capacity accounting.
func (p *Payload) Size() int {
	n := len(p.Key) + len(p.Body) + len(p.ID)
	for i := range p.Headers {
		n += len(p.Headers[i].Key) + len(p.Headers[i].Value)
	}
	n += len(p.Props.ContentType) + len(p.Props.ContentEncoding) + len(p.Props.CorrelationID) +
		len(p.Props.CausationID) + len(p.Props.ReplyTo)
	return n
}

// Clone returns a deep copy that shares no mutable storage with p.
func (p *Payload) Clone() *Payload {
	dst := &Payload{}
	p.CloneInto(dst)
	return dst
}

// CloneInto deep-copies p into dst, reusing dst's existing byte and header
// storage where its capacity allows. After the call dst shares no mutable
// storage with p. CloneInto(p) is a no-op.
func (p *Payload) CloneInto(dst *Payload) {
	if dst == p {
		return
	}
	dst.Key = cloneBytes(dst.Key, p.Key)
	dst.Body = cloneBytes(dst.Body, p.Body)
	dst.ID = p.ID
	dst.Props = p.Props
	if p.Headers == nil {
		dst.Headers = nil
		return
	}
	if len(p.Headers) == 0 {
		if dst.Headers == nil {
			dst.Headers = []KV{}
		} else {
			dst.Headers = dst.Headers[:0]
		}
		return
	}
	if cap(dst.Headers) >= len(p.Headers) {
		dst.Headers = dst.Headers[:len(p.Headers)]
	} else {
		dst.Headers = make([]KV, len(p.Headers))
	}
	for i := range p.Headers {
		dst.Headers[i].Key = p.Headers[i].Key
		dst.Headers[i].Value = cloneBytes(dst.Headers[i].Value, p.Headers[i].Value)
	}
}

// cloneBytes copies src into dst's storage when possible and returns the
// result, preserving nil-versus-empty.
func cloneBytes(dst, src []byte) []byte {
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		if dst == nil {
			return []byte{}
		}
		return dst[:0]
	}
	if cap(dst) >= len(src) {
		dst = dst[:len(src)]
	} else {
		dst = make([]byte, len(src))
	}
	copy(dst, src)
	return dst
}

// Reset clears p for reuse while retaining the capacity of its byte and
// header slices. Header values keep their storage too.
func (p *Payload) Reset() {
	p.Key = p.Key[:0]
	p.Body = p.Body[:0]
	for i := range p.Headers {
		p.Headers[i].Key = ""
		p.Headers[i].Value = p.Headers[i].Value[:0]
	}
	p.Headers = p.Headers[:0]
	p.ID = ""
	p.Props = Props{}
}

// Equal reports whether a and b have identical content, treating nil and
// empty byte slices as different (matching the ownership contract).
func Equal(a, b *Payload) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if !bytesEqual(a.Key, b.Key) || !bytesEqual(a.Body, b.Body) || a.ID != b.ID || a.Props != b.Props {
		return false
	}
	if (a.Headers == nil) != (b.Headers == nil) || len(a.Headers) != len(b.Headers) {
		return false
	}
	for i := range a.Headers {
		if a.Headers[i].Key != b.Headers[i].Key || !bytesEqual(a.Headers[i].Value, b.Headers[i].Value) {
			return false
		}
	}
	return true
}

func bytesEqual(a, b []byte) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

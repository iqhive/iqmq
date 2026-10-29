// Package wire is iqmq's portable binary envelope: the two layouts that carry
// a message's key, headers, identifier and typed properties through a backend
// whose body is an opaque blob (the message contract).
//
// A driver reaches for it when the broker behind it has no native header
// support, or when the caller asked for MetadataPortableEnvelope. Both
// layouts encode exactly the same field set, so [Layout.Decode] of
// [Layout.Encode] reproduces the message for every message — including the
// distinction between a nil and an empty key, body or header value, which
// the specification's ownership contract makes observable.
//
// Encoding is canonical: one message has exactly one valid encoding per
// layout, and Decode rejects everything else with a [github.com/iqhive/iqmq.CodeCorruptMessage]
// error (bad magic, unsupported version, reserved flags, overlong or
// truncated varints, lengths that overrun the envelope, duplicate or unknown
// critical fields, invalid UTF-8 in string fields, trailing bytes) or a
// [github.com/iqhive/iqmq.CodeMessageTooLarge] error when a field exceeds the
// caller's [Limits].
//
// Decode never allocates in proportion to a declared length before that
// length has been checked against the remaining input, so a hostile envelope
// can cost at most its own size. It is the property FuzzEnvelope exists for,
// and the reason the limits are explicit rather than implied.
//
// # Allocation
//
// Encode allocates nothing per message in steady state when it is given a
// buffer with room: the scratch it converts a message through is pooled and
// grows once. Decode allocates the storage it hands to the destination
// message — the key, the body and the header table — because that storage
// outlives the call and must not come from a pool the next Decode reuses.
//
// Every Layout is safe for concurrent use.
package wire

import (
	"fmt"
	"iter"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/internal/wire"
)

// Kind identifies an envelope layout.
type Kind uint8

// The layouts. Auto resolves to the winner ADR-018 records.
const (
	// Auto selects the recorded bake-off winner.
	Auto Kind = iota
	// Fixed is the fixed-width length-prefixed layout: a header of
	// little-endian lengths followed by the fields in a fixed order. It is
	// the reference; it cannot carry more than 65,535 headers or a string
	// field longer than 65,535 bytes.
	Fixed
	// TLV is the varint tag-length-value layout: varint metadata and body
	// lengths, then a sequence of tagged fields, then the body. An unknown
	// odd tag is skipped, so a decoder of this version accepts an envelope
	// from a later minor version that adds an optional field.
	TLV
)

// String returns the kind's slug, as used in benchmark names and ADRs.
func (k Kind) String() string { return wire.Kind(k).String() }

// Valid reports whether k names a concrete layout or Auto.
func (k Kind) Valid() bool { return wire.Kind(k).Valid() }

// Resolve maps Auto to the recorded default and returns every other kind
// unchanged.
func (k Kind) Resolve() Kind { return Kind(wire.Kind(k).Resolve()) }

// ForwardCompatible reports whether a decoder of this layout accepts an
// envelope carrying optional fields it does not know.
func (k Kind) ForwardCompatible() bool { return wire.Kind(k).ForwardCompatible() }

// Kinds lists every concrete layout in declaration order. Auto is not one of
// them.
func Kinds() []Kind {
	in := wire.Kinds()
	out := make([]Kind, 0, len(in))
	for _, k := range in {
		out = append(out, Kind(k))
	}
	return out
}

// ParseKind returns the layout with the given slug.
func ParseKind(slug string) (Kind, bool) {
	k, ok := wire.ParseKind(slug)
	return Kind(k), ok
}

// Detect reports the layout of the envelope in src from its prefix, without
// decoding it. It reports false for anything that is not an iqmq envelope of
// a known layout and version.
//
// It reads six bytes — the magic, the layout/version byte and the flags byte —
// so it answers true for any byte string that merely BEGINS like an envelope.
// That is the right question for a caller choosing a decoder for something it
// already knows to be an envelope, and the wrong one for a caller deciding
// whether a body is an envelope at all: use [Detector.DetectFull] for that.
func Detect(src []byte) (Kind, bool) {
	k, ok := wire.Detect(src)
	return Kind(k), ok
}

// DetectFull reports the layout of src when src is, exactly and entirely, one
// canonical envelope of a known layout — the question [Detect] approximates
// from six bytes.
//
// It exists because the six-byte approximation is not sound in the one place a
// driver has to ask the question. Under
// [github.com/iqhive/iqmq.MetadataAdaptive] an enveloped record and a native
// one share a subscription, and a broker with no headers — NSQ has none — has
// nowhere to record which is which, so the body is the only evidence there is.
// A native publication whose body began with the magic was therefore decoded as
// metadata, or failed to decode and left a record that could be neither
// delivered nor settled.
//
// DetectFull answers from the whole envelope: the framing, every declared
// length, the canonical field order and the absence of trailing bytes, checked
// by the decoder itself rather than by a second reading of its rules. It
// applies no [Limits] at all, because whether a byte string is an envelope
// cannot depend on what one consumer is willing to accept — a consumer that
// read an over-large envelope as a native body would deliver the envelope's own
// bytes as a payload instead of refusing it.
//
// It costs a decode into a pooled scratch payload, which is discarded, so a
// caller that is going to decode anyway should decode first and use DetectFull
// only to tell a refused envelope from a native body.
//
// It is not a proof of provenance. A producer that deliberately publishes a
// valid envelope as a native body is indistinguishable from one that enveloped
// it, so a driver that can record the answer OUT OF BAND — a native header, a
// message attribute — must do that instead and never sniff at all.
func (d *Detector) DetectFull(src []byte) (Kind, bool) {
	k, ok := d.d.DetectFull(src)
	return Kind(k), ok
}

// Detector answers [Detector.DetectFull]. It owns the decoders a probe runs
// through, which are too large to build per call, so a caller builds one and
// keeps it for as long as it probes. It is safe for concurrent use.
type Detector struct{ d *wire.Detector }

// NewDetector returns a Detector.
func NewDetector() *Detector { return &Detector{d: wire.NewDetector()} }

// Limits bounds what Decode accepts and selects its copy mode. A zero field
// means the [DefaultLimits] value; a negative field means no limit beyond the
// envelope's own length.
type Limits struct {
	// MaxKey bounds the key length in bytes.
	MaxKey int
	// MaxBody bounds the body length in bytes.
	MaxBody int
	// MaxID bounds the message identifier in bytes.
	MaxID int
	// MaxHeaders bounds the number of headers.
	MaxHeaders int
	// MaxHeaderKey bounds one header key in bytes.
	MaxHeaderKey int
	// MaxHeaderValue bounds one header value in bytes.
	MaxHeaderValue int
	// MaxProp bounds each string property in bytes.
	MaxProp int
	// ZeroCopy makes Decode alias src for the key, the body and the header
	// values instead of copying them. The message is then valid only while
	// src is unmodified, which is the borrowed contract a caller
	// that keeps the message past the buffer clones it.
	ZeroCopy bool
}

// DefaultLimits returns the limits Decode applies for a nil *Limits. It is
// a function so that every caller gets its own copy and no caller can change
// the defaults another one decodes under.
func DefaultLimits() Limits {
	return Limits{
		MaxKey:         wire.DefaultLimits.MaxKey,
		MaxBody:        wire.DefaultLimits.MaxBody,
		MaxID:          wire.DefaultLimits.MaxID,
		MaxHeaders:     wire.DefaultLimits.MaxHeaders,
		MaxHeaderKey:   wire.DefaultLimits.MaxHeaderKey,
		MaxHeaderValue: wire.DefaultLimits.MaxHeaderValue,
		MaxProp:        wire.DefaultLimits.MaxProp,
	}
}

// internal is the decoder's own form of the limits.
func (l *Limits) internal() *wire.Limits {
	if l == nil {
		return nil
	}
	return &wire.Limits{
		MaxKey:         l.MaxKey,
		MaxBody:        l.MaxBody,
		MaxID:          l.MaxID,
		MaxHeaders:     l.MaxHeaders,
		MaxHeaderKey:   l.MaxHeaderKey,
		MaxHeaderValue: l.MaxHeaderValue,
		MaxProp:        l.MaxProp,
		ZeroCopy:       l.ZeroCopy,
	}
}

// Layout encodes messages into envelopes and back.
type Layout interface {
	// Kind returns the concrete layout, never Auto.
	Kind() Kind
	// Size returns the exact number of bytes Encode appends for m. It
	// returns 0 for a message the layout cannot represent.
	Size(m *mq.Message) int
	// Encode appends the envelope of m to dst and returns the extended
	// slice. It grows dst at most once, by exactly Size(m) when the
	// capacity is short, and fails only when a field does not fit the
	// layout's widths.
	Encode(dst []byte, m *mq.Message) ([]byte, error)
	// Decode parses one envelope from src into m, replacing every field. A
	// nil lim means DefaultLimits. On error m is unspecified and the error
	// carries CodeCorruptMessage or CodeMessageTooLarge.
	Decode(src []byte, m *mq.Message, lim *Limits) error
	// Check reports whether Encode accepts m and Decode under lim then
	// accepts the envelope Encode produced — the question "will what I am
	// about to publish read back?", answered before any I/O and without
	// encoding anything.
	//
	// It is not a convenience. The encoder validates only the layout's own
	// widths; the decoder additionally enforces every Limits field and
	// validates UTF-8 in the identifier, in every header key and in every
	// string property. A caller that encodes without checking can therefore
	// store a record its own Decode refuses for ever, which on a message
	// broker is a message that can be neither delivered nor settled. Check
	// answers from the same resolved limits and the same validation rules
	// Decode applies, so the two cannot drift.
	//
	// A nil lim means DefaultLimits, and the error carries
	// CodeMessageTooLarge for a bound and CodeCorruptMessage for invalid
	// UTF-8 — the same errors Decode would have returned.
	Check(m *mq.Message, lim *Limits) error
}

// New returns the layout of kind k, or fails on a kind that names no layout.
func New(k Kind) (Layout, error) {
	if !k.Valid() {
		return nil, fmt.Errorf("iqmq/wire: invalid layout kind %d", k)
	}
	l, err := wire.New(wire.Kind(k))
	if err != nil {
		return nil, fmt.Errorf("iqmq/wire: %w", err)
	}
	return &layout{l: l}, nil
}

// layout is the message-shaped view of one internal layout.
type layout struct {
	l wire.Layout
	// scratches holds the storage form a message is converted through, so
	// that a steady stream of encodings through one layout allocates nothing
	// per message.
	scratches sync.Pool
}

// Kind returns the concrete layout.
func (w *layout) Kind() Kind { return Kind(w.l.Kind()) }

// Size returns the exact encoded length of m.
func (w *layout) Size(m *mq.Message) int {
	if m == nil {
		return 0
	}
	s := w.scratchFor(m)
	defer w.putScratch(s)
	return w.l.Size(&s.p)
}

// Encode appends the envelope of m to dst.
func (w *layout) Encode(dst []byte, m *mq.Message) ([]byte, error) {
	if m == nil {
		return dst, errNilMessage
	}
	s := w.scratchFor(m)
	defer w.putScratch(s)
	return w.l.Encode(dst, &s.p)
}

// Decode parses one envelope from src into m.
//
// The storage it hands to m is its own — a fresh key, body and header table —
// because m outlives the call and a pooled buffer would be reused by the next
// Decode. Under Limits.ZeroCopy nothing is copied at all and the storage is
// src's, which is the borrowed form.
func (w *layout) Decode(src []byte, m *mq.Message, lim *Limits) error {
	if m == nil {
		return errNilMessage
	}
	var p payload.Payload
	if err := w.l.Decode(src, &p, lim.internal()); err != nil {
		return err
	}
	m.Key = p.Key
	m.Body = p.Body
	m.ID = p.ID
	m.Properties = mq.Properties{
		ContentType:     p.Props.ContentType,
		ContentEncoding: p.Props.ContentEncoding,
		CorrelationID:   p.Props.CorrelationID,
		CausationID:     p.Props.CausationID,
		ReplyTo:         p.Props.ReplyTo,
		CreatedAt:       p.Props.CreatedAt,
	}
	// A new table rather than m.Headers.Reset: a copy of m shares its header
	// storage, so refilling that storage in place rewrote the headers of
	// every copy taken after an earlier Decode.
	var h mq.Headers
	for _, kv := range p.Headers {
		h.Add(kv.Key, kv.Value)
	}
	m.Headers = h
	return nil
}

// Check reports whether Decode under lim would accept the envelope Encode
// produces for m.
//
// The message is converted through the same pooled scratch Encode uses, so a
// check costs no allocation in steady state and a driver can afford one per
// publication.
func (w *layout) Check(m *mq.Message, lim *Limits) error {
	if m == nil {
		return errNilMessage
	}
	s := w.scratchFor(m)
	defer w.putScratch(s)
	return w.l.Check(&s.p, lim.internal())
}

// scratch is the storage form a message is converted through, plus the header
// table it is built in.
//
// The table is kept beside the payload rather than in it, because the
// payload's own field has to be able to go back to NIL: the layouts
// distinguish a nil header slice from an empty one — they distinguish nil
// from empty for every byte field — and a pooled table would make an empty
// one non-nil, so the same message would encode to different bytes depending
// on what the pool had been used for. That is what scratch.headers is for:
// the capacity is reused and the nil-ness is not
// (adr/ADR-017-assumptions.md).
type scratch struct {
	p       payload.Payload
	headers []payload.KV
}

// scratchFor converts a message into the engine's storage form, reusing a
// pooled header table.
//
// It borrows the message's slices rather than copying them: Encode and Size
// only read, and the scratch never outlives the call. The mapping is a
// FUNCTION of what the message can be observed to hold, so that Encode is one
// too: a message with no headers always converts to a nil header slice,
// whatever the pool held before it.
func (w *layout) scratchFor(m *mq.Message) *scratch {
	s, _ := w.scratches.Get().(*scratch)
	if s == nil {
		s = new(scratch)
	}
	s.p.Key = m.Key
	s.p.Body = m.Body
	s.p.ID = m.ID
	s.p.Props = payload.Props{
		ContentType:     m.Properties.ContentType,
		ContentEncoding: m.Properties.ContentEncoding,
		CorrelationID:   m.Properties.CorrelationID,
		CausationID:     m.Properties.CausationID,
		ReplyTo:         m.Properties.ReplyTo,
		CreatedAt:       m.Properties.CreatedAt,
	}
	s.headers = appendHeaders(s.headers[:0], m.Headers.All())
	s.p.Headers = nil
	if len(s.headers) > 0 {
		s.p.Headers = s.headers
	}
	return s
}

// appendHeaders copies a message's header pairs into the storage form.
func appendHeaders(dst []payload.KV, seq iter.Seq2[string, []byte]) []payload.KV {
	for k, v := range seq {
		dst = append(dst, payload.KV{Key: k, Value: v})
	}
	return dst
}

// putScratch returns the scratch to the pool without keeping a reference to
// the caller's storage.
func (w *layout) putScratch(s *scratch) {
	s.p.Key, s.p.Body, s.p.ID = nil, nil, ""
	s.p.Props = payload.Props{}
	s.p.Headers = nil
	clear(s.headers)
	s.headers = s.headers[:0]
	w.scratches.Put(s)
}

// The decode and encode failures. Each carries the code named in its
// documentation, and errors.Is matches by code, so a caller compares
// mq.CodeOf(err) against mq.CodeCorruptMessage or mq.CodeMessageTooLarge —
// the distinct values are for a caller that needs to tell two corruptions
// apart.
var (
	// ErrMagic reports an envelope that does not begin with the iqmq magic.
	ErrMagic = wire.ErrMagic
	// ErrVersion reports a layout or version byte this decoder does not
	// understand, including an envelope of the other layout.
	ErrVersion = wire.ErrVersion
	// ErrFlags reports a reserved flag bit, or a field whose presence
	// contradicts the flags.
	ErrFlags = wire.ErrFlags
	// ErrTruncated reports a declared length that overruns the envelope.
	ErrTruncated = wire.ErrTruncated
	// ErrVarint reports an overlong, overflowing or incomplete varint.
	ErrVarint = wire.ErrVarint
	// ErrFieldOrder reports metadata fields that are not in ascending tag
	// order: the canonical encoding writes them sorted, so a decoder that
	// accepted any order would accept several encodings of one message.
	ErrFieldOrder = wire.ErrFieldOrder
	// ErrDuplicate reports a singular field that appears twice.
	ErrDuplicate = wire.ErrDuplicate
	// ErrUnknownField reports a critical tag this decoder does not know.
	ErrUnknownField = wire.ErrUnknownField
	// ErrTrailing reports bytes after the end of the envelope.
	ErrTrailing = wire.ErrTrailing
	// ErrUTF8 reports invalid UTF-8 in the identifier, a header key or a
	// string property.
	ErrUTF8 = wire.ErrUTF8
	// ErrTimestamp reports a created-at field with nanoseconds at or above
	// one second, or a malformed length.
	ErrTimestamp = wire.ErrTimestamp
	// ErrLimit reports a field that fits the envelope but exceeds the
	// caller's Limits.
	ErrLimit = wire.ErrLimit
	// ErrUnencodable reports a field too long for the layout's widths.
	ErrUnencodable = wire.ErrUnencodable
)

// errNilMessage is what a nil destination or source message is refused with.
// The specification names no sentinel for it and errors.Is matches by code, so a caller
// compares mq.CodeOf(err) against mq.CodeInvalidArgument.
var errNilMessage = errcode.New(errcode.InvalidArgument, "iqmq/wire: message is nil")

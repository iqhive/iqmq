// Package wire holds the portable binary envelope layouts the two ways a payload.Payload is serialised for
// backends that carry an opaque body, so that headers, key, ID and typed
// properties travel with the message.
//
// Both layouts share a six-byte prefix — the magic "IQMQ", one layout and
// version byte, one flags byte — and encode exactly the same field set, so
// Decode(Encode(p)) reproduces p for every payload, including the
// distinction between a nil and an empty key, body, header slice or header
// value. Encoding is canonical: one payload has exactly one valid encoding
// per layout, and Decode rejects everything else with a CorruptMessage
// error (bad magic, unsupported version, reserved flags, overlong or
// truncated varints, lengths that overrun the envelope, duplicate or
// unknown critical fields, invalid UTF-8 in string fields, trailing bytes)
// or a MessageTooLarge error when a field exceeds the caller's Limits.
//
// Decode never allocates in proportion to a declared length before that
// length has been checked against the remaining input, so a hostile
// envelope can cost at most its own size. Byte fields are copied into the
// destination payload's existing storage (or alias the input under
// Limits.ZeroCopy); header keys and string properties come from a small
// per-layout intern table, so a steady stream of messages with the usual
// header names decodes without allocating. The message ID is the one field
// that allocates when present.
//
// Every Layout is safe for concurrent use.
package wire

import (
	"encoding/binary"
	"time"
	"unicode/utf8"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/payload"
)

// Kind identifies an envelope layout. Auto resolves to AutoKind.
type Kind uint8

// Kinds. Fixed is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// Fixed is the fixed-width length-prefixed layout: a 42-byte header of
	// little-endian lengths followed by the fields in a fixed order. It is
	// the reference; it cannot carry more than 65535 headers or string
	// fields longer than 65535 bytes.
	Fixed
	// TLV is the varint tag-length-value layout: varint metadata and body
	// lengths, then a sequence of tagged fields, then the body. Unknown
	// odd tags are skipped, so a v1 decoder accepts envelopes from a later
	// minor version that adds optional fields.
	TLV

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "fixed", "tlv"}

var kindIdents = [kindCount]string{"Auto", "Fixed", "TLV"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := Fixed; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// ForwardCompatible reports whether a decoder of this kind accepts
// envelopes that carry optional fields it does not know.
func (k Kind) ForwardCompatible() bool { return k.Resolve() == TLV }

// Layout encodes payloads into envelopes and back.
type Layout interface {
	// Kind returns the concrete kind (never Auto).
	Kind() Kind
	// Size returns the exact number of bytes Encode appends for p. For a
	// payload Encode rejects it returns 0.
	Size(p *payload.Payload) int
	// Encode appends the envelope of p to dst and returns the extended
	// slice. It grows dst at most once, by exactly Size(p) when the
	// capacity is short, and fails only when a field does not fit the
	// layout (ErrUnencodable).
	Encode(dst []byte, p *payload.Payload) ([]byte, error)
	// Decode parses one envelope from src into p, replacing every field.
	// A nil lim means DefaultLimits. Byte fields are copied into p's
	// existing storage, or alias src when lim.ZeroCopy is set. On error p
	// is unspecified and the error carries CorruptMessage or
	// MessageTooLarge.
	Decode(src []byte, p *payload.Payload, lim *Limits) error
	// Check reports whether Encode accepts p and Decode under lim then
	// accepts what Encode produced, without encoding anything. It answers
	// from the decoder's own resolved limits and the decoder's own
	// validation rules, so that a caller can refuse a message before any
	// I/O rather than storing a record nothing can read back.
	Check(p *payload.Payload, lim *Limits) error
}

// New returns the layout of kind k. It panics on an invalid kind.
func New(k Kind) Layout {
	k = k.Resolve()
	switch k {
	case Fixed:
		return &fixedLayout{}
	case TLV:
		return &tlvLayout{}
	default:
		panic("wire: invalid kind")
	}
}

// Limits bounds what Decode accepts and selects the copy mode. A zero
// field means the DefaultLimits value; a negative field means no limit
// beyond the envelope length itself.
type Limits struct {
	// MaxKey bounds the key length in bytes.
	MaxKey int
	// MaxBody bounds the body length in bytes.
	MaxBody int
	// MaxID bounds the message ID length in bytes.
	MaxID int
	// MaxHeaders bounds the number of headers.
	MaxHeaders int
	// MaxHeaderKey bounds one header key in bytes.
	MaxHeaderKey int
	// MaxHeaderValue bounds one header value in bytes.
	MaxHeaderValue int
	// MaxProp bounds each string property in bytes.
	MaxProp int
	// ZeroCopy makes Decode alias src for the key, body and header values
	// instead of copying them into p. The payload is then valid only
	// while src is unmodified, and must not later be reused as the
	// destination of a copying Decode or payload.CloneInto without first
	// dropping the aliased slices.
	ZeroCopy bool
}

// DefaultLimits are the limits Decode applies for a nil *Limits.
var DefaultLimits = Limits{
	MaxKey:         64 << 10,
	MaxBody:        64 << 20,
	MaxID:          256,
	MaxHeaders:     1024,
	MaxHeaderKey:   256,
	MaxHeaderValue: 64 << 10,
	MaxProp:        4 << 10,
}

// limit resolves one field: zero → default, negative → unbounded.
func limit(v, def int) int {
	switch {
	case v == 0:
		return def
	case v < 0:
		return int(^uint(0) >> 1)
	default:
		return v
	}
}

// resolved is a Limits with every default applied.
type resolved struct {
	key, body, id, headers, hkey, hval, prop int
	zeroCopy                                 bool
}

// defaultResolved is DefaultLimits with every default applied, computed
// once so the common call (nil limits) costs a comparison, not seven.
var defaultResolved = resolveSlow(&DefaultLimits)

func resolve(lim *Limits) resolved {
	if lim == nil || *lim == DefaultLimits {
		return defaultResolved
	}
	return resolveSlow(lim)
}

func resolveSlow(lim *Limits) resolved {
	d := &DefaultLimits
	return resolved{
		key:      limit(lim.MaxKey, d.MaxKey),
		body:     limit(lim.MaxBody, d.MaxBody),
		id:       limit(lim.MaxID, d.MaxID),
		headers:  limit(lim.MaxHeaders, d.MaxHeaders),
		hkey:     limit(lim.MaxHeaderKey, d.MaxHeaderKey),
		hval:     limit(lim.MaxHeaderValue, d.MaxHeaderValue),
		prop:     limit(lim.MaxProp, d.MaxProp),
		zeroCopy: lim.ZeroCopy,
	}
}

// Decode errors. All but ErrLimit and ErrUnencodable carry CorruptMessage.
var (
	// ErrMagic reports an envelope that does not start with "IQMQ".
	ErrMagic = errcode.New(errcode.CorruptMessage, "wire: bad magic")
	// ErrVersion reports a layout or version byte this decoder does not
	// understand, including an envelope of the other layout.
	ErrVersion = errcode.New(errcode.CorruptMessage, "wire: unsupported layout or version")
	// ErrFlags reports a reserved flag bit, or a field whose presence
	// contradicts the flags.
	ErrFlags = errcode.New(errcode.CorruptMessage, "wire: invalid flags")
	// ErrTruncated reports a declared length that overruns the envelope.
	ErrTruncated = errcode.New(errcode.CorruptMessage, "wire: truncated envelope")
	// ErrVarint reports an overlong, overflowing or incomplete varint.
	ErrVarint = errcode.New(errcode.CorruptMessage, "wire: malformed varint")
	// ErrFieldOrder reports metadata fields that are not in ascending tag
	// order: the canonical encoding writes them sorted, so a decoder that
	// accepted any order would accept several encodings of one payload.
	ErrFieldOrder = errcode.New(errcode.CorruptMessage, "wire: fields out of canonical order")
	// ErrDuplicate reports a singular field that appears twice.
	ErrDuplicate = errcode.New(errcode.CorruptMessage, "wire: duplicate field")
	// ErrUnknownField reports a critical (even) tag this decoder does not know.
	ErrUnknownField = errcode.New(errcode.CorruptMessage, "wire: unknown critical field")
	// ErrTrailing reports bytes after the end of the envelope.
	ErrTrailing = errcode.New(errcode.CorruptMessage, "wire: trailing bytes")
	// ErrUTF8 reports invalid UTF-8 in the ID, a header key or a property.
	ErrUTF8 = errcode.New(errcode.CorruptMessage, "wire: invalid UTF-8 in string field")
	// ErrTimestamp reports a created-at field with nanoseconds ≥ 1e9 or a
	// malformed length.
	ErrTimestamp = errcode.New(errcode.CorruptMessage, "wire: invalid timestamp")
	// ErrLimit (MessageTooLarge) reports a field that fits the envelope
	// but exceeds the caller's Limits.
	ErrLimit = errcode.New(errcode.MessageTooLarge, "wire: field exceeds decode limit")
	// ErrUnencodable (MessageTooLarge) is returned by Encode when a field
	// is too long for the layout's width.
	ErrUnencodable = errcode.New(errcode.MessageTooLarge, "wire: field too long for layout")
)

// The shared six-byte prefix.
const (
	magic0, magic1, magic2, magic3 = 'I', 'Q', 'M', 'Q'
	// magicWord is the magic read as one little-endian uint32.
	magicWord = uint32(magic0) | uint32(magic1)<<8 | uint32(magic2)<<16 | uint32(magic3)<<24
	prefixLen = 6
	// Layout/version byte: high nibble layout, low nibble version.
	verTLV   = 0x01
	verFixed = 0x11
)

// Flags byte. Presence bits distinguish nil from empty.
const (
	flagKey       = 0x01
	flagBody      = 0x02
	flagHeaders   = 0x04
	flagCreatedAt = 0x08
	flagsKnown    = flagKey | flagBody | flagHeaders | flagCreatedAt
)

// Header-entry flags byte.
const (
	hflagValue = 0x01
	hflagKnown = hflagValue
)

// flagsOf computes the flags byte of p.
func flagsOf(p *payload.Payload) byte {
	var f byte
	if p.Key != nil {
		f |= flagKey
	}
	if p.Body != nil {
		f |= flagBody
	}
	if p.Headers != nil {
		f |= flagHeaders
	}
	if !p.Props.CreatedAt.IsZero() {
		f |= flagCreatedAt
	}
	return f
}

// Detect reports the kind of the envelope in src from its prefix, without
// decoding it. It reports false for anything that is not an iqmq envelope
// of a known layout and version.
func Detect(src []byte) (Kind, bool) {
	if len(src) < prefixLen || src[0] != magic0 || src[1] != magic1 || src[2] != magic2 || src[3] != magic3 {
		return 0, false
	}
	switch src[4] {
	case verFixed:
		return Fixed, true
	case verTLV:
		return TLV, true
	default:
		return 0, false
	}
}

// checkPrefix validates the shared prefix for the given version byte and
// returns the flags. The magic is compared as one little-endian word;
// telling the three rejections apart is left to prefixError, which is only
// reached for an envelope that is already being refused.
func checkPrefix(src []byte, ver byte) (byte, error) {
	if len(src) >= prefixLen && binary.LittleEndian.Uint32(src) == magicWord && src[4] == ver {
		f := src[5]
		if f&^flagsKnown != 0 {
			return 0, ErrFlags
		}
		return f, nil
	}
	return 0, prefixError(src, ver)
}

func prefixError(src []byte, ver byte) error {
	if len(src) < 4 {
		return ErrTruncated
	}
	if binary.LittleEndian.Uint32(src) != magicWord {
		return ErrMagic
	}
	if len(src) < prefixLen {
		return ErrTruncated
	}
	if src[4] != ver {
		return ErrVersion
	}
	return ErrFlags
}

// appendPrefix writes the shared prefix.
func appendPrefix(dst []byte, ver, flags byte) []byte {
	return append(dst, magic0, magic1, magic2, magic3, ver, flags)
}

// Timestamp encoding: int64 little-endian Unix seconds + uint32
// little-endian nanoseconds, twelve bytes, for the full time.Time range.
const timestampLen = 12

func appendTimestamp(dst []byte, t time.Time) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(t.Unix()))
	return binary.LittleEndian.AppendUint32(dst, uint32(t.Nanosecond()))
}

func decodeTimestamp(b []byte) (time.Time, error) {
	if len(b) != timestampLen {
		return time.Time{}, ErrTimestamp
	}
	sec := int64(binary.LittleEndian.Uint64(b))
	nsec := binary.LittleEndian.Uint32(b[8:])
	if nsec >= 1e9 {
		return time.Time{}, ErrTimestamp
	}
	t := time.Unix(sec, int64(nsec)).UTC()
	if t.IsZero() {
		// The zero instant is only ever encoded through the flag's absence.
		return time.Time{}, ErrTimestamp
	}
	return t, nil
}

// uvarint decodes a canonical unsigned LEB128 value from the front of b.
// It returns n == 0 when b ends inside the varint and n < 0 when the
// encoding is overlong or overflows 64 bits.
func uvarint(b []byte) (v uint64, n int) {
	var s uint
	for i := 0; i < len(b) && i < 10; i++ {
		c := b[i]
		if c < 0x80 {
			if i == 9 && c > 1 {
				return 0, -1
			}
			if i > 0 && c == 0 {
				return 0, -1
			}
			return v | uint64(c)<<s, i + 1
		}
		v |= uint64(c&0x7f) << s
		s += 7
	}
	if len(b) >= 10 {
		return 0, -1
	}
	return 0, 0
}

// uvarintLen returns the encoded length of v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// Widths readLen reports instead of an error, so that the fast path has no
// interface-typed result and stays within the inliner's budget.
const (
	lenTruncated = 0  // the length overruns the envelope, or ends inside it
	lenBadVarint = -1 // overlong or 64-bit-overflowing varint
)

// lenError turns a non-positive readLen width into the decode error it
// stands for. It is only ever reached on a rejected envelope.
func lenError(n int) error {
	if n < 0 {
		return ErrVarint
	}
	return ErrTruncated
}

// readLen decodes a varint length from the front of b. The value must fit
// in the bytes that follow the varint inside b, less reserved bytes that
// belong to a later section of the same buffer. It returns the length and
// the width of the varint; a width of zero or less is a failure the caller
// turns into an error with lenError.
//
// The failure is reported as a width rather than as an error so that the
// one-byte case — every header name and every field shorter than 128
// bytes, which is all but two of the dozen-odd lengths a decode reads —
// inlines into the caller. An error-typed result put this function 40 %
// over the inliner's budget and cost a call per length.
func readLen(b []byte) (v, n int) {
	if len(b) > 0 && b[0] < 0x80 {
		if int(b[0]) >= len(b) {
			return 0, lenTruncated
		}
		return int(b[0]), 1
	}
	return readLenSlow(b, 0)
}

// readLenReserve is readLen for a length that must leave reserved trailing
// bytes of b to a later section of the same buffer. Only the body length,
// which is followed by nothing but is preceded by the metadata section,
// needs it, so it stays out of readLen's inline budget.
func readLenReserve(b []byte, reserved int) (v, n int) {
	if len(b) > 0 && b[0] < 0x80 {
		if int(b[0]) > len(b)-1-reserved {
			return 0, lenTruncated
		}
		return int(b[0]), 1
	}
	return readLenSlow(b, reserved)
}

// readLenSlow decodes the lengths that do not fit in seven bits. Two bytes
// cover everything up to 16 KiB — a metadata section and most bodies — so
// that case is handled here rather than through uvarint.
func readLenSlow(b []byte, reserved int) (int, int) {
	if len(b) > 1 && b[0] >= 0x80 {
		// A zero continuation byte is an overlong encoding of the
		// one-byte case; leave it to uvarint to reject.
		if c := b[1]; c != 0 && c < 0x80 {
			v := int(b[0]&0x7f) | int(c)<<7
			if v > len(b)-2-reserved {
				return 0, lenTruncated
			}
			return v, 2
		}
	}
	v, n := uvarint(b)
	if n <= 0 {
		if n == 0 {
			return 0, lenTruncated
		}
		return 0, lenBadVarint
	}
	avail := len(b) - n - reserved
	if avail < 0 {
		avail = 0
	}
	if v > uint64(avail) {
		return 0, lenTruncated
	}
	return int(v), n
}

// cloneInto copies src into dst's storage when its capacity allows,
// preserving the nil-versus-empty distinction of present.
func cloneInto(dst, src []byte, present, zeroCopy bool) []byte {
	if !present {
		return nil
	}
	if zeroCopy {
		return src[:len(src):len(src)]
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

// headersFor returns p.Headers resized to n entries, reusing its storage.
func headersFor(p *payload.Payload, n int, present bool) []payload.KV {
	if !present {
		return nil
	}
	if n == 0 {
		if p.Headers == nil {
			return []payload.KV{}
		}
		return p.Headers[:0]
	}
	if cap(p.Headers) >= n {
		return p.Headers[:n]
	}
	h := make([]payload.KV, n)
	copy(h, p.Headers)
	return h
}

// setID assigns the decoded ID to p. The comparison does not allocate
// (the compiler elides the conversion), so decoding a stream in which the
// same destination payload is reused for messages whose ID repeats — and
// every ID-less message — costs nothing; a genuinely new ID costs the one
// string allocation the payload's string-typed ID field implies.
func setID(p *payload.Payload, id []byte) {
	if p.ID != string(id) {
		p.ID = string(id)
	}
}

// asciiMask has the high bit of every byte of a word set.
const asciiMask = 0x8080808080808080

// validString reports whether b is valid UTF-8. Header names and string
// properties are ASCII in practice, so the common case is a word-at-a-time
// scan for a high bit; the first non-ASCII byte hands the rest to
// unicode/utf8 (everything before it is ASCII, so no rune is split).
//
// A trailing partial word is covered by one more word load that overlaps
// the last full one rather than by a byte loop, so a header name of any
// length up to eight words costs at most nine loads and no branch per
// byte. Only strings shorter than a word take the byte path.
func validString(b []byte) bool {
	n := len(b)
	i := 0
	for ; i+8 <= n; i += 8 {
		if binary.LittleEndian.Uint64(b[i:])&asciiMask != 0 {
			return utf8.Valid(b[i:])
		}
	}
	if i == n {
		return true
	}
	if n >= 8 {
		if binary.LittleEndian.Uint64(b[n-8:])&asciiMask != 0 {
			return utf8.Valid(b[i:])
		}
		return true
	}
	for ; i < n; i++ {
		if b[i] >= utf8.RuneSelf {
			return utf8.Valid(b[i:])
		}
	}
	return true
}

// inlineHeaders is how many header positions a decode records on the stack
// during validation so that the commit pass does not re-parse them. An
// envelope with more headers than this is parsed twice, which is the right
// trade: the first bytes of a message are hot, the hundredth header is not.
const inlineHeaders = 8

// headerRef is one validated header's position in the source buffer: the
// key starts at off and the value follows it immediately.
type headerRef struct {
	off   int32
	klen  int32
	vlen  int32
	flags byte
}

// keep returns prev when it already equals b — the steady state of a
// consumer decoding a stream of similar messages into one destination
// payload — and otherwise interns b. The comparison does not allocate.
func (in *interner) keep(prev string, b []byte) string {
	if prev == string(b) {
		return prev
	}
	return in.get(b)
}

// grow returns dst with room for at least n more bytes, allocating exactly
// once when needed.
func grow(dst []byte, n int) []byte {
	if cap(dst)-len(dst) >= n {
		return dst
	}
	out := make([]byte, len(dst), len(dst)+n)
	copy(out, dst)
	return out
}

// payloadTime is the type of payload.Props.CreatedAt.
type payloadTime = time.Time

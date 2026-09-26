package streams

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/wire"
)

// defaultWireLimits is the portable envelope's own limit set, which is where
// this layout's defaults come from: driverkit.Envelope decodes under it, so a
// message this package admits has to fit it or MetadataPortableEnvelope
// produces a record nothing can decode.
var defaultWireLimits = wire.DefaultLimits

//	b        the body
//	k        the partition/ordering key
//	t        the logical topic
//	h:<n>    header n, in publication order, n counting from 0
//	p:<n>    typed property n, n naming the property (PropID and friends)
//
// A field is present only when it has a value, and absence is not the same as
// emptiness: the message contract makes a nil Body "no body" and an empty
// non-nil Body "an empty body", and [Clone] preserves that distinction, so
// the layout has to as well. It does it the only way a field-value list can —
// a nil body writes no "b" field and an empty one writes "b" with an empty
// value.
//
// Everything else a Redis Streams driver needs is bookkeeping rather than
// message content, and it lives under the reserved [FieldPrefixInternal]
// prefix, which [Decode] skips: the attempt count a re-add strategy has to
// carry itself, the deliver-at time the delayed mover parks. Keeping them out
// of the message namespace is what lets [Decode] be strict about "h:" and
// "p:" and lenient about a field it has never seen.
const (
	// FieldBody is the body field.
	FieldBody = "b"
	// FieldKey is the key field.
	FieldKey = "k"
	// FieldTopic is the logical-topic field.
	FieldTopic = "t"
	// FieldPrefixHeader prefixes a positional header field.
	FieldPrefixHeader = "h:"
	// FieldPrefixProp prefixes a typed-property field.
	FieldPrefixProp = "p:"
	// FieldPrefixInternal prefixes the driver's own bookkeeping fields,
	// which are not message content and which Decode ignores.
	FieldPrefixInternal = "x:"
	// FieldAttempt carries the delivery attempt across a strategy that
	// re-adds an entry under a new identifier, where the broker's own
	// delivery counter restarts.
	FieldAttempt = FieldPrefixInternal + "a"
	// FieldDeliverAt carries the unix-nanosecond time a delayed redelivery
	// becomes deliverable.
	FieldDeliverAt = FieldPrefixInternal + "d"
	// FieldEmpty is written when a message has NOTHING to encode, so that
	// the entry has at least one field.
	//
	// It exists because XADD refuses an entry with no field-value pairs
	// ("wrong number of arguments for 'xadd' command"), and a message with a
	// nil body, a nil key, no topic, no headers and no properties encodes to
	// nothing — which is exactly the first message the
	// publish/roundtrip-bodies scenario sends. That publication failed
	// against a real server and could not fail against the entry layout's
	// own round-trip test, because the round trip never needed a command.
	FieldEmpty = FieldPrefixInternal + "e"
)

// Prop indexes one typed property inside a "p:<n>" field name.
//
// The indices are part of the wire format and are therefore append-only: a
// reader of an entry written by an older driver reads the same numbers, so a
// renumbering would silently move a correlation identifier into a reply-to.
type Prop uint8

// The typed properties of the API, plus the message identifier.
//
// PropID is index 0 rather than a field of its own, because the specification's
// Message.ID is metadata in exactly the same sense as the rest: it has no
// native home in a Redis Streams entry, so it goes where the other homeless
// metadata goes rather than inventing a sixth top-level field name the plan's
// layout does not have.
const (
	// PropID is Message.ID.
	PropID Prop = iota
	// PropContentType is Properties.ContentType.
	PropContentType
	// PropContentEncoding is Properties.ContentEncoding.
	PropContentEncoding
	// PropCorrelationID is Properties.CorrelationID.
	PropCorrelationID
	// PropCausationID is Properties.CausationID.
	PropCausationID
	// PropReplyTo is Properties.ReplyTo.
	PropReplyTo
	// PropCreatedAt is Properties.CreatedAt, as "<seconds>,<nanoseconds>"
	// since the unix epoch.
	//
	// It is two decimal integers and not a unix-nanosecond count, because a
	// single nanosecond count overflows an int64 outside 1678..2262 and a
	// time a caller can construct must not become a different time on the
	// way back. It is not RFC 3339 either: that would make the round trip
	// depend on a location the entry cannot carry.
	PropCreatedAt

	propCount
)

// Valid reports whether p names a property.
func (p Prop) Valid() bool { return p < propCount }

// String returns the property's field name.
func (p Prop) String() string {
	if p < propCount {
		return propNames[p]
	}
	return FieldPrefixProp + strconv.Itoa(int(p))
}

// The field names, precomputed.
//
// A name is "h:" or "p:" and a decimal index, and building one is a string
// concatenation — which allocates. On the publish path that is one allocation
// per header and one per property, so a message with three headers and two
// properties paid five allocations for names drawn from a set of about
// seventy. TestAllocsMapping is what noticed: the encode was ten allocations
// for a three-header message where the bytes it copies account for five.
//
// The header table covers as many headers as a message usually has; a message
// with more falls back to the concatenation, which is correct and costs what
// it used to.
var (
	propNames   [propCount]string
	headerNames [64]string
)

func init() {
	for i := range propNames {
		propNames[i] = FieldPrefixProp + strconv.Itoa(i)
	}
	for i := range headerNames {
		headerNames[i] = FieldPrefixHeader + strconv.Itoa(i)
	}
}

// headerName is the field name of header n.
func headerName(n int) string {
	if n >= 0 && n < len(headerNames) {
		return headerNames[n]
	}
	return FieldPrefixHeader + strconv.Itoa(n)
}

// Limits bounds what the field layout accepts.
//
// It is checked BEFORE a publication is admitted, and that is the whole point
// of the type. The field layout itself can carry anything — RESP bulk strings
// are binary-safe and the header framing is length-prefixed — so a limit set
// is not a limitation of the encoding. It is a limitation of what the rest of
// the driver can decode:
//
// the message contract's MetadataPortableEnvelope mode puts the whole message
// through driverkit.Envelope, whose Wrap encodes anything and whose Unwrap
// decodes under wire.Limits. Left unchecked, a 259-byte header key — one over
// wire.DefaultLimits.MaxHeaderKey — is accepted at publish and then breaks
// every Fetch on that subscription for ever, because the record cannot be
// decoded and therefore cannot be settled. The specification found that hole with a fuzz
// target in five seconds, and then found it a second time in another
// sixty-seven against a fix that bounded only the headers: the next input was
// a 259-byte message identifier.
//
// So the bound is one set, applied in EVERY metadata mode rather than only in
// the modes that envelope, and it is the wire layout's own set rather than a
// number this package invented. A message accepted in one mode is then
// accepted in all of them, and a message the driver accepted is a message the
// driver can decode — which is the property [FuzzRedisFields] exists to hold
// and the one the specification calls a disqualifier to break.
type Limits struct {
	// MaxKey bounds the key in bytes.
	MaxKey int
	// MaxBody bounds the body in bytes.
	MaxBody int
	// MaxID bounds Message.ID in bytes.
	MaxID int
	// MaxTopic bounds the logical topic in bytes.
	MaxTopic int
	// MaxHeaders bounds the number of headers.
	MaxHeaders int
	// MaxHeaderKey bounds one header key in bytes.
	MaxHeaderKey int
	// MaxHeaderValue bounds one header value in bytes.
	MaxHeaderValue int
	// MaxProp bounds each string property in bytes.
	MaxProp int
}

// DefaultLimits are the bounds a driver gets by not saying.
//
// Every field but MaxTopic is wire.DefaultLimits', field for field, which is
// what makes the envelope's decoder and the driver's admission check the same
// promise. MaxTopic has no wire counterpart because the envelope carries the
// topic outside the encoded body; 512 is a Redis key that fits comfortably in
// anything that reads a key list.
var DefaultLimits = Limits{
	MaxKey:         defaultWireLimits.MaxKey,
	MaxBody:        defaultWireLimits.MaxBody,
	MaxID:          defaultWireLimits.MaxID,
	MaxTopic:       512,
	MaxHeaders:     defaultWireLimits.MaxHeaders,
	MaxHeaderKey:   defaultWireLimits.MaxHeaderKey,
	MaxHeaderValue: defaultWireLimits.MaxHeaderValue,
	MaxProp:        defaultWireLimits.MaxProp,
}

// resolve fills in the defaults for a zero or partial limit set. A
// non-positive bound means "use the default": zero is what a caller who set
// nothing leaves behind, and reading it as "nothing is allowed" would refuse
// every message.
func (l Limits) resolve() Limits {
	d := DefaultLimits
	if l.MaxKey <= 0 {
		l.MaxKey = d.MaxKey
	}
	if l.MaxBody <= 0 {
		l.MaxBody = d.MaxBody
	}
	if l.MaxID <= 0 {
		l.MaxID = d.MaxID
	}
	if l.MaxTopic <= 0 {
		l.MaxTopic = d.MaxTopic
	}
	if l.MaxHeaders <= 0 {
		l.MaxHeaders = d.MaxHeaders
	}
	if l.MaxHeaderKey <= 0 {
		l.MaxHeaderKey = d.MaxHeaderKey
	}
	if l.MaxHeaderValue <= 0 {
		l.MaxHeaderValue = d.MaxHeaderValue
	}
	if l.MaxProp <= 0 {
		l.MaxProp = d.MaxProp
	}
	return l
}

// Check refuses a message the layout would accept and the rest of the driver
// could not decode.
//
// It is called before any I/O, in every metadata mode, and its refusal is
// [ErrUnrepresentable], which a driver maps to MessageTooLarge or
// InvalidArgument. See [Limits] for why the bound exists at all.
func (l Limits) Check(m *mq.Message) error {
	if m == nil {
		return ErrNilMessage
	}
	l = l.resolve()
	switch {
	case len(m.Key) > l.MaxKey,
		len(m.Body) > l.MaxBody,
		len(m.ID) > l.MaxID,
		len(m.Topic) > l.MaxTopic:
		return ErrUnrepresentable
	}
	if m.Headers.Len() > l.MaxHeaders {
		return ErrUnrepresentable
	}
	if !utf8.ValidString(m.ID) {
		return ErrNonUTF8Field
	}
	for k, v := range m.Headers.All() {
		if len(k) > l.MaxHeaderKey || len(v) > l.MaxHeaderValue {
			return ErrUnrepresentable
		}
		if !utf8.ValidString(k) {
			return ErrNonUTF8Field
		}
	}
	for _, prop := range [...]string{
		m.Properties.ContentType,
		m.Properties.ContentEncoding,
		m.Properties.CorrelationID,
		m.Properties.CausationID,
		m.Properties.ReplyTo,
	} {
		if len(prop) > l.MaxProp {
			return ErrUnrepresentable
		}
		if !utf8.ValidString(prop) {
			return ErrNonUTF8Field
		}
	}
	return nil
}

// Encode appends the entry's field-value pairs for m to dst and returns it.
//
// The pairs are appended in a fixed order — body, key, topic, headers,
// properties — so that two encodings of one message are byte-identical, which
// is what makes an entry comparable in a test and a differential fuzz target
// able to say "these are the same entry".
//
// Values are carried as strings holding arbitrary bytes, because a Redis
// command is a list of bulk strings. The []byte→string conversion is where the
// copy happens, it happens once per field, and it is what makes it safe for
// the caller to reuse m's Key and Body the instant the command has been built
// — however long the client library then holds the command.
//
// [EncodeAliased] is the same layout with the body's copy left out, for the
// clients that can be trusted with the caller's bytes. Use this one unless the
// commander said otherwise: a copy is never wrong, and aliasing is wrong on
// any client that reads a token after the call returns.
func Encode(dst []string, m *mq.Message) []string {
	out, _ := encode(dst, nil, m, false)
	return out
}

// EncodeAliased is [Encode] with m's body carried as a [Blob] instead of
// copied into a string.
//
// It appends the blob to blobs and returns both slices; the blob's At is an
// index into the RETURNED field slice, so a command builder that puts the
// fields behind a prefix shifts it (see [XAdd]). At most one blob is produced
// — the body's — and none at all for a message with no body.
//
// The two encodings are byte-identical: [Cmd.Materialize] on a command built
// from these fields gives exactly what [Encode] would have produced, which is
// what `TestEncodeAliasedMatchesEncode` holds them to and what [FuzzRedisFields]
// compares on every generated message.
//
// Only the BODY is aliased, and the key is not, though the same mechanism
// would carry it. [DefaultLimits] bounds a key at 64 KiB and a body at 64 MiB,
// a factor of a thousand, and the key is hashed on every publication to a
// partitioned topic — so the key is neither what grows with payload nor what
// the stored campaign measured growing. One aliased token per command also
// keeps the blob set to the single entry [Cmd.blobAt]'s linear scan is written
// for.
//
// The caller must not hand the result to a commander that did not answer true
// to [ArgAliaser.AliasArgs], and must keep m.Body unmodified until that
// commander's Do or DoMulti has returned.
func EncodeAliased(dst []string, blobs []Blob, m *mq.Message) ([]string, []Blob) {
	return encode(dst, blobs, m, true)
}

// encode is the one implementation of the entry layout; alias decides whether
// the body is a blob or a copy.
func encode(dst []string, blobs []Blob, m *mq.Message, alias bool) ([]string, []Blob) {
	if m == nil {
		return dst, blobs
	}
	start := len(dst)
	if m.Body != nil {
		if alias {
			dst = append(dst, FieldBody, "")
			blobs = append(blobs, Blob{At: len(dst) - 1, Bytes: m.Body})
		} else {
			dst = append(dst, FieldBody, string(m.Body))
		}
	}
	if m.Key != nil {
		dst = append(dst, FieldKey, string(m.Key))
	}
	if m.Topic != "" {
		dst = append(dst, FieldTopic, m.Topic)
	}
	n := 0
	for k, v := range m.Headers.All() {
		dst = append(dst, headerName(n), packHeader(k, v))
		n++
	}
	if m.ID != "" {
		dst = append(dst, PropID.String(), m.ID)
	}
	p := &m.Properties
	for _, pair := range [...]struct {
		prop Prop
		val  string
	}{
		{PropContentType, p.ContentType},
		{PropContentEncoding, p.ContentEncoding},
		{PropCorrelationID, p.CorrelationID},
		{PropCausationID, p.CausationID},
		{PropReplyTo, p.ReplyTo},
	} {
		if pair.val != "" {
			dst = append(dst, pair.prop.String(), pair.val)
		}
	}
	if !p.CreatedAt.IsZero() {
		dst = append(dst, PropCreatedAt.String(), formatTime(p.CreatedAt))
	}
	if len(dst) == start {
		// A message with nothing in it at all. XADD refuses an entry with no
		// fields, so the marker is what makes an empty message publishable;
		// Decode ignores it like every other "x:" field, so the message
		// still decodes to the empty message that was published.
		dst = append(dst, FieldEmpty, "")
	}
	return dst, blobs
}

// packHeader frames one header as a single field value.
//
// A header is a key and a value and the value is arbitrary bytes, so the two
// cannot be joined by a separator: every byte, the separator included, is a
// byte a value may contain. The framing is therefore a length: a base-10
// decimal key length, a ':', the key, then the value. Decoding reads the
// length and takes exactly that many bytes, so a key containing ':' or a
// value containing anything at all comes back unchanged.
//
// The alternative — two fields per header, "hk:<n>" and "hv:<n>" — doubles
// the field count and makes a half-written pair representable, which is a
// corrupt entry the layout could produce itself.
func packHeader(key string, val []byte) string {
	var b strings.Builder
	b.Grow(len(key) + len(val) + 4)
	b.WriteString(strconv.Itoa(len(key)))
	b.WriteByte(':')
	b.WriteString(key)
	b.Write(val)
	return b.String()
}

// unpackHeader reverses packHeader.
func unpackHeader(s string) (key string, val []byte, err error) {
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return "", nil, ErrCorruptEntry
	}
	n, cerr := strconv.Atoi(s[:colon])
	if cerr != nil || n < 0 || colon+1+n > len(s) {
		return "", nil, ErrCorruptEntry
	}
	body := s[colon+1:]
	// The value is copied out rather than aliased. A reply's strings are the
	// caller's to keep, but a header value that aliased the packed field
	// would keep the whole field alive for as long as the header, and a
	// received message's headers outlive the reply buffer they came from
	// under every ownership mode but Borrowed.
	return body[:n], []byte(body[n:]), nil
}

// formatTime renders a time as "<seconds>,<nanoseconds>" since the epoch.
func formatTime(t time.Time) string {
	return strconv.FormatInt(t.Unix(), 10) + "," + strconv.Itoa(t.Nanosecond())
}

// parseTime reverses formatTime. The result is in UTC, which is the one thing
// the round trip cannot preserve: an entry carries no location, so a
// CreatedAt set in a named zone comes back as the same instant in UTC.
func parseTime(s string) (time.Time, error) {
	comma := strings.IndexByte(s, ',')
	if comma < 0 {
		return time.Time{}, ErrCorruptEntry
	}
	sec, err := strconv.ParseInt(s[:comma], 10, 64)
	if err != nil {
		return time.Time{}, ErrCorruptEntry
	}
	nsec, err := strconv.Atoi(s[comma+1:])
	if err != nil || nsec < 0 || nsec >= int(time.Second) {
		return time.Time{}, ErrCorruptEntry
	}
	return time.Unix(sec, int64(nsec)).UTC(), nil
}

// Decode fills m from an entry's field-value pairs.
//
// fields is the flat list Redis returns: name, value, name, value. An odd
// length is a corrupt entry rather than a truncated one — there is no
// half-field a driver could usefully deliver.
//
// It is strict where a mistake would lose message content and lenient where
// it would not:
//
//   - A repeated "b", "k", "t" or "p:<n>" is [ErrCorruptEntry]. Silently
//     taking the last one would make the decode depend on field order, which
//     a Redis entry does not promise.
//   - "h:<n>" indices must run 0..count-1 with no gap and no repeat. A gap
//     means a header was lost between the encoder and here, and a decoder
//     that skipped it would deliver a message with fewer headers than was
//     published and no error anywhere.
//   - A field name this layout has never seen is IGNORED. That is what lets
//     a future driver add a field and this one still read the entry, and
//     what lets a strategy carry its own bookkeeping under "x:" without
//     every reader having to know about it.
//
// m is reset first, so a pooled message carries nothing from the entry before
// it.
func Decode(m *mq.Message, fields []string) error {
	if m == nil {
		return ErrNilMessage
	}
	if len(fields)%2 != 0 {
		return ErrCorruptEntry
	}
	m.Reset()
	var (
		haveBody, haveKey, haveTopic bool
		propSeen                     [propCount]bool
		headers                      []packedHeader
	)
	for i := 0; i < len(fields); i += 2 {
		name, val := fields[i], fields[i+1]
		switch {
		case name == FieldBody:
			if haveBody {
				return ErrCorruptEntry
			}
			haveBody = true
			m.Body = []byte(val)
		case name == FieldKey:
			if haveKey {
				return ErrCorruptEntry
			}
			haveKey = true
			m.Key = []byte(val)
		case name == FieldTopic:
			if haveTopic {
				return ErrCorruptEntry
			}
			haveTopic = true
			m.Topic = val
		case strings.HasPrefix(name, FieldPrefixHeader):
			idx, err := fieldIndex(name[len(FieldPrefixHeader):])
			if err != nil {
				return err
			}
			headers = append(headers, packedHeader{idx: idx, packed: val})
		case strings.HasPrefix(name, FieldPrefixProp):
			idx, err := fieldIndex(name[len(FieldPrefixProp):])
			if err != nil {
				return err
			}
			if idx < 0 || idx >= int(propCount) {
				// A property index this driver does not know is a field a
				// newer driver wrote, not a corruption: ignoring it is the
				// same rule as for an unknown name.
				continue
			}
			prop := Prop(idx)
			if propSeen[prop] {
				return ErrCorruptEntry
			}
			propSeen[prop] = true
			if err := setProp(m, prop, val); err != nil {
				return err
			}
		default:
			// Unknown, including every "x:" bookkeeping field.
		}
	}
	return loadHeaders(m, headers)
}

// packedHeader is one "h:<n>" field, before the indices have been checked.
type packedHeader struct {
	packed string
	idx    int
}

// loadHeaders checks the header indices and adds the headers in order.
func loadHeaders(m *mq.Message, hs []packedHeader) error {
	if len(hs) == 0 {
		return nil
	}
	// The indices must be a permutation of 0..len-1. Checking it by marking
	// a slot rather than by sorting keeps the decode free of a comparison
	// function and catches a repeat and a gap with the same test.
	order := make([]string, len(hs))
	for _, h := range hs {
		if h.idx >= len(hs) || order[h.idx] != "" {
			return ErrCorruptEntry
		}
		order[h.idx] = h.packed
	}
	for _, packed := range order {
		if packed == "" {
			// An empty packed header is impossible: packHeader always
			// writes at least a length and a colon. So an empty slot here
			// is a gap in the indices.
			return ErrCorruptEntry
		}
		key, val, err := unpackHeader(packed)
		if err != nil {
			return err
		}
		m.Headers.Add(key, val)
	}
	return nil
}

// fieldIndex parses the decimal suffix of an "h:<n>" or "p:<n>" name.
//
// It refuses a leading '+', a leading zero on a multi-digit number and a
// negative value, because each of those is a second spelling of an index that
// already has one: two spellings of the same index would let an entry carry
// two "header 1" fields that the repeat check could not see.
func fieldIndex(s string) (int, error) {
	if s == "" || len(s) > 9 {
		return 0, ErrCorruptEntry
	}
	if len(s) > 1 && s[0] == '0' {
		return 0, ErrCorruptEntry
	}
	n := 0
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, ErrCorruptEntry
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// setProp writes one typed property.
func setProp(m *mq.Message, p Prop, val string) error {
	switch p {
	case PropID:
		m.ID = val
	case PropContentType:
		m.Properties.ContentType = val
	case PropContentEncoding:
		m.Properties.ContentEncoding = val
	case PropCorrelationID:
		m.Properties.CorrelationID = val
	case PropCausationID:
		m.Properties.CausationID = val
	case PropReplyTo:
		m.Properties.ReplyTo = val
	case PropCreatedAt:
		t, err := parseTime(val)
		if err != nil {
			return err
		}
		m.Properties.CreatedAt = t
	default:
		return ErrCorruptEntry
	}
	return nil
}

// Internal returns the value of a bookkeeping field, and false when the entry
// does not carry it.
//
// The bookkeeping fields are not message content, so [Decode] does not
// deliver them; a strategy that wrote one reads it back with this.
func Internal(fields []string, name string) (string, bool) {
	if !strings.HasPrefix(name, FieldPrefixInternal) {
		return "", false
	}
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == name {
			return fields[i+1], true
		}
	}
	return "", false
}

// Attempt returns the attempt count an entry carries in [FieldAttempt], and
// zero when it carries none.
//
// Zero is the API's "the broker does not count attempts", and it
// is the honest answer for an entry that was published rather than re-added:
// the count then comes from the pending-entry's own delivery counter.
func Attempt(fields []string) uint32 {
	s, ok := Internal(fields, FieldAttempt)
	if !ok {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

// DeliverAt returns the time an entry carries in [FieldDeliverAt], and the
// zero time when it carries none.
func DeliverAt(fields []string) time.Time {
	s, ok := Internal(fields, FieldDeliverAt)
	if !ok {
		return time.Time{}
	}
	t, err := parseTime(s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// The field layout's failures.
var (
	// ErrNilMessage is a nil message where one was required.
	ErrNilMessage = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: the message is nil")
	// ErrUnrepresentable is a message the layout's limits refuse.
	//
	// It is refused BEFORE the publication, which is the whole point: a
	// message accepted here and undecodable later is an unsettleable record
	// the driver made itself out of a publication it said yes to.
	ErrUnrepresentable = errcode.New(errcode.MessageTooLarge,
		"iqmq/streams: the message exceeds the entry layout's limits; it would be accepted at publish and then be undecodable, which is a record nothing can settle")
	// ErrNonUTF8Field is a message whose identifier, header key or string
	// property is not valid UTF-8.
	//
	// The field layout itself would carry it: a stream entry's fields are
	// binary-safe. The refusal is the portable envelope's requirement, and
	// finding it cost [FuzzRedisFields] a tenth of a second — which makes it
	// the THIRD skin of the finding, after the header-key length and the
	// identifier length. The envelope's decoder validates UTF-8 in the
	// identifier, every header key and each string property
	// (internal/wire's validString), its encoder validates nothing, and
	// nothing in driverkit answers "would Unwrap accept what Wrap is about
	// to produce?" — so the driver has to ask the question itself, field by
	// field, from the same list.
	//
	// Header VALUES and the key and body are deliberately not checked: the
	// envelope carries those as bytes and does not validate them, so
	// refusing them here would refuse a message the driver can carry.
	ErrNonUTF8Field = errcode.New(errcode.InvalidArgument,
		"iqmq/streams: the message identifier, a header key or a string property is not valid UTF-8; the portable envelope's decoder refuses it, so the publication would be accepted and then be undecodable")
	// ErrCorruptEntry is an entry the layout cannot decode.
	ErrCorruptEntry = errcode.New(errcode.CorruptMessage,
		"iqmq/streams: the stream entry's fields are not a message this layout wrote")
)

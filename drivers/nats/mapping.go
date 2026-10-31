package nats

import (
	"encoding/base64"
	"strings"
	"time"
	"unicode/utf8"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The bounds this package maps within. They are explicit because the specification asks
// every parser and every mapping fed by external bytes to have a stated limit
// rather than an implicit one: a subject is built from a topic name a caller
// chose, and a header comes off the wire.
const (
	// MaxSubjectLen is the longest subject this driver will build or accept.
	MaxSubjectLen = 1024
	// MaxSubjectTokens is the most dot-separated tokens a subject may carry.
	MaxSubjectTokens = 64
	// MaxQueueLen is the longest queue group name.
	MaxQueueLen = 255
	// MaxHeaderKeyLen and MaxHeaderValueLen bound one header.
	MaxHeaderKeyLen   = 256
	MaxHeaderValueLen = 8 << 10
)

// The reserved headers this driver carries iqmq metadata in.
//
// NATS Core has exactly two native fields beyond the payload — the subject and
// the reply subject — so everything else the specification names travels as a header under
// the reserved prefix, which [github.com/iqhive/iqmq.Headers.Validate] keeps a
// caller out of. The names are lower case and stable: they are on the wire, so
// a non-iqmq consumer reads them too.
const (
	// HeaderKey carries [github.com/iqhive/iqmq.Message.Key], base64 encoded
	// because a key is bytes and a NATS header value is a text line.
	HeaderKey = mq.ReservedHeaderPrefix + "key"
	// HeaderID carries Message.ID.
	HeaderID = mq.ReservedHeaderPrefix + "id"
	// HeaderEmptyBody marks a body that was empty rather than absent.
	//
	// nats.go builds every payload with make([]byte, n), so a zero-length
	// payload always arrives as an empty non-nil slice and the distinction
	// The specification makes observable would be lost in one direction. Marking the
	// rarer half keeps both: no marker and no payload is a nil body, which
	// is also what a message from a non-iqmq publisher means.
	HeaderEmptyBody = mq.ReservedHeaderPrefix + "empty-body"
	// The typed properties that have no native NATS field.
	HeaderContentType     = mq.ReservedHeaderPrefix + "content-type"
	HeaderContentEncoding = mq.ReservedHeaderPrefix + "content-encoding"
	HeaderCorrelationID   = mq.ReservedHeaderPrefix + "correlation-id"
	HeaderCausationID     = mq.ReservedHeaderPrefix + "causation-id"
	HeaderCreatedAt       = mq.ReservedHeaderPrefix + "created-at"
	// HeaderEnvelope marks a message whose PAYLOAD is the portable envelope
	// rather than the caller's body, and it is the reason this driver never
	// looks at the payload to decide.
	//
	// NATS has real headers, so the answer to "did iqmq envelope this?" can
	// be recorded where a payload cannot reach it. Under MetadataAdaptive an
	// enveloped message and a native one share a subscription, and the
	// alternative — reading the payload's first bytes — decides provenance
	// from evidence the producer of the PAYLOAD controls: a native
	// publication whose body happens to be a valid envelope was decoded as
	// metadata, which replaced the caller's payload with the envelope's own
	// key and headers.
	//
	// Only its presence is read. Its value is driverkit's marker value,
	// written for an operator reading a message by hand, and the name is
	// driverkit.EnvelopeMarker — the one every driver with somewhere to put
	// it spells, because a marker one driver did not recognise would be an
	// envelope delivered as a payload with nothing reporting it.
	HeaderEnvelope = mq.ReservedHeaderPrefix + "env"
)

// EnvelopeMarkerValue is the value [HeaderEnvelope] carries.
const EnvelopeMarkerValue = "1"

// Subject maps a portable topic onto the NATS subject a publication goes to.
//
// The rules are the driver page's: "/" is the portable hierarchy separator and
// "." is the NATS one, so one becomes the other; [Config.SubjectPrefix] is
// prepended as a token; and a wildcard is refused, because publishing to a
// pattern is not a thing NATS does and a topic that quietly became one would
// publish to every subject under it.
//
// It is pure, bounded and total: no I/O, no allocation beyond the result, and
// no input makes it panic. FuzzNATSSubjectMapping is written against exactly
// those properties.
func Subject(prefix, topic string) (string, error) { return mapSubject(prefix, topic, false) }

// Pattern is [Subject] for a subscription, where the NATS wildcards are the
// point: "*" matches one token and a trailing ">" matches the rest.
//
// A consumer's Topic is the only place a pattern can be written, because
// ConsumerConfig has no filter field  and SourceConfig has no
// pattern beyond Topic. That is what FeatureWildcardSubscribe means for this
// driver, and it is.
func Pattern(prefix, topic string) (string, error) { return mapSubject(prefix, topic, true) }

// mapSubject is the shared half of Subject and Pattern.
func mapSubject(prefix, topic string, wildcards bool) (string, error) {
	if topic == "" {
		return "", errEmptyTopic
	}
	if len(prefix)+len(topic)+1 > MaxSubjectLen {
		return "", errSubjectTooLong
	}
	sub := topic
	if strings.IndexByte(sub, '/') >= 0 {
		sub = strings.ReplaceAll(sub, "/", ".")
	}
	if prefix != "" {
		sub = prefix + "." + sub
	}
	if err := CheckSubject(sub, wildcards); err != nil {
		return "", err
	}
	return sub, nil
}

// CheckSubject reports whether s is a subject this driver will put on the wire,
// with wildcards permitted only where a subscription may write one.
//
// It is stricter than the server: the token alphabet is letters, digits, "_",
// "-" and "$", which is the image of the portable topic grammar under the
// separator swap plus the "$" that NATS system subjects use. Anything else — a
// space, a control byte, a quote, a byte above ASCII — is refused here rather
// than being discovered as a protocol error on a shared connection, where it
// would take down every other publisher and subscriber on it.
func CheckSubject(s string, wildcards bool) error {
	if s == "" || len(s) > MaxSubjectLen {
		return errInvalidSubject
	}
	tokens, start := 0, 0
	for i := 0; i <= len(s); i++ {
		if i < len(s) && s[i] != '.' {
			continue
		}
		tok := s[start:i]
		start = i + 1
		tokens++
		if tokens > MaxSubjectTokens {
			return errInvalidSubject
		}
		switch tok {
		case "":
			return errInvalidSubject
		case "*":
			if !wildcards {
				return errInvalidSubject
			}
		case ">":
			// ">" matches the rest of the subject, so it is only ever the
			// last token; one in the middle is a pattern that matches
			// nothing and reads as though it matched everything.
			if !wildcards || i != len(s) {
				return errInvalidSubject
			}
		default:
			for j := range len(tok) {
				if !subjectByte(tok[j]) {
					return errInvalidSubject
				}
			}
		}
	}
	return nil
}

// subjectByte reports whether c may appear inside a subject token.
func subjectByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '_', c == '-', c == '$':
		return true
	default:
		return false
	}
}

// TopicOf is the reverse of [Subject]: the topic a delivery reports.
//
// It strips [Config.SubjectPrefix] and returns what is left unchanged. It does
// NOT turn "." back into "/", because it cannot: the forward mapping is not
// injective, so "a/b" and "a.b" are the same subject and no reverse can tell
// which one was published. The specification asks for the resolved topic of a delivery and
// this is it — the subject the message really arrived on.
func TopicOf(prefix, subject string) string {
	if prefix == "" {
		return subject
	}
	if rest, ok := strings.CutPrefix(subject, prefix+"."); ok {
		return rest
	}
	return subject
}

// CheckQueue reports whether name may be used as a NATS queue group.
//
// A subscription name becomes a queue group, so the same alphabet applies, with
// "." permitted (a queue group is one name, not a subject) and wildcards not:
// the server treats a wildcard queue group as a literal name, which would make
// two consumers that thought they were competing silently not.
func CheckQueue(name string) error {
	if name == "" || len(name) > MaxQueueLen {
		return errInvalidQueue
	}
	for i := range len(name) {
		if c := name[i]; !subjectByte(c) && c != '.' {
			return errInvalidQueue
		}
	}
	return nil
}

// HeaderKeyOK reports whether a header name survives the NATS header framing.
//
// Headers are an HTTP-like block, "Name: value" per CRLF-terminated line, so a
// name carrying a colon is truncated at it, one carrying CR or LF forges a
// second header, and one carrying a leading space is read as something else
// entirely. The check is therefore on the framing and not on taste.
func HeaderKeyOK(k string) bool {
	if k == "" || len(k) > MaxHeaderKeyLen {
		return false
	}
	for i := range len(k) {
		if c := k[i]; c <= ' ' || c >= 0x7f || c == ':' {
			return false
		}
	}
	return true
}

// HeaderValueOK reports whether a header value survives the framing and comes
// back byte for byte.
//
// Three rules, each of which a round trip really breaks: a value must be valid
// UTF-8 (the wire is a text line and the reader is textproto's, so an arbitrary
// byte string is not carried — which is why this driver does not advertise
// FeatureBinaryHeaders); it must carry no control byte, CR and LF above all;
// and it must not begin with a space or a tab, because the reader eats those
// after the colon and the value would come back shorter than it went out.
func HeaderValueOK(v []byte) bool {
	if len(v) > MaxHeaderValueLen {
		return false
	}
	if !utf8.Valid(v) {
		return false
	}
	for i := range v {
		if c := v[i]; c < 0x20 || c == 0x7f {
			return false
		}
	}
	return len(v) == 0 || (v[0] != ' ' && v[0] != '\t')
}

// Representable reports whether every piece of m's metadata has a native NATS
// field or a reserved header that carries it losslessly.
//
// It is what MetadataNativeStrict refuses on and what MetadataAdaptive wraps
// on, and it never touches the network: the specification requires the refusal to happen
// before any I/O.
//
// the five dead-letter provenance headers are admitted, because they are
// iqmq's own and not a caller's: the core's dead-letter route publishes a
// stamped copy of the poison message through this very path, so a driver that
// refused them made mq.WithDeadLetter unusable — the publication failed before
// any I/O, the runner requeued and counted a DeadLetterFailures, and the
// message was redelivered for ever.
// [driverkit.ReservedHeaderAllowed] names the five and nothing else, so this
// driver's own reserved carriers stay refused from a caller.
//
// They are still held to the header-value rule below: a NATS header block is
// HTTP-shaped, so a value with a control character in it cannot be framed
// whoever wrote it. That is why the core renders the failure reason framable
// before it stamps it.
func Representable(m *mq.Message) error {
	if m == nil {
		return errNilMessage
	}
	for k, v := range m.Headers.All() {
		if strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k) {
			return errReservedHeader
		}
		if !HeaderKeyOK(k) {
			return errHeaderKey
		}
		if !HeaderValueOK(v) {
			return errHeaderValue
		}
	}
	if m.Properties.ReplyTo != "" {
		if _, err := Subject("", m.Properties.ReplyTo); err != nil {
			return errReplyTo
		}
	}
	for _, s := range [...]string{
		m.ID, m.Properties.ContentType, m.Properties.ContentEncoding,
		m.Properties.CorrelationID, m.Properties.CausationID,
	} {
		if s != "" && !HeaderValueOK([]byte(s)) {
			return errHeaderValue
		}
	}
	return nil
}

// Encode fills msg from m: the payload, the native reply subject and the
// headers, reserved ones included.
//
// The reply subject is the reply-to topic with no [Config.SubjectPrefix],
// because Encode is not given one. A publisher of this driver rewrites it to
// the prefixed subject a consumer of that topic listens on, which is what
// [Decode] strips again.
//
// drop is MetadataLenientDrop: a header that cannot be carried is left out
// instead of refusing the publication, and dropped is called with its name so
// that the specification's "with a hook and a metric" has somewhere to land. Under every
// other mode the caller has already run [Representable], so nothing is dropped.
//
// It never retains anything of m: header names and values are copied into the
// nats.Header as strings, which is what the specification requires of a sink that returns
// before the bytes are on the wire.
func Encode(msg *natsgo.Msg, m *mq.Message, drop func(string)) {
	msg.Data = m.Body
	if m.Properties.ReplyTo != "" {
		// The one iqmq property with a native NATS field. Mapping it here
		// is what makes request and reply work through the portable API: a
		// handler reads Delivery.Properties.ReplyTo and publishes to it.
		if sub, err := Subject("", m.Properties.ReplyTo); err == nil {
			msg.Reply = sub
		} else if drop != nil {
			drop("reply-to")
		}
	}
	n := m.Headers.Len() + 8
	h := msg.Header
	if h == nil {
		h = make(natsgo.Header, n)
		msg.Header = h
	}
	for k, v := range m.Headers.All() {
		// A reserved key is the driver's own namespace and a caller's is
		// dropped rather than published — except the five provenance
		// keys, which are the CORE's own and have to reach the dead-letter
		// topic or the route carries no provenance at all. The value rule applies to them like any other.
		reserved := strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k)
		if reserved || !HeaderKeyOK(k) || !HeaderValueOK(v) {
			if drop != nil {
				drop(k)
			}
			continue
		}
		h.Add(k, string(v))
	}
	if m.Key != nil {
		h.Set(HeaderKey, base64.StdEncoding.EncodeToString(m.Key))
	}
	if m.Body != nil && len(m.Body) == 0 {
		h.Set(HeaderEmptyBody, "1")
	}
	setIf(h, HeaderID, m.ID, drop)
	setIf(h, HeaderContentType, m.Properties.ContentType, drop)
	setIf(h, HeaderContentEncoding, m.Properties.ContentEncoding, drop)
	setIf(h, HeaderCorrelationID, m.Properties.CorrelationID, drop)
	setIf(h, HeaderCausationID, m.Properties.CausationID, drop)
	if !m.Properties.CreatedAt.IsZero() {
		h.Set(HeaderCreatedAt, m.Properties.CreatedAt.Format(time.RFC3339Nano))
	}
	if len(h) == 0 {
		// A message with nothing to say carries no header block at all, so
		// a non-iqmq subscriber sees the plain MSG it would have seen from
		// a plain NATS publisher.
		msg.Header = nil
	}
}

// setIf writes one optional reserved header, dropping a value that cannot be
// framed rather than corrupting the block.
func setIf(h natsgo.Header, key, val string, drop func(string)) {
	if val == "" {
		return
	}
	if !HeaderValueOK([]byte(val)) {
		if drop != nil {
			drop(key)
		}
		return
	}
	h.Set(key, val)
}

// Decode fills m from msg: the payload, the reply subject and every header,
// with the reserved ones read back into the fields they came from.
//
// The header names are sorted, which is not what the specification promises but is the best
// a nats.Header can do: it is a map, so the order the publisher wrote is gone
// by the time nats.go hands the message over. Sorting makes what is left
// deterministic, which matters more than it sounds — an unsorted map iteration
// would give the same message a different header order on every delivery.
func Decode(m *mq.Message, msg *natsgo.Msg, prefix string) {
	m.Topic = TopicOf(prefix, msg.Subject)
	m.Body = msg.Data
	if msg.Reply != "" {
		m.Properties.ReplyTo = TopicOf(prefix, msg.Reply)
	}
	if len(msg.Header) == 0 {
		if len(m.Body) == 0 {
			m.Body = nil
		}
		return
	}
	empty := false
	keys := make([]string, 0, len(msg.Header))
	for k := range msg.Header {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for _, k := range keys {
		vals := msg.Header[k]
		switch k {
		case HeaderKey:
			if len(vals) > 0 {
				if raw, err := base64.StdEncoding.DecodeString(vals[0]); err == nil {
					m.Key = raw
				}
			}
		case HeaderEmptyBody:
			empty = true
		case HeaderID:
			m.ID = first(vals)
		case HeaderContentType:
			m.Properties.ContentType = first(vals)
		case HeaderContentEncoding:
			m.Properties.ContentEncoding = first(vals)
		case HeaderCorrelationID:
			m.Properties.CorrelationID = first(vals)
		case HeaderCausationID:
			m.Properties.CausationID = first(vals)
		case HeaderCreatedAt:
			if t, err := time.Parse(time.RFC3339Nano, first(vals)); err == nil {
				m.Properties.CreatedAt = t
			}
		case HeaderEnvelope:
			// Framing, not metadata: [Enveloped] is what reads it, and a
			// caller must not be handed iqmq's own discriminator as though
			// it had published it.
		default:
			for _, v := range vals {
				m.Headers.Add(k, []byte(v))
			}
		}
	}
	if len(m.Body) == 0 && !empty {
		m.Body = nil
	}
}

// first returns the first value of a header, or the empty string.
func first(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// sortStrings is an insertion sort over the header names of one message.
//
// It is here rather than sort.Strings because a header set is a handful of
// entries and an insertion sort over one beats a call that allocates an
// interface value for it; the mapping path is held to an allocation budget by
// TestAllocsMapping.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		v := s[i]
		j := i - 1
		for j >= 0 && s[j] > v {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = v
	}
}

// The mapping's refusals. The specification gives no sentinel for "this broker cannot carry
// that", so each carries the portable code a caller compares with mq.CodeOf,
// and the ones that mean "the broker has no way to do this" are ErrUnsupported
// so that errors.Is reaches the answer.
var (
	errEmptyTopic     = errcode.New(errcode.InvalidArgument, "iqmq/nats: the topic is empty")
	errSubjectTooLong = errcode.New(errcode.InvalidArgument, "iqmq/nats: the topic does not fit a NATS subject")
	errInvalidSubject = errcode.New(errcode.InvalidArgument, "iqmq/nats: the topic is not a NATS subject this driver will publish or subscribe to")
	errInvalidQueue   = errcode.New(errcode.InvalidArgument, "iqmq/nats: the subscription is not a name this driver will use as a queue group")
	errNilMessage     = errcode.New(errcode.InvalidArgument, "iqmq/nats: the message is nil")
	errReservedHeader = errcode.New(errcode.InvalidArgument, "iqmq/nats: a header key uses the reserved "+mq.ReservedHeaderPrefix+" prefix")
)

// The metadata refusals each is ErrUnsupported, because what they
// report is a broker that has no field for the metadata rather than a caller
// who wrote something malformed.
var (
	errHeaderKey   = errcode.New(errcode.Unsupported, "iqmq/nats: NATS headers cannot carry that header name (see docs/drivers/nats.md)")
	errHeaderValue = errcode.New(errcode.Unsupported, "iqmq/nats: NATS headers are text lines and cannot carry that value; use MetadataPortableEnvelope or MetadataAdaptive")
	errReplyTo     = errcode.New(errcode.Unsupported, "iqmq/nats: Properties.ReplyTo is not a subject this driver can address")
)

// Enveloped reports whether hdr carries [HeaderEnvelope], which is the only
// thing that makes the payload the portable envelope rather than a body.
//
// It is the whole of MetadataAdaptive's read-side decision on this driver. A
// message with no marker is native — whatever its bytes look like — because
// this driver writes the marker on every message it envelopes and the specification keeps a
// caller's own headers out of the reserved prefix. That is the "never guess
// from the body" rule [github.com/iqhive/iqmq/wire.Detector.DetectFull] is for a broker with nowhere to
// keep the answer, and it is strictly weaker.
func Enveloped(hdr natsgo.Header) bool {
	_, ok := hdr[HeaderEnvelope]
	return ok
}

// MarkEnveloped records on msg that its payload is the portable envelope,
// creating the header block when the envelope had nothing else to say.
//
// It is written after [Encode] rather than inside it because Encode is given
// the message that is going on the wire and cannot tell an envelope from a
// payload that happens to look like one — which is the defect, one layer down.
func MarkEnveloped(msg *natsgo.Msg) {
	if msg.Header == nil {
		msg.Header = make(natsgo.Header, 1)
	}
	msg.Header.Set(HeaderEnvelope, EnvelopeMarkerValue)
}

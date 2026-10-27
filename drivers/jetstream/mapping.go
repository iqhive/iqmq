package jetstream

import (
	"encoding/base64"
	"hash/fnv"
	"strconv"
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
	// MaxNameLen is the longest stream or consumer name.
	MaxNameLen = 255
	// MaxHeaderKeyLen and MaxHeaderValueLen bound one header.
	MaxHeaderKeyLen   = 256
	MaxHeaderValueLen = 8 << 10
)

// NATSHeaderPrefix is the header namespace the server owns.
//
// JetStream's own control headers all live under it — Nats-Msg-Id, Nats-TTL,
// Nats-Schedule, Nats-Stream, Nats-Sequence, Nats-Scheduler — and the server
// interprets them on the way in and writes them on the way out. A caller header
// in this namespace would therefore change the broker's behaviour rather than
// travel with the message, so this driver refuses one under
// MetadataNativeStrict, drops it under MetadataLenientDrop and never decodes
// one back as a caller header.
const NATSHeaderPrefix = "nats-"

// The reserved headers this driver carries iqmq metadata in.
//
// JetStream has the subject, the reply subject and the server's own Nats-*
// namespace, so everything else the specification names travels as a header under the
// reserved iqmq- prefix, which [github.com/iqhive/iqmq.Headers.Validate] keeps a
// caller out of. The names are lower case, stable and identical to the Core
// driver's, so a message published by one profile is readable by the other.
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
	// The typed properties that have no native JetStream field.
	HeaderContentType     = mq.ReservedHeaderPrefix + "content-type"
	HeaderContentEncoding = mq.ReservedHeaderPrefix + "content-encoding"
	HeaderCorrelationID   = mq.ReservedHeaderPrefix + "correlation-id"
	HeaderCausationID     = mq.ReservedHeaderPrefix + "causation-id"
	HeaderCreatedAt       = mq.ReservedHeaderPrefix + "created-at"
	// HeaderReplyTo carries Properties.ReplyTo as the subject it maps to.
	//
	// It is a header and not the native reply subject because a stream does
	// not keep one: a synchronous JetStream publication puts its own PubAck
	// inbox there, so the stored message has none and a consumer read "", and
	// an asynchronous one refuses a message that has one set
	// (ErrAsyncPublishReplySubjectSet). A delivery's own reply subject is the
	// server's $JS.ACK address in any case, which is the settler's.
	HeaderReplyTo = mq.ReservedHeaderPrefix + "reply-to"
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
	// HeaderDeadLetterFrom is the subject a dead-lettered message was
	// rejected from. It is written by the Reject route and is what makes a
	// dead-letter destination readable without a second lookup.
	HeaderDeadLetterFrom = mq.ReservedHeaderPrefix + "dead-letter-from"
	// HeaderDeadLetterReason carries NackOptions.Reason, which the specification makes
	// local diagnostics unless the driver documents that it transports it.
	// This driver documents that it does, on the dead-letter route only.
	HeaderDeadLetterReason = mq.ReservedHeaderPrefix + "dead-letter-reason"
	// HeaderDeadLetterAttempt is the attempt count the original delivery
	// reported when it was rejected.
	HeaderDeadLetterAttempt = mq.ReservedHeaderPrefix + "dead-letter-attempt"
)

// EnvelopeMarkerValue is the value [HeaderEnvelope] carries.
const EnvelopeMarkerValue = "1"

// Subject maps a portable topic onto the JetStream subject a publication goes
// to.
//
// The rules are the driver page's: "/" is the portable hierarchy separator and
// "." is the NATS one, so one becomes the other; [Config.SubjectPrefix] is
// prepended as a token; and a wildcard is refused, because publishing to a
// pattern is not a thing NATS does and a topic that quietly became one would
// publish to every subject under it.
//
// It is pure, bounded and total: no I/O, no allocation beyond the result, and no
// input makes it panic. FuzzJetStreamSubjectMapping is written against exactly
// those properties.
func Subject(prefix, topic string) (string, error) { return mapSubject(prefix, topic, false) }

// Pattern is [Subject] for a subscription, where the NATS wildcards are the
// point: "*" matches one token and a trailing ">" matches the rest.
//
// A consumer's Topic is the only place a pattern can be written, because
// ConsumerConfig has no filter field  and SourceConfig has no
// pattern beyond Topic. That pattern becomes the durable consumer's
// FilterSubjects, which is what FeatureWildcardSubscribe means here.
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

// CheckName reports whether name may be used as a JetStream stream or consumer
// name.
//
// The server's own rule is that a name carries no whitespace, no ".", "*" or
// ">", no path separator and no non-printable character. This is that rule
// narrowed to an alphabet a name can be built from mechanically: letters,
// digits, "_" and "-". Anything else is refused before the API call, so a
// mistyped stream name is an immediate refusal rather than a round trip.
func CheckName(name string) error {
	if name == "" || len(name) > MaxNameLen {
		return errInvalidName
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_', c == '-':
		default:
			return errInvalidName
		}
	}
	return nil
}

// Durable is the durable consumer name one subscription maps to.
//
// A subscription name is the portable identity of a consumer group, and a
// JetStream durable is per (stream, filter): the same subscription name used on
// two topics is two groups and must be two durables, or the second would inherit
// the first's ack floor and skip messages. The name is therefore the prefix, the
// sanitised subscription and a short hash — readable enough to find in
// `nats consumer ls`, and collision-resistant enough that two filters never
// share one.
//
// The hash covers the filter, and the subscription too whenever sanitising or
// clamping changed it. Both are many-to-one — "billing.v1", "billing/v1" and
// "billing v1" all read "billing-v1", and two long names that share a prefix
// clamp alike — so without the original name in the hash two groups that should
// each see every message would share one durable and split the stream between
// them. A name already in the alphabet that fits is hashed over the filter
// alone, which is the rule every earlier release used: its durable keeps its
// name, and its ack floor, across an upgrade.
//
// It is pure and total: no input makes it produce a name the server refuses,
// which is what FuzzJetStreamMetadata checks.
func Durable(prefix, subscription, filter string) (string, error) {
	if subscription == "" {
		return "", errInvalidSubscription
	}
	if len(subscription) > MaxNameLen {
		return "", errInvalidSubscription
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(filter))
	suffix := "-" + strconv.FormatUint(h.Sum64(), 36)
	room := MaxNameLen - len(prefix) - len(suffix)
	if room <= 0 {
		return "", errInvalidSubscription
	}
	sub := clampName(sanitiseName(subscription), room)
	if sub != subscription {
		// The NUL keeps the (filter, subscription) pairs apart: a filter is
		// a subject, and no subject carries one.
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(subscription))
		suffix = "-" + strconv.FormatUint(h.Sum64(), 36)
		if room = MaxNameLen - len(prefix) - len(suffix); room <= 0 {
			return "", errInvalidSubscription
		}
		sub = clampName(sanitiseName(subscription), room)
	}
	name := prefix + sub + suffix
	if err := CheckName(name); err != nil {
		return "", errInvalidSubscription
	}
	return name, nil
}

// sanitiseName maps a portable name onto the durable alphabet. Every byte the
// server would refuse becomes "-", which is a mapping and not a validation: the
// hash in [Durable] is what keeps two names that sanitise alike apart.
func sanitiseName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := range len(s) {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
			b.WriteByte(c)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// clampName trims a sanitised name to n bytes, never to nothing.
func clampName(s string, n int) string {
	if s == "" {
		return "sub"
	}
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ScheduleSubject is the subject a scheduled publication's control message is
// written to.
//
// It is a sibling of the topic tree, never a child, and it carries a per-message
// token so that two scheduled publications to one topic do not overwrite each
// other's schedule. seq is the caller's own counter.
func ScheduleSubject(root string, seq uint64) string {
	return root + "." + strconv.FormatUint(seq, 36)
}

// PatternReachesSchedules reports whether a subscription pattern would also
// match the driver's schedule tree.
//
// It exists because JetStream consumers have no negative filter: a consumer
// whose FilterSubjects matched the schedule root would receive the driver's own
// control messages as though the application had published them, which is a
// message the caller cannot interpret and cannot settle meaningfully. The
// consumer is refused instead, naming [Config.ScheduleSubject].
//
// It is a subject-matching question and nothing more: "*" matches one token and
// a trailing ">" matches the rest.
func PatternReachesSchedules(pattern, root string) bool {
	if pattern == "" || root == "" {
		return false
	}
	return subjectMatches(pattern, root) || subjectMatches(pattern, root+".x")
}

// subjectMatches reports whether the NATS pattern matches the concrete subject.
func subjectMatches(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return i < len(s)
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

// HeaderKeyOK reports whether a header name survives the NATS header framing
// and belongs to the caller.
//
// Headers are an HTTP-like block, "Name: value" per CRLF-terminated line, so a
// name carrying a colon is truncated at it, one carrying CR or LF forges a
// second header, and one carrying a leading space is read as something else
// entirely. The one rule that is not about framing is the last: a name in the
// server's own Nats- namespace is refused, because the server would read it as a
// control header.
func HeaderKeyOK(k string) bool {
	if k == "" || len(k) > MaxHeaderKeyLen {
		return false
	}
	for i := range len(k) {
		if c := k[i]; c <= ' ' || c >= 0x7f || c == ':' {
			return false
		}
	}
	return !IsNATSHeader(k)
}

// IsNATSHeader reports whether a header name is in the server's own namespace.
// The comparison is case-insensitive: the server matches its own spelling
// exactly, and a name that differs only in case is still close enough to a
// control header that carrying it would be a trap.
func IsNATSHeader(k string) bool {
	if len(k) < len(NATSHeaderPrefix) {
		return false
	}
	return strings.EqualFold(k[:len(NATSHeaderPrefix)], NATSHeaderPrefix)
}

// HeaderValueOK reports whether a header value survives the framing and comes
// back byte for byte.
//
// Three rules, each of which a round trip really breaks: a value must be valid
// UTF-8 (the wire is a text line and the reader is textproto's, so an arbitrary
// byte string is not carried — which is why this driver does not advertise
// FeatureBinaryHeaders); it must carry no control byte, CR and LF above all; and
// it must not begin with a space or a tab, because the reader eats those after
// the colon and the value would come back shorter than it went out.
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

// Representable reports whether every piece of m's metadata has a native
// JetStream field or a reserved header that carries it losslessly.
//
// It is what MetadataNativeStrict refuses on and what MetadataAdaptive wraps on,
// and it never touches the network: the specification requires the refusal to happen before
// any I/O.
//
// the five dead-letter provenance headers are admitted, because they are
// iqmq's own and not a caller's: the core's dead-letter route publishes a
// stamped copy of the poison message through this very path, so a driver that
// refused them made mq.WithDeadLetter unusable — the publication failed before
// any I/O, the runner requeued and counted a DeadLetterFailures, and the
// message was redelivered for ever.
// [driverkit.ReservedHeaderAllowed] names the five and nothing else, so this
// driver's own reserved carriers and the server's Nats- namespace stay refused
// from a caller.
//
// They are still held to the header-value rule below: a JetStream header block
// is HTTP-shaped, so a value with a control character in it cannot be framed
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
		if IsNATSHeader(k) {
			return errNATSHeader
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

// Encode fills msg from m: the payload and the headers, reserved ones included.
// It never sets msg.Reply; the reply-to travels as [HeaderReplyTo].
//
// drop is MetadataLenientDrop: a header that cannot be carried is left out
// instead of refusing the publication, and drop is called with its name so that
// the specification's "with a hook and a metric" has somewhere to land. Under every other
// mode the caller has already run [Representable], so nothing is dropped.
//
// It never retains anything of m EXCEPT the body, which it aliases: header names
// and values are copied into the nats.Header as strings, and msg.Data points at
// m.Body. A synchronous publication is finished with the message before it
// returns, so the alias is invisible; every asynchronous path copies the body
// first, because the client library holds the message until its PubAck (
// ).
func Encode(msg *natsgo.Msg, m *mq.Message, drop func(string)) {
	msg.Data = m.Body
	n := m.Headers.Len() + 9
	h := msg.Header
	if h == nil {
		h = make(natsgo.Header, n)
		msg.Header = h
	}
	for k, v := range m.Headers.All() {
		// A reserved key is the driver's own namespace and a caller's is
		// dropped rather than published — except the five provenance
		// keys, which are the CORE's own and have to reach the dead-letter
		// subject or the route carries no provenance at all. The value rule applies to them like any other.
		reserved := strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k)
		if reserved || !HeaderKeyOK(k) || !HeaderValueOK(v) {
			if drop != nil {
				drop(k)
			}
			continue
		}
		h.Add(k, string(v))
	}
	if m.Properties.ReplyTo != "" {
		// Never msg.Reply: see HeaderReplyTo.
		if sub, err := Subject("", m.Properties.ReplyTo); err == nil {
			h.Set(HeaderReplyTo, sub)
		} else if drop != nil {
			drop("reply-to")
		}
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

// Decode fills m from the parts of one native message, with the reserved headers
// read back into the fields they came from.
//
// reply is a native reply subject, for a caller that has one that means
// reply-to; [HeaderReplyTo] wins over it. A message without the header, which
// is every message an earlier release or a non-iqmq publisher wrote, decodes
// with the reply-to reply gives it.
//
// The server's own Nats- headers are NOT decoded as caller headers. A message
// the scheduler generated carries Nats-Scheduler and Nats-Schedule-Next, a
// republished one carries Nats-Stream and Nats-Sequence, and handing those to
// the application as though it had published them would be inventing metadata.
//
// The header names are sorted, which is not what the specification promises but is the best a
// nats.Header can do: it is a map, so the order the publisher wrote is gone by
// the time nats.go hands the message over. Sorting makes what is left
// deterministic, which matters more than it sounds — an unsorted map iteration
// would give the same message a different header order on every delivery.
func Decode(m *mq.Message, subject, reply string, hdr natsgo.Header, body []byte, prefix string) {
	m.Topic = TopicOf(prefix, subject)
	m.Body = body
	if reply != "" {
		m.Properties.ReplyTo = TopicOf(prefix, reply)
	}
	if len(hdr) == 0 {
		if len(m.Body) == 0 {
			m.Body = nil
		}
		return
	}
	empty := false
	keys := make([]string, 0, len(hdr))
	for k := range hdr {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for _, k := range keys {
		vals := hdr[k]
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
		case HeaderReplyTo:
			if v := first(vals); v != "" {
				m.Properties.ReplyTo = TopicOf(prefix, v)
			}
		case HeaderEnvelope:
			// Framing, not metadata: [Enveloped] is what reads it, and a
			// caller must not be handed iqmq's own discriminator as though
			// it had published it.
		default:
			if IsNATSHeader(k) {
				continue
			}
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
	errEmptyTopic          = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: the topic is empty")
	errSubjectTooLong      = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: the topic does not fit a NATS subject")
	errInvalidSubject      = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: the topic is not a NATS subject this driver will publish or subscribe to")
	errInvalidName         = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: the name is not one JetStream accepts for a stream or a consumer")
	errInvalidSubscription = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: the subscription does not map to a durable consumer name")
	errNilMessage          = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: the message is nil")
	errReservedHeader      = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: a header key uses the reserved "+mq.ReservedHeaderPrefix+" prefix")
)

// The metadata refusals each is ErrUnsupported, because what they
// report is a broker that has no field for the metadata rather than a caller who
// wrote something malformed.
var (
	errHeaderKey   = errcode.New(errcode.Unsupported, "iqmq/jetstream: NATS headers cannot carry that header name (see docs/drivers/jetstream.md)")
	errNATSHeader  = errcode.New(errcode.Unsupported, "iqmq/jetstream: a header key in the server's Nats- namespace would be read as a JetStream control header")
	errHeaderValue = errcode.New(errcode.Unsupported, "iqmq/jetstream: NATS headers are text lines and cannot carry that value; use MetadataPortableEnvelope or MetadataAdaptive")
	errReplyTo     = errcode.New(errcode.Unsupported, "iqmq/jetstream: Properties.ReplyTo is not a subject this driver can address")
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

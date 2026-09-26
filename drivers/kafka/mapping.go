package kafka

import (
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The reserved headers this driver uses for the parts Kafka has no
// native field for.
//
// Kafka's record headers are ordered, duplicate-preserving and binary, so a
// caller's own headers map one for one and need no encoding at all: this list
// is only what iqmq itself has to carry. The names follow the inventory and
// the spelling established, under the reserved prefix the specification sets aside — a
// caller's header under that prefix is refused rather than silently renamed.
const (
	// HeaderID carries Message.ID.
	HeaderID = mq.ReservedHeaderPrefix + "id"
	// HeaderContentType carries Properties.ContentType.
	HeaderContentType = mq.ReservedHeaderPrefix + "content-type"
	// HeaderContentEncoding carries Properties.ContentEncoding.
	HeaderContentEncoding = mq.ReservedHeaderPrefix + "content-encoding"
	// HeaderCorrelationID carries Properties.CorrelationID.
	HeaderCorrelationID = mq.ReservedHeaderPrefix + "correlation-id"
	// HeaderCausationID carries Properties.CausationID.
	HeaderCausationID = mq.ReservedHeaderPrefix + "causation-id"
	// HeaderReplyTo carries Properties.ReplyTo.
	HeaderReplyTo = mq.ReservedHeaderPrefix + "reply-to"
	// HeaderCreated marks the record's native timestamp as the publisher's
	// Properties.CreatedAt rather than the produce time.
	//
	// Kafka gives every record a timestamp whether the publisher set one or
	// not — franz-go stamps the produce time — so a decoder that read it as
	// CreatedAt would invent a creation time for every message that carried
	// none, and strict mode requires a field to come back exactly as
	// it was published. The marker is the rarer half: its presence means
	// the timestamp is the publisher's, and its absence means CreatedAt was
	// unset. Its value is empty; only its presence is read.
	HeaderCreated = mq.ReservedHeaderPrefix + "created"
	// HeaderAttempts carries the delivery attempt a retried record has had,
	// which is what makes an attempt count survive a republication to a
	// retry topic: a freshly produced record's attempt is 1 again as far as
	// the broker is concerned.
	HeaderAttempts = mq.ReservedHeaderPrefix + "attempts"
	// HeaderOriginTopic, HeaderOriginSubscription, HeaderOriginPartition
	// and HeaderOriginOffset are the provenance of a record the driver
	// moved: where it came from before it was republished to a retry
	// topic or a dead-letter topic.
	HeaderOriginTopic        = mq.ReservedHeaderPrefix + "origin-topic"
	HeaderOriginSubscription = mq.ReservedHeaderPrefix + "origin-subscription"
	HeaderOriginPartition    = mq.ReservedHeaderPrefix + "origin-partition"
	HeaderOriginOffset       = mq.ReservedHeaderPrefix + "origin-offset"
	// HeaderFailure is the reason the record was moved, which is local
	// diagnostics and never a payload.
	HeaderFailure = mq.ReservedHeaderPrefix + "failure"
	// HeaderFirstDeliveredAt is when the record was first delivered to a
	// handler. It is RFC 3339 with nanoseconds.
	HeaderFirstDeliveredAt = mq.ReservedHeaderPrefix + "first-delivered-at"
	// HeaderEnvelope marks a record whose VALUE is the portable envelope
	// rather than the caller's payload, and it is the reason this driver
	// never looks at the body to decide.
	//
	// Kafka has real headers, so the answer to "did iqmq envelope this?"
	// can be recorded where a payload cannot reach it. Under
	// MetadataAdaptive an enveloped record and a native one share a
	// consumer group, and the alternative — reading the body's first bytes
	// — decides provenance from evidence the producer of the PAYLOAD
	// controls: a native publication whose value happens to be a valid
	// envelope was decoded as metadata, which replaced the caller's payload
	// with the envelope's own key and headers.
	//
	// Only its presence is read. Its value is driverkit's marker value,
	// written for an operator reading a record by hand, and the name is
	// driverkit.EnvelopeMarker — the one every driver with somewhere to put
	// it spells, because two drivers writing one topic have to agree
	// (TestEnvelopeMarkerIsTheKitsName).
	HeaderEnvelope = mq.ReservedHeaderPrefix + "env"
)

// EnvelopeMarkerValue is the value [HeaderEnvelope] carries.
const EnvelopeMarkerValue = "1"

// MaxTopicLen is the longest topic name Kafka accepts.
const MaxTopicLen = 249

// Topic maps a portable topic onto a Kafka topic name, with the configured
// prefix.
//
// The mapping is the identity plus the prefix, which is the whole of it:
// Kafka's own topic grammar — `[a-zA-Z0-9._-]`, at most 249 characters, and
// never "." or ".." — is a subset of what the specification allows, so the driver refuses a
// name it cannot carry rather than transforming it. A transformation would not
// be injective (the NATS driver's `/` → `.` is not, which is), and a Kafka topic name is something operators type into `kafka-acls`
// and Grafana queries: a driver that silently renamed it would make the
// deployment's own tooling point at a topic nobody configured.
func Topic(prefix, topic string) (string, error) {
	if topic == "" {
		return "", errEmptyTopic
	}
	name := topic
	if prefix != "" {
		name = prefix + topic
	}
	if err := CheckTopic(name); err != nil {
		return "", err
	}
	return name, nil
}

// TopicOf strips the configured prefix from a Kafka topic name, which is the
// resolved topic the specification puts on a delivery.
func TopicOf(prefix, name string) string {
	if prefix != "" {
		return strings.TrimPrefix(name, prefix)
	}
	return name
}

// CheckTopic reports whether name is a topic name Kafka accepts.
func CheckTopic(name string) error {
	switch {
	case name == "":
		return errEmptyTopic
	case len(name) > MaxTopicLen:
		return errTopicTooLong
	case name == "." || name == "..":
		return errTopicDots
	case looksLikePattern(name):
		// A consumer may be trying to subscribe to a pattern. franz-go can
		// do that (ConsumeRegex) and this driver does not wire it, so the
		// answer is ErrUnsupported and not "invalid topic": the difference
		// is what the capability table claims, and the specification requires an
		// unadvertised feature to be refused as unsupported.
		return errNoWildcards
	case !validTopicName(name):
		return errBadTopicChars
	default:
		return nil
	}
}

// validTopicName reports whether every byte of name is one Kafka accepts in a
// topic name.
func validTopicName(name string) bool {
	if name == "" || len(name) > MaxTopicLen {
		return false
	}
	for i := range len(name) {
		if !topicByte(name[i]) {
			return false
		}
	}
	return true
}

// validTopicPart reports whether s may be part of a topic name, which is what
// a configured prefix has to be.
func validTopicPart(s string) bool {
	if s == "" || len(s) > MaxTopicLen {
		return false
	}
	for i := range len(s) {
		if !topicByte(s[i]) {
			return false
		}
	}
	return true
}

// topicByte reports whether c is legal in a Kafka topic name.
func topicByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '.' || c == '_' || c == '-':
		return true
	default:
		return false
	}
}

// patternBytes are the regular-expression metacharacters that tell a
// subscription pattern apart from a topic name Kafka merely refuses.
const patternBytes = `^$*+?()[]{}|\`

// looksLikePattern reports whether name carries a regular-expression
// metacharacter, and is therefore somebody asking for a pattern subscription
// rather than mistyping a topic.
func looksLikePattern(name string) bool {
	return strings.ContainsAny(name, patternBytes)
}

// Representable reports whether m can be published to Kafka with every field
// intact under the strict mode.
//
// Almost everything can: Kafka's headers are ordered, binary and
// duplicate-preserving, its key and value are nullable byte strings, and its
// record timestamp carries the creation time. What cannot is a caller's header
// under the reserved prefix, which the specification refuses outright, and a header key
// that is not valid UTF-8, which the specification requires in portable mode.
//
// the five dead-letter provenance headers are the exception, and they are
// iqmq's own rather than a caller's: the core's dead-letter route publishes a
// stamped copy of the poison message through this very path, so a driver that
// refused them made mq.WithDeadLetter unusable — the publication failed before
// any I/O, the runner requeued and counted a DeadLetterFailures, and the
// message was redelivered for ever.
// [driverkit.ReservedHeaderAllowed] is the one place the five are named, and
// nothing else under the prefix is admitted: this driver's own reserved names
// and [HeaderEnvelope] stay refused, because a caller that could set one could
// forge the driver's own decisions.
func Representable(m *mq.Message) error {
	if m == nil {
		return errNilMessage
	}
	for k := range m.Headers.All() {
		if strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k) {
			return errReservedHeader
		}
		if k == "" {
			return errEmptyHeaderKey
		}
	}
	return nil
}

// Encode fills rec with what this driver publishes for m, and returns the
// scratch buffer it wrote the reserved header values into.
//
// rec is caller storage and its Headers slice is reused, so a publication
// allocates nothing once both have grown. The scratch buffer is why: the
// identifier and the typed properties are STRINGS in the portable API and
// BYTES in a Kafka header, and a conversion per field would cost one
// allocation per populated property on every publication. Passing the buffer
// in keeps this function pure and keeps the cost at zero — the caller reuses
// the buffer for the life of one record, which is exactly how long the
// record's header values point into it.
//
// Nothing of m is retained beyond the call by this function: the record points
// at m's key, body and header values, and the sink's contract is that it does
// not outlive the produce that copies them.
//
// It is pure: no clock, no I/O, no configuration beyond what is passed in. It
// does NOT decide the partition — see [RouteToPartition] for why that cannot
// be a field of the record.
func Encode(rec *kgo.Record, m *mq.Message, topic string, scratch []byte) []byte {
	rec.Topic = topic
	rec.Key = m.Key
	rec.Value = m.Body
	rec.Headers = rec.Headers[:0]
	scratch = scratch[:0]
	for k, v := range m.Headers.All() {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: v})
	}
	// A value appended after an earlier one still points at the bytes it
	// was written with, even when the buffer has since grown into a new
	// array: the old array stays alive for as long as a header references
	// it. That is what makes one buffer enough for every field.
	put := func(key, val string) {
		if val == "" {
			return
		}
		start := len(scratch)
		scratch = append(scratch, val...)
		rec.Headers = append(rec.Headers, kgo.RecordHeader{
			Key: key, Value: scratch[start:len(scratch):len(scratch)],
		})
	}
	put(HeaderID, m.ID)
	p := &m.Properties
	put(HeaderContentType, p.ContentType)
	put(HeaderContentEncoding, p.ContentEncoding)
	put(HeaderCorrelationID, p.CorrelationID)
	put(HeaderCausationID, p.CausationID)
	put(HeaderReplyTo, p.ReplyTo)
	if !p.CreatedAt.IsZero() {
		// The native timestamp carries the creation time and the marker
		// says so, because Kafka stamps every record whether the publisher
		// set a time or not.
		rec.Timestamp = p.CreatedAt
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: HeaderCreated})
	}
	return scratch
}

// Decode fills m from rec, undoing [Encode].
//
// The header values are the record's own storage, surfaced without a copy:
// The specification makes received header values read-only, and makes the delivery's
// storage borrowed until it is settled.
func Decode(m *mq.Message, rec *kgo.Record, prefix string) {
	m.Topic = TopicOf(prefix, rec.Topic)
	m.Key = rec.Key
	m.Body = rec.Value
	m.Headers.Reset()
	created := false
	for _, h := range rec.Headers {
		switch h.Key {
		case HeaderID:
			m.ID = string(h.Value)
		case HeaderContentType:
			m.Properties.ContentType = string(h.Value)
		case HeaderContentEncoding:
			m.Properties.ContentEncoding = string(h.Value)
		case HeaderCorrelationID:
			m.Properties.CorrelationID = string(h.Value)
		case HeaderCausationID:
			m.Properties.CausationID = string(h.Value)
		case HeaderReplyTo:
			m.Properties.ReplyTo = string(h.Value)
		case HeaderCreated:
			created = true
		case HeaderEnvelope:
			// Framing, not metadata: [Enveloped] is what reads it, and a
			// caller must not be handed iqmq's own discriminator as though
			// it had published it.
		default:
			m.Headers.Add(h.Key, h.Value)
		}
	}
	if created {
		m.Properties.CreatedAt = rec.Timestamp
	}
}

// Enveloped reports whether rec carries [HeaderEnvelope], which is the only
// thing that makes its value the portable envelope rather than a payload.
//
// It is the whole of MetadataAdaptive's read-side decision on this driver. A
// record with no marker is native — whatever its bytes look like — because
// this driver writes the marker on every record it envelopes and the specification keeps a
// caller's own headers out of the reserved prefix. That is the "never guess
// from the body" rule [github.com/iqhive/iqmq/wire.DetectFull]
// is for a broker with nowhere to keep the answer, and it is strictly weaker.
func Enveloped(rec *kgo.Record) bool {
	for _, h := range rec.Headers {
		if h.Key == HeaderEnvelope {
			return true
		}
	}
	return false
}

// MarkEnveloped records on rec that its value is the portable envelope.
//
// It is written after [Encode] rather than inside it because Encode is given
// the message that is going on the wire and cannot tell an envelope from a
// payload that happens to look like one — which is the defect, one layer down.
func MarkEnveloped(rec *kgo.Record) {
	rec.Headers = append(rec.Headers, kgo.RecordHeader{
		Key: HeaderEnvelope, Value: []byte(EnvelopeMarkerValue),
	})
}

// Attempt reads the attempt count a retried record carries, or zero when it
// carries none.
//
// The specification makes a zero attempt "the broker does not count attempts", which is the
// truth for Kafka: a record redelivered because its offset was never committed
// arrives looking exactly like a first delivery. What the driver DOES know is
// the attempt of a record it republished itself, and this is where that
// travels.
func Attempt(rec *kgo.Record) uint32 {
	for _, h := range rec.Headers {
		if h.Key != HeaderAttempts {
			continue
		}
		n, err := strconv.ParseUint(string(h.Value), 10, 32)
		if err != nil {
			return 0
		}
		return uint32(n)
	}
	return 0
}

// stampProvenance adds the provenance headers to a record the driver is
// moving to a retry topic or a dead-letter topic, replacing any it already
// carried.
//
// Replacing rather than appending matters: a record that has been through two
// retry topics would otherwise carry two attempt counts, and a consumer
// reading the first would see the older one (Get returns the first
// value of a key).
func stampProvenance(rec *kgo.Record, info *mq.DeliveryInfo, attempt uint32, reason string, firstAt time.Time) {
	drop := func(key string) {
		out := rec.Headers[:0]
		for _, h := range rec.Headers {
			if h.Key != key {
				out = append(out, h)
			}
		}
		rec.Headers = out
	}
	add := func(key, val string) {
		drop(key)
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: key, Value: []byte(val)})
	}
	if attempt > 0 {
		add(HeaderAttempts, strconv.FormatUint(uint64(attempt), 10))
	}
	add(HeaderOriginTopic, info.Topic)
	add(HeaderOriginSubscription, info.Subscription)
	add(HeaderOriginPartition, strconv.FormatInt(int64(info.Partition), 10))
	add(HeaderOriginOffset, strconv.FormatInt(info.Offset, 10))
	if reason != "" {
		add(HeaderFailure, reason)
	}
	if !firstAt.IsZero() {
		add(HeaderFirstDeliveredAt, firstAt.Format(time.RFC3339Nano))
	}
}

// The mapping refusals.
var (
	errEmptyTopic     = errcode.New(errcode.InvalidArgument, "iqmq/kafka: the topic is empty")
	errTopicTooLong   = errcode.New(errcode.InvalidArgument, "iqmq/kafka: the topic is longer than Kafka's 249 characters")
	errTopicDots      = errcode.New(errcode.InvalidArgument, `iqmq/kafka: Kafka reserves the topic names "." and ".."`)
	errBadTopicChars  = errcode.New(errcode.InvalidArgument, "iqmq/kafka: a Kafka topic name is made of [a-zA-Z0-9._-] only")
	errNoWildcards    = errcode.New(errcode.Unsupported, "iqmq/kafka: this driver consumes literal topics only; franz-go's regular-expression subscription is not wired (see docs/drivers/kafka.md)")
	errNilMessage     = errcode.New(errcode.InvalidArgument, "iqmq/kafka: the message is nil")
	errReservedHeader = errcode.New(errcode.InvalidArgument, "iqmq/kafka: a header key uses the reserved "+mq.ReservedHeaderPrefix+" prefix")
	errEmptyHeaderKey = errcode.New(errcode.InvalidArgument, "iqmq/kafka: a header key is empty")
)

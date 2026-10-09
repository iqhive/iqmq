package pulsar

import (
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/wire"
)

// The reserved properties this driver uses for the parts Pulsar has no
// native field for.
//
// Pulsar's properties are a `map[string]string`, so a caller's own headers map
// onto them one for one — with three losses this driver states rather than
// hides: a map has no ORDER, a map cannot hold two values for one key, and a
// protobuf string is UTF-8. Those are what the envelope is for; the names below
// are only what iqmq itself has to carry, under the reserved prefix the specification sets
// aside. A caller's header under that prefix is refused rather than renamed.
const (
	// PropID carries Message.ID.
	PropID = mq.ReservedHeaderPrefix + "id"
	// PropContentType carries Properties.ContentType.
	PropContentType = mq.ReservedHeaderPrefix + "content-type"
	// PropContentEncoding carries Properties.ContentEncoding.
	PropContentEncoding = mq.ReservedHeaderPrefix + "content-encoding"
	// PropCorrelationID carries Properties.CorrelationID.
	PropCorrelationID = mq.ReservedHeaderPrefix + "correlation-id"
	// PropCausationID carries Properties.CausationID.
	PropCausationID = mq.ReservedHeaderPrefix + "causation-id"
	// PropReplyTo carries Properties.ReplyTo.
	PropReplyTo = mq.ReservedHeaderPrefix + "reply-to"
	// PropCreated marks Pulsar's own event time as the publisher's
	// Properties.CreatedAt rather than an unset field.
	//
	// The marker is needed because a Pulsar message that carries no event
	// time arrives with one anyway: pulsar-client-go converts the absent
	// protobuf field through timeFromUnixTimestampMillis(0), which is the
	// Unix epoch and not the zero time. A decoder that read that as
	// CreatedAt would invent a creation time of 1970 for every message that
	// carried none, and the strict mode requires a field to come back
	// exactly as it was published. Its value is empty; only its presence is
	// read.
	PropCreated = mq.ReservedHeaderPrefix + "created"
)

// sysProperties are pulsar-client-go's OWN properties, which travel on a
// message the client moved to a retry or dead-letter topic.
//
// They are stripped on decode. Without that, a message that came back through
// the retry topic would arrive carrying four or five headers the publisher
// never set — which breaks header fidelity for exactly the messages a caller is
// most likely to be inspecting, and which would make a redelivered message
// unequal to the one that was published.
var sysProperties = [...]string{
	pulsar.SysPropertyDelayTime,
	pulsar.SysPropertyRealTopic,
	pulsar.SysPropertyRetryTopic,
	pulsar.SysPropertyReconsumeTimes,
	pulsar.SysPropertyOriginMessageID,
	pulsar.PropertyOriginMessageID,
}

// isSysProperty reports whether a property is pulsar-client-go's own.
func isSysProperty(key string) bool {
	for _, k := range sysProperties {
		if key == k {
			return true
		}
	}
	return false
}

// Representable reports whether Pulsar can carry m's metadata natively, and the
// refusal to use when it cannot (the strict mode).
//
// The five refusals are the five losses a `map[string]string` and a `string`
// key impose, and each of them is something the envelope can carry:
//
//   - a header value that is not valid UTF-8. A Pulsar property is a protobuf
//     string.
//   - a repeated header key. A map holds one value per key, so the second
//     would silently replace the first.
//   - a header key that is empty, or under the reserved prefix — except
//     the five dead-letter provenance keys, which are iqmq's own and not a
//     caller's. The core's dead-letter route publishes a stamped copy of the
//     poison message through this very path, so a driver that refused them
//     made mq.WithDeadLetter unusable: the publication failed before any I/O,
//     the runner requeued and counted a DeadLetterFailures, and the message
//     was redelivered for ever.
//     [driverkit.ReservedHeaderAllowed] names the five and nothing else, so
//     this driver's own reserved properties stay refused from a caller.
//   - a message key that is not valid UTF-8, for the same reason as a header
//     value.
//   - a message key that is EMPTY but not nil. pulsar-client-go writes the
//     partition key only when it is non-empty, so an empty key arrives as no
//     key at all — and the specification makes nil and empty observably different.
//
// A nil BODY is deliberately absent from the list: Pulsar has a null-value flag
// in its message metadata and pulsar-client-go sets it for a nil payload, so
// nil and empty bodies round-trip natively. It is the one broker in this
// repository where that is true without an envelope.
func Representable(m *mq.Message) error {
	if m == nil {
		return errNilMessage
	}
	if m.Key != nil {
		if len(m.Key) == 0 {
			return errEmptyKey
		}
		if !utf8.Valid(m.Key) {
			return errBinaryKey
		}
	}
	seen := make(map[string]struct{}, m.Headers.Len())
	for k, v := range m.Headers.All() {
		switch {
		case k == "":
			return errEmptyHeaderKey
		case strings.HasPrefix(k, mq.ReservedHeaderPrefix) && !driverkit.ReservedHeaderAllowed(k):
			return errReservedHeader
		case !utf8.ValidString(k):
			return errBinaryHeaderKey
		case !utf8.Valid(v):
			return errBinaryHeader
		}
		if _, dup := seen[k]; dup {
			return errDuplicateHeader
		}
		seen[k] = struct{}{}
	}
	return nil
}

// Encode fills pm with what this driver publishes for m.
//
// props is caller storage, cleared and reused, which is what keeps a
// publication from allocating a map per message. Nothing of m is retained: pm
// points at m's body for the length of the call, which is what the specification allows.
//
// It is pure: no clock, no I/O, no configuration beyond what is passed in.
func Encode(pm *pulsar.ProducerMessage, m *mq.Message, props map[string]string) {
	clear(props)
	pm.Payload = m.Body
	pm.Key = ""
	if len(m.Key) > 0 {
		pm.Key = string(m.Key)
	}
	pm.EventTime = time.Time{}
	pm.DeliverAfter = 0
	pm.DeliverAt = time.Time{}
	pm.SequenceID = nil
	for k, v := range m.Headers.All() {
		props[k] = string(v)
	}
	put := func(key, val string) {
		if val != "" {
			props[key] = val
		}
	}
	put(PropID, m.ID)
	p := &m.Properties
	put(PropContentType, p.ContentType)
	put(PropContentEncoding, p.ContentEncoding)
	put(PropCorrelationID, p.CorrelationID)
	put(PropCausationID, p.CausationID)
	put(PropReplyTo, p.ReplyTo)
	if !p.CreatedAt.IsZero() {
		pm.EventTime = p.CreatedAt
		props[PropCreated] = ""
	}
	pm.Properties = props
	if len(props) == 0 {
		// A message with no metadata publishes no property map at all,
		// which is one fewer allocation on the broker's side and what a
		// non-iqmq consumer of the same topic sees.
		pm.Properties = nil
	}
}

// Decode fills m from a received Pulsar message, undoing [Encode].
//
// The body is the client's own storage, surfaced without a copy: the specification makes
// received header values read-only and the specification makes a delivery's storage
// borrowed until it is settled. The header VALUES are copies, because Pulsar's
// properties are strings and a []byte view of a string cannot be had without
// one.
func Decode(m *mq.Message, msg pulsar.Message, topic string) {
	m.Topic = topic
	m.Body = msg.Payload()
	if msg.IsNullValue() {
		// Pulsar's own null-value flag, which is what makes the specification's
		// nil-versus-empty distinction survive the round trip.
		m.Body = nil
	}
	m.Key = nil
	if k := msg.Key(); k != "" {
		m.Key = []byte(k)
	}
	m.ID = ""
	m.Properties = mq.Properties{}
	m.Headers.Reset()
	created := false
	for k, v := range msg.Properties() {
		switch k {
		case PropID:
			m.ID = v
		case PropContentType:
			m.Properties.ContentType = v
		case PropContentEncoding:
			m.Properties.ContentEncoding = v
		case PropCorrelationID:
			m.Properties.CorrelationID = v
		case PropCausationID:
			m.Properties.CausationID = v
		case PropReplyTo:
			m.Properties.ReplyTo = v
		case PropCreated:
			created = true
		default:
			if !isSysProperty(k) {
				m.Headers.Add(k, []byte(v))
			}
		}
	}
	if created {
		m.Properties.CreatedAt = msg.EventTime()
	}
}

// AttemptOf is the delivery attempt the specification puts on a delivery, counting from 1.
//
// Two counters have to be added, and that is Pulsar's doing rather than this
// driver's. `RedeliveryCount` is the broker's and counts how many times THIS
// message was pushed again after a negative acknowledgement or a consumer
// loss. `RECONSUMETIMES` is pulsar-client-go's and counts the hops through the
// RETRY TOPIC — which are republications, so the broker's counter starts again
// at zero for each of them.
//
// A driver that read only the broker's counter would report attempt 1 on every
// delivery of a message that had been round the retry topic five times, and a
// MaxAttempts policy above it would never fire.
func AttemptOf(msg pulsar.Message) uint32 {
	n := msg.RedeliveryCount()
	if s, ok := msg.Properties()[pulsar.SysPropertyReconsumeTimes]; ok {
		if v, err := strconv.ParseUint(s, 10, 32); err == nil {
			n += uint32(v)
		}
	}
	return n + 1
}

// PositionOf is the [BatchPosition] of a received message.
func PositionOf(id pulsar.MessageID) BatchPosition {
	if id == nil {
		return BatchPosition{Ledger: -1, Entry: -1, Index: -1}
	}
	return BatchPosition{
		Ledger:    id.LedgerID(),
		Entry:     id.EntryID(),
		Partition: id.PartitionIdx(),
		Index:     id.BatchIdx(),
		Size:      id.BatchSize(),
	}
}

// wrapForPublish is the whole decision for this driver: what to publish,
// and the refusal when nothing can be.
//
// The shape is the one every header-limited driver ends up with, and the reason
// the ADAPTIVE case does not simply ask [driverkit.Envelope.Needed] is recorded
// in the maintenance notes: Needed reads the CAPABILITY TABLE, and two of this
// driver's five reasons to envelope are not features at all — a message KEY
// that is binary or empty, which the inventory has no word for. Both are
// answered by [Representable], and both take `forced`, a second envelope in the
// portable mode whose Wrap always wraps.
func wrapForPublish(env, forced *driverkit.Envelope, lim *wire.Limits, mode mq.MetadataMode, m *mq.Message) (*mq.Message, error) {
	if m == nil {
		return nil, errNilMessage
	}
	switch mode {
	case mq.MetadataNativeStrict:
		if err := Representable(m); err != nil {
			return nil, err
		}
		return m, nil
	case mq.MetadataLenientDrop:
		if Representable(m) == nil {
			return m, nil
		}
		// the lenient mode: the metadata is dropped and the body is
		// published. Nothing is refused and nothing is hidden — the
		// caller asked for this mode and the driver counts what it
		// discarded.
		return &mq.Message{Topic: m.Topic, Body: m.Body}, nil
	case mq.MetadataAdaptive:
		if Representable(m) != nil {
			out, err := forced.Wrap(m)
			if err != nil {
				return nil, envelopeRefusal(m, lim)
			}
			return out, nil
		}
		// A message Pulsar can carry natively still goes through the
		// kit, because the kit knows the one reason to envelope that
		// this driver cannot see: a BODY whose first bytes the decoder's
		// own discriminator would claim. Wrap envelopes such a body so
		// that this driver never publishes a native body its own Unwrap
		// would read as metadata.
		out, err := env.Wrap(m)
		if err != nil {
			return nil, envelopeRefusal(m, lim)
		}
		return out, nil
	case mq.MetadataPortableEnvelope:
		out, err := env.Wrap(m)
		if err != nil {
			return nil, envelopeRefusal(m, lim)
		}
		return out, nil
	default:
		return nil, errMetadataMode
	}
}

// envelopeRefusal names the field that put m past the envelope's limits.
//
// The DECISION is [driverkit.Envelope.Check]'s and only ever the kit's: this
// function is reached exactly when Check has already refused, and its answer is
// a diagnosis rather than an admission rule. the maintenance notes records the
// fourth hand-written copy of the envelope's limit table as a defect class of
// its own — three drivers accepted a publication their own decoder would refuse
// for ever, each through a copy of the rules that had drifted — and a diagnosis
// that drifts produces a worse error message, never a wrong decision.
func envelopeRefusal(m *mq.Message, lim *wire.Limits) error {
	if m == nil {
		return errNilMessage
	}
	r := wire.DefaultLimits
	if lim != nil {
		r = *lim
	}
	over := func(n, bound int) bool { return bound > 0 && n > bound }
	switch {
	case over(len(m.Key), r.MaxKey):
		return errEnvelopeKey
	case over(len(m.Body), r.MaxBody):
		return errEnvelopeBody
	case over(len(m.ID), r.MaxID):
		return errEnvelopeID
	case over(m.Headers.Len(), r.MaxHeaders):
		return errEnvelopeHeaderCount
	}
	for k, v := range m.Headers.All() {
		if over(len(k), r.MaxHeaderKey) {
			return errEnvelopeHeaderKey
		}
		if over(len(v), r.MaxHeaderValue) {
			return errEnvelopeHeaderValue
		}
		if !utf8.ValidString(k) {
			return errEnvelopeHeaderKeyUTF8
		}
	}
	if !utf8.ValidString(m.ID) {
		return errEnvelopeIDUTF8
	}
	p := &m.Properties
	for _, s := range [...]string{
		p.ContentType, p.ContentEncoding, p.CorrelationID, p.CausationID, p.ReplyTo,
	} {
		if over(len(s), r.MaxProp) {
			return errEnvelopeProperty
		}
		if !utf8.ValidString(s) {
			return errEnvelopePropertyUTF8
		}
	}
	// Check refused and this walk found nothing, which means the two have
	// drifted. Saying so is better than naming a field at random: the
	// publication is still refused, and the message says where to look.
	return errEnvelopeUndiagnosed
}

// droppedMetadata counts what MetadataLenientDrop discarded from m, which is the specification's "dropped with a hook and a metric".
func droppedMetadata(m *mq.Message) int {
	if m == nil {
		return 0
	}
	n := m.Headers.Len()
	if m.ID != "" {
		n++
	}
	if !m.Properties.IsZero() {
		n++
	}
	if m.Key != nil {
		n++
	}
	return n
}

// The mapping's refusals.
var (
	errNilMessage      = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the message is nil")
	errEmptyKey        = errcode.New(errcode.Unsupported, "iqmq/pulsar: pulsar-client-go writes the partition key only when it is non-empty, so an EMPTY key arrives as no key at all; use MetadataPortableEnvelope or MetadataAdaptive to keep the nil-versus-empty distinction")
	errBinaryKey       = errcode.New(errcode.Unsupported, "iqmq/pulsar: a Pulsar message key is a protobuf string and cannot carry bytes that are not valid UTF-8; use MetadataPortableEnvelope or MetadataAdaptive")
	errBinaryHeader    = errcode.New(errcode.Unsupported, "iqmq/pulsar: a Pulsar property value is a protobuf string and cannot carry bytes that are not valid UTF-8; use MetadataPortableEnvelope or MetadataAdaptive")
	errBinaryHeaderKey = errcode.New(errcode.Unsupported, "iqmq/pulsar: a Pulsar property key is a protobuf string and cannot carry bytes that are not valid UTF-8; use MetadataPortableEnvelope or MetadataAdaptive")
	errDuplicateHeader = errcode.New(errcode.Unsupported, "iqmq/pulsar: Pulsar properties are a map and hold one value per key, so a repeated header key would lose every value but the last; use MetadataPortableEnvelope or MetadataAdaptive")
	errEmptyHeaderKey  = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a header key cannot be empty")
	errReservedHeader  = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a header key under the reserved prefix \"iqmq-\" is refused rather than renamed; the driver carries the message identifier and the typed properties there")

	errEnvelopeKey           = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: the key exceeds the envelope's MaxKey")
	errEnvelopeBody          = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: the body exceeds the envelope's MaxBody")
	errEnvelopeID            = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: the identifier exceeds the envelope's MaxID")
	errEnvelopeHeaderCount   = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: it carries more headers than the envelope's MaxHeaders")
	errEnvelopeHeaderKey     = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: a header key exceeds the envelope's MaxHeaderKey")
	errEnvelopeHeaderValue   = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: a header value exceeds the envelope's MaxHeaderValue")
	errEnvelopeProperty      = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped: a typed property exceeds the envelope's MaxProp")
	errEnvelopeIDUTF8        = errcode.New(errcode.CorruptMessage, "iqmq/pulsar: the message cannot be enveloped: the identifier is not valid UTF-8, which the envelope's decoder refuses")
	errEnvelopeHeaderKeyUTF8 = errcode.New(errcode.CorruptMessage, "iqmq/pulsar: the message cannot be enveloped: a header key is not valid UTF-8, which the envelope's decoder refuses")
	errEnvelopePropertyUTF8  = errcode.New(errcode.CorruptMessage, "iqmq/pulsar: the message cannot be enveloped: a typed property is not valid UTF-8, which the envelope's decoder refuses")
	errEnvelopeUndiagnosed   = errcode.New(errcode.MessageTooLarge, "iqmq/pulsar: the message cannot be enveloped and this driver could not say which field is at fault, which means its diagnosis has drifted from driverkit.Envelope.Check")
)

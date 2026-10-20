package amqp10

import (
	"strconv"
	"time"
	"unicode/utf8"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/wire"
)

// The reserved header names this driver maps to and from the AMQP 1.0 message
// properties that iqmq's portable [github.com/iqhive/iqmq.Message] has no field
// for.
//
// The specification reserves the `iqmq-` prefix for iqmq's own metadata and
// [github.com/iqhive/iqmq.Headers.Validate] refuses a caller-supplied key under
// it. These names are the exception this driver documents: a caller may set
// them, the mapping consumes them into the AMQP property they stand for, and
// the decode puts them back — so a message received here and published again
// arrives with the same properties rather than losing half of them. That round
// trip is the whole reason they exist; without it a received delivery could not
// be forwarded.
//
// Everything else under the prefix is still refused, in every metadata mode,
// because a header the mapping does not consume would be published as an
// application property whose name claims to be iqmq's.
const (
	// HeaderTo is the AMQP `to` property: the node the message is intended
	// for, which an intermediary may rewrite. It is not this driver's
	// address, which is the link's target.
	HeaderTo = mq.ReservedHeaderPrefix + "amqp-to"
	// HeaderSubject is the AMQP `subject` property.
	HeaderSubject = mq.ReservedHeaderPrefix + "amqp-subject"
	// HeaderUserID is the AMQP `user-id` property, which is binary.
	HeaderUserID = mq.ReservedHeaderPrefix + "amqp-user-id"
	// HeaderAbsoluteExpiry is the AMQP `absolute-expiry-time` property, in
	// RFC 3339 with nanoseconds. It is an instant, where
	// PublishOptions.TTL is a duration: a broker honours whichever it has,
	// and setting both is a caller naming the same control twice.
	HeaderAbsoluteExpiry = mq.ReservedHeaderPrefix + "amqp-absolute-expiry-time"
	// HeaderGroupSequence is the AMQP `group-sequence` property, a decimal
	// uint32. The group ITSELF is Message.Key: see [Mapper.Encode].
	HeaderGroupSequence = mq.ReservedHeaderPrefix + "amqp-group-sequence"
	// HeaderReplyToGroupID is the AMQP `reply-to-group-id` property.
	HeaderReplyToGroupID = mq.ReservedHeaderPrefix + "amqp-reply-to-group-id"
	// HeaderCausationID carries Properties.CausationID, which AMQP 1.0 has
	// no field for at all.
	HeaderCausationID = mq.ReservedHeaderPrefix + "causation-id"
	// HeaderFirstAcquirer is the AMQP header's `first-acquirer` flag,
	// "true" or absent. It is receive-only in practice — a sender that set
	// it would be asserting something about a message it is about to
	// send — and it is decoded because it is the one header field that says
	// whether anybody has seen the message before.
	HeaderFirstAcquirer = mq.ReservedHeaderPrefix + "amqp-first-acquirer"
	// HeaderKey carries Message.Key when it is NOT valid UTF-8.
	//
	// The specification makes a key BYTES and AMQP 1.0's `group-id` is a string,
	// so a key that is not valid UTF-8 has no property to live in. It used to
	// be refused outright, and the refusal is the wrong answer twice over: the
	// portable API promises a key round trip in every metadata mode, and
	// MetadataNativeStrict is the DEFAULT — so a caller whose keys are hashes
	// found the driver refusing every publication until they changed a mode
	// they had never set. An application property can carry binary, so the
	// key has a native home after all; this is it.
	//
	// The cost is stated rather than hidden: a key in a property is not a
	// message GROUP, so a broker that orders a group cannot order by it.
	// That is why the property is used only when `group-id` cannot hold the
	// key, and why the ordering claim's note says the group is what carries a
	// textual key.
	HeaderKey = mq.ReservedHeaderPrefix + "key"
)

// reservedHeaders is the set the mapping consumes, which is what makes a
// receive-then-publish round trip lossless.
var reservedHeaders = map[string]struct{}{
	HeaderTo:             {},
	HeaderSubject:        {},
	HeaderUserID:         {},
	HeaderAbsoluteExpiry: {},
	HeaderGroupSequence:  {},
	HeaderReplyToGroupID: {},
	HeaderCausationID:    {},
	HeaderFirstAcquirer:  {},
	HeaderKey:            {},
}

// ReservedHeader reports whether key is one of the reserved header names this
// driver maps to an AMQP property.
//
// It is exported so that a caller building a message can ask rather than guess:
// a key under the reserved prefix that is NOT one of these is refused before
// any I/O, and the two answers are worth telling apart.
func ReservedHeader(key string) bool {
	_, ok := reservedHeaders[key]
	return ok
}

// Mapper is the whole iqmq [github.com/iqhive/iqmq.Message] <-> AMQP 1.0
// message mapping, and nothing else.
//
// It is pure: it performs no I/O, reads no clock, touches no link and holds no
// lock. Every limit advertises is checked here, before anything is written,
// which is the specification's "enforced before I/O" and the only place a publication can be
// refused without the broker having seen it.
//
// A Mapper is built once per sink and once per source and is then read-only, so
// it is safe for concurrent use; the scratch storage an encode needs is the
// caller's ([Buf]), which is what keeps the mapping allocation-free on the
// publish path.
type Mapper struct {
	profile Profile
	mode    mq.MetadataMode
	env     *driverkit.Envelope
	limits  mq.Limits
	// durable is the AMQP header's `durable` flag every publication carries.
	// It is a configuration value rather than a per-message one: a
	// deployment publishes durable messages or it does not, and the specification makes
	// the confirmation boundary the per-publisher control.
	durable bool
}

// NewMapper returns the mapping for one sink or source.
//
// The envelope is [github.com/iqhive/iqmq/driverkit.Envelope], and it is what
// makes MetadataPortableEnvelope and MetadataAdaptive honest on this broker: an
// AMQP application-properties map cannot hold one key twice, so a message with
// a repeated header key has no native representation and the envelope is the
// only place its headers can go.
func NewMapper(p Profile, mode mq.MetadataMode, caps mq.Capabilities, durable bool) (*Mapper, error) {
	if !mode.Valid() {
		return nil, errMetadataMode
	}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Capabilities: caps})
	if err != nil {
		return nil, err
	}
	return &Mapper{profile: p, mode: mode, env: env, limits: caps.Limits(), durable: durable}, nil
}

// Mode is the metadata mode this mapping was built with.
func (m *Mapper) Mode() mq.MetadataMode { return m.mode }

// Buf is the scratch an encode is built in.
//
// It exists because rule 1 forbids a per-message allocation on a hot path
// and an AMQP message is four maps and a dozen optional pointers: the
// properties section takes *string and *time.Time, so a naive encode allocates
// once per populated property. A Buf holds the strings and the times the
// pointers point at, and the two maps, and is reset rather than rebuilt — so a
// sink that reuses one encodes a message with headers and properties with no
// allocation at all after the first.
//
// A Buf is not safe for concurrent use and belongs to whoever is encoding. The
// sink keeps a pool of them, because [github.com/iqhive/iqmq.Sink.Publish] may
// be called concurrently.
type Buf struct {
	msg   amqp.Message
	hdr   amqp.MessageHeader
	props amqp.MessageProperties
	data  [1][]byte

	appProps map[string]any
	annos    amqp.Annotations
	delAnnos amqp.Annotations

	// The storage the properties section's pointers point at.
	contentType string
	contentEnc  string
	replyTo     string
	to          string
	subject     string
	groupID     string
	replyGroup  string
	created     time.Time
	expiry      time.Time
	groupSeq    uint32

	// dropped counts the metadata MetadataLenientDrop left out of this
	// message, which the specification requires to be counted rather than silent.
	dropped int
}

// NewBuf returns an encode scratch with its maps allocated.
func NewBuf() *Buf {
	return &Buf{
		appProps: make(map[string]any, 8),
		annos:    make(amqp.Annotations, 4),
		delAnnos: make(amqp.Annotations, 2),
	}
}

// Reset empties the scratch while keeping its maps, which is what makes reuse
// free. Every field is cleared, so a reused Buf holds no reference to the
// message before it — a stale header from the previous publication would be a
// silent cross-message leak of exactly the kind exists to prevent.
func (b *Buf) Reset() {
	clear(b.appProps)
	clear(b.annos)
	clear(b.delAnnos)
	b.msg = amqp.Message{}
	b.hdr = amqp.MessageHeader{}
	b.props = amqp.MessageProperties{}
	b.data[0] = nil
	b.contentType, b.contentEnc, b.replyTo = "", "", ""
	b.to, b.subject, b.groupID, b.replyGroup = "", "", "", ""
	b.created, b.expiry, b.groupSeq = time.Time{}, time.Time{}, 0
	b.dropped = 0
}

// Dropped is how much metadata MetadataLenientDrop left out of the last encode.
func (b *Buf) Dropped() int { return b.dropped }

// Encode maps one iqmq message and its resolved publication options onto the
// AMQP message b holds, and returns it.
//
// The returned message aliases b and src: the body is src.Body itself, and
// every property points into b. That is legal because
// [github.com/iqhive/iqmq.Sink.Publish] does not retain src past the call and
// the sink holds b for the duration of the transfer; an asynchronous
// publication copies the body, because the library holds the message until its
// disposition.
//
// # The mapping
//
// The body is one `data` section for a non-nil Body and an `amqp-value` of null
// for a nil one, which is how the nil-versus-empty distinction
// [github.com/iqhive/iqmq.Message] promises survives: a nil Body is "no body"
// and an empty non-nil Body is "an empty body", and one `data` section of zero
// bytes is the second.
//
// Message.Key becomes the `group-id` property, and that is the one mapping
// worth arguing about. AMQP 1.0 has no partition key; what it has is a message
// GROUP, which is the thing brokers order and route by — Artemis's
// `JMSXGroupID` is this property, and a group is what makes
// FeatureOrderedByKey claimable at all. Putting the key anywhere else (an
// application property of iqmq's own) would make an iqmq publication unordered
// on a broker that orders groups, and would make a message published by a JMS
// client arrive with no key. The group's SEQUENCE has no portable field and
// travels as [HeaderGroupSequence].
//
// Headers become application properties, except those under
// [Profile.AnnotationPrefix], which become message annotations. A value that is
// valid UTF-8 travels as an AMQP string so that a non-iqmq consumer reads it;
// one that is not travels as an AMQP binary, which is a simple type and
// therefore allowed in application-properties, and comes back byte for byte. A
// repeated header KEY has no native representation, because
// application-properties is a map: strict refuses it, the envelope modes carry
// it in the envelope, and lenient drops the later values and counts them.
func (m *Mapper) Encode(b *Buf, src *mq.Message, o *mq.PublishOptions) (*amqp.Message, error) {
	if b == nil || src == nil {
		return nil, errNilMessage
	}
	b.Reset()
	if err := m.CheckPublish(src, o); err != nil {
		return nil, err
	}
	msg := src
	if m.envelopes(src) {
		wrapped, err := m.env.Wrap(src)
		if err != nil {
			return nil, err
		}
		msg = wrapped
	}
	out := &b.msg
	m.encodeBody(b, out, msg)
	if err := m.encodeHeader(b, out, o); err != nil {
		return nil, err
	}
	if err := m.encodeProperties(b, out, msg); err != nil {
		return nil, err
	}
	if err := m.encodeControls(b, out, o); err != nil {
		return nil, err
	}
	return out, nil
}

// envelopes reports whether this message is carried in the portable envelope
// rather than in native fields.
//
// MetadataAdaptive asks three questions and not one.
// [github.com/iqhive/iqmq/driverkit.Envelope.Needed] reads the capability
// table, which catches the repeated header key this broker cannot hold; it does
// not catch a header KEY that is not valid UTF-8, because no capability
// describes that — an AMQP application-property key is a string and a Go string
// can hold any bytes at all. So the driver's own representability check is the
// second question, and asking only the first is how a message the broker cannot
// carry gets published anyway.
//
// The third is about the BODY, and it is the one [Mapper.Decode] depends on.
// The adaptive read path is [github.com/iqhive/iqmq/driverkit.Envelope.Unwrap],
// which decides "is this an envelope" from the body, so a native body that
// begins like one -- `IQMQ` and a version byte, which is all
// [github.com/iqhive/iqmq/wire.Detect] reads -- is claimed as an envelope on
// the way back and refused when it does not decode. The kit's own
// [github.com/iqhive/iqmq/driverkit.Envelope.Wrap] envelopes such a body for
// exactly that reason , and this predicate used to decide
// BEFORE Wrap was asked, without the question, so the escape never ran: a
// payload of `IQMQ\x01\x02\x00\x02` was published natively and its own decoder
// refused it, which is a record that can be neither delivered nor settled
// FuzzAMQPMapping's seed accf8580a55f6015 is that message.
func (m *Mapper) envelopes(src *mq.Message) bool {
	switch m.mode {
	case mq.MetadataPortableEnvelope:
		return true
	case mq.MetadataAdaptive:
		return m.env.Needed(src) || m.representable(src) != nil || claimable(src.Body)
	case mq.MetadataNativeStrict, mq.MetadataLenientDrop:
		return false
	}
	return false
}

// claimable reports whether a body published natively would be read back as an
// envelope by the adaptive decoder. It is the kit's own publish-side question,
// asked with the same six-byte [github.com/iqhive/iqmq/wire.Detect] the kit
// asks it with, so the two halves cannot disagree.
func claimable(body []byte) bool {
	_, ok := wire.Detect(body)
	return ok
}

// encodeBody writes the body section.
func (m *Mapper) encodeBody(b *Buf, out *amqp.Message, src *mq.Message) {
	if src.Body == nil {
		// An AMQP message with no body section at all is legal and several
		// brokers refuse it, so "no body" is an explicit null value rather
		// than an omission.
		out.Value = amqp.Null{}
		return
	}
	b.data[0] = src.Body
	out.Data = b.data[:1]
}

// encodeHeader writes the AMQP message header: durability, priority and the
// relative time to live.
func (m *Mapper) encodeHeader(b *Buf, out *amqp.Message, o *mq.PublishOptions) error {
	b.hdr.Durable = m.durable
	if o != nil {
		if o.PrioritySet {
			b.hdr.Priority = o.Priority
		}
		if o.TTL > 0 {
			if o.TTL.Milliseconds() <= 0 {
				// A sub-millisecond time to live rounds to zero, and an AMQP
				// ttl of zero means "no expiry" rather than "expire at once":
				// honouring it would publish a message that never expires
				// after the caller asked for one that expires immediately.
				return errTTLTooSmall
			}
			b.hdr.TTL = o.TTL
		}
	}
	out.Header = &b.hdr
	return nil
}

// encodeProperties writes the properties section and the header set.
func (m *Mapper) encodeProperties(b *Buf, out *amqp.Message, src *mq.Message) error {
	p := &b.props
	if src.ID != "" {
		p.MessageID = src.ID
	}
	if src.Properties.CorrelationID != "" {
		p.CorrelationID = src.Properties.CorrelationID
	}
	if src.Properties.ContentType != "" {
		b.contentType = src.Properties.ContentType
		p.ContentType = &b.contentType
	}
	if src.Properties.ContentEncoding != "" {
		b.contentEnc = src.Properties.ContentEncoding
		p.ContentEncoding = &b.contentEnc
	}
	if src.Properties.ReplyTo != "" {
		b.replyTo = src.Properties.ReplyTo
		p.ReplyTo = &b.replyTo
	}
	if !src.Properties.CreatedAt.IsZero() {
		b.created = src.Properties.CreatedAt
		p.CreationTime = &b.created
	}
	if len(src.Key) > 0 {
		if utf8.Valid(src.Key) {
			b.groupID = string(src.Key)
			p.GroupID = &b.groupID
		} else {
			// See [HeaderKey]: a key `group-id` cannot hold travels as a
			// binary application property. The slice is the CALLER's and is
			// not copied, for the reason [Mapper.Encode] gives about the
			// body: the library encodes the property before the call
			// returns.
			b.appProps[HeaderKey] = src.Key
		}
	}
	if src.Properties.CausationID != "" {
		b.appProps[HeaderCausationID] = src.Properties.CausationID
	}
	if err := m.encodeHeaders(b, src); err != nil {
		return err
	}
	out.Properties = p
	if len(b.appProps) > 0 {
		out.ApplicationProperties = b.appProps
	}
	if len(b.annos) > 0 {
		out.Annotations = b.annos
	}
	if len(b.delAnnos) > 0 {
		out.DeliveryAnnotations = b.delAnnos
	}
	return nil
}

// encodeHeaders folds the header set into the properties, the annotations and
// the application properties.
//
// A repeated key cannot exist in either map. MetadataNativeStrict has already
// refused such a message in CheckPublish and the envelope modes never reach
// here with one, so the only mode that arrives is MetadataLenientDrop: the
// later value is left out and COUNTED, which is the specification's "dropped with a hook and
// a metric" rather than silently.
func (m *Mapper) encodeHeaders(b *Buf, src *mq.Message) error {
	for k, v := range src.Headers.All() {
		if ReservedHeader(k) {
			if err := m.encodeReserved(b, k, v); err != nil {
				return err
			}
			continue
		}
		val := headerValue(v)
		if m.profile.annotationHeader(k) {
			if _, dup := b.annos[k]; dup {
				b.dropped++
				continue
			}
			b.annos[k] = val
			continue
		}
		if _, dup := b.appProps[k]; dup {
			b.dropped++
			continue
		}
		b.appProps[k] = val
	}
	return nil
}

// headerValue is the AMQP value a header's bytes travel as.
//
// A value that is valid UTF-8 becomes a string, so that a consumer written
// against another AMQP client reads it as text; one that is not becomes a
// binary, which is a simple type and therefore allowed in
// application-properties, and comes back byte for byte. Both round trip
// exactly, which is what FuzzAMQPMapping holds.
func headerValue(v []byte) any {
	if v == nil {
		// A nil value and an empty one are distinct in iqmq's header set, and
		// an AMQP null is the only simple type that can carry the difference.
		return amqp.Null{}
	}
	if utf8.Valid(v) {
		return string(v)
	}
	return append([]byte(nil), v...)
}

// encodeReserved writes one of the reserved headers into the property it stands
// for.
func (m *Mapper) encodeReserved(b *Buf, key string, val []byte) error {
	switch key {
	case HeaderTo:
		b.to = string(val)
		b.props.To = &b.to
	case HeaderSubject:
		b.subject = string(val)
		b.props.Subject = &b.subject
	case HeaderUserID:
		b.props.UserID = append([]byte(nil), val...)
	case HeaderReplyToGroupID:
		b.replyGroup = string(val)
		b.props.ReplyToGroupID = &b.replyGroup
	case HeaderCausationID:
		b.appProps[HeaderCausationID] = string(val)
	case HeaderFirstAcquirer:
		b.hdr.FirstAcquirer = string(val) == "true"
	case HeaderAbsoluteExpiry:
		t, err := time.Parse(time.RFC3339Nano, string(val))
		if err != nil {
			return errBadExpiryHeader
		}
		b.expiry = t
		b.props.AbsoluteExpiryTime = &b.expiry
	case HeaderGroupSequence:
		n, err := strconv.ParseUint(string(val), 10, 32)
		if err != nil {
			return errBadGroupSequence
		}
		b.groupSeq = uint32(n)
		b.props.GroupSequence = &b.groupSeq
	default:
		return errReservedHeaderName
	}
	return nil
}

// encodeControls writes the per-message publication controls the profile
// expresses as annotations or application properties.
func (m *Mapper) encodeControls(b *Buf, out *amqp.Message, o *mq.PublishOptions) error {
	if o == nil {
		return nil
	}
	if o.Delay > 0 || !o.DeliverAt.IsZero() {
		v := m.profile.Delay.encode(o.Delay, o.DeliverAt)
		if err := m.put(b, out, m.profile.Delay.Slot, m.profile.Delay.Key, v); err != nil {
			return err
		}
	}
	if o.DedupKey != "" {
		if err := m.put(b, out, m.profile.Dedup.Slot, m.profile.Dedup.Key, o.DedupKey); err != nil {
			return err
		}
	}
	return nil
}

// put writes one value into the section a profile rule names.
func (m *Mapper) put(b *Buf, out *amqp.Message, slot Slot, key string, v any) error {
	switch slot {
	case SlotMessageAnnotation:
		b.annos[key] = v
		out.Annotations = b.annos
	case SlotDeliveryAnnotation:
		b.delAnnos[key] = v
		out.DeliveryAnnotations = b.delAnnos
	case SlotApplicationProperty:
		b.appProps[key] = v
		out.ApplicationProperties = b.appProps
	case SlotNone, slotCount:
		return errUnsupportedControl
	default:
		return errUnsupportedControl
	}
	return nil
}

// CheckPublish is "enforced before I/O": everything this broker and this
// profile cannot honour is refused here, before a byte is written.
//
// The order is fixed so that one message always produces one verdict: the
// controls first, because a caller who asked for a delay this broker cannot
// express wants to hear about the delay rather than about a header; then the
// size limits, because a payload over the peer's ceiling is refused whatever
// its metadata; then the metadata mode's own rule.
func (m *Mapper) CheckPublish(src *mq.Message, o *mq.PublishOptions) error {
	if src == nil {
		return errNilMessage
	}
	if o != nil {
		switch {
		case o.Delay > 0 && !o.DeliverAt.IsZero():
			return errDelayAndDeliverAt
		case (o.Delay > 0 || !o.DeliverAt.IsZero()) && !m.profile.Delay.Supported():
			return errNoDelay
		case o.Delay > 0 && m.profile.Delay.Max > 0 && o.Delay > m.profile.Delay.Max:
			return errDelayTooLong
		case o.DedupKey != "" && !m.profile.Dedup.Supported():
			return errNoDedup
		case o.PartitionSet:
			// AMQP 1.0 has no partitions. A message GROUP is the closest
			// thing and it is named rather than numbered, which is what
			// Message.Key reaches.
			return errNoPartition
		}
	}
	if lim := m.limits.MaxMessageBytes; lim.Known && int64(len(src.Body)) > lim.Value {
		return mq.ErrMessageTooLarge
	}
	if err := src.Headers.Validate(m.limits, false); err != nil && !onlyReservedHeaders(src, err) {
		return err
	}
	switch m.mode {
	case mq.MetadataNativeStrict:
		return m.representable(src)
	case mq.MetadataPortableEnvelope, mq.MetadataAdaptive:
		// The envelope is what carries what the native fields cannot, and
		// Envelope.Check is what answers "will what I am about to publish
		// read back?" before any I/O. A record the decoder refuses can be
		// neither delivered nor settled, which is the disqualifier
		// reached through a record the driver made itself.
		if m.envelopes(src) {
			return m.env.Check(src)
		}
		return m.representable(src)
	case mq.MetadataLenientDrop:
		// Lenient drops what it cannot carry, so the only refusals left are
		// the ones that are not about metadata at all: a header key that is
		// not valid UTF-8 cannot be an AMQP property name, so there is
		// nowhere to put it and nothing to drop it in favour of.
		return m.keysValid(src)
	}
	return errMetadataMode
}

// onlyReservedHeaders reports whether a header-set refusal was ONLY about
// reserved names this driver carries rather than refuses.
//
// [github.com/iqhive/iqmq.Headers.Validate] refuses every key under the specification's
// reserved prefix, which is right for a caller and wrong for a message this
// driver decoded: a received delivery carries the reserved names the decode
// wrote, and refusing to publish it again would make a forwarding consumer
// impossible. So the reserved-prefix half of the verdict is re-decided here
// against the names this driver really carries, and every other refusal — a
// count or a byte ceiling, an invalid key — stands.
func onlyReservedHeaders(src *mq.Message, err error) bool {
	if mq.CodeOf(err) != mq.CodeInvalidArgument {
		return false
	}
	for k := range src.Headers.All() {
		if !carriedReservedHeader(k) && hasReservedPrefix(k) {
			return false
		}
		if !utf8.ValidString(k) {
			return false
		}
	}
	return true
}

// carriedReservedHeader reports whether a key under the reserved prefix is
// one this driver carries rather than refuses: one of the names the mapping
// maps to an AMQP property, or one of the five dead-letter provenance keys.
//
// The two are kept apart because they travel differently. A [ReservedHeader] is
// consumed into an AMQP property by encodeReserved and never reaches an
// application property; a provenance key is iqmq's own metadata and travels as
// an ordinary application property, which is what makes it readable to whoever
// operates the dead-letter queue. Merging them would send a provenance key to
// encodeReserved, whose default is errReservedHeaderName — the very refusal
// this repairs.
func carriedReservedHeader(k string) bool {
	return ReservedHeader(k) || driverkit.ReservedHeaderAllowed(k)
}

// hasReservedPrefix reports whether a key is under the reserved prefix.
func hasReservedPrefix(k string) bool {
	return len(k) >= len(mq.ReservedHeaderPrefix) && k[:len(mq.ReservedHeaderPrefix)] == mq.ReservedHeaderPrefix
}

// representable reports whether the message maps to native AMQP 1.0 fields with
// nothing lost, which is MetadataNativeStrict's whole rule.
//
// It is exported through [Representable] so that a caller can ask before
// publishing, and it is also what MetadataAdaptive consults in order to decide
// whether the envelope is needed.
func (m *Mapper) representable(src *mq.Message) error {
	if err := m.keysValid(src); err != nil {
		return err
	}
	// A key that is not valid UTF-8 is representable: `group-id` cannot hold
	// it and the binary application property [HeaderKey] can, so nothing is
	// lost and MetadataNativeStrict has nothing to refuse. What it loses is
	// the message GROUP, which is a routing property rather than a field of
	// the message, and the capability note is where that is said.
	seen := make(map[string]struct{}, src.Headers.Len())
	for k := range src.Headers.All() {
		if ReservedHeader(k) {
			continue
		}
		if _, dup := seen[k]; dup {
			return errDuplicateHeader
		}
		seen[k] = struct{}{}
	}
	return nil
}

// keysValid refuses a header key no AMQP section can name.
func (m *Mapper) keysValid(src *mq.Message) error {
	for k := range src.Headers.All() {
		if k == "" {
			return errEmptyHeaderKey
		}
		if !utf8.ValidString(k) {
			return errBinaryHeaderKey
		}
		if hasReservedPrefix(k) && !carriedReservedHeader(k) {
			return errReservedHeaderName
		}
	}
	return nil
}

// Representable reports whether m maps to native AMQP 1.0 fields with nothing
// lost, under p's naming rules.
//
// It is the question MetadataNativeStrict answers for itself and a caller may
// want to ask first: a message with two headers under one key, or a key that is
// not valid UTF-8, has no native representation on this protocol and needs
// MetadataPortableEnvelope or MetadataAdaptive.
func Representable(p Profile, msg *mq.Message) error {
	if msg == nil {
		return errNilMessage
	}
	mp := &Mapper{profile: p}
	return mp.representable(msg)
}

// Decode maps one AMQP message onto dst.
//
// dst's header storage is reused: [github.com/iqhive/iqmq.Headers.Reset] keeps
// the backing array, so a source that decodes into per-slot storage pays for
// the header slice once rather than once per delivery. What it cannot avoid is
// one allocation per string-valued application property, because iqmq's header
// value is a []byte and converting a string to one copies — and the alternative
// is unsafe, which rule 8 confines to two packages that are not this one.
//
// A body this driver cannot represent is a REFUSAL and not a silent drop. The specification
// forbids dropping metadata silently, and an AMQP `amqp-sequence` body or an
// `amqp-value` of a map has no portable form at all; the source releases such a
// delivery with `modified` rather than accepting it, so the message is not lost
// and is not handed to a caller that cannot read it (see source.go).
func (m *Mapper) Decode(dst *mq.Message, src *amqp.Message) (lossy int, err error) {
	if dst == nil || src == nil {
		return 0, errNilMessage
	}
	topic := dst.Topic
	dst.Key = nil
	dst.Body = nil
	dst.ID = ""
	dst.Properties = mq.Properties{}
	dst.Headers.Reset()
	dst.Receipt = nil
	dst.Topic = topic
	if err := decodeBody(dst, src); err != nil {
		return 0, err
	}
	lossy = decodeProperties(dst, src)
	decodeHeader(dst, src)
	lossy += decodeMaps(dst, src)
	if m.mode == mq.MetadataPortableEnvelope || m.mode == mq.MetadataAdaptive {
		var out mq.Message
		if err := m.env.Unwrap(&out, dst); err != nil {
			return lossy, err
		}
		out.Topic = topic
		*dst = out
	}
	return lossy, nil
}

// emptyBody is the shared zero-length body a message with an empty `data`
// section decodes to. It keeps the nil-versus-empty distinction without an
// allocation: a nil Body is "no body" and this is "an empty body".
var emptyBody = []byte{}

// decodeBody reads the body section.
func decodeBody(dst *mq.Message, src *amqp.Message) error {
	switch {
	case len(src.Sequence) > 0:
		// An amqp-sequence body is a list of lists of AMQP values. iqmq's
		// Message has bytes, so there is nowhere for it to go and nothing
		// honest to put there.
		return errUnrepresentableBody
	case len(src.Data) == 1:
		if src.Data[0] == nil {
			dst.Body = emptyBody
			return nil
		}
		dst.Body = src.Data[0]
		return nil
	case len(src.Data) > 1:
		// Several data sections are one body, so they are joined — which is
		// the one place decode allocates a payload. A sender that split its
		// body is rare and a receiver that dropped the tail would be losing
		// the message's content.
		n := 0
		for _, d := range src.Data {
			n += len(d)
		}
		out := make([]byte, 0, n)
		for _, d := range src.Data {
			out = append(out, d...)
		}
		dst.Body = out
		return nil
	case src.Value == nil:
		dst.Body = nil
		return nil
	}
	switch v := src.Value.(type) {
	case amqp.Null:
		dst.Body = nil
	case *amqp.Null:
		dst.Body = nil
	case []byte:
		if v == nil {
			dst.Body = emptyBody
			return nil
		}
		dst.Body = v
	case string:
		dst.Body = []byte(v)
	default:
		return errUnrepresentableBody
	}
	return nil
}

// decodeProperties reads the properties section into the portable fields and
// the reserved headers, and returns how many properties it could not carry.
func decodeProperties(dst *mq.Message, src *amqp.Message) (lossy int) {
	p := src.Properties
	if p == nil {
		return 0
	}
	if s, ok := stringOf(p.MessageID); ok {
		dst.ID = s
	} else if p.MessageID != nil {
		lossy++
	}
	if s, ok := stringOf(p.CorrelationID); ok {
		dst.Properties.CorrelationID = s
	} else if p.CorrelationID != nil {
		lossy++
	}
	if p.ContentType != nil {
		dst.Properties.ContentType = *p.ContentType
	}
	if p.ContentEncoding != nil {
		dst.Properties.ContentEncoding = *p.ContentEncoding
	}
	if p.ReplyTo != nil {
		dst.Properties.ReplyTo = *p.ReplyTo
	}
	if p.CreationTime != nil {
		dst.Properties.CreatedAt = *p.CreationTime
	}
	if p.GroupID != nil && *p.GroupID != "" {
		dst.Key = []byte(*p.GroupID)
	}
	// [HeaderKey] wins over `group-id` when both are there: a message carrying
	// the property was published by this driver with a key `group-id` could
	// not hold, and an intermediary that filled in a group-id of its own must
	// not overwrite the caller's key.
	if p.To != nil {
		dst.Headers.Add(HeaderTo, []byte(*p.To))
	}
	if p.Subject != nil {
		dst.Headers.Add(HeaderSubject, []byte(*p.Subject))
	}
	if len(p.UserID) > 0 {
		dst.Headers.Add(HeaderUserID, p.UserID)
	}
	if p.AbsoluteExpiryTime != nil {
		dst.Headers.Add(HeaderAbsoluteExpiry, []byte(p.AbsoluteExpiryTime.Format(time.RFC3339Nano)))
	}
	if p.GroupSequence != nil {
		dst.Headers.Add(HeaderGroupSequence, strconv.AppendUint(nil, uint64(*p.GroupSequence), 10))
	}
	if p.ReplyToGroupID != nil {
		dst.Headers.Add(HeaderReplyToGroupID, []byte(*p.ReplyToGroupID))
	}
	return lossy
}

// decodeHeader reads the AMQP message header.
//
// Only `first-acquirer` reaches the header set. The durable flag, the priority
// and the time to live are publication CONTROLS rather than metadata — the specification
// gives each of them a PublishOption — and the delivery count is the attempt
// number, which the source reads through the profile's own rule. A
// decode that put all five into the header set would hand a caller five headers
// it did not publish, and a forwarding consumer would then publish them as
// application properties.
func decodeHeader(dst *mq.Message, src *amqp.Message) {
	if src.Header != nil && src.Header.FirstAcquirer {
		dst.Headers.Add(HeaderFirstAcquirer, trueBytes)
	}
}

// trueBytes is the one header value this driver writes from a flag.
var trueBytes = []byte("true")

// decodeMaps reads the application properties and the annotations into the
// header set.
//
// The annotations come SECOND, so that a broker annotation and an application
// property of the same name arrive in a stable order — and a caller that
// re-publishes the message puts each back where it came from, because the
// profile's annotation prefix is what decides. It returns how many entries it
// could not carry.
func decodeMaps(dst *mq.Message, src *amqp.Message) (lossy int) {
	for k, v := range src.ApplicationProperties {
		if k == HeaderCausationID {
			if s, ok := v.(string); ok {
				dst.Properties.CausationID = s
				continue
			}
		}
		if k == HeaderKey {
			// See [HeaderKey]: the key of a message whose bytes `group-id`
			// could not hold. It WINS over the group-id read above, and it is
			// not added to the header set — a reserved name that reached the
			// caller's headers would be a header no application could have
			// set.
			if b, ok := valueBytes(v); ok {
				dst.Key = b
				continue
			}
		}
		b, ok := valueBytes(v)
		if !ok {
			lossy++
			continue
		}
		dst.Headers.Add(k, b)
	}
	for k, v := range src.Annotations {
		key, ok := annotationKey(k)
		if !ok {
			// An annotation key that is neither a symbol nor a string is
			// legal AMQP — the key may be a ulong — and has no portable
			// header name. Rendering it would invent one, so it is counted
			// rather than named.
			lossy++
			continue
		}
		b, ok := valueBytes(v)
		if !ok {
			lossy++
			continue
		}
		dst.Headers.Add(key, b)
	}
	return lossy
}

// annotationKey renders an annotation's key as a header name.
//
// An AMQP annotation key is a symbol or a ulong, and go-amqp decodes a symbol
// into either [github.com/Azure/go-amqp.Symbol] or a string depending on how
// the peer encoded it, so both are accepted. A ulong key is legal and has no
// portable name.
func annotationKey(k any) (string, bool) {
	switch x := k.(type) {
	case string:
		return x, x != ""
	case amqp.Symbol:
		return string(x), x != ""
	default:
		return "", false
	}
}

// valueBytes renders an AMQP simple value as header bytes, reporting whether it
// could be rendered at all.
//
// The three that round trip exactly are the three this driver writes: a string,
// a binary and a null. The rest are values some other producer chose, and they
// are rendered rather than dropped — a caller that can see "42" is better
// served than one whose header vanished — but the rendering is not reversible,
// which the driver page states.
//
// A value this function cannot render is COUNTED rather than turned into an
// empty header: the specification forbids dropping metadata silently, and an empty header
// would be indistinguishable from an AMQP null, which is a value a producer may
// really have sent. Application-properties admits only simple types, so a
// conforming peer never produces one; a non-conforming one is why the count
// exists.
func valueBytes(v any) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		return nil, true
	case amqp.Null:
		return nil, true
	case *amqp.Null:
		return nil, true
	case string:
		return []byte(x), true
	case []byte:
		return x, true
	case bool:
		if x {
			return trueBytes, true
		}
		return falseBytes, true
	case int8:
		return strconv.AppendInt(nil, int64(x), 10), true
	case int16:
		return strconv.AppendInt(nil, int64(x), 10), true
	case int32:
		return strconv.AppendInt(nil, int64(x), 10), true
	case int64:
		return strconv.AppendInt(nil, x, 10), true
	case int:
		return strconv.AppendInt(nil, int64(x), 10), true
	case uint8:
		return strconv.AppendUint(nil, uint64(x), 10), true
	case uint16:
		return strconv.AppendUint(nil, uint64(x), 10), true
	case uint32:
		return strconv.AppendUint(nil, uint64(x), 10), true
	case uint64:
		return strconv.AppendUint(nil, x, 10), true
	case uint:
		return strconv.AppendUint(nil, uint64(x), 10), true
	case float32:
		return strconv.AppendFloat(nil, float64(x), 'g', -1, 32), true
	case float64:
		return strconv.AppendFloat(nil, x, 'g', -1, 64), true
	case amqp.Symbol:
		return []byte(x), true
	case time.Time:
		return []byte(x.Format(time.RFC3339Nano)), true
	case amqp.UUID:
		return []byte(uuidString(x)), true
	default:
		return nil, false
	}
}

// falseBytes is trueBytes's other half.
var falseBytes = []byte("false")

// uuidString renders an AMQP UUID in the canonical form.
func uuidString(u amqp.UUID) string {
	const hexDigits = "0123456789abcdef"
	var out [36]byte
	j := 0
	for i, b := range u {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			out[j] = '-'
			j++
		}
		out[j] = hexDigits[b>>4]
		out[j+1] = hexDigits[b&0x0f]
		j += 2
	}
	return string(out[:])
}

// stringOf renders an AMQP identifier as a string.
//
// AMQP's message-id and correlation-id may be a string, a binary, a ulong or a
// UUID, and iqmq's are strings. A ulong and a UUID are rendered in their
// canonical forms, which is reversible for both; a binary is rendered as its
// bytes, which is reversible only when it is valid UTF-8 and is the one lossy
// case — stated on the driver page rather than hidden.
func stringOf(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, x != ""
	case []byte:
		return string(x), len(x) > 0
	case amqp.UUID:
		return uuidString(x), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	default:
		return "", false
	}
}

// The mapping's refusals. The specification names no sentinel for most of them and
// errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeUnsupported, mq.CodeInvalidArgument or mq.CodeCorruptMessage.
var (
	errNilMessage = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the message is nil")
	errMetadataMode = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the metadata mode is not one of the four")
	errDelayAndDeliverAt = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: Delay and DeliverAt name the same control twice")
	errNoDelay = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: this profile has no way to express a delayed publication; AMQP 1.0 has no standard scheduling annotation, so the broker-specific profile names one (Profile.Delay) or refuses")
	errDelayTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the delay is longer than this broker accepts (Profile.Delay.Max)")
	errNoDedup = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: this profile has no way to express a de-duplication key (Profile.Dedup)")
	errNoPartition = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: AMQP 1.0 has no partitions; a message group is the nearest thing and it is named rather than numbered, which Message.Key reaches as the group-id property")
	errTTLTooSmall = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the time to live rounds to zero milliseconds, and an AMQP ttl of zero means no expiry rather than immediate expiry")
	errDuplicateHeader = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: AMQP application-properties is a map, so one header key cannot carry two values; use MetadataPortableEnvelope or MetadataAdaptive")
	errEmptyHeaderKey = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: an AMQP application-property name cannot be empty")
	errBinaryHeaderKey = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: an AMQP application-property name is a string, so a header key that is not valid UTF-8 has nowhere to go")
	errReservedHeaderName = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the header key uses the reserved iqmq- prefix and is not one of the names this driver maps to an AMQP property (see ReservedHeader)")
	errBadExpiryHeader = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the "+HeaderAbsoluteExpiry+" header is not an RFC 3339 instant")
	errBadGroupSequence = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: the "+HeaderGroupSequence+" header is not a 32-bit unsigned decimal")
	errUnsupportedControl = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: the profile rule names no part of the message to write the control to")
	errUnrepresentableBody = errcode.New(errcode.CorruptMessage,
		"iqmq/amqp10: the message body is an amqp-sequence or an amqp-value this driver cannot represent as bytes; iqmq's Message carries a byte payload")
	errIncompleteTable = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: the capability table does not mention every feature of the inventory")
	errUnknownFeature = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: a profile claim names a feature that is not in the inventory")
	errBadClaim = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claim names a support level or a scope outside the enumerations")
	errOverclaim = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claims a feature more strongly than this core can honour; see CoreCeiling")
	errClaimNeedsAdmin = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claims FeatureAdmin and supplies no Profile.Admin, so Client.Admin would answer ErrUnsupported for a capability the table advertises")
	errClaimNeedsDurable = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claims FeatureDurableConfirm and defines no durable boundary (Profile.Durable)")
	errClaimNeedsDelay = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claims FeatureScheduledPublish and names no way to express a delay (Profile.Delay)")
	errClaimNeedsNackDelay = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claims FeatureDelayedRedelivery and names no way to express one (Profile.NackDelay)")
	errClaimNeedsDedup = errcode.New(errcode.InvalidConfig,
		"iqmq/amqp10: a profile claims de-duplication and names no key to carry it (Profile.Dedup)")
)

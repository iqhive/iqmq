package gcppubsub

import (
	"encoding/base64"
	"sync/atomic"
	"time"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The reserved attribute names.
//
// The specification reserves the "iqmq-" prefix "with conflicts rejected", and these are
// the names this driver spends it on. Every one of them is optional and
// present only when the field it carries is set, which is what makes the
// hundred-attribute budget mostly the caller's: a message with a key, an
// identifier and one property spends three and leaves ninety-six for headers.
const (
	attrKey      = "iqmq-k"
	attrKeyEnc   = "iqmq-kenc"
	attrID       = "iqmq-id"
	attrEncoding = "iqmq-enc"
	attrEnvelope = "iqmq-env"
	attrType     = "iqmq-ct"
	attrEncodeAs = "iqmq-ce"
	attrCorrID   = "iqmq-cid"
	attrCauseID  = "iqmq-cause"
	attrReplyTo  = "iqmq-reply"
	attrCreated  = "iqmq-ts"
)

// reservedNames is every name above, which is what a caller's header is
// refused for colliding with, and whose count is subtracted from the
// advertised MaxHeaderCount.
var reservedNames = [...]string{
	attrKey, attrKeyEnc, attrID, attrEncoding, attrEnvelope,
	attrType, attrEncodeAs, attrCorrID, attrCauseID, attrReplyTo, attrCreated,
}

// The body and key encodings, carried in [attrEncoding] and [attrKeyEnc].
//
// They exist for one reason: the specification distinguishes a nil body from an empty one
// and a nil key from an empty one, and protobuf does not. An absent `bytes`
// field and a present empty one decode to the same nil slice, so a driver with
// no marker reports every empty body as nil and fails `publish/roundtrip-bodies`
// — which is the scenario that exists to catch exactly this.
const (
	// encNil says the value was nil.
	encNil = "nil"
	// encEmpty says the value was an empty non-nil slice.
	encEmpty = "empty"
	// encBase64 says the attribute holds the base64 of a value that is not
	// valid UTF-8. It is only ever used for the KEY: a header value that is
	// not text takes the envelope instead, because base64-encoding one
	// silently would change what a non-iqmq consumer reads.
	encBase64 = "b64"
)

// envelopeMarker is [attrEnvelope]'s value on a message whose body is an iqmq
// envelope. It is diagnostics rather than protocol — the decoder detects an
// envelope from the body's own magic — and it is what makes a message readable
// as "this is not the payload you are looking for" from the console.
const envelopeMarker = "1"

// encoded is one publication in the shape the service takes it.
//
// It is built by [encoder.encode], which is pure: no clock, no I/O, and every
// limit checked before anything is sent. The specification asks for "limits before I/O",
// and this is where that happens.
type encoded struct {
	// Out is what goes to the client library.
	Out psapi.Outgoing
	// Enveloped reports that Out.Data is an iqmq envelope rather than the
	// caller's payload.
	Enveloped bool
	// Bytes is the accounted size: the body, plus every attribute name and
	// value, plus the ordering key — which is what the service's
	// ten-megabyte limit counts.
	Bytes int
}

// encoder turns a message into the service's shape and back.
//
// It holds TWO envelopes rather than one, for the reason `drivers/sqs`
// recorded. [driverkit.Envelope] decides whether
// MetadataAdaptive has to wrap by reading the CAPABILITY TABLE — binary
// values, duplicate keys, headers at all — and this driver has two reasons the
// table cannot express: a message needing more than a hundred attributes, and
// a header key that is not a legal attribute name. So `env` carries the
// configured mode and answers the mode questions, and `forced` is a second
// envelope in MetadataPortableEnvelope mode whose Wrap always wraps, which is
// what this driver reaches for when its own reason fires.
type encoder struct {
	cfg   Config
	env   *driverkit.Envelope
	force *driverkit.Envelope
	mode  mq.MetadataMode
	// budget is how many attributes a caller's headers may spend, which is
	// the service's hundred less the reserved names this message needs.
	drops atomic.Int64
}

// newEncoder builds the mapping for one publisher or consumer.
func newEncoder(cfg Config, caps mq.Capabilities, mode mq.MetadataMode) (*encoder, error) {
	if !mode.Valid() {
		mode = mq.MetadataNativeStrict
	}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Capabilities: caps})
	if err != nil {
		return nil, err
	}
	force, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode: mq.MetadataPortableEnvelope, Capabilities: caps,
	})
	if err != nil {
		return nil, err
	}
	return &encoder{cfg: cfg, env: env, force: force, mode: mode}, nil
}

// Dropped is how many headers MetadataLenientDrop has discarded, which is the metric.
func (e *encoder) Dropped() int64 { return e.drops.Load() }

// encode turns one message and its resolved publication options into the
// service's shape, refusing everything that can be refused before any I/O.
func (e *encoder) encode(m *mq.Message, o *mq.PublishOptions) (encoded, error) {
	if m == nil {
		return encoded{}, errNilMessage
	}
	if err := e.checkOptions(o); err != nil {
		return encoded{}, err
	}
	if err := e.admit(m); err != nil {
		return encoded{}, err
	}
	msg, enveloped, err := e.wrap(m)
	if err != nil {
		return encoded{}, err
	}
	out := encoded{Enveloped: enveloped}
	attrs := make(map[string]string, 8)
	out.Out.Data = msg.Body
	switch {
	case msg.Body == nil:
		attrs[attrEncoding] = encNil
	case len(msg.Body) == 0:
		attrs[attrEncoding] = encEmpty
	}
	// An enveloped message carries its key, identifier, headers and
	// properties INSIDE the envelope, so putting them beside it as well would
	// produce a message that decodes to something different from what was
	// published. driverkit.Envelope.Wrap already returns a message with none
	// of them; this is what stops the metadata being added back.
	if enveloped {
		attrs[attrEnvelope] = envelopeMarker
	} else if err := e.encodeMetadata(m, attrs); err != nil {
		return encoded{}, err
	}
	key, err := e.orderingKey(m)
	if err != nil {
		return encoded{}, err
	}
	out.Out.OrderingKey = key
	out.Out.Attributes = attrs
	out.Bytes = accountedSize(&out.Out)
	if out.Bytes > MaxMessageBytes {
		return encoded{}, errMessageTooLarge
	}
	if len(attrs) > MaxAttributes {
		return encoded{}, errTooManyAttributes
	}
	return out, nil
}

// admit refuses what NEITHER half of this driver can carry, in every metadata
// mode.
//
// The interesting case is a header key longer than 256 bytes, and it is
// interesting because MetadataAdaptive is NOT an answer to it. The service's
// attribute-name limit is 256 bytes and the envelope decoder's MaxHeaderKey is
// also 256, so a longer key has no representation in either half — and a
// driver that enveloped it anyway would publish a record its own Unwrap
// refuses for ever: undeliverable, therefore unsettleable, which is the specification's
// disqualifier reached through a record the driver made out of a publication
// it accepted. That is the hole and the specification record, and
// [driverkit.Envelope.Check] is the affordance they added for it; this
// function is where a Pub/Sub publication meets it.
//
// The bound is asked of the DECODER rather than copied from it, because a copy
// is the same defect one layer out: the specification recorded a driver whose hand-written
// MaxHeaderKey was three bytes over the decoder's.
//
// A reserved collision is refused in every mode too, because it is not
// something an envelope fixes: the caller named one of this driver's own
// attributes and the specification says that is rejected.
//
// MetadataLenientDrop is the exception and only for the HEADERS: the mode's
// promise is that the message is published with whatever fits, so
// [encoder.encodeHeaders] drops the offending header and counts it. The key,
// the identifier and the typed properties are NOT headers, so there is nothing
// to drop for them and they are bounded in every mode — which is also what
// makes a message accepted in one mode accepted in all of them. The mode never
// envelopes, so for it that bound is the service's attribute-value limit as
// well as the envelope's, the same one MetadataNativeStrict holds them to.
func (e *encoder) admit(m *mq.Message) error {
	for k := range m.Headers.All() {
		if reservedName(k) {
			return errReservedAttribute
		}
	}
	if e.mode == mq.MetadataLenientDrop {
		// The same decoder, asked about the fields the mode cannot drop. A
		// non-UTF-8 identifier is the case FuzzPubSubAttributes found in a
		// third of a second: Message.ID is a Go string and a Pub/Sub
		// attribute value is a STRING, so an identifier that is not valid
		// UTF-8 has no representation on either side.
		if err := e.force.Check(&mq.Message{
			Topic: m.Topic, Key: m.Key, Body: m.Body,
			ID: m.ID, Properties: m.Properties,
		}); err != nil {
			return err
		}
		// The mode never envelopes, so these fields travel as attribute
		// values or not at all, and the service refuses the whole
		// publication for one value over its limit.
		if e.valueTooLong(m) {
			return errMetadataValueTooLong
		}
		return nil
	}
	for k := range m.Headers.All() {
		if legalAttributeName(k) {
			continue
		}
		if !utf8.ValidString(k) {
			return errAttributeKeyInvalid
		}
		return errAttributeKeyTooLong
	}
	return e.force.Check(m)
}

// wrap applies the mode, returning the message to publish and whether it is
// an envelope.
func (e *encoder) wrap(m *mq.Message) (*mq.Message, bool, error) {
	needs := e.needsEnvelope(m)
	switch {
	case e.mode == mq.MetadataPortableEnvelope, needs && e.mode == mq.MetadataAdaptive:
		// force always wraps, which is what this driver's OWN triggers need:
		// driverkit.Envelope decides from the capability table, and a message
		// that needs more attributes than the service carries is not
		// something the table can express.
		wrapped, err := e.force.Wrap(m)
		if err != nil {
			return nil, false, err
		}
		return wrapped, true, nil
	case needs && e.mode == mq.MetadataNativeStrict:
		if e.attributesNeeded(m) > MaxAttributes {
			return nil, false, errTooManyAttributes
		}
		return nil, false, errEnvelopeNeeded
	default:
		return m, false, nil
	}
}

// needsEnvelope reports whether the message has metadata the ATTRIBUTES cannot
// carry and the envelope can.
//
// The capability table expresses three of these — a binary header value, a
// repeated header key, headers at all — and [driverkit.Envelope.Needed] reads
// it for them. Two more are this driver's own and have no field in the table:
// a message that needs more than the service's hundred attributes, and a value
// (or a key) longer than the service's 1024-byte attribute-value limit but
// inside the envelope's own 64 KiB. So this predicate is the union, and it is
// the reason the encoder holds two envelopes rather than one.
func (e *encoder) needsEnvelope(m *mq.Message) bool {
	if e.attributesNeeded(m) > MaxAttributes || e.valueTooLong(m) {
		return true
	}
	seen := make(map[string]struct{}, m.Headers.Len())
	for k, v := range m.Headers.All() {
		if len(v) > MaxAttributeValueBytes || !utf8.Valid(v) {
			return true
		}
		if _, dup := seen[k]; dup {
			return true
		}
		seen[k] = struct{}{}
	}
	return false
}

// valueTooLong reports whether the key, the identifier or a typed property is
// longer than the service's 1024-byte attribute-value limit.
func (e *encoder) valueTooLong(m *mq.Message) bool {
	if len(m.Key) > 0 && len(e.keyAttribute(m.Key)) > MaxAttributeValueBytes {
		return true
	}
	// The identifier and the typed properties are carried as attribute
	// VALUES, so the service's 1024 bytes bound them too — and the envelope's
	// own limits are wider (256 for the identifier, 4 KiB for a property),
	// which is why a long property is an envelope trigger and a long
	// identifier is not.
	if len(m.ID) > MaxAttributeValueBytes {
		return true
	}
	p := &m.Properties
	for _, v := range [...]string{
		p.ContentType, p.ContentEncoding, p.CorrelationID,
		p.CausationID, p.ReplyTo,
	} {
		if len(v) > MaxAttributeValueBytes {
			return true
		}
	}
	return false
}

// keyAttribute is the attribute value the key would be carried as, which is
// the key itself when it is text and its base64 otherwise.
func (e *encoder) keyAttribute(key []byte) string {
	if utf8.Valid(key) {
		return string(key)
	}
	return base64.StdEncoding.EncodeToString(key)
}

// checkOptions refuses every publication control this broker cannot honour,
// before any I/O, which is what the specification requires of an option a driver cannot
// keep.
func (e *encoder) checkOptions(o *mq.PublishOptions) error {
	if o == nil {
		return nil
	}
	if o.Delay > 0 || !o.DeliverAt.IsZero() {
		return errNoScheduledPublish
	}
	if o.TTL > 0 {
		return errNoTTL
	}
	if o.PrioritySet {
		return errNoPriority
	}
	if o.PartitionSet {
		return errNoPartition
	}
	if o.DedupKey != "" {
		return errNoDedup
	}
	return nil
}

// orderingKey is the publication's ordering key.
//
// A key with no ordering configured is NOT an error: Message.Key is also the
// partition key on brokers that have partitions, and a caller that sets one on
// an unordered publisher has asked for nothing this driver cannot do — the key
// is carried as metadata and round-trips, and the capability note says
// publications are not ordered by it.
//
// A key that is not valid UTF-8 is BASE64-ENCODED rather than refused, and
// that is the one interesting decision here. A Pub/Sub ordering key is a
// string, so a binary key cannot be one verbatim; the three answers were
// refusing the publication, publishing it unordered, and encoding the key.
// Refusing costs `publish/key-roundtrip` — a core scenario that publishes a
// key of {0x00, 0xff} and is not about ordering at all — and publishing it
// unordered is the silent downgrade of an ordering guarantee that
// forbids. Encoding keeps the guarantee, because base64 is injective: two
// messages with the same key still share an ordering key.
//
// What encoding costs is that a TEXT key which happens to spell the base64 of
// some binary key shares that key's ordering group. That can only MERGE two
// groups and never split one, so no ordering a caller was promised is
// weakened — the same argument `drivers/sqs` makes for hashing a key into a
// FIFO message group.
func (e *encoder) orderingKey(m *mq.Message) (string, error) {
	if !e.cfg.Ordering || len(m.Key) == 0 {
		return "", nil
	}
	if utf8.Valid(m.Key) {
		if len(m.Key) > MaxOrderingKeyBytes {
			return "", errOrderingKeyTooLong
		}
		return string(m.Key), nil
	}
	encoded := base64.StdEncoding.EncodeToString(m.Key)
	if len(encoded) > MaxOrderingKeyBytes {
		return "", errOrderingKeyTooLong
	}
	return encoded, nil
}

// attributesNeeded is how many attributes the message would spend natively:
// its headers, plus one per reserved field it sets.
func (e *encoder) attributesNeeded(m *mq.Message) int {
	n := m.Headers.Len()
	if m.Key != nil {
		n += 2 // the key and its encoding marker, at worst
	}
	if m.ID != "" {
		n++
	}
	if len(m.Body) == 0 {
		n++
	}
	p := &m.Properties
	for _, set := range [...]bool{
		p.ContentType != "", p.ContentEncoding != "", p.CorrelationID != "",
		p.CausationID != "", p.ReplyTo != "", !p.CreatedAt.IsZero(),
	} {
		if set {
			n++
		}
	}
	return n
}

// encodeMetadata writes the key, the identifier, the typed properties and the
// headers into the attribute map.
func (e *encoder) encodeMetadata(m *mq.Message, attrs map[string]string) error {
	if m.Key != nil {
		switch {
		case len(m.Key) == 0:
			// An empty attribute value is something the service may or may
			// not round-trip, so the marker carries the fact and the value
			// carries nothing.
			attrs[attrKeyEnc] = encEmpty
		case utf8.Valid(m.Key):
			attrs[attrKey] = string(m.Key)
		default:
			attrs[attrKey] = base64.StdEncoding.EncodeToString(m.Key)
			attrs[attrKeyEnc] = encBase64
		}
	}
	if m.ID != "" {
		attrs[attrID] = m.ID
	}
	p := &m.Properties
	setString(attrs, attrType, p.ContentType)
	setString(attrs, attrEncodeAs, p.ContentEncoding)
	setString(attrs, attrCorrID, p.CorrelationID)
	setString(attrs, attrCauseID, p.CausationID)
	setString(attrs, attrReplyTo, p.ReplyTo)
	if !p.CreatedAt.IsZero() {
		attrs[attrCreated] = p.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return e.encodeHeaders(m, attrs)
}

// encodeHeaders writes the caller's headers into the attribute map, applying
// the mode to every value the service cannot carry.
func (e *encoder) encodeHeaders(m *mq.Message, attrs map[string]string) error {
	for k, v := range m.Headers.All() {
		if reservedName(k) {
			return errReservedAttribute
		}
		switch {
		case !legalAttributeName(k):
			if e.mode == mq.MetadataLenientDrop {
				e.drops.Add(1)
				continue
			}
			if !utf8.ValidString(k) {
				return errAttributeKeyInvalid
			}
			return errAttributeKeyTooLong
		case len(v) > MaxAttributeValueBytes:
			if e.mode == mq.MetadataLenientDrop {
				e.drops.Add(1)
				continue
			}
			return errAttributeValueTooLong
		case !utf8.Valid(v):
			if e.mode == mq.MetadataLenientDrop {
				e.drops.Add(1)
				continue
			}
			return errAttributeValueBinary
		}
		if _, dup := attrs[k]; dup {
			// An attribute set is a map, so a repeated key has no native
			// representation. Under MetadataAdaptive the whole message has
			// already been enveloped by driverkit.Envelope, so reaching
			// here means the mode does not envelope.
			if e.mode == mq.MetadataLenientDrop {
				e.drops.Add(1)
				continue
			}
			return errEnvelopeNeeded
		}
		if len(attrs) >= MaxAttributes {
			if e.mode == mq.MetadataLenientDrop {
				e.drops.Add(1)
				continue
			}
			return errTooManyAttributes
		}
		attrs[k] = string(v)
	}
	return nil
}

// decode turns one received message into a portable one, overwriting dst.
//
// Which half the metadata comes from is the MODE's decision and not the
// message's: under MetadataAdaptive and MetadataPortableEnvelope an enveloped
// body carries the key, the identifier, the headers and the properties inside
// it, and reading the attributes as well would put each of them back twice.
// Under MetadataNativeStrict and MetadataLenientDrop nothing is ever
// unwrapped, so the attributes are the only metadata there is — and a message
// somebody else enveloped arrives with its envelope as its body, which is what
// those modes promise.
func (e *encoder) decode(src *psapi.Incoming, topic string, dst *mq.Message) error {
	raw := mq.Message{Topic: topic, Body: src.Data}
	switch src.Attributes[attrEncoding] {
	case encNil:
		raw.Body = nil
	case encEmpty:
		raw.Body = []byte{}
	}
	*dst = mq.Message{}
	if err := e.env.Unwrap(dst, &raw); err != nil {
		return err
	}
	dst.Topic = topic
	if e.enveloped(src) && e.unwraps() {
		return nil
	}
	e.decodeMetadata(src, dst)
	return nil
}

// unwraps reports whether this encoder's mode decodes an envelope at all.
func (e *encoder) unwraps() bool {
	return e.mode == mq.MetadataAdaptive || e.mode == mq.MetadataPortableEnvelope
}

// enveloped reports whether the received message's body is an iqmq envelope,
// as this driver marked it.
func (e *encoder) enveloped(src *psapi.Incoming) bool {
	return src.Attributes[attrEnvelope] == envelopeMarker
}

// decodeMetadata reads the key, the identifier, the typed properties and the
// headers back out of the attribute map.
func (e *encoder) decodeMetadata(src *psapi.Incoming, dst *mq.Message) {
	switch src.Attributes[attrKeyEnc] {
	case encEmpty:
		dst.Key = []byte{}
	case encBase64:
		if raw, err := base64.StdEncoding.DecodeString(src.Attributes[attrKey]); err == nil {
			dst.Key = raw
		}
	default:
		if v, ok := src.Attributes[attrKey]; ok {
			dst.Key = []byte(v)
		} else if src.OrderingKey != "" {
			// A message published by a producer that is not iqmq carries no
			// iqmq-k, and its ordering key is the closest thing to a key it
			// has.
			dst.Key = []byte(src.OrderingKey)
		}
	}
	dst.ID = src.Attributes[attrID]
	p := &dst.Properties
	p.ContentType = src.Attributes[attrType]
	p.ContentEncoding = src.Attributes[attrEncodeAs]
	p.CorrelationID = src.Attributes[attrCorrID]
	p.CausationID = src.Attributes[attrCauseID]
	p.ReplyTo = src.Attributes[attrReplyTo]
	if ts, ok := src.Attributes[attrCreated]; ok {
		if at, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			p.CreatedAt = at
		}
	}
	for k, v := range src.Attributes {
		if reservedName(k) {
			continue
		}
		dst.Headers.Add(k, []byte(v))
	}
}

// setString writes one attribute when the value is set.
func setString(attrs map[string]string, name, value string) {
	if value != "" {
		attrs[name] = value
	}
}

// reservedName reports whether a name belongs to this driver or to the
// service.
//
// The specification reserves "iqmq-" for iqmq and the service reserves "goog" for itself,
// so a caller's header may be neither. The whole "iqmq-" prefix is refused and
// not merely the names this driver spends, because a name this driver adds
// later would otherwise start colliding with a header somebody is already
// publishing.
func reservedName(name string) bool {
	return hasPrefix(name, "iqmq-") || hasPrefix(name, ReservedAttributePrefix)
}

// hasPrefix is strings.HasPrefix without the import, which keeps this file's
// dependencies to what the mapping needs.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// legalAttributeName reports whether a header key can be a Pub/Sub attribute
// name: valid UTF-8, non-empty, and at most the service's 256 bytes.
func legalAttributeName(k string) bool {
	return k != "" && len(k) <= MaxAttributeKeyBytes && utf8.ValidString(k)
}

// accountedSize is what the service's ten-megabyte limit counts: the data,
// plus every attribute name and value, plus the ordering key.
func accountedSize(out *psapi.Outgoing) int {
	n := len(out.Data) + len(out.OrderingKey)
	for k, v := range out.Attributes {
		n += len(k) + len(v)
	}
	return n
}

// attemptOf is the delivery attempt this message reports, and whether the
// service reported one at all.
//
// Pub/Sub populates DeliveryAttempt ONLY on a subscription with a dead-letter
// policy. DeliveryInfo.Attempt has a spelling for exactly that — "zero means
// the broker does not count attempts" — so a subscription with no policy
// reports zero rather than an invented 1, and Redelivered stays false because
// nothing observed says otherwise. docs/drivers/gcppubsub.md says so, and
// FeatureDeadLetter's capability note ties the two together: the attempt count
// arrives WITH the dead-letter policy and not on its own.
func attemptOf(src *psapi.Incoming) (uint32, bool) {
	if !src.HasAttempt || src.Attempt <= 0 {
		return 0, false
	}
	return uint32(src.Attempt), true
}

package sqs

import (
	"encoding/base64"
	"slices"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The reserved message-attribute names.
//
// The specification reserves the "iqmq-" prefix, and these are the names this driver spends
// it on. Every one of them is optional and present only when the field it
// carries is set, which is what makes the ten-attribute budget mostly the
// caller's: a message with a key, an identifier and one property spends three
// and leaves seven for headers.
//
// They count against the budget, and that is the whole reason
// MetadataAdaptive exists here. The question for this driver is what
// happens to the eleventh piece of metadata, and the answer is the envelope.
const (
	attrKey      = "iqmq-k"
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
// refused for colliding with.
var reservedNames = [...]string{
	attrKey, attrID, attrEncoding, attrEnvelope,
	attrType, attrEncodeAs, attrCorrID, attrCauseID, attrReplyTo, attrCreated,
}

// The body-encoding markers, carried in [attrEncoding].
const (
	// encBase64 says the body is the base64 of the real payload.
	encBase64 = "b64"
	// encNil says the payload was nil, which is not the same as empty
	// and which the service cannot carry: SendMessage requires a
	// non-empty MessageBody.
	encNil = "nil"
	// encEmpty says the payload was an empty non-nil slice.
	encEmpty = "empty"
)

// bodyPlaceholder is the body of a message whose payload is nil or empty. Its
// content is never read — [attrEncoding] is authoritative — and it exists
// because the service refuses an empty MessageBody outright.
const bodyPlaceholder = "-"

// The data types a message attribute carries. The service defines String,
// Number and Binary; this driver uses two of them, and Binary is what makes
// FeatureBinaryHeaders a native claim rather than an emulation.
const (
	typeString = "String"
	typeBinary = "Binary"
)

// encoded is one publication as the service takes it.
//
// It is built by [encode], which is pure: no clock, no I/O, and every limit
// checked before anything is sent. The specification asks for "limits before I/O" and this
// is where that happens.
type encoded struct {
	// Body is the MessageBody.
	Body string
	// Attrs are the message attributes, at most MaxAttributes of them.
	Attrs map[string]types.MessageAttributeValue
	// GroupID is the FIFO MessageGroupId, empty on a standard queue.
	GroupID string
	// DedupID is the FIFO MessageDeduplicationId, empty when the caller
	// named no DedupKey.
	DedupID string
	// Delay is DelaySeconds, zero when the caller asked for no delay.
	Delay int32
	// Enveloped reports that Body is an iqmq envelope rather than the
	// caller's payload.
	Enveloped bool
	// Bytes is the accounted size: the body plus the attribute names and
	// values, which is what the service's one-mebibyte limit counts.
	Bytes int
}

// encoder turns a message into the service's shape.
//
// It holds the two envelopes rather than one, and the reason is a gap in the
// kit. [driverkit.Envelope] decides whether MetadataAdaptive has to wrap by
// reading the CAPABILITY TABLE — binary values, duplicate keys, headers at
// all — and this driver has two reasons the table cannot express: a message
// with more than ten attributes, and a header key that is not a legal
// attribute name. So `env` carries the configured mode and answers the mode
// questions, and `forced` is a second envelope in MetadataPortableEnvelope
// mode whose Wrap always wraps, which is what this driver reaches for when its
// own reason fires. Recorded in the maintenance notes.
type encoder struct {
	cfg    Config
	env    *driverkit.Envelope
	forced *driverkit.Envelope
	mode   mq.MetadataMode
	limit  int

	// drops counts the headers MetadataLenientDrop has discarded, which is the metric. OnDrop is the driver's own observation point for it.
	drops  atomic.Int64
	onDrop func(m *mq.Message, n int)
}

// newEncoder builds the encoder for one sink or source.
func newEncoder(cfg Config, caps mq.Capabilities, mode mq.MetadataMode) (*encoder, error) {
	if !mode.Valid() {
		return nil, errMetadataMode
	}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Capabilities: caps})
	if err != nil {
		return nil, err
	}
	forced, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode: mq.MetadataPortableEnvelope, Capabilities: caps,
	})
	if err != nil {
		return nil, err
	}
	return &encoder{cfg: cfg, env: env, forced: forced, mode: mode, limit: int(cfg.MaxMessageBytes)}, nil
}

// encode turns one message and its resolved options into the service's shape.
//
// Everything that can be refused is refused here, before any I/O: an
// unsupported option, a body the service's character set cannot carry under
// the text policy, metadata that does not fit under the strict policy, and a
// message over the size limit. The rule that a refusal names what was
// refused is why each of them has its own error.
func (e *encoder) encode(m *mq.Message, o *mq.PublishOptions) (encoded, error) {
	if m == nil {
		return encoded{}, errNilMessage
	}
	out := encoded{}
	if err := e.options(m, o, &out); err != nil {
		return encoded{}, err
	}
	msg, wrapped, err := e.shape(m)
	if err != nil {
		return encoded{}, err
	}
	out.Enveloped = wrapped
	attrs, err := e.attributes(msg, wrapped)
	if err != nil {
		return encoded{}, err
	}
	body, enc, err := e.body(msg.Body)
	if err != nil {
		return encoded{}, err
	}
	if enc != "" {
		attrs[attrEncoding] = stringAttr(enc)
	}
	out.Body, out.Attrs = body, attrs
	if len(attrs) > MaxAttributes {
		// Unreachable through shape's own accounting; asserting it here is
		// what keeps a future reserved attribute from silently pushing a
		// message over the service's limit.
		return encoded{}, errTooManyAttributes
	}
	out.Bytes = len(body)
	for k, v := range attrs {
		out.Bytes += len(k) + len(aws.ToString(v.StringValue)) + len(v.BinaryValue)
	}
	if out.Bytes > e.limit {
		return encoded{}, mq.ErrMessageTooLarge
	}
	return out, nil
}

// options applies the resolved publication options, refusing the ones this
// profile has no answer for.
//
// The specification makes "silently drops unsupported metadata or options" a
// disqualifier, so every one of these is a refusal and not a shrug.
func (e *encoder) options(m *mq.Message, o *mq.PublishOptions, out *encoded) error {
	if o == nil {
		o = &mq.PublishOptions{}
	}
	switch {
	case o.TTL != 0:
		return errNoTTL
	case o.PrioritySet:
		return errNoPriority
	case o.PartitionSet:
		return errNoPartition
	}
	delay := o.Delay
	if !o.DeliverAt.IsZero() {
		if delay != 0 {
			return errDelayAndDeliverAt
		}
		// DeliverAt is an absolute time and DelaySeconds is a duration, so
		// the conversion needs the clock. It is the one place the encode
		// path reads it, and it reads the INJECTED clock so that a test
		// driving time drives this too.
		delay = o.DeliverAt.Sub(e.now())
		if delay < 0 {
			delay = 0
		}
	}
	switch {
	case delay < 0:
		return errNegativeDelay
	case delay > 0 && e.cfg.FIFO:
		return errFIFODelay
	case delay > MaxDelay:
		return errDelayTooLong
	}
	// Whole seconds, rounded UP: a 500ms delay that rounded down to zero
	// would deliver the message immediately, which is the guarantee the
	// caller asked for being quietly withdrawn.
	out.Delay = int32((delay + time.Second - 1) / time.Second)

	if o.DedupKey != "" {
		if !e.cfg.FIFO {
			return errNoDedup
		}
		out.DedupID = DedupID(o.DedupKey)
	}
	if e.cfg.FIFO {
		out.GroupID = GroupID(m.Key)
	}
	return nil
}

// now reads the injected clock.
func (e *encoder) now() time.Time {
	if e.cfg.Clock != nil {
		return e.cfg.Clock.Now()
	}
	return time.Now()
}

// shape decides whether the message travels natively or in an envelope, and
// returns the message to send.
//
// The four modes, and none of them reached by accident:
//
//   - MetadataNativeStrict refuses metadata that does not fit, before any
//     I/O.
//   - MetadataPortableEnvelope always envelopes, so every message on the
//     queue has the same shape and a non-iqmq consumer reads base64.
//   - MetadataAdaptive envelopes only the messages that need it.
//   - MetadataLenientDrop drops what will not fit, from the end of the header
//     list, with the hook and the metric the specification asks for.
func (e *encoder) shape(m *mq.Message) (*mq.Message, bool, error) {
	// The specification reserves the "iqmq-" prefix "with conflicts rejected", and the
	// rejection is not a mode's business: a caller who set "iqmq-k" meant
	// something by it, and every mode here would do something else with it —
	// the envelope would bring it back as a header beside the key it
	// collided with, and the lenient mode would discard it. So it is refused
	// before the mode is consulted at all.
	//
	// Written as part of envelopeNeeded's answer it was NOT refused under
	// MetadataAdaptive: "needs the envelope" and "is a collision" were one
	// boolean, and the adaptive path read the boolean and enveloped it.
	// FuzzSQSAttributes found that in forty-five seconds.
	if err := checkReserved(m); err != nil {
		return nil, false, err
	}
	need, why := e.envelopeNeeded(m)
	switch e.mode {
	case mq.MetadataNativeStrict:
		if need {
			return nil, false, why
		}
		return m, false, nil
	case mq.MetadataPortableEnvelope:
		// Envelope.Wrap refuses, before any I/O, anything Envelope.Check
		// refuses — an identifier, a property or a header key that is not
		// valid UTF-8, which the encoder accepts and the decoder does not.
		// FuzzSQSAttributes found that on its first run and this driver
		// carried a local stand-in for it until the kit's own check landed.
		wrapped, err := e.forced.Wrap(m)
		return wrapped, true, err
	case mq.MetadataAdaptive:
		if !need {
			return m, false, nil
		}
		// forced rather than e.env, because e.env re-derives the decision
		// from the capability table and would answer "no" to the two
		// reasons that are this driver's own.
		wrapped, err := e.forced.Wrap(m)
		return wrapped, true, err
	case mq.MetadataLenientDrop:
		if !need {
			return m, false, nil
		}
		return e.drop(m), false, nil
	default:
		return nil, false, errMetadataMode
	}
}

// envelopeNeeded reports whether m's metadata fits in the service's message
// attributes, and the refusal to use when it does not.
//
// The four reasons are the whole story for this driver:
//
//   - more than ten attributes, counting the reserved ones;
//   - a header key that is not a legal attribute name;
//   - a repeated header key, because attributes are a map;
//
// The first two are invisible to [driverkit.Envelope.Needed], which reads the
// capability table and has no way to express a count or a grammar; the third
// it does see, through FeatureDuplicateHeaders. The reserved prefix is
// NOT one of them: [checkReserved] refuses a collision before any mode is
// consulted.
func (e *encoder) envelopeNeeded(m *mq.Message) (bool, error) {
	seen := make(map[string]struct{}, m.Headers.Len())
	dup, bad := false, false
	for k := range m.Headers.All() {
		if !attributeNameOK(k) {
			bad = true
		}
		if _, ok := seen[k]; ok {
			dup = true
		}
		seen[k] = struct{}{}
	}
	switch {
	case dup:
		return true, errDuplicateHeader
	case bad:
		return true, errHeaderName
	case e.reservedCount(m)+len(seen) > MaxAttributes:
		return true, errTooManyAttributes
	}
	return false, nil
}

// checkReserved refuses a header key colliding with one of this driver's
// reserved attribute names, which the specification requires of every mode.
func checkReserved(m *mq.Message) error {
	for k := range m.Headers.All() {
		if slices.Contains(reservedNames[:], k) {
			return errReservedHeader
		}
	}
	return nil
}

// reservedCount is how many of the ten attributes this driver's own metadata
// spends on m.
func (e *encoder) reservedCount(m *mq.Message) int {
	n := 0
	if len(m.Key) > 0 {
		n++
	}
	if m.ID != "" {
		n++
	}
	for _, set := range [...]bool{
		m.Properties.ContentType != "", m.Properties.ContentEncoding != "",
		m.Properties.CorrelationID != "", m.Properties.CausationID != "",
		m.Properties.ReplyTo != "", !m.Properties.CreatedAt.IsZero(),
	} {
		if set {
			n++
		}
	}
	// The encoding marker is only present for a body the service cannot
	// carry, and counting it always is the conservative direction: it can
	// make a message take the envelope it did not strictly need, and it can
	// never let one through that does not fit.
	return n + 1
}

// drop is MetadataLenientDrop: keep what fits and discard the rest, with the
// hook and the metric the specification requires.
//
// The order is fixed and documented: a header with an illegal name goes
// first, then a repeat of a key already kept, then headers from the END of the
// list until the budget is met. The key, the identifier and the typed
// properties are never dropped — a caller who chose this mode chose to lose
// HEADERS, and losing an identifier would break correlation rather than
// metadata.
func (e *encoder) drop(m *mq.Message) *mq.Message {
	out := &mq.Message{
		Topic: m.Topic, Key: m.Key, Body: m.Body,
		ID: m.ID, Properties: m.Properties,
	}
	budget := MaxAttributes - e.reservedCount(m)
	seen := make(map[string]struct{}, m.Headers.Len())
	dropped := 0
	for k, v := range m.Headers.All() {
		_, repeat := seen[k]
		if !attributeNameOK(k) || repeat || budget <= 0 {
			dropped++
			continue
		}
		seen[k] = struct{}{}
		out.Headers.Add(k, v)
		budget--
	}
	if dropped > 0 {
		// the specification's "drops unsupported metadata with a hook and a metric". The
		// counter is what this driver's Dropped reports and what its own
		// tests assert on; the hook is the caller's OnDrop, which is the
		// only observation point the SPI leaves a driver for a metadata
		// loss (finding recorded in the maintenance notes).
		e.drops.Add(int64(dropped))
		if e.onDrop != nil {
			e.onDrop(m, dropped)
		}
	}
	return out
}

// Dropped is how many headers this encoder has discarded under
// MetadataLenientDrop. It is the metric the specification asks for, and it is what a test
// asserts on: a mode that drops must be observable, or "lenient" is
// indistinguishable from "lossy".
func (e *encoder) Dropped() int64 { return e.drops.Load() }

// attributeNameOK reports whether k is a name the service accepts for a
// message attribute.
//
// From the pinned SDK's documentation: alphanumerics, hyphens, underscores and
// periods; no leading or trailing period; no two periods in succession; at
// most 256 characters; and not beginning with "AWS." or "Amazon.", which are
// the service's own.
func attributeNameOK(k string) bool {
	if k == "" || len(k) > 256 {
		return false
	}
	if k[0] == '.' || k[len(k)-1] == '.' {
		return false
	}
	if hasPrefixFold(k, "aws.") || hasPrefixFold(k, "amazon.") {
		return false
	}
	for i := range len(k) {
		c := k[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '_':
		case c == '.':
			if i+1 < len(k) && k[i+1] == '.' {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// hasPrefixFold is strings.HasPrefix with an ASCII case fold, for the two
// prefixes the service reserves case-insensitively.
func hasPrefixFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range len(prefix) {
		a, b := s[i], prefix[i]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}

// attributes builds the message attributes for a message that has already been
// shaped.
func (e *encoder) attributes(m *mq.Message, wrapped bool) (map[string]types.MessageAttributeValue, error) {
	attrs := make(map[string]types.MessageAttributeValue, MaxAttributes)
	if wrapped {
		// An enveloped message carries its whole metadata in the body, so
		// the only attributes are the two markers. The envelope marker is
		// what the source reads to decide whether to decode, rather than
		// sniffing the body: a native body that happened to look like an
		// envelope would otherwise be decoded as one.
		attrs[attrEnvelope] = stringAttr("1")
		return attrs, nil
	}
	if len(m.Key) > 0 {
		attrs[attrKey] = binaryAttr(m.Key)
	}
	if m.ID != "" {
		attrs[attrID] = valueAttr([]byte(m.ID))
	}
	p := &m.Properties
	for _, pair := range [...]struct {
		name  string
		value string
	}{
		{attrType, p.ContentType},
		{attrEncodeAs, p.ContentEncoding},
		{attrCorrID, p.CorrelationID},
		{attrCauseID, p.CausationID},
		{attrReplyTo, p.ReplyTo},
	} {
		if pair.value != "" {
			attrs[pair.name] = valueAttr([]byte(pair.value))
		}
	}
	if !p.CreatedAt.IsZero() {
		attrs[attrCreated] = stringAttr(p.CreatedAt.Format(time.RFC3339Nano))
	}
	for k, v := range m.Headers.All() {
		if !attributeNameOK(k) {
			return nil, errHeaderName
		}
		attrs[k] = valueAttr(v)
	}
	return attrs, nil
}

// valueAttr carries a value in the service's String type when the service can
// hold it and in its Binary type when it cannot.
//
// The test is the SERVICE's character set and not merely valid UTF-8, which is
// the correction FuzzSQSAttributes forced: an attribute's String value is
// bounded by the same XML character set as the body, so a value that is valid
// UTF-8 and carries a control byte would be refused by the service — and a
// driver that only checked UTF-8 would send it. Binary is what makes
// FeatureBinaryHeaders a native claim on a broker whose attributes are
// otherwise strings.
func valueAttr(v []byte) types.MessageAttributeValue {
	if sqsSafeText(v) {
		return stringAttr(string(v))
	}
	return binaryAttr(v)
}

// body applies the binary-body policy of [BodyEncoding].
func (e *encoder) body(b []byte) (string, string, error) {
	switch {
	case b == nil:
		return bodyPlaceholder, encNil, nil
	case len(b) == 0:
		return bodyPlaceholder, encEmpty, nil
	}
	switch e.cfg.Body {
	case BodyBase64:
		return base64.StdEncoding.EncodeToString(b), encBase64, nil
	case BodyText:
		if !sqsSafeText(b) {
			return "", "", errBodyNotText
		}
		return string(b), "", nil
	case BodyAuto:
		if sqsSafeText(b) {
			return string(b), "", nil
		}
		return base64.StdEncoding.EncodeToString(b), encBase64, nil
	default:
		return "", "", errBodyEncoding
	}
}

// sqsSafeText reports whether b is a body the service accepts unchanged.
//
// The character set is the service's own, from the pinned SDK's SendMessage
// documentation: #x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD |
// #x10000-#x10FFFF. utf8.Valid already excludes the surrogates and anything
// above #x10FFFF, so what is left to check is the control range and #xFFFE
// and #xFFFF.
func sqsSafeText(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		switch {
		case r == 0x9 || r == 0xA || r == 0xD:
		case r >= 0x20 && r <= 0xD7FF:
		case r >= 0xE000 && r <= 0xFFFD:
		case r >= 0x10000 && r <= 0x10FFFF:
		default:
			return false
		}
	}
	return true
}

// stringAttr is one String-typed attribute.
func stringAttr(v string) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String(typeString), StringValue: aws.String(v)}
}

// binaryAttr is one Binary-typed attribute.
func binaryAttr(v []byte) types.MessageAttributeValue {
	return types.MessageAttributeValue{DataType: aws.String(typeBinary), BinaryValue: v}
}

// decode turns one received message into a portable one.
//
// dst is the caller's storage and is overwritten, which is what keeps a fetch
// from allocating a message per delivery.
func (e *encoder) decode(src *types.Message, topic string, dst *mq.Message) error {
	if src == nil || dst == nil {
		return errNilMessage
	}
	dst.Reset()
	dst.Topic = topic
	enc, enveloped := "", false
	for _, name := range sortedNames(src.MessageAttributes) {
		v := src.MessageAttributes[name]
		switch name {
		case attrEncoding:
			enc = aws.ToString(v.StringValue)
		case attrEnvelope:
			enveloped = true
		case attrKey:
			dst.Key = attrBytes(v)
		case attrID:
			// Read through attrBytes rather than StringValue: a value the
			// service's character set cannot hold travels in a Binary
			// attribute, and a decoder that only read StringValue would
			// hand back an empty identifier for one.
			dst.ID = string(attrBytes(v))
		case attrType:
			dst.Properties.ContentType = string(attrBytes(v))
		case attrEncodeAs:
			dst.Properties.ContentEncoding = string(attrBytes(v))
		case attrCorrID:
			dst.Properties.CorrelationID = string(attrBytes(v))
		case attrCauseID:
			dst.Properties.CausationID = string(attrBytes(v))
		case attrReplyTo:
			dst.Properties.ReplyTo = string(attrBytes(v))
		case attrCreated:
			if t, err := time.Parse(time.RFC3339Nano, string(attrBytes(v))); err == nil {
				dst.Properties.CreatedAt = t
			}
		default:
			dst.Headers.Add(name, attrBytes(v))
		}
	}
	body, err := decodeBody(aws.ToString(src.Body), enc)
	if err != nil {
		return err
	}
	dst.Body = body
	if !enveloped && e.mode != mq.MetadataPortableEnvelope {
		return nil
	}
	var out mq.Message
	if err := e.forced.Unwrap(&out, dst); err != nil {
		return err
	}
	out.Topic = topic
	*dst = out
	return nil
}

// sortedNames is the attribute names in a stable order.
//
// The service's attributes are a MAP, so the order a publisher wrote its
// headers in is not on the wire at all. The specification promises ordered headers, and
// this driver cannot keep that promise natively: what it does instead is
// decode in name order, so that two consumers of one message see the same
// order and a test is reproducible. A caller who needs the published order
// preserved uses MetadataPortableEnvelope or MetadataAdaptive with a message
// the envelope carries, and docs/drivers/sqs.md's "known gotchas" says so.
func sortedNames(attrs map[string]types.MessageAttributeValue) []string {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]string, 0, len(attrs))
	for k := range attrs {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// attrBytes is an attribute's value as bytes, whichever type it carries.
func attrBytes(v types.MessageAttributeValue) []byte {
	if len(v.BinaryValue) > 0 {
		return v.BinaryValue
	}
	if v.StringValue == nil {
		return nil
	}
	return []byte(*v.StringValue)
}

// decodeBody reverses the binary-body policy.
func decodeBody(body, enc string) ([]byte, error) {
	switch enc {
	case encNil:
		return nil, nil
	case encEmpty:
		return []byte{}, nil
	case encBase64:
		out, err := base64.StdEncoding.DecodeString(body)
		if err != nil {
			return nil, errBodyNotBase64
		}
		return out, nil
	case "":
		return []byte(body), nil
	default:
		// An unknown marker is a message written by a newer driver, and
		// guessing at the body would hand the application bytes that are
		// not the payload.
		return nil, errBodyEncodingUnknown
	}
}

// attemptOf is the specification's "ApproximateReceiveCount→Attempt".
//
// A count the service did not send, or one that does not parse, reads as zero,
// which the specification makes "the broker does not count attempts" rather than "attempt
// zero". Reading a missing count as one would claim a first delivery for a
// message that may have been delivered ten times.
func attemptOf(attrs map[string]string) uint32 {
	v, ok := attrs[string(types.MessageSystemAttributeNameApproximateReceiveCount)]
	if !ok {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return 0
	}
	return uint32(n)
}

// sequenceOf is a FIFO message's SequenceNumber as the optional sequence.
//
// The service's sequence number is a 128-bit decimal, and a uint64 cannot hold
// every one of them. A number that does not fit is reported as ABSENT rather
// than truncated: the rule that an unknown is never an overloaded zero
// applies to a value that was too large just as much as to one that was never
// sent.
func sequenceOf(attrs map[string]string) (uint64, bool) {
	v, ok := attrs[string(types.MessageSystemAttributeNameSequenceNumber)]
	if !ok || v == "" {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// groupOf is a FIFO message's MessageGroupId, empty on a standard queue.
func groupOf(attrs map[string]string) string {
	return attrs[string(types.MessageSystemAttributeNameMessageGroupId)]
}

// The mapping refusals.
var (
	errNilMessage          = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the message is nil")
	errMetadataMode        = errcode.New(errcode.InvalidConfig, "iqmq/sqs: the metadata mode is not one of the four")
	errNoTTL               = errcode.New(errcode.Unsupported, "iqmq/sqs: the service has no per-message time to live; the queue's MessageRetentionPeriod is the only expiry it offers")
	errNoPriority          = errcode.New(errcode.Unsupported, "iqmq/sqs: the service has no message priority")
	errNoPartition         = errcode.New(errcode.Unsupported, "iqmq/sqs: a queue has no partitions; the FIFO profile's message group is the closest thing and comes from the key")
	errNoDedup             = errcode.New(errcode.Unsupported, "iqmq/sqs: deduplication needs the FIFO profile, where DedupKey becomes MessageDeduplicationId")
	errFIFODelay           = errcode.New(errcode.Unsupported, "iqmq/sqs: a FIFO queue takes no per-message delay, only the queue's own DelaySeconds")
	errDelayTooLong        = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the delay is longer than the service's fifteen-minute message timer")
	errNegativeDelay       = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the delay is negative")
	errDelayAndDeliverAt   = errcode.New(errcode.InvalidArgument, "iqmq/sqs: Delay and DeliverAt are alternatives, not an addition")
	errReservedHeader      = errcode.New(errcode.InvalidArgument, "iqmq/sqs: the header key collides with one of this driver's reserved iqmq- message attributes")
	errDuplicateHeader     = errcode.New(errcode.Unsupported, "iqmq/sqs: a message attribute set is a map, so a repeated header key needs MetadataAdaptive or MetadataPortableEnvelope")
	errHeaderName          = errcode.New(errcode.Unsupported, "iqmq/sqs: the header key is not a legal message-attribute name; MetadataAdaptive carries it in the envelope")
	errTooManyAttributes   = errcode.New(errcode.Unsupported, "iqmq/sqs: the message needs more than the service's ten message attributes; MetadataAdaptive carries it in the envelope")
	errBodyNotText         = errcode.New(errcode.Unsupported, "iqmq/sqs: the body is not in the service's character set and Config.Body is text; auto or base64 encodes it")
	errBodyNotBase64       = errcode.New(errcode.CorruptMessage, "iqmq/sqs: the message says its body is base64 and it does not decode")
	errBodyEncodingUnknown = errcode.New(errcode.CorruptMessage, "iqmq/sqs: the message names a body encoding this driver does not know")
)

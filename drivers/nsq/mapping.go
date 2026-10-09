package nsq

import (
	"strings"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/wire"
)

// MaxNameLen is the longest topic or channel name nsqd accepts.
const MaxNameLen = 64

// EphemeralSuffix is nsqd's marker for a topic or channel that is deleted once
// its last client goes away.
const EphemeralSuffix = "#ephemeral"

// CheckTopic reports whether name is a legal NSQ topic name.
//
// nsqd's grammar is `^[.a-zA-Z0-9_-]+(#ephemeral)?$`, one to sixty-four bytes.
// It is checked here rather than left to nsqd because an illegal name is
// answered with `E_BAD_TOPIC` after a round trip, and the specification wants the refusal
// before any I/O — and because a topic that nsqd refuses is one this driver
// would otherwise have created a channel on.
//
// TestTopicGrammarAgreesWithTheClient and FuzzNSQTopicName hold this function
// to [github.com/nsqio/go-nsq.IsValidTopicName], which is the same grammar
// written as a regular expression. The hand-rolled form is here so that
// validating a name costs no regular-expression match, and the fuzz target is
// what keeps the two from drifting.
func CheckTopic(name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	return nil
}

// CheckChannel reports whether name is a legal NSQ channel name. The grammar is
// the topic's.
func CheckChannel(name string) error { return checkName(name) }

// checkName is nsqd's shared topic/channel grammar.
func checkName(name string) error {
	body := name
	if rest, ok := strings.CutSuffix(name, EphemeralSuffix); ok {
		body = rest
	}
	switch {
	case len(name) == 0:
		return errEmptyName
	case len(name) > MaxNameLen:
		return errNameTooLong
	case len(body) == 0:
		// "#ephemeral" on its own: the suffix is legal, a name of only the
		// suffix is not.
		return errBadNameChar
	}
	for i := range len(body) {
		c := body[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '-':
		default:
			return errBadNameChar
		}
	}
	return nil
}

// TopicOf maps an iqmq topic onto an NSQ topic name.
//
// The mapping is the identity plus [Config.TopicPrefix], which is how two iqmq
// deployments share one nsqd. It is injective for a fixed prefix, which is what
// the target 3 asks of a mapper: two iqmq topics never become one NSQ
// topic, because the prefix is a pure prepend and nothing is elided.
//
// There is no escaping. An iqmq topic that is not a legal NSQ name is REFUSED
// rather than transformed: a mapper that replaced the illegal bytes would map
// "a/b" and "a-b" onto one topic, and a caller would then find one
// subscription receiving another's messages.
func TopicOf(prefix, topic string) (string, error) {
	// The wildcard check comes FIRST, before the grammar. A pattern
	// metacharacter is outside NSQ's alphabet, so CheckTopic would refuse it
	// as an invalid name — and the conformance kit's negative probe for
	// FeatureWildcardSubscribe reads ErrUnsupported and nothing else, so a
	// driver that let the grammar answer first would leave the claim
	// unproved in both directions. It is also the more useful answer for a
	// caller who wrote a pattern on purpose: the feature is missing, not the
	// name mistyped. drivers/sqs does the same, for the same reason.
	if HasWildcard(topic) {
		return "", errNoWildcards
	}
	name := prefix + topic
	if err := CheckTopic(name); err != nil {
		return "", err
	}
	return name, nil
}

// HasWildcard reports whether a topic carries a pattern metacharacter from any
// of the grammars a portable caller might write.
//
// It is the union rather than one broker's alphabet, because the question is
// "did the caller mean a pattern?" and the answer must not depend on which
// broker they had in mind. A legitimate trailing "#ephemeral" is not a pattern
// and is stripped before the scan, or every ephemeral topic would be refused
// as one.
func HasWildcard(topic string) bool {
	body, _ := strings.CutSuffix(topic, EphemeralSuffix)
	for i := range len(body) {
		switch body[i] {
		case '*', '?', '#', '>':
			return true
		}
	}
	return false
}

// ChannelOf maps an iqmq subscription onto an NSQ channel name.
//
// A subscription is a channel: every channel of a topic receives a copy of
// every message and every consumer of one channel competes for its messages,
// which is exactly the consumer group inside the fan-out. An empty
// subscription is refused, because a channel with no name is not a thing nsqd
// has — the specification's "FanOut unsupported" shortcut of naming no subscription is the
// SQS profile's and not this one's.
func ChannelOf(subscription string) (string, error) {
	if subscription == "" {
		return "", errNoSubscription
	}
	if err := CheckChannel(subscription); err != nil {
		return "", err
	}
	return subscription, nil
}

// Representable reports whether NSQ can carry m's metadata natively, and the
// refusal to use when it cannot.
//
// The answer is short, because NSQ has no headers: a message with any header, a
// message with an identifier and a message with any typed property all need the
// envelope. It is what MetadataNativeStrict refuses on and what
// MetadataAdaptive wraps on.
//
// It is deliberately NOT the envelope's admission check. A message can be
// unrepresentable natively and perfectly representable in an envelope, which is
// the normal case here; [envelopeRefusal] is the other question.
func Representable(m *mq.Message) error {
	if m == nil {
		return errNilMessage
	}
	switch {
	case m.Headers.Len() > 0:
		return errNoHeaders
	case m.ID != "":
		return errNoIdentifier
	case !m.Properties.IsZero():
		return errNoProperties
	case m.Key != nil:
		// Not len(m.Key) > 0. The specification makes a nil key and an empty one
		// observably different, so a message carrying an EMPTY key is
		// carrying a key, and a broker with no key field cannot keep it.
		// FuzzNSQEnvelope found this in the first second of its first run:
		// the message went out natively and the empty key came back nil.
		return errNoKey
	}
	return nil
}

// envelopeRefusal names the field that put m past the envelope's limits.
//
// The DECISION is [driverkit.Envelope.Check]'s and only ever the kit's: this
// function is reached exactly when Check has already refused, and its answer is
// a diagnosis rather than an admission rule. That asymmetry is the point. The
// fourth hand-written copy of the envelope's limit table was recorded in
// the maintenance notes as a defect class in its own right — three drivers accepted
// a publication their own decoder would refuse for ever, each through a copy of
// the rules that had drifted — and a diagnosis that drifts produces a worse
// error message, never a wrong decision.
//
// It matters on this driver more than on any other. The envelope is the ONLY
// metadata path NSQ has, so a message the envelope cannot carry cannot be
// published at all, and "iqmq/nsq: the message cannot be enveloped" with no
// field named would leave a caller nothing to act on.
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
		if !validUTF8(k) {
			return errEnvelopeHeaderKeyUTF8
		}
	}
	if !validUTF8(m.ID) {
		return errEnvelopeIDUTF8
	}
	p := &m.Properties
	for _, s := range [...]string{
		p.ContentType, p.ContentEncoding, p.CorrelationID, p.CausationID, p.ReplyTo,
	} {
		if over(len(s), r.MaxProp) {
			return errEnvelopeProperty
		}
		if !validUTF8(s) {
			return errEnvelopePropertyUTF8
		}
	}
	// Check refused and this walk found nothing, which means the two have
	// drifted. Saying so is better than naming a field at random: the
	// publication is still refused, and the message says where to look.
	return errEnvelopeUndiagnosed
}

// validUTF8 is utf8.ValidString under a name that says what it is for: the
// envelope's decoder validates the identifier, every header key and every
// string property, and its encoder does not.
func validUTF8(s string) bool { return utf8.ValidString(s) }

// wrapForPublish is the whole decision for this driver: what to publish,
// and the refusal when nothing can be.
//
// It is short because NSQ leaves no choice. There is nowhere to put a header,
// so either the whole message goes into the envelope or the metadata does not
// travel, and the three modes that do anything differ only in which of those
// they pick.
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
		// the lenient mode: the metadata is dropped and the body is
		// published. Nothing is refused and nothing is hidden — the caller
		// asked for this mode and the driver counts what it discarded.
		if Representable(m) == nil {
			return m, nil
		}
		return &mq.Message{Topic: m.Topic, Body: m.Body}, nil
	case mq.MetadataAdaptive:
		// This driver has two reasons to envelope that
		// [driverkit.Envelope.Needed] cannot express, because Needed reads
		// the CAPABILITY TABLE and neither of them is a feature:
		//
		//   - a MESSAGE KEY. The inventory has no "native key" feature
		//     at all — every other broker has a key field, so nothing ever
		//     needed one — and Needed's own early return is "no headers, no
		//     identifier, no properties, nothing to do". A keyed message
		//     with no other metadata therefore answers false and would go
		//     out natively with its key discarded, silently.
		//   - an EMPTY BODY. nsqd refuses a PUB with a zero-length body, so
		//     the nil-versus-empty distinction survives here only
		//     inside an envelope, which is never empty.
		//
		// Both are answered by [Representable] plus the length, and both
		// take `forced` — a second envelope in the portable mode, whose Wrap
		// always wraps — which is the workaround the specification recorded
		// for the same gap on SQS.
		if Representable(m) != nil || len(m.Body) == 0 {
			out, err := forced.Wrap(m)
			if err != nil {
				return nil, envelopeRefusal(m, lim)
			}
			return out, nil
		}
		// A message with nothing to carry still reaches the kit, because
		// the kit is what knows the other reason to envelope: a body that
		// the decoder's own discriminator would claim. Wrap envelopes such
		// a body even though the broker could carry it natively, so that
		// this driver never publishes a native body its own Unwrap would
		// read as metadata.
		out, err := env.Wrap(m)
		if err != nil {
			return nil, envelopeRefusal(m, lim)
		}
		return out, nil
	case mq.MetadataPortableEnvelope:
		out, err := env.Wrap(m)
		if err != nil {
			// The envelope is the only metadata path NSQ has, so this is
			// a publication that cannot happen at all. Name the field.
			return nil, envelopeRefusal(m, lim)
		}
		return out, nil
	default:
		return nil, errMetadataMode
	}
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
	errEmptyName      = errcode.New(errcode.InvalidArgument, "iqmq/nsq: an NSQ topic or channel name cannot be empty")
	errNameTooLong    = errcode.New(errcode.InvalidArgument, "iqmq/nsq: an NSQ topic or channel name is at most 64 bytes")
	errBadNameChar    = errcode.New(errcode.InvalidArgument, "iqmq/nsq: an NSQ topic or channel name may hold only letters, digits, '.', '_' and '-', optionally followed by '#ephemeral'")
	errNoSubscription = errcode.New(errcode.InvalidArgument, "iqmq/nsq: a subscription is an NSQ channel and cannot be empty")
	errNoWildcards    = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no pattern subscription; a topic is a name and a channel receives only that topic's messages")

	errNilMessage   = errcode.New(errcode.InvalidArgument, "iqmq/nsq: the message is nil")
	errNoHeaders    = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ messages have no headers at all; use MetadataPortableEnvelope or MetadataAdaptive")
	errNoIdentifier = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no field for a message identifier; use MetadataPortableEnvelope or MetadataAdaptive")
	errNoProperties = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no field for a typed property; use MetadataPortableEnvelope or MetadataAdaptive")
	errNoKey        = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no message key; use MetadataPortableEnvelope or MetadataAdaptive")

	errEnvelopeKey           = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: the key exceeds the envelope's MaxKey")
	errEnvelopeBody          = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: the body exceeds the envelope's MaxBody")
	errEnvelopeID            = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: the identifier exceeds the envelope's MaxID")
	errEnvelopeHeaderCount   = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: it carries more headers than the envelope's MaxHeaders")
	errEnvelopeHeaderKey     = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: a header key exceeds the envelope's MaxHeaderKey")
	errEnvelopeHeaderValue   = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: a header value exceeds the envelope's MaxHeaderValue")
	errEnvelopeProperty      = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped: a typed property exceeds the envelope's MaxProp")
	errEnvelopeIDUTF8        = errcode.New(errcode.CorruptMessage, "iqmq/nsq: the message cannot be enveloped: the identifier is not valid UTF-8, which the envelope's decoder refuses")
	errEnvelopeHeaderKeyUTF8 = errcode.New(errcode.CorruptMessage, "iqmq/nsq: the message cannot be enveloped: a header key is not valid UTF-8, which the envelope's decoder refuses")
	errEnvelopePropertyUTF8  = errcode.New(errcode.CorruptMessage, "iqmq/nsq: the message cannot be enveloped: a typed property is not valid UTF-8, which the envelope's decoder refuses")
	errEnvelopeUndiagnosed   = errcode.New(errcode.MessageTooLarge, "iqmq/nsq: the message cannot be enveloped and this driver could not say which field is at fault, which means its diagnosis has drifted from driverkit.Envelope.Check")

	errMetadataMode = errcode.New(errcode.InvalidConfig, "iqmq/nsq: the metadata mode is not one of the four")
)

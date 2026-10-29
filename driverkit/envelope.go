package driverkit

import (
	"context"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/wire"
)

// EnvelopeMarker and EnvelopeMarkerValue are the OUT-OF-BAND record of "this
// body is an envelope", for a driver whose broker has somewhere to keep one.
//
// A driver with native headers, message attributes or user properties must use
// them and must never sniff the body. The reason is
// [Envelope.Unwrap]'s: under [github.com/iqhive/iqmq.MetadataAdaptive] an
// enveloped record and a native one share a subscription, and any marker
// INSIDE the body is forgeable by a payload that contains it — so a body sniff
// decides provenance from evidence the producer of the payload controls, and a
// native publication whose body is a valid envelope is claimed as metadata.
// A header the driver writes itself is evidence the payload cannot reach:
// The specification reserves the "iqmq-" prefix and every driver refuses a caller's header
// under it, so a marker's presence means THIS driver enveloped the record and
// its absence means it did not.
//
// A broker with nowhere to put it — NSQ has nowhere — is the case
// [github.com/iqhive/iqmq/wire.Detector.DetectFull] exists for, and it is strictly
// weaker.
const (
	EnvelopeMarker      = mq.ReservedHeaderPrefix + "env"
	EnvelopeMarkerValue = "1"
)

// EnvelopeConfig configures an [Envelope].
type EnvelopeConfig struct {
	// Mode is the metadata mode. The zero value is
	// MetadataNativeStrict, under which the envelope is never used and
	// unrepresentable metadata is refused before any I/O.
	Mode mq.MetadataMode
	// Layout is the envelope layout. The zero value is Auto,
	// which resolves to the recorded bake-off winner.
	Layout wire.Kind
	// Limits bounds what Decode accepts. Nil means wire.DefaultLimits.
	Limits *wire.Limits
	// Capabilities is the driver's own table, which decides what
	// MetadataAdaptive has to wrap: a broker with native binary headers
	// needs no envelope for one, and a broker with no headers at all needs
	// one for every message that has any.
	Capabilities mq.Capabilities
}

// Envelope is the header shim for a backend whose body is an opaque blob (
// ).
//
// NSQ has no headers at all; SQS's attributes are string-valued and there are at
// most ten; Pub/Sub's attributes cannot carry a duplicate key. The specification promises
// ordered, duplicate-preserving, binary-capable headers everywhere, and the
// envelope is how that promise is kept on a broker that cannot keep it: the
// whole message — key, headers, identifier and typed properties — is encoded
// into the body.
//
// The three modes it implements are the specification, and none of them is reached by
// accident:
//
//   - MetadataNativeStrict, the default, never envelopes. Metadata the broker
//     cannot represent fails with ErrUnsupported before any I/O.
//   - MetadataPortableEnvelope always envelopes, which changes the bytes on the
//     wire and therefore what a non-iqmq consumer reads.
//   - MetadataAdaptive envelopes only the messages that need it, which is
//     [Envelope.Needed].
//   - MetadataLenientDrop never envelopes and never refuses; the driver drops
//     what it cannot carry, with a hook and a metric.
type Envelope struct {
	layout wire.Layout
	// detect tells a refused envelope from a native body under
	// MetadataAdaptive.
	detect *wire.Detector
	limits *wire.Limits
	mode   mq.MetadataMode
	caps   mq.Capabilities
}

// NewEnvelope returns the shim.
func NewEnvelope(cfg EnvelopeConfig) (*Envelope, error) {
	if !cfg.Mode.Valid() {
		return nil, errEnvelopeMode
	}
	if !cfg.Layout.Valid() {
		return nil, errEnvelopeLayout
	}
	layout, err := wire.New(cfg.Layout)
	if err != nil {
		return nil, err
	}
	return &Envelope{
		layout: layout,
		detect: wire.NewDetector(),
		limits: cfg.Limits,
		mode:   cfg.Mode,
		caps:   cfg.Capabilities,
	}, nil
}

// Mode is the metadata mode the shim was built with.
func (e *Envelope) Mode() mq.MetadataMode { return e.mode }

// Needed reports whether m has metadata this backend cannot carry natively, and
// therefore whether MetadataAdaptive has to envelope it.
//
// It reads the capability table rather than a list of driver names: binary
// header values need FeatureBinaryHeaders, a repeated header key needs
// FeatureDuplicateHeaders, and any header at all needs FeatureNativeHeaders.
func (e *Envelope) Needed(m *mq.Message) bool {
	if m == nil {
		return false
	}
	if m.Headers.Len() == 0 && m.ID == "" && m.Properties.IsZero() {
		return false
	}
	if m.Headers.Len() > 0 && !e.caps.Has(mq.FeatureNativeHeaders) {
		return true
	}
	seen := make(map[string]struct{}, m.Headers.Len())
	for k, v := range m.Headers.All() {
		if !e.caps.Has(mq.FeatureBinaryHeaders) && !utf8.Valid(v) {
			return true
		}
		if _, dup := seen[k]; dup && !e.caps.Has(mq.FeatureDuplicateHeaders) {
			return true
		}
		seen[k] = struct{}{}
	}
	return false
}

// Check reports whether [Envelope.Unwrap] would accept the envelope
// [Envelope.Wrap] produces for m — "will what I am about to publish read
// back?", answered before any I/O and without encoding anything.
//
// It is the affordance and the specification asked for three times, in three skins of one
// hole. Wrap used to encode whatever it was given and Unwrap decodes under
// Limits, and the decoder enforces more than the encoder ever did: every bound
// in [github.com/iqhive/iqmq/wire.Limits], and valid UTF-8 in the identifier,
// in every header key and in every string property. A driver that accepted a
// publication and enveloped it could therefore store a record that breaks
// every subsequent Fetch on that subscription for ever — the record cannot be
// decoded, so it cannot be delivered, so it cannot be settled, which is the specification's
// disqualifier reached through a record the driver made itself out of a
// publication it accepted. The fuzz target found the first skin in five
// seconds (a 259-byte header key), the second in another sixty-seven (a
// 259-byte identifier), and FuzzRedisFields found the third — invalid UTF-8 —
// in a tenth of a second.
//
// The limits are the decoder's own, not a copy of its numbers: the specification recorded
// streams.DefaultLimits.MaxHeaderKey = 259 against wire.DefaultLimits's 256 as
// a defect of exactly this kind, made by a driver reproducing the rules by
// hand. A driver outside this repository cannot read internal/wire at all, so
// for it the hole was not merely easy to fall into but unavoidable.
//
// It answers for the envelope CODEC and does not consult the metadata mode, so
// a driver may use it as an admission bound in a mode that never envelopes —
// which is what makes a message accepted in one mode accepted in all of them.
// What the mode decides is whether Wrap consults it, and Wrap does so exactly
// on the path where it encodes.
//
// The failure carries CodeMessageTooLarge for a bound and
// CodeCorruptMessage for invalid UTF-8, which are the codes Unwrap would have
// answered with.
func (e *Envelope) Check(m *mq.Message) error {
	if m == nil {
		return errNilEnvelopeMessage
	}
	return e.layout.Check(m, e.limits)
}

// Wrap returns the message a sink should publish: the original when the mode
// does not envelope it, and a message whose whole body is the envelope when it
// does.
//
// The wrapped message carries no headers and no key of its own: on a broker with
// no header support there is nowhere to put them, and a driver that put half the
// metadata in the envelope and half beside it would produce a message that
// decodes to something different from what was published.
//
// The key is the exception a driver makes for itself, from the ORIGINAL
// message. A broker that partitions, orders, groups or compacts by a native key
// cannot read the envelope, so a driver for one gives the publication the
// native key the message would have had unwrapped: the Kafka record key, the
// Pulsar partition key, ActiveMQ's message group (the AMQP 1.0 group-id and the
// STOMP JMSXGroupID header), the RabbitMQ routing key, the SQS FIFO message
// group and the Pub/Sub ordering key. A record built from the wrapped message
// alone goes out keyless, and per-key ordering goes with it. The decode still
// reads the key from the envelope.
//
// It refuses, before any I/O, anything [Envelope.Check] refuses: an envelope
// Unwrap cannot read back is a record that can be neither delivered nor
// settled, and producing one is worse than declining the publication.
func (e *Envelope) Wrap(m *mq.Message) (*mq.Message, error) {
	if m == nil {
		return nil, errNilEnvelopeMessage
	}
	switch e.mode {
	case mq.MetadataPortableEnvelope:
	case mq.MetadataAdaptive:
		if !e.Needed(m) && !ambiguous(m.Body) {
			return m, nil
		}
	case mq.MetadataNativeStrict, mq.MetadataLenientDrop:
		return m, nil
	}
	// Nothing this function envelopes can be something Check would have
	// refused: an envelope its own Unwrap rejects is an unsettleable record,
	// and refusing here costs a publication that was never admitted.
	if err := e.Check(m); err != nil {
		return nil, err
	}
	body, err := e.layout.Encode(nil, m)
	if err != nil {
		return nil, err
	}
	return &mq.Message{Topic: m.Topic, Body: body}, nil
}

// Unwrap decodes an enveloped message into dst.
//
// dst and src must be different messages. Decode replaces every field of its
// destination and reuses that destination's existing byte storage, so
// unwrapping a message into itself would have the decoder writing into the
// array it is reading from.
//
// A body that is not an envelope is not an error under MetadataAdaptive: the
// message was published natively, so dst takes src unchanged. Under
// MetadataPortableEnvelope a body that is not an envelope IS an error, because
// the mode says every message is one and a body that is not is a corrupt record
// or a producer in a different mode.
//
// The adaptive branch decides from the BODY, which is the best a broker with
// nowhere else to keep the answer can do and is not what a driver with native
// headers should be calling: such a driver writes [EnvelopeMarker] and decodes
// through a second Envelope in MetadataPortableEnvelope mode, which never
// guesses. See EnvelopeMarker.
func (e *Envelope) Unwrap(dst, src *mq.Message) error {
	if dst == nil || src == nil {
		return errNilEnvelopeMessage
	}
	switch e.mode {
	case mq.MetadataPortableEnvelope:
	case mq.MetadataAdaptive:
		native := *src
		if !ambiguous(native.Body) {
			*dst = native
			return nil
		}
		if err := e.layout.Decode(native.Body, dst, e.limits); err != nil {
			// The decode failed, so this is either a native body that
			// begins like an envelope or an envelope this consumer
			// refuses. DetectFull is the only thing that can tell
			// them apart, and the two answers must not be confused: a
			// native body has to be delivered, and a refused envelope has
			// to be refused, because delivering ITS bytes would hand the
			// application a payload nobody published.
			if _, whole := e.detect.DetectFull(native.Body); whole {
				return err
			}
			*dst = native
			return nil
		}
		dst.Topic = native.Topic
		return nil
	case mq.MetadataNativeStrict, mq.MetadataLenientDrop:
		*dst = *src
		return nil
	}
	topic := src.Topic
	if err := e.layout.Decode(src.Body, dst, e.limits); err != nil {
		return err
	}
	dst.Topic = topic
	return nil
}

// ambiguous reports whether a body published natively could be read back as an
// envelope — the question MetadataAdaptive's two halves have to agree on.
//
// It is [github.com/iqhive/iqmq/wire.Detect] rather than
// [github.com/iqhive/iqmq/wire.Detector.DetectFull] on purpose, and the asymmetry is the
// design: the publish side is conservative and costs six bytes, so it envelopes
// anything a reader COULD claim; the read side is exact and costs a decode, so
// it claims only what a reader SHOULD. A conservative publish side envelopes a
// message it did not have to, which costs nothing but interoperability for a
// body that begins with "IQMQ"; a conservative read side would decode a payload
// as metadata, which is the defect.
func ambiguous(body []byte) bool {
	_, ok := wire.Detect(body)
	return ok
}

// WrapSource returns a source that decodes every delivery's envelope as it is
// fetched.
//
// It preserves the settler through the wrapper, extension included, with
// [WrapSettler]: an adapter that forgot it would leave the wrapped driver unable
// to extend a lease with nothing anywhere reporting a failure.
func (e *Envelope) WrapSource(src mq.Source) mq.Source {
	if src == nil {
		return nil
	}
	return &envelopeSource{src: src, env: e}
}

// envelopeSource decodes envelopes on the way out of a Fetch.
type envelopeSource struct {
	src mq.Source
	env *Envelope
}

// Fetch decodes each delivery the wrapped source produced.
//
// A delivery whose envelope this consumer refuses is taken out of the batch
// and dead-lettered, and the rest of the batch is still handed over: every
// delivery in it has already been taken from the source, so stopping at the
// first refusal would leave the ones after it held by nobody. The first
// refusal is returned beside the deliveries that decoded.
func (s *envelopeSource) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	n, err := s.src.Fetch(ctx, dst)
	live := 0
	var derr error
	for i := range n {
		if dst[i].Message != nil {
			var out mq.Message
			if uerr := s.env.Unwrap(&out, dst[i].Message); uerr != nil {
				s.refuse(ctx, &dst[i])
				if derr == nil {
					derr = uerr
				}
				continue
			}
			*dst[i].Message = out
		}
		if live != i {
			dst[live] = dst[i]
		}
		live++
	}
	if derr != nil {
		return live, derr
	}
	return live, err
}

// refuse dead-letters a delivery whose envelope cannot be decoded, which is
// unreadable for ever and so belongs on the dead-letter route. A source with
// no such route refuses the nack, and the delivery is then left unsettled
// rather than destroyed: the broker redelivers it, which is noticed, where a
// message dropped here would not be.
func (s *envelopeSource) refuse(ctx context.Context, d *mq.DriverDelivery) {
	st := s.src.Settler()
	if st == nil {
		return
	}
	_ = st.Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter, Reason: "iqmq/driverkit: the envelope cannot be decoded"})
}

// Settler returns the wrapped source's settler unchanged.
//
// This adapter has nothing to add to a settlement — the envelope is a body
// encoding and not a handle — so it passes the driver's own settler through
// rather than wrapping it. That is the correct form of the rule:
// an adapter either delegates Extend or does not wrap the settler at all. What
// it must never do is return a wrapper of its own that implements Ack and Nack
// and stops there.
func (s *envelopeSource) Settler() mq.Settler { return s.src.Settler() }

// Close closes the wrapped source.
func (s *envelopeSource) Close() error { return s.src.Close() }

// The envelope's failures.
var (
	errEnvelopeMode       = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: the metadata mode is not one of the four")
	errEnvelopeLayout     = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: the envelope layout is not a known kind")
	errNilEnvelopeMessage = errcode.New(errcode.InvalidArgument, "iqmq/driverkit: the message is nil")
)

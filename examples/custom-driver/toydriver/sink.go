package toydriver

import (
	"context"
	"sync/atomic"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/wire"
)

// The sink implements the mandatory publication surface and NOTHING else.
//
// It is not a BatchSink, an AsyncSink, an OwnedSink or a FlushSink, and that
// is the decision (ADR-021, Optional) being taken up on the driver's
// side: the core supplies every one of those loops over Sink.Publish, so a
// driver whose broker has no native batch, no native callback and no buffer to
// flush implements one method and gets the whole publisher API. The
// conformance kit's asynchronous and owned scenarios pass against this driver
// without a line of code here, which is what "a driver that implements nothing
// but this interface works" means.
var _ mq.Sink = (*sink)(nil)

// sink is one open publication channel, bound to its topic for its lifetime
// (ADR-021, Bound).
type sink struct {
	b     *Broker
	topic string
	mode  mq.MetadataMode
	env   *driverkit.Envelope
	// closed is atomic rather than guarded because Publish must not take
	// the broker's mutex to find out it is closed.
	closed atomic.Bool
}

// OpenSink opens a publication channel to one destination.
//
// Two things are refused here rather than at the first message, both because
// say a caller learns what it cannot have at construction:
// ConfirmDurable, which this broker has no durable boundary to define, and a
// metadata mode outside the four.
func (b *Broker) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Confirmation.Resolve() == mq.ConfirmDurable {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoDurableConfirm)
	}
	b.mu.Lock()
	_, known := b.topics[cfg.Topic]
	b.mu.Unlock()
	if !known {
		// The specification: nothing creates topology behind an operator's back, so a
		// sink on a topic nobody created fails at open. Doing it here and
		// not at the first publication is what makes a misconfigured
		// destination a startup failure.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoSuchTopic)
	}
	env, err := b.envelope(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	return &sink{b: b, topic: cfg.Topic, mode: cfg.Metadata, env: env}, nil
}

// envelopeLimits bounds what this broker's headers may be, and it is the WIRE
// layout's own limit set rather than a number this driver invented.
//
// It has to be, and a fuzz target found out why in five seconds.
// driverkit.Envelope's two halves are not symmetric: Wrap encodes whatever it
// is given, and Unwrap decodes under wire.Limits, so a driver that left both at
// their defaults accepted a message with a 259-byte header key — one byte over
// wire.DefaultLimits.MaxHeaderKey — and then could never deliver it. Publish
// returned nil, the record sat in the queue, and every Fetch after it failed
// with "wire: field exceeds decode limit". The committed input
// testdata/fuzz/FuzzMetadataModes/c8ed1898166b7081 is that message.
//
// So the limits are named once, checked before the publication is admitted,
// and handed to the envelope on both sides. What driverkit
// does not offer is the check itself; that is recorded in the maintenance notes.
var envelopeLimits = wire.DefaultLimits

// envelope builds the metadata shim for one handle.
func (b *Broker) envelope(mode mq.MetadataMode) (*driverkit.Envelope, error) {
	lim := envelopeLimits
	return driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mode,
		Limits:       &lim,
		Capabilities: b.caps,
	})
}

// checkRepresentable refuses a message this broker cannot carry.
//
// Every bound is wire.Limits', field for field, and the reason it is the WHOLE
// struct and not just the headers is the second thing the fuzzer found: the
// first failure was a 259-byte header key, and the fix that bounded only
// headers survived sixty-seven more seconds before it produced a 259-byte
// message IDENTIFIER, one byte over wire.DefaultLimits.MaxID, with exactly the
// same symptom. Key, body and each string property are bounded here for the
// same reason and not because a fuzzer has reached them yet.
//
// The limit applies in every metadata mode, because it is the BROKER's limit
// and not an artefact of the envelope: a real broker's attribute-name bound
// (256 bytes, for SQS as it happens) does not depend on how iqmq chose to
// encode the message. Limits has a field for the header COUNT and for
// the message size, which are advertised so the core can refuse before any
// I/O; the lengths of one key, one header key, one header value, one
// identifier and one property have no field, so they are the driver's to
// check.
func checkRepresentable(m *mq.Message) error {
	if m == nil {
		return nil
	}
	if len(m.Key) > envelopeLimits.MaxKey ||
		len(m.Body) > envelopeLimits.MaxBody ||
		len(m.ID) > envelopeLimits.MaxID {
		return errUnrepresentableSize
	}
	if m.Headers.Len() > envelopeLimits.MaxHeaders {
		return errUnrepresentableSize
	}
	for k, v := range m.Headers.All() {
		if len(k) > envelopeLimits.MaxHeaderKey || len(v) > envelopeLimits.MaxHeaderValue {
			return errUnrepresentableSize
		}
	}
	for _, prop := range [...]string{
		m.Properties.ContentType,
		m.Properties.ContentEncoding,
		m.Properties.CorrelationID,
		m.Properties.CausationID,
		m.Properties.ReplyTo,
	} {
		if len(prop) > envelopeLimits.MaxProp {
			return errUnrepresentableSize
		}
	}
	return nil
}

// Publish admits one message.
//
// The strict-mode refusal is the line a driver author is most likely to miss.
// driverkit.Envelope.Needed answers "does this message carry metadata this
// broker cannot represent natively?", and Envelope.Wrap deliberately does NOT
// refuse under MetadataNativeStrict — it returns the message unchanged,
// because wrapping is not what strict mode does. The specification says strict mode fails
// with ErrUnsupported BEFORE any I/O, so the refusal is the driver's, and a
// driver that only calls Wrap silently accepts a binary header and drops it.
func (s *sink) Publish(ctx context.Context, m *mq.Message, _ *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	if s.mode == mq.MetadataNativeStrict && s.env.Needed(m) {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", errUnrepresentable)
	}
	if err := checkRepresentable(m); err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	out, err := s.env.Wrap(m)
	if err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	return s.b.publish(ctx, s.topic, out)
}

// Close releases the sink. It is idempotent, and a Publish afterwards returns
// ErrClosed.
func (s *sink) Close() error {
	s.closed.Store(true)
	return nil
}

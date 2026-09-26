package sqs

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/sqs/internal/sqsapi"
)

// Encoder is this driver's mapping, exported so that a driver
// BUILT ON this one maps a message exactly as this one does.
//
// It exists for `drivers/snssqs`, whose publications go to SNS and whose
// deliveries come back out of an SQS queue. The two services take the SAME
// message shape — a text body under one character set, and at most ten
// name/type/value attributes — so the fan-out driver's only real difference on
// the publish path is which SDK struct the attribute map is made of. Writing
// the mapping twice was the alternative, and it is four hundred lines of
// metadata policy that would drift: the correction FuzzSQSAttributes forced on
// [valueAttr] (the service's character set, not merely valid UTF-8) would have
// landed in one copy.
//
// Nothing here is new behaviour. Every refusal, the envelope decision, the
// reserved-prefix check, the body policy and the size accounting are the same
// code the sink and the source run.
type Encoder struct{ enc *encoder }

// Publication is one message in the shape the service takes it, which is also
// the shape SNS takes it: [Publication.Attrs] is the SQS SDK's attribute
// struct, and a driver publishing to another service translates it field for
// field.
type Publication struct {
	// Body is the MessageBody, already through the binary-body policy of
	// [BodyEncoding]; a body the service cannot carry is base64 and is
	// marked in Attrs.
	Body string
	// Attrs are the message attributes, at most [MaxAttributes] of them.
	Attrs map[string]types.MessageAttributeValue
	// GroupID is the FIFO MessageGroupId, empty on a standard queue.
	GroupID string
	// DedupID is the FIFO MessageDeduplicationId, empty when the caller
	// named no DedupKey.
	DedupID string
	// Delay is DelaySeconds, zero when the caller asked for no delay. A
	// service with no per-message timer at all — SNS is one — refuses the
	// option itself and never sees a non-zero value here.
	Delay int32
	// Enveloped reports that Body is an iqmq envelope rather than the
	// caller's payload, which is what a consumer needs in order to unwrap.
	Enveloped bool
	// Bytes is the accounted size: the body plus the attribute names and
	// values, which is what the service's message limit counts.
	Bytes int
}

// NewEncoder builds the mapping for one publisher or consumer.
//
// cfg is resolved first, so a zero [Config.MaxMessageBytes] means the service's
// limit rather than a limit of zero — which is what a caller constructing this
// directly would otherwise hit on its first publication. caps is the CALLER's
// capability table, because it is what decides whether MetadataAdaptive has to
// envelope: a driver whose table differs from this one's gets its own answer.
func NewEncoder(cfg Config, caps mq.Capabilities, mode mq.MetadataMode) (*Encoder, error) {
	enc, err := newEncoder(cfg.resolve(), caps, mode)
	if err != nil {
		return nil, err
	}
	return &Encoder{enc: enc}, nil
}

// Encode turns one message and its resolved publication options into the
// service's shape, refusing everything that can be refused before any I/O.
func (e *Encoder) Encode(m *mq.Message, o *mq.PublishOptions) (Publication, error) {
	out, err := e.enc.encode(m, o)
	if err != nil {
		return Publication{}, err
	}
	// A conversion rather than a field-by-field copy, and that is the
	// stricter choice: it compiles only while the internal shape and the
	// exported one are identical, so a field added to the encoder's own
	// struct and not to this one is a build failure rather than a value the
	// exported view silently omits.
	return Publication(out), nil
}

// Decode turns one received message into a portable one, overwriting dst.
//
// It is the inverse of [Encoder.Encode] and is exported for the same reason:
// a driver whose deliveries are SQS messages published by another service must
// read them exactly as this driver reads its own, and its fuzz target must be
// able to close the round trip.
func (e *Encoder) Decode(src *types.Message, topic string, dst *mq.Message) error {
	return e.enc.decode(src, topic, dst)
}

// Dropped is how many headers this encoder has discarded under
// MetadataLenientDrop, which is the metric.
func (e *Encoder) Dropped() int64 { return e.enc.Dropped() }

// MaxMessageAttributes is the attribute budget [Encoder.Encode] enforces, and
// the number a driver built on this one advertises as its own header limit
// minus the marker this driver reserves.
const MaxMessageAttributes = MaxAttributes

// API is the slice of the SQS API this driver is built over, exported so that
// a driver BUILT ON this one can be built over the same one.
//
// It is an alias rather than a declaration because it has to be the same type
// [BackendWithAPI] takes: `drivers/snssqs` composes this driver for its whole
// consume path, so it needs to name the interface in order to pass a client
// through — a value of type `any` is not assignable to an interface type, and
// an internal package's name is not reachable from another module.
//
// *sqs.Client implements it, and so does
// [github.com/iqhive/iqmq/drivers/sqs/sqsfake.Server].
type API = sqsapi.API

// ParseBodyEncoding is [BodyEncoding]'s slug parser.
//
// It is exported for the same reason [Encoder] is: a driver built on this one
// takes the same `body=auto|text|base64` DSN parameter and must map the names
// to the same policies. Two parsers for one enumeration is two places for a
// name to mean something different.
func ParseBodyEncoding(s string) (BodyEncoding, bool) { return parseBodyEncoding(s) }

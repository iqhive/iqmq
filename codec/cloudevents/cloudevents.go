// Package cloudevents carries CloudEvents 1.0 through iqmq.
//
// # CloudEvents is a binding, and the root Codec interface cannot express one
//
// The CloudEvents specification defines two content modes, and they are not
// two encodings of one thing:
//
//   - **Structured mode** puts the whole event -- context attributes and data
//     together -- in the body, with content type
//     `application/cloudevents+json`. One body, nothing else.
//   - **Binary mode** puts every context attribute in a TRANSPORT HEADER
//     (`ce-id`, `ce-type`, `ce-source`, ...) and the data alone in the body,
//     with the content type of the data itself.
//
// codec.Codec is `Marshal(v any) ([]byte, error)`: a body in, a body out, and
// no way to say anything about headers. So **the codec.Codec this package
// provides is structured mode, and binary mode is a separate type** --
// Binding -- whose Encode returns a body AND a header set and whose Decode
// takes both. Binding is not a codec.Codec and cannot be registered in a
// codec.Registry; it is used at the publish and receive call sites, against
// mq.Message.Headers.
//
// That is a limitation of the root contract rather than of this package, and
// it is recorded the first codec whose format is a
// transport binding cannot be installed as a codec at all for half of what it
// does. Two alternatives were rejected. Widening codec.Codec to return headers
// would change the interface every existing codec implements and the typed
// helpers wrap, for one codec's benefit. Smuggling the attributes into the
// body under binary mode would produce something that is neither content mode
// and that no other CloudEvents implementation can read -- which defeats the
// only reason to use CloudEvents.
//
// # Which iqmq metadata maps to which attribute
//
// Decided here, because a binding that leaves it implicit is not a binding:
//
//	ce-specversion      always "1.0"; a body declaring anything else is refused
//	ce-id               the EVENT's id, which the caller sets. It is NOT iqmq's
//	                    message id: that is assigned by the driver or the
//	                    broker at publish time and does not exist when the
//	                    codec runs, so mapping it here would either be empty or
//	                    be a second, different identifier. The message id
//	                    and a CloudEvent's id are different identities and this
//	                    package keeps them separate.
//	ce-source           the event's source URI
//	ce-type             the event's type
//	ce-subject          the event's subject, optional
//	ce-time             the event's time, RFC 3339. iqmq has no publish-time
//	                    field of its own, so nothing is derived from it and
//	                    nothing populates it.
//	ce-datacontenttype  the data's own media type. In BINARY mode it is also
//	                    the message's `content-type` header, because that is
//	                    what the CloudEvents HTTP binding says; in structured
//	                    mode `content-type` is application/cloudevents+json and
//	                    the data's type stays an attribute.
//	ce-dataschema       the data's schema URI. It is deliberately NOT copied to
//	                    codec.HeaderSchemaID: that header is documented as "the
//	                    schema a schema-registry codec encoded with", an
//	                    identifier a registry resolves, and a dataschema URI is
//	                    not one.
//	ce-partitionkey     the CloudEvents Partitioning extension, mapped to and
//	                    from iqmq's own partition key. This is the one
//	                    genuine two-way mapping, and it exists because both
//	                    sides already mean the same thing.
//	ce-<name>           every other extension, lower-cased (CloudEvents
//	                    attribute names are already restricted to lower-case
//	                    letters and digits).
//
// The prefix is `ce-` by default, which is the HTTP and MQTT binding's form
// and the one iqmq's lower-case-kebab header convention (the specification) matches. The
// Kafka binding uses `ce_`, so Config.HeaderPrefix exists; the AMQP binding's
// `cloudEvents:` form is NOT offered, because a colon in a header name is
// refused by drivers whose header block is a text line. Choosing the prefix
// from the driver was rejected: the codec does not know the driver, and iqmq's
// header contract is portable on purpose.
//
// # What this package does NOT do
//
//   - **No JSON batch mode.** `application/cloudevents-batch+json` is a JSON
//     array of events and one iqmq message is one event; a batch belongs in
//     PublishBatch, not in a body.
//   - **No format other than JSON.** The Avro, Protobuf and XML CloudEvents
//     formats exist; each would be a content type and a dependency of its own.
//   - **No transport-specific binding.** This is the generic attribute-to-
//     header mapping above. A driver that has a native place for an attribute
//     (a Kafka key, a Pub/Sub ordering key) is served by the partition-key
//     mapping and nothing else.
//   - **Almost no validation beyond the specification's.** event.Event.Validate
//     is what decides whether something is a CloudEvent and this package calls
//     it on both sides. Two rules are added, because the SDK does not enforce
//     them and an event that breaks either does not mean one thing: a
//     structured body must be a JSON OBJECT (the SDK accepts one with no
//     opening brace at all), and an extension name may not collide with a
//     context attribute name (the SDK lower-cases extension names, so a
//     member called "dAtA" becomes an extension called "data" beside the real
//     one and the event re-encodes to a different body). Both were found by
//     FuzzCloudEvents and both are what the specification already says.
//   - **No auto-detection between the two content modes**. A Codec
//     decodes structured bodies and a Binding decodes binary ones, and each
//     refuses the other's shape.
//   - **No silent normalisation in binary mode.** An attribute value with
//     leading or trailing whitespace cannot travel in a transport header --
//     HTTP strips optional whitespace by definition, and the SDK's own setters
//     trim, so `type: "0 "` comes back as `type: "0"`. Structured mode carries
//     it faithfully and binary mode refuses it (ErrUnrepresentable) rather
//     than changing the event on the way past. Found by FuzzCloudEvents,
//     whose fifth property is that the two modes agree.
//
// # Untrusted input (the security checks)
//
// MaxBytes is checked against the body before the decoder is handed anything.
// Binary mode adds a count limit that structured mode does not need:
// MaxAttributes bounds how many `ce-` headers are read, because a delivery
// with a hundred thousand of them would otherwise build a hundred thousand
// extensions on the receive path -- an allocation the SENDER chooses. Both are
// checked before anything is built.
//
// All values returned by this package are stateless and safe for concurrent
// use by multiple goroutines.
package cloudevents

import (
	"strings"

	"github.com/cloudevents/sdk-go/v2/event"

	"bitbucket.org/iqhive/iqmq/codec"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Content types and header names this package uses.
const (
	// ContentType is the structured-mode media type, and what the codec.Codec
	// in this package produces.
	ContentType = "application/cloudevents+json"
	// ContentTypeBatch is the batch media type. It is named so that a body
	// carrying one can be recognised and refused rather than mis-parsed; this
	// package does not implement batch mode.
	ContentTypeBatch = "application/cloudevents-batch+json"
	// DefaultHeaderPrefix is the binary-mode attribute prefix of the HTTP and
	// MQTT bindings.
	DefaultHeaderPrefix = "ce-"
	// KafkaHeaderPrefix is the binary-mode attribute prefix of the Kafka
	// binding.
	KafkaHeaderPrefix = "ce_"
	// SpecVersion is the only CloudEvents version this package reads or
	// writes.
	SpecVersion = "1.0"
	// PartitionKeyExtension is the CloudEvents Partitioning extension's
	// attribute name, mapped to and from iqmq's partition key.
	PartitionKeyExtension = "partitionkey"
)

// Defaults for Config.
const (
	// DefaultMaxBytes is the largest body Unmarshal or Binding.Decode will
	// read.
	DefaultMaxBytes = 8 << 20
	// DefaultMaxAttributes is the largest number of prefixed headers
	// Binding.Decode will read from a delivery.
	DefaultMaxAttributes = 64
)

// Errors this package reports.
var (
	// ErrNotEvent (InvalidArgument) reports a value that is not an
	// event.Event.
	ErrNotEvent = errcode.New(errcode.InvalidArgument, "codec/cloudevents: value is not a cloudevents event")
	// ErrTooLarge (MessageTooLarge) reports a body over Config.MaxBytes,
	// before anything is allocated from it.
	ErrTooLarge = errcode.New(errcode.MessageTooLarge, "codec/cloudevents: body exceeds the configured maximum")
	// ErrTooManyAttributes (MessageTooLarge) reports a delivery with more
	// prefixed headers than Config.MaxAttributes, before any extension is
	// built.
	ErrTooManyAttributes = errcode.New(errcode.MessageTooLarge, "codec/cloudevents: delivery exceeds the configured attribute count")
	// ErrSpecVersion (CorruptMessage) reports an event whose specversion is
	// not 1.0.
	ErrSpecVersion = errcode.New(errcode.CorruptMessage, "codec/cloudevents: unsupported specversion")
	// ErrBatch (CorruptMessage) reports a body that is a CloudEvents JSON
	// batch, which this package does not implement.
	ErrBatch = errcode.New(errcode.CorruptMessage, "codec/cloudevents: body is a cloudevents batch, which is not supported")
	// ErrInvalidEvent (CorruptMessage) reports a decoded event that
	// event.Event.Validate rejects: a missing id, source, type or
	// specversion. Something that is not a CloudEvent is not a message this
	// codec can hand on.
	ErrInvalidEvent = errcode.New(errcode.CorruptMessage, "codec/cloudevents: body is not a valid cloudevent")
	// ErrNotObject (CorruptMessage) reports a structured body that is not a
	// JSON object. The CloudEvents JSON format is an object; the SDK's
	// decoder is lenient enough to accept a body that never had an opening
	// brace, which is a body that was never an event.
	ErrNotObject = errcode.New(errcode.CorruptMessage, "codec/cloudevents: body is not a JSON object")
	// ErrReservedExtension (CorruptMessage) reports an event carrying an
	// extension whose name is a reserved context-attribute name. The
	// CloudEvents specification forbids the collision, and the SDK does not:
	// it lower-cases extension names, so a member called "dAtA" becomes an
	// extension called "data" beside the real one. Such an event re-encodes
	// to a DIFFERENT body -- found by FuzzCloudEvents, which is what the
	// round-trip property is for.
	ErrReservedExtension = errcode.New(errcode.CorruptMessage, "codec/cloudevents: extension name collides with a context attribute")
	// ErrUnrepresentable (InvalidArgument) reports an event BINARY mode
	// cannot carry: a context attribute or extension value with leading or
	// trailing whitespace. A transport header cannot hold one -- HTTP strips
	// optional whitespace by definition, and the SDK's own setters trim -- so
	// the value would come back changed. Structured mode carries it
	// unchanged, which is why the refusal is on the binding and not on the
	// codec.
	ErrUnrepresentable = errcode.New(errcode.InvalidArgument, "codec/cloudevents: attribute value has leading or trailing whitespace, which binary mode cannot carry")
	// ErrNoAttributes (CorruptMessage) reports a binary-mode delivery with no
	// prefixed headers at all, which is a body that was never a CloudEvent
	// rather than an event with nothing in it.
	ErrNoAttributes = errcode.New(errcode.CorruptMessage, "codec/cloudevents: delivery carries no cloudevents attributes")
)

// Config configures both the structured codec and the binary binding.
type Config struct {
	// MaxBytes is the largest body that is decoded. Zero means
	// DefaultMaxBytes; negative removes the limit.
	MaxBytes int
	// MaxAttributes bounds how many prefixed headers Binding.Decode reads.
	// Zero means DefaultMaxAttributes; negative removes the limit.
	MaxAttributes int
	// HeaderPrefix is the binary-mode attribute prefix. Empty means
	// DefaultHeaderPrefix ("ce-"); a Kafka deployment sets KafkaHeaderPrefix.
	HeaderPrefix string
	// Validate runs event.Event.Validate on encode and decode. It defaults to
	// ON; SkipValidation turns it off for a caller who has already validated.
	SkipValidation bool
}

func (c Config) maxBytes() int {
	switch {
	case c.MaxBytes == 0:
		return DefaultMaxBytes
	case c.MaxBytes < 0:
		return -1
	default:
		return c.MaxBytes
	}
}

func (c Config) maxAttributes() int {
	switch {
	case c.MaxAttributes == 0:
		return DefaultMaxAttributes
	case c.MaxAttributes < 0:
		return -1
	default:
		return c.MaxAttributes
	}
}

func (c Config) prefix() string {
	if c.HeaderPrefix == "" {
		return DefaultHeaderPrefix
	}
	return c.HeaderPrefix
}

// New returns the STRUCTURED-mode codec with the default configuration. It is
// a codec.Codec and a codec.Appender; binary mode is NewBinding.
func New() codec.Codec { return NewWithConfig(Config{}) }

// NewWithConfig returns the structured-mode codec configured by c.
func NewWithConfig(c Config) codec.Codec {
	return structured{limit: c.maxBytes(), validate: !c.SkipValidation}
}

// Typed returns the structured-mode codec bound to event.Event.
func Typed() codec.Typed[event.Event] { return codec.Adapt[event.Event](New()) }

// Register installs the structured-mode codec in r under
// application/cloudevents+json.
func Register(r *codec.Registry) error { return r.Register(New()) }

// structured is the structured-mode codec: the whole event in the body.
type structured struct {
	limit    int
	validate bool
}

// ContentType implements codec.Codec.
func (structured) ContentType() string { return ContentType }

// Marshal implements codec.Codec.
func (s structured) Marshal(v any) ([]byte, error) {
	e, err := eventOf(v)
	if err != nil {
		return nil, err
	}
	if s.validate {
		if verr := e.Validate(); verr != nil {
			return nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: "+verr.Error())
		}
	}
	if xerr := checkExtensions(e); xerr != nil {
		return nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: "+xerr.Error())
	}
	return e.MarshalJSON()
}

// AppendMarshal implements codec.Appender.
//
// The SDK has no append entry point -- event.WriteJson takes an io.Writer but
// buffers the whole value first -- so the encoding is built and copied. The specification's
// pinned failure contract is honoured by returning the caller's own buffer
// untouched on failure, which here is free: nothing is written until the
// encoding has succeeded.
func (s structured) AppendMarshal(dst []byte, v any) ([]byte, error) {
	b, err := s.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

// Unmarshal implements codec.Codec. The size limit is checked before the
// decoder is handed anything, and a batch body is refused by name rather
// than mis-parsed.
func (s structured) Unmarshal(data []byte, v any) error {
	if s.limit >= 0 && len(data) > s.limit {
		return ErrTooLarge
	}
	t, err := codec.Target(v)
	if err != nil {
		return err
	}
	e, ok := t.(*event.Event)
	if !ok {
		return ErrNotEvent
	}
	switch shapeOf(data) {
	case '[':
		return &codec.DecodeError{ContentType: ContentType, Err: ErrBatch}
	case '{':
	default:
		return &codec.DecodeError{ContentType: ContentType, Err: ErrNotObject}
	}
	*e = event.Event{}
	if derr := e.UnmarshalJSON(data); derr != nil {
		return &codec.DecodeError{ContentType: ContentType, Err: derr}
	}
	if e.SpecVersion() != SpecVersion {
		return &codec.DecodeError{ContentType: ContentType, Err: ErrSpecVersion}
	}
	if s.validate {
		if verr := e.Validate(); verr != nil {
			return &codec.DecodeError{ContentType: ContentType, Err: ErrInvalidEvent}
		}
	}
	// Checked whatever SkipValidation says: a reserved extension name is not
	// a strictness preference, it is a body that does not mean one thing.
	if xerr := checkExtensions(e); xerr != nil {
		return &codec.DecodeError{ContentType: ContentType, Err: xerr}
	}
	return nil
}

// shapeOf reports the first significant byte of a body, or 0 when it is empty
// or all whitespace. It decides between "this is an object" and the two ways a
// body can be refused; it never switches content modes, which the specification forbids.
func shapeOf(data []byte) byte {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return b
		}
	}
	return 0
}

// reservedAttributes are the CloudEvents 1.0 context attribute names and the
// two JSON-format data members. An extension may not be called any of them,
// and the SDK does not enforce it.
var reservedAttributes = map[string]bool{
	"specversion": true, "id": true, "source": true, "type": true,
	"subject": true, "time": true, "datacontenttype": true, "dataschema": true,
	"data": true, "data_base64": true,
}

// checkExtensions reports an extension whose name is reserved.
func checkExtensions(e *event.Event) error {
	for name := range e.Extensions() {
		if reservedAttributes[lower(name)] {
			return ErrReservedExtension
		}
	}
	return nil
}

// eventOf resolves v to an event, following the indirection codec.Adapt adds.
func eventOf(v any) (*event.Event, error) {
	switch t := codec.Indirect(v).(type) {
	case *event.Event:
		if t == nil {
			return nil, ErrNotEvent
		}
		return t, nil
	case event.Event:
		return &t, nil
	default:
		return nil, ErrNotEvent
	}
}

// lower lower-cases an ASCII string without allocating when it is already
// lower-case. CloudEvents attribute names are lower-case letters and digits,
// so the fast path is the only path for a conforming sender.
func lower(s string) string {
	for i := range len(s) {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			return strings.ToLower(s)
		}
	}
	return s
}

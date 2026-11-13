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
//	ce-datacontenttype  the data's own media type. In BINARY mode Encode
//	                    writes it twice, as this header and as the message's
//	                    `content-type` header. That is NOT what the
//	                    CloudEvents HTTP binding says: its §3.1.1 maps the
//	                    attribute to Content-Type alone, and a
//	                    ce-datacontenttype header MUST NOT also be present.
//	                    Both are written because codec/cloudevents v1.0 read
//	                    only this header, and because a driver may refuse a
//	                    header called content-type (the STOMP arm of
//	                    drivers/activemq, where it is a frame header), so the
//	                    type then travels in Properties.ContentType, which a
//	                    Binding cannot see. Decode takes this header when it
//	                    is there and `content-type` otherwise, so a
//	                    conforming sender's type is read. In structured mode
//	                    `content-type` is application/cloudevents+json and
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
// # Data under a JSON media type
//
// In a structured body, data whose datacontenttype is a JSON media type, or
// which has no datacontenttype (CloudEvents then reads it as JSON), is written
// as a JSON value, and it must be exactly one. Marshal refuses an event whose
// data is not one JSON value with an InvalidArgument error, and Unmarshal
// refuses such a body as corrupt. The SDK wrote that data as it stood, so an
// event from a binary-mode delivery whose body was not JSON, or a hand-built
// one, re-encoded to a body that was not JSON, or to one whose extra members
// overrode the event's type and source once it was decoded again.
//
// A media type with the +json suffix (application/vnd.acme+json, RFC 6839) is
// JSON too. Unmarshal reads its data either as a JSON value, the form the JSON
// format and other SDKs write, or as a JSON string holding the data, the form
// sdk-go writes, and Marshal writes it as sdk-go does, as a JSON string.
//
// A media type is read without regard to case, or to space around it, when a
// structured body is decoded ("Application/JSON" is JSON), and the attribute is
// kept as it was sent. The SDK's Event.DataAs reads the media type exactly as
// written and knows only the lower-case names, so it reports an unsupported
// content type for such an event, from either content mode: decode e.Data()
// with encoding/json there.
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
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
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
	// It is used lower-cased, as every header name in this package is.
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

// Marshal implements codec.Codec. It refuses, with an InvalidArgument error,
// an event whose specversion is not 1.0 and one whose data is not the JSON its
// datacontenttype says it is (see "Data under a JSON media type").
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
	// As Binding.Encode refuses it, and Unmarshal a body that declares
	// another version. The SDK takes a 0.3 event's data as base64 from
	// datacontentencoding rather than from DataBase64, so a 0.3 event got past
	// the check below with data that rewrote its type and source.
	if e.SpecVersion() != SpecVersion {
		return nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: specversion "+e.SpecVersion()+" is not "+SpecVersion)
	}
	if xerr := checkExtensions(e); xerr != nil {
		return nil, errcode.New(errcode.InvalidArgument, "codec/cloudevents: "+xerr.Error())
	}
	// The SDK writes such data after `"data":` as it stands, so data that is
	// not one JSON value made a body that is not JSON, or one whose extra
	// members override the event's own attributes once it is decoded.
	if writesDataRaw(e) && !json.Valid(e.DataEncoded) {
		return nil, errDataNotJSONToEncode
	}
	return e.MarshalJSON()
}

// Errors for data that is not the JSON its datacontenttype says it is.
var (
	errDataNotJSONToEncode = errcode.New(errcode.InvalidArgument,
		"codec/cloudevents: data is not one JSON value, and datacontenttype says it is JSON")
	errDataNotJSON = errcode.New(errcode.CorruptMessage,
		"codec/cloudevents: data is not one JSON value, and datacontenttype says it is JSON")
)

// writesDataRaw reports whether the SDK's encoder writes e's data into a
// structured body as it stands, rather than as a JSON string: data that is
// not base64, under a media type that is JSON once lower-cased and trimmed,
// or under none, which CloudEvents reads as JSON. It lower-cases as the SDK
// does, with strings.ToLower: U+0130 lower-cases to an ASCII i there.
func writesDataRaw(e *event.Event) bool {
	if e.DataBase64 || e.DataEncoded == nil {
		return false
	}
	mt := e.DataContentType()
	if i := strings.IndexByte(mt, ';'); i >= 0 {
		mt = mt[:i]
	}
	return sdkJSON(strings.TrimSpace(strings.ToLower(mt)))
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
// than mis-parsed. The target is checked first, so a target that is the
// caller's error is reported as one whatever the body holds.
//
// A datacontenttype whose media type is JSON in another case or with space
// around it ("Application/JSON") decodes to the event the lower-case type
// gives, with the attribute kept as it was sent, and data under a +json media
// type may be a JSON value: see "Data under a JSON media type" in the package
// comment, and decodeEvent.
func (s structured) Unmarshal(data []byte, v any) error {
	t, err := codec.Target(v)
	if err != nil {
		return err
	}
	e, ok := t.(*event.Event)
	if !ok {
		return ErrNotEvent
	}
	if s.limit >= 0 && len(data) > s.limit {
		return ErrTooLarge
	}
	switch shapeOf(data) {
	case '[':
		return &codec.DecodeError{ContentType: ContentType, Err: ErrBatch}
	case '{':
	default:
		return &codec.DecodeError{ContentType: ContentType, Err: ErrNotObject}
	}
	*e = event.Event{}
	if derr := decodeEvent(data, e); derr != nil {
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

// decodeEvent is event.Event.UnmarshalJSON with the SDK's two readings of
// datacontenttype made one, a +json media type read as JSON, and data read as
// JSON held to being JSON.
//
// cloudevents/sdk-go v2.16.2 decides whether "data" holds JSON from the media
// type exactly as written when it decodes (case kept, and space kept unless a
// parameter follows) and from the media type lower-cased and trimmed when it
// encodes. Media types are case-insensitive (RFC 9110 §8.3.1). So for
// "Application/JSON" the decoder read string data as a string, or refused an
// object, and the encoder wrote the string back raw: `"data":abc}`, a body no
// JSON decoder accepts, this codec's own included. FuzzCloudEvents found it.
// Such a body is decoded again with its media type in the form the decoder
// reads as JSON, and the attribute is then put back as it was sent. The event
// is the one the lower-case type gives, and it re-encodes to the same body.
//
// The SDK knows no structured syntax suffix: under application/vnd.acme+json
// it reads data only as a JSON string, the form it writes, and refused the
// JSON value other SDKs write there (the JSON format has data under a JSON
// media type written as a JSON value, and RFC 6839 makes a +json type one).
// A body the SDK refuses under such a type is decoded again as if the type
// were application/json, and the attribute is put back. String data is read
// as before, as the string's contents.
//
// The SDK's decoder does not check a number it skips, so `"data":-` was
// accepted as data the encoder then wrote back as it stood. Data read as JSON
// that is not one JSON value is refused, as Marshal refuses it.
func decodeEvent(data []byte, e *event.Event) error {
	derr := e.UnmarshalJSON(data)
	if derr == nil && (e.DataBase64 || e.DataEncoded == nil || !foldsToJSON(e.DataContentType())) {
		if writesDataRaw(e) && !json.Valid(e.DataEncoded) {
			return errDataNotJSON
		}
		return nil
	}
	if derr != nil && !bytes.Contains(data, []byte(`"datacontenttype"`)) {
		return derr
	}
	// A +json type is folded only for a body the SDK refused: under one it
	// reads string data as the string's contents, which is its own form.
	dct, folded, ok := foldDataContentType(data, derr != nil)
	if !ok {
		if derr != nil {
			return derr
		}
		return errUnfoldable
	}
	*e = event.Event{}
	if err := e.UnmarshalJSON(folded); err != nil {
		if derr != nil {
			return derr
		}
		return err
	}
	if ctx, v1 := e.Context.(*event.EventContextV1); v1 && ctx.DataContentType != nil {
		ctx.DataContentType = &dct
	}
	// The folded type is JSON, so data that is not base64 was read as JSON.
	if !e.DataBase64 && e.DataEncoded != nil && !json.Valid(e.DataEncoded) {
		return errDataNotJSON
	}
	return nil
}

// errUnfoldable is the reason for a body decodeEvent cannot decode the way
// its encoder would write it back: its datacontenttype member could not be
// found again to fold.
var errUnfoldable = errcode.New(errcode.CorruptMessage,
	"codec/cloudevents: datacontenttype is a JSON media type the SDK does not read as one")

// foldsToJSON reports whether the SDK encodes "data" under dct as JSON but
// decodes it as something else: whether its media type is JSON only once
// lower-cased and trimmed.
func foldsToJSON(dct string) bool {
	mt, sdkMT := dct, dct
	if i := strings.IndexByte(dct, ';'); i >= 0 {
		mt, sdkMT = dct[:i], strings.TrimSpace(dct[:i])
	}
	return !sdkJSON(sdkMT) && sdkJSON(strings.TrimSpace(strings.ToLower(mt)))
}

// sdkJSON is the SDK's own list of the media types whose data is JSON, the
// empty one included.
func sdkJSON(mt string) bool {
	switch mt {
	case "", event.ApplicationJSON, event.TextJSON, event.ApplicationCloudEventsJSON, event.ApplicationCloudEventsBatchJSON:
		return true
	}
	return false
}

// foldDataContentType finds the first top-level datacontenttype member of a
// JSON object and, when foldedMediaType gives a form for it, returns its value
// and a copy of data with that form in its place, the rest of the body byte
// for byte as it was. ok is false when there is no such member, it is not a
// string, or it needs no folding.
//
// It uses encoding/json rather than encoding/json/jsontext, which Go 1.27
// builds only under the jsonv2 experiment: a build with GOEXPERIMENT=nojsonv2
// must compile this package.
func foldDataContentType(data []byte, plusJSON bool) (dct string, folded []byte, ok bool) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return "", nil, false
	}
	for dec.More() {
		name, err := dec.Token()
		if err != nil {
			return "", nil, false
		}
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return "", nil, false
		}
		if name != "datacontenttype" {
			continue
		}
		if len(val) == 0 || val[0] != '"' || !utf8.Valid(val) {
			return "", nil, false
		}
		end := int(dec.InputOffset())
		start := end - len(val)
		// encoding/json reads an escaped lone surrogate as U+FFFD without an
		// error, and such a value is not put back as anything but what it was.
		if err := json.Unmarshal(val, &dct); err != nil || strings.ContainsRune(dct, utf8.RuneError) {
			return "", nil, false
		}
		mt, fold := foldedMediaType(dct, plusJSON)
		if !fold {
			return "", nil, false
		}
		quoted, err := json.Marshal(mt)
		if err != nil {
			return "", nil, false
		}
		folded = make([]byte, 0, len(data)-len(val)+len(quoted))
		folded = append(append(append(folded, data[:start]...), quoted...), data[end:]...)
		return dct, folded, true
	}
	return "", nil, false
}

// foldedMediaType returns the datacontenttype the SDK's decoder reads dct's
// data under as JSON, and false when there is none to give: dct's media type
// lower-cased and trimmed when foldsToJSON holds for it, and application/json
// for a +json media type when plusJSON is set, its parameters kept either way.
func foldedMediaType(dct string, plusJSON bool) (string, bool) {
	mt, params := dct, ""
	if i := strings.IndexByte(dct, ';'); i >= 0 {
		mt, params = dct[:i], dct[i:]
	}
	mt = strings.TrimSpace(strings.ToLower(mt))
	switch {
	case foldsToJSON(dct):
		return mt + params, true
	case plusJSON && isPlusJSON(mt):
		return event.ApplicationJSON + params, true
	}
	return "", false
}

// isPlusJSON reports whether the lower-case media type mt has the +json
// structured syntax suffix (RFC 6839) and is not one the SDK already reads as
// JSON.
func isPlusJSON(mt string) bool {
	_, sub, ok := strings.Cut(mt, "/")
	return ok && len(sub) > len("+json") && strings.HasSuffix(sub, "+json") && !sdkJSON(mt)
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

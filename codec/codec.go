// Package codec turns Go values into message bodies and back.
//
// The core of iqmq is bytes in, bytes out: nothing here is mandatory and the
// engine never serialises a value on its own. A codec is installed by the
// caller (directly, or through the typed helpers) and is the only place that
// knows how a body is encoded.
//
// # Two shapes
//
// Both interface shapes of specification ship, because the typed
// helpers accept either:
//
//   - Codec is the non-generic shape: Marshal(any) and Unmarshal([]byte, any).
//     One codec value serves every message type, which is what a registry, a
//     middleware or a driver needs.
//   - Typed[T] is the generic shape: Encode(T) and Decode([]byte) (T, error).
//     It is what application code wants, because the compiler rejects the
//     wrong message type instead of the decoder rejecting it at run time.
//
// Adapt lifts any Codec into a Typed[T]; a codec that also implements Appender
// is lifted into a value that implements AppendingTyped[T] as well, so the
// append-into-caller-buffer path survives the adaptation. The measured
// comparison of the two shapes is the specification, in
// docs/benchmarks/variants/.md and benchmarks/reports/apibench-scorecard.json.
//
// # Kinds
//
// Kind selects the JSON implementation (specification):
// JSONv1 is encoding/json, JSONv2 is encoding/json/v2, which is part of the
// standard library from Go 1.27 and is therefore compiled only when the
// toolchain provides it (Available reports whether it is). Auto resolves to
// AutoKind, the bake-off winner recorded in auto.go. Raw is not a JSON kind
// and is not part of that family: it is the pass-through codec for callers
// that already hold bytes.
//
// # Content types
//
// Every codec names the content type it produces. Registry maps content types
// to codecs for the receiving side, ParseContentType parses one according to
// RFC 9110 rules, and Match reports a mismatch as a *ContentTypeError, which
// carries errcode.Unsupported so errors.Is(err, mq.ErrUnsupported) holds.
//
// # Concurrency and ownership
//
// Every codec in this package is stateless and safe for concurrent use by
// multiple goroutines. Registry is safe for concurrent use. Marshal returns a
// buffer owned by the caller except where a codec documents otherwise (Raw
// returns the caller's own bytes); Unmarshal never retains data after it
// returns, except where the decoded value is documented to alias it.
package codec

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Header names carrying codec metadata on a message. They are lower-case
// because that is the portable form iqmq uses on the wire.
const (
	// HeaderContentType names the content type of the body.
	HeaderContentType = "content-type"
	// HeaderSchemaID names the schema a schema-registry codec encoded with.
	HeaderSchemaID = "schema-id"
)

// Canonical content types produced by the codecs in this package.
const (
	// ContentTypeJSON is produced by both JSON kinds.
	ContentTypeJSON = "application/json"
	// ContentTypeOctetStream is produced by Raw.
	ContentTypeOctetStream = "application/octet-stream"
)

// Codec encodes and decodes message bodies without knowing the Go type in
// advance. It is the non-generic shape of.
//
// Implementations must be safe for concurrent use. Marshal returns a fresh
// buffer owned by the caller unless the implementation documents otherwise.
// Unmarshal must not retain data.
type Codec interface {
	// ContentType returns the media type Marshal produces, for example
	// "application/json". It never returns an empty string.
	ContentType() string
	// Marshal encodes v.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes data into v, which must be a non-nil pointer.
	Unmarshal(data []byte, v any) error
}

// Appender is the optional extension of Codec for callers that own a buffer:
// AppendMarshal appends the encoding of v to dst and returns the extended
// slice, so a publisher can encode into a pooled buffer.
//
// Hot path: a codec that cannot append without an intermediate copy still
// implements this correctly, and the copy is what the bake-off measures.
type Appender interface {
	// AppendMarshal appends the encoding of v to dst.
	AppendMarshal(dst []byte, v any) ([]byte, error)
}

// Typed is the generic shape of a codec bound to one message type.
//
// Implementations must be safe for concurrent use.
type Typed[T any] interface {
	// ContentType returns the media type Encode produces.
	ContentType() string
	// Encode encodes v.
	Encode(v T) ([]byte, error)
	// Decode decodes data into a value of type T.
	Decode(data []byte) (T, error)
}

// AppendingTyped is the optional append extension of Typed.
type AppendingTyped[T any] interface {
	// AppendEncode appends the encoding of v to dst.
	AppendEncode(dst []byte, v T) ([]byte, error)
}

// Schemer is implemented by codecs bound to a registered schema. Publishers
// set HeaderSchemaID from it; codecs without a schema do not implement it.
type Schemer interface {
	// SchemaID returns the identifier of the schema in force.
	SchemaID() string
}

// Marshaler is the hook a message type implements to encode itself, bypassing
// reflection entirely. Raw calls it; so does any codec that documents it.
type Marshaler interface {
	// MarshalMessage returns the encoded body.
	MarshalMessage() ([]byte, error)
}

// AppendMarshaler is the append form of Marshaler and is preferred over it
// when a value implements both.
type AppendMarshaler interface {
	// AppendMessage appends the encoded body to dst.
	AppendMessage(dst []byte) ([]byte, error)
}

// Unmarshaler is the hook a message type implements to decode itself. The
// implementation must copy anything it retains: data belongs to the caller.
type Unmarshaler interface {
	// UnmarshalMessage decodes data into the receiver.
	UnmarshalMessage(data []byte) error
}

// Errors reported by the codecs in this package. Each carries a portable code,
// so errors.Is against the root package's sentinels holds across the boundary.
var (
	// ErrUnavailable (Unsupported) reports a kind that this build does not
	// contain, for example JSONv2 under a toolchain older than Go 1.27.
	ErrUnavailable = errcode.New(errcode.Unsupported, "codec: kind unavailable in this build")
	// ErrNotRawValue (InvalidArgument) reports a value Raw cannot pass
	// through: it is neither bytes, nor a string, nor a hook implementor.
	ErrNotRawValue = errcode.New(errcode.InvalidArgument, "codec: value is not raw bytes")
	// ErrNilTarget (InvalidArgument) reports Unmarshal into something that is
	// not a non-nil pointer.
	ErrNilTarget = errcode.New(errcode.InvalidArgument, "codec: decode target is not a non-nil pointer")
	// ErrNoCodec (Unsupported) reports a registry lookup with no fallback and
	// no registered codec for the content type.
	ErrNoCodec = errcode.New(errcode.Unsupported, "codec: no codec registered")
	// ErrDuplicateCodec (Conflict) reports registering a second, different
	// codec under a content type that is already taken.
	ErrDuplicateCodec = errcode.New(errcode.Conflict, "codec: content type already registered")
)

// DecodeError reports a body the codec could not decode: malformed syntax,
// a value the target type cannot hold, or a rejected encoding.
//
// It carries errcode.CorruptMessage, because a body that fails to decode is
// the message's fault, not the caller's; the decoder's own error is preserved
// for errors.As (for example *json.SyntaxError). Programmer errors — a nil or
// non-pointer target — are ErrNilTarget instead, and encoding failures are
// returned unwrapped, since they are local bugs and not portable conditions.
type DecodeError struct {
	// ContentType is the content type of the codec that failed.
	ContentType string
	// Err is the decoder's error.
	Err error
}

// Error implements error.
func (e *DecodeError) Error() string {
	return "codec: cannot decode " + e.ContentType + " body: " + e.Err.Error()
}

// Code implements errcode.Coder.
func (e *DecodeError) Code() errcode.Code { return errcode.CorruptMessage }

// Unwrap returns the decoder's error.
func (e *DecodeError) Unwrap() error { return e.Err }

// Is reports whether target carries the same Code.
//
// This matches by CODE and not by reason, which is what makes
// errcode.CorruptMessage hold across a module boundary — and it means
// errors.Is cannot separate two reasons within one codec: every rejection a
// codec has carries CorruptMessage, so errors.Is(<any corrupt body>, a
// specific framing sentinel) is true of all of them. Use [Reason] to ask which
// rejection it was; the specification records a test that went on passing with
// the refusal it was about removed.
func (e *DecodeError) Is(target error) bool {
	c, ok := target.(errcode.Coder)
	return ok && c.Code() == e.Code()
}

// Reason returns the decoder's own error from inside err — WHICH rejection it
// was — or nil if err carries no [DecodeError].
//
// Compare the result by identity (==), or with errors.As for a concrete type.
// Deliberately do NOT compare it with errors.Is: a codec's sentinels are
// errcode values that match each other by code in exactly the way
// [DecodeError.Is] does, so errors.Is(Reason(err), want) is vacuous between
// any two sentinels of one code — the same trap one level down.
//
//	if codec.Reason(err) == avro.ErrFraming { … }
func Reason(err error) error {
	var de *DecodeError
	if errors.As(err, &de) {
		return de.Err
	}
	return nil
}

// checkTarget rejects a decode target that is not a non-nil pointer before the
// decoder sees it, so that a programmer error is InvalidArgument and only a
// bad body is CorruptMessage. reflect.ValueOf does not allocate: v is boxed
// already.
func checkTarget(v any) error {
	if v == nil {
		return ErrNilTarget
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return ErrNilTarget
	}
	return nil
}

// Kind identifies a JSON implementation, the variant family. Auto
// resolves to AutoKind.
type Kind uint8

// Kinds. JSONv1 is the reference implementation.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// JSONv1 is encoding/json: the reference, available in every build.
	JSONv1
	// JSONv2 is encoding/json/v2, available from Go 1.27. It is configured
	// for deterministic output (sorted object members) so that a body is a
	// function of the value alone; see Available.
	JSONv2

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "json", "jsonv2"}

var kindIdents = [kindCount]string{"Auto", "JSONv1", "JSONv2"}

// String returns the kind's slug as used in benchmark names and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// Available reports whether this build contains the kind. Only JSONv2 can be
// unavailable, and only under a toolchain older than Go 1.27.
func (k Kind) Available() bool {
	if k.Resolve() == JSONv2 {
		return jsonv2Available
	}
	return k.Valid()
}

// ParseKind returns the kind with the given slug. It reports kinds that this
// build does not contain, so a configuration error is distinguishable from a
// typo; use Available to tell them apart.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind this build contains, in declaration order
// (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := JSONv1; k < kindCount; k++ {
		if k.Available() {
			out = append(out, k)
		}
	}
	return out
}

// AllKinds lists every concrete kind declared by the family, including any
// that this build does not contain. Documentation and configuration parsing
// use it; benchmarks and model tests use Kinds.
func AllKinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := JSONv1; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// New returns the codec of kind k. It fails on an invalid kind, and with
// ErrUnavailable on a kind this build does not contain.
func New(k Kind) (Codec, error) {
	switch k.Resolve() {
	case JSONv1:
		return jsonV1{}, nil
	case JSONv2:
		return newJSONv2()
	default:
		return nil, fmt.Errorf("codec: invalid kind %s", k)
	}
}

// autoJSON is the codec of AutoKind. AutoKind is a kind every build
// contains, so binding it cannot fail; TestAutoJSONIsAutoKind holds it to New.
func autoJSON() Codec { return jsonV1{} }

// JSON returns the JSON codec of kind Auto bound to T. It is the shape
// application code uses: codec.JSON[Order]().
func JSON[T any]() Typed[T] { return Adapt[T](autoJSON()) }

// TypedKind returns the codec of kind k bound to T, or fails as New does.
func TypedKind[T any](k Kind) (Typed[T], error) {
	c, err := New(k)
	if err != nil {
		return nil, err
	}
	return Adapt[T](c), nil
}

// Adapt lifts a Codec into the generic shape. When c implements Appender the
// result also implements AppendingTyped[T].
//
// Decode allocates a T, decodes into a pointer to it and returns it; for a T
// that is itself a pointer type the decoded value is a freshly allocated
// element, never a reused one.
//
// So the value a codec's Unmarshal receives here is the ADDRESS of the
// caller's T, which is one indirection more than a caller who called Unmarshal
// itself would have passed. A codec that inspects its target -- one that
// unwraps a pointer-to-pointer, or that refuses anything but a struct -- wants
// [Indirect] and [Target] rather than its own reflection, and they are what
// this function's own behaviour is defined in terms of.
func Adapt[T any](c Codec) Typed[T] {
	if a, ok := c.(Appender); ok {
		return appendingTyped[T]{typed[T]{c: c}, a}
	}
	return typed[T]{c: c}
}

// typed is the Codec-to-Typed adapter.
type typed[T any] struct{ c Codec }

func (t typed[T]) ContentType() string { return t.c.ContentType() }

func (t typed[T]) Encode(v T) ([]byte, error) { return t.c.Marshal(&v) }

func (t typed[T]) Decode(data []byte) (T, error) {
	var v T
	if err := t.c.Unmarshal(data, &v); err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// appendingTyped adds AppendEncode when the underlying codec can append.
type appendingTyped[T any] struct {
	typed[T]
	a Appender
}

func (t appendingTyped[T]) AppendEncode(dst []byte, v T) ([]byte, error) {
	return t.a.AppendMarshal(dst, &v)
}

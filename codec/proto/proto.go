// Package proto encodes message bodies as protocol-buffer binary wire format.
//
// It is a codec module, not a schema system. What that means in practice is
// the list of things it deliberately does NOT do, because a codec that implies
// a completeness it lacks is worse than one that says so:
//
//   - **No schema resolution and no registry lookup.** The Go type the caller
//     passes is the schema. Nothing here reads a descriptor set, resolves a
//     writer schema against a reader schema, or asks a schema registry
//     anything. A body carries no schema identifier, so a receiver that does
//     not already know which message type to expect cannot find out from the
//     body — that is protobuf's own property and this package does not paper
//     over it. Schemer is therefore NOT implemented: one codec value serves
//     every message type, so there is no single schema identifier to report.
//   - **No dynamic messages by name.** google.golang.org/protobuf/types/dynamicpb
//     gives a caller a proto.Message built from a descriptor at run time, and
//     such a value works here like any other; wiring a name-to-descriptor
//     lookup into the codec would be the registry this package does not have.
//   - **No JSON or text format.** protojson and prototext are different
//     encodings with different content types and different compatibility
//     rules. This package is the binary wire format only.
//   - **No google.protobuf.Any packing.** A caller who wants a self-describing
//     body packs an Any itself and publishes that.
//   - **No canonical form.** MarshalOptions.Deterministic is set, which orders
//     map entries and makes one build's output a function of the value —
//     enough for a dedup key computed and compared inside one deployment.
//     protobuf-go explicitly does not promise the same bytes across library
//     versions, so a content hash of a proto body is NOT stable across a
//     library upgrade. The dedup key must not be derived from one of these
//     bodies across deployments.
//
// # Untrusted input (the security checks)
//
// A broker-delivered body is untrusted, and the specification requires a decoder to enforce
// its limits BEFORE allocation. Two limits do that here:
//
//   - MaxBytes is checked against len(data) before the decoder is handed
//     anything, and reports ErrTooLarge rather than a decode failure: a body
//     bigger than this codec will decode is a different fact from a malformed
//     one, and a consumer's poison-message policy wants to tell them apart.
//   - RecursionLimit bounds message nesting. protobuf-go's default is 10,000,
//     which is a recursion depth an attacker chooses with two bytes per level;
//     the default here is 100, which is deeper than any hand-written schema
//     and cheap to refuse.
//
// What the specification does NOT need here, stated so that the absence is a finding rather
// than an omission: protobuf's length-delimited fields cannot drive an
// allocation larger than the input, because protobuf-go validates every length
// prefix against the bytes that remain before it allocates. A five-byte body
// claiming a four-gigabyte field is `cannot parse invalid wire-format data`,
// measured, not assumed — TestHostileLengthPrefixIsRejectedByTheLibrary pins
// it, so a library upgrade that lost the property would fail here rather than
// silently become a denial-of-service surface.
//
// All values returned by this package are stateless and safe for concurrent
// use by multiple goroutines.
package proto

import (
	pb "google.golang.org/protobuf/proto"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Content types this package produces and accepts.
const (
	// ContentType is the media type Marshal produces.
	ContentType = "application/protobuf"
	// ContentTypeAlias is the older, equally widespread spelling. Register
	// installs the codec under both.
	ContentTypeAlias = "application/x-protobuf"
)

// Defaults for Config. They are the values New uses.
const (
	// DefaultMaxBytes is the largest body Unmarshal will decode: 8 MiB, which
	// is above every broker maximum message size in the harness and far below
	// anything that matters to a heap.
	DefaultMaxBytes = 8 << 20
	// DefaultRecursionLimit is the deepest message nesting Unmarshal accepts.
	// protobuf-go's own default is 10,000.
	DefaultRecursionLimit = 100
)

// Errors this package reports. Each carries a portable code, so errors.Is
// against the root package's sentinels holds across the module boundary.
var (
	// ErrNotMessage (InvalidArgument) reports a value that is not a
	// proto.Message. It is the caller's bug, so it is InvalidArgument and not
	// a decode failure.
	ErrNotMessage = errcode.New(errcode.InvalidArgument, "codec/proto: value is not a proto.Message")
	// ErrNilMessage (InvalidArgument) reports a nil proto.Message. protobuf
	// encodes one as an empty body with no error, which would publish an
	// empty message where the caller meant a value.
	ErrNilMessage = errcode.New(errcode.InvalidArgument, "codec/proto: proto.Message is nil")
	// ErrTooLarge (MessageTooLarge) reports a body over Config.MaxBytes. It is
	// reported BEFORE the decoder sees the body and is deliberately not a
	// *codec.DecodeError: the body is not malformed, it is bigger than this
	// codec will decode.
	ErrTooLarge = errcode.New(errcode.MessageTooLarge, "codec/proto: body exceeds the configured maximum")
)

// Config configures the codec. The zero value is the default configuration,
// which New uses.
type Config struct {
	// MaxBytes is the largest body Unmarshal decodes. Zero means
	// DefaultMaxBytes; a negative value removes the limit, which is only
	// appropriate for a body the caller produced itself.
	MaxBytes int
	// RecursionLimit is the deepest message nesting Unmarshal accepts. Zero
	// means DefaultRecursionLimit; a negative value means protobuf-go's own
	// default of 10,000.
	RecursionLimit int
	// DiscardUnknown drops fields the Go type does not know instead of
	// retaining them. Retaining them (the default) is what keeps a
	// round trip through an older binary lossless.
	DiscardUnknown bool
	// AllowPartial permits encoding and decoding a message with a required
	// field missing (proto2 only). Off by default.
	AllowPartial bool
}

// maxBytes resolves the configured limit; a negative value means unlimited and
// is returned as -1.
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

func (c Config) recursionLimit() int {
	switch {
	case c.RecursionLimit == 0:
		return DefaultRecursionLimit
	case c.RecursionLimit < 0:
		return 0 // protobuf-go reads 0 as "my own default"
	default:
		return c.RecursionLimit
	}
}

// New returns the codec with the default configuration.
func New() codec.Codec { return NewWithConfig(Config{}) }

// NewWithConfig returns the codec configured by c. The options are resolved
// once, here, so that the hot paths join no options per call.
func NewWithConfig(c Config) codec.Codec {
	return protoCodec{
		limit: c.maxBytes(),
		marshal: pb.MarshalOptions{
			Deterministic: true,
			AllowPartial:  c.AllowPartial,
		},
		unmarshal: pb.UnmarshalOptions{
			DiscardUnknown: c.DiscardUnknown,
			AllowPartial:   c.AllowPartial,
			RecursionLimit: c.recursionLimit(),
		},
	}
}

// Typed returns the codec bound to T, the generic shape. T is a
// pointer to a generated message type, which is the shape protoc emits.
func Typed[T pb.Message]() codec.Typed[T] { return codec.Adapt[T](New()) }

// TypedWithConfig returns the configured codec bound to T.
func TypedWithConfig[T pb.Message](c Config) codec.Typed[T] {
	return codec.Adapt[T](NewWithConfig(c))
}

// Register installs the codec in r under both spellings of its content type.
// It replaces nothing: a content type already taken reports
// codec.ErrDuplicateCodec, as Registry.Register does.
func Register(r *codec.Registry) error {
	c := New()
	if err := r.Register(c); err != nil {
		return err
	}
	return r.RegisterAs(ContentTypeAlias, c)
}

// protoCodec is stateless: the resolved option values are immutable and the
// protobuf options types are values.
type protoCodec struct {
	limit     int
	marshal   pb.MarshalOptions
	unmarshal pb.UnmarshalOptions
}

// ContentType implements codec.Codec.
func (protoCodec) ContentType() string { return ContentType }

// message resolves v to the proto.Message a codec can encode, following the
// indirection codec.Adapt adds. Without codec.Indirect a Typed[*pb.Order]
// hands this method a **pb.Order, which does not implement proto.Message.
func message(v any) (pb.Message, error) {
	m, ok := codec.Indirect(v).(pb.Message)
	if !ok {
		return nil, ErrNotMessage
	}
	// A typed nil is a pointer that satisfies the interface and encodes as an
	// empty body, which is not what a caller who passed nil meant.
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil, ErrNilMessage
	}
	return m, nil
}

// Marshal implements codec.Codec.
func (c protoCodec) Marshal(v any) ([]byte, error) {
	m, err := message(v)
	if err != nil {
		return nil, err
	}
	return c.marshal.Marshal(m)
}

// AppendMarshal implements codec.Appender.
//
// It honours the pinned failure contract, which google.golang.org/protobuf
// does not: MarshalAppend returns the PARTIALLY encoded buffer alongside its
// error, so a publisher that reuses a pooled buffer would find the rejected
// value's first fields in front of the next message. Watched, on
// protobuf v1.36.12, with a StringValue carrying invalid UTF-8:
//
//	nil msg:  out="keep-me"                err=<nil>
//	bad utf8: out="keep-me\n\x02\xff\xfe"  err=string field contains invalid UTF-8
//
// The caller's own slice header is returned on failure, so the bytes past its
// length are garbage the caller cannot see and the next append overwrites.
func (c protoCodec) AppendMarshal(dst []byte, v any) ([]byte, error) {
	m, err := message(v)
	if err != nil {
		return dst, err
	}
	out, err := c.marshal.MarshalAppend(dst, m)
	if err != nil {
		return dst, err
	}
	return out, nil
}

// Unmarshal implements codec.Codec.
//
// The size limit is checked before the decoder is handed anything, and
// an over-size body is ErrTooLarge rather than a *codec.DecodeError.
func (c protoCodec) Unmarshal(data []byte, v any) error {
	if c.limit >= 0 && len(data) > c.limit {
		return ErrTooLarge
	}
	t, err := codec.Target(v)
	if err != nil {
		return err
	}
	m, ok := t.(pb.Message)
	if !ok {
		return ErrNotMessage
	}
	if err := c.unmarshal.Unmarshal(data, m); err != nil {
		return &codec.DecodeError{ContentType: ContentType, Err: err}
	}
	return nil
}

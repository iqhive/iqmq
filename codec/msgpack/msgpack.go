// Package msgpack encodes message bodies as MessagePack.
//
// What it deliberately does NOT do:
//
//   - **No schema.** MessagePack is self-describing and carries no schema
//     identifier, so nothing here resolves, registers or negotiates one.
//     Schemer is not implemented. A field renamed on one side is a field
//     missing on the other, and the format cannot tell you which.
//   - **No extension-type registry.** MessagePack's ext types (the 0xc7–0xc9
//     and 0xd4–0xd8 families) pass through the validator as opaque payloads,
//     and the decoder resolves whatever the library resolves — time.Time by
//     default. Registering an application ext type is the caller's business
//     with the library's own API, not a surface this package adds.
//   - **No canonical form.** Map keys are sorted (SetSortMapKeys), so one
//     build's output is a function of the value, which is what a dedup key
//     computed and compared inside one deployment needs. MessagePack itself
//     defines no canonical encoding — an integer has several valid widths and
//     a string several valid length prefixes — so a body from another
//     implementation may be byte-different and value-identical.
//   - **No struct-tag reinvention.** The library's `msgpack:"..."` tags apply;
//     this package does not add a tag of its own.
//
// # Untrusted input (the security checks)
//
// This is the codec for which the specification's "decoders enforce size and count limits
// BEFORE allocation" is not a formality. MessagePack containers carry an
// element COUNT in their header, and the library allocates from it:
// github.com/vmihailenco/msgpack/v5 v5.4.1 handed a five-byte body —
// `dd ff ff ff ff`, an array32 claiming 4,294,967,295 elements — allocated
// **64 GiB** before returning EOF, and the map32 of the same shape ended the
// measuring process with `signal: killed`. Five bytes.
//
// So every body is walked by Validate before the library sees it. The walk is
// iterative (no recursion, so a nesting depth cannot exhaust the stack while
// the depth limit is being checked) and enforces four things:
//
//   - MaxBytes against len(data), before anything else;
//   - every declared length and count against the bytes that REMAIN — an
//     array of n elements needs at least n more bytes and a map of n pairs at
//     least 2n, so a count the input cannot justify is refused at the header
//     rather than allocated from;
//   - MaxElements over the whole body, which bounds the total work;
//   - MaxDepth on container nesting.
//
// Validate is exported because it is useful without a decode — a driver
// deciding whether to hand a body on can ask — and because a check nobody can
// call from outside is a check nobody tests.
//
// All values returned by this package are stateless and safe for concurrent
// use by multiple goroutines.
package msgpack

import (
	"io"
	"sync"

	mp "github.com/vmihailenco/msgpack/v5"

	"github.com/iqhive/iqmq/codec"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Content types this package produces and accepts. MessagePack has no IANA
// registration, so all three spellings are in the wild; Register installs the
// codec under each.
const (
	// ContentType is the media type Marshal produces.
	ContentType = "application/msgpack"
	// ContentTypeVendor is the vendor-tree spelling.
	ContentTypeVendor = "application/vnd.msgpack"
	// ContentTypeX is the experimental-tree spelling.
	ContentTypeX = "application/x-msgpack"
)

// Defaults for Config.
const (
	// DefaultMaxBytes is the largest body Unmarshal will decode.
	DefaultMaxBytes = 8 << 20
	// DefaultMaxElements is the largest number of values (scalars, keys and
	// container headers) one body may contain.
	DefaultMaxElements = 1 << 20
	// DefaultMaxDepth is the deepest container nesting one body may contain.
	DefaultMaxDepth = 64
)

// Errors this package reports.
var (
	// ErrTooLarge (MessageTooLarge) reports a body over Config.MaxBytes,
	// before anything is allocated from it.
	ErrTooLarge = errcode.New(errcode.MessageTooLarge, "codec/msgpack: body exceeds the configured maximum")
	// ErrTooManyElements (MessageTooLarge) reports a body whose total value
	// count is over Config.MaxElements.
	ErrTooManyElements = errcode.New(errcode.MessageTooLarge, "codec/msgpack: body exceeds the configured element count")
	// ErrTooDeep (MessageTooLarge) reports container nesting over
	// Config.MaxDepth.
	ErrTooDeep = errcode.New(errcode.MessageTooLarge, "codec/msgpack: body exceeds the configured nesting depth")
	// ErrMalformed (CorruptMessage) reports a body the validator refused: a
	// truncated value, a length or count the input cannot justify, a reserved
	// header byte, or bytes after the top-level value. It is wrapped in a
	// *codec.DecodeError by Unmarshal.
	ErrMalformed = errcode.New(errcode.CorruptMessage, "codec/msgpack: malformed body")
)

// Config configures the codec. The zero value is the default configuration.
type Config struct {
	// MaxBytes is the largest body Unmarshal decodes. Zero means
	// DefaultMaxBytes; negative removes the limit.
	MaxBytes int
	// MaxElements is the largest total value count one body may contain.
	// Zero means DefaultMaxElements; negative removes the limit.
	MaxElements int
	// MaxDepth is the deepest container nesting. Zero means DefaultMaxDepth;
	// negative removes the limit, which also removes the bound on the
	// validator's own stack.
	MaxDepth int
	// StructTag is the struct tag key. Empty means the library's own
	// "msgpack".
	StructTag string
	// ArrayEncodedStructs encodes a struct as an array of its field values
	// rather than a map of name to value. It is smaller and faster and is NOT
	// the default, because it makes field order part of the wire contract:
	// adding a field in the middle silently reinterprets every later one.
	ArrayEncodedStructs bool
	// OmitEmpty omits zero-valued struct fields.
	OmitEmpty bool
}

func (c Config) limits() limits {
	l := limits{bytes: c.MaxBytes, elements: c.MaxElements, depth: c.MaxDepth}
	if c.MaxBytes == 0 {
		l.bytes = DefaultMaxBytes
	}
	if c.MaxElements == 0 {
		l.elements = DefaultMaxElements
	}
	if c.MaxDepth == 0 {
		l.depth = DefaultMaxDepth
	}
	return l
}

// limits is the resolved form of the three limits. A negative value means
// unlimited.
type limits struct {
	bytes    int
	elements int
	depth    int
}

// New returns the codec with the default configuration.
func New() codec.Codec { return NewWithConfig(Config{}) }

// NewWithConfig returns the codec configured by c.
func NewWithConfig(c Config) codec.Codec {
	return &msgpackCodec{
		cfg: c,
		lim: c.limits(),
		encoders: sync.Pool{New: func() any {
			e := mp.NewEncoder(nil)
			// Sorted map keys make one build's output a function of the
			// value, which is what dedup keys and content hashes need.
			e.SetSortMapKeys(true)
			e.SetOmitEmpty(c.OmitEmpty)
			e.UseArrayEncodedStructs(c.ArrayEncodedStructs)
			if c.StructTag != "" {
				e.SetCustomStructTag(c.StructTag)
			}
			return e
		}},
		decoders: sync.Pool{New: func() any {
			st := new(decState)
			st.d = mp.NewDecoder(&st.r)
			if c.StructTag != "" {
				st.d.SetCustomStructTag(c.StructTag)
			}
			return st
		}},
	}
}

// Typed returns the codec bound to T, the generic shape.
func Typed[T any]() codec.Typed[T] { return codec.Adapt[T](New()) }

// TypedWithConfig returns the configured codec bound to T.
func TypedWithConfig[T any](c Config) codec.Typed[T] { return codec.Adapt[T](NewWithConfig(c)) }

// Register installs the codec in r under all three spellings of its content
// type.
func Register(r *codec.Registry) error {
	c := New()
	if err := r.Register(c); err != nil {
		return err
	}
	if err := r.RegisterAs(ContentTypeVendor, c); err != nil {
		return err
	}
	return r.RegisterAs(ContentTypeX, c)
}

// msgpackCodec holds the resolved configuration and the encoder and decoder
// pools. It is immutable after construction; the pools are safe for
// concurrent use.
type msgpackCodec struct {
	cfg      Config
	lim      limits
	encoders sync.Pool
	decoders sync.Pool
}

// ContentType implements codec.Codec.
func (*msgpackCodec) ContentType() string { return ContentType }

// Marshal implements codec.Codec.
func (c *msgpackCodec) Marshal(v any) ([]byte, error) {
	return c.AppendMarshal(nil, v)
}

// AppendMarshal implements codec.Appender.
//
// The library encodes to an io.Writer, so the value is written straight into
// the caller's buffer with no intermediate copy. The pinned failure
// contract is honoured by returning the caller's own slice header on failure:
// the encoder may have written a partial value past the caller's length, and
// those bytes are garbage the caller cannot see and the next append
// overwrites.
func (c *msgpackCodec) AppendMarshal(dst []byte, v any) ([]byte, error) {
	w, _ := appendWriters.Get().(*appendWriter)
	w.b = dst
	e, _ := c.encoders.Get().(*mp.Encoder)
	// ResetWriter, not Reset: Reset (and ResetDict) zero the encoder's FLAGS
	// and struct tag, so an encoder configured once in the pool's New and then
	// Reset per call encodes with the library's defaults -- unsorted map keys
	// included. Watched: TestContract/Determinism failed with two different
	// orderings of the same two-key map.
	e.ResetWriter(w)
	err := e.Encode(codec.Indirect(v))
	out := w.b
	w.b = nil
	appendWriters.Put(w)
	// The encoder holds a reference to w until it is reset, so it is cleared
	// before being returned to the pool: a pooled encoder pointing at a
	// pooled writer is how two publications come to share a buffer.
	e.ResetWriter(nil)
	c.encoders.Put(e)
	if err != nil {
		return dst, err
	}
	return out, nil
}

// Unmarshal implements codec.Codec.
//
// The body is validated before the library sees it, which is the specification's
// requirement and not a nicety: see the package comment for what five bytes
// of unvalidated input cost.
func (c *msgpackCodec) Unmarshal(data []byte, v any) error {
	if err := validate(data, c.lim); err != nil {
		if errcode.CodeOf(err) == errcode.CorruptMessage {
			return &codec.DecodeError{ContentType: ContentType, Err: err}
		}
		return err
	}
	t, err := codec.Target(v)
	if err != nil {
		return err
	}
	st, _ := c.decoders.Get().(*decState)
	st.r.b, st.r.i = data, 0
	st.d.ResetReader(&st.r)
	derr := st.d.Decode(t)
	st.r.b, st.r.i = nil, 0
	c.decoders.Put(st)
	if derr != nil {
		return &codec.DecodeError{ContentType: ContentType, Err: derr}
	}
	return nil
}

// Validate reports whether data is a well-formed MessagePack body within c's
// limits, without decoding it and without allocating anything proportional to
// what the body declares.
//
// It is the check the specification requires, exposed so that a caller who is not decoding
// can still apply it — and so that it is reachable from a test in another
// package.
func Validate(data []byte, c Config) error { return validate(data, c.limits()) }

// appendWriter is an io.Writer that appends to a caller-owned slice.
type appendWriter struct{ b []byte }

// Write implements io.Writer.
func (w *appendWriter) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)
	return len(p), nil
}

// WriteByte completes the library's internal writer interface (io.Writer plus
// WriteByte). Without it every call wraps the writer in a fresh adapter.
func (w *appendWriter) WriteByte(c byte) error {
	w.b = append(w.b, c)
	return nil
}

// appendWriters keeps the writer off the heap on the append path.
var appendWriters = sync.Pool{New: func() any { return new(appendWriter) }}

// decState is one pooled decoder and the reader it is bound to. They are
// pooled together because the decoder holds a reference to its reader: a
// pooled decoder pointing at a reader another goroutine has taken is how two
// deliveries come to share a buffer.
type decState struct {
	d *mp.Decoder
	r byteReader
}

// byteReader is an io.Reader over a byte slice that also implements
// io.ByteScanner, which is what the decoder asks for.
type byteReader struct {
	b []byte
	i int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

func (r *byteReader) ReadByte() (byte, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	b := r.b[r.i]
	r.i++
	return b, nil
}

func (r *byteReader) UnreadByte() error {
	if r.i == 0 {
		return io.EOF
	}
	r.i--
	return nil
}

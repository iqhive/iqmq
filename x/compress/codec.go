package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"math"
	"sync"
	"sync/atomic"
)

// CodecID identifies a coding inside a frame. It is one byte, so a frame's
// header stays ten bytes and a registry lookup is an array index.
type CodecID uint8

// The codings this package implements. All four are in the standard library,
// because the root module may import nothing else (ADR-002).
const (
	// CodecStored is no compression at all.
	//
	// It is not a pointless entry: it is what [Encoder] writes when a body
	// does not get smaller, so that every message on a framed topic carries
	// a frame and a consumer never has to decide whether an unframed body is
	// an old publisher or a corrupted one.
	CodecStored CodecID = 0
	// CodecGzip is compress/gzip: deflate with a gzip header and a CRC32.
	// It is the default because it is the coding every other system
	// understands, and its 18-byte header is the reason [Config.MinSize]
	// exists.
	CodecGzip CodecID = 1
	// CodecFlate is compress/flate: raw deflate, no header, no checksum. It
	// is gzip minus eighteen bytes and minus the integrity check the frame's
	// declared size partly replaces.
	CodecFlate CodecID = 2
	// CodecZlib is compress/zlib: deflate with a two-byte header and an
	// Adler-32. It is here because it is what several brokers' own client
	// libraries emit.
	CodecZlib CodecID = 3
	// FirstUserCodec is the lowest id a caller outside this package may
	// register. Everything below it is reserved so that a future stdlib
	// coding can be added without colliding with somebody's zstd.
	FirstUserCodec CodecID = 32
)

// Codec is one coding. Implementations must be safe for concurrent use.
//
// The interface takes and returns byte slices rather than readers and
// writers, because a message body is already in memory: a streaming interface
// would oblige every implementation to reason about a bound it cannot see,
// and the bound is the whole point of this package. [Decode]'s max is the
// limit in bytes and an implementation MUST NOT produce more, MUST NOT
// allocate more, and should stop reading as soon as it knows it would.
type Codec interface {
	// ID is the codec's registered id, written into the frame.
	ID() CodecID
	// Name is the coding's conventional name ("gzip"), used in errors and in
	// [Registry.Names].
	Name() string
	// Encode appends the compressed form of src to dst and returns it.
	Encode(dst, src []byte) ([]byte, error)
	// Decode appends the decompressed form of src to dst and returns it,
	// producing at most max bytes and reporting ErrTooLarge rather than
	// producing more.
	Decode(dst, src []byte, max int64) ([]byte, error)
}

// Registry stores registered compression codecs by ID.
type Registry struct {
	// mu serialises writers only. Readers never touch it.
	mu sync.Mutex
	// table is the published snapshot. A table reachable from it is
	// immutable: Register builds a new one rather than writing to it.
	table atomic.Pointer[codecTable]
}

// codecTable is one immutable snapshot of a [Registry], indexed by id.
type codecTable [256]Codec

// NewRegistry returns a registry holding [CodecStored], [CodecGzip],
// [CodecFlate] and [CodecZlib].
func NewRegistry() *Registry {
	t := new(codecTable)
	for _, c := range []Codec{stored{}, &deflateCodec{id: CodecGzip}, &deflateCodec{id: CodecFlate}, &deflateCodec{id: CodecZlib}} {
		t[c.ID()] = c
	}
	r := &Registry{}
	r.table.Store(t)
	return r
}

// Register adds a codec.
//
// It refuses an id below [FirstUserCodec] with [ErrReservedCodec] and an id
// already taken with [ErrDuplicateCodec]. A silent overwrite is the failure
// this refuses to have: two modules that both register id 4 would otherwise
// give one process a decoder for the other's frames, and the symptom would be
// a corrupt body rather than a configuration error.
//
// It copies the whole table (256 interface values, about 4 KiB) and publishes
// the copy, so a decode that loaded the previous table keeps a consistent view
// of it. That is why Register is a start-up operation: it is cheap enough to
// call a handful of times and deliberately not cheap enough to call per
// message.
func (r *Registry) Register(c Codec) error {
	if c == nil || c.Name() == "" {
		return ErrInvalidConfig
	}
	id := c.ID()
	if id < FirstUserCodec {
		return ErrReservedCodec
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := new(codecTable)
	if cur := r.table.Load(); cur != nil {
		*next = *cur
	}
	if next[id] != nil {
		return ErrDuplicateCodec
	}
	next[id] = c
	r.table.Store(next)
	return nil
}

// Lookup returns the codec registered for id.
//
// Hot path: one atomic load and one array index; no lock, no allocation.
func (r *Registry) Lookup(id CodecID) (Codec, bool) {
	t := r.table.Load()
	if t == nil {
		return nil, false
	}
	c := t[id]
	return c, c != nil
}

// Names returns the registered ids and names, for a log line at start-up.
func (r *Registry) Names() map[CodecID]string {
	out := make(map[CodecID]string, 8)
	t := r.table.Load()
	if t == nil {
		return out
	}
	for id, c := range t {
		if c != nil {
			out[CodecID(id)] = c.Name()
		}
	}
	return out
}

// ---------------------------------------------------------------- stored

type stored struct{}

func (stored) ID() CodecID  { return CodecStored }
func (stored) Name() string { return "stored" }

func (stored) Encode(dst, src []byte) ([]byte, error) { return append(dst, src...), nil }

func (stored) Decode(dst, src []byte, maxBytes int64) ([]byte, error) {
	if int64(len(src)) > maxBytes {
		return nil, ErrTooLarge
	}
	return append(dst, src...), nil
}

// ---------------------------------------------------------------- deflate family

// deflateCodec is gzip, raw flate and zlib, which differ only in the framing
// the standard library puts round the same deflate stream. One type with an
// id rather than three is not a saving of code so much as a guarantee that
// the three cannot drift apart in how they bound a decode, which is the part
// that matters.
type deflateCodec struct {
	id CodecID
	// writers recycles this codec's compressors, which hold a 32 KiB window
	// each. A publisher that frames every message would otherwise make that
	// the largest allocation on the publish path.
	writers sync.Pool
}

func (c *deflateCodec) ID() CodecID { return c.id }

func (c *deflateCodec) Name() string {
	switch c.id {
	case CodecGzip:
		return "gzip"
	case CodecFlate:
		return "flate"
	case CodecZlib:
		return "zlib"
	default:
		// CodecStored and every user id are not this type's: only the three
		// deflate framings reach deflateCodec, and the default keeps the
		// switch total so `exhaustive` need not be waived.
		return "unknown"
	}
}

func (c *deflateCodec) Encode(dst, src []byte) ([]byte, error) {
	buf := bytes.NewBuffer(dst)
	w, put, err := c.writer(buf)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(src); err != nil {
		put(false)
		return nil, err
	}
	if err := w.Close(); err != nil {
		put(false)
		return nil, err
	}
	put(true)
	return buf.Bytes(), nil
}

func (c *deflateCodec) Decode(dst, src []byte, maxBytes int64) ([]byte, error) {
	zr, err := c.reader(src)
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	return copyBounded(dst, zr, maxBytes)
}

// copyBounded appends at most maxBytes from r to dst, reporting ErrTooLarge
// if r would produce more.
//
// The limited reader is given maxBytes+1: reading exactly the limit is
// allowed and reading one more is how "over the limit" is detected without
// materialising the rest of the stream. This is the one implementation of
// that arithmetic in the package, so the four codecs cannot disagree about
// whether the limit is inclusive.
func copyBounded(dst []byte, r io.Reader, maxBytes int64) ([]byte, error) {
	buf := bytes.NewBuffer(dst)
	// There is no byte past math.MaxInt64: maxBytes+1 overflowed there to a
	// negative limit, which reads nothing and reports no error, so every
	// frame decoded to nothing and was refused as a size mismatch. Nothing
	// can be longer than that limit anyway.
	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	n, err := io.Copy(buf, io.LimitReader(r, limit))
	if err != nil {
		return nil, err
	}
	if n > maxBytes {
		return nil, ErrTooLarge
	}
	return buf.Bytes(), nil
}

// writer returns a pooled compressor writing to buf, and the function that
// returns it to its pool. ok=false discards it: a writer whose Write or Close
// failed may hold a broken state, and recycling it would carry that state
// into an unrelated message. It fails when no writer could be built.
func (c *deflateCodec) writer(buf *bytes.Buffer) (io.WriteCloser, func(ok bool), error) {
	p := &c.writers
	got := p.Get()
	if got == nil {
		w, err := c.newWriter()
		if err != nil {
			return nil, nil, err
		}
		got = w
	}
	switch w := got.(type) {
	case *gzip.Writer:
		w.Reset(buf)
		return w, func(ok bool) {
			if ok {
				p.Put(w)
			}
		}, nil
	case *zlib.Writer:
		w.Reset(buf)
		return w, func(ok bool) {
			if ok {
				p.Put(w)
			}
		}, nil
	case *flate.Writer:
		w.Reset(buf)
		return w, func(ok bool) {
			if ok {
				p.Put(w)
			}
		}, nil
	}
	// newWriter covers all three ids and no other id reaches this type.
	return nil, nil, ErrInvalidConfig
}

func (c *deflateCodec) reader(src []byte) (io.ReadCloser, error) {
	switch c.id {
	case CodecGzip:
		return gzip.NewReader(bytes.NewReader(src))
	case CodecZlib:
		return zlib.NewReader(bytes.NewReader(src))
	case CodecFlate:
		return flate.NewReader(bytes.NewReader(src)), nil
	default:
		return nil, ErrUnknownCodec
	}
}

// newWriter builds a compressor of this codec's framing for the pool.
func (c *deflateCodec) newWriter() (any, error) {
	switch c.id {
	case CodecGzip:
		return gzip.NewWriter(io.Discard), nil
	case CodecZlib:
		return zlib.NewWriter(io.Discard), nil
	default:
		return flate.NewWriter(io.Discard, flate.DefaultCompression)
	}
}

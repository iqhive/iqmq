package compress

// Defaults.
const (
	// DefaultMinSize is the body size below which [Encoder.Encode] frames
	// with [CodecStored] rather than compressing.
	//
	// Gzip's own header is eighteen bytes and deflate needs a few more
	// before it breaks even, so compressing a short body reliably makes it
	// bigger. The number matches middleware.DefaultMinCompressSize on
	// purpose: two compression paths in one repository that disagreed about
	// when compression is worth it would be a difference nobody could
	// explain.
	DefaultMinSize = 512
	// DefaultMaxDecompressed is the limit [Config.MaxDecompressed] takes
	// when it is zero. The specification makes a limit mandatory and this package offers
	// no way to ask for none.
	DefaultMaxDecompressed = 16 << 20
	// maxInitialAlloc caps the buffer a decode STARTS with.
	//
	// This is the number that makes the declared size safe. A frame that
	// declares the limit exactly is within its rights, and sizing the first
	// allocation from that claim would let a ten-byte message cost 16 MiB of
	// resident memory times the runner's concurrency — an amplification that
	// needs no compression bomb at all, only a lie. So the decode starts
	// from what the COMPRESSED payload justifies, clamped here, and grows as
	// bytes actually arrive. A legitimate 8 MiB body pays a handful of
	// doublings for that, which is the trade made deliberately.
	maxInitialAlloc = 64 << 10
)

// Config configures an [Encoder] and a [Decoder].
type Config struct {
	// Codec is the coding [Encoder.Encode] produces. The zero value is
	// [CodecGzip] rather than [CodecStored]: an encoder nobody configured
	// should compress, and a caller who wants no compression says so.
	Codec CodecID
	// MinSize is the smallest body Encode will compress; a smaller one is
	// framed with [CodecStored]. Zero takes [DefaultMinSize]; negative is
	// refused.
	MinSize int
	// MaxDecompressed bounds what a [Decoder] will produce from one frame,
	// in bytes. Zero takes [DefaultMaxDecompressed]; negative is refused.
	// There is no "unlimited": the specification requires a cap.
	MaxDecompressed int64
	// Registry holds the codecs. Nil takes a fresh [NewRegistry].
	Registry *Registry
}

func (c Config) resolve() (Config, error) {
	if c.MinSize < 0 || c.MaxDecompressed < 0 {
		return Config{}, ErrInvalidConfig
	}
	if c.MinSize == 0 {
		c.MinSize = DefaultMinSize
	}
	if c.MaxDecompressed == 0 {
		c.MaxDecompressed = DefaultMaxDecompressed
	}
	if c.Registry == nil {
		c.Registry = NewRegistry()
	}
	if _, ok := c.Registry.Lookup(c.Codec); !ok {
		// An encoder whose own coding is not registered would produce
		// frames it cannot read back, which is a start-up error and not a
		// per-message one.
		return Config{}, ErrUnknownCodec
	}
	return c, nil
}

// Encoder frames bodies. It is safe for concurrent use.
type Encoder struct{ cfg Config }

// NewEncoder returns an encoder, or [ErrInvalidConfig] / [ErrUnknownCodec].
func NewEncoder(cfg Config) (*Encoder, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Encoder{cfg: c}, nil
}

// Encode returns the framed form of body.
//
// A body shorter than [Config.MinSize], and one the coding does not actually
// shrink, is framed with [CodecStored]: every message on a framed topic
// carries a frame, so a consumer never has to decide whether an unframed body
// came from an old publisher or from a corruption. A body already framed is
// returned unchanged rather than framed twice.
func (e *Encoder) Encode(body []byte) ([]byte, error) {
	if Framed(body) {
		return body, nil
	}
	if int64(len(body)) > MaxDeclaredSize {
		return nil, errTooLong
	}
	size := uint32(len(body))
	if len(body) < e.cfg.MinSize || e.cfg.Codec == CodecStored {
		return e.store(body, size), nil
	}
	c, ok := e.cfg.Registry.Lookup(e.cfg.Codec)
	if !ok {
		return nil, ErrUnknownCodec
	}
	out := AppendHeader(make([]byte, 0, HeaderLen+len(body)/2+64), e.cfg.Codec, size)
	out, err := c.Encode(out, body)
	if err != nil {
		return nil, err
	}
	if len(out) >= HeaderLen+len(body) {
		// Compression made it bigger. Publishing that is a cost with no
		// benefit, and a caller cannot tell from the outside which of their
		// payloads those are.
		return e.store(body, size), nil
	}
	return out, nil
}

func (e *Encoder) store(body []byte, size uint32) []byte {
	out := AppendHeader(make([]byte, 0, HeaderLen+len(body)), CodecStored, size)
	return append(out, body...)
}

// Decoder unframes bodies under a size limit. It is safe for concurrent use.
type Decoder struct{ cfg Config }

// NewDecoder returns a decoder, or [ErrInvalidConfig].
//
// A decoder's [Config.Codec] is not used — a frame names its own coding — so
// a decode-only program may leave it at zero.
func NewDecoder(cfg Config) (*Decoder, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return &Decoder{cfg: c}, nil
}

// MaxDecompressed is the limit this decoder enforces.
func (d *Decoder) MaxDecompressed() int64 { return d.cfg.MaxDecompressed }

// Decode returns the original body of a framed one.
//
// The order of the checks is the security property, so it is spelled out:
//
//  1. the header is parsed, which allocates nothing;
//  2. the DECLARED size is compared with the limit and a frame over it is
//     [ErrTooLarge] — refused here, before a codec is looked up and before
//     one byte is allocated;
//  3. the codec is looked up, and an unregistered id is [ErrUnknownCodec];
//  4. the payload is decompressed into a buffer sized from the COMPRESSED
//     length (see maxInitialAlloc) and bounded by the limit, and a payload
//     the codec cannot read carries CodeCorruptMessage;
//  5. the result's ACTUAL length is compared with the declared one, and a
//     disagreement is [ErrSizeMismatch].
//
// Step 2 is what stops a bomb cheaply and step 4 is what stops one that lies
// about its size. Step 5 is what turns a truncated stream into an error
// instead of a short body the handler would treat as real.
func (d *Decoder) Decode(body []byte) ([]byte, error) {
	h, payload, err := Parse(body)
	if err != nil {
		return nil, err
	}
	if int64(h.Size) > d.cfg.MaxDecompressed {
		return nil, ErrTooLarge
	}
	c, ok := d.cfg.Registry.Lookup(h.Codec)
	if !ok {
		return nil, ErrUnknownCodec
	}
	out, err := c.Decode(make([]byte, 0, initialCap(h, len(payload), d.cfg.MaxDecompressed)), payload, d.cfg.MaxDecompressed)
	if err != nil {
		return nil, unreadable(err)
	}
	if int64(len(out)) != int64(h.Size) {
		return nil, ErrSizeMismatch
	}
	return out, nil
}

// initialCap is how big the decode buffer starts.
//
// It is derived from the compressed length and never from the declared size
// alone, and it is clamped by maxInitialAlloc and by the limit. The declared
// size only ever LOWERS it: a frame that honestly says "this is 40 bytes"
// saves the growth, and one that says "this is 16 MiB" is not believed.
func initialCap(h Header, compressed int, limit int64) int {
	n := int64(compressed)
	if n > maxInitialAlloc {
		n = maxInitialAlloc
	}
	if int64(h.Size) < n {
		n = int64(h.Size)
	}
	if n > limit {
		n = limit
	}
	if n < 0 {
		n = 0
	}
	return int(n)
}

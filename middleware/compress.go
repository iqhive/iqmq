package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"sync"

	mq "github.com/iqhive/iqmq"
)

// ContentEncodingGzip is the content coding this package produces and
// recognises.
const ContentEncodingGzip = "gzip"

// HeaderContentEncoding carries the content coding when the backend has no
// native property for it.
const HeaderContentEncoding = "content-encoding"

// Compression defaults.
const (
	// DefaultMinCompressSize is the body size below which PublishCompress
	// leaves a body alone. Gzip's own header is 18 bytes, so compressing a
	// short body reliably makes it bigger.
	DefaultMinCompressSize = 512
	// DefaultMaxDecompressed is the decompressed-size limit Compress applies
	// when the configuration names none. The specification makes it mandatory: an
	// unbounded decompressor turns a kilobyte of broker-delivered input into
	// as much memory as the attacker likes.
	DefaultMaxDecompressed = 16 << 20
)

// CompressConfig configures Compress and PublishCompress. The zero value is
// usable: DefaultMinCompressSize, DefaultMaxDecompressed and gzip's default
// compression level.
type CompressConfig struct {
	// MinSize is the smallest body PublishCompress will compress. Zero
	// selects DefaultMinCompressSize; a negative value is refused.
	MinSize int
	// MaxDecompressed bounds what Compress will produce from one body, in
	// bytes. Zero selects DefaultMaxDecompressed; a negative value is
	// refused. There is no way to ask for "no limit": the specification requires one.
	MaxDecompressed int64
	// Level is the gzip compression level, as compress/gzip defines it. Zero
	// selects gzip.DefaultCompression.
	Level int
}

// resolve fills in the defaults and refuses a configuration that cannot work.
func (cfg CompressConfig) resolve() CompressConfig {
	if cfg.MinSize < 0 || cfg.MaxDecompressed < 0 {
		panic(ErrInvalidConfig)
	}
	if cfg.MinSize == 0 {
		cfg.MinSize = DefaultMinCompressSize
	}
	if cfg.MaxDecompressed == 0 {
		cfg.MaxDecompressed = DefaultMaxDecompressed
	}
	if cfg.Level == 0 {
		cfg.Level = gzip.DefaultCompression
	}
	if cfg.Level < gzip.HuffmanOnly || cfg.Level > gzip.BestCompression {
		panic(ErrInvalidConfig)
	}
	return cfg
}

// Compress decompresses an inbound body whose content encoding says it is
// compressed, under a hard size limit.
//
// # The limit is the point
//
// The specification requires that "compression middleware caps decompressed output", and
// the reason is that a gzip stream is a program: a few hundred bytes a broker
// delivered can expand to gigabytes, and a decompressor that reads until EOF
// into a growing buffer will allocate every one of them before anything
// notices. This one never does. It copies through an io.LimitedReader of
// MaxDecompressed+1 bytes into a buffer that starts at the compressed size
// and is capped at the limit, so the largest allocation a malicious body can
// cause is MaxDecompressed — and a body that would exceed it is refused with
// ErrTooLarge while it is still compressed. The +1 is what tells "exactly the
// limit" (accepted) from "more than the limit" (refused) without reading the
// rest of the stream.
//
// A body whose content encoding is not gzip is passed through untouched: this
// middleware recognises what it produces and does not guess.
//
// A decode failure is a *mq.Delivery's problem and not the connection's:
// ErrTooLarge carries CodeMessageTooLarge and a malformed stream carries the
// gzip reader's own error, both of which the runner treats as ordinary
// handler failures.
func Compress(cfg CompressConfig) mq.Middleware {
	c := cfg.resolve()
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if encodingOf(d) != ContentEncodingGzip {
				return next.Handle(ctx, d)
			}
			body, err := decompress(d.Body, c.MaxDecompressed)
			if err != nil {
				return err
			}
			// The delivery's body is replaced for the rest of the chain.
			// The compressed original is not kept: nothing downstream wants
			// it, and holding a second copy of every message is how a
			// decompressing consumer runs out of memory honestly.
			d.Body = body
			d.Properties.ContentEncoding = ""
			d.Headers.Delete(HeaderContentEncoding)
			return next.Handle(ctx, d)
		})
	}
}

// PublishCompress compresses an outbound body over CompressConfig.MinSize and
// marks its content encoding.
//
// A body that does not get smaller is published uncompressed: compression
// that grows the message is a cost with no benefit, and a caller cannot tell
// from the outside which of their payloads those are.
//
// A message that already declares a content encoding is left alone — it has
// been compressed by something else, and compressing it again would make the
// content-encoding header a lie.
func PublishCompress(cfg CompressConfig) mq.PublishMiddleware {
	c := cfg.resolve()
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message) error {
			for _, m := range ms {
				if m == nil || len(m.Body) < c.MinSize || m.Properties.ContentEncoding != "" {
					continue
				}
				if _, ok := m.Headers.Get(HeaderContentEncoding); ok {
					continue
				}
				body, err := compress(m.Body, c.Level)
				if err != nil {
					return err
				}
				if len(body) >= len(m.Body) {
					continue
				}
				m.Body = body
				m.Properties.ContentEncoding = ContentEncodingGzip
			}
			return next(ctx, topic, ms)
		}
	}
}

// encodingOf reads the delivery's content encoding: the typed property when
// the driver filled it, and the header otherwise.
func encodingOf(d *mq.Delivery) string {
	if e := d.Properties.ContentEncoding; e != "" {
		return e
	}
	if v, ok := d.Headers.Get(HeaderContentEncoding); ok {
		return string(v)
	}
	return ""
}

// gzipWriters recycles the compressors, which hold a 32 KiB window each. A
// publisher that compresses every message would otherwise allocate one per
// publication, which is the single largest allocation on the path.
var gzipWriters sync.Map // level int -> *sync.Pool of *gzip.Writer

// compress gzips b at the given level.
func compress(b []byte, level int) ([]byte, error) {
	p := writerPool(level)
	w, _ := p.Get().(*gzip.Writer)
	var buf bytes.Buffer
	buf.Grow(len(b)/2 + 32)
	w.Reset(&buf)
	if _, err := w.Write(b); err != nil {
		p.Put(w)
		return nil, err
	}
	if err := w.Close(); err != nil {
		p.Put(w)
		return nil, err
	}
	p.Put(w)
	return buf.Bytes(), nil
}

// writerPool returns the pool of writers for one compression level.
func writerPool(level int) *sync.Pool {
	if p, ok := gzipWriters.Load(level); ok {
		pool, _ := p.(*sync.Pool)
		return pool
	}
	pool := &sync.Pool{New: func() any {
		w, err := gzip.NewWriterLevel(io.Discard, level)
		if err != nil {
			// resolve() already refused every level gzip rejects, so this
			// is unreachable; a nil writer would panic later and further
			// from the cause.
			panic(ErrInvalidConfig)
		}
		return w
	}}
	actual, _ := gzipWriters.LoadOrStore(level, pool)
	got, _ := actual.(*sync.Pool)
	return got
}

// decompress gunzips b, refusing anything that would exceed max bytes.
//
// Nothing larger than max is ever allocated: the copy runs through a limited
// reader of max+1 bytes, and the buffer it copies into is capped at max+1
// too. The check is therefore made while the body is still compressed, which
// is what "bound the size before allocating it" means.
func decompress(b []byte, maxBytes int64) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	// Start at the compressed size and grow no further than the limit: a
	// body that compresses well still allocates in proportion to what it
	// decompresses to, and one that does not is bounded by the limit.
	start := int64(len(b))
	if start > maxBytes {
		start = maxBytes
	}
	buf := bytes.NewBuffer(make([]byte, 0, start))
	n, err := io.Copy(buf, io.LimitReader(zr, maxBytes+1))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n > maxBytes {
		return nil, ErrTooLarge
	}
	return buf.Bytes(), nil
}

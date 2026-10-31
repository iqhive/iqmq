package middleware

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"math"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
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
func (cfg CompressConfig) resolve() (CompressConfig, error) {
	if cfg.MinSize < 0 || cfg.MaxDecompressed < 0 {
		return CompressConfig{}, ErrInvalidConfig
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
		return CompressConfig{}, ErrInvalidConfig
	}
	return cfg, nil
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
// The decompressed body, and a coding that no longer says gzip, replace the
// delivery's own for the rest of the chain; neither is changed in place. The
// runner keeps the compressed message it received, so a message the handler
// fails is dead-lettered compressed and still marked gzip, and a strictly
// ordered retry decompresses it again.
//
// A body whose content encoding is not gzip is passed through untouched: this
// middleware recognises what it produces and does not guess.
//
// A decode failure is a *mq.Delivery's problem and not the connection's:
// ErrTooLarge carries CodeMessageTooLarge, and a malformed or truncated
// stream carries CodeCorruptMessage with the gzip reader's own error kept
// beside it. The runner treats both as ordinary handler failures, and
// neither is Transient, so Retry does not decode the body again. Sentinels
// match by code, so errors.Is reports the malformed stream as ErrNotSealed
// and ErrNotAuthentic too: a caller that alerts on tampering tells the two
// apart by the message.
func Compress(cfg CompressConfig) (mq.Middleware, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			if encodingOf(d) != ContentEncodingGzip {
				return next.Handle(ctx, d)
			}
			body, err := decompress(d.Body, c.MaxDecompressed)
			if err != nil {
				return err
			}
			// The delivery's body and coding are replaced for the rest of
			// the chain, never changed in place: the runner keeps the
			// message it received, compressed, and that is what it
			// dead-letters and what a strict retry decompresses again.
			d.Body = body
			d.Properties.ContentEncoding = ""
			d.Headers = headersWithout(&d.Headers, HeaderContentEncoding)
			return next.Handle(ctx, d)
		})
	}, nil
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
func PublishCompress(cfg CompressConfig) (mq.PublishMiddleware, error) {
	c, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	writers := &gzipWriters{level: c.Level}
	return func(next mq.PublishFunc) mq.PublishFunc {
		return func(ctx context.Context, topic string, ms []*mq.Message, opts []mq.PublishOption) error {
			for _, m := range ms {
				if m == nil || len(m.Body) < c.MinSize || m.Properties.ContentEncoding != "" {
					continue
				}
				if _, ok := m.Headers.Get(HeaderContentEncoding); ok {
					continue
				}
				body, err := writers.compress(m.Body)
				if err != nil {
					return err
				}
				if len(body) >= len(m.Body) {
					continue
				}
				m.Body = body
				m.Properties.ContentEncoding = ContentEncodingGzip
			}
			return next(ctx, topic, ms, opts)
		}
	}, nil
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

// gzipWriters recycles one PublishCompress's compressors, which hold a 32 KiB
// window each. A publisher that compresses every message would otherwise
// allocate one per publication, which is the single largest allocation on the
// path.
type gzipWriters struct {
	level int
	pool  sync.Pool
}

// compress gzips b at the writers' level.
func (g *gzipWriters) compress(b []byte) ([]byte, error) {
	w, err := g.get()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(len(b)/2 + 32)
	w.Reset(&buf)
	if _, err := w.Write(b); err != nil {
		g.pool.Put(w)
		return nil, err
	}
	if err := w.Close(); err != nil {
		g.pool.Put(w)
		return nil, err
	}
	g.pool.Put(w)
	return buf.Bytes(), nil
}

// get takes a writer out of the pool, or builds one when the pool is empty.
// resolve() already refused every level gzip rejects, so the error is for a
// level that bypassed it.
func (g *gzipWriters) get() (*gzip.Writer, error) {
	if w, ok := g.pool.Get().(*gzip.Writer); ok {
		return w, nil
	}
	return gzip.NewWriterLevel(io.Discard, g.level)
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
		return nil, errors.Join(errNotGzip, err)
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
	// There is no byte past math.MaxInt64: maxBytes+1 overflowed there to a
	// negative limit, which reads nothing and reports no error, so every body
	// reached the handler empty and was acked. Nothing can be longer than
	// that limit anyway.
	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	n, err := io.Copy(buf, io.LimitReader(zr, limit))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.Join(errNotGzip, err)
	}
	if n > maxBytes {
		return nil, ErrTooLarge
	}
	return buf.Bytes(), nil
}

// errNotGzip is what Compress joins gzip's own error to for a body that says
// it is gzip and is not, or ends early. It carries CodeCorruptMessage, as
// ErrNotAuthentic does for a sealed body that does not open. gzip's error
// alone carried no code, so middleware.Transient retried a body that can
// never decode and a circuit breaker counted each one against the dependency.
var errNotGzip error = errcode.New(errcode.CorruptMessage, "iqmq/middleware: body is not a readable gzip stream")

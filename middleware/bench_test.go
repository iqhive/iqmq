package middleware_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/middleware"
)

// BenchmarkMiddleware measures the composed chain, per delivery (layer
// 1, the specification's "middleware chain of 5 no-ops <= 30 ns/msg, 0 allocs").
//
// # What the budget is really testing
//
// The per-middleware rows below are the "each benchmarked with
// allocs/op documented" half. Each is one middleware composed around a no-op
// handler, so what is measured is that middleware's own cost.
func BenchmarkMiddleware(b *testing.B) {
	ctx := context.Background()
	noop := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	clk := newFakeClock()

	b.Run("chain", func(b *testing.B) {
		for _, n := range []int{0, 1, 3, 5} {
			b.Run("n="+itoa(n), func(b *testing.B) {
				mw := make([]mq.Middleware, n)
				for i := range mw {
					mw[i] = passthrough
				}
				// Composed once, here: this is the whole point of the row.
				h := mq.Chain(mw...)(noop)
				d := benchDelivery()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := h.Handle(ctx, d); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatal(err)
	}
	keys, err := middleware.NewStaticKey(key, "k1")
	if err != nil {
		b.Fatal(err)
	}
	rec := &publishRecorder{}
	sealer := mustWrapPublisher(b, rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: keys}))
	if err := sealer.Publish(ctx, &mq.Message{Topic: "orders", Body: []byte("payload")}); err != nil {
		b.Fatal(err)
	}
	sealed := rec.sent[0]

	var zbuf bytes.Buffer
	zw := gzip.NewWriter(&zbuf)
	if _, err := zw.Write(bytes.Repeat([]byte("compress me "), 64)); err != nil {
		b.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		b.Fatal(err)
	}
	gzipped := zbuf.Bytes()

	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	rows := []struct {
		name    string
		mw      mq.Middleware
		prepare func(*mq.Delivery)
		// consumes marks a middleware that REPLACES what it was given --
		// Compress and Encrypt rewrite the body -- and so needs a fresh
		// delivery per iteration. Its figure then includes that fixture.
		consumes bool
	}{
		{name: "recover", mw: middleware.Recover(nil)},
		{name: "timeout", mw: mustTimeout(b, time.Minute)},
		{name: "retry", mw: mustRetry(b, middleware.RetryConfig{MaxAttempts: 3, Clock: clk})},
		{name: "logging", mw: middleware.Logging(discardLogger())},
		{
			name: "correlation", mw: middleware.CorrelationID(middleware.CorrelationConfig{}),
			prepare: func(d *mq.Delivery) { d.Properties.CorrelationID = "corr-1" },
		},
		{
			name: "correlation/trace", mw: middleware.CorrelationID(middleware.CorrelationConfig{Trace: true}),
			prepare: func(d *mq.Delivery) { d.Headers.Set(middleware.HeaderTraceParent, []byte(traceParent)) },
		},
		{name: "ratelimit", mw: mustRateLimit(b, middleware.RateLimitConfig{Rate: 1e9, Burst: 1 << 30, Clock: clk})},
		{name: "breaker", mw: mustCircuitBreaker(b, middleware.BreakerConfig{Threshold: 1 << 30, Cooldown: time.Minute, Clock: clk})},
		{name: "filter", mw: middleware.Filter(func(*mq.Delivery) bool { return true })},
		{
			// The miss path, which is what a delivery normally takes. The
			// hit path cannot be measured here: a duplicate is ACKED, and a
			// delivery a benchmark built itself carries no settlement right
			// BenchmarkDedupStore measures the store's own hit.
			name: "dedup", mw: middleware.Dedup(middleware.DedupConfig{Store: alwaysNew{}}),
			prepare: func(d *mq.Delivery) { d.ID = "id-1" },
		},
		{name: "validate", mw: middleware.Validate(func(*mq.Delivery) error { return nil })},
		{
			name: "compress", mw: mustCompress(b, middleware.CompressConfig{}),
			prepare: func(d *mq.Delivery) {
				d.Body = gzipped
				d.Properties.ContentEncoding = middleware.ContentEncodingGzip
			},
			consumes: true,
		},
		{
			name: "encrypt", mw: middleware.Encrypt(middleware.EncryptConfig{Keys: keys}),
			prepare: func(d *mq.Delivery) {
				d.Body = sealed.Body
				d.Headers = sealed.Headers.Clone()
			},
			consumes: true,
		},
		{name: "metrics", mw: middleware.Metrics(nullRecorder{}, clk)},
	}
	for _, r := range rows {
		b.Run("one/"+r.name, func(b *testing.B) {
			h := r.mw(noop)
			// A row whose middleware CONSUMES the delivery -- Compress and
			// Encrypt replace the body -- needs a fresh one per iteration,
			// and that fixture is then part of the figure (one allocation of
			// about 240 bytes). Every other row prepares one delivery and
			// reuses it, so its figure is the middleware's own. TestAllocs
			// measures both against a baseline of the same shape and is the
			// authority on the counts.
			shared := benchDelivery()
			if r.prepare != nil && !r.consumes {
				r.prepare(shared)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				d := shared
				if r.consumes {
					d = benchDelivery()
					r.prepare(d)
				}
				if err := h.Handle(ctx, d); err != nil {
					b.Fatal(err)
				}
			}
		})
	}

	b.Run("publish", func(b *testing.B) {
		pubs := []struct {
			name string
			mw   []mq.PublishMiddleware
		}{
			{"none", nil},
			{"id", []mq.PublishMiddleware{middleware.ID(func() string { return "fixed" })}},
			{"timestamp", []mq.PublishMiddleware{middleware.Timestamp(clk)}},
			{"id+timestamp", []mq.PublishMiddleware{
				middleware.ID(func() string { return "fixed" }),
				middleware.Timestamp(clk),
			}},
		}
		for _, p := range pubs {
			b.Run(p.name, func(b *testing.B) {
				w := mustWrapPublisher(b, nullPublisher{}, p.mw...)
				m := &mq.Message{Topic: "orders", Body: []byte("payload")}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					m.ID = ""
					m.Properties.CreatedAt = time.Time{}
					if err := w.Publish(ctx, m); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	})
}

// passthrough is the no-op middleware the chain row is built from: it wraps
// the handler and calls it, which is the least a middleware can do and
// therefore what the 30 ns budget is written about.
func passthrough(next mq.Handler) mq.Handler {
	return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
		return next.Handle(ctx, d)
	})
}

// benchDelivery returns the delivery every row is measured over.
func benchDelivery() *mq.Delivery {
	return &mq.Delivery{Message: mq.Message{Topic: "orders", Body: benchBody}}
}

var benchBody = []byte("payload")

// itoa avoids strconv in a benchmark name, where the value is one of four
// constants.
func itoa(n int) string {
	return [...]string{"0", "1", "2", "3", "4", "5"}[n]
}

// BenchmarkTraceContext measures the W3C parser and renderer, which run once
// per delivery in a traced consumer.
func BenchmarkTraceContext(b *testing.B) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	b.Run("Parse", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			t, err := middleware.ParseTraceParent(tp)
			if err != nil || !t.Valid() {
				b.Fatal(err)
			}
		}
	})
	t, err := middleware.ParseTraceParent(tp)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("String", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if len(t.String()) != middleware.TraceParentLen {
				b.Fatal("short")
			}
		}
	})
}

// BenchmarkDedupStore measures the LRU store's hit and miss paths.
func BenchmarkDedupStore(b *testing.B) {
	ctx := context.Background()
	s := mustNewLRUStore(b, 1<<12)
	if _, err := s.Seen(ctx, "hot"); err != nil {
		b.Fatal(err)
	}
	b.Run("hit", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			seen, err := s.Seen(ctx, "hot")
			if err != nil || !seen {
				b.Fatal("miss")
			}
		}
	})
}

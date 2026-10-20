package middleware_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"log/slog"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/middleware"
)

// TestAllocs pins what each middleware costs per delivery, exactly (
// rule 1, the specification's "each benchmarked with allocs/op documented").
//
// The counts are not all zero and are not meant to be: a Timeout is a context
// and a context is an allocation, an encrypted body is a new buffer, and a
// log line is a log line. What this test pins is that the number does not
// grow — a middleware that starts allocating one more object per message
// fails here rather than in a release benchmark six weeks later.
//
// Each case measures the middleware composed around a no-op handler, so what
// is counted is the middleware's own cost and the one indirect call through
// the chain.
func TestAllocs(t *testing.T) {
	ctx := context.Background()
	noop := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	clk := newFakeClock()

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	keys, err := middleware.NewStaticKey(key, "k1")
	if err != nil {
		t.Fatal(err)
	}

	// A sealed delivery for Encrypt, and a compressed one for Compress. Both
	// are built once: the measurement is of opening them, not of making them.
	rec := &publishRecorder{}
	sealer := mustWrapPublisher(t, rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: keys}))
	if err := sealer.Publish(ctx, &mq.Message{Topic: "orders", Body: []byte("payload")}); err != nil {
		t.Fatal(err)
	}
	sealedBody, sealedHeaders := rec.sent[0].Body, rec.sent[0].Headers

	var zbuf bytes.Buffer
	zw := gzip.NewWriter(&zbuf)
	if _, err := zw.Write(bytes.Repeat([]byte("compress me "), 64)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	gzipped := zbuf.Bytes()

	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	cases := []struct {
		name string
		mw   mq.Middleware
		// prepare fills a fresh delivery for each run, since a middleware
		// that consumes its input (Compress, Encrypt) cannot be measured
		// twice over the same one.
		prepare func(*mq.Delivery)
		want    float64
		why     string
		// skipUnderRace marks a row whose count is dominated by a standard
		// library allocation the race detector perturbs.
		skipUnderRace bool
	}{
		{
			name: "Recover", mw: middleware.Recover(nil), want: 0,
			why: "a deferred recover that does not fire allocates nothing",
		},
		{
			name: "Timeout", mw: mustTimeout(t, time.Minute), want: 4,
			why: "a context with a deadline: the context, its timer and the cancel",
		},
		{
			name: "Retry", mw: mustRetry(t, middleware.RetryConfig{MaxAttempts: 3, Clock: clk}), want: 0,
			why: "the attempt loop on the success path takes no branch that allocates",
		},
		{
			name: "Retry/unconfigured", mw: mustRetry(t, middleware.RetryConfig{}), want: 0,
			why: "an unconfigured Retry is not in the chain at all",
		},
		{
			name: "Logging/off", mw: middleware.Logging(nil), want: 0,
			why: "a nil logger is not in the chain at all",
		},
		{
			name: "Logging/success", mw: middleware.Logging(discardLogger()), want: 0,
			why: "The specification logs failures only, so a successful delivery reaches no formatting",
		},
		{
			name: "CorrelationID", mw: middleware.CorrelationID(middleware.CorrelationConfig{}),
			prepare: func(d *mq.Delivery) { d.Properties.CorrelationID = "corr-1" },
			want:    2, why: "the context carrying the id, and the string it boxes into an any",
		},
		{
			name: "CorrelationID/trace", mw: middleware.CorrelationID(middleware.CorrelationConfig{Trace: true}),
			prepare: func(d *mq.Delivery) {
				d.Properties.CorrelationID = "corr-1"
				d.Headers.Set(middleware.HeaderTraceParent, []byte(traceParent))
			},
			want: 5, why: "a context and a boxed value for each of the id and the trace, and the trace state",
		},
		{
			name: "RateLimit", mw: mustRateLimit(t, middleware.RateLimitConfig{Rate: 1e9, Burst: 1 << 20, Clock: clk}),
			want: 0, why: "a token bucket is a mutex and two words",
		},
		{
			name: "CircuitBreaker", mw: mustCircuitBreaker(t, middleware.BreakerConfig{Threshold: 1 << 20, Cooldown: time.Minute, Clock: clk}),
			want: 0, why: "a closed circuit is two lock-guarded state reads",
		},
		{
			name: "Filter/kept", mw: middleware.Filter(func(*mq.Delivery) bool { return true }), want: 0,
			why: "a predicate that keeps the delivery adds one call and nothing else",
		},
		{
			name: "Dedup/new", mw: middleware.Dedup(middleware.DedupConfig{Store: alwaysNew{}}),
			prepare: func(d *mq.Delivery) { d.ID = "id-1" },
			want:    0, why: "the store is the caller's; the middleware itself allocates nothing",
		},
		{
			name: "Dedup/unkeyed", mw: middleware.Dedup(middleware.DedupConfig{Store: alwaysNew{}}), want: 0,
			why: "a delivery with no id is passed through without touching the store",
		},
		{
			name: "Validate/valid", mw: middleware.Validate(func(*mq.Delivery) error { return nil }), want: 0,
			why: "a validator that accepts adds one call and nothing else",
		},
		{
			name: "Compress/plain", mw: mustCompress(t, middleware.CompressConfig{}), want: 0,
			why: "a body with no content encoding is not touched",
		},
		{
			name: "Compress/gzip", mw: mustCompress(t, middleware.CompressConfig{}),
			prepare: func(d *mq.Delivery) {
				d.Body = gzipped
				d.Properties.ContentEncoding = middleware.ContentEncodingGzip
			},
			want: 11, why: "the gzip reader, its 32 KiB window and the output buffer",
			skipUnderRace: true,
		},
		{
			name: "Encrypt/off", mw: middleware.Encrypt(middleware.EncryptConfig{}), want: 0,
			why: "a nil key provider is not in the chain at all",
		},
		{
			name: "Encrypt/sealed", mw: middleware.Encrypt(middleware.EncryptConfig{Keys: keys}),
			prepare: func(d *mq.Delivery) {
				d.Body = sealedBody
				d.Headers = sealedHeaders.Clone()
			},
			want: 2, why: "the opened plaintext and the AEAD's own scratch; both are new buffers by construction",
		},
		{
			name: "Metrics/off", mw: middleware.Metrics(nil, nil), want: 0,
			why: "a nil recorder is not in the chain at all",
		},
		{
			name: "Metrics", mw: middleware.Metrics(nullRecorder{}, clk), want: 0,
			why: "two clock reads and one call; the label values are the delivery's own strings",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.skipUnderRace && allocsRaceDetector {
				t.Skip("the race detector changes how many objects compress/gzip allocates; `make allocs` runs without it")
			}
			h := c.mw(noop)
			got := testing.AllocsPerRun(200, func() {
				d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte("payload")}}
				if c.prepare != nil {
					c.prepare(d)
				}
				if err := h.Handle(ctx, d); err != nil {
					t.Fatal(err)
				}
			})
			// The fresh delivery itself is one allocation in every case, and
			// the prepared header clone is another where prepare makes one;
			// what the case declares is the middleware's own cost above the
			// baseline, which baselineAllocs measures with the same shape.
			base := baselineAllocs(t, ctx, noop, c.prepare)
			if got-base != c.want {
				t.Fatalf("%s allocated %v times per delivery above the %v-allocation baseline, want %v (%s)",
					c.name, got-base, base, c.want, c.why)
			}
		})
	}
}

// baselineAllocs measures the fixture without the middleware, so that a case
// declares the middleware's own cost and not the test's.
func baselineAllocs(t *testing.T, ctx context.Context, noop mq.Handler, prepare func(*mq.Delivery)) float64 {
	t.Helper()
	return testing.AllocsPerRun(200, func() {
		d := &mq.Delivery{Message: mq.Message{Topic: "orders", Body: []byte("payload")}}
		if prepare != nil {
			prepare(d)
		}
		if err := noop.Handle(ctx, d); err != nil {
			t.Fatal(err)
		}
	})
}

// alwaysNew is an IdempotencyStore that has never seen anything, so that the
// measurement is of the middleware and not of a map.
type alwaysNew struct{}

func (alwaysNew) Seen(context.Context, string) (bool, error) { return false, nil }

func (alwaysNew) Forget(context.Context, string) {}

// nullRecorder keeps nothing.
type nullRecorder struct{}

func (nullRecorder) HandlerDuration(string, string, string, time.Duration) {}

// discardLogger is a logger enabled at every level whose handler writes
// nowhere, so that "did the middleware format anything" is what is measured.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestPublishAllocs pins what the publish-side middlewares cost per
// publication.
func TestPublishAllocs(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock()
	cases := []struct {
		name string
		mw   []mq.PublishMiddleware
		want float64
		why  string
	}{
		{"none", nil, 0, "the wrapper recycles the one-element slice a single publication presents itself as"},
		{"ID/off", []mq.PublishMiddleware{middleware.ID(nil)}, 0, "a nil generator is not in the chain"},
		{"ID", []mq.PublishMiddleware{middleware.ID(func() string { return "fixed" })}, 0, "the id is a string the generator already holds"},
		{"Timestamp", []mq.PublishMiddleware{middleware.Timestamp(clk)}, 0, "one clock read for the whole batch"},
		{
			"Compress/below MinSize",
			[]mq.PublishMiddleware{mustPublishCompress(t, middleware.CompressConfig{MinSize: 1 << 20})},
			0, "a body below MinSize is not touched",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := mustWrapPublisher(t, nullPublisher{}, c.mw...)
			m := &mq.Message{Topic: "orders", Body: []byte("payload")}
			got := testing.AllocsPerRun(200, func() {
				m.ID = ""
				m.Properties.CreatedAt = time.Time{}
				if err := p.Publish(ctx, m); err != nil {
					t.Fatal(err)
				}
			})
			if got != c.want {
				t.Fatalf("a publication through %s allocated %v times, want %v (%s)", c.name, got, c.want, c.why)
			}
		})
	}
}

// nullPublisher accepts everything and keeps nothing.
type nullPublisher struct{}

func (nullPublisher) Publish(context.Context, *mq.Message, ...mq.PublishOption) error { return nil }

func (nullPublisher) PublishBatch(context.Context, []*mq.Message, ...mq.PublishOption) error {
	return nil
}

func (nullPublisher) Flush(context.Context) error { return nil }

func (nullPublisher) Close() error { return nil }

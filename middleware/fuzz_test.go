package middleware_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/middleware"
)

// FuzzTraceContext is the test suite's target 24: "W3C parse never
// panics; valid contexts round trip; bounded metadata".
//
// It fuzzes the parser directly and through the middleware, because the two
// have different failure modes: the parser must never accept something it
// cannot render back, and the middleware must never let a malformed header
// fail a delivery — the specification makes broker-delivered metadata untrusted, and a
// consumer a publisher can stop by writing nonsense into a header is a denial
// of service.
func FuzzTraceContext(f *testing.F) {
	// The seeds are middleware.TraceParentSeeds and not a literal list here:
	// the test suite puts this target's twin in integrations/otel, which
	// is a separate module, and a corpus written out twice is two corpora the
	// day after somebody adds a case to one.
	for _, s := range middleware.TraceParentSeeds() {
		f.Add(s.Parent, s.State)
	}

	f.Fuzz(func(t *testing.T, parent, state string) {
		got, err := middleware.ParseTraceParent(parent)
		if err != nil {
			// A refusal renders as nothing at all, so a rejected context can
			// never be propagated as though it were one.
			if got != (middleware.TraceContext{}) {
				t.Fatalf("ParseTraceParent(%q) refused but returned %+v", parent, got)
			}
			if s := got.String(); s != "" {
				t.Fatalf("a refused trace context renders as %q, want the empty string", s)
			}
		} else {
			if !got.Valid() {
				t.Fatalf("ParseTraceParent(%q) accepted an invalid context %+v", parent, got)
			}
			// Round trip: what was parsed renders back byte for byte, and
			// parsing that again gives the same value.
			rendered := got.String()
			if rendered != parent {
				t.Fatalf("round trip: parsed %q and rendered %q", parent, rendered)
			}
			again, err := middleware.ParseTraceParent(rendered)
			if err != nil || again != got {
				t.Fatalf("re-parsing %q gave %+v, %v, want %+v", rendered, again, err, got)
			}
		}

		// The same input through the middleware: never a failure, never a
		// panic, and any state it carries is bounded and free of control
		// characters.
		var carried middleware.TraceContext
		var ok bool
		h := middleware.CorrelationID(middleware.CorrelationConfig{Trace: true})(
			mq.HandlerFunc(func(ctx context.Context, _ *mq.Delivery) error {
				carried, ok = middleware.TraceFromContext(ctx)
				return nil
			}),
		)
		d := delivery("x")
		d.Headers.Set(middleware.HeaderTraceParent, []byte(parent))
		d.Headers.Set(middleware.HeaderTraceState, []byte(state))
		if herr := h.Handle(context.Background(), d); herr != nil {
			t.Fatalf("Handle = %v; a malformed trace header must not fail a delivery ", herr)
		}
		if ok != (err == nil) {
			t.Fatalf("the middleware carried a trace context = %v for a header the parser answered %v", ok, err)
		}
		if ok {
			if len(carried.State) > middleware.MaxTraceStateLen {
				t.Fatalf("tracestate is %d bytes, want at most %d (bounded metadata)", len(carried.State), middleware.MaxTraceStateLen)
			}
			for i := range len(carried.State) {
				if c := carried.State[i]; c < 0x20 || c == 0x7f {
					t.Fatalf("tracestate carries control byte %#x at %d; forwarding it is an injection", c, i)
				}
			}
		}
	})
}

// FuzzDedupStore holds the LRU idempotency store to the contract Dedup relies
// on, over arbitrary key sequences: the bound is never exceeded, a key just
// recorded is reported as seen, and Forget really forgets.
//
// The bound is the interesting half. The specification requires bounded local state, and a
// store that grew past its capacity under some interleaving of keys would be
// a slow memory leak in every consumer that installed Dedup.
func FuzzDedupStore(f *testing.F) {
	f.Add([]byte("abcabc"), uint8(4))
	f.Add([]byte(""), uint8(1))
	f.Add([]byte("aaaaaaaaaa"), uint8(1))
	f.Add(bytes.Repeat([]byte("k"), 300), uint8(8))
	f.Add([]byte{0, 1, 2, 3, 0, 1}, uint8(2))

	f.Fuzz(func(t *testing.T, keys []byte, capacity uint8) {
		c := int(capacity)%64 + 1
		s := middleware.NewLRUStore(c)
		ctx := context.Background()
		for i := range keys {
			k := string(keys[i : i+1])
			seen, err := s.Seen(ctx, k)
			if err != nil {
				t.Fatalf("Seen(%q) = %v", k, err)
			}
			// Whatever it answered, the key is present now.
			again, err := s.Seen(ctx, k)
			if err != nil || !again {
				t.Fatalf("Seen(%q) twice in a row = %v, %v; a key just recorded must be seen", k, again, err)
			}
			_ = seen
			if s.Len() > c {
				t.Fatalf("the store holds %d keys, want at most its capacity %d", s.Len(), c)
			}
		}
		if len(keys) > 0 {
			k := string(keys[0:1])
			s.Forget(ctx, k)
			seen, err := s.Seen(ctx, k)
			if err != nil || seen {
				t.Fatalf("Seen(%q) after Forget = %v, %v; a forgotten key must be new again", k, seen, err)
			}
		}
		if s.Cap() != c {
			t.Fatalf("Cap() = %d, want %d", s.Cap(), c)
		}
	})
}

// FuzzCompressLimit holds the decompression limit over arbitrary bodies and
// arbitrary limits: what comes out is never larger than the limit, a body
// that fits round-trips exactly, and one that does not is refused rather than
// expanded.
func FuzzCompressLimit(f *testing.F) {
	f.Add([]byte("hello"), uint32(1024))
	f.Add([]byte(""), uint32(1))
	f.Add(bytes.Repeat([]byte("a"), 4096), uint32(64))
	f.Add(bytes.Repeat([]byte("a"), 4096), uint32(4096))
	f.Add(make([]byte, 1<<16), uint32(1<<10))
	f.Add([]byte("not gzip at all"), uint32(1024))

	f.Fuzz(func(t *testing.T, body []byte, limit uint32) {
		lim := int64(limit%(1<<20)) + 1
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}

		var got []byte
		h := middleware.Compress(middleware.CompressConfig{MaxDecompressed: lim})(
			mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
				got = bytes.Clone(d.Body)
				return nil
			}),
		)
		d := delivery("")
		d.Body = buf.Bytes()
		d.Properties.ContentEncoding = middleware.ContentEncodingGzip
		err := h.Handle(context.Background(), d)
		switch {
		case int64(len(body)) > lim:
			if !errors.Is(err, middleware.ErrTooLarge) {
				t.Fatalf("a %d-byte body under a %d-byte limit = %v, want ErrTooLarge", len(body), lim, err)
			}
			if got != nil {
				t.Fatalf("the handler saw %d bytes of a body that exceeds the limit", len(got))
			}
		default:
			if err != nil {
				t.Fatalf("a %d-byte body under a %d-byte limit = %v, want nil", len(body), lim, err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("round trip of %d bytes gave %d", len(body), len(got))
			}
		}
		if int64(len(got)) > lim {
			t.Fatalf("the handler was given %d bytes under a %d-byte limit", len(got), lim)
		}
	})
}

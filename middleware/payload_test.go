package middleware_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"runtime"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/middleware"
)

// gzipOf compresses b the way a publisher would, so that a test can build an
// inbound body without going through the publish chain.
func gzipOf(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// ---------------------------------------------------------------- Compress

func TestCompressDecompressesAnInboundBody(t *testing.T) {
	want := bytes.Repeat([]byte("the quick brown fox "), 64)
	var got []byte
	h := middleware.Compress(middleware.CompressConfig{})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			got = bytes.Clone(d.Body)
			if d.Properties.ContentEncoding != "" {
				t.Errorf("the content encoding is still %q after decompression", d.Properties.ContentEncoding)
			}
			return nil
		}),
	)
	d := delivery("")
	d.Body = gzipOf(t, want)
	d.Properties.ContentEncoding = middleware.ContentEncodingGzip
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %d bytes, want the %d the publisher compressed", len(got), len(want))
	}
}

func TestCompressReadsTheContentEncodingHeaderToo(t *testing.T) {
	want := []byte("payload")
	var got []byte
	h := middleware.Compress(middleware.CompressConfig{})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			got = bytes.Clone(d.Body)
			return nil
		}),
	)
	d := delivery("")
	d.Body = gzipOf(t, want)
	d.Headers.Set(middleware.HeaderContentEncoding, []byte(middleware.ContentEncodingGzip))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("body = %q, want %q: the specification maps the property to a header on most backends", got, want)
	}
	if _, ok := d.Headers.Get(middleware.HeaderContentEncoding); ok {
		t.Fatal("the content-encoding header survived the decompression; the body is no longer encoded")
	}
}

func TestCompressPassesThroughAnUnencodedBody(t *testing.T) {
	var got []byte
	h := middleware.Compress(middleware.CompressConfig{})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			got = d.Body
			return nil
		}),
	)
	d := delivery("plain")
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if string(got) != "plain" {
		t.Fatalf("body = %q, want it untouched", got)
	}
}

func TestCompressRefusesAnOverLimitPayloadRatherThanExpandingIt(t *testing.T) {
	// The specification: "compression middleware caps decompressed output". A gzip stream
	// is a program, and a decompressor that reads to EOF into a growing
	// buffer allocates everything the stream describes before anything can
	// notice. 8 MiB of zeros compresses to a few kilobytes.
	const limit = 64 << 10
	bomb := gzipOf(t, make([]byte, 8<<20))
	if len(bomb) > limit {
		t.Fatalf("the fixture is %d compressed bytes, which is not a bomb below the %d limit", len(bomb), limit)
	}
	var reached bool
	h := middleware.Compress(middleware.CompressConfig{MaxDecompressed: limit})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			reached = true
			return nil
		}),
	)
	d := delivery("")
	d.Body = bomb
	d.Properties.ContentEncoding = middleware.ContentEncodingGzip
	err := h.Handle(context.Background(), d)
	if !errors.Is(err, middleware.ErrTooLarge) {
		t.Fatalf("Handle = %v, want ErrTooLarge: a body that expands past the limit is refused, not expanded", err)
	}
	if reached {
		t.Fatal("the handler saw a body that exceeded the decompression limit")
	}
	if mq.CodeOf(err) != mq.CodeMessageTooLarge {
		t.Fatalf("CodeOf = %v, want CodeMessageTooLarge", mq.CodeOf(err))
	}
}

func TestCompressAllocatesNoMoreThanTheLimitBeforeRefusing(t *testing.T) {
	// Refusing an over-limit body is not enough on its own: a decompressor
	// that reads the whole stream and THEN compares the size has already
	// allocated everything the attacker asked for, which is the denial of
	// service the specification names. The limit has to be applied while the body is still
	// compressed, and the only way to see the difference from outside is to
	// count the bytes the process allocated.
	const limit = 64 << 10
	const bombSize = 64 << 20
	bomb := gzipOf(t, make([]byte, bombSize))
	h := middleware.Compress(middleware.CompressConfig{MaxDecompressed: limit})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			t.Error("the handler saw a body that exceeded the limit")
			return nil
		}),
	)
	d := delivery("")
	d.Body = bomb
	d.Properties.ContentEncoding = middleware.ContentEncodingGzip

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	err := h.Handle(context.Background(), d)
	runtime.ReadMemStats(&after)

	if !errors.Is(err, middleware.ErrTooLarge) {
		t.Fatalf("Handle = %v, want ErrTooLarge", err)
	}
	// Generous: the gzip reader's window is 32 KiB, the output buffer may
	// double once to 2x the limit, and the measurement catches whatever else
	// the test goroutine did. Anything approaching the 64 MiB the stream
	// describes is the unbounded decompressor.
	const ceiling = 8 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > ceiling {
		t.Fatalf("decompressing a %d-byte bomb under a %d-byte limit allocated %d bytes, want under %d: the limit must be applied while the body is still compressed",
			bombSize, limit, grew, ceiling)
	}
}

func TestCompressAcceptsExactlyTheLimit(t *testing.T) {
	// The boundary is what tells a correct limit from an off-by-one that
	// refuses a body it should accept.
	const limit = 4096
	body := bytes.Repeat([]byte("x"), limit)
	var got int
	h := middleware.Compress(middleware.CompressConfig{MaxDecompressed: limit})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			got = len(d.Body)
			return nil
		}),
	)
	d := delivery("")
	d.Body = gzipOf(t, body)
	d.Properties.ContentEncoding = middleware.ContentEncodingGzip
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil for a body of exactly the limit", err)
	}
	if got != limit {
		t.Fatalf("body = %d bytes, want %d", got, limit)
	}
	// One byte more is refused.
	d2 := delivery("")
	d2.Body = gzipOf(t, append(body, 'x'))
	d2.Properties.ContentEncoding = middleware.ContentEncodingGzip
	if err := h.Handle(context.Background(), d2); !errors.Is(err, middleware.ErrTooLarge) {
		t.Fatalf("Handle of limit+1 bytes = %v, want ErrTooLarge", err)
	}
}

func TestCompressReportsAMalformedStream(t *testing.T) {
	h := middleware.Compress(middleware.CompressConfig{})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			t.Error("the handler saw a body that is not a gzip stream")
			return nil
		}),
	)
	d := delivery("not gzip at all")
	d.Properties.ContentEncoding = middleware.ContentEncodingGzip
	if err := h.Handle(context.Background(), d); err == nil {
		t.Fatal("Handle = nil for a body whose content encoding lies about it")
	}
}

func TestCompressRefusesAnUnusableConfiguration(t *testing.T) {
	for _, cfg := range []middleware.CompressConfig{
		{MinSize: -1}, {MaxDecompressed: -1}, {Level: 42}, {Level: -5},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("Compress(%+v) did not panic", cfg)
				}
			}()
			middleware.Compress(cfg)
		}()
	}
}

func TestPublishCompressRoundTrips(t *testing.T) {
	body := bytes.Repeat([]byte("compress me "), 128)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishCompress(middleware.CompressConfig{MinSize: 16}))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: bytes.Clone(body)}); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	sent := rec.sent[0]
	if sent.Properties.ContentEncoding != middleware.ContentEncodingGzip {
		t.Fatalf("content encoding = %q, want gzip", sent.Properties.ContentEncoding)
	}
	if len(sent.Body) >= len(body) {
		t.Fatalf("the compressed body is %d bytes and the original %d; it was not compressed", len(sent.Body), len(body))
	}
	var got []byte
	h := middleware.Compress(middleware.CompressConfig{})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			got = bytes.Clone(d.Body)
			return nil
		}),
	)
	d := delivery("")
	d.Body = sent.Body
	d.Properties.ContentEncoding = sent.Properties.ContentEncoding
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the round trip did not return the published body")
	}
}

func TestPublishCompressLeavesSmallAndIncompressibleBodiesAlone(t *testing.T) {
	small := []byte("tiny")
	// Random bytes do not compress; gzip's header makes them bigger.
	noisy := make([]byte, 4096)
	if _, err := rand.Read(noisy); err != nil {
		t.Fatal(err)
	}
	already := bytes.Repeat([]byte("a"), 4096)

	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishCompress(middleware.CompressConfig{MinSize: 16}))
	ms := []*mq.Message{
		{Topic: "t", Body: bytes.Clone(small)},
		{Topic: "t", Body: bytes.Clone(noisy)},
		{Topic: "t", Body: bytes.Clone(already), Properties: mq.Properties{ContentEncoding: "br"}},
	}
	if err := p.PublishBatch(context.Background(), ms); err != nil {
		t.Fatalf("PublishBatch = %v, want nil", err)
	}
	got := rec.batch[0]
	if !bytes.Equal(got[0].Body, small) || got[0].Properties.ContentEncoding != "" {
		t.Fatalf("a %d-byte body below MinSize was compressed", len(small))
	}
	if !bytes.Equal(got[1].Body, noisy) || got[1].Properties.ContentEncoding != "" {
		t.Fatal("an incompressible body was published compressed and larger")
	}
	if !bytes.Equal(got[2].Body, already) || got[2].Properties.ContentEncoding != "br" {
		t.Fatal("a body that already declared a content encoding was compressed again")
	}
}

// ---------------------------------------------------------------- Encrypt

func testKey(t *testing.T) *middleware.StaticKey {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	k, err := middleware.NewStaticKey(key, "k1")
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryptRoundTrips(t *testing.T) {
	k := testKey(t)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: k}))
	body := []byte("the crown jewels")
	if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: bytes.Clone(body)}); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	sent := rec.sent[0]
	if bytes.Contains(sent.Body, body) {
		t.Fatalf("the sealed body contains the plaintext: %q", sent.Body)
	}
	id, ok := sent.Headers.Get(middleware.HeaderKeyID)
	if !ok || string(id) != "k1" {
		t.Fatalf("key id header = %q, %v, want k1", id, ok)
	}

	var got []byte
	h := middleware.Encrypt(middleware.EncryptConfig{Keys: k})(
		mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			got = bytes.Clone(d.Body)
			if _, still := d.Headers.Get(middleware.HeaderKeyID); still {
				t.Error("the key-id header survived the open; the body is no longer sealed")
			}
			return nil
		}),
	)
	d := delivery("")
	d.Body = sent.Body
	d.Headers = sent.Headers.Clone()
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatalf("Handle = %v, want nil", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("opened body = %q, want %q", got, body)
	}
}

func TestTwoSealsOfOnePlaintextDiffer(t *testing.T) {
	// AES-GCM's failure mode is nonce reuse: two messages sealed under one
	// key with one nonce leak the exclusive-or of their plaintexts and let
	// an attacker forge tags. Every nonce comes from crypto/rand, so two
	// seals of the same bytes are different bodies — and a counter-based
	// nonce, which is the implementation this test exists to refuse, would
	// make them identical after a restart.
	k := testKey(t)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: k}))
	body := []byte("the same plaintext, twice")
	const n = 16
	for range n {
		if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: bytes.Clone(body)}); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool, n)
	for i, m := range rec.sent {
		s := string(m.Body)
		if seen[s] {
			t.Fatalf("seal %d produced a body identical to an earlier one; the nonce repeated", i)
		}
		seen[s] = true
	}
}

func TestATamperedCiphertextFailsToOpen(t *testing.T) {
	k := testKey(t)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: k}))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: []byte("authentic payload")}); err != nil {
		t.Fatal(err)
	}
	sealed := rec.sent[0].Body

	cases := []struct {
		name string
		body []byte
	}{
		{"one bit of ciphertext", flip(sealed, len(sealed)-20)},
		{"one bit of the tag", flip(sealed, len(sealed)-1)},
		{"one bit of the nonce", flip(sealed, 0)},
		{"truncated", sealed[:len(sealed)-1]},
		{"empty", nil},
		{"shorter than a nonce", sealed[:4]},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := middleware.Encrypt(middleware.EncryptConfig{Keys: k})(
				mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
					t.Error("the handler saw a body that did not authenticate")
					return nil
				}),
			)
			d := delivery("")
			d.Body = c.body
			d.Headers.Set(middleware.HeaderKeyID, []byte("k1"))
			err := h.Handle(context.Background(), d)
			if err == nil {
				t.Fatal("Handle = nil for a body that was tampered with")
			}
			if mq.CodeOf(err) != mq.CodeCorruptMessage {
				t.Fatalf("CodeOf = %v, want CodeCorruptMessage", mq.CodeOf(err))
			}
		})
	}
}

// flip returns a copy of b with one bit of byte i inverted.
func flip(b []byte, i int) []byte {
	out := bytes.Clone(b)
	out[i] ^= 0x01
	return out
}

func TestOpeningUnderAnotherKeyFails(t *testing.T) {
	sealer, opener := testKey(t), testKey(t)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishEncrypt(middleware.EncryptConfig{Keys: sealer}))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: []byte("secret")}); err != nil {
		t.Fatal(err)
	}
	h := middleware.Encrypt(middleware.EncryptConfig{Keys: opener})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			t.Error("a body sealed under another key was opened")
			return nil
		}),
	)
	d := delivery("")
	d.Body = rec.sent[0].Body
	d.Headers = rec.sent[0].Headers.Clone()
	if err := h.Handle(context.Background(), d); err == nil {
		t.Fatal("Handle = nil for a body sealed under a different key")
	}
}

func TestAdditionalDataIsAuthenticated(t *testing.T) {
	k := testKey(t)
	rec := &publishRecorder{}
	p := middleware.WrapPublisher(rec, middleware.PublishEncrypt(
		middleware.EncryptConfig{Keys: k, AdditionalData: []byte("tenant-a")},
	))
	if err := p.Publish(context.Background(), &mq.Message{Topic: "orders", Body: []byte("secret")}); err != nil {
		t.Fatal(err)
	}
	h := middleware.Encrypt(middleware.EncryptConfig{Keys: k, AdditionalData: []byte("tenant-b")})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			t.Error("a body sealed with other additional data was opened")
			return nil
		}),
	)
	d := delivery("")
	d.Body = rec.sent[0].Body
	d.Headers = rec.sent[0].Headers.Clone()
	if err := h.Handle(context.Background(), d); err == nil {
		t.Fatal("Handle = nil for a body whose additional data differs")
	}
}

func TestEncryptPassesUnsealedBodiesThroughUnlessRequired(t *testing.T) {
	k := testKey(t)
	var reached bool
	lenient := middleware.Encrypt(middleware.EncryptConfig{Keys: k})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error { reached = true; return nil }),
	)
	if err := lenient.Handle(context.Background(), delivery("plain")); err != nil {
		t.Fatalf("Handle = %v, want nil: an unsealed body passes through during a migration", err)
	}
	if !reached {
		t.Fatal("the handler did not see an unsealed body")
	}
	strict := middleware.Encrypt(middleware.EncryptConfig{Keys: k, RequireSealed: true})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			t.Error("RequireSealed let an unsealed body through")
			return nil
		}),
	)
	if err := strict.Handle(context.Background(), delivery("plain")); !errors.Is(err, middleware.ErrNotSealed) {
		t.Fatalf("Handle = %v, want ErrNotSealed", err)
	}
}

func TestAnUnknownKeyIDIsRefused(t *testing.T) {
	k := testKey(t)
	h := middleware.Encrypt(middleware.EncryptConfig{Keys: k})(
		mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			t.Error("a body sealed under an unknown key reached the handler")
			return nil
		}),
	)
	d := delivery("anything")
	d.Headers.Set(middleware.HeaderKeyID, []byte("a-key-we-do-not-have"))
	if err := h.Handle(context.Background(), d); !errors.Is(err, middleware.ErrNoKey) {
		t.Fatalf("Handle = %v, want ErrNoKey", err)
	}
}

func TestNewAESGCMRefusesAKeyThatIsNotAKey(t *testing.T) {
	for _, n := range []int{0, 1, 15, 24, 31, 33} {
		if _, err := middleware.NewAESGCM(make([]byte, n)); !errors.Is(err, middleware.ErrInvalidConfig) {
			t.Fatalf("NewAESGCM(%d bytes) = %v, want ErrInvalidConfig", n, err)
		}
	}
	for _, n := range []int{16, 32} {
		if _, err := middleware.NewAESGCM(make([]byte, n)); err != nil {
			t.Fatalf("NewAESGCM(%d bytes) = %v, want nil", n, err)
		}
	}
}

func TestEncryptWithNoKeysCostsTheChainNothing(t *testing.T) {
	inner := mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil })
	if got := middleware.Encrypt(middleware.EncryptConfig{})(inner); got == nil {
		t.Fatal("Encrypt with no keys returned no handler")
	} else if _, isFunc := got.(mq.HandlerFunc); !isFunc {
		t.Fatalf("Encrypt with no keys returned %T, want the handler it was given", got)
	}
}

package compress_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/compress"
)

// incompressible returns n bytes gzip cannot shrink.
//
// A PRNG and not an arithmetic pattern. The first version of this helper used
// `byte(i*i*7 + i*31)`, which LOOKS structureless and is not: gzip found the
// quadratic and shrank it, TestIncompressibleBodyFallsBackToStored failed
// asserting CodecStored against a genuine CodecGzip, and the defect was in
// the fixture rather than in the encoder. Random bytes are incompressible by
// construction, and the seed is fixed so the corpus is reproducible.
func incompressible(n int) []byte {
	r := rand.New(rand.NewPCG(0xd01d01d01d01d01, 0xc0117e55))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// allCodecs is every coding NewRegistry holds, which every round-trip case
// below runs against: a family whose members are tested one at a time is a family whose members drift.
var allCodecs = []struct {
	id   compress.CodecID
	name string
}{
	{compress.CodecStored, "stored"},
	{compress.CodecGzip, "gzip"},
	{compress.CodecFlate, "flate"},
	{compress.CodecZlib, "zlib"},
}

func TestRoundTripEveryCodec(t *testing.T) {
	bodies := map[string][]byte{
		"empty":          {},
		"one byte":       {0},
		"short text":     []byte("hello"),
		"compressible":   bytes.Repeat([]byte("iqmq "), 4096),
		"incompressible": incompressible(8192),
		"magic prefixed": append([]byte(compress.Magic), "not really a frame"...),
	}
	for _, c := range allCodecs {
		for name, body := range bodies {
			t.Run(c.name+"/"+name, func(t *testing.T) {
				e, err := compress.NewEncoder(compress.Config{Codec: c.id, MinSize: 1})
				if err != nil {
					t.Fatal(err)
				}
				d, err := compress.NewDecoder(compress.Config{})
				if err != nil {
					t.Fatal(err)
				}
				framed, err := e.Encode(body)
				if err != nil {
					t.Fatal(err)
				}
				if compress.Framed(body) {
					// A body that already looks framed is passed through
					// untouched rather than framed twice, which is the only
					// way a caller can end up with a body this package
					// cannot read back. It is a real hazard, not a
					// curiosity: see TestAlreadyFramedBodyIsNotReframed.
					if !bytes.Equal(framed, body) {
						t.Fatal("a body that already carries the magic was re-framed")
					}
					return
				}
				got, err := d.Decode(framed)
				if err != nil {
					t.Fatalf("Decode after Encode: %v", err)
				}
				if !bytes.Equal(got, body) {
					t.Fatalf("round trip changed the body: %d bytes in, %d out", len(body), len(got))
				}
			})
		}
	}
}

// TestAlreadyFramedBodyIsNotReframed pins the pass-through above and says
// what it costs, because it is the one sharp edge in this package's API.
//
// A caller whose plaintext genuinely begins with "IQZ1" gets it published
// unframed, and a consumer with UnframedFail would then refuse a message the
// publisher thought it had framed. The alternative -- frame unconditionally --
// makes Encode(Encode(b)) double-framed, which is worse: it decodes to a
// framed body silently, and the handler sees a body with ten bytes of header
// glued to the front. The specification records the choice; a caller who
// cannot accept either uses the wire envelope, which is length-prefixed and
// needs no sniff.
func TestAlreadyFramedBodyIsNotReframed(t *testing.T) {
	e, err := compress.NewEncoder(compress.Config{MinSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	body := append([]byte(compress.Magic), bytes.Repeat([]byte("x"), 2048)...)
	got, err := e.Encode(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Encode framed a body that already carried the magic (%d bytes -> %d)", len(body), len(got))
	}
}

func TestShortBodyIsStored(t *testing.T) {
	e, err := compress.NewEncoder(compress.Config{Codec: compress.CodecGzip, MinSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	framed, err := e.Encode([]byte("short"))
	if err != nil {
		t.Fatal(err)
	}
	h, payload, err := compress.Parse(framed)
	if err != nil {
		t.Fatal(err)
	}
	if h.Codec != compress.CodecStored {
		t.Errorf("codec = %d, want CodecStored: a body under MinSize must not be compressed", h.Codec)
	}
	if h.Size != 5 || !bytes.Equal(payload, []byte("short")) {
		t.Errorf("header = %+v, payload = %q", h, payload)
	}
}

func TestIncompressibleBodyFallsBackToStored(t *testing.T) {
	body := incompressible(4096)
	e, err := compress.NewEncoder(compress.Config{Codec: compress.CodecGzip, MinSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	framed, err := e.Encode(body)
	if err != nil {
		t.Fatal(err)
	}
	h, _, err := compress.Parse(framed)
	if err != nil {
		t.Fatal(err)
	}
	if h.Codec != compress.CodecStored {
		t.Errorf("codec = %d, want CodecStored", h.Codec)
	}
	if len(framed) > compress.HeaderLen+len(body) {
		t.Errorf("framed body is %d bytes for a %d-byte input: framing made it bigger than storing it", len(framed), len(body))
	}
}

func TestParseRefusals(t *testing.T) {
	good := func() []byte { return frame(compress.CodecGzip, 4, []byte("payload")) }
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{"nil", nil, compress.ErrNotFramed},
		{"short", []byte("IQZ"), compress.ErrNotFramed},
		{"header minus one", good()[:compress.HeaderLen-1], compress.ErrNotFramed},
		{"wrong magic", append([]byte("IQZ2"), good()[4:]...), compress.ErrNotFramed},
		{"reserved flag", func() []byte { b := good(); b[5] = 1; return b }(), compress.ErrBadFlags},
		{"every reserved bit", func() []byte { b := good(); b[5] = 0xFF; return b }(), compress.ErrBadFlags},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := compress.Parse(c.body); !errors.Is(err, c.want) {
				t.Errorf("Parse = %v, want %v", err, c.want)
			}
		})
	}
	// A header with nothing after it is a valid frame of an empty payload:
	// exactly HeaderLen bytes is the boundary, and it must not be an error.
	if _, payload, err := compress.Parse(good()[:compress.HeaderLen]); err != nil || len(payload) != 0 {
		t.Errorf("Parse of a bare header = (%q, %v), want an empty payload and no error", payload, err)
	}
}

func TestDecodeRefusals(t *testing.T) {
	d, err := compress.NewDecoder(compress.Config{MaxDecompressed: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("unknown codec", func(t *testing.T) {
		if _, err := d.Decode(frame(200, 4, []byte("x"))); !errors.Is(err, compress.ErrUnknownCodec) {
			t.Errorf("Decode = %v, want ErrUnknownCodec", err)
		}
	})
	t.Run("truncated stream", func(t *testing.T) {
		e, _ := compress.NewEncoder(compress.Config{Codec: compress.CodecGzip, MinSize: 1})
		framed, err := e.Encode(bytes.Repeat([]byte("iqmq "), 1024))
		if err != nil {
			t.Fatal(err)
		}
		// Chop the tail. gzip notices its CRC is missing; the point is that
		// nothing silently returns a short body.
		if _, err := d.Decode(framed[:len(framed)-8]); err == nil {
			t.Error("Decode accepted a truncated stream")
		}
	})
	t.Run("size mismatch", func(t *testing.T) {
		// A stored frame that declares five bytes and carries three. The
		// declared size is under the limit, so only the final comparison can
		// catch it -- which is the step that also catches a truncation whose
		// codec does not notice.
		if _, err := d.Decode(frame(compress.CodecStored, 5, []byte("abc"))); !errors.Is(err, compress.ErrSizeMismatch) {
			t.Errorf("Decode = %v, want ErrSizeMismatch", err)
		}
	})
	t.Run("declared over the limit", func(t *testing.T) {
		if _, err := d.Decode(frame(compress.CodecStored, 1<<20, []byte("abc"))); !errors.Is(err, compress.ErrTooLarge) {
			t.Errorf("Decode = %v, want ErrTooLarge", err)
		}
	})
}

func TestRegistryRefusals(t *testing.T) {
	r := compress.NewRegistry()
	t.Run("reserved id", func(t *testing.T) {
		if err := r.Register(userCodec{id: 4}); !errors.Is(err, compress.ErrReservedCodec) {
			t.Errorf("Register = %v, want ErrReservedCodec", err)
		}
	})
	t.Run("duplicate id", func(t *testing.T) {
		if err := r.Register(userCodec{id: compress.FirstUserCodec}); err != nil {
			t.Fatal(err)
		}
		if err := r.Register(userCodec{id: compress.FirstUserCodec}); !errors.Is(err, compress.ErrDuplicateCodec) {
			t.Errorf("Register = %v, want ErrDuplicateCodec", err)
		}
	})
	t.Run("nil and nameless", func(t *testing.T) {
		if err := r.Register(nil); !errors.Is(err, compress.ErrInvalidConfig) {
			t.Errorf("Register(nil) = %v, want ErrInvalidConfig", err)
		}
		if err := r.Register(userCodec{id: compress.FirstUserCodec + 1, nameless: true}); !errors.Is(err, compress.ErrInvalidConfig) {
			t.Errorf("Register(nameless) = %v, want ErrInvalidConfig", err)
		}
	})
	t.Run("stdlib four are present", func(t *testing.T) {
		names := compress.NewRegistry().Names()
		for _, c := range allCodecs {
			if names[c.id] != c.name {
				t.Errorf("Names()[%d] = %q, want %q", c.id, names[c.id], c.name)
			}
		}
	})
}

// TestAThirdPartyCodecRoundTrips is the follow-up's acceptance test: the
// reason this package exists rather than only middleware.Compress is that a
// coding the root module cannot import can be plugged in. userCodec stands in
// for zstd.
func TestAThirdPartyCodecRoundTrips(t *testing.T) {
	r := compress.NewRegistry()
	const id = compress.FirstUserCodec + 7
	if err := r.Register(userCodec{id: id}); err != nil {
		t.Fatal(err)
	}
	e, err := compress.NewEncoder(compress.Config{Codec: id, MinSize: 1, Registry: r})
	if err != nil {
		t.Fatal(err)
	}
	d, err := compress.NewDecoder(compress.Config{Registry: r})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("aaaaabbbbb"), 200)
	framed, err := e.Encode(body)
	if err != nil {
		t.Fatal(err)
	}
	h, _, err := compress.Parse(framed)
	if err != nil {
		t.Fatal(err)
	}
	if h.Codec != id {
		t.Fatalf("codec = %d, want the registered %d", h.Codec, id)
	}
	got, err := d.Decode(framed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("the third-party codec did not round trip")
	}
	// And a decoder WITHOUT the registration refuses rather than guessing.
	plain, err := compress.NewDecoder(compress.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Decode(framed); !errors.Is(err, compress.ErrUnknownCodec) {
		t.Errorf("a decoder without the codec returned %v, want ErrUnknownCodec", err)
	}
}

// userCodec is a stand-in third-party coding: run-length encoding, which is
// real enough to shrink the test's body and small enough to be obviously
// correct.
type userCodec struct {
	id       compress.CodecID
	nameless bool
}

func (c userCodec) ID() compress.CodecID { return c.id }

func (c userCodec) Name() string {
	if c.nameless {
		return ""
	}
	return "rle"
}

func (c userCodec) Encode(dst, src []byte) ([]byte, error) {
	for i := 0; i < len(src); {
		n := 1
		for i+n < len(src) && src[i+n] == src[i] && n < 255 {
			n++
		}
		dst = append(dst, byte(n), src[i])
		i += n
	}
	return dst, nil
}

func (c userCodec) Decode(dst, src []byte, maxBytes int64) ([]byte, error) {
	if len(src)%2 != 0 {
		return nil, compress.ErrSizeMismatch
	}
	var total int64
	for i := 0; i < len(src); i += 2 {
		total += int64(src[i])
		if total > maxBytes {
			return nil, compress.ErrTooLarge
		}
	}
	for i := 0; i < len(src); i += 2 {
		for range int(src[i]) {
			dst = append(dst, src[i+1])
		}
	}
	return dst, nil
}

func TestConfigRefusals(t *testing.T) {
	for name, cfg := range map[string]compress.Config{
		"negative MinSize":         {MinSize: -1},
		"negative MaxDecompressed": {MaxDecompressed: -1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := compress.NewEncoder(cfg); !errors.Is(err, compress.ErrInvalidConfig) {
				t.Errorf("NewEncoder = %v, want ErrInvalidConfig", err)
			}
			if _, err := compress.NewDecoder(cfg); !errors.Is(err, compress.ErrInvalidConfig) {
				t.Errorf("NewDecoder = %v, want ErrInvalidConfig", err)
			}
		})
	}
	t.Run("codec not in the registry", func(t *testing.T) {
		if _, err := compress.NewEncoder(compress.Config{Codec: 99}); !errors.Is(err, compress.ErrUnknownCodec) {
			t.Errorf("NewEncoder = %v, want ErrUnknownCodec at construction, not per message", err)
		}
	})
}

// ---------------------------------------------------------------- middleware

func TestMiddlewareRoundTrip(t *testing.T) {
	pm, err := compress.PublishCompress(compress.Config{Codec: compress.CodecGzip, MinSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	mw, err := compress.Decompress(compress.Config{}, compress.UnframedPass)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("iqmq "), 500)
	var published []*mq.Message
	pub := pm(func(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
		published = ms
		return nil
	})
	// A nil message in the batch is skipped, not refused.
	if err := pub(context.Background(), "t", []*mq.Message{{Body: body}, nil}, nil); err != nil {
		t.Fatal(err)
	}
	if !compress.Framed(published[0].Body) {
		t.Fatal("PublishCompress did not frame the body")
	}
	if len(published[0].Body) >= len(body) {
		t.Errorf("framed body is %d bytes for a %d-byte input", len(published[0].Body), len(body))
	}

	var seen []byte
	h := mw(mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		seen = bytes.Clone(d.Body)
		return nil
	}))
	d := &mq.Delivery{Message: mq.Message{Body: published[0].Body}}
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, body) {
		t.Fatalf("the handler saw %d bytes, want the original %d", len(seen), len(body))
	}
}

func TestUnframedPolicy(t *testing.T) {
	for _, c := range []struct {
		name   string
		policy compress.Unframed
		ran    bool
		want   error
	}{
		{"pass", compress.UnframedPass, true, nil},
		{"fail", compress.UnframedFail, false, compress.ErrNotFramed},
	} {
		t.Run(c.name, func(t *testing.T) {
			mw, err := compress.Decompress(compress.Config{}, c.policy)
			if err != nil {
				t.Fatal(err)
			}
			ran := false
			h := mw(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { ran = true; return nil }))
			err = h.Handle(context.Background(), &mq.Delivery{Message: mq.Message{Body: []byte("plain")}})
			switch {
			case c.want == nil:
				if err != nil {
					t.Errorf("Handle = %v, want nil", err)
				}
			case !errors.Is(err, c.want):
				t.Errorf("Handle = %v, want %v", err, c.want)
			}
			if ran != c.ran {
				t.Errorf("handler ran = %v, want %v", ran, c.ran)
			}
		})
	}
}

// TestDecompressFailuresCarryTheirCode: the runner and a driver classify a
// middleware failure by its code, so each refusal's code is part of the
// contract rather than an implementation detail.
func TestDecompressFailuresCarryTheirCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want mq.ErrorCode
	}{
		{"ErrNotFramed", compress.ErrNotFramed, mq.CodeCorruptMessage},
		{"ErrBadFlags", compress.ErrBadFlags, mq.CodeCorruptMessage},
		{"ErrSizeMismatch", compress.ErrSizeMismatch, mq.CodeCorruptMessage},
		{"ErrTooLarge", compress.ErrTooLarge, mq.CodeMessageTooLarge},
		{"ErrUnknownCodec", compress.ErrUnknownCodec, mq.CodeUnsupported},
		{"ErrInvalidConfig", compress.ErrInvalidConfig, mq.CodeInvalidConfig},
		{"ErrDuplicateCodec", compress.ErrDuplicateCodec, mq.CodeInvalidConfig},
		{"ErrReservedCodec", compress.ErrReservedCodec, mq.CodeInvalidConfig},
	} {
		if got := mq.CodeOf(c.err); got != c.want {
			t.Errorf("CodeOf(%s) = %v, want %v", c.name, got, c.want)
		}
		if !strings.Contains(c.err.Error(), "iqmq/x/compress") {
			t.Errorf("%s does not name its package: %q", c.name, c.err.Error())
		}
	}
}

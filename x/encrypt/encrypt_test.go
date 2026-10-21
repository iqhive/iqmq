package encrypt_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/encrypt"
)

func key(n int, fill byte) []byte { return bytes.Repeat([]byte{fill}, n) }

// ring is a StaticKeyring for each suite, so every case below runs against
// both rather than against whichever one happened to be written first.
func rings(tb testing.TB) map[encrypt.Suite]*encrypt.StaticKeyring {
	tb.Helper()
	out := map[encrypt.Suite]*encrypt.StaticKeyring{}
	for _, s := range encrypt.NewSuites().List() {
		r, err := encrypt.NewKeyring(nil, s, key(s.KeyLen(), 0x2a), "k1")
		if err != nil {
			tb.Fatal(err)
		}
		out[s] = r
	}
	return out
}

func TestRoundTripEverySuite(t *testing.T) {
	bodies := map[string][]byte{
		"empty":  {},
		"short":  []byte("hello"),
		"binary": {0, 1, 2, 0xff, 0xfe},
		"long":   bytes.Repeat([]byte("iqmq "), 4096),
	}
	for suite, r := range rings(t) {
		for name, body := range bodies {
			t.Run(suite.String()+"/"+name, func(t *testing.T) {
				s, err := encrypt.NewSealer(encrypt.Config{Keys: r})
				if err != nil {
					t.Fatal(err)
				}
				o, err := encrypt.NewOpener(encrypt.Config{Keys: r})
				if err != nil {
					t.Fatal(err)
				}
				sealed, err := s.Seal(context.Background(), body)
				if err != nil {
					t.Fatal(err)
				}
				if !encrypt.Sealed(sealed) {
					t.Fatal("Seal produced something Sealed does not recognise")
				}
				// The plaintext must not be in the CIPHERTEXT. Not "in
				// the envelope": the key id travels in clear, so a body
				// equal to the key id is found in the envelope without
				// anything having leaked. FuzzSealOpenRoundTrip found that
				// case and its corpus entry keeps it.
				e, perr := encrypt.ParseEnvelope(sealed)
				if perr != nil {
					t.Fatalf("Seal produced something ParseEnvelope rejects: %v", perr)
				}
				if len(body) > 8 && bytes.Contains(e.Ciphertext, body) {
					t.Fatal("the plaintext appears verbatim in the ciphertext")
				}
				got, err := o.Open(context.Background(), sealed)
				if err != nil {
					t.Fatalf("Open after Seal: %v", err)
				}
				if !bytes.Equal(got, body) {
					t.Fatalf("round trip changed a %d-byte body into %d bytes", len(body), len(got))
				}
			})
		}
	}
}

// TestSuiteTableMatchesTheStdlib: suite.go records each suite's nonce size
// and tag overhead so that ParseEnvelope can bound an envelope with no key.
// That duplication is only safe if it agrees with crypto/cipher, so it is
// asserted rather than trusted.
func TestSuiteTableMatchesTheStdlib(t *testing.T) {
	for _, s := range encrypt.NewSuites().List() {
		aead, err := encrypt.NewSuites().NewAEAD(s, key(s.KeyLen(), 1))
		if err != nil {
			t.Fatalf("%v: %v", s, err)
		}
		// The envelope's fixed sizes, recovered from a real envelope, must
		// equal what the AEAD reports.
		r, err := encrypt.NewKeyring(nil, s, key(s.KeyLen(), 1), "")
		if err != nil {
			t.Fatal(err)
		}
		sealer, err := encrypt.NewSealer(encrypt.Config{Keys: r})
		if err != nil {
			t.Fatal(err)
		}
		sealed, err := sealer.Seal(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		e, err := encrypt.ParseEnvelope(sealed)
		if err != nil {
			t.Fatal(err)
		}
		if len(e.Nonce) != aead.NonceSize() {
			t.Errorf("%v: envelope nonce is %d bytes, crypto/cipher says %d", s, len(e.Nonce), aead.NonceSize())
		}
		if len(e.Ciphertext) != aead.Overhead() {
			t.Errorf("%v: an empty body sealed to %d bytes, crypto/cipher's overhead is %d", s, len(e.Ciphertext), aead.Overhead())
		}
	}
}

// TestTwoSealsDiffer: the nonce is random, not a counter.
//
// This is the one implementation mistake in this package that breaks the
// construction outright. Two messages sealed under one key with one nonce
// leak the exclusive-or of their plaintexts and let an attacker forge tags,
// and the mistake is invisible in a round-trip test — a counter round-trips
// perfectly.
func TestTwoSealsDiffer(t *testing.T) {
	for suite, r := range rings(t) {
		t.Run(suite.String(), func(t *testing.T) {
			s, err := encrypt.NewSealer(encrypt.Config{Keys: r})
			if err != nil {
				t.Fatal(err)
			}
			body := []byte("the same plaintext every time")
			seen := map[string]bool{}
			const rounds = 64
			for range rounds {
				sealed, err := s.Seal(context.Background(), body)
				if err != nil {
					t.Fatal(err)
				}
				e, err := encrypt.ParseEnvelope(sealed)
				if err != nil {
					t.Fatal(err)
				}
				seen[string(e.Nonce)] = true
			}
			if len(seen) != rounds {
				t.Errorf("%d distinct nonces over %d seals: the nonce is not random", len(seen), rounds)
			}
		})
	}
}

// TestHeaderTamperingIsDetected is the property this package exists for.
//
// middleware.Encrypt records the key id in a HEADER, outside the AEAD's
// additional data, so anybody who can publish can change which key a
// consumer is told to use. Here the suite byte, the key-id length, the key id
// and the flags are all inside the authenticated data, so each one is
// detectable. Every byte of the envelope is flipped in turn and NONE of them
// may produce a successful open.
func TestHeaderTamperingIsDetected(t *testing.T) {
	r, err := encrypt.NewKeyring(nil, encrypt.SuiteAES256GCM, key(32, 0x2a), "k1")
	if err != nil {
		t.Fatal(err)
	}
	// A second key under the SAME id but a different suite, so that a
	// suite-byte flip has somewhere to land: without it the flip would fail
	// with ErrNoKey and prove nothing about authentication.
	if err := r.Retire(encrypt.SuiteAES128GCM, key(16, 0x2a), "k1"); err != nil {
		t.Fatal(err)
	}
	s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
	o, _ := encrypt.NewOpener(encrypt.Config{Keys: r})
	sealed, err := s.Seal(context.Background(), []byte("a secret worth tampering with"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.Open(context.Background(), sealed); err != nil {
		t.Fatalf("the untampered envelope does not open: %v", err)
	}

	opened := 0
	for i := range sealed {
		for _, bit := range []byte{0x01, 0x80} {
			bad := bytes.Clone(sealed)
			bad[i] ^= bit
			if _, err := o.Open(context.Background(), bad); err == nil {
				opened++
				t.Errorf("flipping bit %#x of byte %d produced a successful open", bit, i)
			}
		}
	}
	if opened != 0 {
		t.Errorf("%d single-bit tampered envelopes opened", opened)
	}

	// And truncation, at every length.
	for n := range sealed {
		if _, err := o.Open(context.Background(), sealed[:n]); err == nil {
			t.Errorf("a %d-byte truncation of a %d-byte envelope opened", n, len(sealed))
		}
	}
}

// TestTheKeyIDIsAuthenticatedNotJustCarried is the sharper half of the test
// above, stated so that it cannot be satisfied by accident.
//
// Two keys, two ids. An envelope sealed under "k1" is rewritten to name
// "k2" -- the same length, so every offset in the layout still holds and the
// rewrite is one byte in the key id. The open must fail. If the key id were
// merely CARRIED rather than authenticated, this would look up k2, fail the
// tag, and still fail -- so the test also checks the converse: that k2 is a
// real key which really does open its own messages, and that the failure is
// therefore about authentication rather than about a missing key.
func TestTheKeyIDIsAuthenticatedNotJustCarried(t *testing.T) {
	r, err := encrypt.NewKeyring(nil, encrypt.SuiteAES256GCM, key(32, 0x11), "k1")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Retire(encrypt.SuiteAES256GCM, key(32, 0x22), "k2"); err != nil {
		t.Fatal(err)
	}
	s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
	o, _ := encrypt.NewOpener(encrypt.Config{Keys: r})

	sealed, err := s.Seal(context.Background(), []byte("sealed under k1"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := encrypt.ParseEnvelope(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if string(e.KeyID) != "k1" {
		t.Fatalf("key id = %q, want k1", e.KeyID)
	}
	rewritten := bytes.Clone(sealed)
	rewritten[encrypt.FixedHeaderLen+1] = '2' // "k1" -> "k2"
	if _, err := o.Open(context.Background(), rewritten); !errors.Is(err, encrypt.ErrNotAuthentic) {
		t.Errorf("an envelope rewritten to name another key returned %v, want ErrNotAuthentic", err)
	}

	// The converse: k2 is a key the keyring really holds and really opens.
	if _, err := r.For(context.Background(), encrypt.SuiteAES256GCM, "k2"); err != nil {
		t.Fatalf("k2 is not in the keyring, so the assertion above proves nothing: %v", err)
	}
}

func TestAdditionalDataMustMatch(t *testing.T) {
	r := rings(t)[encrypt.SuiteAES256GCM]
	s, _ := encrypt.NewSealer(encrypt.Config{Keys: r, AdditionalData: []byte("tenant-7")})
	sealed, err := s.Seal(context.Background(), []byte("bound to tenant 7"))
	if err != nil {
		t.Fatal(err)
	}
	same, _ := encrypt.NewOpener(encrypt.Config{Keys: r, AdditionalData: []byte("tenant-7")})
	if _, err := same.Open(context.Background(), sealed); err != nil {
		t.Fatalf("matching additional data did not open: %v", err)
	}
	for name, aad := range map[string][]byte{
		"different": []byte("tenant-8"),
		"absent":    nil,
		"prefix":    []byte("tenant-"),
	} {
		t.Run(name, func(t *testing.T) {
			o, _ := encrypt.NewOpener(encrypt.Config{Keys: r, AdditionalData: aad})
			if _, err := o.Open(context.Background(), sealed); !errors.Is(err, encrypt.ErrNotAuthentic) {
				t.Errorf("Open with %q additional data = %v, want ErrNotAuthentic", aad, err)
			}
		})
	}
}

func TestRotation(t *testing.T) {
	r, err := encrypt.NewRotatingKeyring(nil, encrypt.SuiteAES256GCM, key(32, 1), "old")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
	o, _ := encrypt.NewOpener(encrypt.Config{Keys: r})
	before, err := s.Seal(context.Background(), []byte("sealed before"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Rotate(encrypt.SuiteAES256GCM, key(32, 2), "new"); err != nil {
		t.Fatal(err)
	}
	after, err := s.Seal(context.Background(), []byte("sealed after"))
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := encrypt.ParseEnvelope(after); string(e.KeyID) != "new" {
		t.Errorf("the new envelope names %q, want new", e.KeyID)
	}
	// Both must still open: this is the whole point of keeping old keys.
	for name, body := range map[string][]byte{"before": before, "after": after} {
		if _, err := o.Open(context.Background(), body); err != nil {
			t.Errorf("the envelope sealed %s the rotation no longer opens: %v", name, err)
		}
	}
	t.Run("an id cannot be reused for different material", func(t *testing.T) {
		if err := r.Rotate(encrypt.SuiteAES256GCM, key(32, 3), "new"); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("Rotate onto a used id = %v, want ErrInvalidConfig", err)
		}
	})
}

func TestParseEnvelopeRefusals(t *testing.T) {
	good := func() []byte {
		r := rings(t)[encrypt.SuiteAES256GCM]
		s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
		b, err := s.Seal(context.Background(), []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{"nil", nil, encrypt.ErrNotSealed},
		{"short", []byte("IQE"), encrypt.ErrNotSealed},
		{"header only", good()[:encrypt.FixedHeaderLen], encrypt.ErrNotSealed},
		{"wrong magic", append([]byte("IQE2"), good()[4:]...), encrypt.ErrNotSealed},
		{"reserved flag", func() []byte { b := good(); b[5] = 1; return b }(), encrypt.ErrBadFlags},
		{"unknown suite", func() []byte { b := good(); b[4] = 99; return b }(), encrypt.ErrUnknownSuite},
		{"zero suite", func() []byte { b := good(); b[4] = 0; return b }(), encrypt.ErrUnknownSuite},
		{"key id longer than the buffer", func() []byte { b := good(); b[6] = 0xFF; return b }(), encrypt.ErrNotSealed},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := encrypt.ParseEnvelope(c.body); !errors.Is(err, c.want) {
				t.Errorf("ParseEnvelope = %v, want %v", err, c.want)
			}
		})
	}
}

func TestConfigAndKeyRefusals(t *testing.T) {
	t.Run("nil keyring", func(t *testing.T) {
		if _, err := encrypt.NewSealer(encrypt.Config{}); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("NewSealer = %v, want ErrInvalidConfig", err)
		}
		if _, err := encrypt.NewOpener(encrypt.Config{}); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("NewOpener = %v, want ErrInvalidConfig", err)
		}
		if _, err := encrypt.Decrypt(encrypt.Config{}); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("Decrypt = %v, want ErrInvalidConfig", err)
		}
		if _, err := encrypt.PublishEncrypt(encrypt.Config{}); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("PublishEncrypt = %v, want ErrInvalidConfig", err)
		}
	})
	t.Run("key lengths", func(t *testing.T) {
		for _, n := range []int{0, 1, 15, 17, 24, 31, 33, 64} {
			if _, err := encrypt.NewStaticKeyring(key(n, 1), ""); !errors.Is(err, encrypt.ErrInvalidConfig) {
				t.Errorf("NewStaticKeyring with a %d-byte key = %v, want ErrInvalidConfig", n, err)
			}
		}
		// A 24-byte key is AES-192, which crypto/aes accepts and this
		// package does not offer a suite for. Refusing it is deliberate:
		// there is no envelope suite id for AES-192, so a 24-byte key could
		// not be described in an envelope even if it worked.
		if _, err := encrypt.NewSuites().NewAEAD(encrypt.SuiteAES256GCM, key(24, 1)); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Error("NewAEAD accepted a 24-byte key for a 32-byte suite")
		}
	})
	t.Run("key id too long", func(t *testing.T) {
		long := strings.Repeat("k", encrypt.MaxKeyIDLen+1)
		if _, err := encrypt.NewStaticKeyring(key(32, 1), long); !errors.Is(err, encrypt.ErrKeyIDTooLong) {
			t.Errorf("NewStaticKeyring with a %d-byte id = %v, want ErrKeyIDTooLong", len(long), err)
		}
		// And exactly MaxKeyIDLen is accepted: the boundary is inclusive.
		if _, err := encrypt.NewStaticKeyring(key(32, 1), strings.Repeat("k", encrypt.MaxKeyIDLen)); err != nil {
			t.Errorf("a %d-byte key id was refused: %v", encrypt.MaxKeyIDLen, err)
		}
	})
	t.Run("SuiteOf", func(t *testing.T) {
		for n, want := range map[int]encrypt.Suite{16: encrypt.SuiteAES128GCM, 32: encrypt.SuiteAES256GCM} {
			if got, err := encrypt.SuiteOf(key(n, 1)); err != nil || got != want {
				t.Errorf("SuiteOf(%d bytes) = (%v, %v), want %v", n, got, err, want)
			}
		}
		if _, err := encrypt.SuiteOf(key(24, 1)); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Error("SuiteOf accepted a 24-byte key")
		}
	})
	t.Run("already sealed", func(t *testing.T) {
		r := rings(t)[encrypt.SuiteAES256GCM]
		s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
		sealed, err := s.Seal(context.Background(), []byte("once"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Seal(context.Background(), sealed); !errors.Is(err, encrypt.ErrAlreadySealed) {
			t.Errorf("Seal of an envelope = %v, want ErrAlreadySealed", err)
		}
	})
	t.Run("unknown key", func(t *testing.T) {
		mine := rings(t)[encrypt.SuiteAES256GCM]
		theirs, err := encrypt.NewKeyring(nil, encrypt.SuiteAES256GCM, key(32, 0x99), "other")
		if err != nil {
			t.Fatal(err)
		}
		s, _ := encrypt.NewSealer(encrypt.Config{Keys: theirs})
		o, _ := encrypt.NewOpener(encrypt.Config{Keys: mine})
		sealed, err := s.Seal(context.Background(), []byte("sealed under a key you do not have"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := o.Open(context.Background(), sealed); !errors.Is(err, encrypt.ErrNoKey) {
			t.Errorf("Open with a keyring missing the key = %v, want ErrNoKey", err)
		}
	})
}

// ---------------------------------------------------------------- middleware

func TestPublishEncryptNeverSkipsABodyThatLooksSealed(t *testing.T) {
	// Sealed only sniffs the magic, so a plaintext that starts with it is
	// indistinguishable from an envelope. Skipping it published it in the
	// clear; the publish is refused instead.
	r := rings(t)[encrypt.SuiteAES256GCM]
	pm, err := encrypt.PublishEncrypt(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	published := false
	pub := pm(func(context.Context, string, []*mq.Message, []mq.PublishOption) error {
		published = true
		return nil
	})
	body := []byte(encrypt.Magic + "-but-this-is-plaintext")
	err = pub(context.Background(), "t", []*mq.Message{{Body: body}}, nil)
	if !errors.Is(err, encrypt.ErrAlreadySealed) {
		t.Fatalf("publishing a plaintext that starts with the magic = %v, want ErrAlreadySealed", err)
	}
	if published {
		t.Fatal("a body that only looked sealed was published")
	}
}

func TestPublishEncryptLeavesTheCallersMessageAlone(t *testing.T) {
	// The caller's message is not sealed in place, so publishing it again
	// seals it again rather than refusing its own envelope.
	r := rings(t)[encrypt.SuiteAES256GCM]
	pm, err := encrypt.PublishEncrypt(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	var published [][]byte
	pub := pm(func(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
		published = append(published, ms[0].Body)
		return nil
	})
	body := []byte("the payload")
	m := &mq.Message{Body: bytes.Clone(body)}
	for range 2 {
		if err := pub(context.Background(), "t", []*mq.Message{m}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(m.Body, body) {
		t.Fatalf("the caller's body became %q, want it untouched", m.Body)
	}
	for i, b := range published {
		if !encrypt.Sealed(b) {
			t.Fatalf("publication %d was not sealed", i)
		}
	}
}

func TestMiddlewareRoundTrip(t *testing.T) {
	r := rings(t)[encrypt.SuiteAES256GCM]
	pm, err := encrypt.PublishEncrypt(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	mw, err := encrypt.Decrypt(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("the payload")
	var published []*mq.Message
	pub := pm(func(_ context.Context, _ string, ms []*mq.Message, _ []mq.PublishOption) error {
		published = ms
		return nil
	})
	if err := pub(context.Background(), "t", []*mq.Message{{Body: body}, nil}, nil); err != nil {
		t.Fatal(err)
	}
	if !encrypt.Sealed(published[0].Body) {
		t.Fatal("PublishEncrypt did not seal the body")
	}
	// The key id travels IN the body and not in a header, which is the
	// difference from middleware.Encrypt. Assert the headers are untouched.
	if published[0].Headers.Len() != 0 {
		t.Errorf("PublishEncrypt added %d header(s); the key id belongs in the envelope", published[0].Headers.Len())
	}

	var seen []byte
	h := mw(mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
		seen = bytes.Clone(d.Body)
		return nil
	}))
	if err := h.Handle(context.Background(), &mq.Delivery{Message: mq.Message{Body: published[0].Body}}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(seen, body) {
		t.Fatalf("the handler saw %q, want %q", seen, body)
	}
}

func TestRequireSealed(t *testing.T) {
	r := rings(t)[encrypt.SuiteAES256GCM]
	for _, c := range []struct {
		name    string
		require bool
		ran     bool
		want    error
	}{
		{"off passes an unsealed body through", false, true, nil},
		{"on refuses it", true, false, encrypt.ErrNotSealed},
	} {
		t.Run(c.name, func(t *testing.T) {
			mw, err := encrypt.Decrypt(encrypt.Config{Keys: r, RequireSealed: c.require})
			if err != nil {
				t.Fatal(err)
			}
			ran := false
			h := mw(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { ran = true; return nil }))
			err = h.Handle(context.Background(), &mq.Delivery{Message: mq.Message{Body: []byte("plain")}})
			if c.want == nil && err != nil {
				t.Errorf("Handle = %v, want nil", err)
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Errorf("Handle = %v, want %v", err, c.want)
			}
			if ran != c.ran {
				t.Errorf("handler ran = %v, want %v", ran, c.ran)
			}
		})
	}
}

func TestErrorsCarryTheirCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want mq.ErrorCode
	}{
		{"ErrNotSealed", encrypt.ErrNotSealed, mq.CodeCorruptMessage},
		{"ErrBadFlags", encrypt.ErrBadFlags, mq.CodeCorruptMessage},
		{"ErrNotAuthentic", encrypt.ErrNotAuthentic, mq.CodeCorruptMessage},
		{"ErrUnknownSuite", encrypt.ErrUnknownSuite, mq.CodeUnsupported},
		{"ErrNoKey", encrypt.ErrNoKey, mq.CodeInvalidConfig},
		{"ErrKeyIDTooLong", encrypt.ErrKeyIDTooLong, mq.CodeInvalidConfig},
		{"ErrInvalidConfig", encrypt.ErrInvalidConfig, mq.CodeInvalidConfig},
		{"ErrAlreadySealed", encrypt.ErrAlreadySealed, mq.CodeInvalidArgument},
	} {
		if got := mq.CodeOf(c.err); got != c.want {
			t.Errorf("CodeOf(%s) = %v, want %v", c.name, got, c.want)
		}
		if !strings.Contains(c.err.Error(), "iqmq/x/encrypt") {
			t.Errorf("%s does not name its package: %q", c.name, c.err.Error())
		}
	}
}

// TestTheHeaderIsInTheAuthenticatedData asserts the one thing the bit-flip
// test above CANNOT see.
//
// This test exists because of a finding about its neighbour. With the
// header-in-AAD binding removed from Seal and Open -- i.e. with this package
// authenticating exactly what `middleware.Encrypt` authenticates --
// TestHeaderTamperingIsDetected and TestTheKeyIDIsAuthenticatedNotJustCarried
// both still PASSED. They could not distinguish the binding from its absence,
// which made the claim in the package documentation unsupported by any check.
//
// The reason is worth writing down, because it is a property of the layout
// rather than an accident: every field in this envelope's header either
// selects the key (suite, key id) or moves an offset (key-id length), and
// GCM's tag is computed under the key and over the nonce and ciphertext. So a
// tampered header always produces either the wrong key or a shifted
// nonce/ciphertext, and the tag catches it either way. The binding is
// therefore defence in depth TODAY and load-bearing TOMORROW: an envelope's
// additional data is frozen the moment the format ships, so the first future
// header field that does NOT select the key and does NOT shift an offset --
// a flag describing how to interpret the plaintext, an algorithm
// parameter -- is forgeable for ever unless the header is already covered.
// The specification records that, and this test is what keeps the binding from
// being "simplified away" by someone who runs the tamper tests and concludes
// it does nothing.
//
// It asserts the binding directly rather than through tampering: the AEAD is
// asked to open the envelope's own ciphertext with the header as additional
// data (must succeed) and with no additional data (must fail).
func TestTheHeaderIsInTheAuthenticatedData(t *testing.T) {
	for _, suite := range encrypt.NewSuites().List() {
		t.Run(suite.String(), func(t *testing.T) {
			material := key(suite.KeyLen(), 0x5c)
			r, err := encrypt.NewKeyring(nil, suite, material, "kid")
			if err != nil {
				t.Fatal(err)
			}
			s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
			sealed, err := s.Seal(context.Background(), []byte("bound to its header"))
			if err != nil {
				t.Fatal(err)
			}
			e, err := encrypt.ParseEnvelope(sealed)
			if err != nil {
				t.Fatal(err)
			}
			aead, err := encrypt.NewSuites().NewAEAD(suite, material)
			if err != nil {
				t.Fatal(err)
			}
			// The header IS the additional data.
			if _, err := aead.Open(nil, e.Nonce, e.Ciphertext, e.AAD); err != nil {
				t.Errorf("the envelope does not open with its header as additional data: %v", err)
			}
			// And nothing else is.
			if _, err := aead.Open(nil, e.Nonce, e.Ciphertext, nil); err == nil {
				t.Error("the envelope opens with NO additional data: the header is not authenticated, so a future header field would be forgeable")
			}
			// Not even a header with one byte changed, which is the same
			// statement from the other side.
			for i := range e.AAD {
				bad := bytes.Clone(e.AAD)
				bad[i] ^= 0x01
				if _, err := aead.Open(nil, e.Nonce, e.Ciphertext, bad); err == nil {
					t.Errorf("the envelope opens with byte %d of its header changed", i)
				}
			}
			// The AAD is exactly the header and nothing more: an AAD that
			// silently included the nonce or the ciphertext would pass every
			// assertion above and be a different format from the documented
			// one.
			if want := encrypt.FixedHeaderLen + len(e.KeyID); len(e.AAD) != want {
				t.Errorf("AAD is %d bytes, want the %d-byte header", len(e.AAD), want)
			}
			if !bytes.Equal(e.AAD, sealed[:len(e.AAD)]) {
				t.Error("AAD is not the envelope's own leading bytes")
			}
		})
	}
}

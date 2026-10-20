// Package encrypttest is the contract every
// [github.com/iqhive/iqmq/x/encrypt] suite is held to, as one set of cases
// each suite's module runs.
//
// It exists because the suites live in two modules. The AES-GCM suites are
// the standard library's and are tested where x/encrypt is; the
// ChaCha20-Poly1305 suite's primitive is in golang.org/x/crypto, which the
// root module may not import, so it is registered and tested from its own
// module. Two copies of the envelope, rotation and tamper tests would be how
// the suites came to be held to different standards, and the copy that
// drifted would be the one nobody ran. So the cases are written once, here,
// and every suite runs them.
//
// Every case is one sentence about the envelope, and the ones a broken AEAD
// can get wrong have been watched failing: x/encrypt's own
// TestTheSuiteContractCanFail runs [Run] in a child process against a suite
// registered with a deliberately broken AEAD, once per defect, and requires
// the named case to fail.
package encrypttest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/encrypt"
)

// Harness names the suite under test.
type Harness struct {
	// Suites builds every key the contract uses. Nil knows only the
	// AES-GCM suites; a suite another module supplies is registered in it
	// before Run.
	Suites *encrypt.Suites
	// Suite is the suite under test. It must be one [encrypt.Suites.List]
	// of Suites lists, i.e. a suite whose primitive Suites can construct.
	Suite encrypt.Suite
	// Other is a second suite, for the cases that need a key under the
	// same id in a different suite -- a flipped suite byte has to have
	// somewhere to land, or it fails with ErrNoKey and proves nothing
	// about authentication -- and for rotating across suites. Zero takes
	// the first suite [encrypt.Suites.List] of Suites lists that is not
	// Suite.
	Other encrypt.Suite
}

func (h Harness) other(tb testing.TB) encrypt.Suite {
	tb.Helper()
	if h.Other != encrypt.SuiteUnknown {
		return h.Other
	}
	for _, s := range h.Suites.List() {
		if s != h.Suite {
			return s
		}
	}
	tb.Fatalf("encrypttest: no suite other than %v is available", h.Suite)
	return encrypt.SuiteUnknown
}

// Key returns key material of the right length for s, filled with fill.
func Key(s encrypt.Suite, fill byte) []byte {
	return bytes.Repeat([]byte{fill}, s.KeyLen())
}

// Run runs every case against h.Suite.
func Run(t *testing.T, h Harness) {
	t.Helper()
	available := false
	for _, s := range h.Suites.List() {
		if s == h.Suite {
			available = true
		}
	}
	if !available {
		t.Fatalf("encrypttest: suite %v is not available in Harness.Suites (List() = %v); register its primitive before running the contract", h.Suite, h.Suites.List())
	}
	cases := []struct {
		name string
		fn   func(*testing.T, Harness)
	}{
		{"RoundTrip", testRoundTrip},
		{"SizesMatchThePrimitive", testSizesMatchThePrimitive},
		{"TwoSealsDiffer", testTwoSealsDiffer},
		{"TamperingFailsClosed", testTamperingFailsClosed},
		{"TamperingFailsClosedThroughTheMiddleware", testTamperingFailsClosedThroughTheMiddleware},
		{"KeyIDIsAuthenticated", testKeyIDIsAuthenticated},
		{"WrongKeyDoesNotOpen", testWrongKeyDoesNotOpen},
		{"AdditionalDataMustMatch", testAdditionalDataMustMatch},
		{"Rotation", testRotation},
		{"HeaderIsInTheAuthenticatedData", testHeaderIsInTheAuthenticatedData},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, h) })
	}
}

func pair(tb testing.TB, cfg encrypt.Config) (*encrypt.Sealer, *encrypt.Opener) {
	tb.Helper()
	s, err := encrypt.NewSealer(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	o, err := encrypt.NewOpener(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return s, o
}

func keyring(tb testing.TB, h Harness, s encrypt.Suite, fill byte, id string) *encrypt.StaticKeyring {
	tb.Helper()
	r, err := encrypt.NewKeyring(h.Suites, s, Key(s, fill), id)
	if err != nil {
		tb.Fatalf("NewKeyring(%v): %v", s, err)
	}
	return r
}

func seal(tb testing.TB, s *encrypt.Sealer, body string) []byte {
	tb.Helper()
	sealed, err := s.Seal(context.Background(), []byte(body))
	if err != nil {
		tb.Fatal(err)
	}
	return sealed
}

// testRoundTrip: every body survives sealing and opening, the envelope names
// the suite, and the plaintext is not in the ciphertext.
func testRoundTrip(t *testing.T, h Harness) {
	s, o := pair(t, encrypt.Config{Keys: keyring(t, h, h.Suite, 0x2a, "k1")})
	for name, body := range map[string][]byte{
		"empty":  {},
		"short":  []byte("hello"),
		"binary": {0, 1, 2, 0xff, 0xfe},
		"long":   bytes.Repeat([]byte("iqmq "), 4096),
	} {
		sealed, err := s.Seal(context.Background(), body)
		if err != nil {
			t.Fatalf("%s: Seal: %v", name, err)
		}
		e, err := encrypt.ParseEnvelope(sealed)
		if err != nil {
			t.Fatalf("%s: Seal produced something ParseEnvelope rejects: %v", name, err)
		}
		if e.Suite != h.Suite {
			t.Fatalf("%s: the envelope names %v, want %v", name, e.Suite, h.Suite)
		}
		if len(body) > 8 && bytes.Contains(e.Ciphertext, body) {
			t.Fatalf("%s: the plaintext appears verbatim in the ciphertext", name)
		}
		got, err := o.Open(context.Background(), sealed)
		if err != nil {
			t.Fatalf("%s: Open after Seal: %v", name, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("%s: round trip changed a %d-byte body into %d bytes", name, len(body), len(got))
		}
	}
}

// testSizesMatchThePrimitive: the suite table's nonce and tag sizes, which
// ParseEnvelope bounds an envelope with before any key is known, are what the
// primitive actually uses.
func testSizesMatchThePrimitive(t *testing.T, h Harness) {
	aead, err := h.Suites.NewAEAD(h.Suite, Key(h.Suite, 1))
	if err != nil {
		t.Fatal(err)
	}
	s, _ := pair(t, encrypt.Config{Keys: keyring(t, h, h.Suite, 1, "")})
	e, err := encrypt.ParseEnvelope(seal(t, s, ""))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Nonce) != aead.NonceSize() {
		t.Errorf("the envelope's nonce is %d bytes, the primitive's %d", len(e.Nonce), aead.NonceSize())
	}
	if len(e.Ciphertext) != aead.Overhead() {
		t.Errorf("an empty body sealed to %d bytes, the primitive's overhead is %d", len(e.Ciphertext), aead.Overhead())
	}
}

// testTwoSealsDiffer: two seals of one plaintext under one key differ in the
// nonce AND in the ciphertext. The nonce half is Seal's (crypto/rand); the
// ciphertext half is the primitive's -- an AEAD that ignored the nonce it was
// handed would round-trip perfectly and seal every message under one
// keystream.
func testTwoSealsDiffer(t *testing.T, h Harness) {
	s, _ := pair(t, encrypt.Config{Keys: keyring(t, h, h.Suite, 0x2a, "k1")})
	nonces := map[string]bool{}
	ciphertexts := map[string]bool{}
	const rounds = 64
	for range rounds {
		e, err := encrypt.ParseEnvelope(seal(t, s, "the same plaintext every time"))
		if err != nil {
			t.Fatal(err)
		}
		nonces[string(e.Nonce)] = true
		ciphertexts[string(e.Ciphertext)] = true
	}
	if len(nonces) != rounds {
		t.Errorf("%d distinct nonces over %d seals: the nonce is not random", len(nonces), rounds)
	}
	if len(ciphertexts) != rounds {
		t.Errorf("%d distinct ciphertexts over %d seals of one plaintext: the primitive is not using the nonce", len(ciphertexts), rounds)
	}
}

// testTamperingFailsClosed: no single-bit change anywhere in the envelope,
// and no truncation, opens -- and a failed open returns no plaintext.
func testTamperingFailsClosed(t *testing.T, h Harness) {
	r := keyring(t, h, h.Suite, 0x2a, "k1")
	// A key under the SAME id in another suite, so a flipped suite byte
	// has somewhere to land.
	other := h.other(t)
	if err := r.Retire(other, Key(other, 0x2a), "k1"); err != nil {
		t.Fatal(err)
	}
	s, o := pair(t, encrypt.Config{Keys: r})
	sealed := seal(t, s, "a secret worth tampering with")
	if _, err := o.Open(context.Background(), sealed); err != nil {
		t.Fatalf("the untampered envelope does not open: %v", err)
	}
	opened := 0
	for i := range sealed {
		for _, bit := range []byte{0x01, 0x80} {
			bad := bytes.Clone(sealed)
			bad[i] ^= bit
			plain, err := o.Open(context.Background(), bad)
			if err == nil {
				opened++
				if opened <= 3 {
					t.Errorf("flipping bit %#x of byte %d of %d produced a successful open", bit, i, len(sealed))
				}
				continue
			}
			if plain != nil {
				t.Errorf("flipping bit %#x of byte %d failed with %v but returned %d bytes", bit, i, err, len(plain))
			}
		}
	}
	if opened != 0 {
		t.Errorf("%d single-bit tampered envelopes opened", opened)
	}
	for n := range sealed {
		if _, err := o.Open(context.Background(), sealed[:n]); err == nil {
			t.Errorf("a %d-byte truncation of a %d-byte envelope opened", n, len(sealed))
		}
	}
}

// testTamperingFailsClosedThroughTheMiddleware: a tampered ciphertext reaching
// a consumer never reaches its handler, and the error is ErrNotAuthentic with
// CodeCorruptMessage, so the runner's failure policy (not a retry loop)
// decides what happens to it.
func testTamperingFailsClosedThroughTheMiddleware(t *testing.T, h Harness) {
	r := keyring(t, h, h.Suite, 0x2a, "k1")
	s, _ := pair(t, encrypt.Config{Keys: r})
	mw, err := encrypt.Decrypt(encrypt.Config{Keys: r, RequireSealed: true})
	if err != nil {
		t.Fatal(err)
	}
	ran := false
	handler := mw(mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
		ran = true
		return nil
	}))
	sealed := seal(t, s, "a secret worth tampering with")
	e, err := encrypt.ParseEnvelope(sealed)
	if err != nil {
		t.Fatal(err)
	}
	// Every byte of the ciphertext and tag, which is the region a header
	// check cannot see.
	start := len(e.AAD) + len(e.Nonce)
	for i := start; i < len(sealed); i++ {
		bad := bytes.Clone(sealed)
		bad[i] ^= 0x01
		ran = false
		herr := handler.Handle(context.Background(), &mq.Delivery{Message: mq.Message{Body: bad}})
		if ran {
			t.Fatalf("a delivery whose byte %d was flipped reached the handler", i)
		}
		if !errors.Is(herr, encrypt.ErrNotAuthentic) || mq.CodeOf(herr) != mq.CodeCorruptMessage {
			t.Fatalf("a delivery whose byte %d was flipped = %v (code %v), want ErrNotAuthentic and CodeCorruptMessage", i, herr, mq.CodeOf(herr))
		}
	}
}

// testKeyIDIsAuthenticated: an envelope rewritten to name another key the
// keyring really holds does not open under it.
func testKeyIDIsAuthenticated(t *testing.T, h Harness) {
	r := keyring(t, h, h.Suite, 0x11, "k1")
	if err := r.Retire(h.Suite, Key(h.Suite, 0x22), "k2"); err != nil {
		t.Fatal(err)
	}
	s, o := pair(t, encrypt.Config{Keys: r})
	sealed := seal(t, s, "sealed under k1")
	rewritten := bytes.Clone(sealed)
	rewritten[encrypt.FixedHeaderLen+1] = '2' // "k1" -> "k2", one byte, every offset unchanged
	if _, err := o.Open(context.Background(), rewritten); !errors.Is(err, encrypt.ErrNotAuthentic) {
		t.Errorf("an envelope rewritten to name another key = %v, want ErrNotAuthentic", err)
	}
	// The converse, so the failure above is about authentication and not
	// about a missing key: k2 is real and opens its own envelopes.
	s2, _ := pair(t, encrypt.Config{Keys: keyring(t, h, h.Suite, 0x22, "k2")})
	if _, err := o.Open(context.Background(), seal(t, s2, "sealed under k2")); err != nil {
		t.Fatalf("k2 does not open its own envelope, so the assertion above proves nothing: %v", err)
	}
}

// testWrongKeyDoesNotOpen: the same suite and id with different key material
// -- what a consumer given the wrong secret has -- is ErrNotAuthentic.
func testWrongKeyDoesNotOpen(t *testing.T, h Harness) {
	s, _ := pair(t, encrypt.Config{Keys: keyring(t, h, h.Suite, 0x01, "k1")})
	_, o := pair(t, encrypt.Config{Keys: keyring(t, h, h.Suite, 0x02, "k1")})
	if _, err := o.Open(context.Background(), seal(t, s, "for the right key only")); !errors.Is(err, encrypt.ErrNotAuthentic) {
		t.Errorf("Open under the wrong key material = %v, want ErrNotAuthentic", err)
	}
}

// testAdditionalDataMustMatch: Config.AdditionalData is authenticated, and a
// reader supplying anything else -- including nothing, or a prefix -- cannot
// open.
func testAdditionalDataMustMatch(t *testing.T, h Harness) {
	r := keyring(t, h, h.Suite, 0x2a, "k1")
	s, same := pair(t, encrypt.Config{Keys: r, AdditionalData: []byte("tenant-7")})
	sealed := seal(t, s, "bound to tenant 7")
	if _, err := same.Open(context.Background(), sealed); err != nil {
		t.Fatalf("matching additional data did not open: %v", err)
	}
	for name, aad := range map[string][]byte{
		"different": []byte("tenant-8"),
		"absent":    nil,
		"prefix":    []byte("tenant-"),
	} {
		_, o := pair(t, encrypt.Config{Keys: r, AdditionalData: aad})
		if _, err := o.Open(context.Background(), sealed); !errors.Is(err, encrypt.ErrNotAuthentic) {
			t.Errorf("%s: Open with %q additional data = %v, want ErrNotAuthentic", name, aad, err)
		}
	}
}

// testRotation: a RotatingKeyring moves ONTO this suite from another and on
// again within it, every envelope sealed along the way still opens, and an id
// cannot be reused for different material.
func testRotation(t *testing.T, h Harness) {
	other := h.other(t)
	r, err := encrypt.NewRotatingKeyring(h.Suites, other, Key(other, 1), "old")
	if err != nil {
		t.Fatal(err)
	}
	s, o := pair(t, encrypt.Config{Keys: r})
	sealed := map[string][]byte{"before": seal(t, s, "sealed before")}
	if err := r.Rotate(h.Suite, Key(h.Suite, 2), "new"); err != nil {
		t.Fatalf("Rotate onto %v: %v", h.Suite, err)
	}
	sealed["after"] = seal(t, s, "sealed after")
	if err := r.Rotate(h.Suite, Key(h.Suite, 3), "newer"); err != nil {
		t.Fatalf("Rotate within %v: %v", h.Suite, err)
	}
	sealed["after again"] = seal(t, s, "sealed after again")
	e, err := encrypt.ParseEnvelope(sealed["after again"])
	if err != nil {
		t.Fatal(err)
	}
	if e.Suite != h.Suite || string(e.KeyID) != "newer" {
		t.Errorf("the newest envelope names (%v, %q), want (%v, newer)", e.Suite, e.KeyID, h.Suite)
	}
	for name, env := range sealed {
		if _, err := o.Open(context.Background(), env); err != nil {
			t.Errorf("the envelope sealed %s the rotation no longer opens: %v", name, err)
		}
	}
	if err := r.Rotate(h.Suite, Key(h.Suite, 4), "newer"); !errors.Is(err, encrypt.ErrInvalidConfig) {
		t.Errorf("Rotate onto a used id = %v, want ErrInvalidConfig", err)
	}
	// A static keyring finishes a rotation the same way: the new key seals,
	// the retired one still opens.
	static := keyring(t, h, h.Suite, 5, "current")
	if err := static.Retire(other, Key(other, 1), "old"); err != nil {
		t.Fatal(err)
	}
	_, so := pair(t, encrypt.Config{Keys: static})
	if _, err := so.Open(context.Background(), sealed["before"]); err != nil {
		t.Errorf("a retired key does not open what it sealed: %v", err)
	}
}

// testHeaderIsInTheAuthenticatedData asserts the binding directly: the primitive opens the envelope's ciphertext with the header as
// additional data, and with no additional data or any altered header it does
// not.
func testHeaderIsInTheAuthenticatedData(t *testing.T, h Harness) {
	material := Key(h.Suite, 0x5c)
	r, err := encrypt.NewKeyring(h.Suites, h.Suite, material, "kid")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := pair(t, encrypt.Config{Keys: r})
	e, err := encrypt.ParseEnvelope(seal(t, s, "bound to its header"))
	if err != nil {
		t.Fatal(err)
	}
	aead, err := h.Suites.NewAEAD(h.Suite, material)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aead.Open(nil, e.Nonce, e.Ciphertext, e.AAD); err != nil {
		t.Errorf("the envelope does not open with its header as additional data: %v", err)
	}
	if _, err := aead.Open(nil, e.Nonce, e.Ciphertext, nil); err == nil {
		t.Error("the envelope opens with NO additional data: the header is not authenticated, so a future header field would be forgeable")
	}
	for i := range e.AAD {
		bad := bytes.Clone(e.AAD)
		bad[i] ^= 0x01
		if _, err := aead.Open(nil, e.Nonce, e.Ciphertext, bad); err == nil {
			t.Errorf("the envelope opens with byte %d of its header changed", i)
		}
	}
	if want := encrypt.FixedHeaderLen + len(e.KeyID); len(e.AAD) != want {
		t.Errorf("AAD is %d bytes, want the %d-byte header", len(e.AAD), want)
	}
}

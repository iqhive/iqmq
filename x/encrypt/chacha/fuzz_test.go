package chacha_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"bitbucket.org/iqhive/iqmq/x/encrypt"
)

const knownPlaintext = "the only body that may open"

// FuzzOpen: no input opens under the ChaCha20-Poly1305 key, ever, except the
// one body sealed with it. The fuzzer never sees the key, so any other
// successful open is a forgery or a sign the tag is not being checked -- the
// fail-closed property, stated over arbitrary input rather than over bit
// flips of one envelope.
//
// An AES-256 key sits under the same id, so a mutated suite byte lands on a
// real key rather than on ErrNoKey.
func FuzzOpen(f *testing.F) {
	register(f)
	r, err := encrypt.NewKeyring(encrypt.SuiteChaCha20Poly1305, bytes.Repeat([]byte{0x5a}, 32), "kid")
	if err != nil {
		f.Fatal(err)
	}
	if err := r.Retire(encrypt.SuiteAES256GCM, bytes.Repeat([]byte{0x5a}, 32), "kid"); err != nil {
		f.Fatal(err)
	}
	s, err := encrypt.NewSealer(encrypt.Config{Keys: r})
	if err != nil {
		f.Fatal(err)
	}
	o, err := encrypt.NewOpener(encrypt.Config{Keys: r})
	if err != nil {
		f.Fatal(err)
	}
	ctx := context.Background()
	known, err := s.Seal(ctx, []byte(knownPlaintext))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(known)
	f.Add([]byte(nil))
	f.Add(bytes.Clone(known)[:len(known)-1])
	f.Add(append(bytes.Clone(known), 0))
	f.Add(append([]byte("IQE1\x03\x00\x03kid"), make([]byte, 12+16)...))

	f.Fuzz(func(t *testing.T, body []byte) {
		plain, err := o.Open(ctx, body)
		if err != nil {
			if plain != nil {
				t.Fatalf("Open failed with %v but returned %d bytes", err, len(plain))
			}
			return
		}
		// The seed is re-sealed in every worker with a fresh nonce, so the
		// invariant is over the plaintext, which is constant.
		if string(plain) != knownPlaintext {
			t.Fatalf("Open of a %d-byte body produced %q, which was never sealed with this key", len(body), plain)
		}
	})
}

// FuzzSealOpenRoundTrip: every body survives sealing and opening under every
// suite this process has -- the two AES-GCM suites and ChaCha20-Poly1305 --
// and every key id.
func FuzzSealOpenRoundTrip(f *testing.F) {
	register(f)
	f.Add([]byte(nil), uint8(2), "")
	f.Add([]byte("hello"), uint8(2), "kid")
	f.Add(bytes.Repeat([]byte("x"), 4096), uint8(0), "a-longer-key-identifier")
	f.Add([]byte(encrypt.Magic), uint8(1), "k")

	f.Fuzz(func(t *testing.T, body []byte, suiteSel uint8, keyID string) {
		if len(keyID) > encrypt.MaxKeyIDLen {
			return
		}
		suites := encrypt.Suites()
		suite := suites[int(suiteSel)%len(suites)]
		r, err := encrypt.NewKeyring(suite, bytes.Repeat([]byte{0x33}, suite.KeyLen()), keyID)
		if err != nil {
			t.Fatal(err)
		}
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
			if encrypt.Sealed(body) && errors.Is(err, encrypt.ErrAlreadySealed) {
				return
			}
			t.Fatalf("Seal(%d bytes) under %v = %v", len(body), suite, err)
		}
		e, err := encrypt.ParseEnvelope(sealed)
		if err != nil {
			t.Fatalf("Seal produced something ParseEnvelope rejects: %v", err)
		}
		if e.Suite != suite || string(e.KeyID) != keyID {
			t.Fatalf("envelope names (%v, %q), want (%v, %q)", e.Suite, e.KeyID, suite, keyID)
		}
		got, err := o.Open(context.Background(), sealed)
		if err != nil {
			t.Fatalf("Open after Seal of %d bytes under %v: %v", len(body), suite, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("round trip under %v changed a %d-byte body into %d bytes", suite, len(body), len(got))
		}
		if len(body) > 8 && bytes.Contains(e.Ciphertext, body) {
			t.Fatal("the plaintext appears verbatim in the ciphertext")
		}
	})
}

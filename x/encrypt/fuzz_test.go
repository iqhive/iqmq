package encrypt_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/iqhive/iqmq/x/encrypt"
)

// knownPlaintext is the only body FuzzOpen's keyring ever seals. See the
// comment in FuzzOpen for why the invariant is stated over the plaintext.
const knownPlaintext = "the only body that may open"

// FuzzParseEnvelope: the envelope parser must never panic, and the slices it
// returns must partition the input exactly.
//
// The partition is the property that matters. ParseEnvelope returns four
// aliasing slices decided by one attacker-controlled length byte, and the
// whole class of bug it can have is an offset that makes two of them overlap
// or run past the end -- which would hand the AEAD a nonce built partly from
// the key id, or a ciphertext that includes the nonce. Checking the lengths
// would not catch that; checking that AAD||nonce||ciphertext reconstructs the
// input byte for byte does.
func FuzzParseEnvelope(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("IQE1"))
	f.Add([]byte("IQE1\x02\x00\x00"))
	f.Add([]byte("IQE1\x02\x00\xff"))
	f.Add(append([]byte("IQE1\x02\x00\x03kid"), make([]byte, 12+16)...))
	f.Add(append([]byte("IQE1\x01\x00\x00"), make([]byte, 12+16)...))
	f.Add(append([]byte("IQE1\x02\x01\x00"), make([]byte, 12+16)...))
	if r, err := encrypt.NewKeyring(encrypt.SuiteAES256GCM, bytes.Repeat([]byte{3}, 32), "seed"); err == nil {
		if s, err := encrypt.NewSealer(encrypt.Config{Keys: r}); err == nil {
			if b, err := s.Seal(context.Background(), []byte("a real envelope")); err == nil {
				f.Add(b)
			}
		}
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		e, err := encrypt.ParseEnvelope(body)
		if err != nil {
			for _, want := range []error{encrypt.ErrNotSealed, encrypt.ErrBadFlags, encrypt.ErrUnknownSuite} {
				if errors.Is(err, want) {
					return
				}
			}
			t.Fatalf("ParseEnvelope returned %v, which is none of the three documented refusals", err)
		}
		if e.Flags != 0 {
			t.Fatalf("ParseEnvelope accepted flags %#x", e.Flags)
		}
		if e.Suite.KeyLen() == 0 {
			t.Fatalf("ParseEnvelope accepted suite %d, which has no definition", e.Suite)
		}
		// The four regions partition the input exactly and in order.
		rebuilt := make([]byte, 0, len(body))
		rebuilt = append(rebuilt, e.AAD...)
		rebuilt = append(rebuilt, e.Nonce...)
		rebuilt = append(rebuilt, e.Ciphertext...)
		if !bytes.Equal(rebuilt, body) {
			t.Fatalf("AAD(%d)+nonce(%d)+ciphertext(%d) = %d bytes, want the input's %d: the regions do not partition the envelope",
				len(e.AAD), len(e.Nonce), len(e.Ciphertext), len(rebuilt), len(body))
		}
		// The key id sits inside the AAD, after the fixed header.
		if !bytes.Equal(e.KeyID, e.AAD[encrypt.FixedHeaderLen:]) {
			t.Fatalf("KeyID %q is not the AAD's tail %q", e.KeyID, e.AAD[encrypt.FixedHeaderLen:])
		}
		// The nonce and the tag are the suite's sizes, never shorter.
		if len(e.Nonce) != 12 {
			t.Fatalf("nonce is %d bytes, want 12", len(e.Nonce))
		}
		if len(e.Ciphertext) < 16 {
			t.Fatalf("ciphertext is %d bytes, which cannot hold a 16-byte tag", len(e.Ciphertext))
		}
	})
}

// FuzzOpen: no input opens, ever, except one this package sealed.
//
// The keyring is fixed and the fuzzer never sees the key, so a successful
// open is either a forgery or a bug. It is the strongest statement available
// about an AEAD and it costs one comparison.
func FuzzOpen(f *testing.F) {
	r, err := encrypt.NewKeyring(encrypt.SuiteAES256GCM, bytes.Repeat([]byte{0x5a}, 32), "kid")
	if err != nil {
		f.Fatal(err)
	}
	if err := r.Retire(encrypt.SuiteAES128GCM, bytes.Repeat([]byte{0x5b}, 16), "kid"); err != nil {
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
	// The seed corpus is re-created in every fuzz WORKER process, and Seal
	// draws a fresh random nonce each time, so the envelope bytes differ
	// between the coordinator that serialised the seed and the worker that
	// runs it. The invariant is therefore stated over the PLAINTEXT, which
	// is a constant, and not over the ciphertext -- comparing the ciphertext
	// failed on seed#0 with "Open succeeded on a 65-byte body that is not
	// the sealed one", which was the test comparing against a value it had
	// itself regenerated.
	known, err := s.Seal(ctx, []byte(knownPlaintext))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(known)
	f.Add([]byte(nil))
	f.Add(bytes.Clone(known)[:len(known)-1])
	f.Add(append(bytes.Clone(known), 0))
	f.Add(append([]byte("IQE1\x02\x00\x03kid"), make([]byte, 12+16)...))

	f.Fuzz(func(t *testing.T, body []byte) {
		plain, err := o.Open(ctx, body)
		if err != nil {
			if plain != nil {
				t.Fatalf("Open failed with %v but returned %d bytes", err, len(plain))
			}
			return
		}
		// The fuzzer never sees the key, and the only body ever sealed
		// with it carried knownPlaintext, so any successful open must
		// produce that -- or be a forgery, which for AES-GCM is a 2^-128
		// event and would be the finding of the decade.
		if string(plain) != knownPlaintext {
			t.Fatalf("Open of a %d-byte body produced %q, which was never sealed with this key: a forgery, or the AEAD is not being consulted", len(body), plain)
		}
	})
}

// FuzzSealOpenRoundTrip: every body survives sealing and opening, under every
// suite and every key id, and the sealed form never contains the plaintext.
func FuzzSealOpenRoundTrip(f *testing.F) {
	f.Add([]byte(nil), uint8(0), "")
	f.Add([]byte("hello"), uint8(1), "kid")
	f.Add(bytes.Repeat([]byte("x"), 4096), uint8(0), "a-longer-key-identifier")
	f.Add([]byte(encrypt.Magic), uint8(1), "k")

	f.Fuzz(func(t *testing.T, body []byte, suiteSel uint8, keyID string) {
		if len(keyID) > encrypt.MaxKeyIDLen {
			return // refused at construction, and TestConfigAndKeyRefusals covers it
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
				return // documented refusal
			}
			t.Fatalf("Seal(%d bytes) = %v", len(body), err)
		}
		e, err := encrypt.ParseEnvelope(sealed)
		if err != nil {
			t.Fatalf("Seal produced something ParseEnvelope rejects: %v", err)
		}
		if string(e.KeyID) != keyID {
			t.Fatalf("envelope names key %q, want %q", e.KeyID, keyID)
		}
		if e.Suite != suite {
			t.Fatalf("envelope names suite %v, want %v", e.Suite, suite)
		}
		got, err := o.Open(context.Background(), sealed)
		if err != nil {
			t.Fatalf("Open after Seal of %d bytes under %v: %v", len(body), suite, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("round trip changed a %d-byte body into %d bytes", len(body), len(got))
		}
		// The plaintext must not appear in the CIPHERTEXT. Not "in the
		// envelope": the key id travels in clear by design, so a body that
		// happens to equal the key id is found in the envelope and is not a
		// leak. The fuzzer produced exactly that -- body and keyID both
		// "A\xbe\xf7)\x94\xe7-H5" -- and the committed corpus entry keeps
		// the case. It is a good demonstration of the package
		// documentation's "what this does NOT protect": the key id is not
		// secret.
		if len(body) > 8 && bytes.Contains(e.Ciphertext, body) {
			t.Fatal("the plaintext appears verbatim in the ciphertext")
		}
	})
}

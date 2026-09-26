package chacha_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/x/encrypt"
	"github.com/iqhive/iqmq/x/encrypt/chacha"
	"github.com/iqhive/iqmq/x/encrypt/encrypttest"
)

func register(tb testing.TB) {
	tb.Helper()
	if err := chacha.Register(); err != nil {
		tb.Fatalf("Register: %v", err)
	}
}

// TestContract holds ChaCha20-Poly1305 to exactly the cases the two AES-GCM
// suites are held to in x/encrypt's own TestContract -- the same function,
// not a copy of it  -- once against each AES suite as the
// "other" suite a flipped suite byte lands on and a rotation starts from.
func TestContract(t *testing.T) {
	register(t)
	for _, other := range []encrypt.Suite{encrypt.SuiteAES128GCM, encrypt.SuiteAES256GCM} {
		t.Run("other="+other.String(), func(t *testing.T) {
			encrypttest.Run(t, encrypttest.Harness{Suite: encrypt.SuiteChaCha20Poly1305, Other: other})
		})
	}
}

// TestTheAESSuitesStillPassWithChaChaRegistered: registering a third suite
// changes what Suites() lists, and the AES suites' contract must not notice.
// With ChaCha20-Poly1305 registered, the AES suites' "other" suite may now be
// it, so this is also rotation OFF ChaCha20-Poly1305 and back onto AES.
func TestTheAESSuitesStillPassWithChaChaRegistered(t *testing.T) {
	register(t)
	for _, s := range []encrypt.Suite{encrypt.SuiteAES128GCM, encrypt.SuiteAES256GCM} {
		t.Run(s.String(), func(t *testing.T) {
			encrypttest.Run(t, encrypttest.Harness{Suite: s, Other: encrypt.SuiteChaCha20Poly1305})
		})
	}
}

// TestKnownAnswer pins the suite to RFC 8439 §2.8.2's AEAD test vector, so
// that envelope suite byte 3 means the IETF construction and not merely a
// ChaCha-shaped one. The expected ciphertext and tag are the RFC's, copied
// from the document rather than produced by the library under test.
func TestKnownAnswer(t *testing.T) {
	key, _ := hex.DecodeString("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f")
	nonce, _ := hex.DecodeString("070000004041424344454647")
	aad, _ := hex.DecodeString("50515253c0c1c2c3c4c5c6c7")
	plain := []byte("Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.")
	want, _ := hex.DecodeString(strings.Join([]string{
		"d31a8d34648e60db7b86afbc53ef7ec2",
		"a4aded51296e08fea9e2b5a736ee62d6",
		"3dbea45e8ca9671282fafb69da92728b",
		"1a71de0a9e060b2905d6a5b67ecd3b36",
		"92ddbd7f2d778b8c9803aee328091b58",
		"fab324e4fad675945585808b4831d7bc",
		"3ff4def08e4b7a9de576d26586cec64b",
		"6116",
		"1ae10b594f09e26a7e902ecbd0600691", // the tag
	}, ""))
	aead, err := chacha.NewAEAD(key)
	if err != nil {
		t.Fatal(err)
	}
	if got := aead.Seal(nil, nonce, plain, aad); !bytes.Equal(got, want) {
		t.Fatalf("Seal of RFC 8439 §2.8.2's input =\n%x\nwant\n%x", got, want)
	}
	got, err := aead.Open(nil, nonce, want, aad)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("Open of RFC 8439 §2.8.2's output = (%q, %v)", got, err)
	}
	// And through the registry, so the AEAD x/encrypt builds for the suite is
	// this construction and not some other one registered under its id.
	register(t)
	viaRegistry, err := encrypt.NewAEAD(encrypt.SuiteChaCha20Poly1305, key)
	if err != nil {
		t.Fatal(err)
	}
	if got := viaRegistry.Seal(nil, nonce, plain, aad); !bytes.Equal(got, want) {
		t.Fatal("the AEAD encrypt.NewAEAD builds for SuiteChaCha20Poly1305 is not RFC 8439's construction")
	}
}

// TestRegisterIsIdempotentAndListed: Register may be called from several
// places at start-up, and after it the suite is one Suites() lists.
func TestRegisterIsIdempotentAndListed(t *testing.T) {
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = chacha.Register()
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Register #%d = %v", i, err)
		}
	}
	listed := false
	for _, s := range encrypt.Suites() {
		listed = listed || s == encrypt.SuiteChaCha20Poly1305
	}
	if !listed {
		t.Fatalf("Suites() = %v after Register, want it to list ChaCha20-Poly1305", encrypt.Suites())
	}
	// A second primitive under the same id is refused: which construction a
	// suite byte means must not depend on which module registered first.
	if err := encrypt.RegisterSuite(encrypt.SuiteChaCha20Poly1305, chacha.NewAEAD); !errors.Is(err, encrypt.ErrInvalidConfig) {
		t.Errorf("a second RegisterSuite = %v, want ErrInvalidConfig", err)
	}
}

// TestKeyLengths: the suite takes exactly a 32-byte key, and SuiteOf never
// selects it -- a 32-byte key is AES-256 unless the caller names this suite.
func TestKeyLengths(t *testing.T) {
	register(t)
	for _, n := range []int{0, 16, 24, 31, 33, 64} {
		if _, err := chacha.NewAEAD(make([]byte, n)); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("NewAEAD with a %d-byte key = %v, want ErrInvalidConfig", n, err)
		}
		if _, err := encrypt.NewKeyring(encrypt.SuiteChaCha20Poly1305, make([]byte, n), "k"); !errors.Is(err, encrypt.ErrInvalidConfig) {
			t.Errorf("NewKeyring with a %d-byte key = %v, want ErrInvalidConfig", n, err)
		}
	}
	if s, err := encrypt.SuiteOf(make([]byte, 32)); err != nil || s != encrypt.SuiteAES256GCM {
		t.Errorf("SuiteOf(32 bytes) = (%v, %v), want AES-256-GCM even with ChaCha20-Poly1305 registered", s, err)
	}
	if encrypt.SuiteChaCha20Poly1305.KeyLen() != chacha.KeySize {
		t.Errorf("the suite table says %d bytes, the primitive %d", encrypt.SuiteChaCha20Poly1305.KeyLen(), chacha.KeySize)
	}
}

// TestAnEnvelopeSealedHereOpensThroughTheMiddleware is the deployment shape:
// PublishEncrypt on one side, Decrypt on the other, the suite named in the
// envelope and nowhere else.
func TestAnEnvelopeSealedHereOpensThroughTheMiddleware(t *testing.T) {
	register(t)
	r, err := encrypt.NewKeyring(encrypt.SuiteChaCha20Poly1305, bytes.Repeat([]byte{9}, 32), "2026-09")
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
	sealed, err := s.Seal(context.Background(), []byte("for an ARM consumer"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := encrypt.ParseEnvelope(sealed)
	if err != nil || e.Suite != encrypt.SuiteChaCha20Poly1305 {
		t.Fatalf("envelope = (%v, %v)", e.Suite, err)
	}
	if sealed[4] != 3 {
		t.Fatalf("the suite byte is %d, want 3: the wire id is part of the format", sealed[4])
	}
	got, err := o.Open(context.Background(), sealed)
	if err != nil || string(got) != "for an ARM consumer" {
		t.Fatalf("Open = (%q, %v)", got, err)
	}
}

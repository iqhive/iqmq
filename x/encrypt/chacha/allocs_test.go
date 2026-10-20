package chacha_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/iqhive/iqmq/x/encrypt"
)

// TestAllocsSealAndOpen pins the two per-message paths under this suite to the
// same budget the AES suites have: one allocation each, for the buffer the
// caller keeps -- the envelope on Seal, the plaintext on Open. A second would
// mean the primitive, or x/encrypt's capacity arithmetic for its sizes, is
// regrowing a buffer.
//
// Everything is built outside the measured closures; building the keyring
// inside is how the specification once reported 840 allocs/op for a path that allocates none.
func TestAllocsSealAndOpen(t *testing.T) {
	r, err := encrypt.NewKeyring(register(t), encrypt.SuiteChaCha20Poly1305, bytes.Repeat([]byte{7}, 32), "kid")
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
	ctx := context.Background()
	body := []byte("a body of some particular length")
	sealed, err := s.Seal(ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	var out []byte
	var serr error
	seal := testing.AllocsPerRun(200, func() { out, serr = s.Seal(ctx, body) })
	open := testing.AllocsPerRun(200, func() { out, serr = o.Open(ctx, sealed) })
	if serr != nil || !bytes.Equal(out, body) {
		t.Fatalf("the last Open = (%q, %v)", out, serr)
	}
	if seal > 1 {
		t.Errorf("Seal allocs/op = %v, want at most 1 (the envelope)", seal)
	}
	if open > 1 {
		t.Errorf("Open allocs/op = %v, want at most 1 (the plaintext)", open)
	}
	t.Logf("ChaCha20-Poly1305: Seal allocs/op = %v, Open allocs/op = %v", seal, open)
}

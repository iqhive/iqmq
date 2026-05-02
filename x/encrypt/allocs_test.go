package encrypt_test

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"bitbucket.org/iqhive/iqmq/x/encrypt"
)

// TestAllocsParseEnvelope: the envelope parse is on every sealed delivery's
// path and allocates nothing.
//
// Everything the measured closure needs is built BEFORE it -- the envelope,
// and the variables the results land in. Building the keyring or sealing
// inside the closure is the mistake the specification made when it reported 840 allocs/op
// for a path that allocates none.
func TestAllocsParseEnvelope(t *testing.T) {
	r, err := encrypt.NewKeyring(encrypt.SuiteAES256GCM, key(32, 7), "kid")
	if err != nil {
		t.Fatal(err)
	}
	s, err := encrypt.NewSealer(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := s.Seal(context.Background(), []byte("a body to parse round"))
	if err != nil {
		t.Fatal(err)
	}
	var e encrypt.Envelope
	var perr error
	if got := testing.AllocsPerRun(200, func() {
		e, perr = encrypt.ParseEnvelope(sealed)
	}); got != 0 {
		t.Errorf("ParseEnvelope allocs/op = %v, want 0", got)
	}
	// Read the results so nothing above is dead code the compiler may delete
	// along with the allocation this test is about.
	if perr != nil || e.Suite != encrypt.SuiteAES256GCM || string(e.KeyID) != "kid" || len(e.Nonce) != 12 {
		t.Fatalf("ParseEnvelope = (%+v, %v)", e, perr)
	}
}

// TestAllocsSealed: the sniff runs on every delivery, sealed or not.
func TestAllocsSealed(t *testing.T) {
	sealed := append([]byte(encrypt.Magic), make([]byte, 64)...)
	plain := []byte("nothing to see here")
	var a, b bool
	if got := testing.AllocsPerRun(200, func() {
		a = encrypt.Sealed(sealed)
		b = encrypt.Sealed(plain)
	}); got != 0 {
		t.Errorf("Sealed allocs/op = %v, want 0", got)
	}
	if !a || b {
		t.Fatalf("Sealed(sealed) = %v, Sealed(plain) = %v", a, b)
	}
}

// TestAllocsSeal pins the publish path's cost at one allocation: the envelope
// itself.
//
// One and not zero, and the reason is the whole shape of the API: Seal
// returns a buffer the caller keeps, sized for the header, the nonce, the
// ciphertext and the tag, and written once. A second allocation would mean
// the capacity arithmetic in Seal is wrong and the append is regrowing --
// which is exactly the bug this number would catch, since it is invisible to
// every correctness test in the package.
func TestAllocsSeal(t *testing.T) {
	r, err := encrypt.NewKeyring(encrypt.SuiteAES256GCM, key(32, 7), "kid")
	if err != nil {
		t.Fatal(err)
	}
	s, err := encrypt.NewSealer(encrypt.Config{Keys: r})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	body := []byte("a body of some particular length")
	var out []byte
	var serr error
	got := testing.AllocsPerRun(200, func() { out, serr = s.Seal(ctx, body) })
	if serr != nil {
		t.Fatal(serr)
	}
	if got > 1 {
		t.Errorf("Seal allocs/op = %v, want at most 1 (the envelope): the capacity arithmetic is regrowing the buffer", got)
	}
	if len(out) != encrypt.FixedHeaderLen+3+12+len(body)+16 {
		t.Errorf("the envelope is %d bytes, which is not header+keyid+nonce+body+tag", len(out))
	}
	t.Logf("Seal allocs/op = %v for a %d-byte body", got, len(body))
}

// TestAllocsOpen pins the consume path's cost at one allocation: the
// plaintext, which the caller keeps.
//
// It was two, and the second was not the cipher: the envelope's key id is a
// []byte aliasing the delivery and Keyring.For takes a string, so
// string(e.KeyID) escaped through the interface call and allocated on every
// message. With an empty key id the same path measured one. Opener now keeps
// the last id it converted and reuses it when the next envelope names the same
// key, which is every message between two rotations.
func TestAllocsOpen(t *testing.T) {
	for _, suite := range encrypt.Suites() {
		t.Run(suite.String(), func(t *testing.T) {
			r, err := encrypt.NewKeyring(suite, key(suite.KeyLen(), 7), "a-key-id-of-ordinary-length")
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
			var oerr error
			got := testing.AllocsPerRun(200, func() { out, oerr = o.Open(ctx, sealed) })
			if oerr != nil || !bytes.Equal(out, body) {
				t.Fatalf("Open = (%q, %v)", out, oerr)
			}
			if got > 1 {
				t.Errorf("Open allocs/op = %v, want at most 1 (the plaintext)", got)
			}
		})
	}
}

// TestTheKeyIDCacheIsNotState: Opener's remembered key id is a cache, so
// eight goroutines opening envelopes that alternate between two keys -- the
// shape of the minutes after a rotation -- must each open under the key their
// envelope names, whatever the other goroutines left in the cache. -race
// checks the cache's own synchronisation.
func TestTheKeyIDCacheIsNotState(t *testing.T) {
	r, err := encrypt.NewRotatingKeyring(encrypt.SuiteAES256GCM, key(32, 1), "old")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := encrypt.NewSealer(encrypt.Config{Keys: r})
	o, _ := encrypt.NewOpener(encrypt.Config{Keys: r})
	ctx := context.Background()
	old, err := s.Seal(ctx, []byte("sealed under old"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Rotate(encrypt.SuiteAES256GCM, key(32, 2), "new"); err != nil {
		t.Fatal(err)
	}
	cur, err := s.Seal(ctx, []byte("sealed under new"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				env, want := old, "sealed under old"
				if (i+g)%2 == 0 {
					env, want = cur, "sealed under new"
				}
				got, err := o.Open(ctx, env)
				if err != nil || string(got) != want {
					t.Errorf("Open = (%q, %v), want %q", got, err, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}

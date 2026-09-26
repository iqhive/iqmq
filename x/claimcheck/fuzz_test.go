package claimcheck_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/x/claimcheck"
)

// FuzzParseRef: the reference parser must never panic, and the three
// variable-length regions must partition the bytes after the header exactly.
//
// The partition is the property that matters. ParseRef slices Store, Key and
// Sum out of one buffer using two attacker-controlled uint16 lengths, and the
// bug it can have is an offset that makes two of them overlap or run past the
// end -- which would hand the store a key built partly out of the digest, or
// verify a payload against bytes from the key. Lengths alone would not catch
// that; reconstructing the input does.
func FuzzParseRef(f *testing.F) {
	ref := func(store, key string, size uint64, d claimcheck.Digest, sum []byte) []byte {
		b, err := claimcheck.AppendRef(nil, store, key, size, d, sum)
		if err != nil {
			f.Fatal(err)
		}
		return b
	}
	sum := sha256.Sum256([]byte("payload"))
	f.Add([]byte(nil))
	f.Add([]byte("IQK1"))
	f.Add([]byte("IQK1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	f.Add(ref("s", "k", 0, claimcheck.DigestNone, nil))
	f.Add(ref("bucket-1", "obj-1", 1<<40, claimcheck.DigestSHA256, sum[:]))
	f.Add(append(ref("s", "k", 1, claimcheck.DigestNone, nil), 'x'))
	f.Add(func() []byte { b := ref("s", "k", 1, claimcheck.DigestNone, nil); b[4] = 1; return b }())

	f.Fuzz(func(t *testing.T, body []byte) {
		r, err := claimcheck.ParseRef(body)
		if err != nil {
			for _, want := range []error{claimcheck.ErrNotRef, claimcheck.ErrBadFlags, claimcheck.ErrUnknownDigest} {
				if errors.Is(err, want) {
					return
				}
			}
			t.Fatalf("ParseRef returned %v, which is none of the three documented refusals", err)
		}
		if r.Flags != 0 {
			t.Fatalf("ParseRef accepted flags %#x", r.Flags)
		}
		if len(r.Sum) != r.Digest.Len() {
			t.Fatalf("digest %v carries %d bytes, want %d", r.Digest, len(r.Sum), r.Digest.Len())
		}
		// The three regions partition the bytes after the header, in order
		// and with nothing left over.
		rebuilt := make([]byte, 0, len(body))
		rebuilt = append(rebuilt, body[:claimcheck.HeaderLen]...)
		rebuilt = append(rebuilt, r.Store...)
		rebuilt = append(rebuilt, r.Key...)
		rebuilt = append(rebuilt, r.Sum...)
		if !bytes.Equal(rebuilt, body) {
			t.Fatalf("header+store(%d)+key(%d)+sum(%d) = %d bytes, want the input's %d: the regions do not partition the reference",
				len(r.Store), len(r.Key), len(r.Sum), len(rebuilt), len(body))
		}
		// A parsed reference re-encodes to itself, so AppendRef and ParseRef
		// cannot drift apart.
		again, aerr := claimcheck.AppendRef(nil, string(r.Store), string(r.Key), r.Size, r.Digest, r.Sum)
		if aerr != nil {
			// The only legitimate refusal here is an empty store or key,
			// which ParseRef accepts (a zero length is a well-formed
			// field) and AppendRef does not (it is never a useful
			// reference to write). Anything else is a real disagreement.
			if len(r.Store) == 0 || len(r.Key) == 0 {
				return
			}
			t.Fatalf("AppendRef refused a reference ParseRef accepted: %v", aerr)
		}
		if !bytes.Equal(again, body) {
			t.Fatalf("AppendRef(ParseRef(b)) = %x, want %x", again, body)
		}
	})
}

// FuzzCheckOut: an arbitrary body handed to the consume half never panics,
// never produces a payload that fails the reference's own checks, and never
// exceeds the limit.
func FuzzCheckOut(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("not a reference at all"))
	f.Add([]byte("IQK1"))
	if b, err := claimcheck.AppendRef(nil, "bucket-1", "obj-1", 8, claimcheck.DigestNone, nil); err == nil {
		f.Add(b)
	}
	if b, err := claimcheck.AppendRef(nil, "bucket-1", "obj-1", 1<<40, claimcheck.DigestNone, nil); err == nil {
		f.Add(b)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		// A fresh store per input, holding one known object under the key
		// the seeds use, so that a fuzzer-built reference can actually hit
		// something rather than always failing at ErrPayloadGone.
		s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{Name: "bucket-1"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		const known = "the known payload"
		s.Overwrite("obj-1", []byte(known))

		cfg := claimcheck.WithoutDigest(claimcheck.Config{Store: s, Threshold: 16, MaxPayload: 1 << 16})
		mw, err := claimcheck.CheckOut(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var seen []byte
		ran := false
		h := mw(handlerFunc(func(b []byte) { ran, seen = true, bytes.Clone(b) }))
		herr := h.Handle(context.Background(), delivery(body))
		if herr != nil {
			if ran {
				t.Fatal("the handler ran AND the middleware failed")
			}
			return
		}
		if !ran {
			t.Fatal("the middleware succeeded without running the handler")
		}
		if int64(len(seen)) > 1<<16 {
			t.Fatalf("the handler saw %d bytes against a 65536-byte limit", len(seen))
		}
		if !claimcheck.IsRef(body) {
			// A non-reference is passed through untouched.
			if !bytes.Equal(seen, body) {
				t.Fatalf("a non-reference body reached the handler changed: %d bytes in, %d out", len(body), len(seen))
			}
			return
		}
		// A reference that checked out must agree with what it declared.
		r, perr := claimcheck.ParseRef(body)
		if perr != nil {
			t.Fatalf("CheckOut succeeded on a body ParseRef rejects with %v", perr)
		}
		if uint64(len(seen)) != r.Size {
			t.Fatalf("the handler saw %d bytes from a reference declaring %d", len(seen), r.Size)
		}
		if string(seen) != known {
			t.Fatalf("the handler saw %q, which is not the only payload in the store", seen)
		}
	})
}

// FuzzCheckInCheckOutRoundTrip: every body over the threshold survives being
// parked and fetched, and the reference it becomes is always parseable.
func FuzzCheckInCheckOutRoundTrip(f *testing.F) {
	f.Add([]byte(nil), true)
	f.Add([]byte("under the threshold"), true)
	f.Add(bytes.Repeat([]byte("x"), 4096), true)
	f.Add(bytes.Repeat([]byte("x"), 4096), false)
	f.Add([]byte(claimcheck.Magic), true)

	f.Fuzz(func(t *testing.T, body []byte, digest bool) {
		s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{Name: "bucket-1"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		cfg := claimcheck.Config{Store: s, Threshold: 16, MaxPayload: 1 << 20}
		if !digest {
			cfg = claimcheck.WithoutDigest(cfg)
		}
		pm, err := claimcheck.PublishCheckIn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		mw, err := claimcheck.CheckOut(cfg)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := publishRaw(pm, body)
		if err != nil {
			t.Fatalf("PublishCheckIn(%d bytes) = %v", len(body), err)
		}
		if len(body) < 16 || claimcheck.IsRef(body) {
			if !bytes.Equal(wire, body) {
				t.Fatal("a body under the threshold, or already a reference, was checked in")
			}
			return
		}
		if !claimcheck.IsRef(wire) {
			t.Fatalf("a %d-byte body was not checked in", len(body))
		}
		var seen []byte
		ran := false
		h := mw(handlerFunc(func(b []byte) { ran, seen = true, bytes.Clone(b) }))
		if err := h.Handle(context.Background(), delivery(wire)); err != nil {
			t.Fatalf("CheckOut after CheckIn of %d bytes: %v", len(body), err)
		}
		if !ran {
			t.Fatal("the handler did not run")
		}
		if !bytes.Equal(seen, body) {
			t.Fatalf("round trip changed a %d-byte body into %d bytes", len(body), len(seen))
		}
	})
}

// FuzzFileStoreGet: a FileStore handed an arbitrary key -- which is what a
// forged or corrupted reference hands it -- never panics, never reads outside
// its directory, and answers with exactly one of: the one payload it holds
// (for exactly that key), ErrPayloadGone, or ErrTooLarge. Anything else,
// ErrStoreUnavailable included, would be a key that reached the filesystem in
// a shape the store did not expect.
func FuzzFileStoreGet(f *testing.F) {
	dir := f.TempDir()
	if err := os.WriteFile(filepath.Join(filepath.Dir(dir), "outside"), []byte("outside the store"), 0o600); err != nil {
		f.Fatal(err)
	}
	s, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "files-1", Dir: dir, SweepEvery: -1})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = s.Close() })
	const known = "the only payload in the store"
	key, err := s.Put(context.Background(), []byte(known))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(key)
	f.Add("")
	f.Add("../outside")
	f.Add("/etc/passwd")
	f.Add(strings.ToUpper(key))
	f.Add(key + "/")
	f.Add(".tmp-" + key[:27])
	f.Add(strings.Repeat(".", 32))
	f.Add(strings.Repeat("0", 32))

	f.Fuzz(func(t *testing.T, k string) {
		got, err := s.Get(context.Background(), k, 1<<16)
		switch {
		case err == nil:
			if k != key || string(got) != known {
				t.Fatalf("Get(%q) = %q, but the store holds only %q under %q", k, got, known, key)
			}
		case errors.Is(err, claimcheck.ErrPayloadGone), errors.Is(err, claimcheck.ErrTooLarge):
			if got != nil {
				t.Fatalf("Get(%q) failed with %v and returned %d bytes", k, err, len(got))
			}
			if k == key {
				t.Fatalf("Get of the key the store holds = %v", err)
			}
		default:
			t.Fatalf("Get(%q) = %v, which is none of the answers a key can get", k, err)
		}
	})
}

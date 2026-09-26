package claimcheck_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/iqhive/iqmq/x/claimcheck"
)

// TestAllocsParseRef: the reference parse is on every checked delivery's path
// and allocates nothing.
//
// The reference is built BEFORE the measured closure, and the results are
// assigned to variables read afterwards. Building it inside is the mistake
// The specification made when it reported 840 allocs/op for a path that allocates none.
func TestAllocsParseRef(t *testing.T) {
	sum := sha256.Sum256([]byte("payload"))
	body, err := claimcheck.AppendRef(nil, "bucket-1", "obj-1", 7, claimcheck.DigestSHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	var r claimcheck.Ref
	var perr error
	if got := testing.AllocsPerRun(200, func() {
		r, perr = claimcheck.ParseRef(body)
	}); got != 0 {
		t.Errorf("ParseRef allocs/op = %v, want 0", got)
	}
	if perr != nil || string(r.Store) != "bucket-1" || string(r.Key) != "obj-1" || r.Size != 7 || len(r.Sum) != sha256.Size {
		t.Fatalf("ParseRef = (%+v, %v)", r, perr)
	}
}

// TestAllocsIsRef: the sniff runs on every delivery, checked or not, so it is
// the one function in this package that a consumer pays for unconditionally.
func TestAllocsIsRef(t *testing.T) {
	ref, err := claimcheck.AppendRef(nil, "s", "k", 1, claimcheck.DigestNone, nil)
	if err != nil {
		t.Fatal(err)
	}
	plain := []byte("an ordinary message body")
	var a, b bool
	if got := testing.AllocsPerRun(200, func() {
		a = claimcheck.IsRef(ref)
		b = claimcheck.IsRef(plain)
	}); got != 0 {
		t.Errorf("IsRef allocs/op = %v, want 0", got)
	}
	if !a || b {
		t.Fatalf("IsRef(ref) = %v, IsRef(plain) = %v", a, b)
	}
}

// TestAllocsAppendRef: the publish path's reference write reuses the caller's
// buffer when it has room.
func TestAllocsAppendRef(t *testing.T) {
	buf := make([]byte, 0, 128)
	sum := sha256.Sum256([]byte("payload"))
	var out []byte
	var err error
	if got := testing.AllocsPerRun(200, func() {
		out, err = claimcheck.AppendRef(buf[:0], "bucket-1", "obj-1", 7, claimcheck.DigestSHA256, sum[:])
	}); got != 0 {
		t.Errorf("AppendRef allocs/op = %v, want 0", got)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, perr := claimcheck.ParseRef(out); perr != nil {
		t.Fatalf("AppendRef wrote something ParseRef rejects: %v", perr)
	}
}

// TestAllocsFileStoreRefusesAForgedKey: a reference whose key the file store
// could not have issued is refused before any I/O and without allocating, so
// a flood of forged references costs a consumer nothing but the parse.
func TestAllocsFileStoreRefusesAForgedKey(t *testing.T) {
	s, err := claimcheck.NewFileStore(claimcheck.FileOptions{Name: "files-1", Dir: t.TempDir(), SweepEvery: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	var gerr error
	if got := testing.AllocsPerRun(200, func() {
		_, gerr = s.Get(ctx, "../../etc/passwd", 1<<20)
	}); got != 0 {
		t.Errorf("Get of a forged key allocs/op = %v, want 0", got)
	}
	if !errors.Is(gerr, claimcheck.ErrPayloadGone) {
		t.Fatalf("Get of a forged key = %v, want ErrPayloadGone", gerr)
	}
}

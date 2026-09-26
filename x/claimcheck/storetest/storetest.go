// Package storetest is the contract every [claimcheck.Store] is held to, as
// one set of cases each store runs.
//
// It exists because a store is the one part of the claim-check pattern a
// caller is expected to write, and the one mistake it must not make is easy:
// reporting "could not ask" as "no such object", or the reverse. The first
// dead-letters good messages whenever a bucket is briefly unreachable; the
// second retries a lane for ever on a payload that will never come back. The
// package's own [claimcheck.MemoryStore] and [claimcheck.FileStore] run these
// cases, the S3-compatible store in x/claimcheck/s3store runs them against an
// in-process S3 and against MinIO, and a store written anywhere else can run
// them the same way.
//
// Every case is one sentence about the contract, and each has been watched
// failing: x/claimcheck's TestTheStoreContractCanFail runs [Run] in a child
// process against a store carrying one defect at a time and requires the
// named case to fail.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iqhive/iqmq/x/claimcheck"
)

// Harness is what one store implementation hands the contract.
type Harness struct {
	// New builds a store for one case. It is called once per case, and the
	// harness closes the store itself (tb.Cleanup) if the case did not.
	New func(tb testing.TB) claimcheck.Store
	// Remove makes the object under key disappear the way the store's
	// lifecycle policy would -- a TTL passing, a bucket rule, an operator --
	// without going through the Store interface, which has no Delete.
	// Required.
	Remove func(tb testing.TB, s claimcheck.Store, key string)
	// Break makes the store unreachable, and returns the function that
	// restores it. Nil, or a Break returning nil, skips the case that needs
	// it -- for a real server that the test cannot take down -- and the
	// skip is reported rather than silent.
	Break func(tb testing.TB, s claimcheck.Store) (restore func())
}

// Run runs every case against h.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil || h.Remove == nil {
		t.Fatal("storetest: a Harness needs New and Remove")
	}
	cases := []struct {
		name string
		fn   func(*testing.T, Harness)
	}{
		{"RoundTrip", testRoundTrip},
		{"PutCopiesThePayload", testPutCopiesThePayload},
		{"KeysAreDistinct", testKeysAreDistinct},
		{"GoneIsPermanent", testGoneIsPermanent},
		{"UnreachableIsRetryable", testUnreachableIsRetryable},
		{"TheLimitIsInclusiveAndEnforced", testTheLimitIsInclusiveAndEnforced},
		{"AForgedKeyIsGone", testAForgedKeyIsGone},
		{"ClosedIsClosed", testClosedIsClosed},
		{"TheNameIsStable", testTheNameIsStable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, h) })
	}
}

func newStore(t *testing.T, h Harness) claimcheck.Store {
	t.Helper()
	s := h.New(t)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func put(t *testing.T, s claimcheck.Store, payload []byte) string {
	t.Helper()
	key, err := s.Put(context.Background(), payload)
	if err != nil {
		t.Fatalf("Put(%d bytes): %v", len(payload), err)
	}
	if key == "" {
		t.Fatal("Put returned an empty key; no reference can be written for it")
	}
	return key
}

// testRoundTrip: a payload comes back byte for byte, including an empty one.
func testRoundTrip(t *testing.T, h Harness) {
	s := newStore(t, h)
	for _, payload := range [][]byte{
		bytes.Repeat([]byte("payload "), 8192),
		{0, 1, 2, 0xff},
		{},
	} {
		key := put(t, s, payload)
		got, err := s.Get(context.Background(), key, 1<<20)
		if err != nil {
			t.Fatalf("Get of a %d-byte payload: %v", len(payload), err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("Get returned %d bytes for a %d-byte payload", len(got), len(payload))
		}
	}
}

// testPutCopiesThePayload: the caller's slice is a message body a publisher
// may reuse, so a store that aliased it would hand a later consumer
// whatever the buffer holds by then; and a Get's result belongs to the
// caller, so writing to it must not change what the next Get returns.
func testPutCopiesThePayload(t *testing.T, h Harness) {
	s := newStore(t, h)
	body := []byte("the original bytes")
	key := put(t, s, body)
	copy(body, "XXXXXXXXXXXXXXXXXX")
	got, err := s.Get(context.Background(), key, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "the original bytes" {
		t.Fatalf("Get = %q after the caller reused its buffer: Put kept the caller's slice", got)
	}
	got[0] = 'Y'
	again, err := s.Get(context.Background(), key, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != "the original bytes" {
		t.Fatalf("Get = %q after a previous Get's result was written to", again)
	}
}

// testKeysAreDistinct: two Puts -- of the same bytes, too -- get two keys, so
// the lifecycle of one publication never decides another's.
func testKeysAreDistinct(t *testing.T, h Harness) {
	s := newStore(t, h)
	seen := map[string]bool{}
	for range 16 {
		key := put(t, s, []byte("the same payload"))
		if seen[key] {
			t.Fatalf("key %q was issued twice", key)
		}
		seen[key] = true
	}
}

// testGoneIsPermanent: an object the store no longer holds is ErrPayloadGone
// -- CodePoisonMessage, permanent -- and is NOT ErrStoreUnavailable.
func testGoneIsPermanent(t *testing.T, h Harness) {
	s := newStore(t, h)
	key := put(t, s, []byte("soon to be gone"))
	h.Remove(t, s, key)
	got, err := s.Get(context.Background(), key, 1<<20)
	if !errors.Is(err, claimcheck.ErrPayloadGone) {
		t.Fatalf("Get of a removed object = (%q, %v), want ErrPayloadGone", got, err)
	}
	if errors.Is(err, claimcheck.ErrStoreUnavailable) {
		t.Fatal("a removed object also reads as ErrStoreUnavailable: the consumer would retry it for ever")
	}
	if got != nil {
		t.Fatalf("Get of a removed object returned %d bytes with its error", len(got))
	}
	// A key that was never issued is gone too.
	if _, err := s.Get(context.Background(), key+"0", 1<<20); err == nil {
		t.Fatal("Get of a key the store never issued succeeded")
	}
}

// testUnreachableIsRetryable: a store that cannot be asked reports
// ErrStoreUnavailable -- CodeUnavailable, retried -- for Get and for Put, is
// NOT ErrPayloadGone, and answers normally again once it is back.
func testUnreachableIsRetryable(t *testing.T, h Harness) {
	s := newStore(t, h)
	key := put(t, s, []byte("still there"))
	if h.Break == nil {
		t.Skip("this harness cannot make its store unreachable; its classification of an outage is not checked here")
	}
	restore := h.Break(t, s)
	if restore == nil {
		t.Skip("this harness cannot make its store unreachable; its classification of an outage is not checked here")
	}
	_, gerr := s.Get(context.Background(), key, 1<<20)
	_, perr := s.Put(context.Background(), []byte("during the outage"))
	restore()
	for op, err := range map[string]error{"Get": gerr, "Put": perr} {
		if !errors.Is(err, claimcheck.ErrStoreUnavailable) {
			t.Errorf("%s during an outage = %v, want ErrStoreUnavailable", op, err)
		}
		if errors.Is(err, claimcheck.ErrPayloadGone) {
			t.Errorf("%s during an outage also reads as ErrPayloadGone: a good message would be dead-lettered for a transient failure", op)
		}
	}
	if got, err := s.Get(context.Background(), key, 1<<20); err != nil || string(got) != "still there" {
		t.Fatalf("Get after the outage = (%q, %v): the retry the error asked for does not succeed", got, err)
	}
}

// testTheLimitIsInclusiveAndEnforced: a payload over max is ErrTooLarge and
// one exactly at it is returned -- the limit is the cap on a body that
// arrives from a store.
func testTheLimitIsInclusiveAndEnforced(t *testing.T, h Harness) {
	s := newStore(t, h)
	key := put(t, s, bytes.Repeat([]byte("z"), 1000))
	if got, err := s.Get(context.Background(), key, 999); !errors.Is(err, claimcheck.ErrTooLarge) || got != nil {
		t.Errorf("Get with max 999 of a 1000-byte payload = (%d bytes, %v), want ErrTooLarge", len(got), err)
	}
	if got, err := s.Get(context.Background(), key, 1000); err != nil || len(got) != 1000 {
		t.Errorf("Get with max 1000 of a 1000-byte payload = (%d bytes, %v)", len(got), err)
	}
}

// testAForgedKeyIsGone: a reference is remote input, so its key is. A key the
// store could not have issued -- a path, a traversal, a neighbour of a real
// key -- never returns a payload, and never reads as an outage.
func testAForgedKeyIsGone(t *testing.T, h Harness) {
	s := newStore(t, h)
	key := put(t, s, []byte("the real payload"))
	for _, forged := range []string{
		"../" + key, "/etc/passwd", "../../etc/passwd", key + "/", key + "/..",
		strings.ToUpper(key), " " + key, key[:len(key)-1], "..", ".",
	} {
		if forged == key {
			continue
		}
		got, err := s.Get(context.Background(), forged, 1<<20)
		if err == nil {
			t.Errorf("Get(%q) returned %q: a forged key reached a payload", forged, got)
			continue
		}
		if errors.Is(err, claimcheck.ErrStoreUnavailable) {
			t.Errorf("Get(%q) = %v: a forged key reads as an outage, so its message would be retried for ever", forged, err)
		}
	}
}

// testClosedIsClosed: Close is idempotent, and a closed store refuses Put and
// Get with ErrClosed rather than reaching the backend.
func testClosedIsClosed(t *testing.T, h Harness) {
	s := h.New(t)
	key := put(t, s, []byte("x"))
	for i := range 3 {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d = %v", i+1, err)
		}
	}
	if _, err := s.Put(context.Background(), []byte("x")); !errors.Is(err, claimcheck.ErrClosed) {
		t.Errorf("Put after Close = %v, want ErrClosed", err)
	}
	if _, err := s.Get(context.Background(), key, 1<<20); !errors.Is(err, claimcheck.ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
}

// testTheNameIsStable: the name is what every reference records and every
// consumer matches, so it is non-empty, fits a reference, and does not change
// over the store's life.
func testTheNameIsStable(t *testing.T, h Harness) {
	s := newStore(t, h)
	name := s.Name()
	if name == "" || len(name) > claimcheck.MaxStoreLen {
		t.Fatalf("Name() = %q (%d bytes)", name, len(name))
	}
	_ = put(t, s, []byte("x"))
	if s.Name() != name {
		t.Fatalf("Name() changed from %q to %q", name, s.Name())
	}
}

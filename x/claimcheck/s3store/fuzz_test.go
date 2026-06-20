package s3store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"bitbucket.org/iqhive/iqmq/x/claimcheck"
)

// FuzzGetKey: the key a reference names is remote input. For any key, Get
// makes a request to the bucket only when the key is exactly the prefix and
// 32 lower-case hex characters -- the shape the store issues -- and answers
// with nothing but the one payload it holds (for exactly its key),
// ErrPayloadGone or ErrTooLarge. A request for any other shape would let a
// forged reference read another object in the bucket into a handler.
func FuzzGetKey(f *testing.F) {
	fake := newFakeS3(f)
	s := newStore(f, fake.client())
	const known = "the only payload in the bucket"
	key, err := s.Put(context.Background(), []byte(known))
	if err != nil {
		f.Fatal(err)
	}
	fake.objectsForTest("config/secrets.json", []byte("somebody else's object"))
	f.Add(key)
	f.Add("config/secrets.json")
	f.Add(prefix + "../config/secrets.json")
	f.Add(prefix + strings.Repeat("0", 32))
	f.Add(strings.ToUpper(key))
	f.Add("")

	f.Fuzz(func(t *testing.T, k string) {
		shaped := strings.HasPrefix(k, prefix) && len(k) == len(prefix)+32 &&
			strings.Trim(k[len(prefix):], "0123456789abcdef") == ""
		before := fake.requests.Load()
		got, err := s.Get(context.Background(), k, 1<<16)
		requested := fake.requests.Load() != before
		if requested && !shaped {
			t.Fatalf("Get(%q) made a request for a key the store could not have issued", k)
		}
		switch {
		case err == nil:
			if k != key || string(got) != known {
				t.Fatalf("Get(%q) = %q, but the store wrote only %q under %q", k, got, known, key)
			}
		case errors.Is(err, claimcheck.ErrPayloadGone), errors.Is(err, claimcheck.ErrTooLarge):
			if got != nil {
				t.Fatalf("Get(%q) failed with %v and returned %d bytes", k, err, len(got))
			}
		default:
			t.Fatalf("Get(%q) = %v, which is none of the answers a key can get", k, err)
		}
	})
}

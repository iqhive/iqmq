package claimcheck_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/iqhive/iqmq/x/claimcheck"
)

// TestPutRacesClose is the playbook's recurring defect class applied to this
// package: a payload placed in a store while the store is closing.
//
// The engine's version of this bug consumed a reservation for ever because a
// record landed after the drain. Here the equivalent is a Put that lands
// after Close has dropped the object map, which would leave a reference in
// the broker pointing at a payload nothing holds -- a message that can never
// be handled, and the exact failure the publish half fails closed to avoid.
//
// The property is not "Put never fails" but "Put either stores the payload or
// reports ErrClosed, and never reports success for a payload the store does
// not hold". Thousands of rounds, because the known instances of this class
// appeared at five to twenty failures per two or three thousand rounds and a
// hundred rounds proves nothing.
func TestPutRacesClose(t *testing.T) {
	const rounds = 3000
	ctx := context.Background()
	for i := range rounds {
		s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{MaxEntries: 8})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		type result struct {
			key string
			err error
		}
		results := make([]result, 4)
		wg.Add(len(results))
		for j := range results {
			go func(j int) {
				defer wg.Done()
				key, err := s.Put(ctx, []byte("payload"))
				results[j] = result{key, err}
			}(j)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Close()
		}()
		wg.Wait()

		for _, r := range results {
			if r.err != nil {
				// ErrClosed and ErrStoreUnavailable (the bound) are the
				// two legitimate refusals.
				if !errors.Is(r.err, claimcheck.ErrClosed) && !errors.Is(r.err, claimcheck.ErrStoreUnavailable) {
					t.Fatalf("round %d: Put = %v, want nil, ErrClosed or ErrStoreUnavailable", i, r.err)
				}
				if r.key != "" {
					t.Fatalf("round %d: Put failed with %v and still returned key %q", i, r.err, r.key)
				}
				continue
			}
			// Put reported success. The payload must therefore be
			// fetchable, OR the store must now be closed -- which is the
			// honest outcome of a Put that won the race and a Close that
			// followed it. What must NOT happen is a success whose payload
			// is missing from a store that is still open.
			got, gerr := s.Get(ctx, r.key, 1<<20)
			switch {
			case gerr == nil:
				if string(got) != "payload" {
					t.Fatalf("round %d: Get returned %q", i, got)
				}
			case errors.Is(gerr, claimcheck.ErrClosed):
				// The store closed after the successful Put. Fine.
			default:
				t.Fatalf("round %d: Put reported success for key %q but Get says %v; a reference to this payload would be a message that can never be handled", i, r.key, gerr)
			}
		}
	}
}

// TestConcurrentGetsAndPuts is the plain concurrency check the race detector
// needs to have something to look at: make ci runs this package under -race.
func TestConcurrentGetsAndPuts(t *testing.T) {
	s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{MaxEntries: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				key, err := s.Put(ctx, []byte("payload"))
				if err != nil {
					continue
				}
				if _, err := s.Get(ctx, key, 1<<20); err != nil {
					t.Errorf("Get of a key Put just returned: %v", err)
					return
				}
				s.Overwrite(key, []byte("payload"))
				_ = s.Len()
				_ = s.Keys()
			}
		}()
	}
	wg.Wait()
}

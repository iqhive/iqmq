package claimcheck_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iqhive/iqmq/x/claimcheck"
	"github.com/iqhive/iqmq/x/claimcheck/storetest"
)

// TestStoreContract holds both of this package's stores to the contract every
// store is held to.
func TestStoreContract(t *testing.T) {
	t.Run("MemoryStore", func(t *testing.T) {
		storetest.Run(t, storetest.Harness{
			New: func(tb testing.TB) claimcheck.Store {
				s, err := claimcheck.NewMemoryStore(claimcheck.MemoryOptions{Name: "memory-1"})
				if err != nil {
					tb.Fatal(err)
				}
				return s
			},
			// The memory store has no lifecycle, so a gone payload is
			// modelled the way the store documents: SetGone.
			Remove: func(tb testing.TB, s claimcheck.Store, _ string) {
				asMemory(tb, s).SetGone(true)
			},
			Break: func(tb testing.TB, s claimcheck.Store) func() {
				m := asMemory(tb, s)
				m.SetUnavailable(true)
				return func() { m.SetUnavailable(false) }
			},
		})
	})
	t.Run("FileStore", func(t *testing.T) {
		type fileHarness struct {
			clock *clock
			dir   string
		}
		var mu sync.Mutex
		built := map[claimcheck.Store]fileHarness{}
		lookup := func(tb testing.TB, s claimcheck.Store) fileHarness {
			tb.Helper()
			mu.Lock()
			defer mu.Unlock()
			h, ok := built[s]
			if !ok {
				tb.Fatal("a store this harness did not build")
			}
			return h
		}
		storetest.Run(t, storetest.Harness{
			New: func(tb testing.TB) claimcheck.Store {
				h := fileHarness{clock: &clock{}, dir: tb.TempDir()}
				s, err := claimcheck.NewFileStore(claimcheck.FileOptions{
					Name: "files-1", Dir: h.dir, TTL: time.Hour, SweepEvery: -1, Now: h.clock.options(),
				})
				if err != nil {
					tb.Fatal(err)
				}
				mu.Lock()
				built[s] = h
				mu.Unlock()
				return s
			},
			// Gone the ordinary way: the TTL passes.
			Remove: func(tb testing.TB, s claimcheck.Store, _ string) {
				lookup(tb, s).clock.advance(2 * time.Hour)
			},
			Break: func(tb testing.TB, s claimcheck.Store) func() {
				if os.Geteuid() == 0 {
					return nil // permissions do not bind root
				}
				dir := lookup(tb, s).dir
				if err := os.Chmod(dir, 0); err != nil {
					tb.Fatal(err)
				}
				return func() { _ = os.Chmod(dir, 0o700) }
			},
		})
	})
}

func asMemory(tb testing.TB, s claimcheck.Store) *claimcheck.MemoryStore {
	tb.Helper()
	m, ok := s.(*claimcheck.MemoryStore)
	if !ok {
		tb.Fatalf("a %T, not the memory store this harness built", s)
	}
	return m
}

func asBroken(tb testing.TB, s claimcheck.Store) *brokenStore {
	tb.Helper()
	b, ok := s.(*brokenStore)
	if !ok {
		tb.Fatalf("a %T, not the broken store this harness built", s)
	}
	return b
}

// defectEnv names the defect the child process injects into brokenStore.
const defectEnv = "IQMQ_CLAIMCHECK_STORETEST_DEFECT"

// storeDefects maps each deliberately broken store to the contract cases
// that must catch it. "none" must pass everything.
var storeDefects = map[string][]string{
	"none":                    nil,
	"gone-as-unavailable":     {"GoneIsPermanent"},
	"unavailable-as-gone":     {"UnreachableIsRetryable"},
	"aliases-the-payload":     {"PutCopiesThePayload"},
	"ignores-the-limit":       {"TheLimitIsInclusiveAndEnforced"},
	"follows-a-forged-key":    {"AForgedKeyIsGone"},
	"reuses-keys":             {"KeysAreDistinct"},
	"forgets-it-was-closed":   {"ClosedIsClosed"},
	"limit-is-exclusive":      {"TheLimitIsInclusiveAndEnforced"},
	"forged-key-looks-broken": {"AForgedKeyIsGone"},
}

// TestTheStoreContractCanFail is the check on the check: storetest is the
// instrument every store is certified with, so it is part of the code under
// test. The whole contract runs in a child process against a store carrying
// one defect at a time, and the named case must FAIL; the correct store must
// pass.
func TestTheStoreContractCanFail(t *testing.T) {
	if os.Getenv(defectEnv) != "" {
		t.Skip("this process IS the child; TestTheStoreContractAgainstABrokenStore is the subject")
	}
	for defect, mustFail := range storeDefects {
		t.Run(defect, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0],
				"-test.run=^TestTheStoreContractAgainstABrokenStore$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), defectEnv+"="+defect)
			raw, err := cmd.CombinedOutput()
			out := string(raw)
			if passed := err == nil; passed != (len(mustFail) == 0) {
				t.Fatalf("the contract passed=%v against the store whose defect is %q, want the opposite:\n%s", passed, defect, out)
			}
			if len(mustFail) == 0 {
				if !strings.Contains(out, "--- PASS: TestTheStoreContractAgainstABrokenStore") {
					t.Fatalf("the correct store's run did not report the contract passing:\n%s", out)
				}
				return
			}
			for _, c := range mustFail {
				if !strings.Contains(out, "--- FAIL: TestTheStoreContractAgainstABrokenStore/"+c+" ") {
					t.Errorf("the run failed, but not in case %s:\n%s", c, out)
				}
			}
		})
	}
}

// TestTheStoreContractAgainstABrokenStore is the child's whole job, skipped in
// an ordinary run: TestTheStoreContractCanFail runs it and cannot be skipped.
func TestTheStoreContractAgainstABrokenStore(t *testing.T) {
	defect := os.Getenv(defectEnv)
	if defect == "" {
		t.Skip(defectEnv + " is unset: TestTheStoreContractCanFail runs this, in a child process, once per defect")
	}
	if _, ok := storeDefects[defect]; !ok {
		t.Fatalf("unknown defect %q", defect)
	}
	storetest.Run(t, storetest.Harness{
		New: func(testing.TB) claimcheck.Store {
			return &brokenStore{defect: defect, objects: map[string][]byte{}}
		},
		Remove: func(tb testing.TB, s claimcheck.Store, key string) {
			b := asBroken(tb, s)
			b.mu.Lock()
			delete(b.objects, key)
			b.mu.Unlock()
		},
		Break: func(tb testing.TB, s claimcheck.Store) func() {
			b := asBroken(tb, s)
			b.mu.Lock()
			b.down = true
			b.mu.Unlock()
			return func() {
				b.mu.Lock()
				b.down = false
				b.mu.Unlock()
			}
		},
	})
}

// brokenStore is a store written from scratch -- not a wrapper around this
// package's own stores, so that a defect in them cannot make the self-test
// pass -- carrying exactly one defect.
type brokenStore struct {
	defect  string
	mu      sync.Mutex
	objects map[string][]byte
	next    int
	down    bool
	closed  bool
}

func (s *brokenStore) Name() string { return "broken" }

func (s *brokenStore) Put(_ context.Context, payload []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.defect != "forgets-it-was-closed" {
		return "", claimcheck.ErrClosed
	}
	if s.down {
		if s.defect == "unavailable-as-gone" {
			return "", claimcheck.ErrPayloadGone
		}
		return "", claimcheck.ErrStoreUnavailable
	}
	if s.defect != "reuses-keys" {
		s.next++
	}
	key := "k" + strings.Repeat("0", 4) + string(rune('a'+s.next%26)) + string(rune('a'+s.next/26%26))
	if s.defect == "aliases-the-payload" {
		s.objects[key] = payload
	} else {
		s.objects[key] = slices.Clone(payload)
	}
	return key, nil
}

func (s *brokenStore) Get(_ context.Context, key string, maxBytes int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.defect != "forgets-it-was-closed" {
		return nil, claimcheck.ErrClosed
	}
	if s.down {
		if s.defect == "unavailable-as-gone" {
			return nil, claimcheck.ErrPayloadGone
		}
		return nil, claimcheck.ErrStoreUnavailable
	}
	p, ok := s.objects[key]
	if !ok && s.defect == "follows-a-forged-key" {
		// Resolves a path-shaped key by its last element, the way a store
		// that joined the key onto a directory would.
		p, ok = s.objects[filepath.Base(strings.TrimSpace(strings.ToLower(key)))]
	}
	if !ok {
		if s.defect == "gone-as-unavailable" || (s.defect == "forged-key-looks-broken" && strings.ContainsAny(key, "./ ")) {
			return nil, claimcheck.ErrStoreUnavailable
		}
		return nil, claimcheck.ErrPayloadGone
	}
	switch {
	case s.defect == "ignores-the-limit":
	case s.defect == "limit-is-exclusive" && int64(len(p)) >= maxBytes:
		return nil, claimcheck.ErrTooLarge
	case int64(len(p)) > maxBytes:
		return nil, claimcheck.ErrTooLarge
	}
	if s.defect == "aliases-the-payload" {
		return p, nil
	}
	return slices.Clone(p), nil
}

func (s *brokenStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

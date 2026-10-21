// Package storetest is the behavioural contract every [idempotency.Store]
// implementation is held to, as one suite two modules run.
//
// It exists because this package ships two stores and a third is easy to
// write. Two implementations of one contract is how they come to disagree, and
// the half that disagrees is the half nobody ran: [idempotency.MemoryStore] is
// exercised on every `go test`, while the Redis store needs a broker and skips
// in CI. So the contract is written once, here, and both run it — the Redis
// one against a real server when `IQMQ_REDIS_ADDR` names one, and against
// miniredis when it does not.
//
// Every case below is one sentence about the contract, and every one has been
// watched failing: [Run] is run against [Broken] by the suite's own selftest,
// which is a store that gets each sentence wrong in turn.
package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/iqhive/iqmq/x/idempotency"
)

// Harness is what one implementation hands the suite.
type Harness struct {
	// Name identifies the implementation in the subtest names.
	Name string
	// New builds a store for one case. It is called once per case, so a case
	// cannot be polluted by the keys of another, and the store must be closed
	// by the harness itself (tb.Cleanup).
	New func(tb testing.TB) idempotency.Store
	// Elapse moves the store's world past d. Nil sleeps d on the wall clock,
	// which is what a real Redis needs; a store on an injected clock passes a
	// function that advances it, which is what makes the suite fast.
	Elapse func(tb testing.TB, s idempotency.Store, d time.Duration)
	// Window is the claim window the cases use. It must be long enough that
	// nothing lapses mid-case and short enough that a wall-clock Elapse is
	// not a slow test. Zero takes DefaultWindow.
	Window time.Duration
	// NoConcurrency skips the election case for a store that is deliberately
	// not safe for concurrent use. Nothing shipped here sets it; it exists so
	// that a caller with a single-threaded store says so rather than
	// silently passing a case it does not satisfy.
	NoConcurrency bool
}

// DefaultWindow is the claim window a Harness with none takes.
//
// 400 ms is a compromise between the two Elapse shapes: long enough that no
// case lapses while a loaded host runs it, short enough that the four cases
// which wait it out cost under two seconds against a real Redis.
const DefaultWindow = 400 * time.Millisecond

func (h Harness) window() time.Duration {
	if h.Window > 0 {
		return h.Window
	}
	return DefaultWindow
}

func (h Harness) elapse(tb testing.TB, s idempotency.Store, d time.Duration) {
	tb.Helper()
	if h.Elapse != nil {
		h.Elapse(tb, s, d)
		return
	}
	time.Sleep(d)
}

// Run runs the whole contract against h.
func Run(t *testing.T, h Harness) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(*testing.T, Harness)
	}{
		{"FirstThenDuplicate", testFirstThenDuplicate},
		{"AnOpenClaimReadsInFlightAndNotDuplicate", testOpenClaimIsInFlight},
		{"AbortMakesTheKeyClaimableAgain", testAbortReleases},
		{"TheWindowLapsesAndTheKeyIsFirstAgain", testWindowLapses},
		{"CommitStartsTheWindowAfresh", testCommitRebasesTheWindow},
		{"CommitTakesTheClaimsWindow", testCommitTakesTheClaimsWindow},
		{"CommitIsIdempotent", testCommitIsIdempotent},
		{"CommitOfALapsedClaimIsRefused", testCommitOfALapsedClaim},
		{"AbortOfALapsedClaimDoesNotReleaseTheNewHolder", testAbortOfALapsedClaim},
		{"KeysAreIndependent", testKeysAreIndependent},
		{"AnEmptyKeyAndANonPositiveWindowAreRefused", testArgumentRefusals},
		{"CommitAndAbortNeedAHeldClaim", testNeedAHeldClaim},
		{"AClosedStoreRefusesEverything", testClosedStore},
		{"ConcurrentClaimsElectExactlyOneWinner", testElection},
	}
	for _, c := range cases {
		t.Run(h.Name+"/"+c.name, func(t *testing.T) { c.fn(t, h) })
	}
}

// key returns a key nothing else in the suite uses. A shared Redis is shared
// between runs as well as between cases, so the name carries the case and the
// clock.
func key(tb testing.TB, suffix string) string {
	tb.Helper()
	return "storetest:" + tb.Name() + ":" + suffix + ":" + time.Now().Format("150405.000000000")
}

func testFirstThenDuplicate(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	c, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the first Claim reported %v", err)
	}
	if c.Status != idempotency.StatusFirst {
		t.Fatalf("the first Claim of a fresh key is %v, want %v", c.Status, idempotency.StatusFirst)
	}
	if c.Token == "" {
		t.Error("a held claim carries no token, so nothing can commit or abort it")
	}
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("Commit reported %v", err)
	}
	again, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the second Claim reported %v", err)
	}
	if again.Status != idempotency.StatusDuplicate {
		t.Fatalf("a committed key reads %v, want %v", again.Status, idempotency.StatusDuplicate)
	}
	if again.Token != "" {
		t.Error("a duplicate verdict carries a token; only a held claim has one")
	}
}

func testOpenClaimIsInFlight(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	if _, err := s.Claim(t.Context(), k, h.window()); err != nil {
		t.Fatalf("the first Claim reported %v", err)
	}
	// Nothing has been committed, so nothing has been DONE. This is the
	// sentence the whole design turns on: an open claim must not read as a
	// duplicate, because the caller would skip the work and ack the message.
	second, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the second Claim reported %v", err)
	}
	if second.Status != idempotency.StatusInFlight {
		t.Fatalf("an UNCOMMITTED claim reads %v, want %v: reading it as a duplicate skips work that never happened",
			second.Status, idempotency.StatusInFlight)
	}
}

func testAbortReleases(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	c, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if err := s.Abort(t.Context(), c); err != nil {
		t.Fatalf("Abort reported %v", err)
	}
	again, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the Claim after an Abort reported %v", err)
	}
	if again.Status != idempotency.StatusFirst {
		t.Fatalf("after an Abort the key reads %v, want %v: an aborted claim must leave the work to be done",
			again.Status, idempotency.StatusFirst)
	}
}

func testWindowLapses(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	c, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("Commit reported %v", err)
	}
	h.elapse(t, s, h.window()+h.window()/4)
	again, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the Claim after the window reported %v", err)
	}
	if again.Status != idempotency.StatusFirst {
		t.Fatalf("past the window the key reads %v, want %v: the window is bounded and a store that remembers for ever is not this one",
			again.Status, idempotency.StatusFirst)
	}
}

// testCommitRebasesTheWindow: the duplicate-suppression window starts at the
// COMMIT and lasts a whole window, not whatever was left of the open claim.
//
// A store that re-based on the remainder would suppress almost nothing after a
// handler that took most of its window -- and a slow handler is exactly the
// case a redelivery is most likely to follow.
func testCommitRebasesTheWindow(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	w := h.window()
	c, err := s.Claim(t.Context(), k, w)
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if c.Window != w {
		t.Fatalf("the held claim reports Window %v, want %v: Commit has nothing to re-base on", c.Window, w)
	}
	// Most of the window goes by inside the "handler".
	h.elapse(t, s, w*3/4)
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("Commit after three quarters of the window reported %v", err)
	}
	// Now past the ORIGINAL deadline, and well inside a fresh window.
	h.elapse(t, s, w/2)
	again, err := s.Claim(t.Context(), k, w)
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if again.Status != idempotency.StatusDuplicate {
		t.Fatalf("%v after the commit the key reads %v, want %v: the committed window was re-based on the REMAINDER of the open claim rather than started afresh",
			w/2, again.Status, idempotency.StatusDuplicate)
	}
}

// testCommitTakesTheClaimsWindow: Commit remembers the key for the Claim's
// own Window, which a caller may raise past the window the claim was taken
// with. That is how an open claim is kept to a short lease, so that a worker
// that died mid-handler blocks its redelivery for the lease and not for the
// whole duplicate window.
func testCommitTakesTheClaimsWindow(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	lease := h.window() / 2
	c, err := s.Claim(t.Context(), k, lease)
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	c.Window = 2 * h.window()
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("Commit reported %v", err)
	}
	h.elapse(t, s, h.window())
	again, err := s.Claim(t.Context(), k, lease)
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if again.Status != idempotency.StatusDuplicate {
		t.Fatalf("%v after a commit with Window %v the key reads %v, want %v: the commit took the window the claim was opened with",
			h.window(), c.Window, again.Status, idempotency.StatusDuplicate)
	}
}

// testCommitIsIdempotent: a second Commit of a live claim is not an error.
//
// It is the shape a retry of the store call takes, and the two
// implementations reach it by different routes -- the in-process store matches
// the token, the Redis script accepts the committed value as well as the open
// one -- so it is pinned here or they drift.
func testCommitIsIdempotent(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	c, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("the first Commit reported %v", err)
	}
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("a second Commit of the same live claim reported %v, want nil", err)
	}
	again, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if again.Status != idempotency.StatusDuplicate {
		t.Fatalf("after two commits the key reads %v, want %v", again.Status, idempotency.StatusDuplicate)
	}
}

func testCommitOfALapsedClaim(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	stale, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	h.elapse(t, s, h.window()+h.window()/4)
	// Another worker takes the key.
	fresh, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the second Claim reported %v", err)
	}
	if fresh.Status != idempotency.StatusFirst {
		t.Fatalf("the key was not re-claimable after its window: %v", fresh.Status)
	}
	if fresh.Token == stale.Token {
		t.Fatal("the re-claim reused the lapsed claim's token, so nothing can tell the two apart")
	}
	err = s.Commit(t.Context(), stale)
	if !errors.Is(err, idempotency.ErrClaimLost) {
		t.Fatalf("committing a lapsed claim reported %v, want ErrClaimLost: it would otherwise mark ANOTHER worker's work done", err)
	}
	// And the new holder's claim is untouched: still open, not committed.
	third, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the third Claim reported %v", err)
	}
	if third.Status != idempotency.StatusInFlight {
		t.Fatalf("after the refused Commit the key reads %v, want %v", third.Status, idempotency.StatusInFlight)
	}
}

func testAbortOfALapsedClaim(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	stale, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	h.elapse(t, s, h.window()+h.window()/4)
	fresh, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("the second Claim reported %v", err)
	}
	if err := s.Commit(t.Context(), fresh); err != nil {
		t.Fatalf("the new holder's Commit reported %v", err)
	}
	err = s.Abort(t.Context(), stale)
	if !errors.Is(err, idempotency.ErrClaimLost) {
		t.Fatalf("aborting a lapsed claim reported %v, want ErrClaimLost", err)
	}
	after, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if after.Status != idempotency.StatusDuplicate {
		t.Fatalf("the refused Abort left the key reading %v, want %v: it deleted the new holder's committed claim",
			after.Status, idempotency.StatusDuplicate)
	}
}

func testKeysAreIndependent(t *testing.T, h Harness) {
	s := h.New(t)
	a, b := key(t, "a"), key(t, "b")
	ca, err := s.Claim(t.Context(), a, h.window())
	if err != nil {
		t.Fatalf("Claim(a) reported %v", err)
	}
	if err := s.Commit(t.Context(), ca); err != nil {
		t.Fatalf("Commit(a) reported %v", err)
	}
	cb, err := s.Claim(t.Context(), b, h.window())
	if err != nil {
		t.Fatalf("Claim(b) reported %v", err)
	}
	if cb.Status != idempotency.StatusFirst {
		t.Fatalf("key b reads %v after key a was committed, want %v", cb.Status, idempotency.StatusFirst)
	}
}

func testArgumentRefusals(t *testing.T, h Harness) {
	s := h.New(t)
	if _, err := s.Claim(t.Context(), "", h.window()); !errors.Is(err, idempotency.ErrEmptyKey) {
		t.Errorf("Claim with an empty key reported %v, want ErrEmptyKey: one shared key makes every message a duplicate of the first", err)
	}
	if _, err := s.Claim(t.Context(), key(t, "k"), 0); !errors.Is(err, idempotency.ErrBadWindow) {
		t.Errorf("Claim with a zero window reported %v, want ErrBadWindow", err)
	}
	if _, err := s.Claim(t.Context(), key(t, "k"), -time.Second); !errors.Is(err, idempotency.ErrBadWindow) {
		t.Errorf("Claim with a negative window reported %v, want ErrBadWindow", err)
	}
}

func testNeedAHeldClaim(t *testing.T, h Harness) {
	s := h.New(t)
	if err := s.Commit(t.Context(), idempotency.Claim{}); !errors.Is(err, idempotency.ErrNoClaim) {
		t.Errorf("Commit of the zero Claim reported %v, want ErrNoClaim", err)
	}
	if err := s.Abort(t.Context(), idempotency.Claim{}); !errors.Is(err, idempotency.ErrNoClaim) {
		t.Errorf("Abort of the zero Claim reported %v, want ErrNoClaim", err)
	}
	dup := idempotency.Claim{Key: key(t, "k"), Status: idempotency.StatusDuplicate}
	if err := s.Commit(t.Context(), dup); !errors.Is(err, idempotency.ErrNoClaim) {
		t.Errorf("Commit of a duplicate verdict reported %v, want ErrNoClaim", err)
	}
}

func testClosedStore(t *testing.T, h Harness) {
	s := h.New(t)
	k := key(t, "k")
	c, err := s.Claim(t.Context(), k, h.window())
	if err != nil {
		t.Fatalf("Claim reported %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close reported %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("a second Close reported %v, want nil: Close is idempotent ", err)
	}
	if _, err := s.Claim(t.Context(), k, h.window()); !errors.Is(err, idempotency.ErrClosed) {
		t.Errorf("Claim on a closed store reported %v, want ErrClosed", err)
	}
	if err := s.Commit(t.Context(), c); !errors.Is(err, idempotency.ErrClosed) {
		t.Errorf("Commit on a closed store reported %v, want ErrClosed", err)
	}
	if err := s.Abort(t.Context(), c); !errors.Is(err, idempotency.ErrClosed) {
		t.Errorf("Abort on a closed store reported %v, want ErrClosed", err)
	}
}

func testElection(t *testing.T, h Harness) {
	if h.NoConcurrency {
		t.Skip("the harness declares this store unsafe for concurrent use")
	}
	s := h.New(t)
	k := key(t, "k")
	const workers = 16
	var (
		mu      sync.Mutex
		byState = map[idempotency.Status]int{}
		firsts  []idempotency.Claim
	)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c, err := s.Claim(context.Background(), k, h.window())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				byState[idempotency.StatusUnknown]++
				return
			}
			byState[c.Status]++
			if c.Status == idempotency.StatusFirst {
				firsts = append(firsts, c)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(firsts) != 1 {
		t.Fatalf("%d of %d concurrent claims on one key were told they were first, want exactly 1 (statuses: %v)",
			len(firsts), workers, byState)
	}
	if got := byState[idempotency.StatusInFlight]; got != workers-1 {
		t.Errorf("%d claims read in-flight, want %d (statuses: %v); a loser that reads DUPLICATE would skip work nobody has done",
			got, workers-1, byState)
	}
	if got := byState[idempotency.StatusDuplicate]; got != 0 {
		t.Errorf("%d concurrent claims read duplicate, want 0: nothing has been committed yet", got)
	}
}

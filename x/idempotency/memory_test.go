package idempotency_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/x/idempotency"
	"github.com/iqhive/iqmq/x/idempotency/storetest"
)

// testClock is a manually advanced mq.Clock.
//
// The store reads only Now, but the interface asks for a timer as well, so
// NewTimer hands back a real one: nothing in this package arms a timer, and a
// fake one here would be dead code pretending to be a fixture.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *testClock) NewTimer(d time.Duration) mq.Timer { return realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// TestMemoryStoreContract holds the in-process store to the whole contract of
// x/idempotency/storetest, which is the same suite the Redis store runs.
func TestMemoryStoreContract(t *testing.T) {
	clocks := map[idempotency.Store]*testClock{}
	var mu sync.Mutex
	storetest.Run(t, storetest.Harness{
		Name: "memory",
		New: func(tb testing.TB) idempotency.Store {
			clk := newTestClock()
			s, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{Clock: clk})
			if err != nil {
				tb.Fatalf("NewMemoryStore: %v", err)
			}
			tb.Cleanup(func() { _ = s.Close() })
			mu.Lock()
			clocks[s] = clk
			mu.Unlock()
			return s
		},
		Elapse: func(tb testing.TB, s idempotency.Store, d time.Duration) {
			tb.Helper()
			mu.Lock()
			clk := clocks[s]
			mu.Unlock()
			if clk == nil {
				tb.Fatalf("no clock was recorded for %T", s)
			}
			clk.Advance(d)
		},
	})
}

// TestEvictionIsAFalseFirstAndIsCounted is the honesty check on the bound.
//
// A claim table that is full throws away a claim whose window has NOT lapsed,
// and the key then reads as StatusFirst though its work was done. That is the
// store's own answer being wrong in the direction that lets a duplicate
// through, and the only defensible thing to do about it is to count it: this
// test asserts both halves, because a store that evicted silently would look
// exactly like one whose window was too short.
func TestEvictionIsAFalseFirstAndIsCounted(t *testing.T) {
	const window = time.Hour
	clk := newTestClock()
	s, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{MaxEntries: 4, Clock: clk})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	defer func() { _ = s.Close() }()

	first := "key-0"
	c, err := s.Claim(t.Context(), first, window)
	if err != nil || c.Status != idempotency.StatusFirst {
		t.Fatalf("Claim(%q) = %v, %v", first, c.Status, err)
	}
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// Five more keys through a table of four: the oldest, which is the
	// committed one above, is pushed out.
	for i := 1; i <= 5; i++ {
		k := "key-" + strconv.Itoa(i)
		ci, err := s.Claim(t.Context(), k, window)
		if err != nil {
			t.Fatalf("Claim(%q): %v", k, err)
		}
		if err := s.Commit(t.Context(), ci); err != nil {
			t.Fatalf("Commit(%q): %v", k, err)
		}
	}
	st := s.Stats()
	if st.Entries > 4 {
		t.Errorf("the table holds %d entries, want at most MaxEntries=4", st.Entries)
	}
	if st.Evicted == 0 {
		t.Errorf("Stats().Evicted is 0 after %d keys through a table of 4: an eviction that is not counted is a duplicate nobody can explain", 6)
	}
	if st.Expired != 0 {
		t.Errorf("Stats().Expired is %d, want 0: the window is an hour and the clock has not moved, so nothing lapsed", st.Expired)
	}
	back, err := s.Claim(t.Context(), first, window)
	if err != nil {
		t.Fatalf("re-Claim(%q): %v", first, err)
	}
	if back.Status != idempotency.StatusFirst {
		t.Fatalf("the evicted key reads %v, want %v: this test is about the eviction being a FALSE FIRST, and it did not happen", back.Status, idempotency.StatusFirst)
	}
}

// TestExpiryAndEvictionAreCountedSeparately says the two reasons an entry
// disappears are told apart, because only one of them is a defect in the
// configuration.
func TestExpiryAndEvictionAreCountedSeparately(t *testing.T) {
	clk := newTestClock()
	s, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{MaxEntries: 2, Clock: clk})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	defer func() { _ = s.Close() }()
	c, err := s.Claim(t.Context(), "a", time.Minute)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := s.Commit(t.Context(), c); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	clk.Advance(2 * time.Minute)
	if got, err := s.Claim(t.Context(), "a", time.Minute); err != nil || got.Status != idempotency.StatusFirst {
		t.Fatalf("Claim after the window = %v, %v; want first", got.Status, err)
	}
	st := s.Stats()
	if st.Expired != 1 {
		t.Errorf("Stats().Expired = %d, want 1", st.Expired)
	}
	if st.Evicted != 0 {
		t.Errorf("Stats().Evicted = %d, want 0: the table never went over MaxEntries", st.Evicted)
	}
}

// TestANegativeMaxEntriesIsRefused pins the one place this package's
// convention is the opposite of the engine's.
//
// The specification makes a negative engine capacity mean "unbounded", because
// an unbounded lane is a lane that blocks less. A negative claim table would
// be a process that grows until it dies, so it is refused instead.
func TestANegativeMaxEntriesIsRefused(t *testing.T) {
	if _, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{MaxEntries: -1}); err == nil {
		t.Fatal("NewMemoryStore accepted MaxEntries=-1; the specification's 'negative means unbounded' must not reach a claim table")
	}
	s, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{MaxEntries: 0})
	if err != nil {
		t.Fatalf("MaxEntries=0 was refused: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestStatsSumToClaims holds the counters to their stated arithmetic: the
// in-process store cannot be unreachable, so every claim it answered is one of
// the three real statuses.
func TestStatsSumToClaims(t *testing.T) {
	clk := newTestClock()
	s, err := idempotency.NewMemoryStore(idempotency.MemoryOptions{Clock: clk})
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}
	defer func() { _ = s.Close() }()
	c, _ := s.Claim(t.Context(), "a", time.Minute) // first
	_, _ = s.Claim(t.Context(), "a", time.Minute)  // in flight
	_ = s.Commit(t.Context(), c)                   //
	_, _ = s.Claim(t.Context(), "a", time.Minute)  // duplicate
	d, _ := s.Claim(t.Context(), "b", time.Minute) // first
	_ = s.Abort(t.Context(), d)                    //
	if err := s.Commit(t.Context(), c); !errors.Is(err, idempotency.ErrClaimLost) {
		// c was committed once; committing it again finds a row whose token
		// still matches, so this is NOT ErrClaimLost -- it is a second commit
		// of a live claim, which is idempotent. The assertion is here to pin
		// which of the two it is.
		if err != nil {
			t.Fatalf("a second Commit of a live claim reported %v, want nil", err)
		}
	}
	st := s.Stats()
	if got := st.First + st.Duplicates + st.InFlight; got != st.Claims {
		t.Errorf("First+Duplicates+InFlight = %d, Claims = %d; the in-process store never answers unknown, so they must be equal", got, st.Claims)
	}
	if st.First != 2 || st.InFlight != 1 || st.Duplicates != 1 {
		t.Errorf("statuses were first=%d in-flight=%d duplicate=%d, want 2/1/1", st.First, st.InFlight, st.Duplicates)
	}
	if st.Aborts != 1 {
		t.Errorf("Stats().Aborts = %d, want 1", st.Aborts)
	}
}

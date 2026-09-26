package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iqhive/iqmq/x/idempotency"
)

// Defect names one way a [Store] implementation can be wrong.
//
// Every one of these is a mistake somebody would plausibly make, and each is
// the negation of one sentence [Run] asserts. They exist so that the suite can
// be WATCHED failing: a check nobody has seen fail is assumed not to work, and
// this repository has found nine that could not.
type Defect string

// The defects, each with the case it must break.
const (
	// DefectNone is the correct store. Run must PASS against it, which is the
	// other half of the selftest: a suite that failed everything would also
	// "fail on every defect".
	DefectNone Defect = "none"
	// DefectOpenClaimReadsDuplicate is the single-phase store: it marks the
	// key done at Claim, so an open claim reads as a duplicate and the work
	// of a worker that died is skipped for ever. Breaks
	// AnOpenClaimReadsInFlightAndNotDuplicate and
	// ConcurrentClaimsElectExactlyOneWinner.
	DefectOpenClaimReadsDuplicate Defect = "an-open-claim-reads-duplicate"
	// DefectCommitDoesNothing loses the commit, so nothing is ever a
	// duplicate. Breaks FirstThenDuplicate.
	DefectCommitDoesNothing Defect = "commit-does-nothing"
	// DefectAbortDoesNothing leaves the claim open, so a failed handler's
	// redelivery is deferred for a whole window instead of processed. Breaks
	// AbortMakesTheKeyClaimableAgain.
	DefectAbortDoesNothing Defect = "abort-does-nothing"
	// DefectWindowNeverLapses is the unbounded ledger. Breaks
	// TheWindowLapsesAndTheKeyIsFirstAgain.
	DefectWindowNeverLapses Defect = "the-window-never-lapses"
	// DefectIgnoresTheToken is the store with no compare-and-set: a stale
	// Commit marks another worker's claim done and a stale Abort deletes it.
	// Breaks CommitOfALapsedClaimIsRefused and
	// AbortOfALapsedClaimDoesNotReleaseTheNewHolder.
	DefectIgnoresTheToken Defect = "commit-and-abort-ignore-the-token"
	// DefectAcceptsAnEmptyKey turns every keyless message into a duplicate of
	// the first. Breaks AnEmptyKeyAndANonPositiveWindowAreRefused.
	DefectAcceptsAnEmptyKey Defect = "an-empty-key-is-accepted"
	// DefectAcceptsAnyClaim accepts the zero Claim. Breaks
	// CommitAndAbortNeedAHeldClaim.
	DefectAcceptsAnyClaim Defect = "any-claim-is-accepted"
	// DefectCloseDoesNothing keeps serving after Close. Breaks
	// AClosedStoreRefusesEverything.
	DefectCloseDoesNothing Defect = "close-does-nothing"
	// DefectEveryClaimIsFirst is the store with no mutual exclusion at all —
	// the read-then-write race written as a defect. Breaks
	// ConcurrentClaimsElectExactlyOneWinner.
	DefectEveryClaimIsFirst Defect = "every-claim-is-first"
)

// Defects returns every defect, [DefectNone] first.
func Defects() []Defect {
	return []Defect{
		DefectNone,
		DefectOpenClaimReadsDuplicate,
		DefectCommitDoesNothing,
		DefectAbortDoesNothing,
		DefectWindowNeverLapses,
		DefectIgnoresTheToken,
		DefectAcceptsAnEmptyKey,
		DefectAcceptsAnyClaim,
		DefectCloseDoesNothing,
		DefectEveryClaimIsFirst,
	}
}

// BrokenStore is a deliberately wrong [idempotency.Store].
//
// It is written from scratch rather than as a wrapper around
// [idempotency.MemoryStore] so that a defect in the real store cannot make a
// selftest pass: the subject of the selftest is the SUITE, and it has to be
// held against an implementation the suite's own dependencies do not share.
type BrokenStore struct {
	defect Defect

	mu     sync.Mutex
	now    time.Time
	next   int
	rows   map[string]*brokenRow
	closed bool
}

type brokenRow struct {
	token   string
	done    bool
	expires time.Time
	window  time.Duration
}

// Broken builds a store carrying exactly one defect. [DefectNone] is correct.
func Broken(d Defect) *BrokenStore {
	return &BrokenStore{
		defect: d,
		now:    time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
		rows:   map[string]*brokenRow{},
	}
}

// Advance moves this store's clock, which is what a [Harness.Elapse] over a
// BrokenStore does instead of sleeping.
func (s *BrokenStore) Advance(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = s.now.Add(d)
}

// live reports the row for key if it is still within its window.
func (s *BrokenStore) live(key string) (*brokenRow, bool) {
	r, ok := s.rows[key]
	if !ok {
		return nil, false
	}
	if s.defect != DefectWindowNeverLapses && !s.now.Before(r.expires) {
		delete(s.rows, key)
		return nil, false
	}
	return r, true
}

// Claim implements [idempotency.Store].
func (s *BrokenStore) Claim(_ context.Context, key string, window time.Duration) (idempotency.Claim, error) {
	if key == "" && s.defect != DefectAcceptsAnEmptyKey {
		return idempotency.Claim{}, idempotency.ErrEmptyKey
	}
	if window <= 0 {
		return idempotency.Claim{}, idempotency.ErrBadWindow
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.defect != DefectCloseDoesNothing {
		return idempotency.Claim{}, idempotency.ErrClosed
	}
	if s.defect == DefectEveryClaimIsFirst {
		s.next++
		return idempotency.Claim{
			Key: key, Token: "t" + itoa(s.next),
			Status: idempotency.StatusFirst, Window: window, Expires: s.now.Add(window),
		}, nil
	}
	if r, ok := s.live(key); ok {
		if r.done {
			return idempotency.Claim{Key: key, Status: idempotency.StatusDuplicate}, nil
		}
		return idempotency.Claim{Key: key, Status: idempotency.StatusInFlight}, nil
	}
	s.next++
	tok := "t" + itoa(s.next)
	s.rows[key] = &brokenRow{
		token: tok,
		// The single-phase defect: done at Claim rather than at Commit.
		done:    s.defect == DefectOpenClaimReadsDuplicate,
		expires: s.now.Add(window),
		window:  window,
	}
	return idempotency.Claim{Key: key, Token: tok, Status: idempotency.StatusFirst, Window: window, Expires: s.now.Add(window)}, nil
}

// Commit implements [idempotency.Store].
func (s *BrokenStore) Commit(_ context.Context, c idempotency.Claim) error {
	if err := s.check(c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.defect != DefectCloseDoesNothing {
		return idempotency.ErrClosed
	}
	if s.defect == DefectCommitDoesNothing {
		return nil
	}
	r, ok := s.live(c.Key)
	if !ok {
		return idempotency.ErrClaimLost
	}
	if r.token != c.Token && s.defect != DefectIgnoresTheToken {
		return idempotency.ErrClaimLost
	}
	r.done = true
	r.expires = s.now.Add(r.window)
	return nil
}

// Abort implements [idempotency.Store].
func (s *BrokenStore) Abort(_ context.Context, c idempotency.Claim) error {
	if err := s.check(c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.defect != DefectCloseDoesNothing {
		return idempotency.ErrClosed
	}
	r, ok := s.live(c.Key)
	if !ok {
		return idempotency.ErrClaimLost
	}
	if r.token != c.Token && s.defect != DefectIgnoresTheToken {
		return idempotency.ErrClaimLost
	}
	if s.defect == DefectAbortDoesNothing {
		return nil
	}
	delete(s.rows, c.Key)
	return nil
}

// Close implements [idempotency.Store].
func (s *BrokenStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// check is the held-claim guard, or its absence.
func (s *BrokenStore) check(c idempotency.Claim) error {
	if s.defect == DefectAcceptsAnyClaim {
		return nil
	}
	if c.Status != idempotency.StatusFirst || c.Key == "" || c.Token == "" {
		return idempotency.ErrNoClaim
	}
	return nil
}

// itoa is strconv.Itoa without the import, so that this file's dependencies
// are exactly the two the store interface needs.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// BrokenHarness is the [Harness] for a store carrying d.
func BrokenHarness(d Defect) Harness {
	// One store for the whole suite would let one case's keys reach another,
	// so New builds one per case -- and the Elapse closure has to reach
	// whichever store the case is using, which is why it takes the store back
	// as an argument rather than capturing one.
	return Harness{
		Name: string(d),
		New: func(tb testing.TB) idempotency.Store {
			s := Broken(d)
			tb.Cleanup(func() { _ = s.Close() })
			return s
		},
		Elapse: func(tb testing.TB, s idempotency.Store, dur time.Duration) {
			tb.Helper()
			bs, ok := s.(*BrokenStore)
			if !ok {
				tb.Fatalf("Elapse was handed a %T, not a *BrokenStore", s)
			}
			bs.Advance(dur)
		},
	}
}

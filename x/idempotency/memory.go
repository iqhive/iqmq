package idempotency

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
)

// DefaultMaxEntries bounds a [MemoryStore] whose Options.MaxEntries is zero.
//
// A bound is not optional. An unbounded claim table is a memory leak whose
// size is chosen by whoever publishes to the topic, and this store's whole
// purpose is to be the one a program can run without a Redis — so it is the
// one most likely to be left with defaults in a process that must not grow.
const DefaultMaxEntries = 100_000

// MemoryOptions configures a [MemoryStore].
type MemoryOptions struct {
	// MaxEntries bounds the claim table. Zero takes [DefaultMaxEntries]; a
	// negative value is refused rather than read as "unbounded", which is the
	// convention the specification set for the engine's capacities and the
	// opposite convention on purpose: an unbounded capacity is a queue that
	// blocks less, an unbounded claim table is a process that dies.
	MaxEntries int
	// Clock is the time source. Nil takes the wall clock.
	Clock mq.Clock
}

// MemoryStore is an in-process [Store]: a bounded LRU of claims with a
// per-entry window.
//
// It is what makes this package usable and testable with no Redis at all, and
// it is honest about the two things it is not:
//
//   - It is PER PROCESS. Two replicas of a service have two stores and
//     therefore two independent windows, so a duplicate delivered to the other
//     replica is a [StatusFirst] there. A store that crosses replicas is what
//     the redisstore package is for.
//   - It is LOSSY BY DESIGN, and it counts the loss. An entry evicted to stay
//     inside MaxEntries is a claim that will read as [StatusFirst] before its
//     window lapsed, and [Stats.Evicted] is how many. A non-zero Evicted with
//     a duplicate rate that matters means MaxEntries is too small; nothing
//     here raises it for you.
type MemoryStore struct {
	max   int
	clock mq.Clock

	mu      sync.Mutex
	entries map[string]*list.Element // key -> element of lru
	lru     *list.List               // front = most recently touched
	closed  bool

	stats memStats
}

// memStats is [MemoryStore]'s counters. They are atomics rather than fields
// under the mutex so that Stats can be read from a metrics goroutine without
// contending with the claim path.
type memStats struct {
	claims     atomic.Int64
	first      atomic.Int64
	duplicates atomic.Int64
	inFlight   atomic.Int64
	commits    atomic.Int64
	aborts     atomic.Int64
	evicted    atomic.Int64
	expired    atomic.Int64
	lost       atomic.Int64
}

// Stats is what a [MemoryStore] has counted.
type Stats struct {
	// Claims is how many Claim calls were answered.
	Claims int64
	// First, Duplicates and InFlight are how many of those answers each
	// status was. They sum to Claims: this store never answers
	// [StatusUnknown], because it cannot be unreachable.
	First      int64
	Duplicates int64
	InFlight   int64
	// Commits and Aborts are how many claims were closed each way.
	Commits int64
	Aborts  int64
	// Evicted is how many entries were dropped to stay inside MaxEntries —
	// claims whose window had NOT lapsed. Every one of them is a duplicate
	// this store will not catch.
	Evicted int64
	// Expired is how many entries were dropped because their window lapsed.
	// Those are the window working as intended.
	Expired int64
	// Lost is how many Commit or Abort calls reported [ErrClaimLost].
	Lost int64
	// Entries is how many claims the table holds right now.
	Entries int
}

// memEntry is one claim.
type memEntry struct {
	key   string
	token string
	// done distinguishes a committed claim (the key reads as a duplicate)
	// from an open one (it reads as in flight).
	done bool
	// expires is when this entry lapses. Commit re-bases it on the Claim's
	// own Window, which the caller may have raised past the open claim's.
	expires time.Time
}

// NewMemoryStore builds an in-process store.
//
// It returns an error rather than panicking on a bad option, because a store
// is built where a client is built and that is a place that already handles
// errors.
func NewMemoryStore(opts MemoryOptions) (*MemoryStore, error) {
	if opts.MaxEntries < 0 {
		return nil, ErrBadMaxEntries
	}
	max := opts.MaxEntries
	if max == 0 {
		max = DefaultMaxEntries
	}
	clk := opts.Clock
	if clk == nil {
		clk = wallClock{}
	}
	return &MemoryStore{
		max:     max,
		clock:   clk,
		entries: make(map[string]*list.Element),
		lru:     list.New(),
	}, nil
}

// ErrBadMaxEntries reports a negative MemoryOptions.MaxEntries.
var ErrBadMaxEntries = memErr("idempotency: MemoryOptions.MaxEntries must not be negative; zero takes the default")

// memErr is a tiny named error type so that these errors are distinguishable
// from a caller's without exporting a type.
type memErr string

func (e memErr) Error() string { return string(e) }

// Claim implements [Store].
func (s *MemoryStore) Claim(_ context.Context, key string, window time.Duration) (Claim, error) {
	if err := validate(key, window); err != nil {
		return Claim{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Claim{}, ErrClosed
	}
	now := s.clock.Now()
	if el, ok := s.entries[key]; ok {
		e := el.Value.(*memEntry)
		if now.Before(e.expires) {
			// A live entry. Touching it on a READ would be wrong: the LRU
			// order decides which unexpired claim is thrown away, and a
			// duplicate storm on one key would otherwise protect that key
			// while evicting every key that is being seen for the first
			// time — the opposite of what the table is for.
			s.stats.claims.Add(1)
			if e.done {
				s.stats.duplicates.Add(1)
				return Claim{Key: key, Status: StatusDuplicate}, nil
			}
			s.stats.inFlight.Add(1)
			return Claim{Key: key, Status: StatusInFlight}, nil
		}
		// Lapsed: drop it and fall through to a fresh claim.
		s.dropLocked(el)
		s.stats.expired.Add(1)
	}
	tok, err := newToken()
	if err != nil {
		// A store that cannot produce a token cannot hand out a claim, and
		// answering StatusFirst without one would hand out a claim nobody can
		// commit. crypto/rand does not fail in practice; if it does, this is
		// the unreachable-store case and the caller's OnUnavailable policy is
		// the right thing to reach.
		s.stats.claims.Add(1)
		return Claim{Key: key, Status: StatusUnknown}, err
	}
	e := &memEntry{key: key, token: tok, expires: now.Add(window)}
	s.entries[key] = s.lru.PushFront(e)
	s.evictLocked()
	s.stats.claims.Add(1)
	s.stats.first.Add(1)
	return Claim{Key: key, Token: tok, Status: StatusFirst, Window: window, Expires: e.expires}, nil
}

// Commit implements [Store].
func (s *MemoryStore) Commit(_ context.Context, c Claim) error {
	if err := heldClaim(c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	e, err := s.liveLocked(c)
	if err != nil {
		return err
	}
	e.done = true
	e.expires = s.clock.Now().Add(c.Window)
	s.stats.commits.Add(1)
	return nil
}

// Abort implements [Store].
func (s *MemoryStore) Abort(_ context.Context, c Claim) error {
	if err := heldClaim(c); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, err := s.liveLocked(c); err != nil {
		return err
	}
	s.dropLocked(s.entries[c.Key])
	s.stats.aborts.Add(1)
	return nil
}

// liveLocked resolves c to the entry it claims, or reports [ErrClaimLost].
//
// The token comparison is what makes Commit and Abort safe against a lapsed
// claim: without it, an Abort arriving after another worker re-claimed the key
// would delete THEIR claim, and a Commit would mark THEIR work done.
func (s *MemoryStore) liveLocked(c Claim) (*memEntry, error) {
	el, ok := s.entries[c.Key]
	if !ok {
		s.stats.lost.Add(1)
		return nil, ErrClaimLost
	}
	e := el.Value.(*memEntry)
	if e.token != c.Token || !s.clock.Now().Before(e.expires) {
		s.stats.lost.Add(1)
		return nil, ErrClaimLost
	}
	return e, nil
}

// dropLocked removes one element from both the map and the list.
func (s *MemoryStore) dropLocked(el *list.Element) {
	e := el.Value.(*memEntry)
	delete(s.entries, e.key)
	s.lru.Remove(el)
}

// evictLocked brings the table back inside MaxEntries, oldest first.
//
// An expired entry is preferred over a live one, which is why the sweep looks
// at the back of the list first: the back is the least recently CLAIMED key,
// and dropping an expired one costs nothing. Only when the oldest entry is
// still live is the eviction a real loss, and that is the one counted in
// [Stats.Evicted].
func (s *MemoryStore) evictLocked() {
	now := s.clock.Now()
	for s.lru.Len() > s.max {
		el := s.lru.Back()
		if el == nil {
			return
		}
		e := el.Value.(*memEntry)
		s.dropLocked(el)
		if now.Before(e.expires) {
			s.stats.evicted.Add(1)
		} else {
			s.stats.expired.Add(1)
		}
	}
}

// Stats reports what this store has counted.
func (s *MemoryStore) Stats() Stats {
	s.mu.Lock()
	n := s.lru.Len()
	s.mu.Unlock()
	return Stats{
		Claims:     s.stats.claims.Load(),
		First:      s.stats.first.Load(),
		Duplicates: s.stats.duplicates.Load(),
		InFlight:   s.stats.inFlight.Load(),
		Commits:    s.stats.commits.Load(),
		Aborts:     s.stats.aborts.Load(),
		Evicted:    s.stats.evicted.Load(),
		Expired:    s.stats.expired.Load(),
		Lost:       s.stats.lost.Load(),
		Entries:    n,
	}
}

// Close implements [Store]. It is idempotent and drops the table.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.entries = nil
	s.lru.Init()
	return nil
}

// newToken returns an unguessable claim token.
//
// It is 16 random bytes rather than a counter because a counter is predictable
// across restarts: a store that restarts and re-issues token "7" would let a
// stale Commit from before the restart match a live claim after it, which is
// the one thing the token exists to stop.
func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// wallClock is the default time source.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

func (wallClock) NewTimer(d time.Duration) mq.Timer { return wallTimer{t: time.NewTimer(d)} }

type wallTimer struct{ t *time.Timer }

func (w wallTimer) C() <-chan time.Time        { return w.t.C }
func (w wallTimer) Stop() bool                 { return w.t.Stop() }
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

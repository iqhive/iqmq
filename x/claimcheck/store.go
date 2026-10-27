package claimcheck

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
)

// errStoreFull is the cause a store at its bound joins to
// [ErrStoreUnavailable], so the refusal says why without leaving the
// retryable class a full disk's ENOSPC already reports.
var errStoreFull = errors.New("iqmq/x/claimcheck: the store is full")

// Store is the external storage a payload is parked in. Implementations must
// be safe for concurrent use.
//
// There is no Delete. That is not an omission: the package documentation
// explains why the consumer never deletes a payload, and an interface with a
// Delete on it is an invitation to call it after a successful handler — which
// breaks at-least-once redelivery in a way that presents as a good message
// being dead-lettered. Deletion belongs to the store's own lifecycle policy,
// which is configured where the bucket is, not here.
type Store interface {
	// Name is the store's identity, recorded in every reference it backs.
	//
	// It must be stable across restarts and across every replica of a
	// service: it is what a consumer matches a reference against, and a
	// store whose name changed would make every reference already in the
	// broker read as [ErrWrongStore]. A bucket name, or a bucket and a
	// prefix.
	Name() string
	// Put stores payload and returns the key it can be fetched by.
	//
	// The key is the store's to choose. A store that derives it from the
	// payload's digest gets deduplication for nothing; one that uses a
	// random name gets no collisions. What it must not do is reuse a key
	// for different bytes, because a reference is only as good as the key's
	// stability.
	//
	// A store that is full or over a quota reports [ErrStoreUnavailable],
	// because room can come back; [ErrTooLarge] is for a payload no state
	// of the store could accept. The publisher's caller acts on the
	// difference: x/outbox retries the first for as long as it lasts, and
	// makes a row that keeps failing with the second poison.
	Put(ctx context.Context, payload []byte) (key string, err error)
	// Get fetches the payload for key, refusing anything over max bytes
	// with [ErrTooLarge].
	//
	// The three failures are distinct and an implementation MUST keep them
	// so: [ErrPayloadGone] for "the store answered and there is no such
	// object" (permanent), [ErrStoreUnavailable] for "could not ask"
	// (retryable), and [ErrTooLarge] for a payload over the limit. Reporting
	// the wrong one of the first two is the most consequential mistake a
	// store implementation can make — it either dead-letters good messages
	// or retries a lane for ever.
	Get(ctx context.Context, key string, max int64) ([]byte, error)
	// Close releases the store's own resources. It is idempotent, and it
	// does not close a client the caller lent it (the specification's ownership rule).
	Close() error
}

// MemoryStore is an in-process [Store].
//
// It is what makes this package testable with no bucket at all, and it is
// honest about what it is not: it is per process, it is not durable, and it
// holds every payload in memory for ever, because it has no lifecycle policy
// and this package's consumer never deletes. A program that ran it in
// production would run out of memory, which is why [MemoryOptions.MaxEntries]
// bounds it and why exceeding that bound is an error rather than an eviction:
// an evicted payload is a message that will be dead-lettered as
// [ErrPayloadGone], and silently converting a memory bound into poison
// messages is worse than refusing the publication.
type MemoryStore struct {
	name string
	max  int

	mu      sync.RWMutex
	objects map[string][]byte
	next    uint64
	closed  bool

	// unavailable makes every call report [ErrStoreUnavailable], so that a
	// test can exercise the policy branches without a network. It is
	// exported through [MemoryStore.SetUnavailable] rather than being a
	// test-only hook, because the three failure modes are the interesting
	// part of this package and a caller writing their own store wants to
	// test their handler against them too.
	unavailable bool
	// gone makes Get report [ErrPayloadGone] for every key, which is what a
	// store looks like after its lifecycle policy has expired the payloads.
	gone bool
}

// MemoryOptions configures a [MemoryStore].
type MemoryOptions struct {
	// Name is the store's identity. Empty takes "memory".
	Name string
	// MaxEntries bounds how many payloads the store holds. A Put past it is
	// [ErrStoreUnavailable] (retryable), as it is for [FileStore] at its
	// bound: a full store is not an oversized payload. Zero takes
	// [DefaultMaxEntries]; negative is refused rather than read as
	// unbounded.
	MaxEntries int
}

// DefaultMaxEntries bounds a [MemoryStore] whose MaxEntries is zero.
const DefaultMaxEntries = 1024

// NewMemoryStore returns an in-process store.
func NewMemoryStore(opt MemoryOptions) (*MemoryStore, error) {
	if opt.MaxEntries < 0 {
		return nil, ErrInvalidConfig
	}
	if opt.MaxEntries == 0 {
		opt.MaxEntries = DefaultMaxEntries
	}
	if opt.Name == "" {
		opt.Name = "memory"
	}
	if len(opt.Name) > MaxStoreLen {
		return nil, ErrInvalidConfig
	}
	return &MemoryStore{name: opt.Name, max: opt.MaxEntries, objects: map[string][]byte{}}, nil
}

// Name implements [Store].
func (s *MemoryStore) Name() string { return s.name }

// Put implements [Store].
func (s *MemoryStore) Put(_ context.Context, payload []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	if s.unavailable {
		return "", ErrStoreUnavailable
	}
	if len(s.objects) >= s.max {
		// A full store says nothing about this payload, so it is not
		// ErrTooLarge: that is CodeMessageTooLarge, which x/outbox reads as
		// the row's own fault and turns into ErrPoisonRow, so a good row
		// was dead-lettered because the store had no room. It is the
		// retryable class the real stores report for the same condition --
		// a FileStore whose sweep has not freed room yet, a disk's ENOSPC,
		// a bucket's quota -- so a program tested against this store meets
		// the answer it will meet in production.
		return "", errors.Join(ErrStoreUnavailable, errStoreFull)
	}
	s.next++
	key := "obj-" + itoa(s.next)
	// The payload is COPIED. The caller's slice is the message body, which
	// a publisher may reuse (the reuse modes), and a store that aliased
	// it would hand a later consumer whatever the buffer holds by then.
	s.objects[key] = slices.Clone(payload)
	return key, nil
}

// Get implements [Store].
func (s *MemoryStore) Get(_ context.Context, key string, maxBytes int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.unavailable {
		return nil, ErrStoreUnavailable
	}
	if s.gone {
		return nil, ErrPayloadGone
	}
	payload, ok := s.objects[key]
	if !ok {
		return nil, ErrPayloadGone
	}
	if int64(len(payload)) > maxBytes {
		return nil, ErrTooLarge
	}
	return slices.Clone(payload), nil
}

// discard drops the payload at key. Only PublishCheckIn calls it, for a
// payload whose publication definitely failed.
func (s *MemoryStore) discard(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

// Close implements [Store]. It is idempotent.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	// The objects are dropped: this store has no durability to pretend to,
	// and holding them after Close would only leak.
	s.objects = map[string][]byte{}
	return nil
}

// SetUnavailable makes every call report [ErrStoreUnavailable] (retryable)
// while on. It is how the publish-fails-closed and consume-retries branches
// are exercised without a network.
func (s *MemoryStore) SetUnavailable(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = on
}

// SetGone makes every Get report [ErrPayloadGone] (permanent) while on, which
// is what the store looks like once its lifecycle policy has expired the
// payloads. It is the branch that decides whether a redelivery after the TTL
// is retried for ever or dead-lettered.
func (s *MemoryStore) SetGone(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gone = on
}

// Overwrite replaces the payload at key with different bytes, without
// changing the key.
//
// It models the threat the digest exists for and which no other method can
// produce: a store somebody else can write to, a key collision, a stale
// replica, an operator restoring a backup over an object. It is exported for
// the same reason [MemoryStore.SetUnavailable] and [MemoryStore.SetGone] are
// -- a caller writing a real store against a real bucket wants to test their
// own handler against a substituted payload, and a test-only hook reachable
// from nowhere would leave them with no way to do it.
//
// A key the store does not hold is added, so that a caller can also stage the
// object a reference names without going through Put and getting a key of the
// store's choosing.
func (s *MemoryStore) Overwrite(key string, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.objects[key] = slices.Clone(payload)
}

// Len is how many payloads the store holds.
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.objects)
}

// Keys returns the keys the store holds, sorted, for a test's assertions.
func (s *MemoryStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Sorted(maps.Keys(s.objects))
}

// itoa is strconv.FormatUint under a shorter name, kept local so that the
// key format lives beside Put rather than being assembled at the call site.
func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

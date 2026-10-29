package middleware

import (
	"container/list"
	"context"
	"sync"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// IdempotencyStore records the keys this process has already handled
// (Dedup store, the best-effort idempotency).
//
// # It is a cache, not a guarantee
//
// Read that sentence again before building on this. A store answers "have I
// seen this key" from whatever it happens to be holding, and everything it
// holds is finite and local:
//
//   - LRUStore is bounded, so a key evicted by later traffic is not seen
//     again and its message is handled twice;
//   - it is per process, so a second consumer, a restart or a rebalance
//     starts with an empty store;
//   - Seen records the key before the handler runs, so a handler that fails
//     and is redelivered finds its own key already there — which is why
//     DedupConfig.ForgetOnFailure exists;
//   - a crash between Seen and the handler's side effect loses the work and
//     keeps the key;
//   - a redelivery that arrives while the first copy is still being handled
//     is postponed rather than acked, but only when both reach the same
//     Dedup: another process sees the key and acks it.
//
// The specification says idempotency here is best-effort and says iqmq delivers at
// least once. A caller who needs exactly-once semantics needs an idempotent
// handler — a transactional upsert keyed by the message id — and this
// middleware only ever makes that cheaper, never unnecessary. A durable store
// (x/idempotency's Redis store) moves the bound and changes none of this.
//
// Implementations must be safe for concurrent use.
type IdempotencyStore interface {
	// Seen records key and reports whether it was already present. It is
	// called once per delivery, before the handler.
	Seen(ctx context.Context, key string) (bool, error)
	// Forget removes key, so that a delivery whose handler failed can be
	// processed again when the broker redelivers it.
	Forget(ctx context.Context, key string)
}

// KeyFunc extracts the de-duplication key from a delivery. A key of "" means
// "this delivery cannot be de-duplicated" and the delivery passes through.
type KeyFunc func(d *mq.Delivery) string

// MessageIDKey is the default KeyFunc: the message's application identifier.
//
// It is empty unless the publisher set one or was configured with an
// IDGenerator, and a delivery with no id is therefore never
// de-duplicated. That is the honest default: a key this consumer invented —
// from the body, from the arrival order — would be different on the
// redelivery it is supposed to recognise.
func MessageIDKey(d *mq.Delivery) string { return d.ID }

// DedupConfig configures Dedup.
type DedupConfig struct {
	// Store is where keys are recorded. Nil selects a new LRUStore of
	// DefaultDedupCapacity.
	Store IdempotencyStore
	// Key extracts the key. Nil selects MessageIDKey.
	Key KeyFunc
	// ForgetOnFailure removes the key again when the handler fails, so that
	// the broker's redelivery is processed rather than suppressed. A handler
	// that settles its own delivery with a Nack or a Reject and returns nil
	// has failed it too; one that acks it has not.
	//
	// It is true by default -- the field is inverted for that reason: a
	// store that remembered a key whose handler failed would turn every
	// retry into a silent ack, which is the worst failure this middleware
	// can have. Set KeepOnFailure to keep the key regardless, which is right
	// only when the handler's side effect is known to have happened before
	// it reported the failure. A postponement from behind Dedup
	// (mq.Postpone, as RateLimit and CircuitBreaker return) forgets the key
	// either way, because the handler never ran.
	KeepOnFailure bool
	// OnDuplicate, when non-nil, is called for a delivery whose key was
	// already present, before it is settled.
	OnDuplicate func(ctx context.Context, d *mq.Delivery, key string)
}

// DefaultDedupCapacity is the size of the store DedupConfig builds when it is
// given none.
const DefaultDedupCapacity = 1 << 14

// Dedup suppresses a delivery whose key this process has already handled
//
// A duplicate is ACKED and the handler is not called — the same settlement
// rule as Filter's, and for the same reason: a middleware that short-circuits
// the handler has taken the handler's place, and one that returned without
// settling would strand the lease. A delivery whose key is still in the
// handler is postponed instead (mq.Postpone, after the run's configured
// backoff), so the runner offers it again later without counting an attempt:
// it has not been handled, and a refusal that spent attempts halted the
// runner with a *PoisonError whenever the first copy took longer than the
// backoff the attempts added up to.
//
// Read IdempotencyStore's documentation before relying on this. It is a
// cache: a miss means "handle it", a hit means "probably already handled",
// and neither is a guarantee.
func Dedup(cfg DedupConfig) mq.Middleware {
	store := cfg.Store
	if store == nil {
		store = newLRUStore(DefaultDedupCapacity)
	}
	key := cfg.Key
	if key == nil {
		key = MessageIDKey
	}
	forget := !cfg.KeepOnFailure
	busy := &inFlight{keys: make(map[string]struct{})}
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			k := key(d)
			if k == "" {
				return next.Handle(ctx, d)
			}
			// A redelivery that arrives while the first copy is still in the
			// handler is not a duplicate: the work has not happened yet, and
			// acking it would lose the message if the first copy then failed.
			// It is postponed instead, which is not a failed attempt.
			if !busy.claim(k) {
				return mq.Postpone(errInFlight, 0)
			}
			defer busy.release(k)
			seen, err := store.Seen(ctx, k)
			if err != nil {
				// The store failed. Handling the message is the safe answer:
				// iqmq delivers at least once, so a duplicate is expected and
				// survivable, where dropping the message is not.
				return next.Handle(ctx, d)
			}
			if seen {
				if cfg.OnDuplicate != nil {
					cfg.OnDuplicate(ctx, d, k)
				}
				return ackAndStop(d)
			}
			// A panicking handler has failed too, and the runner recovers the
			// panic above this middleware, so the key is forgotten on the way
			// out rather than after a return that never comes.
			returned := false
			defer func() {
				if !returned && forget {
					store.Forget(ctx, k)
				}
			}()
			herr := next.Handle(ctx, d)
			returned = true
			switch {
			case postponed(herr):
				// Something behind Dedup (a rate limit, an open circuit)
				// turned the delivery away before the handler ran, so there
				// is no side effect for KeepOnFailure to protect. A kept key
				// made the next offer a duplicate, acked unhandled.
				store.Forget(ctx, k)
			case (herr != nil || settledWithoutAck(d)) && forget:
				// A handler that nacked or rejected its own delivery and
				// returned nil has failed it as surely as one that returned
				// an error: it asked for the message again, and a remembered
				// key would ack that redelivery as a duplicate, unprocessed.
				store.Forget(ctx, k)
			}
			return herr
		})
	}
}

// settledWithoutAck reports whether the handler settled d itself with
// something other than an Ack.
func settledWithoutAck(d *mq.Delivery) bool {
	return d.Settled() && !d.Acked()
}

// errInFlight is what Dedup postpones a delivery with when its key is still
// in this middleware's handler.
var errInFlight error = errcode.New(errcode.Unavailable, "iqmq/middleware: a delivery with this key is still being handled")

// inFlight is the set of keys whose handler is running in one Dedup.
type inFlight struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

// claim marks key in flight, and reports false when it already was.
func (f *inFlight) claim(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.keys[key]; ok {
		return false
	}
	f.keys[key] = struct{}{}
	return true
}

// release clears key.
func (f *inFlight) release(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, key)
}

// LRUStore is the bounded in-memory IdempotencyStore ships: a map and a
// recency list behind one mutex, holding at most Capacity keys and evicting
// the least recently seen.
//
// It is a cache. See IdempotencyStore.
//
// The zero value is not usable; NewLRUStore returns a ready one. It is safe
// for concurrent use.
type LRUStore struct {
	mu    sync.Mutex
	items map[string]*list.Element
	order *list.List
	cap   int
}

// NewLRUStore returns a store holding at most capacity keys. It fails with
// ErrInvalidConfig on a capacity below 1: a store that holds nothing reports every delivery as new,
// which is a de-duplicator that does not de-duplicate.
func NewLRUStore(capacity int) (*LRUStore, error) {
	if capacity < 1 {
		return nil, ErrInvalidConfig
	}
	return newLRUStore(capacity), nil
}

// newLRUStore builds a store for a capacity already known to be at least 1.
func newLRUStore(capacity int) *LRUStore {
	return &LRUStore{
		items: make(map[string]*list.Element, capacity),
		order: list.New(),
		cap:   capacity,
	}
}

// Seen records key and reports whether it was already present.
func (s *LRUStore) Seen(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		s.order.MoveToFront(e)
		return true, nil
	}
	s.items[key] = s.order.PushFront(key)
	if s.order.Len() > s.cap {
		back := s.order.Back()
		if back != nil {
			s.order.Remove(back)
			k, _ := back.Value.(string)
			delete(s.items, k)
		}
	}
	return false, nil
}

// Forget removes key.
func (s *LRUStore) Forget(_ context.Context, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.items[key]; ok {
		s.order.Remove(e)
		delete(s.items, key)
	}
}

// Len returns the number of keys held. It is for tests and diagnostics.
func (s *LRUStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// Cap returns the bound.
func (s *LRUStore) Cap() int { return s.cap }

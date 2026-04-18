package middleware

import (
	"container/list"
	"context"
	"sync"

	mq "bitbucket.org/iqhive/iqmq"
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
//     keeps the key.
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
	// the broker's redelivery is processed rather than suppressed.
	//
	// It is true by default -- the field is inverted for that reason: a
	// store that remembered a key whose handler failed would turn every
	// retry into a silent ack, which is the worst failure this middleware
	// can have. Set KeepOnFailure to keep the key regardless, which is right
	// only when the handler's side effect is known to have happened before
	// it reported the failure.
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
// settling would strand the lease.
//
// Read IdempotencyStore's documentation before relying on this. It is a
// cache: a miss means "handle it", a hit means "probably already handled",
// and neither is a guarantee.
func Dedup(cfg DedupConfig) mq.Middleware {
	store := cfg.Store
	if store == nil {
		store = NewLRUStore(DefaultDedupCapacity)
	}
	key := cfg.Key
	if key == nil {
		key = MessageIDKey
	}
	forget := !cfg.KeepOnFailure
	return func(next mq.Handler) mq.Handler {
		return mq.HandlerFunc(func(ctx context.Context, d *mq.Delivery) error {
			k := key(d)
			if k == "" {
				return next.Handle(ctx, d)
			}
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
			herr := next.Handle(ctx, d)
			if herr != nil && forget {
				store.Forget(ctx, k)
			}
			return herr
		})
	}
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

// NewLRUStore returns a store holding at most capacity keys. It panics on a
// capacity below 1: a store that holds nothing reports every delivery as new,
// which is a de-duplicator that does not de-duplicate.
func NewLRUStore(capacity int) *LRUStore {
	if capacity < 1 {
		panic(ErrInvalidConfig)
	}
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

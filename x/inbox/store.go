package inbox

import (
	"context"
	"sync"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Store records processed keys. Implementations must be safe for concurrent
// use.
type Store interface {
	// Once records key and runs fn IN THE SAME TRANSACTION, and reports
	// whether fn ran.
	//
	// The contract is exactly:
	//
	//   - key not present: fn is called with a context carrying the
	//     transaction; if fn returns nil the transaction commits and Once
	//     reports (true, nil); if fn returns an error the transaction rolls
	//     back — so the key is NOT recorded — and Once reports (false, err)
	//     with fn's error. A handler that failed must be retryable, which
	//     means its record must not survive.
	//   - key already present: fn is NOT called and Once reports
	//     (false, nil). The caller acks.
	//   - the store could not be reached: fn is NOT called and Once reports
	//     (false, err). The caller's [Config.OnUnavailable] policy decides.
	//
	// The three cases are distinguished by the pair and not by the error
	// alone, because "did not run because it is a duplicate" and "did not
	// run because the database is down" are the same (false, …) shape with
	// opposite correct responses.
	// fn MUST NOT call Once again for its OWN key. A real SQL store cannot
	// serve a second transaction that inserts the uncommitted row without
	// blocking it on the first, and [MemoryStore] makes a second Once on a
	// key wait for the first to commit or roll back, so a re-entrant call
	// on the same key deadlocks. This constraint is inherent to the pattern
	// rather than an implementation limit: the point of the transaction is
	// that nobody else sees the record until it commits, and "nobody else"
	// includes the caller's own second call.
	//
	// A fn that PANICS has not committed: an implementation rolls the
	// record back and lets the panic continue, exactly as a database rolls
	// back a transaction nobody committed.
	Once(ctx context.Context, key string, fn func(context.Context) error) (ran bool, err error)
	// Close releases the store's own resources. It is idempotent, and it
	// does not close a database handle the caller lent it.
	Close() error
}

// The errors this package reports.
var (
	// ErrNoStore (CodeInvalidConfig) reports a nil [Config.Store].
	ErrNoStore error = errcode.New(errcode.InvalidConfig, "iqmq/x/inbox: no store")
	// ErrNoKeyFunc (CodeInvalidConfig) reports a nil [Config.Key].
	ErrNoKeyFunc error = errcode.New(errcode.InvalidConfig, "iqmq/x/inbox: no key function")
	// ErrEmptyKey (CodeInvalidArgument) reports an empty key.
	//
	// Refused rather than treated as one shared key, which is what a store
	// that accepted it would be: every keyless message a duplicate of the
	// first, silently acked.
	ErrEmptyKey error = errcode.New(errcode.InvalidArgument, "iqmq/x/inbox: the key is empty")
	// ErrMissingKey (CodeInvalidArgument) reports a delivery
	// [Config.Key] found no key on, under [MissingKeyFail].
	ErrMissingKey error = errcode.New(errcode.InvalidArgument, "iqmq/x/inbox: the delivery carries no key")
	// ErrFull (CodeResourceExhausted) reports a [MemoryStore] at its bound.
	//
	// Refused rather than evicted: an evicted key is a message that will be
	// processed twice, and silently converting a memory bound into a
	// duplicate is the opposite of what this package is for. A real inbox
	// table is pruned by a job that deletes rows older than the broker's
	// retention, which is a deletion the caller can reason about.
	ErrFull error = errcode.New(errcode.ResourceExhausted, "iqmq/x/inbox: the store is full")
	// ErrClosed (CodeClosed) reports a call on a closed store.
	ErrClosed error = errcode.New(errcode.Closed, "iqmq/x/inbox: the store is closed")
)

// DefaultMaxEntries bounds a [MemoryStore] whose MaxEntries is zero.
const DefaultMaxEntries = 100_000

// MemoryOptions configures a [MemoryStore].
type MemoryOptions struct {
	// MaxEntries bounds the recorded keys. Zero takes
	// [DefaultMaxEntries]; negative is refused rather than read as
	// unbounded.
	MaxEntries int
}

// MemoryStore is an in-process [Store].
//
// Its "transaction" is a per-key wait, and that is the honest description: a
// database with a unique index on the inbox's key column makes a second
// transaction inserting the SAME key wait until the first commits or rolls
// back, and lets a transaction inserting a DIFFERENT key straight through.
// This store does both. What it does NOT model is the SQL, the isolation level
// or the durability: a program that used it in production would get
// per-process deduplication that a restart forgets, with none of the
// guarantee the pattern is for.
//
// # Concurrency
//
// Deliveries with different keys run their handlers concurrently. A second
// Once on a key whose handler is still running waits -- for the first to
// commit, when it reports a duplicate without calling its fn, or to roll back,
// when it takes the key and runs its own fn -- and gives up with the context's
// error if its context ends first, which the middleware treats as a store that
// could not answer. It used to hold one mutex across every fn, which
// serialised the whole process behind its slowest handler and hid from any
// test written against it the interleaving production is guaranteed to have.
//
// **fn must not call Once again for its own key**, or it waits for itself.
// That is not a bug a finer lock could fix: a re-entrant insert of the same
// key is asking for a record that has not committed yet, which a real SQL
// store answers by blocking the caller on itself. Calls on OTHER keys, and
// [MemoryStore.Has] and [MemoryStore.Len], are safe from inside fn.
//
// A fn that panics is rolled back: its key is not recorded, anybody waiting on
// the key is released to run their own fn, and the panic continues to the
// caller. A runner that recovers the panic and redelivers the message
// therefore runs the handler again rather than acking a duplicate of work that
// never completed.
//
// # When this is not the store you want
//
// It is for tests, examples and a single process whose side effects are also
// in process. A consumer using the inbox pattern in anger implements [Store]
// over the database its handler writes to -- the INSERT-then-work transaction
// in the package documentation -- because that is the only arrangement in
// which the record and the effect commit together. A consumer that wants
// in-process duplicate suppression without a database wants
// [github.com/iqhive/iqmq/x/idempotency]'s memory store, a bounded LRU with a
// window per claim that evicts, and counts what it evicted, rather than
// refusing at its bound.
type MemoryStore struct {
	max int

	mu sync.Mutex
	// keys holds every key the store knows. A nil channel is a committed
	// record; a non-nil one is a transaction in flight, closed when it
	// commits or rolls back.
	keys map[string]chan struct{}
	// committed counts the nil entries of keys, so that Len does not scan.
	committed int
	closed    bool
}

// NewMemoryStore returns an in-process store.
func NewMemoryStore(opt MemoryOptions) (*MemoryStore, error) {
	if opt.MaxEntries < 0 {
		return nil, ErrNoStore
	}
	if opt.MaxEntries == 0 {
		opt.MaxEntries = DefaultMaxEntries
	}
	return &MemoryStore{max: opt.MaxEntries, keys: make(map[string]chan struct{}, 64)}, nil
}

// Once implements [Store].
//
// A duplicate -- the path a replayed topic takes for every message -- takes
// the lock once, finds a committed record and allocates nothing. A first
// delivery allocates the channel a concurrent duplicate waits on.
func (s *MemoryStore) Once(ctx context.Context, key string, fn func(context.Context) error) (bool, error) {
	if key == "" {
		return false, ErrEmptyKey
	}
	s.mu.Lock()
	for {
		if s.closed {
			s.mu.Unlock()
			return false, ErrClosed
		}
		inFlight, known := s.keys[key]
		if !known {
			break
		}
		if inFlight == nil {
			// Committed: the work is done.
			s.mu.Unlock()
			return false, nil
		}
		// Another worker holds the key. Wait for its transaction to end
		// and look again: it may have committed (a duplicate) or rolled
		// back (the key is free again).
		s.mu.Unlock()
		select {
		case <-inFlight:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		s.mu.Lock()
	}
	if len(s.keys) >= s.max {
		s.mu.Unlock()
		return false, ErrFull
	}
	// The "transaction": the key is taken BEFORE fn runs, which models the
	// INSERT-then-work order and is the order that matters -- a store that
	// ran fn first and recorded afterwards would let a second worker in
	// while the first was still working.
	done := make(chan struct{})
	s.keys[key] = done
	s.mu.Unlock()

	committed := false
	defer func() {
		// Runs on return AND on a panic out of fn: a transaction nobody
		// committed is rolled back, whatever ended it.
		s.mu.Lock()
		if committed {
			s.keys[key] = nil
			s.committed++
		} else {
			delete(s.keys, key)
		}
		close(done)
		s.mu.Unlock()
	}()
	if err := fn(ctx); err != nil {
		return false, err
	}
	committed = true
	return true, nil
}

// Close implements [Store]. It is idempotent.
//
// It refuses every later Once with [ErrClosed] -- including one already
// waiting on a key -- and returns once every transaction that was in flight
// has committed or rolled back, so a caller that closes the store knows no fn
// is still running inside it. It must not be called from inside fn, which
// would wait for itself.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	s.closed = true
	var inFlight []chan struct{}
	for _, ch := range s.keys {
		if ch != nil {
			inFlight = append(inFlight, ch)
		}
	}
	s.mu.Unlock()
	for _, ch := range inFlight {
		<-ch
	}
	return nil
}

// Len is how many keys the store has recorded: committed, and not counting a
// transaction still in flight.
func (s *MemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// Has reports whether key has been recorded and committed. A key whose
// transaction is still in flight is not recorded yet -- the view another
// transaction gets of an uncommitted row -- and Has does not wait for it.
func (s *MemoryStore) Has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	inFlight, known := s.keys[key]
	return known && inFlight == nil
}

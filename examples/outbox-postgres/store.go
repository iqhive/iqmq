package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"
)

// lockFor is how long a claimed outbox row is invisible to another relay.
//
// It is short because this example really waits for it to lapse -- that is how
// the republication is demonstrated -- and it is not zero because a lock that
// never holds is not a lock. A service sets it to rather more than its longest
// publish timeout: a lock that lapses while the publication is still in flight
// is a duplicate, which is survivable, and a lock that never lapses is a row
// nobody will ever send, which is not.
const lockFor = 150 * time.Millisecond

// pastTheLock is how long to wait for a lock to lapse. It is longer than
// lockFor with room for a loaded host.
const pastTheLock = 400 * time.Millisecond

// Outgoing is what a business transaction appends to the outbox.
type Outgoing struct {
	// Key is the idempotency key. It is the BUSINESS operation's identifier
	// and not a per-publication one: the whole point is that a row
	// republished after a relay died carries the same key, so the consumer
	// can tell it is the same operation (applied).
	Key string
	// Body is the payload.
	Body string
}

// Row is one claimed outbox row.
type Row struct {
	// ID is the row's primary key, which is what MarkSent takes.
	ID int64
	// Key and Body are what was appended.
	Key  string
	Body string
	// Attempts is how many times this row has been claimed, including this
	// one. It is the number that tells a relay a row is poison rather than
	// merely unlucky.
	Attempts int
}

// Store is the persistence an outbox needs, as the smallest interface that
// admits both a real database and an in-process stand-in.
//
// Every method takes a context and every one may fail: the whole subject of an
// outbox is what happens when one of them does.
type Store interface {
	// Name identifies the implementation in the example's output.
	Name(ctx context.Context) string
	// Migrate creates the two tables. It is idempotent.
	Migrate(ctx context.Context) error
	// Place writes one order AND its outbox rows in ONE transaction.
	//
	// When rollback is true the transaction is rolled back after both writes,
	// which is the atomicity demonstration: neither the order nor the outbox
	// row survives, and that is the property a message published from inside
	// a transaction cannot have.
	Place(ctx context.Context, order string, out []Outgoing, rollback bool) error
	// Claim takes up to n rows that are unsent and unlocked, locks them for
	// lockFor, and returns them in id order.
	//
	// Two relays running at once must get DISJOINT rows, which on Postgres is
	// `FOR UPDATE SKIP LOCKED` inside the same UPDATE that sets the lock.
	Claim(ctx context.Context, n int) ([]Row, error)
	// MarkSent marks rows as sent. A row that was already sent is not an
	// error: the relay is at-least-once in this direction too.
	MarkSent(ctx context.Context, ids []int64) error
	// Unsent is how many rows have not been marked sent.
	Unsent(ctx context.Context) (int, error)
	// Orders lists the business table, in insertion order.
	Orders(ctx context.Context) ([]string, error)
	// Close releases the store's resources. It is idempotent.
	Close() error
}

// ErrStoreClosed reports a call on a closed store.
var ErrStoreClosed = errors.New("outbox: the store is closed")

// memStore is the in-process Store.
//
// It is NOT a database and it models none of one: no durability, no isolation
// levels, no second connection. What it does model is the three properties the
// relay's correctness rests on -- a business write and an outbox append that
// either both happen or neither does, a claim that two relays cannot both win,
// and a lock that lapses -- so that the whole protocol above it is exercised
// with no container at all. What it does NOT check is the SQL, and the example
// says so.
type memStore struct {
	mu     sync.Mutex
	orders []string
	rows   []*memRow
	nextID int64
	closed bool
}

type memRow struct {
	id          int64
	key, body   string
	attempts    int
	sent        bool
	lockedUntil time.Time
}

func newMemStore() *memStore { return &memStore{} }

func (s *memStore) Name(context.Context) string { return "in-memory (no SQL)" }

func (s *memStore) Migrate(context.Context) error { return s.alive() }

func (s *memStore) Place(_ context.Context, order string, out []Outgoing, rollback bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	// The "transaction": both writes are staged and applied together, or
	// neither is. Under one mutex this is trivially atomic, which is the
	// point -- the property is easy in process and needs a database
	// transaction across a table and a queue table.
	orders := append(slices.Clone(s.orders), order)
	rows := slices.Clone(s.rows)
	next := s.nextID
	for _, o := range out {
		next++
		rows = append(rows, &memRow{id: next, key: o.Key, body: o.Body})
	}
	if rollback {
		return errRolledBack
	}
	s.orders, s.rows, s.nextID = orders, rows, next
	return nil
}

// errRolledBack is what Place reports for a transaction it rolled back. It is
// the example asking for a failure, not a store defect, so it is a named error
// the caller expects rather than a generic one.
var errRolledBack = errors.New("outbox: the business transaction failed, so neither the order nor its outbox row was written")

func (s *memStore) Claim(_ context.Context, n int) ([]Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	now := time.Now()
	out := make([]Row, 0, n)
	for _, r := range s.rows {
		if len(out) == n {
			break
		}
		if r.sent || now.Before(r.lockedUntil) {
			continue
		}
		r.lockedUntil = now.Add(lockFor)
		r.attempts++
		out = append(out, Row{ID: r.id, Key: r.key, Body: r.body, Attempts: r.attempts})
	}
	return out, nil
}

func (s *memStore) MarkSent(_ context.Context, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	for _, r := range s.rows {
		if slices.Contains(ids, r.id) {
			r.sent = true
		}
	}
	return nil
}

func (s *memStore) Unsent(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrStoreClosed
	}
	n := 0
	for _, r := range s.rows {
		if !r.sent {
			n++
		}
	}
	return n, nil
}

func (s *memStore) Orders(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	return slices.Clone(s.orders), nil
}

func (s *memStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *memStore) alive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}

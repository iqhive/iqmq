package outbox

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DefaultLockFor is how long a claimed row is invisible to another relay.
//
// It is not zero, because a lock that never holds is not a lock and two
// relays would publish every row twice. It is not long either: the lock is
// what turns a CRASHED relay into a duplicate rather than a permanently
// stranded row, so it has to lapse, and it should be set to rather more than
// the longest publish timeout. A lock that lapses while the publication is
// still in flight is a duplicate, which is survivable; a lock that never
// lapses is a row nobody will ever send, which is not.
const DefaultLockFor = 30 * time.Second

// MemoryOptions configures a [MemoryStore].
type MemoryOptions struct {
	// LockFor is the claim lock's duration. Zero takes [DefaultLockFor];
	// negative is refused.
	LockFor time.Duration
	// Clock is the time source. Nil takes the wall clock. A test that wants
	// to watch a lock lapse without sleeping passes a fake.
	Clock mq.Clock
}

// MemoryStore is an in-process [Store].
//
// It is NOT a database and models none of one: no durability, no isolation
// levels, no second connection, no SQL. What it does model is the three
// properties a relay's correctness rests on — a claim two relays cannot both
// win, a lock that lapses, and a mark that is idempotent — so that the whole
// protocol above it is exercised with no container at all. What it does NOT
// check is the SQL, which `examples/outbox-postgres` exercises against a real
// Postgres.
//
// [MemoryStore.Append] exists here and deliberately NOT on the [Store]
// interface: this store has no transaction to append inside, so its Append is
// a plain method for tests and examples rather than part of the contract a
// real store implements. See the package documentation.
type MemoryStore struct {
	lockFor time.Duration
	clock   mq.Clock

	mu     sync.Mutex
	rows   []*memRow
	nextID int
	closed bool

	// unavailable makes every method report [ErrClosed]'s retryable sibling,
	// so a test can exercise a relay against a store that is down. It is a
	// method rather than a field so that it can be set while a relay runs.
	unavailable bool
}

type memRow struct {
	row         Row
	sent        bool
	failed      error
	dead        bool
	lockedUntil time.Time
}

// NewMemoryStore returns an in-process store.
func NewMemoryStore(opt MemoryOptions) (*MemoryStore, error) {
	if opt.LockFor < 0 {
		return nil, ErrInvalidConfig
	}
	if opt.LockFor == 0 {
		opt.LockFor = DefaultLockFor
	}
	clock := opt.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &MemoryStore{lockFor: opt.LockFor, clock: clock}, nil
}

// Append adds a row and returns its id.
//
// The row's Body, Key and Headers are COPIED. The caller is appending inside
// what stands in for a transaction, and a store that aliased the caller's
// slices would publish whatever those buffers held by the time the relay got
// to them.
func (s *MemoryStore) Append(row Row) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrClosed
	}
	s.nextID++
	row.ID = "row-" + itoa(s.nextID)
	row.Body = slices.Clone(row.Body)
	row.Key = slices.Clone(row.Key)
	row.Headers = row.Headers.Clone()
	row.Attempts = 0
	s.rows = append(s.rows, &memRow{row: row})
	return row.ID, nil
}

// Claim implements [Store]. Rows come back in append order.
func (s *MemoryStore) Claim(_ context.Context, n int) ([]Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.unavailable {
		return nil, errUnavailable
	}
	now := s.clock.Now()
	out := make([]Row, 0, n)
	for _, r := range s.rows {
		if len(out) == n {
			break
		}
		if r.sent || r.dead || now.Before(r.lockedUntil) {
			continue
		}
		r.lockedUntil = now.Add(s.lockFor)
		r.row.Attempts++
		// The returned Row is a copy, so a relay that mutated it could not
		// reach the store's bookkeeping.
		out = append(out, r.row)
	}
	return out, nil
}

// MarkSent implements [Store]. A row already sent is not an error.
func (s *MemoryStore) MarkSent(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.unavailable {
		return errUnavailable
	}
	for _, r := range s.rows {
		if slices.Contains(ids, r.row.ID) {
			r.sent = true
		}
	}
	return nil
}

// Release implements [Store]: the lock is dropped so another pass takes the
// row immediately rather than waiting for the lapse.
func (s *MemoryStore) Release(_ context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.unavailable {
		return errUnavailable
	}
	for _, r := range s.rows {
		if slices.Contains(ids, r.row.ID) {
			r.lockedUntil = time.Time{}
		}
	}
	return nil
}

// Fail implements [Store]: the reason is recorded against the row, which is
// what a real store would put in a dead-letter column, and a poison row
// leaves rotation for good.
func (s *MemoryStore) Fail(_ context.Context, id string, reason error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	for _, r := range s.rows {
		if r.row.ID == id {
			r.failed = reason
			r.dead = r.dead || errors.Is(reason, ErrPoisonRow)
		}
	}
	return nil
}

// Close implements [Store]. It is idempotent.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// SetUnavailable makes Claim, MarkSent and Release report a retryable
// failure while on, so a relay can be run against a store that is down.
func (s *MemoryStore) SetUnavailable(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = on
}

// Unsent is how many rows have not been marked sent.
func (s *MemoryStore) Unsent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.rows {
		if !r.sent {
			n++
		}
	}
	return n
}

// Sent returns the ids of the rows marked sent, in append order.
func (s *MemoryStore) Sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.rows {
		if r.sent {
			out = append(out, r.row.ID)
		}
	}
	return out
}

// Attempts is how many times a row has been claimed.
func (s *MemoryStore) Attempts(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.row.ID == id {
			return r.row.Attempts
		}
	}
	return -1
}

// FailureOf is the reason last recorded against a row by [MemoryStore.Fail].
func (s *MemoryStore) FailureOf(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.rows {
		if r.row.ID == id {
			return r.failed
		}
	}
	return nil
}

// errUnavailable is what a store that is "down" reports. CodeUnavailable so
// that a relay driven by [Relay.Run] reports it and stops rather than looping
// on a dead database, which is what the Run documentation says.
var errUnavailable error = errcode.New(errcode.Unavailable, "iqmq/x/outbox: the store is unavailable")

// itoa formats a small positive int without strconv, kept local so the id
// format lives beside Append.
func itoa(n int) string {
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

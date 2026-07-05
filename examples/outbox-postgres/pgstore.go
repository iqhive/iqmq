package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The schema. Two tables and one partial index, which is the whole of an
// outbox.
//
// It is spelled out here rather than assembled so that running it by hand
// against a database reproduces exactly what this program creates. `IF NOT
// EXISTS` everywhere makes Migrate idempotent, which the rule for an
// Ensure operation asks of topology and is just as necessary for a schema a
// service applies at start-up.
const schema = `
CREATE TABLE IF NOT EXISTS orders (
  id          bigserial PRIMARY KEY,
  reference   text NOT NULL,
  placed_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
  id           bigserial PRIMARY KEY,
  idem_key     text NOT NULL,
  body         text NOT NULL,
  attempts     int NOT NULL DEFAULT 0,
  locked_until timestamptz,
  sent_at      timestamptz
);
-- The relay reads only unsent rows, and a partial index is what keeps that
-- read cheap once the table has a year of sent rows in it. Without it the
-- relay's query degrades from an index scan over the backlog to a scan of
-- everything ever published, which is the outbox pattern's classic
-- production failure.
CREATE INDEX IF NOT EXISTS outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
`

// claimSQL is the relay's claim, and the reason this example needs Postgres.
//
// It is ONE statement, and every part of it is load-bearing:
//
//   - `FOR UPDATE SKIP LOCKED` is what lets two relays run at once. Without
//     SKIP LOCKED the second relay BLOCKS on the first's rows and then claims
//     them; without FOR UPDATE both relays read the same rows and both
//     publish them.
//   - the UPDATE wraps the SELECT so that taking the lock and marking it are
//     the same statement. A SELECT ... FOR UPDATE that returned to Go and
//     then issued an UPDATE would hold the row lock only for as long as the
//     transaction, and a relay that committed before publishing would have
//     released it.
//   - `locked_until` rather than a held transaction, because the publication
//     happens between the claim and the mark: holding a database transaction
//     open across a network publish is how an outbox comes to exhaust its
//     connection pool.
//   - `ORDER BY id` so that a run's output is the same every time, and
//     because an outbox that publishes out of order defeats the purpose of
//     having written the rows in order.
const claimSQL = `
UPDATE outbox SET locked_until = now() + $2::interval, attempts = attempts + 1
WHERE id IN (
  SELECT id FROM outbox
  WHERE sent_at IS NULL AND (locked_until IS NULL OR locked_until < now())
  ORDER BY id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, idem_key, body, attempts
`

// pgStore is the Postgres Store.
type pgStore struct {
	pool *pgxpool.Pool
}

// openPG dials Postgres and returns a store.
//
// The DSN is the caller's: this program neither reads a credential file nor
// guesses a password, and the address form used by the rest of this repository
// (host:port) is turned into a DSN by dsnFor so that the example takes the same
// IQMQ_<BROKER>_ADDR shape every other one does.
func openPG(ctx context.Context, dsn string) (*pgStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("outbox: connecting to Postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("outbox: pinging Postgres: %w", err)
	}
	return &pgStore{pool: pool}, nil
}

// dsnFor turns a host:port (or a full DSN) into a libpq connection string.
//
// The credentials are the pinned container's defaults and are not a secret;
// a deployment passes a whole DSN instead, which is what the `://` test
// detects.
func dsnFor(addr string) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	return "postgres://iqmq:iqmq@" + addr + "/iqmq?sslmode=disable"
}

func (s *pgStore) Name(ctx context.Context) string {
	var v string
	if err := s.pool.QueryRow(ctx, "SHOW server_version").Scan(&v); err != nil {
		return "postgres (version unknown)"
	}
	// Only the major version, because the printed output has to be the same
	// on every run and a patch release must not change it.
	major, _, _ := strings.Cut(v, ".")
	return "postgres " + major
}

func (s *pgStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("outbox: applying the schema: %w", err)
	}
	return nil
}

// Place is the transaction the whole pattern exists for.
//
// The order and its outbox rows are written by one transaction, so a crash
// between them is impossible. A service that published to the broker HERE --
// inside the transaction -- would have the opposite problem: the publication
// cannot be rolled back, so a transaction that failed afterwards would leave a
// message about an order that does not exist. That is the defect this pattern
// removes, and it is the same shape as the specification: an event raised
// from inside an operation is not evidence that the operation's after-effects
// have landed.
func (s *pgStore) Place(ctx context.Context, order string, out []Outgoing, rollback bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("outbox: beginning the transaction: %w", err)
	}
	// Rollback on every path that is not an explicit Commit. pgx makes a
	// Rollback after a Commit a no-op, so this is the whole error handling.
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `INSERT INTO orders (reference) VALUES ($1)`, order); err != nil {
		return fmt.Errorf("outbox: inserting the order: %w", err)
	}
	for _, o := range out {
		if _, err := tx.Exec(ctx,
			`INSERT INTO outbox (idem_key, body) VALUES ($1, $2)`, o.Key, o.Body); err != nil {
			return fmt.Errorf("outbox: appending to the outbox: %w", err)
		}
	}
	if rollback {
		// The deferred Rollback does the work. Returning before Commit is
		// the whole mechanism, and it is what a failed constraint, a
		// serialisation error or a panic would do for real.
		return errRolledBack
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("outbox: committing: %w", err)
	}
	return nil
}

func (s *pgStore) Claim(ctx context.Context, n int) ([]Row, error) {
	// The interval is passed as a string because `now() + $2::interval`
	// needs one; pgx would send a time.Duration as an int8 and the cast
	// would fail.
	rows, err := s.pool.Query(ctx, claimSQL, n, intervalOf(lockFor))
	if err != nil {
		return nil, fmt.Errorf("outbox: claiming rows: %w", err)
	}
	defer rows.Close()
	out := []Row{}
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Key, &r.Body, &r.Attempts); err != nil {
			return nil, fmt.Errorf("outbox: scanning a claimed row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: reading the claimed rows: %w", err)
	}
	// The UPDATE's RETURNING is not ordered by the subquery's ORDER BY --
	// Postgres makes no such promise -- so the order is imposed here. An
	// outbox that publishes out of order defeats the purpose of having
	// written the rows in order.
	slicesSortByID(out)
	return out, nil
}

// intervalOf renders a duration as a Postgres interval literal.
func intervalOf(d time.Duration) string {
	return fmt.Sprintf("%d milliseconds", d.Milliseconds())
}

func (s *pgStore) MarkSent(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE outbox SET sent_at = now(), locked_until = NULL WHERE id = ANY($1)`, ids); err != nil {
		return fmt.Errorf("outbox: marking rows sent: %w", err)
	}
	return nil
}

func (s *pgStore) Unsent(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("outbox: counting unsent rows: %w", err)
	}
	return n, nil
}

func (s *pgStore) Orders(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT reference FROM orders ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("outbox: reading the orders: %w", err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, fmt.Errorf("outbox: scanning an order: %w", err)
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// Close releases the pool. It is idempotent: pgxpool.Close is.
func (s *pgStore) Close() error {
	s.pool.Close()
	return nil
}

// Truncate empties both tables, so that a run against a database another run
// has used starts from nothing.
//
// It is exported to the test rather than run by the program: the specification's
// destructive operations are explicit, and a program that truncated a table it
// found would be a very bad example.
func (s *pgStore) Truncate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `TRUNCATE orders, outbox`); err != nil {
		return fmt.Errorf("outbox: truncating: %w", err)
	}
	return nil
}

// slicesSortByID sorts rows by id. It is a named function rather than an
// inline closure so that the reason above stays attached to it.
func slicesSortByID(rows []Row) {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && rows[j-1].ID > rows[j].ID; j-- {
			rows[j-1], rows[j] = rows[j], rows[j-1]
		}
	}
}

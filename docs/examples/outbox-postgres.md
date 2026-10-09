# outbox-postgres

[`examples/outbox-postgres`](https://github.com/iqhive/iqmq/tree/main/examples/outbox-postgres)
— the transactional outbox: a business write and a message made atomic by a
database transaction, a relay that turns outbox rows into publications, and an
idempotent consumer built on `x/idempotency` that copes with the duplicates the
relay is **guaranteed** to produce.

## Why an outbox exists

A broker is not in the database transaction, so a service that writes an order
and then publishes "order placed" has two windows and can close neither:

- publish **inside** the transaction, and a transaction that fails afterwards
  leaves a message about an order that does not exist. A publication cannot be
  rolled back. This is the defect one level up: *an event
  raised from inside an operation is not evidence that the operation's
  after-effects have landed.*
- publish **after** the commit, and a crash between the two loses the message
  with no trace at all.

The outbox closes both: the order and a row in an `outbox` table are written
together, and a separate relay publishes the rows. A committed row is a
committed promise to publish.

## It is at-least-once *by construction*

The relay publishes and **then** marks the row sent, and those are two
operations. A relay that dies between them republishes the row on its next
pass. Reversing the order does not help — marking first and dying loses the
message. So the duplicate is the normal consequence of a restart, not a rare
race, and the consumer has to be idempotent.

The key is the **business operation's** identifier, carried on the outbox row
and forwarded as a header. A relay that generated a key per publication would
make every republication a new operation, and the consumer could not tell a
duplicate from a new order — the false-first, produced by
accident.

## The claim

```sql
UPDATE outbox SET locked_until = now() + $2::interval, attempts = attempts + 1
WHERE id IN (
  SELECT id FROM outbox
  WHERE sent_at IS NULL AND (locked_until IS NULL OR locked_until < now())
  ORDER BY id LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, idem_key, body, attempts
```

- **`FOR UPDATE SKIP LOCKED`** lets two relays run at once: the loser steps
  over a held row instead of blocking on it. A relay that blocks stops
  publishing every *other* message because one row is locked.
- **one statement**, so taking the lock and marking it cannot be separated.
- **`locked_until`, not a held transaction** — the publication happens between
  the claim and the mark, and holding a transaction open across a network
  publish exhausts the connection pool.
- a **partial index** on `(id) WHERE sent_at IS NULL` keeps the read cheap once
  the table holds a year of sent rows. Without it the claim degrades to a scan
  of everything ever published, which is this pattern's classic production
  failure.

## A check that could not fail

`TestTwoRelaysClaimDisjointRows` runs eight relays at once. On the in-process
store it catches a claim that does not hold its lock. **On Postgres it caught
none of three defects tried** — `SKIP LOCKED` removed, `FOR UPDATE` removed,
and the claim split into a `SELECT` plus a separate `UPDATE` all pass. Eight
statements against forty rows serialise faster than eight goroutines can
interleave, so the contention the test is named for never happens.

The property is therefore asserted by an observation that needs no race:
`TestPostgresClaimSkipsARowAnotherTransactionHolds` holds one row in a
transaction of its own and requires the claim to return **promptly** and
**without that row**.

## The two arms

| arm | store | gate |
|---|---|---|
| in-process | an in-memory `Store` | never skips |
| container | Postgres via testcontainers, or an external server | `IQMQ_POSTGRES_TESTCONTAINERS=1`, or `IQMQ_POSTGRES_ADDR` |

The in-process store models the three properties the relay rests on — atomic
write-and-append, a claim two relays cannot both win, and a lock that lapses —
and executes **no SQL**: the schema, the index and the claim run only on the
container arm.

Starting a container is **opt-in** and not "start one if Docker is available":
`make examples` runs in every `make ci`, and a developer running
`go test./...` should not have a database started for them.

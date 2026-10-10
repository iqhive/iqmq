# outbox-postgres — the transactional outbox, and the duplicate it guarantees

A business write and a message made atomic by a database transaction, a relay
that turns outbox rows into publications, and an idempotent consumer built on
[`x/idempotency`](../../x/idempotency) that copes with the duplicates the relay
is **guaranteed** to produce.

Postgres through [pgx](https://github.com/jackc/pgx), started by
[testcontainers](https://golang.testcontainers.org/) from the digest
`conformance/harness/images.lock` pins. The broker is the in-process memory
broker on both arms: the subject is the **database**.

    go run .                                    # against a Postgres on 127.0.0.1:5432
    go test ./...                               # the protocol, against an in-process store
    IQMQ_POSTGRES_TESTCONTAINERS=1 go test ./...  # ...and the SQL, against the pinned container

## What it shows

```
store: postgres 17
atomic: 3 order(s) and 3 outbox row(s) committed together; the rolled-back transaction left neither
relay: pass 1 claimed 3 row(s), published 3 and marked 2 sent — the last row's relay died before its mark
relay: pass 2 re-claimed that row once its 150ms lock lapsed and published it AGAIN, on attempt 2 — an outbox is at-least-once by construction
consumer: 4 publication(s) became 3 side effect(s); x/idempotency skipped 1 duplicate(s)
outbox: 0 row(s) left unsent
```

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

The outbox closes both by making the message part of the transaction. A
committed outbox row is a committed promise to publish.

## The half people skip: it is at-least-once *by construction*

The relay publishes and **then** marks the row sent, and those are two
operations. A relay that dies between them republishes the row on its next
pass. Reversing the order does not help — marking first and dying loses the
message, which is worse. So the duplicate is not a rare race; it is the normal
consequence of a restart, and this example produces it on purpose: pass 1
publishes three rows and marks **two**, and pass 2 re-claims the third once its
lock lapses and publishes it again.

The consumer is therefore idempotent, over the **same key the outbox row
carries**. That key is the business operation's identifier and not a
per-publication one — a relay that generated a key per publication would make
every republication a new operation, which is the false-first
produced deliberately. `TestTheKeyIsTheOperationAndNotThePublication` pins it.

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

- **`FOR UPDATE SKIP LOCKED`** is what lets two relays run at once: the loser
  steps over a held row instead of blocking on it. A relay that blocks stops
  publishing every *other* message in the outbox because one row is locked.
- **one statement**, so taking the lock and marking it cannot be separated.
- **`locked_until`, not a held transaction**, because the publication happens
  between the claim and the mark, and holding a database transaction open
  across a network publish is how an outbox exhausts its connection pool.
- **`ORDER BY id`**, because an outbox that publishes out of order defeats the
  purpose of having written the rows in order. `RETURNING` makes no ordering
  promise, so the Go side re-sorts.

A partial index — `ON outbox (id) WHERE sent_at IS NULL` — keeps the relay's
read cheap once the table holds a year of sent rows. Without it the claim
degrades from an index scan over the backlog to a scan of everything ever
published, which is this pattern's classic production failure.

## A check of mine that could not fail

`TestTwoRelaysClaimDisjointRows` runs eight relays at once and asserts they
partition the rows. On the in-process store it catches a claim that does not
hold its lock across the read and the write (watched failing, with and without
`-race`). **On Postgres it catches nothing**: removing `SKIP LOCKED` passes,
removing `FOR UPDATE` passes, and splitting the claim into a `SELECT` and a
separate `UPDATE` passes. Eight statements against forty rows serialise faster
than eight goroutines can interleave — the first takes every row and the rest
then see `locked_until` already set — so the contention the test is named for
never happens.

The SQL property is therefore asserted by an observation that does not depend
on winning a race: `TestPostgresClaimSkipsARowAnotherTransactionHolds` holds a
row with a transaction of its own and requires the claim to come back
**promptly** and **without that row**. Without `SKIP LOCKED` it blocks and the
test fails with a context deadline.

## The two arms

| arm | store | gate |
|---|---|---|
| in-process | an in-memory `Store` | never skips |
| container | Postgres via testcontainers, or `IQMQ_POSTGRES_ADDR` | `IQMQ_POSTGRES_TESTCONTAINERS=1`, or `IQMQ_POSTGRES_ADDR` |

The in-process store models the three properties the relay's correctness rests
on — an atomic business-write-and-append, a claim two relays cannot both win,
and a lock that lapses — so the whole protocol runs on every `make ci`. It
executes **no SQL**: the schema, the partial index and the claim are only run
on the container arm.

Starting a container is **opt-in** rather than "start one if Docker is
available". `make examples` runs in every `make ci`, several times per
increment, and this repository is developed on a shared machine running about
fifty other people's containers — so an example that silently started a
database whenever it could would start one on every run of the gate.
The container is removed by the handle that started it,
through `testcontainers.CleanupContainer`, never by a name or an image match.

`postgres 17` in the output is a fact about the pinned digest, not about the
tag: that digest's prefix reads `18cfe3ef` and the server is PostgreSQL 17,
which is precisely the coincidence that makes a guessed version look checked.
The first run of this arm reported `postgres 17` against an expected
`postgres 18`, which is how the number was established.

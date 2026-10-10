# `x/outbox`

Publish the messages your database transaction committed, and nothing else.

```go
// In your transaction — your SQL, your handle:
//   INSERT INTO orders  …
//   INSERT INTO outbox (topic, key, headers, body) VALUES …

relay, err := outbox.New(outbox.Config{
    Store:   myPostgresOutbox,
    Publish: publishThroughYourMiddlewareChain,
    Batch:   100,
})
go relay.Run(ctx)
```

See [inbox and outbox](../reliability/inbox-outbox.md) for why there is no
ordering of a database write and a broker call that is correct, and
`examples/outbox-postgres` for the worked Postgres version with
`FOR UPDATE SKIP LOCKED` against a real container.

## There is no `Append` on the `Store` interface

That is the design, not a gap. The append happens inside **your**
transaction, with your own handle — a `*sql.Tx`, a pgx transaction, a GORM
session — and an interface that took one of those would bind this package to
that library.

So `Store` is the relay's half only: `Claim`, `MarkSent`, `Release`, `Fail`.
The append is an `INSERT` you write, and `Row` plus `EncodeHeaders` and
`DecodeHeaders` define the column shape so the round trip is specified rather
than improvised.

## The two rules the relay must not break

Both are about the same thing — a row must never be marked sent unless it was
really published — and both have a test named after them.

**An ambiguous publication does not mark the row sent.** the specification's
[ambiguous outcome](../concepts/ambiguous-publishes.md) is a publication whose
acceptance is *unknown*. Marking it sent turns "we do not know" into "we are
done", which is the only way this pattern can **lose** a message. Leaving it
unsent produces a duplicate, which at-least-once already promised, and
`Stats.Ambiguous` is its own counter precisely because it is the number that
explains a duplicate a consumer saw.

Both halves of the surface are checked — an error carrying
`CodeAmbiguous`, and one whose `*OpError` sets the `Ambiguous` flag — because
a relay reading only one would mark half of them sent.

**Only rows that were published are marked.** A batch whose third row fails
marks the first two and releases the rest. Marking the whole claim because
the call returned is how a partial failure loses everything after the failure
point.

## What the store must guarantee

`Claim` has two requirements the relay's correctness rests on, and both are
SQL rather than Go:

- **Two relays must get disjoint rows**, which on Postgres is
  `FOR UPDATE SKIP LOCKED` inside the `UPDATE` that sets the lock. A store
  that let both claim the same row would publish it twice per pass, giving a
  duplicate rate set by the number of relays.
- **The lock must lapse.** A relay that dies holding a claim must not strand
  its rows for ever, and the lapse is what turns a crashed relay into a
  duplicate rather than a loss. Set it to rather more than the longest
  publish timeout: a lock that lapses mid-flight is a duplicate, which is
  survivable; a lock that never lapses is a row nobody will ever send, which
  is not.

## Poison rows

A row whose topic does not exist, or whose body exceeds the broker's limit,
fails identically on every attempt. `MaxAttempts` (default 10) is compared
with `Row.Attempts`, and a row past it is recorded via `Store.Fail` with
`ErrPoisonRow` and not attempted — otherwise the relay spends every claim on
the same unpublishable row and everything behind it makes no progress.

There is deliberately no "unbounded" setting.

## Ordering

`Concurrency` of 1 — the default — publishes in row order. Anything above 1
**reorders**, and this package does not pretend otherwise: two workers
claiming disjoint rows finish in whatever order the broker acknowledges them.
A deployment that needs per-key order *and* throughput partitions the claim
by key hash, which is a `Store` concern because it is a `WHERE` clause.

## The message id should be the business operation's

`Row.MessageID` is what the consumer's [`x/inbox`](inbox.md) will key on, so
it must be the **business operation's** identifier and not a per-publication
one. A row republished after a relay died carries the same id, which is
exactly what lets the consumer recognise it as the same operation.

## Headers

`EncodeHeaders` renders headers as one opaque blob for a `BYTEA` or `BLOB`
column: a count-prefixed sequence of length-prefixed key/value pairs.

It is deliberately **not JSON**. Headers are ordered and duplicate-preserving
 and a JSON object is neither, so round-tripping them through one would
silently drop a repeated header and reorder the rest — which for
`traceparent` and a multi-valued application header is a real loss.

`DecodeHeaders` reads a column something else wrote, so every length in the
blob is a claim: the declared pair count is compared with what the blob can
possibly hold before anything is allocated, each field's length is checked
against what remains, and trailing bytes are `ErrBadHeaders` rather than
ignored.

## `Run` reports a broken store rather than looping

A pass that published something does not wait before the next one — sleeping
between full batches is how a relay falls behind a burst. A pass that found
nothing waits `Interval`.

A store or publish failure is **returned** to the caller, who decides whether
to keep going. A relay that swallowed them and looped would hide a
permanently broken database behind a process that looks healthy. A cancelled
context and a `Close` are both clean stops and return `nil`.

# Inbox and outbox

The pattern for when your database and the broker must not disagree. Both
halves are in this library as extensions —
[`x/outbox`](../extensions/outbox.md) and [`x/inbox`](../extensions/inbox.md)
— and neither ships a SQL store: the part that has to live in your database is
yours, and those pages say exactly what it must guarantee. The shapes are here
because the pattern is what people reach for first and because the
half-measures are worse than the pattern.

## The problem

```go
tx.Exec("INSERT INTO orders …")   // (1)
tx.Commit()                       // (2)
pub.Publish(ctx, msg)             // (3)
```

Three failure points, and each of them produces a different wrong answer:

* fail after (2) and before (3): the order exists and nobody was told.
* fail *during* (3) with an [ambiguous](../concepts/ambiguous-publishes.md)
  outcome: nobody knows whether anybody was told.
* publish before committing instead: somebody is told about an order that was
  rolled back.

There is no ordering of those two operations that is correct, because they are
two systems and there is no transaction across them. That is not a limitation
of this library; it is the shape of the problem.

## The outbox

Write the message **into your own database, in the transaction that produced
it**. A separate process reads the unpublished rows, publishes them, and marks
them sent.

```
BEGIN
  INSERT INTO orders  …
  INSERT INTO outbox (topic, key, headers, body) VALUES …
COMMIT
-- then, separately:
--   SELECT … FROM outbox WHERE sent_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED
--   publish, then UPDATE outbox SET sent_at = now()
```

What it buys: the order and the intent-to-publish commit or roll back together,
so the first and third failures above disappear. An ambiguous publication is
now *resolvable* — the row is still unsent, so it is retried, and the
duplicate that produces is a duplicate rather than a loss.

What it costs: at-least-once publication, which you had anyway; a relay to run;
and ordering that is the relay's rather than your application's, so a relay
with concurrency reorders unless it partitions by key.

[`x/outbox`](../extensions/outbox.md) is the relay half: `outbox.New` runs the
claim, publish and mark loop over a `Store` whose four loop methods — `Claim`,
`MarkSent`, `Release`, `Fail` — you implement against your own table, and
`Row` with `EncodeHeaders`/`DecodeHeaders` fixes the column shape. The append
is an `INSERT` in your own transaction, so the interface deliberately has no
`Append`. The worked Postgres version, with `FOR UPDATE SKIP LOCKED` against a
real container, is [`examples/outbox-postgres`](../examples/outbox-postgres.md);
it predates the package and still declares its own store types rather than
implementing `outbox.Store`.
Status is the current state of every module.

## The inbox

The consumer half: record that you processed a message, keyed on its identity,
in the same transaction as the side effect.

```
BEGIN
  INSERT INTO inbox (message_id) VALUES ($1)   -- fails on a duplicate
  …the side effect…
COMMIT
```

A unique-constraint violation means you have seen it: ack and move on. This is
how at-least-once delivery becomes effectively-once *processing*, and it is the
only way that works on every broker — which is the argument
[the duplicates page](../concepts/idempotency.md) makes at more length.

The identity is `Delivery.ID` where the broker provides one, or your own
`DedupKey` carried in a header where it does not.

[`x/inbox`](../extensions/inbox.md) is this half: its middleware hands each
delivery to `Store.Once`, which records the key and runs your handler in one
transaction. Its `MemoryStore` is for tests; in production the `Store` is your
database. `x/idempotency` is the cheaper,
weaker sibling — a claim in a separate store, in process or in Redis
(`x/idempotency/redisstore`), which reduces the duplicate rate rather than
closing the window — and
the extensions index
compares the two.

## What not to do instead

**Do not publish inside the database transaction.** A broker call inside a
transaction holds the transaction open for a network round trip, and on a
timeout you have both problems at once.

**Do not use a broker transaction as a substitute.** Kafka transactions and
Pulsar transactions are real and this library exposes them — `x/kafkatx`,
`x/pulsartx` — and they make a set of *broker* operations atomic with respect
to a *broker* read. They do not make your database write atomic with them.
The specification says so and it is worth repeating: a broker dedup or transaction feature
does not make an arbitrary side effect exactly-once.

**Do not use `mq.FailureSkip` to make the numbers look better.** It acks a
message nobody handled. It is the only destructive failure policy, it is never
a default, and reaching for it here means the outbox row was never written and
nobody will ever know.

## The claim-check variant

When the message is too large for the broker — `mq.ErrMessageTooLarge`, refused
before any I/O against the driver's advertised `MaxMessageBytes` — the pattern
is to store the payload somewhere addressable and publish the address. That is
`x/claimcheck`, which stores the body and
publishes a reference to it. The relevant part here is that the refusal is
local and cheap, so discovering the limit costs no round trip.

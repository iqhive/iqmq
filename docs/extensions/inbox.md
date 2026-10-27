# `x/inbox`

Record that you processed a message **in the same transaction as the side
effect**. This is the one way at-least-once delivery becomes effectively-once
processing on every broker.

```go
mw, err := inbox.New(inbox.Config{
    Store: myPostgresInbox,               // your database
    Key:   inbox.KeyFromHeader("operation-id"),
})
err = consumer.Run(ctx, handler, mq.WithMiddleware(mw.Wrap()))
```

The idea is one sentence: the record that you handled a message and the effect
of handling it commit or roll back together, so there is no window in which
one exists without the other.

```sql
BEGIN
  INSERT INTO inbox (message_id) VALUES ($1);   -- fails on a duplicate
  -- …the side effect…
COMMIT
```

See [inbox and outbox](../reliability/inbox-outbox.md) for the pattern, and
[the extensions index](index.md#which-duplicate-suppression-do-i-want) for
the comparison with `x/idempotency`.

## The condition, which the interface cannot enforce

`Store.Once` hands your function a context and runs it inside the store's
transaction. **If your function's side effect goes anywhere other than that
database** — a different database, an HTTP API, another broker — then the
record and the effect are two systems again, with no transaction across them,
and you have bought a slower [`x/idempotency`](idempotency.md).

Nothing in the type system says so. This paragraph is the only thing standing
in the way, which is why it is here and in the package documentation.

## `fn` must not insert its own key again

`Store.Once` holds the transaction across your function. A re-entrant `Once`
for the **same** key asks to insert a record that has not committed yet, which
a real SQL store answers by blocking the caller on itself and `MemoryStore`
answers by making the call wait for itself.

This is inherent to the pattern rather than an implementation limit: the
point of the transaction is that nobody else sees the record until it
commits, and "nobody else" includes your own second call. Reading the store,
or a `Once` for a *different* key, is fine.

A function that **panics** has not committed. The store rolls the record back
and lets the panic continue, so a runner that recovers it and redelivers the
message runs your handler again rather than acking a duplicate of work that
never finished.

A handler that nacks or rejects its own delivery and returns nil is rolled
back the same way, record and writes together, so the redelivery it asked for
runs it again. A duplicate is acked by the middleware itself, so it is settled
under `mq.SettlementManual` too.

## Both defaults are the opposite of `x/idempotency`'s

That is deliberate, and it is the most useful thing to understand about this
package.

**When the store cannot answer, the default is to retry** (`OnUnavailableRetry`).
An idempotency store is a cache *beside* the work and fails open, because the
substrate underneath is at-least-once already — processing during an outage
returns the system to the guarantee it advertises. An inbox store **is** the
database the work happens in: if it cannot be reached the side effect cannot
happen either, so there is nothing to fail open to. Processing anyway is not
"a duplicate for the duration of the outage"; it is a handler that will fail
a moment later for the same reason.

`OnUnavailableProcess` exists for the deployment whose handler writes
elsewhere — which is the case this pattern does not cover anyway.

**An unkeyed message is refused** (`MissingKeyFail`). An idempotency
middleware that refused them would make adoption a breaking change to a
running service, and its cost for letting one through is a duplicate. Here
the cost is a side effect that is never recorded and is therefore repeated on
every redelivery for ever — the failure the pattern was adopted to remove.

## The key

`Config.Key` has no default. The one failure in this package that can **lose
work** is two distinct messages deriving one key: the second reads as a
duplicate and is acked without being handled.

`KeyFromHeader` is right when the *publisher* owns the key, which is the
usual case — the business operation's identifier, the same on every retry of
that operation. `KeyFromID` is right only where `Delivery.ID` is the
publisher's and is stable across its retries; a driver-assigned or
broker-assigned id is not, and a publisher configured with the specification's
`IDGenerator` is exactly the case it must not be used for.

Both come from [`x/idempotency`](idempotency.md) rather than being declared
again here: the problem is identical, and two copies that could drift apart
would be worse than the coupling.

## Counts

`Middleware.Counts()` returns `Processed`, `Duplicate`, `Failed`, `NoKey` and
`Unavailable`. A deployment that adopts this pattern wants to know its actual
duplicate rate, and `Duplicate` is that number.

## The memory store

`NewMemoryStore` is per process, and its "transaction" is a per-key wait —
the part of a database it models. A second delivery of a key whose handler is
still running waits for that handler's transaction to commit (and is then a
duplicate) or roll back (and then runs); deliveries of different keys run
concurrently, exactly as two transactions inserting different rows into a
table with a unique index do. A waiter whose context ends stops waiting and is
treated as a store that could not answer, so the delivery is retried.

It used to hold one mutex across every handler, which serialised the whole
process behind its slowest handler and meant a test written against it never
saw two handlers interleave — the one thing production is guaranteed to do.

It models none of the SQL, the isolation level or the durability. It refuses
at its bound (`ErrFull`) rather than evicting, because an evicted key is a
message that will be processed twice and silently turning a memory bound into
a duplicate is the opposite of what this package is for.

**Using it in anger** — for more than tests, examples and a single process
whose side effects are also in process — is the wrong store. A restart forgets
every key and the record is not in the same transaction as anything, so for
in-process duplicate suppression without a database
[`x/idempotency`](idempotency.md)'s memory store is the right choice: a
bounded LRU with a window per claim, which evicts — and counts what it
evicted — rather than refusing at its bound. The inbox pattern proper wants a `Store` over the database your
handler writes to: `Once` opens a transaction, inserts the key, runs your
function with a context carrying that transaction, and commits. A unique
violation on the insert is the duplicate.

A real inbox table is pruned by a job that deletes rows older than the
broker's retention — a deletion you can reason about.

# `x/idempotency/redisstore`

A claim store on Redis, so that every replica of a service shares one
duplicate-suppression window.

    import (
        mq "github.com/iqhive/iqmq"
        "github.com/iqhive/iqmq/dsn"
        "github.com/iqhive/iqmq/x/idempotency"
        "github.com/iqhive/iqmq/x/idempotency/redisstore"
    )

    store, err := redisstore.Open(ctx, redisstore.Options{
        Addrs:  []string{"127.0.0.1:6379"},
        Prefix: "payments:idem:",   // never share a key space
        // A dsn.Secret: printing the Options prints "redacted", not this.
        Password: dsn.Secret(os.Getenv("REDIS_PASSWORD")),
        // Refuse to start against a Redis that can delete a claim inside its
        // window. See "The window is not the TTL" below.
        RequireSafeEviction: true,
    })
    guard, err := idempotency.New(idempotency.Config{
        Store:  store,
        Key:    idempotency.KeyFromHeader("idempotency-key"),
        Window: 24 * time.Hour,
    })
    err = consumer.Run(ctx, handler, mq.WithMiddleware(guard.Middleware()))

`Open` owns the connection it dials; `New(client, opts)` borrows one the
process already has — including the client behind
[`drivers/redis`](../../../drivers/redis) — so the claims and the stream can
share a pool.

## What the claim is

One string key per idempotency key. Its value is `p:<token>` while the claim is
open and `d:<token>` once the work is done, with a `PX` expiry equal to the
window. Three Lua scripts, because each of the three operations is a
compare-and-set and Redis has no other way to make one atomic:

| operation | script | refuses when |
|---|---|---|
| `Claim`  | `SET key p:<token> NX PX <window>`, then `GET` on the miss | — (answers `first`, `duplicate` or `in-flight`) |
| `Commit` | `GET`; if the value is this claim's, `SET d:<token> PX <window>` | the token no longer matches → `ErrClaimLost` |
| `Abort`  | `GET`; if the value is this claim's, `DEL` | the token no longer matches → `ErrClaimLost` |

Without the token comparison a `Commit` arriving after its own claim lapsed
would mark **another worker's** claim done, and an `Abort` would delete it.
`TestTheScriptsAreCompareAndSet` reads the script text for exactly that, and
the contract suite's `CommitOfALapsedClaimIsRefused` is the behaviour.

Two round trips for a message seen for the first time (claim, then commit), one
for a duplicate.

## The window is not the TTL

`StatusFirst` means "I have no record of this key". It does not mean the key is
new, and four things shorten the effective window:

- **Eviction.** A Redis at `maxmemory` with `allkeys-lru`, `allkeys-lfu` or
  `allkeys-random` deletes keys that have not expired. So does every
  `volatile-*` policy — "volatile" means *only* keys with a TTL, and every key
  this store writes has one, so a volatile policy evicts precisely this store's
  keys and spares the cache entries beside them. **Only `noeviction` is safe.**
  Nothing reports an eviction: the claim is gone and the key reads as first.
  This is the most likely way for the store to be quietly worthless, and the
  operator usually chose the policy for a cache. Put the claims on their own
  instance or logical database (`Options.DB`), set `RequireSafeEviction`, and
  alert on `evicted_keys`.
- **Failover.** Replication is asynchronous. A primary that accepts a claim and
  dies before its replica has it promotes a replica that never saw the key.
  `WAIT` narrows that window; it does not close it. Redis Cluster is the same.
- **Restart without persistence**, or AOF with `appendfsync everysec` — which
  is a second of claims.
- **A prefix collision.** Two services sharing a key space make each other's
  messages duplicates. A value under this store's prefix that this store did
  not write is reported as an error rather than guessed at
  (`TestAPrefixCollisionIsReportedAndNotGuessed`).

Every one of those is a **false first**: a duplicate the store lets through,
which degrades to the at-least-once delivery the broker already promised, so
the handler's effect must still be safe to repeat. None of them is a false
**duplicate** — the direction that discards work — because the only way to get
one of those is a key-derivation mistake, which is why
`idempotency.Config.Key` has no default.

## Unreachable is not a verdict

Every method reports an error, and an error carries `StatusUnknown`. What to do
about it is `idempotency.Config.OnUnavailable`, which processes by default:
failing closed turns a cache outage into a consumer that processes nothing and,
with `mq.WithMaxAttempts` and a dead-letter topic, dead-letters perfectly good
messages. Both costs are in that field's documentation.

## Tests

    go test ./...                              # the whole contract, on miniredis
    IQMQ_REDIS_ADDR=127.0.0.1:6379 go test ./...   # the same contract, on a real server

The suite is [`x/idempotency/storetest`](../storetest), the one contract the
in-process store runs too. The miniredis arm never skips and executes the same
Lua through the same client; the `IQMQ_REDIS_ADDR` arm is what says miniredis
did not lie. To start the pinned server this project takes its Redis evidence
against:

    docker run -d -p 6379:6379 \
      redis@sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf

`IQMQ_REQUIRE_BROKERS=1` turns the unset variable from a skip into a failure.

# `x/idempotency`

Reduce the duplicate rate with a two-phase claim in a store beside the work.

```go
mw, err := idempotency.New(idempotency.Config{
    Store:  redisStore,                       // or idempotency.NewMemoryStore
    Key:    idempotency.KeyFromHeader("operation-id"),
    Window: 24 * time.Hour,
})
```

## What it is, and what it is not

The claim goes in a **separate** store, so the claim and the work are two
systems with no transaction across them. It therefore **reduces** a duplicate
rate that was never zero. It does not remove duplicates, and no amount of
configuration makes it do so — which is the honest description of every
idempotency cache and is why [`x/inbox`](inbox.md) exists for the case where
your handler writes to a database you control.

`Status` has three values and the middle one is the one people forget:

| Status | Meaning | The caller |
|---|---|---|
| `StatusFirst` | no record; you hold an open claim | must `Commit` or `Abort` |
| `StatusDuplicate` | a claim was **committed** in the window | holds nothing; skip |
| `StatusInFlight` | somebody else holds an open, unexpired claim | holds nothing; come back later |

`StatusInFlight` is **not** a duplicate. The work may never have been done,
and skipping it loses the message. The middleware postpones that delivery
(`mq.Postpone`, with `Config.InFlightRetryAfter` as the delay), so the runner
offers it again later and never exhausts, dead-letters or skips it for the
collision. When it came back as a failed attempt, another worker holding the
claim for longer than the attempts added up to halted the runner with a
`*mq.PoisonError`.

A rate limit or an open circuit behind the middleware postpones a delivery
before the handler runs. Its claim is released for the next offer, and
`Counts` records it as neither `Processed` nor `Failed`.

The middleware acks a `StatusDuplicate` delivery itself, so it is settled under
`mq.SettlementManual` too. It commits the claim only when the handler returned
nil and either acked the delivery or left it to the runner. A handler that
nacks or rejects its own delivery and returns nil has asked for the message
again, so its claim is aborted and the redelivery is processed.

The middleware decides as your handler returns and cannot see a settlement
made after that. Under `mq.SettlementManual` your handler may keep the
delivery and settle it later, but one it left unsettled and returned nil for
has its claim committed. If a worker nacks it later, or never settles it and
the broker redelivers it when its lease runs out, the redelivery is skipped
as a duplicate and the message is lost. A handler that keeps its deliveries
should settle a failure before it returns, or claim the key where the
delivery is settled.

`StatusUnknown` is the zero value on purpose: a caller who forgets to look at
the status gets the answer that assumes nothing.

## The window is the only real engineering choice

`Config.Window` must outlast the longest redelivery the substrate can produce
— a lease that expires, a rebalance, a consumer restart, an operator
replaying a day — because a duplicate that arrives after the window is a
duplicate that is processed. It must also be short enough that the store
holds the resulting number of keys.

## Defaults

When the store cannot answer, the default is to **process** — fail open —
because the substrate underneath is at-least-once already, so processing
during an outage returns the system to the guarantee it advertises rather
than to a new one. The alternative turns a cache outage into a consumer that
makes no progress, and a consumer that makes no progress on a broker with a
max-attempts policy dead-letters everything it is sent.

An unkeyed message is **processed, unguarded**, and counted: a topic that
mixes keyed and unkeyed messages is ordinary, and refusing them would make
adding this middleware a breaking change to a running service.

Both defaults are the opposite of [`x/inbox`](inbox.md)'s, and that page
explains why.

## The in-process store is lossy by design, and counts it

`MemoryStore` is a bounded LRU: `DefaultMaxEntries` is 100,000 and an entry
evicted to stay inside the bound is a claim that will read as `StatusFirst`
before its window lapsed. `Stats.Evicted` is how many. A non-zero `Evicted`
with a duplicate rate that matters means `MaxEntries` is too small, and
nothing raises it for you.

The entry evicted is the one claimed or committed least recently. Reading a
key as a duplicate or in flight does not move it, so a duplicate storm on one
key cannot keep that key while every new key is evicted. A commit used to
leave its entry where the claim had put it, so a long handler's key was the
first evicted just after its window began.

It is also per process: two replicas have two stores and two independent
windows, so a duplicate delivered to the other replica is `StatusFirst`
there. `x/idempotency/redisstore` is the store that crosses replicas.

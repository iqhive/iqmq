# iqmq Redis Streams driver, over go-redis

`github.com/iqhive/iqmq/drivers/redis/goredis` is the iqmq [`Backend`][spi] for
**Redis Streams** over [go-redis]. It is the *same driver* as
[`drivers/redis`][redis]: the same key naming, the same entry layout, the same
commands, the same reply semantics, the same reclaim accounting, the same nack
strategies, the same capability table, the same error table, and the same
`Config` type. What differs is one type — a `streams.Commander` over go-redis's
`UniversalClient` — and the connection model that comes with it.

```go
client, err := goredis.Open(ctx, goredis.Config{
        Addrs:      []string{"127.0.0.1:6379"},
        AutoCreate: true,
})
if err != nil {
        return err
}
defer client.Close()
```

`goredis.Config` is `redis.Config`, not a copy of it, so a program that already
configures the Redis driver changes one import and one call.

## Which one should I use?

One thing that is about speed and is not in that geomean: this client does not
copy the message body. The batched budget is breached on both Redis
profiles, and part of the breach was one copy of the whole body per message
that neither raw client makes. It is gone from this arm and it is still there on
rueidis, for a reason that is about correctness rather than effort — see "the
message body" below. The figures above predate the change and re-runs
them; nothing here re-measures, because that campaign owns the host.

Reasons to choose this one that are not about speed:

* go-redis is what a codebase probably already has. Sharing one
  `UniversalClient` with the rest of an application — its hooks, its
  instrumentation, its Sentinel or Ring topology — is `OpenWithClient`.
* `redis.Ring`, `redis.FailoverClient` and Sentinel are reachable through
  `OpenWithClient` with no work on this driver's part.

## What differs because the client library differs

| | |
|---|---|
| **Connections** | go-redis takes a connection from a pool per command and pipelines when asked; rueidis multiplexes everything over one auto-pipelined connection per node. That is what the specification measures. |
| **Retries** | off. Go-redis retries a written command by default, which on this broker is a duplicate reported as one publication; `MaxRetries` is `-1` and the retry policy stays the core's. |
| **Error text** | rueidis strips a leading `ERR ` from an error reply and go-redis does not. The driver's error table matches the distinctive phrase so that both spellings map to one code . |
| **Cluster routing** | go-redis is told the first key's POSITION (`SetFirstKeyPos`) because it cannot find a key in a command sent through `Do`; without it every command would be routed to a random node (`TestKeyRoutingIsDecidedByTheCommandsKeyAndNotAtRandom`). |
| **Allocations** | go-redis takes a command as `...any`, so every token is boxed; rueidis appends strings to a pooled builder. `TestAllocsAdapting` pins what this adapter costs on top. |
| **The message body** | **not copied on this client, copied on rueidis.** go-redis writes and reads in the calling goroutine, so a `[]byte` argument is on the wire before `Publish` returns and the caller gets its buffer back intact; the body therefore goes out as the caller's own slice. rueidis queues a command for a background writer and can return from a cancelled `Do` with the command still queued, so the same trick there would put the caller's LATER bytes on the wire — it copies. One obligation follows for you: a `redis.Hook` installed through `OpenWithClient` that STORES the `Cmder` and reads its arguments after its own call returns must copy what it stores, because the body argument is the publisher's buffer and the publisher has it back. |
| **Module graph** | importing this package imports `drivers/redis`, which imports rueidis, so a program that wants go-redis alone still carries rueidis in its module graph . |

Everything else — the confirmation boundary, the ack boundary, the
at-least-once window, the driver-owned lease, the guarded acknowledgement,
negative settlement, dead-lettering, replay, trimming, the cluster hash-slot
constraints — is [`drivers/redis`][redis]'s and is documented there and in
`docs/drivers/goredis.md`.

## Evidence

## Maturity

Declared on one line and in one place: line 5 of
[`docs/drivers/goredis.md`](../../../docs/drivers/goredis.md), which
`make docs-check` reads. This section used to restate the level, which is how a
maturity label drifts — it still said Experimental after the page said Beta.
Tier 2 (the driver documentation), inherited from the driver this
is a client library for.

[spi]: ../../../the API contract
[redis]: ../README.md
[go-redis]: https://github.com/redis/go-redis

# iqmq Redis Streams driver

`bitbucket.org/iqhive/iqmq/drivers/redis` is the iqmq [`Backend`][spi] for **Redis
Streams** over [rueidis]: a durable log per topic, consumer groups, and
at-least-once delivery with a pending-entries list.

`bitbucket.org/iqhive/iqmq/drivers/redis/goredis` is the same driver over go-redis
and will share everything below it: the key naming, the entry layout, the
commands, the reply parsing, the reclaim accounting and the nack strategies all
live in `internal/streams`, and a second client library is a second
`streams.Commander`.

```go
client, err := redis.Open(ctx, redis.Config{
        Addrs:      []string{"127.0.0.1:6379"},
        AutoCreate: true,
})
if err != nil {
        return err
}
defer client.Close()

if err := client.Publish(ctx, "orders.created", &mq.Message{Body: body}); err != nil {
        return err
}
```

## What you get, and what you do not

| | |
|---|---|
| **Delivery** | at-least-once; the failure mode is duplication, not loss |
| **Settlement** | `XACK`, through a Lua script that checks who holds the entry |
| **Redelivery** | the driver's own `XAUTOCLAIM` sweep — Redis redelivers nothing on its own |
| **Confirmation** | `ConfirmAccepted` is `XADD`'s reply (memory); `ConfirmDurable` is `WAITAOF` and needs Redis ≥ 7.0 with `appendonly yes` |
| **Competing consumers** | yes — a subscription name is a consumer group |
| **Headers** | native, ordered, binary-safe and duplicate-preserving: a stream entry is a field-value list |
| **Delayed redelivery** | only under `nack=zsetmover`; the other three arms refuse a delay they cannot honour |
| **Dead-letter** | only when the configuration names a destination; otherwise `Reject` is `ErrUnsupported` |
| **Ordering** | per shard. A topic with several shards has no total order |
| **Wildcards, request/reply, transactions** | no. Redis's pattern subscription is pub/sub, which has no consumer groups and no acknowledgement |
| **Minimum server** | Redis 6.2 (`XAUTOCLAIM`), or Valkey |

## The three things worth knowing before you deploy it

**A topic is one cluster slot.** Every key of a topic carries the topic's hash
tag, so `Partitions` buys ordering lanes and parallel consumers within a
subscription and does *not* spread a topic across a cluster's nodes. That is
the price of being able to move a message between two of a topic's keys
atomically, which the dead-letter move and the delayed mover both need.

**The reclaim loop is mandatory, not an optimisation.** A Redis consumer group
hands an entry to a consumer and then does nothing about it for ever: no lease
expires, nothing redelivers. This driver's `XAUTOCLAIM` sweep is the only thing
that brings a dead consumer's work back, and `Config.AckDeadline` +
`Config.ClaimInterval` is your worst-case redelivery latency.

**`XACK` does not check who holds the entry**, so acknowledging a delivery
another consumer has reclaimed would throw away that consumer's work. This
driver refuses such an acknowledgement — a per-source generation table for the
same-process case and a Lua ownership check for the cross-process one — and
`AckGuard: none` is the setting that gives that up for one fewer round trip.

## The message body is copied on this client, and that is deliberate

The copy is what makes true here. Rueidis queues a command for a
background writer, and when a context is cancelled `pipe.Do` returns
`ctx.Err()` while the command is still in that queue — `_backgroundWrite` then
writes its tokens from another goroutine, after `Publish` has returned. 
gives the caller its buffer back at that moment, so a body that aliased the
caller's slice would put whatever the caller wrote next on the wire: a corrupt
entry in the stream, reported as nothing. `rueidis.BinaryString`, which is
`unsafe.String(unsafe.SliceData(bs), len(bs))` and is what the raw-client
baseline uses, has exactly that exposure.

So part of the measured overhead on this profile is a guarantee the comparand
does not offer, and the figures stay as they are: they measure what a user
pays. They are not "the library's own overhead" (and
The specification). If your workload publishes large bodies and you can live with the
go-redis pool instead of rueidis's multiplexing,
`drivers/redis/goredis` is the arm without the copy.

## Reading further

`docs/drivers/redis.md` has the fifteen fixed sections of the driver documentation:
the mapping, both confirmation boundaries, the ack/nack/extend table, the four
nack strategies and what each costs, the limits and why they are the portable
envelope's, and the known gotchas.

[spi]: https://pkg.go.dev/bitbucket.org/iqhive/iqmq#Backend
[rueidis]: https://github.com/redis/rueidis

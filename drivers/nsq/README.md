# `drivers/nsq`

iqmq's NSQ driver, over [go-nsq](https://github.com/nsqio/go-nsq).

The reference is [`docs/drivers/nsq.md`](../../docs/drivers/nsq.md), which
carries the fixed sections: the mapping, the confirmation boundary, the
settlement verbs, the limits, the reconnect behaviour, the at-least-once
window, the gotchas and the driver-specific critical tests.

## What you get

| | |
|---|---|
| Topic | an nsqd topic (`Config.TopicPrefix` prepended) |
| Subscription | an nsqd **channel**: every channel gets a copy, every consumer of one channel competes |
| Publish | `PUB`, answered by nsqd — `ConfirmAccepted`, never durable |
| Batch publish | `MPUB`, split on nsqd's `--max-body-size` |
| Scheduled publish | `DPUB`, bounded by `--max-req-timeout` |
| Ack / Nack / Extend | `FIN` / `REQ(delay)` / `TOUCH` |
| Attempt | nsqd's own `Attempts`, 1 on the first delivery |
| Discovery | nsqlookupd when configured, direct nsqd otherwise |
| Admin | nsqd's HTTP API (`Config.AdminAddrs`) |

## What you do not

NSQ has **no headers at all** — not a limited set, not string-only values. So
the ordered, duplicate-preserving, binary-capable headers are carried by
iqmq's portable envelope and by nothing else, and a deployment says so once:

```go
client, err := nsq.Open(ctx, nsq.Config{
    Addrs:    []string{"127.0.0.1:4150"},
    Metadata: mq.MetadataAdaptive, // envelope the messages that need it
})
```

Without it, `MetadataNativeStrict` refuses a message carrying a header, an
identifier, a typed property or a key — before any I/O, naming the field.

There is also **no ordering of any kind** (nsqd promises none, not even for one
producer and one consumer), no message time-to-live, no priority, no
de-duplication, no replay or seek, no wildcard subscription and no native
dead-letter route. `mq.WithDeadLetter` is the core's publish-then-acknowledge
emulation and works here; `NackDeadLetter` is refused with `ErrUnsupported`.

## Testing

```sh
go test ./...                                   # in-process model, no Docker
go test -race ./...
IQMQ_NSQ_CONFORMANCE=1 IQMQ_NSQ_ADDR=127.0.0.1:4150 go test -run TestConformance ./...
make integration DRIVER=nsq                     # the pinned nsqd/nsqlookupd pair
```

`nsqfake` is an in-process model of a channel's message lifecycle. It exists
because everything interesting on this broker is the broker's decision — an
expired per-message timeout, a redelivery to a second consumer, a delayed
requeue, a settlement for a message somebody else now holds — and it reproduces
nsqd's exact laxity: a `FIN`, `REQ` or `TOUCH` names a message *identifier* and
lands on whatever is under it. It counts every settlement that landed on the
wrong delivery attempt, so a driver that stopped checking its lease locally
fails a test rather than merely behaving oddly under load.

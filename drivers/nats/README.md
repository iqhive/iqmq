# iqmq NATS Core driver

`bitbucket.org/iqhive/iqmq/drivers/nats` is the iqmq [`Backend`][spi] for **NATS
Core**: subject-based publish and subscribe with no persistence and no
per-message settlement.

JetStream is a different driver (`bitbucket.org/iqhive/iqmq/drivers/jetstream`)
over the same server. Nothing here reaches for it, and nothing here pretends to
it.

```go
client, err := nats.Open(ctx, nats.Config{Addrs: []string{"nats://127.0.0.1:4222"}})
if err != nil {
        return err
}
defer client.Close()

if err := client.Publish(ctx, "orders/created", &mq.Message{Body: body}); err != nil {
        return err
}
```

## What you get, and what you do not

| | |
|---|---|
| **Delivery** | at-most-once; the failure mode is loss, not duplication |
| **Settlement** | none — `Settler()` is nil, so every `Ack` reports `ErrNotSettleable` (AckNone) |
| **Confirmation** | `ConfirmAccepted` round-trips PING/PONG; `ConfirmDurable` is a construction error |
| **Competing consumers** | yes — a subscription name is a queue group |
| **Fan-out** | yes — two subscriptions each receive everything |
| **Wildcards** | yes — write the pattern in `ConsumerConfig.Topic` |
| **Request/reply** | yes, native — `Backend.Request` and `Properties.ReplyTo` |
| **Delays, TTL, priority, de-duplication, dead-letter, replay** | none; each is refused with `ErrUnsupported` before any I/O |

If you need any of the last row, or at-least-once delivery, use the JetStream
driver instead.

## Documentation

`docs/drivers/nats.md` is the full page: the subject and queue-group mapping,
the confirmation and acknowledgement boundaries, the header framing rules, the
health transitions, the DSN, and the known gotchas.

## Testing

The module's own tests run against an **embedded** `nats-server` and need no
container:

```
go test ./...
```

The conformance evidence is `the conformance suite`, which runs
the whole suite against the pinned `nats:2.14.6-alpine` image:

```
make integration DRIVER=nats
```

[spi]: https://pkg.go.dev/bitbucket.org/iqhive/iqmq#Backend

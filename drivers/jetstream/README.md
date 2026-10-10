# iqmq JetStream driver

`github.com/iqhive/iqmq/drivers/jetstream` is the iqmq [`Backend`][spi] for
**NATS JetStream**: a persistent stream with durable consumers, per-message
acknowledgement and redelivery.

NATS Core is a different driver (`github.com/iqhive/iqmq/drivers/nats`) over the
same server and the same client library. ADR-010 the specification chose separate modules
so that a caller cannot mistake one profile's capabilities for the other's.

```go
client, err := jetstream.Open(ctx, jetstream.Config{
        Addrs:        []string{"nats://127.0.0.1:4222"},
        Stream:       "ORDERS",
        SubjectPrefix: "acme",
})
if err != nil {
        return err
}
defer client.Close()

// The default confirmation waits for the stream's PubAck, which the server
// sends after the message is committed to storage.
if err := client.Publish(ctx, "orders/created", &mq.Message{Body: body}); err != nil {
        return err
}
```

## What you get, and what you do not

| | |
|---|---|
| **Delivery** | at-least-once; the failure mode is duplication, not loss |
| **Settlement** | explicit — `Ack`, `Nack`, `Nack(NackDelay)`, `Nack(NackDrop)`, `Reject`, `Extend` |
| **Confirmation** | `ConfirmAccepted` and `ConfirmDurable` are the same PubAck; `ConfirmNone` is a core publish that reports nothing |
| **Competing consumers** | yes — a subscription name is a durable pull consumer |
| **Fan-out** | yes — two subscriptions are two durables |
| **Replay and seeking** | yes — by time and by stream sequence |
| **Wildcards** | yes — write the pattern in `ConsumerConfig.Topic` |
| **De-duplication** | publications, within the stream's `Duplicates` window (`Nats-Msg-Id`) |
| **Dead-letter** | a durable publication to `Config.DeadLetterTopic`, then `+TERM`, in that order |
| **Per-message TTL** | conditional: a 2.11+ server and a stream with `AllowMsgTTL` |
| **Scheduled publication** | conditional: a 2.12+ server and a stream with `AllowMsgSchedules` |
| **Priority, partitions, transactions, exactly-once delivery** | none; each is refused with `ErrUnsupported` before any I/O |

## Two things to read before using it

**A stale ack must not settle a redelivery.** JetStream acknowledges by reply
subject, and the server deletes its pending entry by *stream sequence* without
looking at which delivery the reply came from — so the reply subject of a
delivery whose lease expired still works, and acking it acknowledges the record a
later delivery now owns. This driver refuses that settlement with `ErrLeaseLost`
before it reaches the wire, and counts the refusals in
`Backend.StaleSettlements`. The performance budgets makes the unguarded behaviour a
disqualifier.

**`Config.ManageStream` is off by default.** The specification forbids a driver from
provisioning topology behind a caller's back, so a client whose stream does not
exist fails at `Open` with `CodeNotFound`, and a publication to a subject the
bound stream does not capture is refused with `ErrTopicNotFound`.

## Documentation

`docs/drivers/jetstream.md` is the full page: the subject, stream and durable
mapping, the confirmation and settlement boundaries, the dead-letter order, the
header framing rules, the version-gated claims, the DSN, and the known gotchas.

## Testing

The module's own tests run against an **embedded** `nats-server` with JetStream
enabled and need no container:

```
go test ./...
```

The conformance evidence is `conformance/suites/jetstream_test.go`, which
runs the whole suite against the pinned `nats:2.14.6-alpine` image:

```
make integration DRIVER=jetstream
```

and the broker-version matrix, which is what holds the two conditional claims to
the servers that decide them:

```
make matrix DRIVER=jetstream
```

[spi]: https://pkg.go.dev/github.com/iqhive/iqmq#Backend

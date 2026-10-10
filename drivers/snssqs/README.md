# iqmq SNS → SQS driver

Amazon SNS fanning out into Amazon SQS queues. A **topic** is an SNS topic; a
**subscription** is an SQS queue subscribed to it with `RawMessageDelivery`
turned on. One publication reaches every subscribed queue, and each queue's
consumers compete for their own copy.

```go
client, err := snssqs.Open(ctx, snssqs.Config{
    Region:          "eu-west-1",
    AccessKeyID:     "AKIAEXAMPLE",
    SecretAccessKey: "…",
    AccountID:       "000000000000",
})
```

or from a DSN:

```
snssqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&accountid=000000000000
snssqs://test:test@127.0.0.1:4566?region=localstack&insecuretransport=true&autocreate=true
```

The host `aws` means "the services' own regional endpoints"; any other host is
an explicit endpoint for **both** services, which is how LocalStack's edge
endpoint is reached.

## What is worth knowing before you use it

**Settlement is per queue, and nothing makes it otherwise.** One publication
becomes N messages with N receipt handles, N visibility windows and N attempt
counts. A subscriber that acknowledges has acknowledged *its* copy; a message
dead-lettered for one subscription is still being processed by the others.
There is no operation in this driver that settles "the publication", and there
must not be.

**A subscription without `RawMessageDelivery` is refused at open.** Without it
SNS wraps the publisher's body in a JSON notification —
`{"Type":"Notification","Message":"<the real body>",…}` — and moves every
message attribute inside it, so a consumer would receive a body that is not the
one that was published and a message with no headers, key or identifier. There
is no reliable way to tell the two shapes apart afterwards, so the driver
refuses the subscription rather than guessing. `Config.AutoCreate` makes
subscriptions with the attribute set; `TypedAdmin.EnableRawDelivery` sets it on
one that exists. The driver never flips it silently — a subscription somebody
else made without it has consumers that expect the envelope.

**SNS has no per-message delay.** An SQS queue has `DelaySeconds`; SNS has no
timer of any kind, so `mq.Delay` and `PublishOptions.DeliverAt` are refused with
`ErrUnsupported` before any I/O. A *redelivery* delay is still available — it is
a `Nack` with one, against the queue.

**SNS takes a quarter of the message an SQS queue takes**: 256 KiB against a
mebibyte. A body between the two is accepted by `drivers/sqs` and refused here.

**A publication to a topic with no subscriptions is accepted and delivered
nowhere,** and a subscription created after a publication never sees it. That
is the service's behaviour. Call `Admin().EnsureSubscription` before you
publish.

**The consume half IS `drivers/sqs`.** The long poll, the bounded ring, the
local lease check, the supersession bookkeeping, the partial delete batch, the
dead-letter ordering and the flush policy are that driver, opened
over the same credentials and reachable through `Backend.SQS()` for everything
the queue owns — the redrive policy, the message-move task, the queue
attributes.

## Documentation

`docs/drivers/snssqs.md` carries the fifteen fixed sections the driver documentation
requires, including the full mapping table, the service limits read out of the
pinned SDKs, the filter-policy condition and the known gotchas.

## Testing

```
go test ./...                                          # the whole suite, no container
IQMQ_SNSSQS_CONFORMANCE=1 IQMQ_SNSSQS_ADDR=host:port \
  go test -run TestConformance ./...                   # against an endpoint
```

The suite needs no broker: `internal/snsfake` is an in-process SNS server that
fans out into `drivers/sqs/sqsfake` by calling its `SendMessage`, which is what
raw message delivery *is*. It is what makes the four critical scenarios of plan
The specification reachable — a subscription with raw delivery off in both of its shapes,
a publication that reached two queues, a three-of-ten partial `PublishBatch`,
and a filter policy that kept a message out of one queue and not another.

The shipped conformance evidence comes from
`conformance/suites/snssqs_test.go` against the pinned LocalStack container.
LocalStack is the only emulator here — ElasticMQ has no SNS — so the report
carries `evidence: emulator` and is not evidence for the managed service
(layer 4).

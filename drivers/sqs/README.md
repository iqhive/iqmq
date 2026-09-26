# iqmq SQS driver

Amazon SQS, in both of its profiles: **Standard** (at-least-once, unordered,
cheap) and **FIFO** (ordered within a message group, deduplicated,
throughput-limited). One module, two profiles, and the profile is reported
through `DriverInfo.Profile` — never hidden — because seven capability claims
differ between them.

```go
client, err := sqs.Open(ctx, sqs.Config{
    Region:          "eu-west-1",
    AccessKeyID:     "AKIAEXAMPLE",
    SecretAccessKey: "…",
})
```

or from a DSN:

```
sqs://AKIAEXAMPLE:secret@aws?region=eu-west-1&settle=hybrid
sqs://x:x@127.0.0.1:9324/dev-?region=elasticmq&insecuretransport=true
```

The host `aws` means "the SDK's own regional endpoint"; any other host is an
explicit endpoint, which is how ElasticMQ and LocalStack are reached.

## What is worth knowing before you use it

**A stale receipt handle deletes the message anyway.** The service accepts a
`DeleteMessage` for a handle whose message another consumer now holds: the
delete does not fail, and the message is gone. There is no API that asks "do I
still hold this?", so this driver refuses a settlement whose visibility window
has passed **before** the call, with `ErrLeaseLost`. It is a local view — it
cannot see a window another process shortened — and both directions of that
trade cost a redelivery rather than somebody else's work.

**An extension is measured from the deadline, not from now.** The service's
`ChangeMessageVisibility` measures its timeout from the moment of the call and
`Delivery.Extend` promises "at least `by` further into the future", so this
driver sends the *remaining* window plus `by`. An extension whose sum passes
the service's twelve hours is refused with `CodeInvalidArgument` rather than
clamped to the ceiling and reported as a success, and nothing in the portable
API can tell a caller what the extension bought — `DeliveryInfo` is the
snapshot the receive handed over, so a caller has to assume the documented
minimum.

**On the FIFO profile an empty poll does not mean an empty queue.** SQS
delivers nothing else from a message group while one of its messages is in
flight, so a receive on a queue whose every group is blocked returns no
messages, no error and no code. `Admin.Stats` is what tells the two apart: a
blocked group always has a message in flight for `QueueStats.InFlight` to
report.

**A message carries at most ten attributes.** The key, the identifier, the
body-encoding marker and the six typed properties each take one when they are
set, so the budget is mostly the caller's — and the specification's `MetadataMode` decides
what happens to the eleventh: refused under the default strict mode, carried in
an iqmq envelope under `MetadataAdaptive`.

**The body is text.** An SQS body may only carry the XML character set, so
`Config.Body` is the policy: `auto` (the default) passes text through and
base64-encodes what it must, `text` refuses a non-text body before any I/O, and
`base64` encodes everything.

**Settlement is batched, and the policy is a bake-off.** `Config.Settle` is
The specification — `immediate`, `count`, `time`, `hybrid` — and it decides how many
`DeleteMessageBatch` calls a consumer makes. The default is `immediate`, which
is the campaign's answer and not the cheapest arm: coalescing only helps
settlements that arrive together, and a consumer that settles in a loop would
pay a whole interval each. Set `flush.Hybrid` when your handlers settle
concurrently — it is a tenth of the requests.

**`SourceConfig.Subscription` is ignored.** A queue has one set of messages, so
every consumer of one topic competes for them. Two independent subscriptions
need `github.com/iqhive/iqmq/drivers/snssqs`.

## Documentation

`docs/drivers/sqs.md` carries the fifteen fixed sections the driver documentation
requires, including the full mapping table, the service limits read out of the
pinned SDK, the ordering statement delegates to each driver page, and the
known gotchas.

## Testing

```
go test ./...                                    # the whole suite, no container
IQMQ_SQS_CONFORMANCE=1 IQMQ_SQS_ADDR=host:port \
  go test -run TestConformance ./...             # against an endpoint
```

The shipped conformance evidence comes from `the conformance suite`
against the pinned ElasticMQ container, with a LocalStack run behind
`IQMQ_SQS_LOCALSTACK`. Both are emulators, so every report carries
`evidence: emulator` and none of them is evidence for the managed service
(layer 4). The LocalStack run is a cross-check and writes its own report
key, `sqs-standard-localstack.json`: the emulator is not part of
`Config.Profile()`, so without that the two runs would have written one file.

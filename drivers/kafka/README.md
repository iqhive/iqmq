# iqmq Kafka driver (franz-go)

```go
import kq "github.com/iqhive/iqmq/drivers/kafka"

client, err := kq.Open(ctx, kq.Config{Brokers: []string{"kafka-1:9092"}})
```

At-least-once with cumulative acknowledgement. A topic is a Kafka topic, a
subscription is a consumer group, a partition is an ordering lane. `acks=all`
with an idempotent producer; auto-commit off; `cooperative-sticky`.

The full contract is **[`docs/drivers/kafka.md`](../../docs/drivers/kafka.md)**
— all fifteen sections the driver documentation asks of a driver page. Three things
are worth knowing before you read it:

- **`ConfirmAccepted` and `ConfirmDurable` are the same boundary.** Kafka's
  strongest ordinary acknowledgement is already durable, so neither costs an
  extra round trip and neither is refused.
- **Everything that fails after franz-go places a record in a partition's
  buffer is AMBIGUOUS**, not failed: `ErrSettlementUnknown` with `IsAmbiguous`
  true and the native cause underneath. Reporting those as definite failures
  is how a caller publishes twice. A record cancelled while it still waited
  for buffer room was never sent, and its failure is definite.
- **What a `Nack` does is a policy you choose.** Kafka has no per-record
  negative acknowledgement. `Config.Nack` selects one of the four
  strategies; the default holds the record in process and keeps the
  partition's order.

Transactions are `github.com/iqhive/iqmq/x/kafkatx`. The Docker-free test
suite is built on `kfake`, franz-go's in-process broker; the conformance
evidence is `conformance/suites/kafka_test.go` against the pinned
`apache/kafka` image.

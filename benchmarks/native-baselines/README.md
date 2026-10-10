# native-baselines

Native Go clients, measured against the same containers the conformance
harness starts, with the same durability settings the corresponding iqmq
driver will use. These numbers are the denominator of
`BenchmarkCompare/{iqmq,watermill,gocloud,raw}` and the sanity check for
every `BenchmarkDriver/<driver>` claim.

This module **must not depend on iqmq**. It requires only the native clients
and `conformance/harness` (which starts containers and itself imports nothing
from the root module); `TestNoRootImport` in `nativebench` enforces both
halves. A baseline that imported iqmq would be measuring iqmq.

## Layout

```
nativebench/ shared shape: sizes, ops, names, session interface, registry, facts
<client>/             one package per baseline, one BenchmarkNative entry point
```

| Baseline | Client | Broker (harness) | Driver page |
|---|---|---|---|
| `franzgo` | franz-go | redpanda | kafka |
| `kafkago` | segmentio/kafka-go | redpanda | kafka |
| `sarama` | IBM/sarama | redpanda | kafka |
| `nats` | nats.go (core) | nats | nats |
| `jetstream` | nats.go (JetStream) | nats | jetstream |
| `nsq` | go-nsq | nsq | nsq |
| `rueidis` | rueidis | redis | redis |
| `goredis` | go-redis/v9 | redis | redis |
| `amqp091` | amqp091-go | rabbitmq | rabbitmq |
| `goamqp` | Azure/go-amqp | artemis | activemq |
| `pubsub` | cloud.google.com/go/pubsub/v2 | pubsub emulator | gcppubsub |
| `sqs` | aws-sdk-go-v2 service/sqs | elasticmq | sqs |
| `pulsar` | pulsar-client-go | pulsar | pulsar |

`nativebench.Clients()` is the source of truth for that table: it carries the
pinned version (checked against `go.mod` by `TestPinnedVersions`), the
harness broker (checked against `harness.Images`) and the confirmation
boundary each baseline waits for (checked against the driver page by
`TestFactsRecorded`).

## Running

```sh
make bench-native                                  # all baselines
make bench-native CLIENTS="franzgo sarama"
SMOKE=1 COUNT=1 BENCHTIME=50ms make bench-native    # quick shake-out
```

Docker must be available; without it every benchmark skips. An externally
managed broker can be used instead of a container with the harness'
`IQMQ_<BROKER>_ADDR` override.

See `docs/performance/native-baselines.md` for the method and its caveats.

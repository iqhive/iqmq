# Installation

## The library

One module per driver: importing the core never pulls a broker SDK.

```
go get github.com/iqhive/iqmq                       # the core, standard library only
go get github.com/iqhive/iqmq/drivers/kafka         # and one module per broker
```

The core module and `github.com/iqhive/iqmq/dsn/global` depend on the standard
library and nothing else, and `make stdlib-check` holds them to it. Each driver
brings its own client library and only its own; a program that talks to Kafka
never compiles the AWS SDK.

| Broker | Module | DSN scheme |
|---|---|---|
| In-memory (testing, and the reference driver) | `github.com/iqhive/iqmq` (package `memory`) | `memory://`, `mem://` |
| Kafka (franz-go) | `github.com/iqhive/iqmq/drivers/kafka` | `kafka://` |
| Kafka (kafka-go) | `github.com/iqhive/iqmq/drivers/kafka/kafkago` | `kafkago://` |
| NATS Core | `github.com/iqhive/iqmq/drivers/nats` | `nats://` |
| NATS JetStream | `github.com/iqhive/iqmq/drivers/jetstream` | `natsjs://` |
| NATS (both profiles behind one facade) | `github.com/iqhive/iqmq/drivers/natsunified` | `nats://`, `natsjs://` |
| NSQ | `github.com/iqhive/iqmq/drivers/nsq` | `nsq://` |
| Google Cloud Pub/Sub | `github.com/iqhive/iqmq/drivers/gcppubsub` | `gcppubsub://` |
| Amazon SQS | `github.com/iqhive/iqmq/drivers/sqs` | `sqs://` |
| SNS fan-out to SQS | `github.com/iqhive/iqmq/drivers/snssqs` | `snssqs://` |
| RabbitMQ (AMQP 0-9-1) | `github.com/iqhive/iqmq/drivers/rabbitmq` | `rabbitmq://` |
| RabbitMQ (AMQP 1.0) | `github.com/iqhive/iqmq/drivers/rabbitmq/amqp10` | `rabbitmq+amqp10://` |
| ActiveMQ Artemis and Classic (AMQP 1.0) | `github.com/iqhive/iqmq/drivers/activemq` | `activemq://` |
| ActiveMQ (STOMP) | `github.com/iqhive/iqmq/drivers/activemq/stomp` | `activemq+stomp://` |
| Redis Streams (rueidis) | `github.com/iqhive/iqmq/drivers/redis` | `redis://` |
| Redis Streams (go-redis) | `github.com/iqhive/iqmq/drivers/redis/goredis` | opened in Go, no scheme |
| Pulsar | `github.com/iqhive/iqmq/drivers/pulsar` | `pulsar://`, `pulsar+ssl://` |
| Generic AMQP 1.0 core | `github.com/iqhive/iqmq/drivers/amqp10` | a profile the caller supplies |

A driver never reaches for a global registry. It exposes
`Register(dsn.Registrar)` and nothing else, so a program that composes two
brokers owns the registry both are registered with; see
[the driver selection guide](../drivers/index.md).

Two rows above have no DSN scheme, and that is deliberate rather than missing.
`drivers/redis/goredis` is the second client library behind one capability
table, so a connection string cannot choose between it and `drivers/redis`;
`drivers/amqp10` is a core rather than a driver and has no capability table of
its own until a caller supplies a profile. The two NATS rows overlap for the
same kind of reason: the facade answers to both profile schemes, so a program
registers either the facade or the two profile drivers, never both.

## The command-line tool

```
go install github.com/iqhive/iqmq/cmd/iqmq@latest
```

The `iqmq` binary links every shipped driver, so one binary talks to every
broker above — the opposite trade from the library, and the right one for a
tool nobody can be asked to rebuild before debugging a deployment. Start with
`iqmq doctor <dsn>`; the CLI page documents every command,
and the exit-code contract.

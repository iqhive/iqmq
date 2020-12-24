# iqmq

A small Go API for publishing and receiving messages. The first backend is a bounded in-memory queue.

```go
client := mq.NewClient(memory.New(64))
defer client.Close()
client.Publish(ctx, mq.Message{Body: []byte("hello")})
delivery, err := client.Receive(ctx)
if err != nil { return err }
return delivery.Ack(ctx)
```

Import `bitbucket.org/iqhive/iqmq` as `mq` and `bitbucket.org/iqhive/iqmq/memory`. Requires Go 1.15. Run `go test ./...`.

The queue applies backpressure when full. A nack requeues the message. Storage is volatile, so this backend does not provide durable delivery. Licensed under Apache-2.0; see LICENSE and NOTICE.

* `drivers/nats`: Add a NATS Core adapter with explicit at-most-once delivery.

* `drivers/nats`: Add a NATS Core adapter with explicit at-most-once delivery.

* `drivers/rabbitmq`: Publish and settle RabbitMQ messages through streadway AMQP.

* `drivers/kafka/kafkago`: Add Kafka publishing and explicit offset commits.

* `drivers/redis/goredis`: Add Redis Streams consumer groups with go-redis v7.

* `drivers/sqs`: Add SQS message receipt and deletion through the AWS SDK.

* `drivers/snssqs`: Publish SNS notifications and consume their SQS subscription.

* `drivers/kafka`: Add the franz-go Kafka client as an alternate backend.

* `drivers/kafka`: Add the franz-go Kafka client as an alternate backend.

* `drivers/gcppubsub`: Add Google Pub/Sub publication and subscription delivery.

* `drivers/nsq`: Add NSQ publishing and deferred settlement.

* `drivers/pulsar`: Add Pulsar producers and individual acknowledgements.

* `drivers/amqp10`: Add AMQP 1.0 senders and receivers.

* `drivers/activemq`: ActiveMQ Classic and Artemis through existing AMQP 1.0 links. Broker provisioning remains caller-owned.

* `drivers/activemq/stomp`: Add ActiveMQ STOMP subscription settlement.

* `drivers/activemq/stomp`: Add ActiveMQ STOMP subscription settlement.

## Delivery limits

Core NATS is at most once. Kafka offset commits are explicit in the kafka-go adapter; the early franz-go adapter uses manual assignment and does not expose commits. Redis pending entries require caller-managed recovery. SNS subscriptions must use raw delivery. SQS accepts text bodies. The AMQP 1.0 adapter shares its implementation with ActiveMQ; STOMP requires client-individual acknowledgements. Provision destinations and configure native clients before constructing an adapter.

Each adapter has its own module. Run `go test ./...` inside the module you use. The root remains standard library only.

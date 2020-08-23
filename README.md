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

Import `bitbucket.org/iqhive/iqmq` as `mq` and `bitbucket.org/iqhive/iqmq/memory`. Requires Go 1.14. Run `go test ./...`.

The queue applies backpressure when full. A nack requeues the message. Storage is volatile, so this backend does not provide durable delivery. Licensed under Apache-2.0; see LICENSE and NOTICE.

* `drivers/nats`: Add a NATS Core adapter with explicit at-most-once delivery.

* `drivers/nats`: Add a NATS Core adapter with explicit at-most-once delivery.

* `drivers/rabbitmq`: Publish and settle RabbitMQ messages through streadway AMQP.

* `drivers/kafka/kafkago`: Add Kafka publishing and explicit offset commits.

* `drivers/redis/goredis`: Add Redis Streams consumer groups with go-redis v7.

* `drivers/sqs`: Add SQS message receipt and deletion through the AWS SDK.

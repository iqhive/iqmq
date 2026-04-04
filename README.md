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

Import `bitbucket.org/iqhive/iqmq` as `mq`. Requires Go 1.26.0; run `go test ./...`.

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

## Runner

`Client.Run` processes and settles messages in order. A handler failure requeues the delivery when the backend supports nack.

## AWS SDK v2

SQS and SNS now accept stable AWS SDK v2 clients. Recreate native clients with the v2 configuration package before calling `New`. Destinations and receipt semantics are unchanged.

* `drivers/jetstream`: Add persistent NATS JetStream delivery and acknowledgements.

Adopt the STOMP v3 module without changing settlement. Existing client handles must use `github.com/go-stomp/stomp/v3`.

Adopt the RabbitMQ-maintained AMQP client. Existing client handles must use `github.com/rabbitmq/amqp091-go`.

## Json

`codec.Encode` and `codec.Decode` use JSON. Malformed bodies return decoding errors.

* `drivers/redis`: Add Redis Streams through the Rueidis command builder.

* `drivers/redis`: Add Redis Streams through the Rueidis command builder.

* `drivers/redis`: Add Redis Streams through the Rueidis command builder.

## Typed

`typed.NewPublisher[T]` binds JSON publication to a Go type. This package requires Go 1.27.0.

## Compatibility

The API remains pre-1.0. Client major-version migrations change the native handles accepted by adapters and are documented with each change. Test a module with its own dependencies, outside a workspace.

## Batch

`PublishBatch` accepts up to 1024 messages and reports the completed prefix on failure. It does not claim atomic publication.

The go-redis adapter now accepts v9 clients. Its native commands receive context arguments.

Use the Redis organisation path for Rueidis. Existing client handles must use `github.com/redis/rueidis`.

## Middleware

`Chain` composes handler middleware in the supplied order.

## Async

`NewAsyncPublisher` bounds pending publication and drains accepted work on close. Each accepted message returns a result channel.

## Requestreply

`reqreply` matches replies by correlation ID, limits pending requests and removes cancelled calls. The application transports the ID alongside its request.

## Compatibility

The API remains pre-1.0. Client major-version migrations change the native handles accepted by adapters and are documented with each change. Test a module with its own dependencies, outside a workspace.

## Pub/Sub v2

The Google adapter now accepts v2 publishers and subscribers. The previous v1 adapter remains available at `drivers/gcppubsub/v1compat`. Create a v2 publisher with `client.Publisher(topic)` and a subscriber with `client.Subscriber(subscription)`.

## Capabilities

`Client.Capabilities` returns explicit adapter claims. An adapter without a provider reports no established guarantees. Memory offers ack and requeue but no durable storage.

## API migration

The earlier client API now lives under `bitbucket.org/iqhive/iqmq/legacy` while the concrete-handle API and richer driver contracts are integrated. Existing adapters use that compatibility package during this transition. The final pre-1.0 release completes the migration and removes the temporary boundary.

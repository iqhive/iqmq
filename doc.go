// Package iqmq provides one small, allocation-conscious, capability-honest,
// bytes-first API for publishing and consuming messages across Apache Kafka,
// NATS (Core and JetStream), NSQ, Google Cloud Pub/Sub, Amazon SQS, RabbitMQ,
// ActiveMQ, Redis Streams and Apache Pulsar, plus a deterministic in-process
// memory engine that doubles as the canonical test double.
//
// The package is conventionally imported under the alias mq:
//
//	import mq "github.com/iqhive/iqmq"
//
// # What is here
//
// This package exports the frozen API: the value types
// (Message and Headers, the capability model, the publication options, the
// error model), the Delivery and its settlement, the Client, PublisherHandle
// and ConsumerHandle, and the driver SPI every backend is written against.
// Every shape was raced against its alternatives under the bake-off protocol
// before it was chosen:
//
//   - Message.Key is []byte, converted at the driver boundary.
//   - Headers is an ordered slice of key/[]byte pairs, so the duplicates and
//     the insertion order survive.
//   - A PublishOption is a value token and not a closure, so a call site
//     allocates nothing.
//   - Capabilities answers Has from an array indexed by Feature.
//   - Errors are preallocated and errors.Is matches by code.
//   - A Delivery embeds its Message by value, so a received message is not a
//     separate allocation, and it is settled through a
//     per-delivery settlement object.
//   - Receive fills a caller's slice of Delivery values, so a batch costs no
//     container and a message no allocation.
//   - Client, PublisherHandle and ConsumerHandle are concrete structs behind
//     narrow interfaces, taking config structs, with a prompt Close and a graceful Shutdown.
//   - A publisher is bound to its destination and the
//     driver's batch method is optional: a driver that cannot batch
//     implements one method and the core loops.
//
// # Settlement
//
// Exactly one settlement of a delivery wins, whatever the driver does. A
// second reports ErrAlreadySettled, one racing another ErrSettlementInProgress,
// one whose delivery is gone ErrLeaseLost, one after the consumer closed
// ErrClosed, and a delivery from a backend with no settlement semantics
// ErrNotSettleable. An ambiguous native outcome reports ErrSettlementUnknown
// and stays unknown: nothing later turns it into success.
package iqmq

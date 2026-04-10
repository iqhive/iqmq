// Package iqmq provides one small, allocation-conscious, capability-honest,
// bytes-first API for publishing and consuming messages across Apache Kafka,
// NATS (Core and JetStream), NSQ, Google Cloud Pub/Sub, Amazon SQS, RabbitMQ,
// ActiveMQ, Redis Streams and Apache Pulsar, plus a deterministic in-process
// memory engine that doubles as the canonical test double.
//
// The package is conventionally imported under the alias mq:
//
//	import mq "bitbucket.org/iqhive/iqmq"
//
// # What is here
//
// This package exports the frozen API of docs/api-freeze.md: the value types
// (Message and Headers, the capability model, the publication options, the
// error model), the Delivery and its settlement, the Client, PublisherHandle
// and ConsumerHandle, and the driver SPI every backend is written against.
// Every shape was raced in x/apibench (removed at the M6 disposition review,
// ADR-025) under the bake-off protocol and decided by the ADR named beside it:
//
//   - Message.Key is []byte, converted at the driver boundary (ADR-006).
//   - Headers is an ordered slice of key/[]byte pairs, so the duplicates and
//     the insertion order survive (ADR-006).
//   - A PublishOption is a value token and not a closure, so a call site
//     allocates nothing (ADR-006).
//   - Capabilities answers Has from an array indexed by Feature (ADR-007).
//   - Errors are preallocated and errors.Is matches by code (ADR-011).
//   - A Delivery embeds its Message by value, so a received message is not a
//     separate allocation (ADR-005), and it is settled through a
//     per-delivery settlement object (ADR-005).
//   - Receive fills a caller's slice of Delivery values, so a batch costs no
//     container and a message no allocation (ADR-006).
//   - Client, PublisherHandle and ConsumerHandle are concrete structs behind
//     narrow interfaces (ADR-003), taking config structs (ADR-006), with a prompt Close and a graceful Shutdown (ADR-006).
//   - A publisher is bound to its destination (ADR-021) and the
//     driver's batch method is optional: a driver that cannot batch
//     implements one method and the core loops (ADR-021).
//
// # Settlement
//
// Exactly one settlement of a delivery wins, whatever the driver does. A
// second reports ErrAlreadySettled, one racing another ErrSettlementInProgress,
// one whose delivery is gone ErrLeaseLost, one after the consumer closed
// ErrClosed, and a delivery from a backend with no settlement semantics
// ErrNotSettleable. An ambiguous native outcome reports ErrSettlementUnknown
// and stays unknown: nothing later turns it into success.
//
// # What is not here yet
//
// The runner (Run, RunBatch, the retry, ordering and dead-letter policies),
// the asynchronous and owned publication paths, lease extension and the
// typed and request/reply layers are later increments'. See API requirements
// for the design contract and docs/status.md for progress.
package iqmq

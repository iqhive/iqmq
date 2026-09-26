// Package memory is the in-process iqmq driver: a [github.com/iqhive/iqmq.Backend]
// over the memory engine of the memory engine.
//
// It is two things at once, which is the goal: a best-in-class in-process
// queue, and a faithful stand-in for a network broker. Partitions, consumer
// groups, fan-out, per-message acknowledgement with leases, negative
// acknowledgement with delay, scheduled delivery, time to live, dead-letter
// routing, optional retention and replay, optional wildcards, priorities and
// deduplication are all here, under an injectable clock. Nothing is
// persistent, and the driver never advertises durable storage.
//
// # Opening
//
//	client, err := memory.Open(ctx, memory.Config{AutoCreate: true})
//
// [Open] performs no I/O and creates no topology: AutoCreate is false by
// default, consistent with every network driver (ADR-016).
// [NewForTest] is the testing form — AutoCreate on, a deterministic clock, and
// the client closed on cleanup — and it hands back the [Broker] so that a test
// can inspect the engine, wait for it to go idle and inject faults.
//
// # What the core owns and what this driver owns
//
// This package is a driver and nothing more. Worker pools, retries,
// dead-letter accounting and ordering lanes belong to the core, so
// nothing here implements a second one. What it does own is the mapping of
// [github.com/iqhive/iqmq.Message] and its option sets onto the engine's
// publication record, the filling of a [github.com/iqhive/iqmq.DriverDelivery]
// from a claim, and the settlement of that claim under the generation the
// claim carried.
//
// # Ownership
//
// The default is ADR-009's safe copy: [github.com/iqhive/iqmq.Sink.Publish]
// clones the message once on admission and hands the caller its own storage
// straight back, so a publisher may reuse its buffers the instant Publish
// returns. The two other modes are reached through the optional SPI
// surfaces: OwnedSink transfers the storage and releases it exactly once, and
// AsyncSink borrows it until the completion callback runs.
//
// A received message's Body, Key and header values alias the engine's own
// storage and are valid until the delivery is settled, which is the safe
// receive mode. Nothing is recycled unless the consumer asked for it with
// ConsumerConfig.BufferReuse.
//
// # Documentation
//
// The driver pages carries the fixed per-driver sections the specification requires,
// including the ordering statement delegates to each driver page.
package memory

// Package kafkago is a Kafka driver over segmentio/kafka-go.
//
// It is the SECOND Kafka driver in this repository and it stands behind the
// same contract as the first: [github.com/iqhive/iqmq/drivers/kafka] is the
// franz-go driver, this one is kafka-go, and the specification is the bake-off
// between them. The two drivers are wire-compatible with each
// other by construction — the same reserved headers, the same topic grammar,
// the same murmur2 partition assignment — and `TestCrossDriver*` in this
// package is what holds them to it: a message published by one is decoded
// field for field by the other, and a key lands on the same partition under
// both.
//
// # Semantics
//
// At-least-once, cumulative acknowledgement. A topic is a Kafka topic, a
// subscription is a consumer group, and a partition is an ordering lane: the
// core's ordering machinery reads
// [github.com/iqhive/iqmq.DeliveryInfo.Partition] and serialises a lane, which
// is what makes per-key order hold end to end. Auto-commit is off — the driver
// drives the group protocol itself — and the offset committed is the one
// driverkit/offset's tracker says is safe, never an offset whose record is
// still in flight.
//
// # What kafka-go is and what that costs
//
// kafka-go's consumer comes in two halves and this driver uses NEITHER of the
// convenient ones whole.
//
// For MEMBERSHIP it uses [kafka.ConsumerGroup], which hands out one
// [kafka.Generation] at a time and lets the caller own everything, rather than
// [kafka.Reader], which owns group membership and hides it. The reason is a Reader never tells its caller that a rebalance happened, so a driver
// built on it cannot refuse a settlement for a partition it no longer owns —
// it would acknowledge a record another member is processing. A Generation is
// the assignment, its context is cancelled the moment the assignment ends, and
// that is exactly the boundary ErrLeaseLost is drawn at.
//
// For READING it uses [kafka.Client.Fetch] rather than a per-partition
// [kafka.Reader], and that is a Kafka record value is a NULLABLE byte
// string, and a Reader reads records through kafka-go's legacy `Conn`/`Batch`
// path, whose `readNewBytes` returns nil for any length at or below zero — so
// it collapses an empty body and an absent one into the same nil Body. The
// modern protocol path keeps them apart. Fetching directly also puts the
// consume path on the same [kafka.Transport] as the producer and the
// administrative client.
//
// Three differences from the franz-go driver follow from the client and are
// not choices this driver made. Each is a claim in the capability table or a
// line on the driver page, never a silent divergence.
//
//   - THERE IS NO IDEMPOTENT PRODUCER. kafka-go's [kafka.Writer] sends no
//     producer id and no sequence numbers, so the broker cannot de-duplicate
//     the producer's own retries: a retried produce whose first attempt was
//     written and whose response was lost leaves TWO copies in the log.
//     FeatureIdempotentPublish is therefore unsupported here and native in the
//     franz-go driver, and `Config.MaxAttempts` is the number of duplicates a
//     record may produce.
//   - THERE IS NO COOPERATIVE-STICKY BALANCER. kafka-go offers range,
//     round-robin and rack-affinity assignors, all of them EAGER: every
//     rebalance revokes the WHOLE assignment and hands it back, so a member
//     that gains one partition loses and regains the ones it already had. A
//     rebalance therefore invalidates every in-flight delivery of every
//     partition rather than of the partitions that moved, and the
//     at-least-once window opens for the whole assignment instead of for one
//     partition.
//   - A PRODUCE IS NOT FLUSHED WHEN THE CALLER STOPS PUBLISHING. kafka-go
//     batches per partition and flushes on a full batch or a timer; there is
//     no "nothing else is pending, send it" path, so a synchronous Publish
//     with a batch size above one waits out [Config.BatchTimeout]. The default
//     batch size is therefore ONE — a synchronous publisher pays no linger —
//     and a caller that publishes in batches raises
//     [Config.BatchMaxRecords] and pays the timer.
//
// # The confirmation boundary
//
// The producer runs with acks=all, which is NOT kafka-go's own default
// (RequireNone, fire and forget). A record's reply therefore arrives when
// every in-sync replica has the record in its log, which means
// ConfirmAccepted and ConfirmDurable are the SAME boundary here, exactly as
// they are on franz-go: Kafka's strongest ordinary acknowledgement is already
// a durable one, so neither mode costs an extra round trip and neither is
// refused. ConfirmNone hands the record to the writer and returns.
//
// # The ambiguity that matters
//
// kafka-go documents the produce path's ambiguity in its own words: when
// `WriteMessages` returns early because the caller's context was cancelled,
// "there are no guarantees made on whether messages were written to kafka,
// they might also still be written after this method has already returned".
// That is the ambiguous outcome verbatim, and it is wider here than on
// franz-go: the produce runs on a goroutine of the writer's with a deadline
// of its own, so a cancelled Publish does not cancel the write. Every failure
// reported after the record entered the writer — a cancellation, a deadline,
// a retry budget spent, a NOT_ENOUGH_REPLICAS_AFTER_APPEND, a
// REQUEST_TIMED_OUT — is ErrSettlementUnknown (CodeSettlementUnknown) with
// IsAmbiguous true and the native cause underneath. Reporting any of them as
// a definite failure is how a caller publishes twice and believes it
// published once.
//
// # The ack boundary and the at-least-once window
//
// A delivery is acknowledged when its offset has completed in the partition's
// tracker AND the resulting watermark has been committed to the group
// coordinator. The two are not simultaneous: a commit is a round trip, so the
// driver commits on a ticker ([Config.CommitInterval], 250 ms by default),
// synchronously when a generation ends, and synchronously on Close. The
// at-least-once window is therefore "everything completed since the last
// commit", and [Backend.Commit] is how a caller narrows it to zero at a point
// of its own choosing.
//
// # Negative settlement
//
// Kafka has no per-record nack, so what a Nack does is a policy, and this
// driver takes it ([github.com/iqhive/iqmq/driverkit/nack]) — the same strategies the
// franz-go driver applies, with the same default. Block-partition holds the
// record in process, pauses the partition by not reading it, leaves the
// offset uncommitted and redelivers after the delay; it is the only strategy
// that changes neither ordering nor durability. Retry topics
// (`<topic>.retry.<n>`), stop-the-consumer and a custom function are the
// other three and are selected explicitly in [Config.Nack].
//
// # Maturity and tier
//
// Tier 1 while the specification is open: the driver ships with this
// repository, is covered by the conformance suite against the pinned
// broker image and is measured by the driver benchmarks. The specification ends
// "loser demoted to community", so ONE of the two Kafka drivers will be
// demoted once ADR-013 is ratified; until then neither is the canonical one
// and the driver page says so.
package kafkago

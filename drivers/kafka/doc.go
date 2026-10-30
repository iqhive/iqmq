// Package kafka is the Kafka driver, over franz-go.
//
// # Semantics
//
// At-least-once, cumulative acknowledgement. A topic is a Kafka topic, a
// subscription is a consumer group, and a partition is an ordering lane: the
// core's ordering machinery reads [github.com/iqhive/iqmq.DeliveryInfo.Partition]
// and serialises a lane, which is what makes per-key order hold end to end.
// Auto-commit is off; the driver commits the offset of the next record to
// consume, computed by driverkit/offset's tracker, and never an offset whose
// record is still in flight.
//
// # The confirmation boundary
//
// The producer runs with acks=all and idempotence, which is franz-go's own
// default and is not turned off by this driver unless
// [Config.DisableIdempotency] says so. A record's promise therefore fires when
// every in-sync replica has the record in its log, which means
// ConfirmAccepted and ConfirmDurable are the SAME boundary here: Kafka's
// strongest ordinary acknowledgement is already a durable one, so neither mode
// costs an extra round trip and neither is refused. ConfirmNone returns as
// soon as the record is in the producer's buffer.
//
// # The ambiguity that matters
//
// `ProduceSync`'s promise is the durable confirmation, and everything that can
// go wrong after franz-go has placed the record in a partition's buffer is
// AMBIGUOUS rather than failed: a context cancellation, a deadline, a record
// timeout, a client close, a `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. In every one
// of those cases the record may be in the log, so ErrSettlementUnknown
// (CodeSettlementUnknown) is the answer, and a caller that retries knows it
// may duplicate. Reporting any of them as a definite failure is how a caller
// publishes twice and believes it published once, which is why sink.go carries
// the record through every path to the mapping and errors.go marks the rules
// Ambiguous. A record that failed before it was placed, while Produce waited
// for room in a full buffer or for its topic's partitions, was never sent,
// and that failure is definite, whatever the rule for its error says.
//
// # The ack boundary and the at-least-once window
//
// A delivery is acknowledged when its offset has completed in the partition's
// tracker AND the resulting watermark has been committed to the group
// coordinator. The two are not simultaneous: a commit is a round trip, so the
// driver commits on a ticker ([Config.CommitInterval], 250 ms by default),
// synchronously in `OnPartitionsRevoked`, and synchronously on Close. The
// at-least-once window is therefore "everything completed since the last
// commit": a process that dies between a completion and the commit that would
// have carried it redelivers those records. That window is a configuration
// choice rather than a defect — a commit per record would cost a round trip
// per record — and [Backend.Commit] is how a caller narrows it to zero at a
// point of its own choosing.
//
// A rebalance closes the window rather than widening it. `OnPartitionsRevoked`
// stops dispatch for the revoked partitions, waits a bounded time for their
// in-flight deliveries to settle ([Config.RevokeWait]), commits the safe
// boundary synchronously, and then invalidates the generation, so a settlement
// that arrives afterwards reports ErrLeaseLost instead of acknowledging a
// record another member is now processing. `OnPartitionsLost` does the same
// without the commit, because a lost partition is one the coordinator has
// already taken away and a commit for it would either fail or overwrite
// another member's position.
//
// # Negative settlement
//
// Kafka has no per-record nack, so what a Nack does is a policy, and this
// driver takes it ([github.com/iqhive/iqmq/driverkit/nack]).
// The default is block-partition: the record is held in process, the partition
// is paused, the offset is not committed, and the record is redelivered after
// the delay — the only strategy that changes neither ordering nor durability.
// Retry topics (`<topic>.retry.<n>`), stop-the-consumer and a custom function
// are the other three and are selected explicitly in [Config.Nack], because
// two of them reorder a partition and one of them halts a subscription.
//
// # Maturity and tier
//
// Tier 1: the driver ships with this repository, is covered by the conformance suite against the pinned broker image, and is measured by
// the driver benchmarks. Maturity: experimental until the v1 defaults freeze.
package kafka

package iqmq

import "bitbucket.org/iqhive/iqmq/internal/stats"

// statsWorkers is how many cells each counter is striped over.
//
// the winner is the striped layout (ADR-017, Striped), which trades
// memory for the contended increment: Load sums every cell, so a cell per CPU
// would cost a page per counter on a large host for counters that are written
// by the settling goroutines — a number bounds by the lane count, not by
// GOMAXPROCS. Eight cache lines per counter keeps a handle's whole counter set
// in a few kilobytes and still spreads the writers.
const statsWorkers = 8

// ConsumerStats is what one consumer has counted.
//
// The counters are read with one load per cell and are consistent only
// individually: a snapshot taken while messages are arriving may show a
// Received that is already ahead of InFlight. Each is exact once the
// consumer is quiescent.
type ConsumerStats struct {
	// Received is how many deliveries the consumer has handed out.
	Received int64
	// Acked is how many were settled positively.
	Acked int64
	// Nacked is how many were settled negatively, including the
	// dead-lettered ones.
	Nacked int64
	// DeadLettered is how many negative settlements asked for the
	// dead-letter destination -- the NATIVE route, Nack(NackDeadLetter()),
	// where the broker moves the message and the same call settles the
	// delivery. Every one of them is also counted in Nacked.
	DeadLettered int64
	// DeadLetterPublished is how many messages the runner dead-lettered
	// through OWN route: a copy published to the dead-letter topic
	// and the delivery then acked, which is the route every driver whose
	// broker has no dead-letter destination of its own takes.
	//
	// It is a second counter rather than a wider meaning for DeadLettered,
	// because the two routes settle the delivery in opposite directions and
	// one number cannot say both: a native dead-letter is a NACK and is
	// counted in Nacked, a published one is an ACK and is counted in Acked.
	// Widening DeadLettered would have made Nacked + DeadLettered stop
	// reconciling for the native route, which is the reason the specification recorded
	// the gap rather than closing it. The total dead-lettered is
	// DeadLettered + DeadLetterPublished.
	DeadLetterPublished int64
	// Redelivered is how many deliveries the broker reported as
	// redeliveries.
	Redelivered int64
	// InFlight is how many deliveries have been handed out and not yet
	// settled. It returns to zero once every delivery is settled and when
	// the consumer closes, because a closed consumer holds nothing: its
	// source has requeued or abandoned what it had.
	InFlight int64
	// SettleFailures is how many settlements failed, ambiguous outcomes
	// included (the iqmq_settle_failures_total).
	SettleFailures int64
	// LeaseExtensions is how many lease extensions succeeded: the specification's
	// iqmq_lease_extensions_total{outcome="success"}. It counts both the
	// runner's shared scheduler and a caller's own Delivery.Extend, because
	// the metric is about the broker's leases and not about who asked.
	LeaseExtensions int64
	// LeaseExtensionFailures is the other half of the same metric,
	// iqmq_lease_extensions_total{outcome="failure"}: extensions the driver
	// refused, could not confirm, or had no lease left to move. An
	// extension is not a settlement, so it is never counted in
	// SettleFailures.
	LeaseExtensionFailures int64
	// DeadLetterFailures is how many dead-letter publications the runner
	// could not confirm. ADR-011 requires a failed or ambiguous dead-letter
	// publish to be "requeued and counted", and this is the count: the
	// message was NOT acked and is redeliverable, so a rising number is a
	// dead-letter destination that is not accepting messages rather than
	// data lost.
	DeadLetterFailures int64
	// Lag is how far behind the subscription is, when the driver reports it.
	// No SPI carries lag at v0, so Known is false; Admin.Stats is where a
	// broker's own answer is available today.
	Lag Limit[int64]
}

// consumerCounters is the live counter set behind ConsumerStats.
type consumerCounters struct {
	received       stats.Counter
	acked          stats.Counter
	nacked         stats.Counter
	deadLettered   stats.Counter
	redelivered    stats.Counter
	inflight       stats.Counter
	settleFailures stats.Counter
	extensions     stats.Counter
	extensionFails stats.Counter
	dlqFailures    stats.Counter
	dlqPublished   stats.Counter
}

// newConsumerCounters builds a zeroed counter set.
func newConsumerCounters() consumerCounters {
	return consumerCounters{
		received:       stats.New(stats.Auto, statsWorkers),
		acked:          stats.New(stats.Auto, statsWorkers),
		nacked:         stats.New(stats.Auto, statsWorkers),
		deadLettered:   stats.New(stats.Auto, statsWorkers),
		redelivered:    stats.New(stats.Auto, statsWorkers),
		inflight:       stats.New(stats.Auto, statsWorkers),
		settleFailures: stats.New(stats.Auto, statsWorkers),
		extensions:     stats.New(stats.Auto, statsWorkers),
		extensionFails: stats.New(stats.Auto, statsWorkers),
		dlqFailures:    stats.New(stats.Auto, statsWorkers),
		dlqPublished:   stats.New(stats.Auto, statsWorkers),
	}
}

// recordExtension counts one lease extension by its outcome, which is the iqmq_lease_extensions_total{outcome}. It has one caller — the
// settlement record's extend — so the runner's shared scheduler and a
// caller's own Delivery.Extend are counted in one place and cannot
// double-count each other.
func (c *consumerCounters) recordExtension(err error) {
	if err != nil {
		c.extensionFails.Add(1)
		return
	}
	c.extensions.Add(1)
}

// snapshot reads every counter.
func (c *consumerCounters) snapshot() ConsumerStats {
	return ConsumerStats{
		Received:       c.received.Load(),
		Acked:          c.acked.Load(),
		Nacked:         c.nacked.Load(),
		DeadLettered:   c.deadLettered.Load(),
		Redelivered:    c.redelivered.Load(),
		InFlight:       c.inflight.Load(),
		SettleFailures: c.settleFailures.Load(),

		LeaseExtensions:        c.extensions.Load(),
		LeaseExtensionFailures: c.extensionFails.Load(),
		DeadLetterFailures:     c.dlqFailures.Load(),
		DeadLetterPublished:    c.dlqPublished.Load(),
	}
}

// PublisherStats is what one publisher has counted.
type PublisherStats struct {
	// Published is how many messages the driver accepted.
	Published int64
	// Failed is how many it refused or could not confirm.
	Failed int64
	// Pending is how many publications have been admitted and not yet
	// completed. It returns to zero once the publisher is quiescent.
	Pending int64
	// BackpressureEvents is how many publications met a full client-side
	// buffer.
	BackpressureEvents int64
	// Drops is how many publications were dropped by a drop-on-full
	// backpressure mode, which is never a default and always counted.
	Drops int64
}

// publisherCounters is the live counter set behind PublisherStats.
type publisherCounters struct {
	published    stats.Counter
	failed       stats.Counter
	pending      stats.Counter
	backpressure stats.Counter
	drops        stats.Counter
}

// newPublisherCounters builds a zeroed counter set.
func newPublisherCounters() publisherCounters {
	return publisherCounters{
		published:    stats.New(stats.Auto, statsWorkers),
		failed:       stats.New(stats.Auto, statsWorkers),
		pending:      stats.New(stats.Auto, statsWorkers),
		backpressure: stats.New(stats.Auto, statsWorkers),
		drops:        stats.New(stats.Auto, statsWorkers),
	}
}

// snapshot reads every counter.
func (p *publisherCounters) snapshot() PublisherStats {
	return PublisherStats{
		Published:          p.published.Load(),
		Failed:             p.failed.Load(),
		Pending:            p.pending.Load(),
		BackpressureEvents: p.backpressure.Load(),
		Drops:              p.drops.Load(),
	}
}

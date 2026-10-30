package iqmq

// Ordering is how the runner routes deliveries to lanes (the specification's
// "partition | hash(key) % lanes | round-robin for keyless").
//
// It decides which deliveries are ordered with respect to each other, because
// one lane is one worker at a time: two deliveries on one lane are handled in
// arrival order, and two on different lanes are not ordered at all. The specification's
// other conditions are not this option's to give — the profile has to support
// the scope, the messages have to carry a key, and exactly one handler runs
// per lane — so a runner configured for an order the broker does not keep
// still sees what the broker sends.
type Ordering uint8

// The ordering modes.
const (
	// OrderingByKey routes a delivery by the hash of its key, so every
	// delivery of one key is handled by one worker and in arrival order. A
	// delivery with no key may be handled anywhere and is spread over the
	// lanes in turn. It is the zero value and the default.
	OrderingByKey Ordering = iota
	// OrderingUnordered routes every delivery as if it had no key: the
	// lanes are a work pool and nothing is ordered against anything. It is
	// what a caller asks for when throughput matters and the messages are
	// independent — two deliveries of one key may then be handled at the
	// same time, on different workers.
	OrderingUnordered
	// OrderingByPartition routes by the source partition the driver
	// reported, so one partition is one lane whatever the keys in it are.
	// It is the partition scope and ADR-011's "one serial worker per
	// assigned partition". A delivery whose driver reports no partition has
	// none to be ordered by and is routed as a keyless one.
	OrderingByPartition

	orderingCount
)

var orderingNames = [orderingCount]string{"bykey", "unordered", "bypartition"}

// String returns the ordering's lower-case name.
func (o Ordering) String() string {
	if o < orderingCount {
		return orderingNames[o]
	}
	return "Ordering(?)"
}

// Valid reports whether o names an ordering.
func (o Ordering) Valid() bool { return o < orderingCount }

// OrderingMode is the retry interaction: what a failed delivery does to
// the deliveries behind it on its lane.
type OrderingMode uint8

// The retry interactions.
const (
	// OrderingStrict blocks the lane on a failed delivery until it is
	// settled or terminally routed: the runner holds the delivery, waits out
	// the backoff on the lane's own worker and calls the handler again,
	// rather than giving the message back to the broker and letting the
	// deliveries behind it overtake the redelivery. It is the default
	// when ordering is enabled, which is what WithOrderingByKey() with no
	// argument selects.
	//
	// Holding a delivery holds its lease with it, so a strict runner over a
	// driver that grants leases wants WithLeaseExtension: without it a
	// backoff longer than the lease ends with the broker redelivering the
	// message underneath the handler.
	//
	// The attempts the runner makes on a held delivery count towards
	// WithMaxAttempts only while it holds it. A delivery it gives back, when
	// the run stops or, under WithLeaseDeadline, when a postponement would
	// outlive half of the lease, takes the broker's count alone with it, so
	// the bound restarts from there (see WithMaxAttempts).
	OrderingStrict OrderingMode = iota
	// OrderingRelaxed lets the deliveries behind a failed one carry on: the
	// message goes back to the broker with the backoff and is redelivered
	// whenever the broker is ready, which may be after messages that were
	// behind it. It is what the runner does when nothing asked for an order.
	OrderingRelaxed
	// OrderingStopOnFailure stops the runner on the first failure a handler
	// reports, so that the broker's own redelivery decides what happens next
	// and nothing overtakes the failed message. The message is requeued
	// first, so stopping loses nothing, and Run reports a *PoisonError.
	// Under SettlementManual the runner stops the same way and leaves the
	// message to the handler, which owns its settlement.
	OrderingStopOnFailure

	orderingModeCount
)

var orderingModeNames = [orderingModeCount]string{"strict", "relaxed", "stoponfailure"}

// String returns the mode's lower-case name.
func (m OrderingMode) String() string {
	if m < orderingModeCount {
		return orderingModeNames[m]
	}
	return "OrderingMode(?)"
}

// Valid reports whether m names a retry interaction.
func (m OrderingMode) Valid() bool { return m < orderingModeCount }

// WithOrdering chooses what shares a lane.
//
// The default is OrderingByKey. It is the routing half and the one
// setting lanes.route reads; the retry half is WithOrderingByKey's.
func WithOrdering(o Ordering) RunOption {
	return RunOption{field: runFieldOrdering, num: int64(o)}
}

// WithOrderingByKey routes by key and asks for the retry interaction
//
// With no argument it selects OrderingStrict, which is the default when
// ordering is enabled: a failed delivery blocks its lane until it is settled
// or terminally routed, so nothing behind it overtakes its redelivery.
// More than one mode is a mistake and is refused.
func WithOrderingByKey(mode ...OrderingMode) RunOption {
	m := OrderingStrict
	if len(mode) > 0 {
		m = mode[0]
	}
	if len(mode) > 1 {
		// A second mode names a second answer to a question with one answer.
		// The option carries the refusal rather than the first value,
		// because silently taking mode[0] is how a caller's second thought
		// disappears.
		return RunOption{field: runFieldOrderingMode, num: -1}
	}
	return RunOption{field: runFieldOrderingMode, num: int64(m)}
}

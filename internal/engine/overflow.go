package engine

// OverflowPolicy decides what a publication does when the lane it targets has
// no room for it (the memory engine TopologyConfig.Overflow, layer 2
// BenchmarkMemory/Overflow).
//
// Overflow and the fan-out policy answer two different questions
// and compose: the fan-out policy decides which subscriptions a publication
// must reach, and the overflow policy decides what happens when it cannot
// reach them. Every policy but OverflowDropOldest is a modifier on the
// outcome the fan-out policy computed:
//
//	Block       the publisher waits for room instead of being refused
//	Reject      the publisher is refused with errcode.ErrQueueFull
//	DropNewest  the publication is discarded, counted and reported as taken
//	DropOldest  the lane makes room by discarding the record it has held
//	            longest, and refuses only when it holds nothing to discard
//
// A drop is never silent: DropNewest counts the discarded publication in
// Snapshot.Overflowed on every subscription that had no room for it, and
// DropOldest counts the record it displaced in Snapshot.Dropped, the same
// counter every other undelivered record ends up in. Neither is a reliable
// mode, which is why both are opt-in.
type OverflowPolicy uint8

// The overflow policies. OverflowDefault is the zero value and reads as
// OverflowReject in the engine; the memory adapter applies Block
// default above it (adr/ADR-017-assumptions.md).
const (
	// OverflowDefault leaves the policy to the topology default.
	OverflowDefault OverflowPolicy = iota
	// OverflowBlock parks the publisher until the record fits, its context
	// ends or its publication's Stop closes. It never loses a record and
	// never refuses one, at the price of making Publish blocking: the
	// publisher inherits the backpressure of the slowest subscription the
	// fan-out policy makes it wait for.
	OverflowBlock
	// OverflowReject refuses the publication with errcode.ErrQueueFull and
	// leaves an owned payload with its publisher.
	OverflowReject
	// OverflowDropNewest discards the publication the lane had no room for,
	// counts it and tells the publisher the record was taken — so an owned
	// payload is released by the engine, exactly as it is for a record that
	// was delivered.
	OverflowDropNewest
	// OverflowDropOldest makes room by discarding what the lane would have
	// served next, which is a record that has been waiting longest and, in a
	// telemetry-shaped workload, matters least. On a ready-queue kind that
	// orders what it holds that is the oldest record; a kind that does not
	// keep an order does not promise which of its records goes, and what the
	// policy promises for every kind is the accounting — one record
	// displaced for every record admitted, counted in Snapshot.Dropped
	// (adr/ADR-017-assumptions.md). It refuses like
	// OverflowReject when the lane holds nothing to discard, which is the
	// case when every record it has is in flight.
	OverflowDropOldest

	overflowCount
)

var overflowNames = [overflowCount]string{"default", "block", "reject", "dropnew", "dropold"}

var overflowIdents = [overflowCount]string{
	"OverflowDefault", "OverflowBlock", "OverflowReject", "OverflowDropNewest", "OverflowDropOldest",
}

// OverflowPolicies returns the concrete policies in declaration order.
// OverflowDefault is not one of them.
func OverflowPolicies() []OverflowPolicy {
	return []OverflowPolicy{OverflowBlock, OverflowReject, OverflowDropNewest, OverflowDropOldest}
}

// String returns the benchmark slug of the policy, which is the name
// the performance budgets gives it in BenchmarkMemory/Overflow.
func (o OverflowPolicy) String() string {
	if o < overflowCount {
		return overflowNames[o]
	}
	return "overflow(?)"
}

// Ident returns the Go identifier of the policy.
func (o OverflowPolicy) Ident() string {
	if o < overflowCount {
		return overflowIdents[o]
	}
	return ""
}

// Valid reports whether o names a policy or OverflowDefault.
func (o OverflowPolicy) Valid() bool { return o < overflowCount }

// Resolve maps OverflowDefault to the engine's default, which is
// OverflowReject.
func (o OverflowPolicy) Resolve() OverflowPolicy {
	if o == OverflowDefault {
		return OverflowReject
	}
	return o
}

// ParseOverflowPolicy maps a slug back to its policy.
func ParseOverflowPolicy(s string) (OverflowPolicy, bool) {
	for i, n := range overflowNames {
		if n == s {
			return OverflowPolicy(i), true
		}
	}
	return OverflowDefault, false
}

// Blocks reports whether the policy parks a publisher that finds no room. It
// is what the engine's own publications consult, since none of them may wait
// (Topic.publishInternal).
func (o OverflowPolicy) Blocks() bool { return o.Resolve() == OverflowBlock }

// OrderingMode is the delivery-order guarantee of a subscription's lane
// (the message contract, TopologyConfig.Ordering).
//
// It governs what a redelivery may overtake. Under OrderingStrict a record
// that comes back from a negative acknowledgement, a lease expiry or a
// consumer close blocks its lane: nothing published behind it is delivered
// until it has been settled or terminally routed to a dead-letter topic,
// which is the "strict-ordering retries block their lane" invariant.
// Under OrderingRelaxed the records behind it carry on and the retry
// placement kind (the specification) decides where it goes.
type OrderingMode uint8

// The ordering modes. OrderingDefault reads as OrderingRelaxed.
const (
	// OrderingDefault leaves the mode to the topology default.
	OrderingDefault OrderingMode = iota
	// OrderingRelaxed lets records behind a failed one carry on.
	OrderingRelaxed
	// OrderingStrict makes a retry block its lane until it is settled.
	OrderingStrict

	orderingCount
)

var orderingNames = [orderingCount]string{"default", "relaxed", "strict"}

var orderingIdents = [orderingCount]string{"OrderingDefault", "OrderingRelaxed", "OrderingStrict"}

// OrderingModes returns the concrete modes in declaration order.
func OrderingModes() []OrderingMode { return []OrderingMode{OrderingRelaxed, OrderingStrict} }

// String returns the benchmark slug of the mode.
func (m OrderingMode) String() string {
	if m < orderingCount {
		return orderingNames[m]
	}
	return "ordering(?)"
}

// Ident returns the Go identifier of the mode.
func (m OrderingMode) Ident() string {
	if m < orderingCount {
		return orderingIdents[m]
	}
	return ""
}

// Valid reports whether m names a mode or OrderingDefault.
func (m OrderingMode) Valid() bool { return m < orderingCount }

// Resolve maps OrderingDefault to the engine default, which is
// OrderingRelaxed.
func (m OrderingMode) Resolve() OrderingMode {
	if m == OrderingDefault {
		return OrderingRelaxed
	}
	return m
}

// ParseOrderingMode maps a slug back to its mode.
func ParseOrderingMode(s string) (OrderingMode, bool) {
	for i, n := range orderingNames {
		if n == s {
			return OrderingMode(i), true
		}
	}
	return OrderingDefault, false
}

// Strict reports whether a retry blocks its lane under the mode.
func (m OrderingMode) Strict() bool { return m.Resolve() == OrderingStrict }

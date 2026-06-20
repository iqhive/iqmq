package prometheus

// Gap is one of the API's canonical metric names and what this
// package can actually say about it.
//
// It exists because "a metric that is always zero is worse than none": a
// registered `iqmq_option_ignored_total` sitting at zero for ever reads as
// "no option was ignored", which is a claim nothing in this repository can
// make. So the names with no source are not registered, and this is the list
// a reader gets instead of a flat graph.
type Gap struct {
	// Metric is the name.
	Metric string
	// Registered reports whether [New] registers it.
	Registered bool
	// Reason is what is missing, or what the caller has to do.
	Reason string
}

// Unsourced returns every canonical metric whose source needs a
// qualification, plus every one that has none at all.
//
// A name absent from this list is registered by [New] and fed by a hook, by
// the handler middleware or by a handle a caller registered, with no
// qualification. [TestUnsourcedMatchesTheRegistry] holds the list to the
// registry in both directions, so a Gap that says "not registered" for a
// metric this package does register fails a test.
func Unsourced() []Gap {
	return []Gap{
		{
			Metric:     "iqmq_option_ignored_total",
			Registered: false,
			Reason: "no source. The specification and ADR-010 the specification make an option a driver " +
				"cannot honour a CONSTRUCTION ERROR naming the option, not a " +
				"silently ignored setting, so nothing in the core or in any " +
				"driver has an ignored option to count. A registered counter " +
				"stuck at zero would claim the refusals never happen.",
		},
		{
			Metric:     "iqmq_reconnects_total",
			Registered: true,
			Reason: "registered, and nothing in the CORE fires it: a Backend is " +
				"built before the Client that holds mq.Hooks and no part of the " +
				"SPI carries them, so at v0 the reconnect event reaches a " +
				"driver's own Config. Pass Hooks() to the driver that offers " +
				"it. Passing it to BOTH the driver and mq.WithHooks counts " +
				"every reconnection twice.",
		},
		{
			Metric:     "iqmq_dropped_total",
			Registered: true,
			Reason: "the value comes from PublisherStats.Drops; the `policy` label " +
				"has no source. PublisherStats does not carry the backpressure " +
				"mode and PublisherHandle has no accessor for it, so " +
				"AddPublisher takes the mq.BackpressureMode from the caller.",
		},
		{
			Metric:     "iqmq_queue_depth",
			Registered: true,
			Reason: "sourced from Admin.Stats, which is a broker round trip, so it " +
				"is pushed in with ObserveQueueStats rather than pulled on " +
				"scrape. A driver whose QueueStats.Depth is not Known produces " +
				"no series at all.",
		},
		{
			Metric:     "iqmq_memory_retained_bytes",
			Registered: true,
			Reason: "read as memory.Snapshot.Bytes, the accounted payload bytes of " +
				"every record the lane holds. The engine has no separate " +
				"retention-log byte count: Snapshot.Retained is a record COUNT " +
				"and Broker.Totals is broker-wide.",
		},
		{
			Metric:     "iqmq_lag",
			Registered: true,
			Reason: "not in the canonical list, which names iqmq_queue_depth and " +
				"no lag metric -- but OpenTelemetry sentence does ask for a " +
				"lag gauge, and integrations/otel registers one. Exposing one " +
				"quantity under two names in two expositions would be worse than " +
				"naming it once, so this is the same name. " +
				"ConsumerStats.Lag is always unknown at v0 because no SPI carries " +
				"lag, so the only source is Admin.Stats through ObserveQueueStats, " +
				"and a subscription whose lag is unknown gets no series.",
		},
	}
}

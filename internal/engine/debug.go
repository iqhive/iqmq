//go:build iqmq_debug

package engine

// debugCanary reports whether the iqmq_debug canaries are compiled in. Under
// the tag the engine arms the use-after-free canary of the memory engine for
// every receive mode that aliases the engine's payload, and checks the
// release contract of an owned publication on every release rather than only
// when Config.DebugOwnership asks for it.
const debugCanary = true

// DoubleReleasePanic is the panic value raised when the engine is about to
// release a record's payload a second time (iqmq_debug builds, or
// Config.DebugOwnership).
const DoubleReleasePanic = "engine: payload released twice (iqmq_debug canary)"

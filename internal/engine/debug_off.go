//go:build !iqmq_debug

package engine

// debugCanary reports whether the iqmq_debug canaries are compiled in.
const debugCanary = false

// DoubleReleasePanic is the panic value raised when the engine is about to
// release a record's payload a second time (iqmq_debug builds, or
// Config.DebugOwnership).
const DoubleReleasePanic = "engine: payload released twice (iqmq_debug canary)"

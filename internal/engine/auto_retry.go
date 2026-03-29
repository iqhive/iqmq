// Default configuration for this implementation.

package engine

// RetryLaneAutoKind is the benchmark-selected default bound to RetryLaneAuto (variant, ADR-014).
const RetryLaneAutoKind = PushFront

// RetryLaneAutoProvisional reports that RetryLaneAutoKind is subject to review
// before a stable release.
const RetryLaneAutoProvisional = true

//go:build race

package middleware_test

// allocsRaceDetector reports that the race detector is compiled in.
//
// It changes nothing this package does; what it changes is how many objects
// the standard library's gzip reader allocates per stream, which stops being
// a fixed number under instrumentation. The rows it perturbs are skipped
// rather than given a second expected value: a count that differs by build
// mode is not a count anybody can reason about, and `make allocs` runs
// without the detector.
const allocsRaceDetector = true

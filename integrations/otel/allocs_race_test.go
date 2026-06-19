//go:build race

package otel_test

// allocsRaceDetector reports that the race detector is compiled in.
//
// It changes nothing this package does; what it changes is how many objects
// the runtime allocates per operation, which stops being a fixed number under
// instrumentation. TestAllocs is skipped rather than given a second expected
// value, and `make allocs` runs without the detector.
const allocsRaceDetector = true

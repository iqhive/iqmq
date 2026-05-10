//go:build !race

package streams_test

// raceEnabled is false when the race detector is off, which is how
// `make allocs` runs.
const raceEnabled = false

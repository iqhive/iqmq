//go:build race

package streams_test

// raceEnabled is true when the race detector is on, which is when an
// allocation budget cannot be measured: the detector allocates shadow state of
// its own, so both the count and the byte figures move.
const raceEnabled = true

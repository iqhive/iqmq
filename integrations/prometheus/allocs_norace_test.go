//go:build !race

package prometheus_test

// allocsRaceDetector reports that the race detector is compiled in. See the
// race-tagged file beside this one.
const allocsRaceDetector = false

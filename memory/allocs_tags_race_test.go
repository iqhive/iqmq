//go:build race

package memory_test

// allocsRaceDetector reports that the race detector is compiled in. It moves
// nothing on its own; what it moves is the debug ledger's allocation, which
// stops being attributed to a fixed number of objects per record.
const allocsRaceDetector = true

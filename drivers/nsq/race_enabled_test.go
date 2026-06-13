//go:build race

package nsq_test

// raceEnabled is true when the race detector is on, which is when the
// allocation budgets cannot be measured.
const raceEnabled = true

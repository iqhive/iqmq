//go:build race

package pulsar_test

// raceEnabled reports whether the race detector is on, which is what the
// allocation budgets skip under: the detector perturbs allocation counts, so an
// AllocsPerRun assertion there measures the detector.
const raceEnabled = true

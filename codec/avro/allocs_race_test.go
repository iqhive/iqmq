//go:build race

package avro_test

// allocsRaceDetector reports that the race detector is compiled in.
//
// It changes nothing this package does; what it changes is how many objects
// the runtime allocates per operation. Measured here: every AppendMarshal arm
// reports 1.0 allocs/op under `-race` and 0 without it, so the number stops
// being a property of the codec. TestAllocs is skipped rather than given a
// second expected value, and `make allocs` runs without the detector. This is
// the convention integrations/prometheus and drivers/{nsq,pulsar} already use.
const allocsRaceDetector = true

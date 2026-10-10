package redisclient_test

import "os"

// benchMatrix is the campaign's dimension grid, which
// exports as IQMQ_BENCH_MATRIX for a family's correctness gate, kind list,
// filter probe and measurement alike.
//
// An unrecognised value is the smoke grid under another name, which is how a
// thirty-eight-family campaign came to measure the short grid throughout
// ; the harness refuses anything but default, full and
// huge before it measures, so reading it here is a lookup rather than a
// validation.
func benchMatrix() string { return os.Getenv("IQMQ_BENCH_MATRIX") }

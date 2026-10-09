// Package notests has no test files, on purpose.
//
// `go test` over a package with no test files prints `?  pkg [no test files]`
// and exits 0, so a campaign stage that is a bare `go test <pkg>` reports
// success having compiled nothing under test. `hack/linearize-campaign.sh
// --selftest` points `LIN_CHECKER_PKG` here and asserts the campaign refuses
// to start, which is the negative control for stage 1's pre-flight -- the one
// stage that had no pre-flight, and the one whose job is to stop the other two
// being vacuous.
//
// It lives under `testdata` with the rest of the fixture, so no `./...` walk
// in the repository builds it.
package notests

// Marker exists so the package is not empty.
const Marker = "linearize-selftest: a package with no test files"

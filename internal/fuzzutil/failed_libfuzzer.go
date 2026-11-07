//go:build gofuzz_libfuzzer

package fuzzutil

import "testing"

// failed is false under the libFuzzer build ClusterFuzzLite makes
// (.clusterfuzzlite/build.sh), whose stand-in for package testing panics in
// T.Failed. Nothing is lost: the same stand-in panics on every Error and
// Fatal, so a failed test never gets back to a caller that could ask.
func failed(testing.TB) bool { return false }

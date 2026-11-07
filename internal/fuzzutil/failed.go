//go:build !gofuzz_libfuzzer

package fuzzutil

import "testing"

// failed is t.Failed. The libFuzzer build has its own; see failed_libfuzzer.go.
func failed(t testing.TB) bool { return t.Failed() }

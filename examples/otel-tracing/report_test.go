package main

import (
	"os"
	"testing"
)

// This file is the reporting contract `make examples` reads, and it is
// duplicated verbatim in every broker-backed example module.
//
// The duplication is deliberate. An example module's whole claim is that
// somebody outside this repository can build it from its own go.mod, and a
// shared helper module would be a fifth `replace` line in each of them for
// twenty lines of test plumbing. hack/examples.sh is the one place that has to
// agree with it, and it agrees by finding the env var's name in the module's
// sources: a module that mentions IQMQ_EXAMPLE_REPORT and then reports no arm
// fails `make examples` by name.
//
// # Why any of this exists
//
// `make examples` runs in every `make ci`, and CI has no brokers. So a
// broker-backed example's container arm has to skip when no broker is
// configured — and a test that always skips is a check that cannot fail,
// which is this repository's most repeated defect. Two things answer that:
//
//   - every example here has an IN-PROCESS arm that always runs (an embedded
//     nats-server, a kfake cluster, the sqsfake server), so the example's own
//     behaviour is checked on every run whether a broker exists or not;
//   - the skip is NAMED in `make examples`'s output through this file, so a
//     run in which nothing reached a broker says so instead of reading as
//     green.

// exampleName is what this module reports as. It is the module's directory
// name, which is what a reader sees in `make examples`'s log.
const exampleName = "otel-tracing"

// The arms every broker-backed example has.
const (
	// armInProcess is the arm that needs no broker and therefore never
	// skips. It is what makes this module's tests able to fail.
	armInProcess = "in-process"
	// armBroker is the arm gated on IQMQ_<BROKER>_ADDR.
	armBroker = "broker"
)

// The statuses. A third — "failed" — is deliberately absent: a failing arm is
// a failing `go test`, and `make examples` reports the failure from the test's
// own exit status rather than from a line the test chose to write.
const (
	statusRan     = "ran"
	statusSkipped = "skipped"
)

// reportArm records that one arm of this example ran or skipped.
//
// It appends "<example>\t<arm>\t<status>\t<detail>" to the file
// IQMQ_EXAMPLE_REPORT names, and does nothing at all when that variable is
// unset — `go test ./...` and `make test-race` reach these modules too, and
// neither is asking for a report. A write that fails fails the test: a report
// nobody can read is exactly the silent green this mechanism exists to
// prevent.
func reportArm(tb testing.TB, arm, status, detail string) {
	tb.Helper()
	path := os.Getenv("IQMQ_EXAMPLE_REPORT")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		tb.Fatalf("opening IQMQ_EXAMPLE_REPORT (%s): %v", path, err)
	}
	line := exampleName + "\t" + arm + "\t" + status + "\t" + detail + "\n"
	if _, err := f.WriteString(line); err != nil {
		_ = f.Close()
		tb.Fatalf("writing to IQMQ_EXAMPLE_REPORT (%s): %v", path, err)
	}
	if err := f.Close(); err != nil {
		tb.Fatalf("closing IQMQ_EXAMPLE_REPORT (%s): %v", path, err)
	}
}

// brokerAddr returns the address of an externally managed broker for this
// example, and reports whether there is one.
//
// The variable is conformance/harness's own: IQMQ_<BROKER>_ADDR names a broker
// the caller started, as host:port, and is what `make integration` and every
// driver's container suite already read. IQMQ_REQUIRE_BROKERS=1 is the other
// half of that convention: it turns "no broker configured" from a skip into a
// failure, which is what a run that MEANT to test against a broker sets.
func brokerAddr(tb testing.TB, envVar string) (string, bool) {
	tb.Helper()
	addr := os.Getenv(envVar)
	if addr != "" {
		return addr, true
	}
	if os.Getenv("IQMQ_REQUIRE_BROKERS") == "1" {
		reportArm(tb, armBroker, statusSkipped, envVar+" unset and IQMQ_REQUIRE_BROKERS=1")
		tb.Fatalf("%s is unset and IQMQ_REQUIRE_BROKERS=1", envVar)
	}
	reportArm(tb, armBroker, statusSkipped, envVar+" unset")
	return "", false
}

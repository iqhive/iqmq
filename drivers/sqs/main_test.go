package sqs_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
//
// It matters more for this driver than the assertion suggests: a source runs
// [Config.Pollers] goroutines parked in a twenty-second long poll, and a Close
// that did not cancel their request would leak one per subscription and be
// invisible in every other test. goleak is what notices.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// The SDK's HTTP transport keeps idle connections and their reader
		// goroutines; nothing in this driver can close them synchronously.
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

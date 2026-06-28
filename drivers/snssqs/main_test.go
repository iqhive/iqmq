package snssqs_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
//
// It matters more for this driver than the assertion suggests: every
// subscription is an SQS source running Config.Pollers goroutines parked in a
// long poll, and a fan-out opens one per subscription — so a Close that did
// not cancel their requests would leak N goroutines per topic and be invisible
// in every other test.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// The SDKs' HTTP transports keep idle connections and their reader
		// goroutines; nothing in this driver can close them synchronously.
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

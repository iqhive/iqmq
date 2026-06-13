package nsq_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
//
// It matters more for this driver than the assertion suggests. A source runs a
// handler pool inside go-nsq, a lookupd poll loop and an RDY loop, and every
// one of them outlives a Close that forgot to wait for the consumer's stop
// channel. go-nsq's own teardown is also conditional on the driver having
// responded to every in-flight message: a Close that did not requeue what it
// was holding leaves a connection draining for thirty seconds, which goleak
// sees as a goroutine and a person sees as a hang.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// go-nsq dials over net/http for lookupd and this driver does for
		// nsqd's administrative API; the transport keeps idle connections
		// and their reader goroutines, and nothing here can close them
		// synchronously.
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

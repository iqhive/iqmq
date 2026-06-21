package activemq_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to the rule that a driver starts
// no goroutine a Close does not stop.
//
// The ignores are the runtime's and the test framework's own, not this
// driver's: `testing.(*T).Run` is the parent of a parallel subtest that has
// not finished when the leak check runs, and go-amqp's own connection
// goroutines are stopped by the backend's Close, which every test registers
// with t.Cleanup.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("testing.(*T).Run"),
	)
}

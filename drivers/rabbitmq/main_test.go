package rabbitmq_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to a clean goroutine set.
//
// It matters more here than on most drivers because this one starts several
// background goroutines per client: a supervision loop that watches the
// connection, a confirm loop per sink and a delivery pump per source. Every
// one of them is bounded by the backend's own context, and a Close that did
// not cancel it would leak one per handle — invisible in every other test and
// fatal in a process that opens a client per request.
//
// Nothing is ignored. The fake broker is in process and starts no goroutine
// the tests do not stop, so a leak reported here is this driver's.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

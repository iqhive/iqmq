package goredis_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to a goroutine budget of zero
// ("goleak in TestMain").
//
// There is more to leak here than in the rueidis driver's own suite, which is
// why it is worth stating: every test in this package runs a real TCP server
// (`internal/streams/respsrv`) with a goroutine per connection, a go-redis
// connection pool, and — in the cross-client tests — a rueidis client with its
// own background reader. A Close that did not stop any one of them would leak
// per test rather than per subscription.
func TestMain(m *testing.M) {
	code := goleak.IgnoreCurrent()
	status := m.Run()
	// The fuzz target keeps one rig per process rather than one per input
	// (see fuzzRig), so it is this function that releases it.
	closeFuzzRig()
	if status == 0 {
		if err := goleak.Find(code); err != nil {
			_, _ = os.Stderr.WriteString("goleak: " + err.Error() + "\n")
			status = 1
		}
	}
	os.Exit(status)
}

package amqp10_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to a goroutine budget of zero
// ("goleak in TestMain").
//
// Nothing is ignored, and that is the point. The peer these tests run against
// is go-amqp on the other end of a loopback socket, so every goroutine that
// exists is either this driver's or a connection somebody forgot to close — a
// supervisor that outlived its backend, a fetch parked on a source nobody
// closed, or a scripted peer left holding a connection. Each of those fails
// here and has nowhere to hide.
func TestMain(m *testing.M) {
	code := goleak.IgnoreCurrent()
	status := m.Run()
	if status == 0 {
		if err := goleak.Find(code); err != nil {
			_, _ = os.Stderr.WriteString("goleak: " + err.Error() + "\n")
			status = 1
		}
	}
	os.Exit(status)
}

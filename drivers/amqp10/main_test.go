package amqp10_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to a goroutine budget of zero
// ("goleak in TestMain").
//
// Nothing is ignored, and that is the point. This module's tests run against a
// scripted peer rather than a client library, so there is no third-party
// goroutine to excuse: a supervisor that outlived its backend, an asynchronous
// publication worker that outlived its sink, or a fetch parked on a source
// nobody closed fails here and has nowhere to hide.
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

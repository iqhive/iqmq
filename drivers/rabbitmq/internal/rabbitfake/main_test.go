package rabbitfake_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain checks that nothing this package started is still running when the
// suite ends ("goleak in TestMain").
//
// It is the check the fake's own design is held to. A broker runs one
// dispatcher goroutine per connection and one watcher per consumer whose
// context can be cancelled, and every one of them has to be stopped by
// [rabbitfake.Broker.Close] -- which the tests call through t.Cleanup. A fake
// that leaked a goroutine per connection would leak one per test in whichever
// suite uses it, which on a driver's own test suite is a slow death nothing
// else would notice.
//
// goleak.IgnoreCurrent is taken first so that the testing package's own
// goroutines are not counted, and the check runs only on a passing run: a
// failing test has already stopped where it stood and its cleanup may not have
// run, so a leak report there would name the symptom rather than the cause.
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

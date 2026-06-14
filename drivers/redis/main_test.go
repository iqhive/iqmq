package redis_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to a goroutine budget of zero
// ("goleak in TestMain").
//
// There are no ignores, and that is the point. This driver starts two
// goroutines per source — the mandatory reclaim sweep and, under the zset
// strategy, the delayed mover — and both are the driver's own: a Close that
// did not stop them would leak one per subscription, which on a process that
// opens and closes consumers is a slow death nothing else in the suite would
// notice. The unit tests run against an in-memory server, so nothing else in
// this package starts a goroutine at all.
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

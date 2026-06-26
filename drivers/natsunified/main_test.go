package natsunified_test

import (
	"os"
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds every test in this package to a goroutine budget of zero
// ("goleak in TestMain").
//
// The ignores are the embedded broker's, not the facade's. nats-server keeps a
// handful of goroutines parked after Shutdown, and a test that started a real
// broker in-process cannot wait for them without waiting for the broker's own
// shutdown grace. Nothing either driver starts is ignored, which is the point:
// this package's tests open and close clients on both profiles dozens of times,
// and a connection the facade opened and then refused on a requirement —
// TestRequireRefusesAtOpen — is exactly the shape that leaks one per call.
func TestMain(m *testing.M) {
	if err := startServers(); err != nil {
		panic(err)
	}
	code := goleak.IgnoreCurrent()
	status := m.Run()
	stopServers()
	if status == 0 {
		if err := goleak.Find(code,
			goleak.IgnoreTopFunction("github.com/nats-io/nats-server/v2/server.(*Server).internalSendLoop"),
			goleak.IgnoreAnyFunction("github.com/nats-io/nats-server/v2/server.(*ipQueue[...]).pop"),
		); err != nil {
			_, _ = os.Stderr.WriteString("goleak: " + err.Error() + "\n")
			status = 1
		}
	}
	os.Exit(status)
}

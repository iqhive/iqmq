package jetstream_test

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/goleak"
)

// sharedAddr is the client URL of the package's own embedded JetStream server.
//
// One server serves every test, and every test gets a stream of its own: a
// JetStream server has a file store to initialise, so starting one per test
// costs a tenth of a second each and buys nothing — two tests on two streams of
// one server cannot see each other's messages, consumers or sequences.
// [freshServer] is for the handful of tests that really need a server to
// themselves.
var sharedAddr string

// TestMain starts the shared server and holds every test in this package to a
// goroutine budget of zero ("goleak in TestMain").
//
// The ignores are the embedded broker's, not the driver's. nats-server keeps a
// handful of goroutines parked after Shutdown, and a test that started a real
// broker in-process cannot wait for them without waiting for the broker's own
// shutdown grace. Nothing the driver starts is ignored, which is the point: a
// consume loop, a parked Push or a settler that outlives Close fails this.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "iqmq-js-*")
	if err != nil {
		panic(err)
	}
	srv, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		JetStream: true, StoreDir: dir, MaxPayload: 1 << 20,
	})
	if err != nil {
		panic(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		srv.Shutdown()
		panic("iqmq/jetstream: the embedded server did not become ready")
	}
	sharedAddr = srv.ClientURL()
	code := goleak.IgnoreCurrent()
	status := m.Run()
	srv.Shutdown()
	srv.WaitForShutdown()
	_ = os.RemoveAll(dir)
	if status == 0 {
		if err := goleak.Find(code,
			goleak.IgnoreTopFunction("github.com/nats-io/nats-server/v2/server.(*Server).internalSendLoop"),
			goleak.IgnoreAnyFunction("github.com/nats-io/nats-server/v2/server.(*ipQueue[...]).pop"),
			goleak.IgnoreAnyFunction("github.com/nats-io/nats-server/v2/server.(*Server).Start.func1"),
		); err != nil {
			_, _ = os.Stderr.WriteString("goleak: " + err.Error() + "\n")
			status = 1
		}
	}
	os.Exit(status)
}

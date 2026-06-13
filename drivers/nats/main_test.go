package nats_test

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/goleak"
)

// exampleAddr is the server the runnable examples connect to.
//
// The examples are real — they open a client, publish and receive, and their
// Output blocks are checked — and starting a server per example would be a
// second of process startup for four lines of documentation. TestMain starts
// one for the package instead.
var exampleAddr string

// TestMain starts the shared example server and holds every test in this
// package to a goroutine budget of zero ("goleak in TestMain").
//
// The ignores are the embedded broker's, not the driver's. nats-server keeps a
// handful of goroutines parked after Shutdown, and a test that started a real
// broker in-process cannot wait for them without waiting for the broker's own
// shutdown grace. Nothing the driver starts is ignored, which is the point: a
// subscription goroutine, a parked Push or a flusher that outlives Close fails
// this.
func TestMain(m *testing.M) {
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		panic(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		srv.Shutdown()
		panic("iqmq/nats: the embedded server for the examples did not become ready")
	}
	exampleAddr = srv.ClientURL()
	code := goleak.IgnoreCurrent()
	status := m.Run()
	srv.Shutdown()
	srv.WaitForShutdown()
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

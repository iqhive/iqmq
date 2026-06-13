package pulsar_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
//
// It matters more for this driver than the assertion suggests. Every source
// runs a lease sweeper of its own, every sink waits for the asynchronous
// publications it admitted, and pulsar-client-go runs a connection goroutine, a
// dispatch goroutine and a partition-discovery ticker per handle. A Close that
// forgot to wake its own sweeper, or one that returned before the native
// callbacks had run, leaves a goroutine holding the caller's message — which
// goleak sees as a leak and a person sees as a message that never came back.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// net/http keeps idle connections and their reader goroutines for
		// the administrative API, and nothing here can close them
		// synchronously.
		// pulsar-client-go starts one failTimeoutMessages goroutine per
		// partition producer, and it exits only when its one-shot timer
		// next fires AND the producer is closing — so it outlives a
		// closed producer by up to one ProducerOptions.SendTimeout and
		// the library offers no way to wake it. Keeping SendTimeout is
		// what makes a publication bounded and its ambiguity
		// classifiable, so the goroutine is ignored rather than
		// the timeout removed; it is recorded in the maintenance notes
		// against pulsar-client-go.
		goleak.IgnoreTopFunction("github.com/apache/pulsar-client-go/pulsar.(*partitionProducer).failTimeoutMessages"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

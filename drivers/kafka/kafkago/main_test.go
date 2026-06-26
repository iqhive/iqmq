package kafkago_test

import (
	"os"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/goleak"
)

//
// kafka-go is only a TEST dependency of this module's own tests through
// kfake — the driver itself links neither kfake nor franz-go, and
// `GOWORK=off go build ./...` proves it. The container run remains the
// conformance evidence (the conformance suite), because the specification
// asks what a driver does against the image the project ships against and
// not against a broker the test compiled in.

// exampleBroker and exampleTopic are the cluster the runnable examples use.
//
// The examples are real — they open a client, publish, receive and check
// their Output blocks — and starting a cluster per example would be a cluster
// per four lines of documentation. TestMain starts one for the package
// instead.
var (
	exampleBroker string
	exampleTopic  = "example-orders"
)

// TestMain holds every test in this package to a goroutine budget of zero
// ("goleak in TestMain").
//
// The ignores are kfake's and the clients' internals, not this driver's.
// kfake keeps listener goroutines until its cluster is closed, and kafka-go's
// transport keeps a pool goroutine per broker connection for its idle
// timeout; nothing this driver starts is ignored, which is the point — a
// generation loop, a partition reader, a commit ticker or a parked Fetch that
// outlives Close fails this.
func TestMain(m *testing.M) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, exampleTopic))
	if err != nil {
		panic(err)
	}
	exampleBroker = c.ListenAddrs()[0]
	code := goleak.IgnoreCurrent()
	status := m.Run()
	c.Close()
	if status == 0 {
		if err := goleak.Find(code,
			goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
			goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).run"),
			goleak.IgnoreAnyFunction("github.com/twmb/franz-go/pkg/kfake.(*Cluster).listen"),
		); err != nil {
			_, _ = os.Stderr.WriteString("goleak: " + err.Error() + "\n")
			status = 1
		}
	}
	os.Exit(status)
}

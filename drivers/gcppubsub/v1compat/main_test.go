package v1compat_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
//
// Both client generations run several goroutines per publisher and per
// subscription, and this package's tests open one of each per arm, so a Close
// that did not end them would leak at twice the rate and be invisible
// elsewhere.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		// Each client generation's per-iterator stream keep-alive handler
		// exits on its own tickers rather than on the iterator's stop signal,
		// so it lingers for up to ten seconds after a source has closed. It
		// is a shutdown LAG in the libraries rather than a leak, and it is
		// recorded in the maintenance notes.
		goleak.IgnoreTopFunction("cloud.google.com/go/pubsub/v2.(*messageIterator).streamKeepAliveHandler"),
		goleak.IgnoreTopFunction("cloud.google.com/go/pubsub.(*messageIterator).streamKeepAliveHandler"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}

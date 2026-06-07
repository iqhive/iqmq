package gcppubsub_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when a test leaves a goroutine behind.
//
// It matters more for this driver than the assertion suggests. A source runs
// the client library's streaming pull, which is several goroutines of the
// library's own; the completion pool runs a fixed set of its own; and a
// delayed negative settlement runs one per held delivery. A Close that did not
// end any one of those would leak silently and be invisible in every other
// test — the delayed-nack hold especially, because a hold that outlives its
// source is a goroutine AND a message nobody nacks. goleak is what notices.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// gRPC's transport keeps reader and writer goroutines per connection
		// and the client library's own pool keeps a few; nothing in this
		// driver can close them synchronously.
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/grpcsync.(*CallbackSerializer).run"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		// The client library's per-iterator stream keep-alive handler exits on
		// its own tickers rather than on the iterator's stop signal, so it
		// lingers for up to the ten-second server-monitor interval after a
		// source has closed and its Receive has returned. It is a shutdown
		// LAG in the library rather than a leak — one goroutine per closed
		// source, bounded, and it does exit — and it is recorded in
		// the maintenance notes as an upstream observation.
		goleak.IgnoreTopFunction("cloud.google.com/go/pubsub/v2.(*messageIterator).streamKeepAliveHandler"),
		// The client library pulls in OpenCensus, whose stats worker is
		// started by a package init and never stops.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
	)
}

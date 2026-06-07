package stompsrv

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain holds this server to the promise its own package comment makes: it
// starts no goroutine [Server.Close] does not stop.
//
// Nothing is ignored, and that is the point. The two go-stomp leaks
// `drivers/activemq/stomp`'s own TestMain has to ignore are the LIBRARY's, and
// nothing in this package uses the library's `Conn` at all — the fuzz target
// drives the server over a raw socket with `frame.Reader` and `frame.Writer`,
// which is the whole of go-stomp this server depends on. So a leak reported
// here is this server's, and a server that leaked would make every test in the
// module above it a test about a goroutine nobody stopped.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

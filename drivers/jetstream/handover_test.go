package jetstream_test

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/jetstream"
)

// callerHandlers are the connection handlers a caller set when it dialled,
// counted, so a test can tell that they still run after a backend chained
// its own in front of them.
type callerHandlers struct {
	disconnects, reconnects, closes atomic.Int64
	reconnected                     chan struct{}
}

// dialWithHandlers dials the shared server with a counting handler of each
// kind.
func dialWithHandlers(t *testing.T) (*natsgo.Conn, *callerHandlers) {
	t.Helper()
	h := &callerHandlers{reconnected: make(chan struct{}, 1)}
	nc, err := natsgo.Connect(sharedAddr,
		natsgo.ReconnectWait(10*time.Millisecond),
		natsgo.DisconnectErrHandler(func(*natsgo.Conn, error) { h.disconnects.Add(1) }),
		natsgo.ReconnectHandler(func(*natsgo.Conn) {
			h.reconnects.Add(1)
			select {
			case h.reconnected <- struct{}{}:
			default:
			}
		}),
		natsgo.ClosedHandler(func(*natsgo.Conn) { h.closes.Add(1) }),
	)
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc, h
}

// reconnectOnce forces one reconnection and waits for the caller's handler.
func reconnectOnce(t *testing.T, nc *natsgo.Conn, h *callerHandlers) {
	t.Helper()
	if err := nc.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case <-h.reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never reconnected")
	}
}

// within waits up to five seconds for cond.
func within(cond func() bool) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// TestAConnectionHandedToOpenWithClientReachesHealth holds what the Reconnect
// section of docs/drivers/jetstream.md says, on a connection given to
// OpenWithClient, owned or borrowed: a reconnection fires Hooks.OnReconnect
// and a connection closed for good makes Health Closed, while the handlers
// the caller set still run. Only a dialled connection got the driver's
// handlers, so on a handed-over one neither happened.
func TestAConnectionHandedToOpenWithClientReachesHealth(t *testing.T) {
	for _, own := range []driverkit.Ownership{driverkit.ClientOwned, driverkit.ClientBorrowed} {
		t.Run(own.String(), func(t *testing.T) {
			nc, h := dialWithHandlers(t)
			var hooks atomic.Int64
			cfg := baseConfig(t)
			cfg.Hooks = mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) { hooks.Add(1) }}
			c, err := nq.OpenWithClient(t.Context(), nc, own, cfg)
			if err != nil {
				t.Fatalf("OpenWithClient: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })

			reconnectOnce(t, nc, h)
			if !within(func() bool { return hooks.Load() == 1 }) {
				t.Fatalf("after a reconnection Hooks.OnReconnect fired %d times, want 1", hooks.Load())
			}
			nc.Close()
			if !within(func() bool { return c.Health().State == mq.HealthClosed }) {
				t.Fatalf("after the connection closed for good Health is %v, want Closed", c.Health().State)
			}
			if !within(func() bool { return h.closes.Load() == 1 }) || h.reconnects.Load() != 1 || h.disconnects.Load() == 0 {
				t.Fatalf("the caller's own handlers ran %d reconnects, %d disconnects and %d closes, want 1, at least 1 and 1",
					h.reconnects.Load(), h.disconnects.Load(), h.closes.Load())
			}
		})
	}
}

// TestABorrowedConnectionOutlivesItsBackend holds that the handlers a closed
// backend leaves on a borrowed connection pass every event to the caller's
// handlers and do nothing of the backend's own: no hook fires for a
// reconnection after Close.
func TestABorrowedConnectionOutlivesItsBackend(t *testing.T) {
	nc, h := dialWithHandlers(t)
	var hooks atomic.Int64
	cfg := baseConfig(t)
	cfg.Hooks = mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) { hooks.Add(1) }}
	c, err := nq.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reconnectOnce(t, nc, h)
	time.Sleep(100 * time.Millisecond)
	if hooks.Load() != 0 {
		t.Fatalf("Hooks.OnReconnect fired %d times for a reconnection after the backend closed", hooks.Load())
	}
	if h.reconnects.Load() != 1 {
		t.Fatalf("the caller's reconnect handler ran %d times, want 1", h.reconnects.Load())
	}
}

// collected waits up to five seconds, collecting garbage, for done to close.
func collected(done chan struct{}) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		runtime.GC()
		select {
		case <-done:
			return true
		case <-time.After(10 * time.Millisecond):
		}
	}
	return false
}

// TestAClosedBackendIsNotKeptByItsConnection holds that the handlers a
// closed backend leaves on a borrowed connection do not keep the backend
// alive. They held it, so every backend ever opened on a long-lived
// connection stayed in memory for as long as the connection lived.
func TestAClosedBackendIsNotKeptByItsConnection(t *testing.T) {
	nc, h := dialWithHandlers(t)
	done := make(chan struct{})
	func() {
		b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, baseConfig(t))
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		runtime.AddCleanup(b, func(done chan struct{}) { close(done) }, done)
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	if !collected(done) {
		t.Fatal("a closed backend is still reachable from the borrowed connection it was handed")
	}
	reconnectOnce(t, nc, h)
}

// liveHeap is the heap in use after two collections.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestAFailedOpenIsNotKeptByItsConnection holds that a BackendWithClient that
// fails, as a start-up loop retrying while JetStream is unavailable does,
// leaves no backend behind on the borrowed connection. Each failed attempt
// used to keep its backend, about 7.5 KB, for as long as the connection
// lived.
func TestAFailedOpenIsNotKeptByItsConnection(t *testing.T) {
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	nc, err := natsgo.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	attempt := func(n int) {
		for range n {
			b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed,
				nq.Config{Addrs: []string{srv.ClientURL()}, Stream: "S", PublishTimeout: 200 * time.Millisecond})
			if err == nil {
				_ = b.Close()
				t.Fatal("BackendWithClient succeeded on a server without JetStream, so the test proves nothing")
			}
		}
	}
	attempt(10)
	before := liveHeap()
	const n = 200
	attempt(n)
	per := (int64(liveHeap()) - int64(before)) / n
	t.Logf("each failed BackendWithClient left %d bytes", per)
	if per > 1024 {
		t.Fatalf("each failed BackendWithClient left %d bytes reachable from the borrowed connection, want under 1 KiB", per)
	}
}

// stackDepth reports, once, how deep the stack of the handler that calls it is.
type stackDepth chan int

func (d stackDepth) record() {
	pcs := make([]uintptr, 1<<16)
	select {
	case d <- runtime.Callers(0, pcs):
	default:
	}
}

// reconnectDepth forces one reconnection and returns how deep the caller's
// reconnect handler ran.
func reconnectDepth(t *testing.T, nc *natsgo.Conn, d stackDepth) int {
	t.Helper()
	if err := nc.ForceReconnect(); err != nil {
		t.Fatalf("ForceReconnect: %v", err)
	}
	select {
	case n := <-d:
		return n
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never reconnected")
		return 0
	}
}

// TestClosedBackendsDoNotDeepenTheConnectionsHandlers holds that a backend
// closed on a borrowed connection leaves nothing in the chain of handlers an
// event runs through. Every backend ever opened on the connection left a
// forwarder that called the one before it, so an event made one nested call
// per backend on nats.go's one callback goroutine, and some millions of open
// and close cycles on a long-lived connection ended the process with a stack
// overflow that nothing can recover from.
func TestClosedBackendsDoNotDeepenTheConnectionsHandlers(t *testing.T) {
	depth := make(stackDepth, 1)
	nc, err := natsgo.Connect(sharedAddr, natsgo.ReconnectWait(10*time.Millisecond),
		natsgo.ReconnectHandler(func(*natsgo.Conn) { depth.record() }))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	cfg := baseConfig(t)
	cycle := func(n int) {
		for range n {
			b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			_ = b.Close()
		}
	}
	bare := reconnectDepth(t, nc, depth)

	var hooks atomic.Int64
	liveCfg := cfg
	liveCfg.Hooks = mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) { hooks.Add(1) }}
	live, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, liveCfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	one := reconnectDepth(t, nc, depth)
	cycle(300)
	after := reconnectDepth(t, nc, depth)
	if after > one {
		t.Fatalf("an event ran %d frames deep with one backend open and %d after 300 more were opened and closed beside it, want no deeper",
			one, after)
	}
	if !within(func() bool { return hooks.Load() == 2 }) {
		t.Fatalf("the backend left open heard %d reconnections, want 2", hooks.Load())
	}
	if err := live.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cycle(300)
	// Two open at once, the first closed first.
	for range 150 {
		first, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		second, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, cfg)
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		_ = first.Close()
		_ = second.Close()
	}
	if got := reconnectDepth(t, nc, depth); got > bare {
		t.Fatalf("an event ran %d frames deep before any backend was opened and %d after 901 were opened and closed, want no deeper",
			bare, got)
	}
}

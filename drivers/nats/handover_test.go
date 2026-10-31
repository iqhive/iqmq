package nats_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	nq "github.com/iqhive/iqmq/drivers/nats"
)

// callerHandlers are the connection handlers a caller set when it dialled,
// counted, so a test can tell that they still run after a backend chained
// its own in front of them.
type callerHandlers struct {
	errs, disconnects, reconnects, closes atomic.Int64
	reconnected                           chan struct{}
}

// dialWithHandlers dials url with a counting handler of each kind.
func dialWithHandlers(t *testing.T, url string) (*natsgo.Conn, *callerHandlers) {
	t.Helper()
	h := &callerHandlers{reconnected: make(chan struct{}, 1)}
	nc, err := natsgo.Connect(url,
		natsgo.ReconnectWait(10*time.Millisecond),
		natsgo.ErrorHandler(func(*natsgo.Conn, *natsgo.Subscription, error) { h.errs.Add(1) }),
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

// TestAConnectionHandedToOpenWithClientReachesHealth holds that a connection
// given to OpenWithClient, owned or borrowed, drives Health and fires
// Hooks.OnReconnect as a dialled one does, while the handlers the caller set
// still run. Only a dialled connection got the driver's handlers, so on a
// handed-over one a reconnection never fired the hook and a connection
// closed for good left Health at Ready.
func TestAConnectionHandedToOpenWithClientReachesHealth(t *testing.T) {
	for _, own := range []driverkit.Ownership{driverkit.ClientOwned, driverkit.ClientBorrowed} {
		t.Run(own.String(), func(t *testing.T) {
			url := serverURL(t)
			nc, h := dialWithHandlers(t, url)
			var hooks atomic.Int64
			c, err := nq.OpenWithClient(t.Context(), nc, own, nq.Config{
				Addrs: []string{url},
				Hooks: mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) { hooks.Add(1) }},
			})
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

// TestABorrowedConnectionReportsASlowConsumer holds that a subscription the
// client library starts dropping messages for degrades Health and counts in
// Backend.SlowConsumers on a borrowed connection, which docs/drivers/nats.md
// calls the only place a dropped message surfaces, and that the caller's own
// error handler still hears it.
func TestABorrowedConnectionReportsASlowConsumer(t *testing.T) {
	url := serverURL(t)
	nc, h := dialWithHandlers(t, url)
	b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{
		Addrs: []string{url}, PendingMessages: 1, PendingBytes: 256,
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c := mq.NewClient(b)
	topic := newTopic("slowborrowed")
	cons, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "slow", Prefetch: 1})
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(func() { _ = cons.Close() })
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	body := bytes.Repeat([]byte("x"), 512)
	for deadline := time.Now().Add(20 * time.Second); h.errs.Load() == 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for range 200 {
			if err := pub.Publish(t.Context(), &mq.Message{Body: body}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
		}
	}
	if h.errs.Load() == 0 {
		t.Fatal("the client library never reported a slow consumer, so the test proves nothing")
	}
	if !within(func() bool { return b.SlowConsumers() > 0 }) || c.Health().State != mq.HealthDegraded {
		t.Fatalf("the client library reported %d slow-consumer errors on the borrowed connection, but Health is %v and SlowConsumers() = %d",
			h.errs.Load(), c.Health().State, b.SlowConsumers())
	}
}

// TestABorrowedConnectionOutlivesItsBackend holds that the handlers a closed
// backend leaves on a borrowed connection pass every event to the caller's
// handlers and do nothing of the backend's own: no hook fires for a
// reconnection after Close.
func TestABorrowedConnectionOutlivesItsBackend(t *testing.T) {
	url := serverURL(t)
	nc, h := dialWithHandlers(t, url)
	var hooks atomic.Int64
	c, err := nq.OpenWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{
		Addrs: []string{url},
		Hooks: mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) { hooks.Add(1) }},
	})
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

// TestAConnectionHandedOverOfflineIsNotReady holds that the health a backend
// starts with is the handed-over connection's: Degraded while it is
// reconnecting and Closed once it has closed. It used to start Ready
// whatever the connection was doing.
func TestAConnectionHandedOverOfflineIsNotReady(t *testing.T) {
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	url := srv.ClientURL()
	nc, err := natsgo.Connect(url, natsgo.MaxReconnects(-1), natsgo.ReconnectWait(time.Second))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	srv.Shutdown()
	srv.WaitForShutdown()
	if !within(nc.IsReconnecting) {
		t.Fatalf("the connection is %v, not reconnecting, so the test proves nothing", nc.Status())
	}
	b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if got := b.Health().State; got != mq.HealthDegraded {
		t.Fatalf("a backend on a reconnecting connection starts %v, want Degraded", got)
	}

	nc.Close()
	closed, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = closed.Close() })
	if got := closed.Health().State; got != mq.HealthClosed {
		t.Fatalf("a backend on a closed connection starts %v, want Closed", got)
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
// connection stayed in memory, about 12 KB each, for as long as the
// connection lived.
func TestAClosedBackendIsNotKeptByItsConnection(t *testing.T) {
	url := serverURL(t)
	nc, h := dialWithHandlers(t, url)
	done := make(chan struct{})
	func() {
		b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
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

// TestAnotherSubscriptionsSlowConsumerIsNotThisBackends holds that a slow
// consumer on a subscription the backend did not open, here the caller's own
// on the borrowed connection, neither degrades the backend's health nor
// counts in its SlowConsumers, while the caller's error handler still hears
// it.
func TestAnotherSubscriptionsSlowConsumerIsNotThisBackends(t *testing.T) {
	url := serverURL(t)
	nc, h := dialWithHandlers(t, url)
	b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	subject := newTopic("callers")
	sub, err := nc.Subscribe(subject, func(*natsgo.Msg) { <-release })
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	if err := sub.SetPendingLimits(1, 256); err != nil {
		t.Fatalf("SetPendingLimits: %v", err)
	}
	pub, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("dialling the publisher: %v", err)
	}
	t.Cleanup(pub.Close)
	body := bytes.Repeat([]byte("x"), 512)
	for deadline := time.Now().Add(20 * time.Second); h.errs.Load() == 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for range 50 {
			_ = pub.Publish(subject, body)
		}
		_ = pub.Flush()
	}
	if h.errs.Load() == 0 {
		t.Fatal("the client library never reported the caller's slow consumer, so the test proves nothing")
	}
	time.Sleep(100 * time.Millisecond)
	if got := b.Health().State; got != mq.HealthReady || b.SlowConsumers() != 0 {
		t.Fatalf("the caller's own subscription fell behind, and the backend reports health %v and SlowConsumers() = %d, want Ready and 0",
			got, b.SlowConsumers())
	}
}

// TestAConnectionHandedOverBeforeItsFirstConnectionBecomesReady holds that a
// backend handed a connection still making its first connection, which
// nats.RetryOnFailedConnect allows, reports Ready once that connection is up.
// It starts Degraded, and nats.go reports the first connection to ConnectedCB,
// which has no getter or setter to chain onto, and not to ReconnectedCB, so
// health stayed Degraded on a working connection until some later reconnection.
func TestAConnectionHandedOverBeforeItsFirstConnectionBecomesReady(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	nc, err := natsgo.Connect(url, natsgo.RetryOnFailedConnect(true),
		natsgo.ReconnectWait(20*time.Millisecond), natsgo.MaxReconnects(-1))
	if err != nil {
		t.Fatalf("dialling with RetryOnFailedConnect: %v", err)
	}
	t.Cleanup(nc.Close)
	b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c := mq.NewClient(b)
	if got := c.Health().State; got != mq.HealthDegraded {
		t.Fatalf("a backend on a connection that has not connected yet starts %v, want Degraded", got)
	}

	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: port, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("the server did not become ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !within(nc.IsConnected) {
		t.Fatalf("the connection never made its first connection: %v", nc.Status())
	}
	if err := c.Ping(t.Context()); err != nil {
		t.Fatalf("Ping on the connected backend: %v", err)
	}
	if !within(func() bool { return c.Health().State == mq.HealthReady }) {
		t.Fatalf("the connection is %v and Ping succeeds, but Health is %v (%v), want Ready",
			nc.Status(), c.Health().State, c.Health().Err)
	}
}

// TestASlowConsumerWhileABackendOpensIsNotARace holds that a slow consumer
// reported on a handed-over connection while a backend is opening on it does
// not race with the opening. The forwarder is installed before init, and the
// slow-consumer filter in it read the backend's sources under the lock while
// init created them without it. It proves something only under -race.
func TestASlowConsumerWhileABackendOpensIsNotARace(t *testing.T) {
	url := serverURL(t)
	nc, err := natsgo.Connect(url, natsgo.ErrorHandler(func(*natsgo.Conn, *natsgo.Subscription, error) {}))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	subject := newTopic("flood")
	sub, err := nc.Subscribe(subject, func(*natsgo.Msg) { time.Sleep(20 * time.Microsecond) })
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}
	if err := sub.SetPendingLimits(1, -1); err != nil {
		t.Fatalf("SetPendingLimits: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flushing the subscription: %v", err)
	}
	pub, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("dialling the publisher: %v", err)
	}
	t.Cleanup(pub.Close)
	var stop atomic.Bool
	flooded := make(chan struct{})
	go func() {
		defer close(flooded)
		for !stop.Load() {
			_ = pub.Publish(subject, []byte("x"))
		}
	}()
	t.Cleanup(func() { stop.Store(true); <-flooded })

	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		_ = b.Close()
	}
	if dropped, _ := sub.Dropped(); dropped == 0 {
		t.Fatal("the caller's subscription dropped nothing, so no slow consumer was reported and the test proves nothing")
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
	url := serverURL(t)
	depth := make(stackDepth, 1)
	nc, err := natsgo.Connect(url, natsgo.ReconnectWait(10*time.Millisecond),
		natsgo.ReconnectHandler(func(*natsgo.Conn) { depth.record() }))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(nc.Close)
	cycle := func(n int) {
		for range n {
			b, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			_ = b.Close()
		}
	}
	bare := reconnectDepth(t, nc, depth)

	var hooks atomic.Int64
	live, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{
		Addrs: []string{url},
		Hooks: mq.Hooks{OnReconnect: func(context.Context, mq.ReconnectEvent) { hooks.Add(1) }},
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	one := reconnectDepth(t, nc, depth)
	cycle(1000)
	after := reconnectDepth(t, nc, depth)
	if after > one {
		t.Fatalf("an event ran %d frames deep with one backend open and %d after 1,000 more were opened and closed beside it, want no deeper",
			one, after)
	}
	if !within(func() bool { return hooks.Load() == 2 }) {
		t.Fatalf("the backend left open heard %d reconnections, want 2", hooks.Load())
	}
	if err := live.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cycle(1000)
	// Two open at once, the first closed first.
	for range 500 {
		first, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		second, err := nq.BackendWithClient(t.Context(), nc, driverkit.ClientBorrowed, nq.Config{Addrs: []string{url}})
		if err != nil {
			t.Fatalf("BackendWithClient: %v", err)
		}
		_ = first.Close()
		_ = second.Close()
	}
	if got := reconnectDepth(t, nc, depth); got > bare {
		t.Fatalf("an event ran %d frames deep before any backend was opened and %d after 2,001 were opened and closed, want no deeper",
			bare, got)
	}
}

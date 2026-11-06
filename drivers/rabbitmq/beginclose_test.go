package rabbitmq_test

import (
	"context"
	"errors"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// TestClientCloseEndsAPublicationParkedInAFullSocket is Client.Close while a
// publication waits inside a socket the broker has stopped reading under an
// alarm.
//
// The write holds the client library's connection-wide write lock, and only
// the broker reading again or the connection closing ends it. The publisher's
// Close waits for the publication, and the client closes every publisher
// before the backend, so Client.Close waited for as long as the alarm lasted.
// The client now tells the backend first (mq.ClosingBackend, iqmq v1.2.0),
// and while the connection is blocked the backend closes it, which fails the
// write: the publication is ambiguous, and Close returns.
func TestClientCloseEndsAPublicationParkedInAFullSocket(t *testing.T) {
	closeBehindAFullSocket(t, func(cl *mq.Client, pub *mq.PublisherHandle, body []byte) error {
		return pub.Publish(context.Background(), &mq.Message{Body: body})
	})
}

// TestClientCloseEndsAReplyParkedInAFullSocket is
// TestClientCloseEndsAPublicationParkedInAFullSocket for a reply. The reply
// publisher's Close leaves a reply still inside its write to the write, which
// answers it once the library has let go of the caller's message, so its
// Close, and Client.Close, waited for the alarm as well.
func TestClientCloseEndsAReplyParkedInAFullSocket(t *testing.T) {
	closeBehindAFullSocket(t, func(cl *mq.Client, _ *mq.PublisherHandle, body []byte) error {
		return cl.Publish(context.Background(), rq.ReplyAddress+".aaaa.bbbb", &mq.Message{Body: body})
	})
}

// closeBehindAFullSocket parks a publication made with publish inside a
// socket the broker has stopped reading, closes the client, and checks that
// the close ends it.
func closeBehindAFullSocket(t *testing.T, publish func(cl *mq.Client, pub *mq.PublisherHandle, body []byte) error) {
	t.Helper()
	if v, old := rootModuleBefore(t, "v1.2.0"); old {
		t.Skipf("this build uses root module %s, whose Client.Close does not tell the backend it is closing", v)
	}
	broker := rabbitfake.New(rabbitfake.Config{SocketBuffer: 64 << 10})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	pub := publishOn(t, cl, "orders")
	// Once before the alarm, so that its publisher has its channel: a
	// `channel.open` waits for the broker to read again.
	if err := publish(cl, pub, []byte("before")); err != nil {
		t.Fatalf("publishing before the alarm: %v", err)
	}

	conn := broker.LastConnection()
	var writes sync.WaitGroup
	t.Cleanup(writes.Wait)
	conn.Alarm("low on disk")
	t.Cleanup(conn.ClearAlarm)
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 5*time.Second, b.Blocked)
	published := make(chan error, 1)
	writes.Go(func() { published <- publish(cl, pub, make([]byte, 128<<10)) })
	waitFor(t, 5*time.Second, conn.WriteBlocked)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Close returned %v after %v", err, time.Since(start))
	case <-time.After(3 * time.Second):
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("Client.Close was still waiting 3s into the alarm behind a publication parked in the socket, "+
			"and returned %v only once it cleared, %v after it began", err, time.Since(start))
	}
	select {
	case err := <-published:
		if !mq.IsAmbiguous(err) {
			t.Fatalf("the parked publication reported %v, want ambiguous: some of it may have reached the broker", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the parked publication had not returned 3s after Close did")
	}
	if n := len(broker.Connections()); n != 1 {
		t.Fatalf("the client made %d connections, want 1: closing the connection for Close made the driver "+
			"dial another", n)
	}
}

// TestAnAlarmHeardAfterBeginCloseEndsAPublicationParkedInAFullSocket is
// TestClientCloseEndsAPublicationParkedInAFullSocket with the broker's
// `connection.blocked` heard after BeginClose rather than before it.
//
// BeginClose found the connection unblocked and closed nothing, which is
// right, and the supervision loop goes on hearing the alarm until the
// teardown. The alarm then came, a publication parked in the socket it had
// filled, and nothing closed the connection: BeginClose runs once per close,
// and the alarm was only recorded. The publisher's Close, which is
// Client.Close's next step, and Client.Close behind it, waited for as long as
// the alarm lasted. The alarm heard after BeginClose now closes the connection
// as BeginClose would have. The test calls BeginClose itself, because
// Client.Close calls it once, before any publisher's close.
//
// The driver closes the connection as soon as it has heard the alarm, so the
// test holds the alarm's report to the OnReconnect hook, which comes first,
// while it parks the publication: the order a write that fills the socket
// just before the broker's notification arrives gives.
func TestAnAlarmHeardAfterBeginCloseEndsAPublicationParkedInAFullSocket(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{SocketBuffer: 64 << 10})
	t.Cleanup(func() { _ = broker.Close() })
	hook, arm, release := holdTheAlarmReport()
	defer release()
	cfg := base()
	cfg.Declare = true
	cfg.Hooks.OnReconnect = hook
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the alarm: %v", err)
	}
	conn := broker.LastConnection()
	t.Cleanup(conn.ClearAlarm)

	b.BeginClose()
	if conn.IsClosed() {
		t.Fatal("BeginClose closed a connection the broker reads, so this test proves nothing")
	}
	var writes sync.WaitGroup
	t.Cleanup(writes.Wait)
	arm()
	conn.Alarm("low on disk")
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 5*time.Second, b.Blocked)
	published := make(chan error, 1)
	writes.Go(func() { published <- pub.Publish(context.Background(), &mq.Message{Body: make([]byte, 128<<10)}) })
	waitFor(t, 5*time.Second, conn.WriteBlocked)

	closed := make(chan error, 1)
	go func() { closed <- pub.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("the publisher's Close returned %v behind a publication parked in the socket, before the driver "+
			"acted on the alarm, so this test proves nothing", err)
	case <-time.After(200 * time.Millisecond):
	}
	start := time.Now()
	release()
	select {
	case err := <-closed:
		t.Logf("the publisher's Close returned %v %v after the driver heard the alarm", err, time.Since(start))
	case <-time.After(3 * time.Second):
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("the publisher's Close after BeginClose was still waiting 3s into an alarm heard after it, and "+
			"returned %v only once the alarm cleared, %v after the driver heard it", err, time.Since(start))
	}
	select {
	case err := <-published:
		if !mq.IsAmbiguous(err) {
			t.Fatalf("the parked publication reported %v, want ambiguous: some of it may have reached the broker", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the parked publication had not returned 3s after the publisher's Close did")
	}
	if !conn.IsClosed() {
		t.Fatal("the connection is open after an alarm heard once the client had begun to close")
	}
	if n := len(broker.Connections()); n != 1 {
		t.Fatalf("the client made %d connections, want 1: closing the connection for the alarm made the driver "+
			"dial another", n)
	}
}

// holdTheAlarmReport is an OnReconnect hook that, once arm has been called,
// holds the report of `connection.blocked` until release. The driver reports
// an alarm before it acts on it ([Backend.onBlocked]), so a test can park a
// write in the full socket in between. release may be called more than once.
func holdTheAlarmReport() (hook func(context.Context, mq.ReconnectEvent), arm, release func()) {
	var armed atomic.Bool
	gate := make(chan struct{})
	var once sync.Once
	hook = func(_ context.Context, ev mq.ReconnectEvent) {
		if ev.Attempt == 0 && ev.Err != nil && armed.Load() {
			<-gate
		}
	}
	return hook, func() { armed.Store(true) }, func() { once.Do(func() { close(gate) }) }
}

// TestNothingReconnectsAfterBeginClose is a connection lost after BeginClose,
// with no alarm, while the handles are still to close.
//
// The contract is that nothing reconnects from BeginClose on, and an operation
// that needs a connection is answered at once. With no alarm BeginClose did
// nothing at all, so the supervision loop dialled a new connection and attached
// every handle to it, for the teardown to close again.
func TestNothingReconnectsAfterBeginClose(t *testing.T) {
	broker, cl := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
	})
	b := backendOf(t, cl)
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	b.BeginClose()
	lost := broker.LastConnection()
	if lost.IsClosed() {
		t.Fatal("BeginClose closed a connection the broker reads, so this test proves nothing")
	}
	lost.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	time.Sleep(300 * time.Millisecond)
	if n := len(broker.Connections()); n != 1 {
		t.Fatalf("the client dialled %d connections, want 1: it reconnected after BeginClose", n)
	}
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); !errors.Is(err, mq.ErrClosed) || mq.IsRetryable(err) {
		t.Fatalf("a publication after BeginClose and the loss: %v, want ErrClosed", err)
	}
	if err := b.Ping(t.Context()); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Ping after BeginClose and the loss: %v, want ErrClosed", err)
	}
}

// TestALossAfterBeginCloseLeavesCloseClean is the connection lost after
// BeginClose, with no alarm, and then the client's close.
//
// Nothing restores a client that has begun to close, and the supervision loop
// returned on the loss without uninstalling the dead connection, so the
// teardown closed it again. amqp091 answers the close of a closed connection
// with 504, and Client.Close returned it; without BeginClose the loss was
// repaired and Close returned nil. The fake answered nil, so only the broker
// showed it (TestALossAfterBeginCloseLeavesCloseCleanOnABroker).
func TestALossAfterBeginCloseLeavesCloseClean(t *testing.T) {
	broker, cl := openFake(t, declaring)
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)
	b.BeginClose()
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	time.Sleep(300 * time.Millisecond)
	if err := cl.Close(); err != nil {
		t.Fatalf("Client.Close after a loss that followed BeginClose: %v, want nil", err)
	}
}

// TestClientCloseEndsADelayedNacksDeclaration is Client.Close while a delayed
// nack waits for a declaration the broker has not answered, written before the
// broker blocked the connection and told the client so. Close closes the
// blocked connection first, which ends the declaration, and the nack reports
// ErrClosed.
func TestClientCloseEndsADelayedNacksDeclaration(t *testing.T) {
	if v, old := rootModuleBefore(t, "v1.2.0"); old {
		t.Skipf("this build uses root module %s, whose Client.Close does not tell the backend it is closing", v)
	}
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare, cfg.DelayKind, cfg.DelayQueueTTL = true, delay.TTLDLX, 10*time.Minute
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	d := receiveOne(t, cons)
	gate := broker.HoldDeclarations()
	t.Cleanup(gate.Release)
	nacked := make(chan error, 1)
	go func() { nacked <- d.Nack(mq.NackDelay(3 * time.Second)) }()
	waitFor(t, 5*time.Second, func() bool { return gate.Parked() == 1 })
	blockConnection(t, broker.LastConnection(), b, "low on disk")

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Close returned %v after %v", err, time.Since(start))
	case <-time.After(3 * time.Second):
		gate.Release()
		t.Fatalf("Client.Close was still waiting 3s behind the declaration, and returned %v once it was answered",
			<-closed)
	}
	select {
	case err := <-nacked:
		if !errors.Is(err, mq.ErrClosed) || mq.IsRetryable(err) {
			t.Fatalf("the delayed nack whose declaration Close ended reported %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		gate.Release()
		t.Fatalf("the delayed nack had not returned 3s after Close, and returned %v once released", <-nacked)
	}
}

// rootModuleBefore reports the version of the root module this test binary was
// built with, and whether it is a release older than want.
//
// CI also builds this module with GOWORK=off against the root release its
// go.mod requires, so a test of behaviour the root module gained after that
// release skips there until the release train moves the requirement on. A
// workspace build uses the root module in the tree, which the build
// information reports as (devel), and so does a replacement by a directory;
// neither is older than anything. drivers/kafka and drivers/redis have the
// same helper.
func rootModuleBefore(t testing.TB, want string) (string, bool) {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("this test binary carries no build information")
	}
	for _, dep := range info.Deps {
		if dep.Path != "github.com/iqhive/iqmq" {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		have, okHave := releaseNumbers(dep.Version)
		limit, okLimit := releaseNumbers(want)
		return dep.Version, okHave && okLimit && slices.Compare(have, limit) < 0
	}
	t.Fatal("this test binary's build information does not list the root module")
	return "", false
}

// releaseNumbers is the major, minor and patch numbers of a module version
// such as v1.1.1 or v1.1.2-0.20261030010203-0123456789ab, and false for
// anything else, (devel) and the empty version of a directory among them.
func releaseNumbers(version string) ([]int, bool) {
	core, ok := strings.CutPrefix(version, "v")
	if !ok {
		return nil, false
	}
	if i := strings.IndexAny(core, "-+"); i >= 0 {
		core = core[:i]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return nil, false
	}
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, false
		}
		out[i] = n
	}
	return out, true
}

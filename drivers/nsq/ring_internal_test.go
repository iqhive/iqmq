package nsq

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
	"github.com/iqhive/iqmq/testkit"
)

// TestADeliveryIsNotHeldInsideGoNSQBehindOneTheSweeperGaveBack holds the ring
// to what it is for: a delivery nsqd sends reaches the driver, and its lease
// starts, while the application is not fetching.
//
// The ring holds as many deliveries as the prefetch, which is all nsqd will
// have in flight to this consumer. But a delivery the sweeper gives back while
// it waits in the ring kept its place there until a Fetch dropped it, and the
// give-back freed a place at nsqd, which sent the next message. Its handler
// blocked waiting for room, and once every handler was blocked go-nsq held the
// message after that: nsqd's timeout for it was running and the driver had not
// seen it. When the application fetched again, that delivery was handed out as
// live with a whole lease ahead of it, after nsqd had timed it out and put the
// message back in the channel. The trigger is ordinary overload, an
// application too busy to fetch for longer than the ack deadline, which is
// exactly when the sweeper gives deliveries back.
func TestADeliveryIsNotHeldInsideGoNSQBehindOneTheSweeperGaveBack(t *testing.T) {
	const deadline = 30 * time.Second
	const guard = 100 * time.Millisecond
	w := newWireNSQD(t)
	w.mu.Lock()
	w.credit = true
	w.mu.Unlock()
	clock := testkit.NewClock(time.Time{})
	b, err := OpenBackend(context.Background(), Config{
		Addrs:       []string{w.ln.Addr().String()},
		AckDeadline: deadline,
		LeaseGuard:  guard,
		APITimeout:  2 * time.Second,
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("OpenBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 1,
		Metadata: mq.MetadataNativeStrict, AckDeadline: deadline,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	src := opened.(*source)
	recorded := func(n int) bool {
		var id gonsq.MessageID
		copy(id[:], fmt.Sprintf("%0*d", gonsq.MsgIDLength, n))
		src.mu.Lock()
		defer src.mu.Unlock()
		for k := range src.inflight {
			if k.id == id {
				return true
			}
		}
		return false
	}

	waitUntil(t, "message-1 to reach the driver", func() bool { return recorded(1) })
	// The application does not fetch. Inside the guard, the sweeper gives
	// message-1 back from the ring and nsqd sends message-2 in its place.
	clock.Advance(deadline - guard/2)
	waitUntil(t, "message-2 to reach the driver", func() bool { return recorded(2) })
	// And again: message-2 is given back and nsqd sends message-3.
	clock.Advance(deadline - guard/2)
	sentAt := clock.Now()
	waitUntil(t, "nsqd to send message-3", func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.sent >= 3
	})
	for start := time.Now(); !recorded(3); time.Sleep(time.Millisecond) {
		if time.Since(start) > 2*time.Second {
			t.Fatal("message-3, which nsqd has sent, has not reached the driver: go-nsq is holding it while the " +
				"only handler waits for room in a ring that holds deliveries the sweeper gave back. nsqd's " +
				"timeout for it is running and the driver's lease for it has not started")
		}
	}

	// When the application fetches, message-3 carries the lease nsqd gave it.
	clock.Advance(deadline / 2)
	d := wireFetch(t, src)
	if got := string(d.Message.Body); got != "message-3" {
		t.Fatalf("Fetch handed out %q, want message-3", got)
	}
	if !d.Info.ReceivedAt.Equal(sentAt) {
		t.Fatalf("message-3 was handed out with a lease from %v, %v after nsqd sent it", d.Info.ReceivedAt,
			d.Info.ReceivedAt.Sub(sentAt))
	}
	if err := src.Settler().Ack(context.Background(), &d); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestAHandlerWaitingForRoomWakesWhenTheSweeperGivesADeliveryBack is the same
// rule for a handler that was already waiting when the ring went dead under it.
//
// A ring full of live deliveries is the backpressure, and a handler that
// arrives then waits. nsqd can briefly have more in flight than the prefetch,
// when go-nsq moves RDY between connections, and that is when it happens. If
// the sweeper then gives a delivery in the ring back, the handler must not go
// on waiting for a Fetch.
func TestAHandlerWaitingForRoomWakesWhenTheSweeperGivesADeliveryBack(t *testing.T) {
	const deadline = 30 * time.Second
	const guard = 100 * time.Millisecond
	clock := testkit.NewClock(time.Time{})
	server := nsqfake.New()
	b, err := BackendWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, Config{
		Addrs:       []string{"nsqfake:4150"},
		AckDeadline: deadline,
		LeaseGuard:  guard,
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 1, Metadata: mq.MetadataNativeStrict,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	src := opened.(*source)
	log := &verbLog{}
	message := func(n byte) *gonsq.Message {
		var id gonsq.MessageID
		copy(id[:], "000000000000000")
		id[15] = n
		m := gonsq.NewMessage(id, []byte{n})
		m.Attempts = 1
		m.NSQDAddress = "nsqd-a:4150"
		m.Delegate = log
		return m
	}
	if err := src.handle(message('1')); err != nil {
		t.Fatalf("handle: %v", err)
	}
	// Half a lease later, so that the second delivery's guard is still far.
	clock.Advance(deadline / 2)
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		_ = src.handle(message('2'))
	}()
	waitUntil(t, "the second handler to wait for room", func() bool {
		src.pushMu.Lock()
		defer src.pushMu.Unlock()
		return src.room != nil
	})
	// The first delivery is now inside its guard, and the sweeper gives it back.
	clock.Advance(deadline/2 - guard/2)
	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatalf("the handler is still waiting for room after the sweeper gave the ring's only delivery back "+
			"(frames written: %q)", log.all())
	}
	d := wireFetch(t, src)
	if got := string(d.Message.Body); got != "2" {
		t.Fatalf("Fetch handed out %q, want the second message", got)
	}
}

// TestAnEvictionThatFreesSeveralPlacesWakesTheWaitingHandlers is the same rule
// when a handler's own eviction frees more room than it takes.
//
// Two live deliveries fill the ring and two more handlers wait behind them.
// The two in the ring then pass into their guard with no sweep to give them
// back, and a fifth delivery arrives: its handler evicts both and takes one
// place. A handler already waiting has to get the other, rather than wait for a
// Fetch or a sweep.
func TestAnEvictionThatFreesSeveralPlacesWakesTheWaitingHandlers(t *testing.T) {
	// A guard long enough that the source's own sweeper does not run.
	const deadline = 30 * time.Minute
	const guard = 20 * time.Minute
	clock := testkit.NewClock(time.Time{})
	server := nsqfake.New()
	b, err := BackendWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, Config{
		Addrs:          []string{"nsqfake:4150"},
		AckDeadline:    deadline,
		MaxAckDeadline: time.Hour,
		LeaseGuard:     guard,
		Clock:          clock,
	})
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 2, Metadata: mq.MetadataNativeStrict,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close() })
	src := opened.(*source)
	log := &verbLog{}
	message := func(n byte) *gonsq.Message {
		var id gonsq.MessageID
		copy(id[:], "000000000000000")
		id[15] = n
		m := gonsq.NewMessage(id, []byte{n})
		m.Attempts = 1
		m.NSQDAddress = "nsqd-a:4150"
		m.Delegate = log
		return m
	}
	for _, n := range []byte{'1', '2'} {
		if err := src.handle(message(n)); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	// The waiting two arrive later, so they are still short of their guard
	// when the first two are inside theirs.
	clock.Advance(5 * time.Minute)
	waiting := make(chan byte, 2)
	for _, n := range []byte{'3', '4'} {
		go func() {
			_ = src.handle(message(n))
			waiting <- n
		}()
	}
	waitUntil(t, "two handlers to wait for room", func() bool {
		buf := make([]byte, 1<<20)
		return strings.Count(string(buf[:runtime.Stack(buf, true)]), "(*source).admit(") == 2
	})
	clock.Advance(deadline - guard - 5*time.Minute + time.Minute)
	if err := src.handle(message('5')); err != nil {
		t.Fatalf("handle: %v", err)
	}
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatal("no waiting handler took the place the fifth handler's eviction freed; they wait for a Fetch")
	}
	// The ring is full of live deliveries again, so the other one still waits
	// and is released by Close.
	select {
	case n := <-waiting:
		t.Fatalf("handler %c was admitted to a ring full of live deliveries", n)
	case <-time.After(50 * time.Millisecond):
	}
}

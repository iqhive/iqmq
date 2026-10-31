package nsq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gonsq "github.com/nsqio/go-nsq"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/nsqfake"
	"github.com/iqhive/iqmq/testkit"
)

// heldRequeue is a message delegate whose FIRST requeue blocks until the test
// releases it, which is what holds the lease sweeper between its scan and the
// rest of its give-backs. Every later requeue passes straight through.
type heldRequeue struct {
	once    sync.Once
	first   chan gonsq.MessageID
	release chan struct{}
}

func (h *heldRequeue) OnFinish(*gonsq.Message) {}

func (h *heldRequeue) OnTouch(*gonsq.Message) {}

func (h *heldRequeue) OnRequeue(m *gonsq.Message, _ time.Duration, _ bool) {
	held := false
	h.once.Do(func() { held = true })
	if !held {
		return
	}
	h.first <- m.ID
	<-h.release
}

// signalRequeue reports that its message was requeued.
type signalRequeue struct{ done chan struct{} }

func (s signalRequeue) OnFinish(*gonsq.Message) {}

func (s signalRequeue) OnTouch(*gonsq.Message) {}

func (s signalRequeue) OnRequeue(*gonsq.Message, time.Duration, bool) { close(s.done) }

// heldTouch is a message delegate whose TOUCH blocks until the test releases
// it, which is an extension waiting on go-nsq's write loop. It records whether
// its message was requeued.
type heldTouch struct {
	entered  chan struct{}
	release  chan struct{}
	requeued atomic.Bool
}

func (h *heldTouch) OnFinish(*gonsq.Message) {}

func (h *heldTouch) OnTouch(*gonsq.Message) {
	close(h.entered)
	<-h.release
}

func (h *heldTouch) OnRequeue(*gonsq.Message, time.Duration, bool) { h.requeued.Store(true) }

// TestAnExtensionTheSweeperHasPickedIsNotGivenBack holds an extension and the
// lease sweeper to one answer between them.
//
// The sweeper picks what is due under the source's lock and gives it back
// outside it. An Extend whose TOUCH was still on its way when the sweeper
// picked the delivery used to move the deadline and report success, and the
// sweeper then gave the delivery back on the deadline it had read before the
// extension: the caller was told its lease was extended and the message was
// already back in the channel. Either the extension wins, and the sweeper
// leaves the delivery alone, or the give-back wins, and Extend reports
// ErrLeaseLost.
//
// The sweeper is held on the REQ of another due delivery, which is what a busy
// write loop does to it. It takes its due deliveries in a map's order, so a
// round in which it took the extended one first does not set the race up and
// is run again.
//
// The second case extends a delivery the moment it arrives, so that the
// extension moves the deadline only if it is measured from when go-nsq took the
// TOUCH: measured from when Extend was called, it left the deadline where it
// was, inside the guard by the time the TOUCH was handed over.
func TestAnExtensionTheSweeperHasPickedIsNotGivenBack(t *testing.T) {
	const deadline = 10 * time.Second
	for name, extendAt := range map[string]time.Duration{
		"half way through the lease": deadline / 2,
		"as it arrives":              0,
	} {
		t.Run(name, func(t *testing.T) {
			for range 40 {
				if extendWhileTheSweeperHolds(t, deadline, extendAt) {
					return
				}
			}
			t.Fatal("in forty rounds the sweeper never took the other delivery first, so the race was never set up")
		})
	}
}

// extendWhileTheSweeperHolds is one round of
// TestAnExtensionTheSweeperHasPickedIsNotGivenBack, with the extension made
// extendAt into a lease of deadline, and reports whether it set the race up.
func extendWhileTheSweeperHolds(t *testing.T, deadline, extendAt time.Duration) bool {
	t.Helper()
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
	defer func() { _ = b.Close() }()
	opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
		Topic: "orders", Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataNativeStrict,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	src := opened.(*source)
	gate := &heldRequeue{first: make(chan gonsq.MessageID, 1), release: make(chan struct{})}
	touch := &heldTouch{entered: make(chan struct{}), release: make(chan struct{})}
	swept := signalRequeue{done: make(chan struct{})}
	releaseGate := sync.OnceFunc(func() { close(gate.release) })
	releaseTouch := sync.OnceFunc(func() { close(touch.release) })
	defer func() {
		releaseGate()
		releaseTouch()
		_ = src.Close()
	}()

	inject := func(name byte, del gonsq.MessageDelegate, received time.Time, state uint32) *delivery {
		var id gonsq.MessageID
		copy(id[:], "000000000000000")
		id[15] = name
		msg := gonsq.NewMessage(id, []byte("x"))
		msg.Delegate = del
		d := &delivery{msg: msg, key: deliveryKey{id: id}, src: src, gen: src.gen.Add(1), received: received}
		d.expires.Store(received.Add(deadline).UnixNano())
		d.state.Store(state)
		src.mu.Lock()
		src.inflight[d.key] = d
		src.mu.Unlock()
		return d
	}
	t0 := clock.Now()
	inject('a', gate, t0, stateActive)
	extended := inject('b', touch, t0, stateActive)

	// The handler extends, and its TOUCH is held.
	clock.Advance(extendAt)
	result := make(chan error, 1)
	go func() {
		dd := mq.DriverDelivery{Ref: extended, Gen: extended.gen}
		result <- src.settleFn.Extend(context.Background(), &dd, deadline)
	}()
	<-touch.entered
	// Both are now inside the guard and short of the deadline, so the next
	// sweep takes both. A delivery already lost is answered after them, which
	// is how the test knows that sweep has finished.
	clock.Advance(deadline - extendAt - guard/2)
	inject('c', swept, clock.Now().Add(time.Hour), stateLost)
	select {
	case <-gate.first:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweeper gave nothing back")
	}
	if touch.requeued.Load() {
		// The sweeper took the extended delivery first, so its give-back
		// came before the extension's state check. Not this race.
		releaseTouch()
		releaseGate()
		if err := <-result; err == nil {
			t.Fatal("Extend reported success for a delivery the sweeper had already given back")
		}
		return false
	}
	releaseTouch()
	extendErr := <-result
	releaseGate()
	select {
	case <-swept.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweeper did not finish the sweep")
	}
	switch requeued := touch.requeued.Load(); {
	case extendErr == nil && requeued:
		t.Fatal("Extend reported success, and the sweeper then gave the same delivery back as if it had not " +
			"been extended")
	case extendErr != nil && !requeued:
		t.Fatalf("Extend reported %v, and the sweeper did not give the delivery back", extendErr)
	case extendErr == nil:
		dd := mq.DriverDelivery{Ref: extended, Gen: extended.gen}
		if err := src.settleFn.Ack(context.Background(), &dd); err != nil {
			t.Fatalf("Ack after a granted extension: %v", err)
		}
	}
	return true
}

// TestTheSweeperCountsOnlyWhatItGaveBack holds the sweeper's stale count to
// the deliveries the sweeper itself took out of circulation.
//
// The sweeper picks what is due under the source's lock and gives it back
// outside it, so a settlement can take a delivery in between: one whose lease
// check passed before the scan and whose claim lands after it, or one the
// lease check itself takes as lost. The sweeper counted every delivery it had
// picked whether or not its own give-back won, so the first was counted stale
// although it was settled, and the second was counted twice. Each case holds
// the sweeper on its first give-back, settles the other due delivery the way
// the case describes, and lets it go. A third delivery, already lost, is
// answered after the due ones in the same sweep, which is how the test knows
// the sweep has finished with them.
func TestTheSweeperCountsOnlyWhatItGaveBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		take func(t *testing.T, src *source, d *delivery)
		want uint64
	}{
		{"settled after the scan", func(t *testing.T, _ *source, d *delivery) {
			// The settlement's lease check ran before the scan; this is
			// its claim landing after it.
			if !d.claim() {
				t.Fatal("the delivery was no longer active")
			}
		}, 1},
		{"taken by the lease check after the scan", func(t *testing.T, src *source, d *delivery) {
			dd := mq.DriverDelivery{Ref: d, Gen: d.gen}
			if err := src.settleFn.Ack(context.Background(), &dd); err == nil {
				t.Fatal("an Ack inside the lease guard succeeded")
			}
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const guard = time.Second
			server := nsqfake.New()
			b, err := BackendWithClient(context.Background(), server.Producer(), driverkit.ClientOwned, Config{
				Addrs:       []string{"nsqfake:4150"},
				AckDeadline: 30 * time.Second,
				LeaseGuard:  guard,
				Clock:       server,
			})
			if err != nil {
				t.Fatalf("BackendWithClient: %v", err)
			}
			t.Cleanup(func() { _ = b.Close() })
			opened, err := b.OpenSource(context.Background(), mq.SourceConfig{
				Topic: "orders", Subscription: "workers", Prefetch: 4,
				Metadata: mq.MetadataNativeStrict,
			})
			if err != nil {
				t.Fatalf("OpenSource: %v", err)
			}
			src := opened.(*source)

			gate := &heldRequeue{first: make(chan gonsq.MessageID, 1), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate.release) }) }
			// Close waits for the sweeper, so a failed case must not leave it held.
			t.Cleanup(func() {
				release()
				_ = src.Close()
			})
			swept := signalRequeue{done: make(chan struct{})}

			// Both due deliveries are inside the guard and short of their
			// deadline at any scan in the next guard less ten milliseconds,
			// and the sweeper scans every half guard.
			now := b.now()
			due := map[gonsq.MessageID]*delivery{}
			src.mu.Lock()
			for i, del := range []gonsq.MessageDelegate{gate, gate, swept} {
				var id gonsq.MessageID
				copy(id[:], "0000000000000000")
				id[15] = byte('a' + i)
				msg := gonsq.NewMessage(id, []byte("x"))
				msg.Delegate = del
				d := &delivery{msg: msg, key: deliveryKey{id: id}, src: src, gen: src.gen.Add(1), received: now}
				d.expires.Store(now.Add(guard - 10*time.Millisecond).UnixNano())
				if i == 2 {
					d.expires.Store(now.Add(time.Hour).UnixNano())
					d.state.Store(stateLost)
				} else {
					due[id] = d
				}
				src.inflight[d.key] = d
			}
			src.mu.Unlock()

			var first gonsq.MessageID
			select {
			case first = <-gate.first:
			case <-time.After(5 * time.Second):
				t.Fatal("the sweeper gave nothing back")
			}
			for id, d := range due {
				if id != first {
					tc.take(t, src, d)
				}
			}
			release()
			select {
			case <-swept.done:
			case <-time.After(5 * time.Second):
				t.Fatal("the sweeper did not finish the sweep")
			}
			if got := b.Stale(); got != tc.want {
				t.Fatalf("Stale() = %d, want %d: the sweeper counted a delivery it did not give back", got, tc.want)
			}
		})
	}
}

// gatedClock is a test clock whose next reading, once the test arms it, waits
// until the test lets it go. It is how a test stops a goroutine at the one
// point between two steps where it reads the clock.
type gatedClock struct {
	*testkit.Clock
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
}

// arm makes the next Now wait for release, and returns the channel that is
// closed when it starts waiting.
func (c *gatedClock) arm(release chan struct{}) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entered, c.release = make(chan struct{}), release
	return c.entered
}

func (c *gatedClock) Now() time.Time {
	c.mu.Lock()
	entered, release := c.entered, c.release
	c.entered, c.release = nil, nil
	c.mu.Unlock()
	if release != nil {
		close(entered)
		<-release
	}
	return c.Clock.Now()
}

// TestTheSweeperCannotGiveBackADeliveryAnExtensionIsStoring holds the second
// half of TestAnExtensionTheSweeperHasPickedIsNotGivenBack: Extend's state
// check and its new deadline are one step as far as the sweeper can tell.
//
// Extend checks that the delivery is still active and then stores the new
// deadline, and between the two it reads the clock. A sweeper that took the
// delivery in between would see the old deadline, give the delivery back, and
// leave Extend to report success for it. The test stops Extend at that clock
// reading and runs the sweeper's give-back decision there; it must wait for
// the extension, then find the delivery no longer due.
//
// The lease guard is long enough that the source's own sweeper does not run
// during the test, so the clock reading Extend stops at is its own.
func TestTheSweeperCannotGiveBackADeliveryAnExtensionIsStoring(t *testing.T) {
	const deadline = 30 * time.Minute
	const guard = 20 * time.Minute
	clock := &gatedClock{Clock: testkit.NewClock(time.Time{})}
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
		Topic: "orders", Subscription: "workers", Prefetch: 4, Metadata: mq.MetadataNativeStrict,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	src := opened.(*source)
	touch := &heldTouch{entered: make(chan struct{}), release: make(chan struct{})}
	gate := make(chan struct{})
	releaseTouch := sync.OnceFunc(func() { close(touch.release) })
	releaseGate := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(func() {
		releaseTouch()
		releaseGate()
		_ = opened.Close()
	})

	var id gonsq.MessageID
	copy(id[:], "000000000000000x")
	msg := gonsq.NewMessage(id, []byte("x"))
	msg.Delegate = touch
	t0 := clock.Now()
	d := &delivery{msg: msg, key: deliveryKey{id: id}, src: src, gen: src.gen.Add(1), received: t0}
	d.expires.Store(t0.Add(deadline).UnixNano())
	src.mu.Lock()
	src.inflight[d.key] = d
	src.mu.Unlock()

	extended := make(chan error, 1)
	go func() {
		dd := mq.DriverDelivery{Ref: d, Gen: d.gen}
		extended <- src.settleFn.Extend(context.Background(), &dd, deadline)
	}()
	<-touch.entered
	// While the TOUCH waits, the delivery comes inside its guard: due for a
	// give-back on the deadline it has now.
	clock.Advance(deadline - guard + time.Minute)
	scan := clock.Now()
	if !d.expiringSoon(scan, guard) {
		t.Fatal("the delivery is not due for a give-back; the test is not set up")
	}
	stopped := clock.arm(gate)
	releaseTouch()
	<-stopped // Extend has checked the state and is reading the clock

	given := make(chan bool, 1)
	go func() { given <- src.loseIfDue(d, scan) }()
	select {
	case lost := <-given:
		// The give-back decision did not wait for the extension. That is
		// the defect whatever it decided, unless it left the delivery alone.
		releaseGate()
		err := <-extended
		if lost && err == nil {
			t.Fatal("Extend reported success for a delivery the sweeper gave back between Extend's state check " +
				"and its new deadline")
		}
		return
	case <-time.After(100 * time.Millisecond):
	}
	releaseGate()
	if err := <-extended; err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if <-given {
		t.Fatal("the sweeper gave back a delivery Extend had reported extended")
	}
	if st := d.state.Load(); st != stateActive {
		t.Fatalf("the extended delivery is in state %d, want active", st)
	}
}

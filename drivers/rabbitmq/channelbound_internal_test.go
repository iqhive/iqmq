package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
)

// These cases are the remedy for the four-way cycle CI found through
// `sink.attachMu`, and they test the BOUND rather than the cycle. The
// distinction is deliberate and the honest part of this change.
//
// The cycle. `sink.attach` holds `attachMu` across the broker round trip that
// opens a channel. A redial parked in there holds that mutex; the sink's own
// `loop` goroutine -- which `s.wg` counts and which `sink.Close` waits on
// through `s.wg.Wait()` -- reaches `onChannelLost` and needs the SAME mutex to
// report the channel it has just lost. The loop blocks on the attach, Close
// blocks on the loop, and nothing in that chain has a deadline, so the stall
// is unbounded. It is a product hazard and not a test artefact: every step is
// ordinary driver code, and a real broker that accepts a connection and then
// does not answer `channel.open` -- which is what resource pressure looks like
// on this protocol -- supplies the only thing the failing run supplied.
//
// **The cycle itself is NOT reproduced here, and these cases do not claim to.**
// It was observed once in CI and has not been reproduced on this host; an
// earlier attempt at a deterministic reproduction through the fake broker
// passed in 0.00 s with the bound at 250 ms, which means it never formed the
// cycle at all and was asserting nothing. What is deterministic, and what
// these cases pin, is that the round trip is bounded and that the bound
// produces a refusal a caller can act on -- which is the whole of the remedy.
// A test that cannot fail is worse than no test, so the
// vacuous one was deleted rather than kept for the look of coverage.

// withholdingConn is a connection that accepts `channel.open` and does not
// answer it, which is the fault the bound exists for.
type withholdingConn struct {
	client.Conn
	entered chan struct{}
	release chan struct{}
	hand    client.Channel
	once    sync.Once
	relOnce sync.Once
}

// releaseNow answers the withheld open. It is idempotent so that a case can
// release explicitly and still register the same call as a Cleanup, without
// reassigning the field the stub's goroutine is reading -- which is a data
// race, and was one here before the detector said so.
func (c *withholdingConn) releaseNow() { c.relOnce.Do(func() { close(c.release) }) }

func (c *withholdingConn) Channel() (client.Channel, error) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.hand, nil
}

// recordingChannel records whether anything closed it. Every other method is
// the embedded nil interface: a test that reaches one is a test that was not
// about the bound, and a nil-pointer panic names it immediately.
type recordingChannel struct {
	client.Channel
	mu     sync.Mutex
	closed bool
}

func (c *recordingChannel) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *recordingChannel) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// boundedBackend is the smallest Backend channelOnWithin needs: the bound it
// reads, a clock and the error mapper for the context route.
func boundedBackend(bound time.Duration) *Backend {
	b := &Backend{clock: wallClock{}, errs: newErrorMapper()}
	b.cfg.DialTimeout = bound
	return b
}

// TestAWithheldChannelOpenIsRefusedAtTheBound is the bound firing, and the
// refusal naming what it was waiting for.
//
// It also pins the one thing a naive bound gets wrong: `Channel()` cannot be
// cancelled, so the goroutine waiting on it survives the refusal and will hand
// back a real channel later. That channel belongs to nobody -- an orphan on a
// live connection is the hazard the specification is about -- so it has to be
// CLOSED, not dropped.
func TestAWithheldChannelOpenIsRefusedAtTheBound(t *testing.T) {
	const bound = 50 * time.Millisecond
	late := &recordingChannel{}
	cn := &withholdingConn{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		hand:    late,
	}
	// Released at the end so the withheld call unwinds; goleak holds this
	// package to leaving no goroutine behind.
	t.Cleanup(cn.releaseNow)

	b := boundedBackend(bound)
	start := time.Now()
	ch, err := b.channelOnWithin(context.Background(), cn)
	elapsed := time.Since(start)

	if ch != nil {
		t.Errorf("a refused open returned a channel: %#v", ch)
	}
	if !errors.Is(err, errChannelOpenTimeout) {
		t.Fatalf("the refusal is %v, want errChannelOpenTimeout; a bound that fires with some other error leaves a caller unable to tell a slow broker from a dead one", err)
	}
	var oe *mq.OpError
	if !errors.As(err, &oe) {
		t.Fatalf("the refusal is not an *mq.OpError, so a caller cannot classify it: %#v", err)
	}
	if !oe.Retryable {
		t.Error("the refusal is not Retryable, but the connection is up and the next attempt costs one round trip")
	}
	if oe.Code != mq.CodeUnavailable {
		t.Errorf("the refusal's code is %v, want CodeUnavailable", oe.Code)
	}
	if elapsed < bound {
		t.Errorf("the refusal took %v, which is less than the %v bound: it did not wait for the broker at all", elapsed, bound)
	}
	// Generous, because what is being measured is a timer firing and not a
	// hand-off: a run that takes twenty times the bound is a hang.
	if elapsed > 20*bound {
		t.Errorf("the refusal took %v against a %v bound", elapsed, bound)
	}

	// The late channel arrives only once the withheld call is released.
	cn.releaseNow()
	deadline := time.Now().Add(5 * time.Second)
	for !late.isClosed() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Microsecond)
	}
	if !late.isClosed() {
		t.Error("the channel the broker handed back after the bound expired was not closed: it is an orphan on a live connection, which is what the specification is about")
	}
}

// TestAWithheldChannelOpenIsRefusedWhenTheCallersContextGoesFirst is the other
// route out of the wait, and it matters because it is the SHORTER bound on the
// path that found the cycle: a closing handle's context expires long before
// Config.DialTimeout does.
func TestAWithheldChannelOpenIsRefusedWhenTheCallersContextGoesFirst(t *testing.T) {
	late := &recordingChannel{}
	cn := &withholdingConn{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		hand:    late,
	}
	t.Cleanup(cn.releaseNow)

	// An hour, so that a pass cannot be the DialTimeout route in disguise.
	b := boundedBackend(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-cn.entered
		cancel()
	}()
	start := time.Now()
	ch, err := b.channelOnWithin(ctx, cn)
	elapsed := time.Since(start)

	if ch != nil {
		t.Errorf("a refused open returned a channel: %#v", ch)
	}
	if err == nil {
		t.Fatal("a cancelled caller was not refused")
	}
	if errors.Is(err, errChannelOpenTimeout) {
		t.Fatalf("the refusal is the DialTimeout one against an hour-long bound, so this case did not test the context route: %v", err)
	}
	if elapsed > 30*time.Second {
		t.Errorf("the cancelled caller waited %v", elapsed)
	}
}

// TestAnOpenOnNoConnectionIsRefusedWithoutWaiting keeps the bounded path's
// promise, which the unbounded `channelOn` it replaced made first: no
// connection is errReconnecting, and it is answered immediately rather than
// after the bound. A caller that waited out DialTimeout for "there is no
// connection right now" would hold a goroutine for the whole outage, which is
// what [Backend.channelOnWithin]'s own comment says must not happen.
func TestAnOpenOnNoConnectionIsRefusedWithoutWaiting(t *testing.T) {
	b := boundedBackend(time.Hour)
	start := time.Now()
	ch, err := b.channelOnWithin(context.Background(), nil)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("a nil connection took %v to refuse", elapsed)
	}
	if ch != nil {
		t.Errorf("a nil connection returned a channel: %#v", ch)
	}
	if !errors.Is(err, errReconnecting) {
		t.Errorf("a nil connection was refused with %v, want errReconnecting", err)
	}
}

// TestAnAnsweredChannelOpenIsHandedStraightBack is the control. Without it
// every case above could be satisfied by a function that always refuses.
func TestAnAnsweredChannelOpenIsHandedStraightBack(t *testing.T) {
	want := &recordingChannel{}
	cn := &withholdingConn{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		hand:    want,
	}
	cn.releaseNow() // answered at once

	b := boundedBackend(time.Hour)
	ch, err := b.channelOnWithin(context.Background(), cn)
	if err != nil {
		t.Fatalf("an answered open was refused: %v", err)
	}
	if ch != client.Channel(want) {
		t.Errorf("the open returned %#v, not the channel the connection handed back", ch)
	}
	if want.isClosed() {
		t.Error("the channel handed to the caller was closed as if it were a late arrival")
	}
}

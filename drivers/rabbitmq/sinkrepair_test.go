package rabbitmq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// The publisher's repair route, and the health answer that
// goes with it,.
//
// A CHANNEL exception leaves the connection up, so the supervision loop never
// runs and the dial loop -- which is the only thing that ever attached a sink
// -- never re-attaches it. The sink's loop detached on the close notification
// and returned, and that was the end of the publisher: every Publish from then
// on was refused Unavailable and Retryable with a message naming a dial loop
// that was not running, for ever, while Health answered Ready. Probed against
// the fake before the fix, a `PRECONDITION_FAILED` on the publisher's channel
// was followed by 296 refusals in three seconds with one connection open and
// no recovery at all.

// TestCriticalAPublisherRepairsItsOwnChannelException is the defect itself: one
// exception, one repair, and a publisher that works again.
//
// The counts are exact rather than "more than none". "The publisher works
// again" is also true of a driver that attached TWO channels, which is the orphan reached from this route -- the broker would go on
// confirming tags on a channel whose confirms resolve the other channel's
// publications. So the assertion is on the number of channels the broker was
// asked to open and on the repair counter, both of which are one.
func TestCriticalAPublisherRepairsItsOwnChannelException(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	// Not armed: this dialler is here only for its count of channel opens,
	// which the broker's own view cannot give -- `Broker.Channels` lists the
	// channels that are OPEN, and a repair closes the one it declared on.
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	b := backendOf(t, cl)
	opens := dialer.opens.Load()
	repairs, restores := rq.ChannelRepairsForTest(b), rq.RestoresForTest(b)

	// A channel exception: the broker closes this one channel and leaves the
	// connection alone. A publication to an exchange somebody deleted reaches
	// exactly this state on a real broker.
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	waitFor(t, 10*time.Second, func() bool { return rq.ChannelRepairsForTest(b) > repairs })
	waitFor(t, 10*time.Second, func() bool { return cl.Health().State == mq.HealthReady })
	broker.Sync()

	if got := rq.ChannelRepairsForTest(b) - repairs; got != 1 {
		t.Fatalf("the exception was repaired %d times, want exactly 1: a second attach orphans the first "+
			"channel and its confirms then resolve the second channel's publications ", got)
	}
	if n := rq.RestoresForTest(b) - restores; n != 0 {
		t.Fatalf("the dial loop restored the client %d more time(s): a channel exception leaves the connection "+
			"up and must not cost a reconnection", n)
	}
	if n := len(broker.Connections()); n != 1 {
		t.Fatalf("the client holds %d connections, want 1: the repair re-dialled", n)
	}
	// One channel for the topology and one to publish on, which is what a
	// single `sink.attach` opens. Four would be two attaches.
	if got := dialer.opens.Load() - opens; got != 2 {
		t.Fatalf("the repair opened %d channels, want 2 (one for the topology, one to publish on): anything "+
			"more is a second attach", got)
	}
	if got := confirmingChannels(broker); got != 1 {
		t.Fatalf("%d publisher confirm channels are open after the repair, want 1: a second one is an orphan "+
			"the broker goes on confirming tags on", got)
	}
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("publishing after the repair: %v -- the publisher never recovered from a channel exception, "+
			"which is what the specification is about", err)
	}
}

// TestCriticalAPublisherRepairDoesNotRaceARedial is the close-vs-redial half.
//
// A lost CONNECTION closes every channel on it, so the sink's own loop gets a
// close notification for a channel the supervision loop has ALREADY detached
// and is about to replace. The generation has not moved at that point --
// `Backend.detachChildren` does not bump it -- so a repair keyed on the
// generation alone would fire here, attach a second channel beside the one the
// dial loop is installing, and leave the publisher with two channels and two
// confirm trackers: the orphan, reached from the repair route
// instead of from a late open.
//
// The interleaving is arranged and not raced for. The declaration gate parks
// the dial loop inside its topology replay, which is strictly AFTER
// `detachChildren` and strictly BEFORE the re-attach; the dialler holds the
// first connection's channel-close notifications until then, so the sink's
// loop reads its notification in exactly that window.
func TestCriticalAPublisherRepairDoesNotRaceARedial(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer, deliverLateCloses := holdChannelCloses(broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cfg.ReconnectBase = time.Millisecond
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the outage: %v", err)
	}
	b := backendOf(t, cl)
	restores, repairs := rq.RestoresForTest(b), rq.ChannelRepairsForTest(b)
	declined := rq.DeclinedClosesForTest(b)

	gate := broker.HoldDeclarations()
	t.Cleanup(gate.Release)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	// The dial loop is now inside the replay: it has detached every handle,
	// dialled, installed the new connection and not yet re-attached anything.
	waitFor(t, 10*time.Second, func() bool { return gate.Parked() > 0 })

	// Only now does the publisher's own close notification reach its loop.
	deliverLateCloses()
	waitFor(t, 10*time.Second, func() bool { return rq.DeclinedClosesForTest(b) > declined })
	if got := rq.ChannelRepairsForTest(b) - repairs; got != 0 {
		t.Fatalf("the publisher repaired its channel %d time(s) while the dial loop was mid-replay: the "+
			"supervision loop had already claimed that loss, and a second attach here is the orphan of "+
			"", got)
	}

	gate.Release()
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	waitFor(t, 10*time.Second, func() bool { return cl.Health().State == mq.HealthReady })
	broker.Sync()
	if got := confirmingChannels(broker); got != 1 {
		t.Fatalf("the publisher holds %d open confirm channels after the reconnection, want 1", got)
	}
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}
}

// TestCriticalAPublisherRepairRacingCloseInstallsNothing puts the repair and
// [sink.Close] in the one order that can go wrong, and holds them there.
//
// The repair runs on the sink's own loop goroutine, which is registered in the
// sink's WaitGroup, and it performs I/O: it opens a channel for the topology
// and another to publish on. So a Close that arrives while the first of those
// is in flight meets an attach that is past its early `isClosed` check. If the
// install section were not that check's second home , the
// attach would install a channel on a closed sink -- one nothing will ever
// close, because `b.forget` has already dropped the handle -- and call
// `wg.Add` while Close is inside `wg.Wait`.
//
// The dialler parks the repair's first channel open, so the order is arranged
// rather than won: Close is entered, and only then is the attach let through.
func TestCriticalAPublisherRepairRacingCloseInstallsNothing(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	b := backendOf(t, cl)

	dialer.arm()
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	// The repair is inside `sink.attach`, parked on its first channel open.
	dialer.waitReached(t)
	if got := confirmingChannels(broker); got != 0 {
		t.Fatalf("%d publisher confirm channels are open while the repair is still opening its first channel, "+
			"want 0", got)
	}

	closed := make(chan error, 1)
	go func() { closed <- pub.Close() }()
	// Close is now past `close(s.closed)` and is waiting for the loop
	// goroutine, which is the goroutine parked at the gate.
	waitFor(t, 10*time.Second, func() bool { return rq.ClosingSinksForTest(b) > 0 })
	// The `!isClosed()` guard in `sink.detached`, read HERE, which is the only
	// place it can be read: the sink is closed, has no channel, and is still
	// in `b.sinks`. The assertion that used to carry this claim ran after
	// Close had RETURNED, by which time `b.forget` has dropped the handle and
	// the walk cannot see it at all -- a check that could not fail for the
	// guard it named (review round 1, rigour-3). This window is also no longer
	// short: on this branch it is as long as a repair's channel open.
	if st := cl.Health().State; st != mq.HealthReady {
		t.Fatalf("the client reports %v while its one publisher is CLOSING, want ready: a handle on its way "+
			"out is nobody's problem -- `b.forget` is about to drop it, and reporting it degraded makes every "+
			"Close look like a fault for as long as it takes", st)
	}
	dialer.release()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("closing the publisher while its repair was in flight: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return: it waits for the loop goroutine, and that goroutine was inside the " +
			"repair's attach")
	}
	broker.Sync()
	if got := confirmingChannels(broker); got != 0 {
		t.Fatalf("%d publisher confirm channels are still open after Close, want 0: the repair installed a "+
			"channel on a closed sink, and nothing will ever close it ", got)
	}
	if st := cl.Health().State; st != mq.HealthReady {
		t.Fatalf("the client reports %v after its one publisher was closed, want ready: a closed handle is not "+
			"a detached one", st)
	}
}

// TestCriticalAPublisherWithNoChannelIsNotReady is.
//
// Health is the state machine of the CONNECTION, and a channel exception does
// not touch the connection -- so a client whose only publisher had lost its
// channel answered Ready while refusing every publication with CodeUnavailable.
// That is the one pair of answers a caller cannot act on: Ready says "use me",
// and the publisher's own error said "the driver's own dial loop is retrying",
// which was not happening either.
//
// The window is HELD OPEN by parking the repair's first channel open, rather
// than sampled between the exception and the repair, so the assertion is about
// the state and not about who won a race.
func TestCriticalAPublisherWithNoChannelIsNotReady(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	if st := cl.Health().State; st != mq.HealthReady {
		t.Fatalf("the client reports %v before anything went wrong, want ready", st)
	}

	dialer.arm()
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	dialer.waitReached(t)

	snap := cl.Health()
	if snap.State == mq.HealthReady {
		t.Fatal("the client reports ready while its only publisher has no channel and refuses every " +
			"publication: Ready is the one state a caller acts on, and answering it here is the defect " +
			"The specification is about")
	}
	if snap.Err == nil {
		t.Fatalf("health reports %v with no reason: the specification asks a degraded client to say why", snap.State)
	}
	// The refusal a publication meets in this window says what is true, and
	// not that a dial loop is retrying: none is.
	perr := pub.Publish(t.Context(), &mq.Message{Body: []byte("during")})
	if perr == nil {
		t.Fatal("a publication succeeded although the publisher has no channel")
	}
	var oe *mq.OpError
	if !errors.As(perr, &oe) || !oe.Retryable || oe.Code != mq.CodeUnavailable {
		t.Fatalf("the refusal is %v, want a retryable CodeUnavailable", perr)
	}
	if contains(perr.Error(), "dial loop") {
		t.Fatalf("the refusal blames the dial loop: %v. A channel exception leaves the connection up, so no "+
			"dial loop is running and the caller is told to wait for something that is not happening", perr)
	}

	dialer.release()
	waitFor(t, 10*time.Second, func() bool { return cl.Health().State == mq.HealthReady })
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the repair: %v", err)
	}
}

// TestCriticalAForeignConsumerCancelDoesNotStrandThePump is the fourth pump exit that skipped the channel's close notification.
//
// The pump is the only reader of `lost`, so every exit has to be one the close
// has already been answered on -- that is the rule, and three
// exits were fixed to obey it. The `basic.cancel` branch was not: it returned
// on ANY cancel notification, including one naming a consumer tag this source
// never registered, and a pump that returns on one leaves the source holding a
// channel whose exception nothing will ever repair.
//
// One channel carries one consumer here, so a foreign tag takes a broker that
// cancels by a name it was not given. The pump does not have to be right about
// that to be safe, and the test injects one through the channel the library's
// own `NotifyCancel` listener is registered on.
func TestCriticalAForeignConsumerCancelDoesNotStrandThePump(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := &cancelInjectingDialer{inner: broker.Dialer()}
	cfg := base()
	declaring(&cfg)
	cfg.ReconnectBase = time.Millisecond
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	cons := consumeOn(t, cl, "orders", "a")
	if _, err := broker.Publish("", "orders.a", amqp.Publishing{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	b := backendOf(t, cl)

	// A `basic.cancel` for a consumer this source never registered, and then
	// the wait that makes the order the defect needs: the pump has READ it
	// before the channel exception below is armed. Without that wait the pump
	// could answer the exception first and the test would pass on a driver
	// that returns on a foreign cancel.
	if !dialer.injectCancel("iqmq-somebody-elses-consumer") {
		t.Fatal("no cancel listener was registered, so nothing was injected")
	}
	waitFor(t, 10*time.Second, dialer.cancelsDrained)

	// The pump has to still be reading its channel's close notification, which
	// is only observable through what that notification does: repair the
	// channel.
	repairs := rq.ChannelRepairsForTest(b)
	consumerChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	waitFor(t, 10*time.Second, func() bool { return rq.ChannelRepairsForTest(b) > repairs })

	if _, err := broker.Publish("", "orders.a", amqp.Publishing{Body: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if err != nil || n != 1 || string(dst[0].Body) != "second" {
		t.Fatalf("after a cancel naming another consumer: n=%d err=%v -- the pump returned on a cancel that "+
			"was not its own, so nobody read the channel's close notification and the source was stranded "+
			"", n, err)
	}
	_ = dst[0].Ack()
}

// TestCriticalHealthDoesNotWaitForAPublicationInFlight is the hazard the
// derived health answer of the specification would otherwise have created.
//
// `sink.admit` holds the sink's lock across the socket write -- the tag, the
// encode and the write are one critical section, and they have to be -- so a
// Health that read `s.ch` under that lock would wait for the write. Under
// `connection.blocked` the broker STOPS READING from a publishing connection,
// so the write blocks for as long as the alarm lasts, and Health is the only
// thing that can explain the stall (`Backend.onBlocked` exists for
// exactly that). Reading a lock under a socket write is how a status call
// becomes an I/O call.
//
// The publication is parked inside `PublishWithContext` and stays parked until
// Health has answered, so a Health that took the lock does not return slowly
// here -- it does not return at all, and the test says so instead of timing
// anything.
func TestCriticalHealthDoesNotWaitForAPublicationInFlight(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	pub := publishOn(t, cl, "orders")

	dialer.armPublish()
	published := make(chan error, 1)
	go func() { published <- pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}) }()
	dialer.waitPublishReached(t)

	answered := make(chan mq.HealthSnapshot, 1)
	go func() { answered <- cl.Health() }()
	select {
	case snap := <-answered:
		if snap.State != mq.HealthReady {
			t.Fatalf("health answered %v while a publication was in flight, want ready: the publisher has its "+
				"channel, and a write in progress is not a degraded client", snap.State)
		}
	case <-time.After(10 * time.Second):
		dialer.releasePublish()
		t.Fatal("Health did not answer while a publication was parked in the socket write: it is reading the " +
			"sink's state under the lock `admit` holds across that write, so a broker under a memory alarm " +
			"would hang the one call that explains the stall ")
	}

	dialer.releasePublish()
	if err := <-published; err != nil {
		t.Fatalf("the held publication failed once released: %v", err)
	}
}

// confirmingChannels is how many open publisher channels the broker holds: in
// confirm mode, with no consumer on them.
func confirmingChannels(b *rabbitfake.Broker) int {
	n := 0
	for _, ch := range b.Channels() {
		if ch.IsConfirming() && len(ch.Consumers()) == 0 && !ch.IsClosed() {
			n++
		}
	}
	return n
}

// channelGateDialer parks ONE `Conn.Channel()` call, the next one after it is
// armed, until a test releases it.
//
// It is how a test stands inside `sink.attach`: between the detach that made
// the sink channel-less and the install that gives it one again, which is the
// state the publisher spends every repair in and the state
// and the specification are about. The declaration gate cannot hold that window, because
// a repair re-declares nothing -- the topology ledger already knows this
// publisher's exchange, so `Backend.ensure` issues no frame at all.
type channelGateDialer struct {
	inner client.Dialer
	// abandon is the test's own context, and it is how a parked channel open
	// frees itself when the test body ends -- on a FAILURE as much as on a
	// pass. A release registered as a cleanup cannot do it: cleanups run LIFO
	// and `publishOn` registers a `pub.Close` of its own, `sink.Close` waits
	// for the loop goroutine, and the loop goroutine is the one parked here.
	// So the close ran first and waited for a goroutine only a later cleanup
	// could free, and a failing run hung until the package timed out. The
	// instrument is part of the code under test, and this one deadlocked
	// exactly when it had something to report.
	abandon context.Context
	// pubArmed, pubReached and pubReleased are the same gate on a
	// `basic.publish` instead of a channel open, which is where the driver
	// holds the sink's lock across a socket write.
	pubArmed    atomic.Bool
	pubReached  chan struct{}
	pubReleased chan struct{}
	pubHit      sync.Once
	pubFreed    sync.Once
	// refuse is how many of the next channel opens fail, which is how a test
	// makes the one-shot repair's attach FAIL.
	refuse atomic.Int64
	// opens counts every channel this client has asked for, which is what
	// says how many times an attach ran: the broker lists the channels that
	// are open, and an attach closes the one it declared on.
	opens    atomic.Int64
	armed    atomic.Bool
	reached  chan struct{}
	released chan struct{}
	hit      sync.Once
	freed    sync.Once
}

func newChannelGateDialer(t *testing.T, inner client.Dialer) *channelGateDialer {
	t.Helper()
	return &channelGateDialer{
		inner:       inner,
		abandon:     t.Context(),
		reached:     make(chan struct{}),
		released:    make(chan struct{}),
		pubReached:  make(chan struct{}),
		pubReleased: make(chan struct{}),
	}
}

func (d *channelGateDialer) arm() { d.armed.Store(true) }

func (d *channelGateDialer) release() { d.freed.Do(func() { close(d.released) }) }

// waitReached blocks until the armed channel open has parked.
func (d *channelGateDialer) waitReached(t *testing.T) {
	t.Helper()
	select {
	case <-d.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("no channel was opened after the exception: nothing tried to repair the publisher, which is " +
			"the defect the specification is about")
	}
}

func (d *channelGateDialer) armPublish() { d.pubArmed.Store(true) }

func (d *channelGateDialer) releasePublish() { d.pubFreed.Do(func() { close(d.pubReleased) }) }

// waitPublishReached blocks until the armed publication has parked INSIDE
// `client.Channel.PublishWithContext`, which is where `sink.admit` holds the
// sink's lock: the tag, the encode and the write are one critical section.
func (d *channelGateDialer) waitPublishReached(t *testing.T) {
	t.Helper()
	select {
	case <-d.pubReached:
	case <-time.After(10 * time.Second):
		t.Fatal("no publication reached the channel")
	}
}

func (d *channelGateDialer) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	cn, err := d.inner.Dial(ctx, urls, cfg)
	if err != nil {
		return cn, err
	}
	return &channelGateConn{Conn: cn, d: d}, nil
}

type channelGateConn struct {
	client.Conn
	d *channelGateDialer
}

// errRefusedChannel is what a channel open fails with when a test has armed
// [channelGateDialer.refuseChannels]: a broker at `channel_max`, or one that
// refuses the repair's declaration.
var errRefusedChannel = errors.New("test: the broker refuses to open another channel")

// refuseChannels makes the next n channel opens fail, which is how a test
// makes the one-shot repair's attach FAIL and reaches the state the specification is about.
func (d *channelGateDialer) refuseChannels(n int64) { d.refuse.Store(n) }

func (c *channelGateConn) Channel() (client.Channel, error) {
	c.d.opens.Add(1)
	if c.d.refuse.Load() > 0 {
		c.d.refuse.Add(-1)
		return nil, errRefusedChannel
	}
	if c.d.armed.CompareAndSwap(true, false) {
		c.d.hit.Do(func() { close(c.d.reached) })
		select {
		case <-c.d.released:
		case <-c.d.abandon.Done():
		}
	}
	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return &channelGateChannel{Channel: ch, d: c.d}, nil
}

type channelGateChannel struct {
	client.Channel
	d *channelGateDialer
}

func (ch *channelGateChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if ch.d.pubArmed.CompareAndSwap(true, false) {
		ch.d.pubHit.Do(func() { close(ch.d.pubReached) })
		select {
		case <-ch.d.pubReleased:
		case <-ch.d.abandon.Done():
		}
	}
	return ch.Channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

// cancelInjectingDialer remembers every `NotifyCancel` listener its channels
// register, so that a test can deliver a `basic.cancel` the broker would not
// send: one naming a consumer tag this client never asked for.
type cancelInjectingDialer struct {
	inner client.Dialer
	mu    sync.Mutex
	// cancels are the listeners, in registration order.
	cancels []chan string
}

func (d *cancelInjectingDialer) injectCancel(tag string) bool {
	d.mu.Lock()
	listeners := append([]chan string(nil), d.cancels...)
	d.mu.Unlock()
	for _, c := range listeners {
		select {
		case c <- tag:
		default:
		}
	}
	return len(listeners) > 0
}

// cancelsDrained reports that every injected cancel has been taken off its
// listener, which is the pump having read it.
func (d *cancelInjectingDialer) cancelsDrained() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.cancels {
		if len(c) > 0 {
			return false
		}
	}
	return true
}

func (d *cancelInjectingDialer) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	cn, err := d.inner.Dial(ctx, urls, cfg)
	if err != nil {
		return cn, err
	}
	return &cancelInjectingConn{Conn: cn, d: d}, nil
}

type cancelInjectingConn struct {
	client.Conn
	d *cancelInjectingDialer
}

func (c *cancelInjectingConn) Channel() (client.Channel, error) {
	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return &cancelInjectingChannel{Channel: ch, d: c.d}, nil
}

type cancelInjectingChannel struct {
	client.Channel
	d *cancelInjectingDialer
}

func (ch *cancelInjectingChannel) NotifyCancel(c chan string) chan string {
	ch.d.mu.Lock()
	ch.d.cancels = append(ch.d.cancels, c)
	ch.d.mu.Unlock()
	return ch.Channel.NotifyCancel(c)
}

// TestCriticalAPublicationThatMetADeadChannelStillLeavesTheRepairToTheLoop
// pins the one decision this branch rests on that looks like an oversight:
// `sink.publishFailedLocked` detaches WITHOUT claiming the loss.
//
// The two other detaches claim, and the natural tidy-up -- make every detach
// consistent -- silently reinstates the original defect. amqp091's
// `Channel.shutdown` marks the channel closed BEFORE it broadcasts to its
// NotifyClose listeners, so a publication that runs between those two points
// takes the `publishFailedLocked` path while the sink's loop has not yet seen
// the notification. That is the ORDINARY case when a publisher's own publish
// is what provoked the exception. If that detach claimed, the close
// notification would find the loss taken, decline, and nobody would ever
// attach a channel again.
//
// Every other repair test injects the exception and lets the loop see it
// first, so none of them reaches this ordering. `holdChannelCloses` is what
// arranges it: the channel's close notification is held until after the
// publication has discovered the dead channel (review round 1, rigour-1).
func TestCriticalAPublicationThatMetADeadChannelStillLeavesTheRepairToTheLoop(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer, deliverLateCloses := holdChannelCloses(broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	t.Cleanup(deliverLateCloses)

	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	b := backendOf(t, cl)
	repairs := rq.ChannelRepairsForTest(b)

	// The channel dies, and its close notification is held, so the sink's loop
	// still believes it has a channel.
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	broker.Sync()

	// This publication is what discovers the dead channel, so
	// `publishFailedLocked` is the detacher. The specification makes it ambiguous: the
	// frame may have reached the broker.
	perr := pub.Publish(t.Context(), &mq.Message{Body: []byte("into the dead channel")})
	if perr == nil {
		t.Fatal("publishing on a channel the broker had closed succeeded")
	}
	var oe *mq.OpError
	if !errors.As(perr, &oe) || !oe.Ambiguous {
		t.Fatalf("the publication that met the dead channel reported %v, want an ambiguous outcome", perr)
	}
	if got := rq.ChannelRepairsForTest(b) - repairs; got != 0 {
		t.Fatalf("a publication's own detach repaired the channel %d time(s): the repair belongs to the loop, "+
			"which is the only thing that reads the close notification", got)
	}

	// Now the notification arrives. The loss must still be unclaimed.
	deliverLateCloses()
	waitFor(t, 10*time.Second, func() bool { return rq.ChannelRepairsForTest(b) > repairs })
	if got := rq.ChannelRepairsForTest(b) - repairs; got != 1 {
		t.Fatalf("the exception was repaired %d times, want exactly 1", got)
	}
	waitFor(t, 10*time.Second, func() bool { return cl.Health().State == mq.HealthReady })
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("the publisher never worked again: %v. A publication that met the dead channel had claimed "+
			"the loss, so the close notification declined it and nothing attached a channel -- which is the "+
			"original defect, reached by the route the specification says must not claim", err)
	}
}

// TestCriticalAPublisherWhoseRepairFailedSaysSoAndStopsBlaming is the state the one-shot repair leaves behind when it does not work.
//
// `sink.attach` is called from the open, from this repair and from the dial
// loop. `Publish` is none of them, and a channel that is already gone sends no
// second close notification, so a repair that fails is the END of the
// publisher until the CONNECTION is replaced -- which, on a channel exception,
// may be never. `errDetached` told that caller "the driver re-attaches it in
// the background": the same lie removed, narrowed from
// "always" to "after a failed repair", and measured at 1,831 refusals in two
// seconds with the broker healthy.
//
// So the assertions are the three a caller acts on: the refusal is NOT
// retryable (a retry fails identically, and telling a caller to spin is the
// original defect), it says what to do instead, and Health says the client
// will not mend itself.
func TestCriticalAPublisherWhoseRepairFailedSaysSoAndStopsBlaming(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	b := backendOf(t, cl)
	repairs := rq.ChannelRepairsForTest(b)

	// The repair's channel opens all fail, which is a broker at `channel_max`
	// or one that refuses the declaration -- the driver page's own example
	// (publish to a deleted exchange) reaches the same place.
	dialer.refuseChannels(8)
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	waitFor(t, 10*time.Second, func() bool { return rq.ChannelRepairsForTest(b) > repairs })
	// Wait for the repair to have FAILED, not merely for the publisher to have
	// lost its channel. "Health is not Ready" is satisfied by the detach, which
	// happens before the repair even starts -- and in THAT window the retryable
	// answer is the correct one, because a repair really is in flight. Waiting
	// on the weaker condition made this test assert on whichever answer it
	// happened to catch: it passed at `-count=1` and failed under
	// `GOMAXPROCS=2 -race -count=50`, reporting the refusal as retryable when
	// what it had caught was the repair still running.
	//
	// The failure's own cause reaching Health is the edge that says the repair
	// is over, because that is the moment `repairFailed` is stored.
	waitFor(t, 10*time.Second, func() bool {
		snap := cl.Health()
		return snap.Err != nil && errors.Is(snap.Err, errRefusedChannel)
	})
	broker.Sync()

	// The connection is up and nothing is re-dialling: this state is permanent.
	if n := len(broker.Connections()); n != 1 {
		t.Fatalf("the client holds %d connections, want 1: this test is about a live connection", n)
	}
	perr := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")})
	if perr == nil {
		t.Fatal("a publication succeeded although the repair failed and the publisher has no channel")
	}
	var oe *mq.OpError
	if !errors.As(perr, &oe) {
		t.Fatalf("the refusal is %v, want an *mq.OpError", perr)
	}
	if oe.Retryable {
		t.Fatalf("the refusal after a FAILED repair is marked retryable: %v. Nothing is attaching a channel "+
			"and nothing will until the connection is replaced, so Retryable tells the caller to spin "+
			"for ever -- which is the defect the specification was opened for", perr)
	}
	if contains(perr.Error(), "re-attaches it in the background") || contains(perr.Error(), "dial loop") {
		t.Fatalf("the refusal claims something is working on it: %v. The repair is one-shot and it failed", perr)
	}
	if !contains(perr.Error(), "close this publisher and open a new one") {
		t.Fatalf("the refusal does not say what the caller can actually do: %v", perr)
	}
	// The broker's own reason travels with it, so an operator can see WHY.
	if !errors.Is(perr, errRefusedChannel) {
		t.Fatalf("the refusal does not carry the reason the repair failed: %v", perr)
	}

	snap := cl.Health()
	if snap.State == mq.HealthReady {
		t.Fatal("the client reports ready although its only publisher will never publish again")
	}
	if snap.Err == nil || !errors.Is(snap.Err, errRefusedChannel) {
		t.Fatalf("health reports %v, which does not name the failure that will not mend itself: a handle "+
			"whose repair failed is different news from one that is merely between channels", snap.Err)
	}
}

// TestCriticalAFailedRepairDoesNotPinHealthDegraded is the other half, and the failure mode this branch's own design argument
// predicted for the half that still reported into the state machine.
//
// `health.Degraded` writes the CONNECTION's state, and nothing clears that
// while the connection stays up: not a redial, because there is none, and not
// `connection.unblocked`. So one transient channel failure left the client
// Degraded through the caller doing exactly what the refusal now tells them --
// closing the broken publisher and opening a new one -- and publishing
// successfully on it.
func TestCriticalAFailedRepairDoesNotPinHealthDegraded(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	broken, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	if err := broken.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	b := backendOf(t, cl)
	repairs := rq.ChannelRepairsForTest(b)

	dialer.refuseChannels(8)
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	waitFor(t, 10*time.Second, func() bool { return rq.ChannelRepairsForTest(b) > repairs })
	// The repair has FAILED, not merely started: its cause reaching Health is
	// the edge, for the reason the test above gives at length.
	waitFor(t, 10*time.Second, func() bool {
		snap := cl.Health()
		return snap.Err != nil && errors.Is(snap.Err, errRefusedChannel)
	})

	// The channel opens work again -- the alarm, or whatever the broker was
	// short of, has passed. Nothing re-attaches the broken publisher even so:
	// that is the whole point of the state.
	dialer.refuseChannels(0)
	// The caller does the documented thing.
	if err := broken.Close(); err != nil {
		t.Fatalf("closing the publisher whose repair failed: %v", err)
	}
	fresh, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening a replacement publisher: %v", err)
	}
	t.Cleanup(func() { _ = fresh.Close() })
	if err := fresh.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("publishing on the replacement: %v", err)
	}

	if snap := cl.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the client reports %v (%v) although every open handle has a channel and a publication just "+
			"succeeded: a handle-level failure was written into the connection's state machine, which nothing "+
			"clears while the connection is up -- so the client is Degraded for ever after one transient "+
			"channel failure ", snap.State, snap.Err)
	}
}

// TestCriticalAConsumerWithNoChannelIsNotReady is the consumer half, which review round 1 found had no test at all: both
// directions of the mutation passed the whole package.
//
// The window is held open by parking the repair's channel open, so the
// assertion is about the state rather than about who won a race.
func TestCriticalAConsumerWithNoChannelIsNotReady(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	cons := consumeOn(t, cl, "orders", "a")
	if _, err := broker.Publish("", "orders.a", amqp.Publishing{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	if st := cl.Health().State; st != mq.HealthReady {
		t.Fatalf("the client reports %v before anything went wrong, want ready", st)
	}

	dialer.arm()
	consumerChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	dialer.waitReached(t)

	snap := cl.Health()
	if snap.State == mq.HealthReady {
		t.Fatal("the client reports ready while its only consumer has no channel and no consumer on the " +
			"broker: a Fetch in this window waits for a delivery nothing will send")
	}
	if snap.Err == nil {
		t.Fatalf("health reports %v with no reason: the specification asks a degraded client to say why", snap.State)
	}

	dialer.release()
	waitFor(t, 10*time.Second, func() bool { return cl.Health().State == mq.HealthReady })
	if _, err := broker.Publish("", "orders.a", amqp.Publishing{Body: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if err != nil || n != 1 {
		t.Fatalf("receiving after the repair: n=%d err=%v", n, err)
	}
	_ = dst[0].Ack()
}

// TestCriticalAConsumerThatNeverFetchedStaysReady is the `started` exclusion, which review round 1 found unpinned -- and which is worse
// than it looks: a source attaches on its FIRST Fetch, so nothing ever clears
// the state, and a client with one idle consumer would report Degraded for
// ever after any reconnection.
func TestCriticalAConsumerThatNeverFetchedStaysReady(t *testing.T) {
	broker, cl := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase = time.Millisecond
	})
	// Opened and never read from, which the specification makes an ordinary thing to do: the
	// `basic.consume` is deliberately deferred to the first Fetch.
	_ = consumeOn(t, cl, "orders", "idle")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	b := backendOf(t, cl)
	restores := rq.RestoresForTest(b)

	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}

	if snap := cl.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the client reports %v (%v) after a reconnection, want ready: its only consumer has never "+
			"fetched, so having no channel is its ordinary state and not a fault -- and since the consumer "+
			"only attaches on a Fetch, nothing would ever clear this", snap.State, snap.Err)
	}
}

// TestCriticalAConsumerTheBrokerCancelledStaysReady is the `cancelled`
// exclusion, also unpinned before round 1.
//
// A consumer the broker cancelled -- its queue was deleted -- has no channel
// and is not coming back, and the driver's documented answer is CodeNotFound
// from Fetch. Reporting the whole client Degraded for ever instead would
// replace a precise answer with a permanent alarm.
func TestCriticalAConsumerTheBrokerCancelledStaysReady(t *testing.T) {
	broker, cl := openFake(t, declaring)
	cons := consumeOn(t, cl, "orders", "a")
	if _, err := broker.Publish("", "orders.a", amqp.Publishing{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	b := backendOf(t, cl)

	ch := consumerChannel(t, broker)
	tags := ch.Consumers()
	if len(tags) != 1 {
		t.Fatalf("the consumer channel carries %d consumers, want 1", len(tags))
	}
	if !ch.CancelConsumer(tags[0]) {
		t.Fatal("the broker did not cancel the consumer")
	}
	waitFor(t, 10*time.Second, func() bool { return rq.AttachedForTest(b) == 0 })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	if _, err := cons.Receive(ctx, dst); err == nil || mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("Receive after the broker cancelled the consumer reported %v, want CodeNotFound", err)
	}
	if snap := cl.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the client reports %v (%v) after the BROKER cancelled one consumer, want ready: Fetch "+
			"already reports CodeNotFound, which says exactly what happened, and a permanent Degraded says "+
			"less", snap.State, snap.Err)
	}
}

// TestCriticalARepairInFlightIsStillRetryable is the near side of the line
// The specification draws, and the reason the state is DERIVED rather than
// stored.
//
// A publisher with no channel gets one of two answers, and they differ in the
// only thing a caller acts on: retry, or close and reopen. The first design
// kept a sticky record of "the repair failed" and made it the decision, and
// `FuzzRabbitTagSettlement` then found two separate races that made the record
// say the opposite of the truth -- a publisher the dial loop had just
// re-attached, refused as one that would never work again, not retryably, with
// advice to throw it away (seeds `12dfa8e3ef11d855` and `dca0feaa105c4983`).
//
// Neither race is reachable now, because there is nothing to corrupt: the
// answer is read from a flag the repair goroutine owns for exactly its own
// lifetime, set and cleared by one defer. This test stands on the near side of
// that flag, and `TestCriticalAPublisherWhoseRepairFailedSaysSoAndStopsBlaming`
// stands on the far side.
func TestCriticalARepairInFlightIsStillRetryable(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatalf("publishing before the exception: %v", err)
	}
	b := backendOf(t, cl)

	// A repair that FAILED, so that there is a recorded cause to be wrong
	// about, and then the publisher is put back into "a repair is in flight".
	repairs := rq.ChannelRepairsForTest(b)
	dialer.refuseChannels(8)
	publisherChannel(t, broker).CloseWith(&amqp.Error{
		Code: amqp.PreconditionFailed, Reason: "unknown delivery tag", Server: true,
	})
	waitFor(t, 10*time.Second, func() bool { return rq.ChannelRepairsForTest(b) > repairs })
	waitFor(t, 10*time.Second, func() bool {
		snap := cl.Health()
		return snap.Err != nil && errors.Is(snap.Err, errRefusedChannel)
	})

	if err := rq.RepairInFlightForTest(b, func() {
		perr := pub.Publish(t.Context(), &mq.Message{Body: []byte("during")})
		if perr == nil {
			t.Error("a publication succeeded while the publisher had no channel")
			return
		}
		var oe *mq.OpError
		if !errors.As(perr, &oe) || !oe.Retryable {
			t.Errorf("a publication refused WHILE a repair is attaching reported %v, want a retryable "+
				"refusal: a channel really is coming, and telling the caller to close the publisher "+
				"would throw away one that is about to work", perr)
			return
		}
		if contains(perr.Error(), "close this publisher") {
			t.Errorf("the refusal tells the caller to reopen while a repair is in flight: %v", perr)
		}
		if snap := cl.Health(); snap.Err != nil && contains(snap.Err.Error(), "re-attach attempt failed") {
			t.Errorf("health reports a failed repair while one is in flight: %v", snap.Err)
		}
	}); err != nil {
		t.Fatalf("driving the in-flight state: %v", err)
	}
}

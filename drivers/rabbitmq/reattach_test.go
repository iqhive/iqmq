package rabbitmq_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
)

// TestCriticalAPublisherOpenedDuringAReplayIsAttachedOnce is the publisher's side of the orphaned channel the specification found
// on the consumer's.
//
// The dial loop installs the new connection, replays the topology -- round
// trips -- and only then re-attaches every publisher it knows about. A
// publisher OPENED during the replay attaches itself on the new connection,
// is registered, and is then attached a second time by the dial loop. The
// second attach used to install a new channel and a new confirm tracker over
// the live ones without closing anything: the first channel stayed open, its
// loop kept applying ITS confirms to the NEW tracker, and the old tracker's
// publications were dropped from the books. The broker confirming the first
// channel's tag 1 then resolved whichever publication held tag 1 on the second
// -- a publication reported confirmed that the broker had not confirmed -- and
// the publication that really was tag 1 on the first channel never completed
// at all.
//
// The replay is held open with the fake's declaration gate, so the publisher
// is opened inside the window rather than raced into it.
func TestCriticalAPublisherOpenedDuringAReplayIsAttachedOnce(t *testing.T) {
	broker, client := openFakeWith(t,
		rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}},
		func(c *rq.Config) {
			c.Declare = true
			c.ReconnectBase = time.Millisecond
			c.ConfirmKind, c.ConfirmWindow = confirm.Async, 8
		})
	_ = publishOn(t, client, "orders")
	b := backendOf(t, client)
	restores := rq.RestoresForTest(b)
	gate := broker.HoldDeclarations()
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 5*time.Second, func() bool { return gate.Parked() > 0 })

	late, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		gate.Release()
		t.Fatalf("opening a publisher during the replay: %v", err)
	}
	t.Cleanup(func() { _ = late.Close() })

	type result struct {
		which string
		err   error
	}
	outcomes := make(chan result, 2)
	publish := func(which string) {
		t.Helper()
		if err := late.PublishAsync(t.Context(), &mq.Message{Body: []byte(which)},
			func(r mq.PublishResult) { outcomes <- result{which, r.Err} }); err != nil {
			t.Fatalf("publishing %s: %v", which, err)
		}
	}
	publish("A")
	gate.Release()
	waitFor(t, 5*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	publish("B")
	broker.Sync()

	var confirming []*rabbitfake.ChannelHandle
	for _, ch := range broker.Channels() {
		if ch.IsConfirming() && len(ch.Consumers()) == 0 && !ch.IsClosed() {
			confirming = append(confirming, ch)
		}
	}
	if len(confirming) != 2 {
		t.Fatalf("two publishers hold %d open confirm channels: the one opened during the replay was attached "+
			"twice, and its first channel is an orphan whose confirms land on the second's tracker", len(confirming))
	}
	// The broker confirms everything it holds, on whichever channel holds it.
	for _, ch := range confirming {
		for _, tag := range ch.PendingConfirms() {
			ch.AckPublication(tag)
		}
	}
	broker.Sync()
	got := map[string]error{}
	for len(got) < 2 {
		select {
		case r := <-outcomes:
			got[r.which] = r.err
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v completed after the broker confirmed every publication: an admitted publication's "+
				"callback has to run exactly once ", got)
		}
	}
	for _, which := range []string{"A", "B"} {
		if got[which] != nil {
			t.Fatalf("publication %s failed although the broker confirmed it: %v", which, got[which])
		}
	}
}

// TestCriticalARedialThatFailsPartwayReattachesEveryHandle is the other half
// a handle attached by a dial attempt that then FAILED is
// attached again by the next attempt.
//
// `redial` attaches the publishers and then the consumers, and when one of
// them fails it closes the connection it has just made and the dial loop tries
// again -- without running the invalidation that precedes the first attempt.
// So the handles attached before the failure come into the next attempt still
// holding channels on the connection that attempt closed. An attach guard that
// read "has a channel" as "is attached" skipped them, and they were then
// repaired only if their own close notification arrived; this test holds those
// notifications for ever, so a handle the next attempt skipped stays dead.
//
// The failure is injected on the second dial: the channel open after its first
// `basic.consume` fails, so one consumer and the publisher are attached on that
// connection and the other consumer is not.
func TestCriticalARedialThatFailsPartwayReattachesEveryHandle(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := &partialDialer{inner: broker.Dialer(), release: make(chan struct{})}
	t.Cleanup(func() { close(dialer.release) })
	cfg := base()
	cfg.Declare = true
	cfg.ReconnectBase = time.Millisecond
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	a := consumeOn(t, cl, "orders", "a")
	c := consumeOn(t, cl, "orders", "c")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	// Both consumers have started, so the dial loop re-attaches both.
	for _, cons := range []*mq.ConsumerHandle{a, c} {
		if d := receiveOne(t, cons); d.Ack() != nil {
			t.Fatal("acknowledging the first delivery failed")
		}
	}

	b := backendOf(t, cl)
	restores := rq.RestoresForTest(b)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	if n := dialer.failed.Load(); n != 1 {
		t.Fatalf("the injected channel failure fired %d time(s), want 1: the dial attempt this test is about "+
			"did not happen", n)
	}

	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("the publisher attached by the failed attempt never worked again: %v", err)
	}
	for name, cons := range map[string]*mq.ConsumerHandle{"a": a, "c": c} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		dst := make([]mq.Delivery, 1)
		n, err := cons.Receive(ctx, dst)
		cancel()
		if err != nil || n != 1 || string(dst[0].Body) != "after" {
			t.Fatalf("consumer %s after the reconnection: n=%d err=%v -- a handle attached by the failed attempt "+
				"was left on that attempt's closed connection", name, n, err)
		}
		_ = dst[0].Ack()
	}
}

// TestCriticalAPublisherReplacedByARetriedDialResolvesWhatItHeld pins the
// replacement half of the attach: when the dial loop replaces
// a publisher's channel -- one a failed attempt left on a connection it then
// closed -- the publications that channel was holding are resolved as
// ambiguous before the new channel's tag sequence starts again at one.
//
// Nothing else would resolve them. The old channel's close notification is
// what detaches a channel that dies alone, but its loop carries the old
// generation, and the new attach has already moved the sink past it, so that
// notification is dropped as stale; and the supervision loop's detach ran
// before the FIRST attempt, not between attempts. An admitted asynchronous
// publication whose callback never runs is the contract broken.
func TestCriticalAPublisherReplacedByARetriedDialResolvesWhatItHeld(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := &partialDialer{
		inner: broker.Dialer(), release: make(chan struct{}),
		gate: make(chan struct{}), reached: make(chan struct{}),
	}
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(dialer.gate) }) }
	t.Cleanup(func() { openGate(); close(dialer.release) })
	cfg := base()
	cfg.Declare = true
	cfg.ReconnectBase = time.Millisecond
	cfg.ConfirmKind, cfg.ConfirmWindow = confirm.Async, 8
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	pub := publishOn(t, cl, "orders")
	a := consumeOn(t, cl, "orders", "a")
	c := consumeOn(t, cl, "orders", "c")
	// Start both consumers with messages the broker puts straight on their
	// queues: the publisher's own confirms are manual here.
	for q, cons := range map[string]*mq.ConsumerHandle{"orders.a": a, "orders.c": c} {
		if _, err := broker.Publish("", q, amqp.Publishing{Body: []byte("start")}); err != nil {
			t.Fatal(err)
		}
		if d := receiveOne(t, cons); d.Ack() != nil {
			t.Fatal("acknowledging the first delivery failed")
		}
	}

	b := backendOf(t, cl)
	restores := rq.RestoresForTest(b)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	select {
	case <-dialer.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the dial attempt that fails part-way never reached its failure")
	}
	// The attempt has attached the publisher on its connection and is about to
	// fail. A publication now goes out on that channel and waits for a confirm
	// the broker will never send.
	outcome := make(chan error, 1)
	if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte("P")},
		func(r mq.PublishResult) { outcome <- r.Err }); err != nil {
		t.Fatalf("publishing on the attempt's channel: %v", err)
	}
	broker.Sync()
	openGate()
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	select {
	case err := <-outcome:
		var oe *mq.OpError
		if !errors.As(err, &oe) || !oe.Ambiguous {
			t.Fatalf("the publication on the replaced channel completed with %v, want an ambiguous outcome: the "+
				"broker may have taken it", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the publication on the replaced channel never completed: its callback has to run exactly once ")
	}
}

// TestCriticalAConsumerReplacedByARetriedDialWakesItsParkedNack pins the
// consumer's side of the same replacement: a channel a failed attempt left
// behind is retired the way a lost one is -- `claimLost` then `discard` --
// and not merely overwritten.
//
// The one thing only the retirement does is wake a delayed nack that is
// parked on the old channel waiting for its re-publication's confirm. The old
// channel's confirm will never come and its close notification is held here,
// so an attach that only installed a new channel left the nack waiting for
// the full publish timeout -- thirty seconds by default -- for an answer the
// retirement gives at once: ambiguous, because the parked copy may have
// landed.
func TestCriticalAConsumerReplacedByARetriedDialWakesItsParkedNack(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := &partialDialer{
		inner: broker.Dialer(), release: make(chan struct{}),
		gate: make(chan struct{}), reached: make(chan struct{}),
	}
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(dialer.gate) }) }
	t.Cleanup(func() { openGate(); close(dialer.release) })
	cfg := base()
	cfg.Declare = true
	cfg.ReconnectBase = time.Millisecond
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	queues := map[string]*mq.ConsumerHandle{
		"orders.a": consumeOn(t, cl, "orders", "a"),
		"orders.c": consumeOn(t, cl, "orders", "c"),
	}
	for q, cons := range queues {
		if _, err := broker.Publish("", q, amqp.Publishing{Body: []byte("start")}); err != nil {
			t.Fatal(err)
		}
		if d := receiveOne(t, cons); d.Ack() != nil {
			t.Fatal("acknowledging the first delivery failed")
		}
	}

	b := backendOf(t, cl)
	restores := rq.RestoresForTest(b)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	select {
	case <-dialer.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the dial attempt that fails part-way never reached its failure")
	}
	// One of the two consumers is attached on the attempt's connection; it is
	// the one that receives.
	for q := range queues {
		if _, err := broker.Publish("", q, amqp.Publishing{Body: []byte("d")}); err != nil {
			t.Fatal(err)
		}
	}
	var held mq.Delivery
	for _, cons := range queues {
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		dst := make([]mq.Delivery, 1)
		n, _ := cons.Receive(ctx, dst)
		cancel()
		if n == 1 {
			held = dst[0]
			break
		}
	}
	if held.Body == nil {
		openGate()
		t.Fatal("neither consumer delivered on the attempt's connection, so there is nothing to park")
	}
	nacked := make(chan error, 1)
	go func() { nacked <- held.Nack(mq.NackDelay(time.Second)) }()
	// The re-publication into the delay queue is out and unconfirmed.
	waitFor(t, 5*time.Second, func() bool {
		for _, ch := range broker.Channels() {
			if len(ch.PendingConfirms()) > 0 {
				return true
			}
		}
		return false
	})
	openGate()
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	select {
	case err := <-nacked:
		if err == nil {
			t.Fatal("the parked nack reported success although its confirm never came")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the delayed nack parked on the replaced channel was still waiting five seconds after the dial " +
			"loop replaced that channel: the replacement did not retire it")
	}
}

// partialDialer makes its SECOND connection -- the first reconnection -- fail
// part-way through the dial loop's re-attach, and holds every channel-close
// notification on that connection until release is closed.
//
// When gate is not nil the failing channel open first closes reached and then
// waits for gate, which is how a test does something to the handles the
// attempt HAS attached before it fails.
type partialDialer struct {
	inner   client.Dialer
	release chan struct{}
	gate    chan struct{}
	reached chan struct{}
	dials   atomic.Int64
	failed  atomic.Int64
}

var errInjectedChannel = errors.New("injected: the broker refused to open a channel")

func (d *partialDialer) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	cn, err := d.inner.Dial(ctx, urls, cfg)
	if err != nil || d.dials.Add(1) != 2 {
		return cn, err
	}
	return &partialConn{Conn: cn, d: d}, nil
}

type partialConn struct {
	client.Conn
	d        *partialDialer
	mu       sync.Mutex
	consumed bool
	failOnce bool
}

func (c *partialConn) Channel() (client.Channel, error) {
	c.mu.Lock()
	fail := c.consumed && !c.failOnce
	if fail {
		c.failOnce = true
	}
	c.mu.Unlock()
	if fail {
		if c.d.gate != nil {
			close(c.d.reached)
			<-c.d.gate
		}
		c.d.failed.Add(1)
		return nil, errInjectedChannel
	}
	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return &partialChannel{Channel: ch, c: c}, nil
}

type partialChannel struct {
	client.Channel
	c *partialConn
}

func (ch *partialChannel) ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	out, err := ch.Channel.ConsumeWithContext(ctx, queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err == nil {
		ch.c.mu.Lock()
		ch.c.consumed = true
		ch.c.mu.Unlock()
	}
	return out, err
}

// NotifyClose holds the notification until the test ends, which is a handle's
// own repair route taken away: what is left is the dial loop's.
func (ch *partialChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	relay := make(chan *amqp.Error, 1)
	ch.Channel.NotifyClose(relay)
	go func() {
		aerr := <-relay
		<-ch.c.d.release
		select {
		case c <- aerr:
		default:
		}
	}()
	return c
}

// TestCriticalALateConfirmFromAReplacedChannelResolvesNothing pins the
// generation check on a publisher's confirms.
//
// A confirm names a tag in its own channel's sequence, and a replacement
// channel's sequence starts again at one. So a confirm that the old channel's
// loop reads AFTER the replacement -- one the library had already queued when
// the connection went -- names, in the new tracker, a publication the broker
// has not confirmed at all. The dialer here holds the first connection's
// confirms and close notifications, releases the confirm once the publisher
// has published again on the new connection, and the broker then NACKS that
// publication: it has to be reported rejected, not confirmed by a stranger's
// ack.
func TestCriticalALateConfirmFromAReplacedChannelResolvesNothing(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := &lateConfirmDialer{inner: broker.Dialer(), confirms: make(chan struct{}), closes: make(chan struct{})}
	t.Cleanup(dialer.releaseAll)
	cfg := base()
	cfg.Declare = true
	cfg.ReconnectBase = time.Millisecond
	cfg.ConfirmKind, cfg.ConfirmWindow = confirm.Async, 8
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	pub := publishOn(t, cl, "orders")

	type result struct {
		which string
		err   error
	}
	outcomes := make(chan result, 2)
	publish := func(which string) {
		t.Helper()
		if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte(which)},
			func(r mq.PublishResult) { outcomes <- result{which, r.Err} }); err != nil {
			t.Fatalf("publishing %s: %v", which, err)
		}
	}
	publish("A")
	broker.Sync()
	// The broker confirms A, and the confirm is held on its way to the client.
	publisherChannel(t, broker).AckPublication(1)
	broker.Sync()

	b := backendOf(t, cl)
	restores := rq.RestoresForTest(b)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 5*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	// A's confirm never arrived, so the connection loss made it ambiguous.
	if r := <-outcomes; r.which != "A" || r.err == nil {
		t.Fatalf("the first outcome is %s=%v, want A reported ambiguous by the connection loss", r.which, r.err)
	}

	publish("B") // tag 1 on the new channel
	broker.Sync()
	stale := rq.StaleConfirmsForTest(b)
	dialer.releaseConfirms() // the OLD channel's "ack 1" now reaches its loop
	// The wait ends either way the old loop can answer: by dropping the
	// confirm, or by applying it to B -- which completes B, so the second
	// condition is what a defect satisfies and the assertion below reads.
	var early *result
	waitFor(t, 5*time.Second, func() bool {
		if rq.StaleConfirmsForTest(b) > stale {
			return true
		}
		select {
		case r := <-outcomes:
			early = &r
			return true
		default:
			return false
		}
	})
	if early != nil {
		t.Fatalf("%s completed with %v before the broker said anything about it: the replaced channel's ack for A "+
			"was applied to the new channel's tag 1", early.which, early.err)
	}
	publisherChannel(t, broker).NackPublication(1)
	broker.Sync()
	select {
	case r := <-outcomes:
		if r.which != "B" || r.err == nil {
			t.Fatalf("%s was reported %v; the broker NACKED B, and the only ack was the replaced channel's for A", r.which, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("B never completed after the broker nacked it")
	}
}

// lateConfirmDialer holds, on its FIRST connection only, every publisher
// confirm until releaseConfirms and every channel close notification until
// releaseAll.
type lateConfirmDialer struct {
	inner    client.Dialer
	confirms chan struct{}
	closes   chan struct{}
	dials    atomic.Int64
	once1    sync.Once
	once2    sync.Once
}

func (d *lateConfirmDialer) releaseConfirms() { d.once1.Do(func() { close(d.confirms) }) }

func (d *lateConfirmDialer) releaseAll() {
	d.releaseConfirms()
	d.once2.Do(func() { close(d.closes) })
}

func (d *lateConfirmDialer) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	cn, err := d.inner.Dial(ctx, urls, cfg)
	if err != nil || d.dials.Add(1) != 1 {
		return cn, err
	}
	return &lateConfirmConn{Conn: cn, d: d}, nil
}

type lateConfirmConn struct {
	client.Conn
	d *lateConfirmDialer
}

func (c *lateConfirmConn) Channel() (client.Channel, error) {
	ch, err := c.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return &lateConfirmChannel{Channel: ch, d: c.d}, nil
}

type lateConfirmChannel struct {
	client.Channel
	d *lateConfirmDialer
}

func (ch *lateConfirmChannel) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	relay := make(chan amqp.Confirmation, cap(c))
	ch.Channel.NotifyPublish(relay)
	go func() {
		<-ch.d.confirms
		for conf := range relay {
			select {
			case c <- conf:
			case <-ch.d.closes:
				return
			}
		}
	}()
	return c
}

func (ch *lateConfirmChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	relay := make(chan *amqp.Error, 1)
	ch.Channel.NotifyClose(relay)
	go func() {
		aerr := <-relay
		<-ch.d.closes
		select {
		case c <- aerr:
		default:
		}
	}()
	return c
}

// TestCriticalAReplayThatKeepsFailingStopsAtMaxReconnects holds the bound a
// caller sets with MaxReconnects when the DIAL succeeds and the topology replay
// is what fails.
//
// `redial` used to install the new connection, close it when the replay
// failed, and leave it installed. Once the dial loop gave up, the supervision
// loop found that closed connection still current, re-armed NotifyClose on it
// -- which answers at once -- and began a new dial loop with a new attempt
// count, so the client redialled for ever and MaxReconnects was never reached.
func TestCriticalAReplayThatKeepsFailingStopsAtMaxReconnects(t *testing.T) {
	const maxReconnects = 3
	var refusing atomic.Bool
	broker, cl := openFakeWith(t, rabbitfake.Config{
		// Every channel opened once the outage starts refuses its first
		// declaration, which is the replay's first round trip.
		OnChannel: func(h *rabbitfake.ChannelHandle) {
			if refusing.Load() {
				h.FailNextDeclare(&amqp.Error{Code: amqp.AccessRefused, Reason: "test: replay refused", Server: true})
			}
		},
	}, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
		c.MaxReconnects = maxReconnects
	})
	_ = publishOn(t, cl, "orders")

	before := len(broker.Dials())
	refusing.Store(true)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 5*time.Second, func() bool { return len(broker.Dials())-before >= maxReconnects })
	// The loop has had every attempt it is allowed. Anything dialled after a
	// pause many times the backoff is a loop that did not stop.
	time.Sleep(200 * time.Millisecond)
	if got := len(broker.Dials()) - before; got != maxReconnects {
		t.Fatalf("the client dialled %d times after the connection was lost, want exactly MaxReconnects (%d): a "+
			"connection whose replay failed was left installed and restarted the dial loop", got, maxReconnects)
	}
	if snap := cl.Health(); snap.State == mq.HealthReady {
		t.Fatalf("health is %v after the dial loop gave up, want it to say the client cannot work", snap.State)
	}
}

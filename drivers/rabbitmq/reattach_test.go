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

// TestCriticalADeletedSubscriptionDoesNotStopTheClientReconnecting is the
// redial meeting a consumer the BROKER cancelled.
//
// Deleting a subscription under a live consumer makes the broker cancel it,
// and the source then answers every Fetch with CodeNotFound for good. The dial
// loop used to subscribe it again all the same, because `reattach` asked only
// whether the consumer had started: the queue was gone and out of the ledger,
// so the `basic.consume` met a 404, the 404 failed the whole attempt, and the
// client never reconnected. Every publisher and every other consumer of the
// client went down with the one deleted subscription.
//
// The cancelled consumer's channel is the other half. Nothing reads from it
// once the broker has cancelled its only consumer, and it used to stay open
// for the life of the connection.
func TestCriticalADeletedSubscriptionDoesNotStopTheClientReconnecting(t *testing.T) {
	broker, cl := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
	})
	cons := consumeOn(t, cl, "orders", "a")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	b := backendOf(t, cl)
	consuming := consumerChannel(t, broker)

	admin, err := cl.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "a"); err != nil {
		t.Fatalf("deleting the subscription: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return rq.AttachedForTest(b) == 0 })
	deadline := time.Now().Add(5 * time.Second)
	for !consuming.IsClosed() {
		if time.Now().After(deadline) {
			t.Fatal("the channel of the consumer the broker cancelled is still open: nothing will ever read from it")
		}
		time.Sleep(time.Millisecond)
	}

	restores, dials := rq.RestoresForTest(b), len(broker.Dials())
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	deadline = time.Now().Add(10 * time.Second)
	for rq.RestoresForTest(b) == restores {
		if time.Now().After(deadline) {
			t.Fatalf("the client has not reconnected after %d dial attempts: the redial subscribed the consumer "+
				"the broker cancelled again, and its deleted queue's 404 failed every attempt", len(broker.Dials())-dials)
		}
		time.Sleep(time.Millisecond)
	}
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	if _, err := cons.Receive(ctx, dst); mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("Receive on the cancelled consumer after the reconnection reported %v, want CodeNotFound", err)
	}
	if snap := cl.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the client reports %v (%v) after reconnecting, want ready", snap.State, snap.Err)
	}
}

// TestCriticalARedialDoesNotResubscribeAConsumerTheBrokerCancelled is the
// same redial when the queue comes BACK: another application deleted it, so
// this client's ledger still records it and the replay declares it again.
//
// The cancelled consumer used to be subscribed to the new queue. Its ring was
// closed, so every message the broker pushed to it stayed unacknowledged on a
// live channel, invisible to the caller and to every other consumer of the
// subscription, until the connection next went down.
func TestCriticalARedialDoesNotResubscribeAConsumerTheBrokerCancelled(t *testing.T) {
	broker, cl := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
	})
	cons := consumeOn(t, cl, "orders", "a")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}
	b := backendOf(t, cl)
	conn := broker.LastConnection()

	cfg := base()
	cfg.Declare = true
	other, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening the other application's client: %v", err)
	}
	t.Cleanup(func() { _ = other.Close() })
	admin, err := other.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), "orders", "a"); err != nil {
		t.Fatalf("deleting the subscription from the other client: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return rq.AttachedForTest(b) == 0 })

	restores := rq.RestoresForTest(b)
	conn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, func() bool { return rq.RestoresForTest(b) > restores })
	if _, ok := broker.Queue("orders.a"); !ok {
		t.Fatal("the replay did not declare the deleted queue again, so this test proves nothing")
	}
	if n := consumerTags(broker); n != 0 {
		t.Fatalf("the broker has %d consumer(s) after the redial, want none: the consumer it cancelled was "+
			"subscribed again, and nothing reads what the broker pushes to it", n)
	}

	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}
	fresh := consumeOn(t, cl, "orders", "a")
	d := receiveOne(t, fresh)
	if string(d.Body) != "after" {
		t.Fatalf("a new consumer of the subscription received %q, want %q", d.Body, "after")
	}
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if _, err := cons.Receive(t.Context(), make([]mq.Delivery, 1)); mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("Receive on the cancelled consumer after the reconnection reported %v, want CodeNotFound", err)
	}
}

// TestCriticalAQueueDeletedDuringAnOutageDoesNotStopTheClientReconnecting is
// the redial meeting a subscription deleted while the client was DISCONNECTED.
//
// The broker cancels a consumer only while the consumer is there to be told,
// so this source never heard of the deletion and the dial loop subscribed it
// again. With no declaration of its own to replay (Declare is off, which is
// the production default) the `basic.consume` met a 404, the 404 failed the
// whole attempt, and the client never reconnected: every attempt after the
// broker came back met the same refusal, and every publisher of the client was
// refused for good. The consumer now gets the answer a broker cancel gives,
// CodeNotFound, and a Receive already waiting gets it too.
func TestCriticalAQueueDeletedDuringAnOutageDoesNotStopTheClientReconnecting(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })

	// The operator's client provisions the subscription and later deletes it.
	opsCfg := base()
	opsCfg.Declare = true
	ops, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), opsCfg)
	if err != nil {
		t.Fatalf("opening the operator's client: %v", err)
	}
	t.Cleanup(func() { _ = ops.Close() })
	admin, err := ops.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "orders", Name: "a"}); err != nil {
		t.Fatalf("provisioning the subscription: %v", err)
	}

	cfg := base()
	cfg.ReconnectBase, cfg.ReconnectMax = time.Millisecond, time.Millisecond
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening the application's client: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	conn := broker.LastConnection()
	b := backendOf(t, cl)

	cons := consumeOn(t, cl, "orders", "a")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}

	waiting := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		_, err := cons.Receive(ctx, make([]mq.Delivery, 1))
		waiting <- err
	}()

	deleteDuringAnOutage(t, broker, conn, b, admin)
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}

	select {
	case err := <-waiting:
		if mq.CodeOf(err) != mq.CodeNotFound {
			t.Fatalf("the Receive waiting through the outage reported %v, want CodeNotFound", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the Receive waiting through the outage is still waiting: nothing will ever deliver to a " +
			"consumer whose queue is gone")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := cons.Receive(ctx, make([]mq.Delivery, 1)); mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("Receive after the reconnection reported %v, want CodeNotFound", err)
	}
	waitFor(t, 5*time.Second, func() bool { return cl.Health().State != mq.HealthConnecting })
	if snap := cl.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the client reports %v (%v) after reconnecting, want ready: the consumer whose queue is gone "+
			"says so through Receive, as one the broker cancelled does", snap.State, snap.Err)
	}
}

// TestCriticalAQueueDeletedDuringAnOutageIsDeclaredAgainWhenTheClientDeclares
// is [TestCriticalAQueueDeletedDuringAnOutageDoesNotStopTheClientReconnecting]
// with Config.Declare on in the application's client.
//
// The client's own declaration of the queue is in the topology it replays, so
// the redial declares the queue again before the `basic.consume`, the consume
// succeeds, and the consumer goes on reading the new, empty queue. The driver
// page once said this consumer reported CodeNotFound as well, which holds only
// with Declare off.
func TestCriticalAQueueDeletedDuringAnOutageIsDeclaredAgainWhenTheClientDeclares(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })

	opsCfg := base()
	opsCfg.Declare = true
	ops, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), opsCfg)
	if err != nil {
		t.Fatalf("opening the operator's client: %v", err)
	}
	t.Cleanup(func() { _ = ops.Close() })
	admin, err := ops.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}

	cfg := base()
	cfg.Declare = true
	cfg.ReconnectBase, cfg.ReconnectMax = time.Millisecond, time.Millisecond
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening the application's client: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	conn := broker.LastConnection()
	b := backendOf(t, cl)

	cons := consumeOn(t, cl, "orders", "a")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}

	type received struct {
		d   mq.Delivery
		n   int
		err error
	}
	waiting := make(chan received, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		dst := make([]mq.Delivery, 1)
		n, err := cons.Receive(ctx, dst)
		waiting <- received{d: dst[0], n: n, err: err}
	}()

	deleteDuringAnOutage(t, broker, conn, b, admin)
	if err := publishRetrying(t.Context(), pub, "after"); err != nil {
		t.Fatalf("publishing after the reconnection: %v", err)
	}

	select {
	case got := <-waiting:
		if got.err != nil || got.n != 1 {
			t.Fatalf("the Receive waiting through the outage returned %d deliveries and %v, want the one "+
				"published to the queue the replay declared again", got.n, got.err)
		}
		if string(got.d.Body) != "after" || got.d.Ack() != nil {
			t.Fatalf("the Receive waiting through the outage got %q, want %q", got.d.Body, "after")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the Receive waiting through the outage is still waiting: the consumer is not reading the " +
			"queue the replay declared again")
	}
	waitFor(t, 5*time.Second, func() bool { return cl.Health().State != mq.HealthConnecting })
	if snap := cl.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the client reports %v (%v) after reconnecting, want ready: its consumer is subscribed to "+
			"the queue the replay declared again", snap.State, snap.Err)
	}
}

// deleteDuringAnOutage drops the client's connection with the broker refusing
// dials, deletes orders/a through admin while the client is in its dial loop,
// brings the broker back and waits for the client to reconnect.
func deleteDuringAnOutage(t *testing.T, broker *rabbitfake.Broker, conn *rabbitfake.ConnHandle, b *rq.Backend, admin mq.Admin) {
	t.Helper()
	restores, dials := rq.RestoresForTest(b), len(broker.Dials())
	broker.SetDown(true)
	conn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	// A dial the broker refuses is the client inside its dial loop, so the
	// deletion below happens while it is disconnected.
	waitFor(t, 10*time.Second, func() bool { return len(broker.Dials()) > dials })
	if err := admin.DeleteSubscription(t.Context(), "orders", "a"); err != nil {
		t.Fatalf("deleting the subscription during the outage: %v", err)
	}
	dials = len(broker.Dials())
	broker.SetDown(false)

	deadline := time.Now().Add(10 * time.Second)
	for rq.RestoresForTest(b) == restores {
		if time.Now().After(deadline) {
			t.Fatalf("the client has not reconnected after %d dial attempts since the broker came back: the "+
				"redial's basic.consume for the deleted queue was refused, and the refusal failed every attempt",
				len(broker.Dials())-dials)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCriticalAnExclusiveConsumerTakenDuringAnOutageDoesNotStopTheClientReconnecting
// is active/standby with ExclusiveConsumer: while the active client is
// disconnected the standby takes the exclusive consumer, and the active
// client's redial then meets 403 ACCESS_REFUSED on its `basic.consume`.
//
// The 403 used to fail the whole dial attempt, every attempt, so the active
// client never reconnected and its publisher was refused for good. The client
// now reconnects, the refused consumer stays without a channel and Health says
// so with the broker's reason. The consumer then behaves like a standby that
// has never consumed: each Receive asks the broker again and reports the 403,
// and the first Receive after the standby lets go takes the queue back,
// without waiting for another reconnection. Before that, a pair whose roles
// one outage had swapped was left with no consumer at all once the new
// active client went away.
func TestCriticalAnExclusiveConsumerTakenDuringAnOutageDoesNotStopTheClientReconnecting(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	open := func() *mq.Client {
		cfg := base()
		cfg.Declare, cfg.ExclusiveConsumer = true, true
		cfg.ReconnectBase, cfg.ReconnectMax = time.Millisecond, time.Millisecond
		c, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
		if err != nil {
			t.Fatalf("opening a client over the fake broker: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	active := open()
	conn := broker.LastConnection()
	b := backendOf(t, active)
	consA := consumeOn(t, active, "orders", "a")
	pubA := publishOn(t, active, "orders")
	if err := pubA.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, consA); d.Ack() != nil {
		t.Fatal("acknowledging the first delivery failed")
	}

	standby := open()
	consB := consumeOn(t, standby, "orders", "a")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	_, err := consB.Receive(ctx, make([]mq.Delivery, 1))
	cancel()
	if mq.CodeOf(err) != mq.CodeForbidden {
		t.Fatalf("the standby's Receive while the active consumer holds the queue reported %v, want "+
			"CodeForbidden, so this test proves nothing", err)
	}

	restores, dials := rq.RestoresForTest(b), len(broker.Dials())
	broker.SetDown(true)
	conn.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, func() bool { return len(broker.Dials()) > dials })

	// The standby's next Receive takes the exclusive consumer over.
	pubB := publishOn(t, standby, "orders")
	if err := pubB.Publish(t.Context(), &mq.Message{Body: []byte("taken over")}); err != nil {
		t.Fatal(err)
	}
	took := false
	for deadline := time.Now().Add(10 * time.Second); !took && time.Now().Before(deadline); {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		dst := make([]mq.Delivery, 1)
		if n, _ := consB.Receive(ctx, dst); n == 1 {
			took = dst[0].Ack() == nil
		}
		cancel()
	}
	if !took {
		t.Fatal("the standby never took the exclusive consumer over, so this test proves nothing")
	}

	dials = len(broker.Dials())
	broker.SetDown(false)
	deadline := time.Now().Add(10 * time.Second)
	for rq.RestoresForTest(b) == restores {
		if time.Now().After(deadline) {
			t.Fatalf("the active client has not reconnected after %d dial attempts since the broker came back: "+
				"the redial's basic.consume met the standby's exclusive consumer, and the 403 failed every attempt",
				len(broker.Dials())-dials)
		}
		time.Sleep(time.Millisecond)
	}
	if err := publishRetrying(t.Context(), pubA, "after"); err != nil {
		t.Fatalf("the active client's publisher after the reconnection: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return active.Health().State != mq.HealthConnecting })
	snap := active.Health()
	if snap.State != mq.HealthDegraded {
		t.Fatalf("the active client reports %v (%v) after reconnecting, want degraded: its consumer has no "+
			"channel and will not get one until the next reconnection", snap.State, snap.Err)
	}
	if code, _ := rq.ReplyCode(snap.Err); code != amqp.AccessRefused {
		t.Fatalf("health reports %v, want the broker's 403 refusal", snap.Err)
	}
	if n := consumerTags(broker); n != 1 {
		t.Fatalf("the broker has %d consumers, want the standby's one", n)
	}
	if d := receiveOne(t, consB); string(d.Body) != "after" || d.Ack() != nil {
		t.Fatalf("the standby received %q, want %q: it holds the exclusive consumer now", d.Body, "after")
	}

	// While the standby holds the queue, a Receive on the refused consumer
	// asks the broker again and reports the refusal, as the standby's own
	// Receive did.
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	_, err = consA.Receive(ctx, make([]mq.Delivery, 1))
	cancel()
	if mq.CodeOf(err) != mq.CodeForbidden {
		t.Fatalf("a Receive on the refused consumer while the standby holds the queue reported %v, want "+
			"CodeForbidden: it should ask the broker again, as a standby does", err)
	}
	if snap := active.Health(); snap.State != mq.HealthDegraded {
		t.Fatalf("the active client reports %v (%v) after its consumer was refused again, want degraded",
			snap.State, snap.Err)
	}

	// With the standby gone, the refused consumer's next Receive takes the
	// queue back.
	if err := consB.Close(); err != nil {
		t.Fatalf("closing the standby's consumer: %v", err)
	}
	restores = rq.RestoresForTest(b)
	if err := pubA.Publish(t.Context(), &mq.Message{Body: []byte("again")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, consA); string(d.Body) != "again" || d.Ack() != nil {
		t.Fatalf("the refused consumer received %q once the standby let go, want %q", d.Body, "again")
	}
	if got := rq.RestoresForTest(b); got != restores {
		t.Fatalf("the client reconnected %d times while its consumer took the queue back, want none: the "+
			"Receive asks the broker itself", got-restores)
	}
	if snap := active.Health(); snap.State != mq.HealthReady {
		t.Fatalf("the active client reports %v (%v) once its consumer is subscribed again, want ready",
			snap.State, snap.Err)
	}
}

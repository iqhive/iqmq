package rabbitmq_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestCloseReturnsUnderAResourceAlarm is Client.Close on a connection the
// broker has stopped reading, which is what RabbitMQ does to a publishing
// connection under a memory or disk alarm.
//
// Every step of the close was a round trip: the consumer's `basic.cancel`, each
// handle's `channel.close` and the `connection.close`. The broker answers none
// of them until the alarm clears, so Close waited for as long as the alarm
// lasted, which an operator may leave for hours. The fake's `Block` only sent
// the notification and went on reading, which is why nothing caught it;
// [rabbitfake.ConnHandle.Alarm] stops reading as the broker does.
//
// Nothing may be lost on the way out: the delivery the caller held and the
// publication the alarm caught are both in the queue afterwards.
func TestCloseReturnsUnderAResourceAlarm(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); string(d.Body) != "held" {
		t.Fatalf("received %q", d.Body)
	}

	conn := broker.LastConnection()
	conn.Alarm("low on disk")
	// The publication that meets the alarm is written and never confirmed.
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	perr := pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	t.Logf("the publication the alarm caught: %v", perr)
	waitFor(t, 5*time.Second, b.Blocked)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Close returned %v after %v", err, time.Since(start))
	case <-time.After(5 * time.Second):
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("Client.Close was still waiting 5s into the alarm, and returned %v only once it cleared: "+
			"it waited for the broker to answer basic.cancel, channel.close and connection.close, which it "+
			"does not while it is not reading the connection", err)
	}

	broker.Sync()
	queued := broker.Ready("orders.workers")
	ready := make([]string, 0, len(queued))
	for _, p := range queued {
		ready = append(ready, string(p.Body))
	}
	t.Logf("in the queue afterwards: %q", ready)
	for _, want := range []string{"held", "caught"} {
		if !containsString(ready, want) {
			t.Errorf("%q is not in the queue after Close: %q", want, ready)
		}
	}
}

// TestAConsumerClosedDuringAnAlarmLetsGoOfItsChannelWhenItClears is one
// handle closed while the client stays open, during an alarm.
//
// Its Close returns at once, and the channel's close goes on until the broker
// reads the connection again, which is when it would have read the close
// anyway: the delivery the consumer held goes back to the queue then, and no
// channel is left open on the connection.
func TestAConsumerClosedDuringAnAlarmLetsGoOfItsChannelWhenItClears(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)
	conn := broker.LastConnection()
	channels := len(conn.Channels())

	conn.Alarm("low on disk")
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 5*time.Second, b.Blocked)

	closed := make(chan error, 1)
	go func() { closed <- cons.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("closing the consumer during the alarm: %v", err)
		}
	case <-time.After(5 * time.Second):
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("the consumer's Close was still waiting 5s into the alarm, and returned %v once it cleared", err)
	}
	if got := len(conn.Channels()); got != channels {
		t.Fatalf("%d channels are open during the alarm after the consumer closed, want the %d there were: "+
			"its channel is kept until the broker reads again", got, channels)
	}

	conn.ClearAlarm()
	waitFor(t, 5*time.Second, func() bool { return len(conn.Channels()) == channels-1 })
	broker.Sync()
	queued := broker.Ready("orders.workers")
	ready := make([]string, 0, len(queued))
	for _, p := range queued {
		ready = append(ready, string(p.Body))
	}
	if !containsString(ready, "held") || !containsString(ready, "caught") {
		t.Errorf("after the alarm the queue holds %q, want the delivery the consumer held and the publication "+
			"the alarm caught", ready)
	}
}

// TestAConsumerTheBrokerCancelledDuringAnAlarmClosesAtOnce is a consumer whose
// queue the broker cancelled (an operator deleted it) while the connection was
// blocked, and then the consumer's Close.
//
// The consumer closes the cancelled channel itself, as soon as it reads the
// cancel, because nothing else holds that channel. `channel.close` is a round
// trip the broker answers only once it reads the connection again, and the
// consumer's Close waited for that close, so it lasted as long as the alarm,
// as Client.Close did behind it. A consumer closed during an alarm otherwise
// returns at once (TestAConsumerClosedDuringAnAlarmLetsGoOfItsChannelWhenItClears).
// The channel's close now goes on in the background, and the channel goes once
// the broker reads again.
func TestAConsumerTheBrokerCancelledDuringAnAlarmClosesAtOnce(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)
	conn := broker.LastConnection()
	channels := len(conn.Channels())

	conn.Alarm("low on disk")
	t.Cleanup(conn.ClearAlarm)
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 5*time.Second, b.Blocked)

	ch := consumerChannel(t, broker)
	tags := ch.Consumers()
	if len(tags) != 1 || !ch.CancelConsumer(tags[0]) {
		t.Fatalf("cancelling the consumer at the broker: tags %v", tags)
	}
	rctx, rcancel := context.WithTimeout(t.Context(), 5*time.Second)
	_, rerr := cons.Receive(rctx, make([]mq.Delivery, 1))
	rcancel()
	if mq.CodeOf(rerr) != mq.CodeNotFound {
		t.Fatalf("Receive after the broker's cancel: %v, want NotFound: the cancel has not reached the consumer", rerr)
	}

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cons.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("closing the consumer the broker cancelled, during the alarm: %v", err)
		}
	case <-time.After(3 * time.Second):
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("the consumer the broker cancelled was still closing 3s into the alarm, and its Close returned "+
			"%v only once the alarm cleared, %v after it began", err, time.Since(start))
	}

	conn.ClearAlarm()
	waitFor(t, 5*time.Second, func() bool { return len(conn.Channels()) == channels-1 })
}

// TestCloseReturnsUnderAFlappingAlarm is Client.Close in a second alarm that
// began before the broker read what the client wrote when the first cleared.
//
// The channels of handles closed during an alarm used to be closed by the
// supervision loop when `connection.unblocked` arrived, which is a round trip.
// An alarm that clears and comes straight back, which is how a memory alarm
// usually clears, parked the loop in that round trip: it could not hear the
// second `connection.blocked`, so the driver thought the broker was reading,
// and Client.Close waited for the whole second alarm. On RabbitMQ the
// library then abandoned the second `connection.unblocked` after five
// seconds and the driver stayed blocked for good.
//
// The OnReconnect hook, which hears `connection.unblocked`, is only the
// deterministic way to raise the second alarm inside that window.
func TestCloseReturnsUnderAFlappingAlarm(t *testing.T) {
	for _, flap := range []bool{false, true} {
		name := "control"
		if flap {
			name = "flap"
		}
		t.Run(name, func(t *testing.T) { closeUnderAFlappingAlarm(t, flap) })
	}
}

func closeUnderAFlappingAlarm(t *testing.T, flap bool) {
	broker := rabbitfake.New(rabbitfake.Config{NotifyTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = broker.Close() })
	var (
		armed  atomic.Bool
		connP  atomic.Pointer[rabbitfake.ConnHandle]
		pubP   atomic.Pointer[mq.PublisherHandle]
		hooked = make(chan struct{})
		again  sync.WaitGroup
	)
	t.Cleanup(again.Wait)
	cfg := base()
	cfg.Declare = true
	cfg.Hooks.OnReconnect = func(_ context.Context, ev mq.ReconnectEvent) {
		if ev.Attempt != 0 || ev.Err != nil || !armed.CompareAndSwap(true, false) {
			return
		}
		conn := connP.Load()
		for !conn.Reading() {
			time.Sleep(time.Millisecond)
		}
		conn.Alarm("again")
		again.Go(func() {
			pctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			_ = pubP.Load().Publish(pctx, &mq.Message{Body: []byte("caught2")})
		})
		for conn.Reading() {
			time.Sleep(time.Millisecond)
		}
		close(hooked)
	}
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	b := backendOf(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	pubP.Store(pub)
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)
	conn := broker.LastConnection()
	connP.Store(conn)

	conn.Alarm("low on disk")
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 5*time.Second, b.Blocked)
	consClosed := make(chan error, 1)
	go func() { consClosed <- cons.Close() }()
	select {
	case err := <-consClosed:
		if err != nil {
			t.Fatalf("closing the consumer during the first alarm: %v", err)
		}
	case <-time.After(3 * time.Second):
		conn.ClearAlarm()
		err := <-consClosed
		t.Fatalf("closing the consumer waited 3s into the first alarm, and returned %v once it cleared", err)
	}
	if flap {
		armed.Store(true)
	}
	conn.ClearAlarm()
	if flap {
		select {
		case <-hooked:
		case <-time.After(5 * time.Second):
			conn.ClearAlarm()
			t.Fatal("the hook never heard connection.unblocked")
		}
		// Not waited for: the driver failing to hear the second alarm is
		// the defect, and Close below is what shows it.
		time.Sleep(200 * time.Millisecond)
		t.Logf("second alarm raised: broker reading %v, driver blocked %v", conn.Reading(), b.Blocked())
	} else {
		waitFor(t, 5*time.Second, func() bool { return !b.Blocked() })
		conn.Alarm("again")
		pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught2")})
		pcancel()
		waitFor(t, 5*time.Second, b.Blocked)
	}

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Close returned %v after %v", err, time.Since(start))
	case <-time.After(3 * time.Second):
		blocked := b.Blocked()
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("Client.Close was still waiting 3s into the second alarm (driver blocked: %v), and returned %v "+
			"only once it cleared, %v after it began", blocked, err, time.Since(start))
	}
}

// TestADroppedUnblockedDoesNotLeaveTheConnectionBlocked is the library
// abandoning a `connection.unblocked`, which amqp091 does to a notification
// its listener has not taken within five seconds.
//
// The driver then went on reading the connection as blocked with no alarm on
// the broker: Health stayed Degraded, and every consumer closed from then on
// kept its channel, and the messages it held, until the next alarm. The round
// trip that closes such a consumer's channel now proves the broker is reading,
// and the driver says so.
//
// The supervision loop is held in the OnReconnect hook while the broker sends
// a second `connection.blocked`, which fills the loop's listener, and its
// `connection.unblocked`, which the fake then abandons as the library does.
func TestADroppedUnblockedDoesNotLeaveTheConnectionBlocked(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{NotifyTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { _ = broker.Close() })
	var (
		hold     atomic.Bool
		held     = make(chan struct{}, 1)
		released = make(chan struct{})
	)
	cfg := base()
	cfg.Declare = true
	cfg.Hooks.OnReconnect = func(_ context.Context, ev mq.ReconnectEvent) {
		if ev.Attempt == 0 && ev.Err == nil && hold.CompareAndSwap(true, false) {
			held <- struct{}{}
			<-released
		}
	}
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	cons, err := cl.Consumer(t.Context(), mq.ConsumerConfig{Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)

	conn := broker.LastConnection()
	blockConnection(t, conn, b, "low on memory")
	hold.Store(true)
	conn.Unblock()
	<-held
	conn.Block("low on memory")
	conn.Unblock() // abandoned: the loop's one slot holds the second blocked
	close(released)
	waitFor(t, 5*time.Second, b.Blocked)
	time.Sleep(50 * time.Millisecond)
	if !b.Blocked() {
		t.Fatal("the driver heard the abandoned connection.unblocked, so this test proves nothing")
	}

	if err := cons.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return !b.Blocked() })
	waitFor(t, 5*time.Second, func() bool {
		for _, p := range broker.Ready("orders.workers") {
			if string(p.Body) == "held" {
				return true
			}
		}
		return false
	})
	if got := cl.Health().State; got != mq.HealthReady {
		t.Errorf("health is %v after the broker was seen reading again, want ready", got)
	}
}

// TestAClosedChannelIsNoProofTheBrokerReads is a channel the broker has
// already closed reaching the close of a handle during an alarm.
//
// A close the broker answers proves it reads the connection again, which is
// how an abandoned `connection.unblocked` is made up for. The library answers
// the close of a channel that has already closed at once, with no round trip,
// and the reply endpoint closes the very channel the broker has just closed
// (as it does a channel a request to a deleted exchange took). That nil was
// read as the proof: the driver marked the connection Ready while the broker
// was not reading, and a consumer closed then waited for the alarm.
func TestAClosedChannelIsNoProofTheBrokerReads(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{NotifyTimeout: 100 * time.Millisecond})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	cfg.Declare = true
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	respondOn(t, cl, "rpc", "workers", func(d mq.Delivery) *mq.Message { return &mq.Message{Body: d.Body} })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	_, err = b.RequestNative(ctx, "rpc", &mq.Message{Body: []byte("ping")})
	cancel()
	if err != nil {
		t.Fatalf("the first request: %v", err)
	}
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)

	conn := broker.LastConnection()
	var reply *rabbitfake.ChannelHandle
	for _, ch := range conn.Channels() {
		for _, c := range ch.ConsumerStates() {
			if strings.HasPrefix(c.Queue, rq.ReplyAddress) {
				reply = ch
			}
		}
	}
	if reply == nil {
		t.Fatal("the reply endpoint has no channel on the connection")
	}

	conn.Alarm("low on disk")
	pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
	pcancel()
	waitFor(t, 5*time.Second, b.Blocked)

	// The broker closes the reply channel, as a request to an exchange
	// deleted after its topic's first request does.
	closedReply := make(chan struct{})
	go func() {
		defer close(closedReply)
		reply.CloseWith(&amqp.Error{Code: amqp.NotFound, Reason: "NOT_FOUND - no exchange 'gone' in vhost '/'", Server: true})
	}()
	<-closedReply
	time.Sleep(300 * time.Millisecond)
	stale := !b.Blocked()
	t.Logf("after the broker closed the reply channel: broker reading %v, driver blocked %v, health %v",
		conn.Reading(), b.Blocked(), cl.Health().State)

	consClosed := make(chan error, 1)
	start := time.Now()
	go func() { consClosed <- cons.Close() }()
	waited := false
	var cerr error
	select {
	case cerr = <-consClosed:
	case <-time.After(2 * time.Second):
		waited = true
	}
	conn.ClearAlarm()
	if waited {
		cerr = <-consClosed
	}
	if cerr != nil {
		t.Errorf("closing the consumer: %v", cerr)
	}
	if stale || waited {
		t.Fatalf("the driver read the connection as unblocked while the broker was not reading (%v), because the "+
			"close of a channel already closed returned nil; a consumer closed then waited %v for the alarm "+
			"(%v)", stale, time.Since(start), waited)
	}
}

// TestAnAlarmThatBeginsDuringAReconnectionIsHeard is a memory or disk alarm in
// effect when the driver reconnects.
//
// The dial loop re-attaches the publishers before the consumers, and a
// publisher with something to publish writes as soon as its channel is back,
// which is what makes the broker stop reading the new connection. Its
// `connection.blocked` reached no listener, because the supervision loop
// registered one only once the dial loop had returned. The dial loop's next
// round trip was then never answered: the consumer's channel open timed out,
// the attempt failed and the library's unbounded close of the connection
// waited for the alarm, or a round trip after the open did. Client.Close
// waited for the dial loop, and the handles' closes, which did not know the
// connection was blocked, waited for the broker, so Close lasted as long as
// the alarm. The driver now hears the alarm while it restores the client,
// Close returns during it, and the dial loop waits for the broker to read
// again rather than dial another connection the broker would block the same
// way, spending an attempt each Config.DialTimeout.
//
// An attempt that fails on the blocked connection for a reason of its own
// closed it with the library's unbounded close, which waited for the alarm,
// and the dial loop with it: it closes it with the connection's bound now and
// dials again.
func TestAnAlarmThatBeginsDuringAReconnectionIsHeard(t *testing.T) {
	for _, tc := range []struct {
		name         string
		afterTheOpen bool
		refuse       bool
		clear        bool
	}{
		{"Close, the alarm at a channel open", false, false, false},
		{"Close, the alarm after a channel open", true, false, false},
		{"the alarm clears", false, false, true},
		{"an attempt that fails on the blocked connection", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) { alarmDuringAReconnection(t, tc.afterTheOpen, tc.refuse, tc.clear) })
	}
}

func alarmDuringAReconnection(t *testing.T, afterTheOpen, refuse, clear bool) {
	var (
		connMu sync.Mutex
		conns  []*rabbitfake.ConnHandle
	)
	// The fake abandons a notification nobody takes, as the library does
	// after five seconds, so a driver that does not listen fails here rather
	// than hanging the fake's dispatcher.
	broker := rabbitfake.New(rabbitfake.Config{NotifyTimeout: time.Second, OnConnection: func(h *rabbitfake.ConnHandle) {
		connMu.Lock()
		conns = append(conns, h)
		n := len(conns)
		connMu.Unlock()
		if n == 2 {
			// In effect on the broker when the client comes back.
			h.Alarm("low on disk")
		}
	}})
	t.Cleanup(func() { _ = broker.Close() })
	second := func() *rabbitfake.ConnHandle {
		connMu.Lock()
		defer connMu.Unlock()
		return conns[1]
	}
	dialer := &openHookDialer{inner: broker.Dialer()}
	var (
		failedDials atomic.Int32
		// blockedAfterFailure counts the failed attempts after which the
		// driver still read a connection as blocked, with none installed.
		blockedAfterFailure atomic.Int32
		backend             atomic.Pointer[rq.Backend]
	)
	cfg := base()
	cfg.Declare = true
	cfg.DialTimeout = 300 * time.Millisecond
	cfg.Hooks.OnReconnect = func(_ context.Context, ev mq.ReconnectEvent) {
		if ev.Attempt > 0 && ev.Err != nil {
			failedDials.Add(1)
			if b := backend.Load(); b != nil && b.Blocked() {
				blockedAfterFailure.Add(1)
			}
		}
	}
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	b := backendOf(t, cl)
	backend.Store(b)
	cons := consumeOn(t, cl, "orders", "workers")
	pubs := []*mq.PublisherHandle{publishOn(t, cl, "orders"), publishOn(t, cl, "orders")}
	if err := pubs[0].Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatal(err)
	}
	receiveOne(t, cons)

	// The dial loop re-attaches the two publishers in no fixed order, and
	// whichever is back first publishes: the other is then still waiting
	// for its channel.
	var (
		triggered atomic.Bool
		waiting   atomic.Pointer[mq.PublisherHandle]
	)
	hook := func(conn int) error {
		if conn != 2 || triggered.Load() {
			return nil
		}
		for i, pub := range pubs {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_ = pub.Publish(ctx, &mq.Message{Body: []byte("during the reconnection")})
			cancel()
			if !second().Reading() {
				waiting.Store(pubs[1-i])
				triggered.Store(true)
				if !refuse {
					return nil
				}
				// The attempt fails once the driver has heard the alarm.
				for deadline := time.Now().Add(2 * time.Second); !b.Blocked() && time.Now().Before(deadline); {
					time.Sleep(time.Millisecond)
				}
				return errRefusedChannel
			}
		}
		return nil
	}
	after := func(conn int) { _ = hook(conn) }
	if afterTheOpen {
		dialer.after.Store(&after)
	} else {
		dialer.before.Store(&hook)
	}
	broker.Connection(0).CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, triggered.Load)
	c2 := second()
	t.Cleanup(c2.ClearAlarm)
	if refuse {
		for deadline := time.Now().Add(3 * time.Second); cl.Health().State != mq.HealthReady; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("3s after an attempt failed on the connection the broker had blocked, with a DialTimeout of "+
					"300ms, the client was not restored on another (health %v, %d connections): the dial loop was "+
					"waiting for the alarm to close the blocked one", cl.Health().State, len(broker.Connections()))
			}
		}
		if got := blockedAfterFailure.Load(); got != 0 {
			t.Errorf("after %d attempts that failed on the connection the broker had blocked, Blocked still read "+
				"true with no connection installed", got)
		}
	}
	for deadline := time.Now().Add(2 * time.Second); !refuse && !b.Blocked(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the broker blocked the connection the dial loop was restoring and the driver never heard it "+
				"(health %v)", cl.Health().State)
		}
	}
	// The dial loop has not re-attached the other publisher, and will once
	// the broker reads again, so its refusal is retryable. The alarm heard
	// while the loop restores the client must not read as the end of the
	// reconnection.
	pctx, pcancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	perr := waiting.Load().Publish(pctx, &mq.Message{Body: []byte("not yet")})
	pcancel()
	if !refuse && (perr == nil || !mq.IsRetryable(perr)) {
		t.Errorf("a publisher the dial loop had yet to re-attach, during the alarm, reported %v (retryable %v), "+
			"want a retryable refusal", perr, mq.IsRetryable(perr))
	}

	if clear {
		time.Sleep(4 * cfg.DialTimeout)
		if n, got := len(broker.Connections()), failedDials.Load(); n != 2 || got != 0 {
			t.Fatalf("four DialTimeouts into the alarm the driver had dialled %d connections and %d attempts had "+
				"failed: it gave up on the connection the broker blocked and dialled another, which the broker "+
				"blocks the same way", n, got)
		}
		c2.ClearAlarm()
		waitFor(t, 5*time.Second, func() bool { return !b.Blocked() && cl.Health().State == mq.HealthReady })
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		dst := make([]mq.Delivery, 4)
		for seen := false; !seen; {
			n, err := cons.Receive(ctx, dst)
			if err != nil {
				t.Fatalf("the consumer received nothing written during the reconnection once the alarm cleared: %v", err)
			}
			for i := range n {
				seen = seen || string(dst[i].Body) == "during the reconnection"
				_ = dst[i].Ack()
			}
		}
		if n := len(broker.Connections()); n != 2 {
			t.Errorf("the client was restored on connection %d, want the one the alarm was raised on", n)
		}
		return
	}

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cl.Close() }()
	select {
	case err := <-closed:
		t.Logf("Client.Close returned %v after %v", err, time.Since(start))
	case <-time.After(3 * time.Second):
		c2.ClearAlarm()
		err := <-closed
		t.Fatalf("Client.Close was still waiting 3s into the alarm, and returned %v only once it cleared, %v after "+
			"it began", err, time.Since(start))
	}
}

// TestAConsumerClosedBehindAFullSocketReturns is a consumer closed during an
// alarm while a publication fills the connection's socket.
//
// Under an alarm a publisher goes on writing until the socket's buffers are
// full, and the write that does not fit waits inside the socket holding the
// library's connection-wide write lock, so every frame on the connection, on
// every channel, waits behind it. A consumer's Close wrote two kinds: its
// `basic.cancel`, sent without waiting for the answer, and a requeueing
// `basic.nack` for each delivery it held unread, so the Close lasted as long
// as the alarm. While the connection is blocked a closing consumer writes
// nothing of its own: its channel's close, which goes on in the background,
// puts back everything the channel held once the broker reads it.
func TestAConsumerClosedBehindAFullSocketReturns(t *testing.T) {
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
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	for _, body := range []string{"held", "unread"} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	if d := receiveOne(t, cons); string(d.Body) != "held" {
		t.Fatalf("received %q", d.Body)
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
	big := make([]byte, 128<<10)
	writes.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = pub.Publish(ctx, &mq.Message{Body: big})
	})
	waitFor(t, 5*time.Second, conn.WriteBlocked)

	start := time.Now()
	closed := make(chan error, 1)
	go func() { closed <- cons.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("closing the consumer behind a full socket: %v", err)
		}
	case <-time.After(3 * time.Second):
		conn.ClearAlarm()
		err := <-closed
		t.Fatalf("the consumer's Close was still waiting 3s into the alarm behind a publication that filled the "+
			"socket, and returned %v only once it cleared, %v after it began", err, time.Since(start))
	}

	conn.ClearAlarm()
	writes.Wait()
	waitFor(t, 5*time.Second, func() bool {
		queued := broker.Ready("orders.workers")
		ready := make([]string, 0, len(queued))
		for _, p := range queued {
			ready = append(ready, string(p.Body))
		}
		return containsString(ready, "held") && containsString(ready, "unread")
	})
}

// containsString reports whether xs holds x.
func containsString(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

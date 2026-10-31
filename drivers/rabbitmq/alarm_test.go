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

// containsString reports whether xs holds x.
func containsString(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

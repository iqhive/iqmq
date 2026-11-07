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
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// TestARefusedDelayQueueDeclarationLeavesTheConsumersOtherDeliveries is a delay
// queue's declaration refused with 406, because another client of the
// subscription declared the queue with another Config.DelayQueueTTL.
//
// The declaration was made on the consumer's own channel, and a refusal
// closes the channel it is made on, so the broker took back every delivery
// the consumer held: the nack reported LeaseLost, and an Ack of another
// delivery after it did too. The declarations are made on a channel of the
// client's own now, so the refusal is the nack's alone: a definite Conflict
// with the broker's reason, the delivery still the caller's, and the next
// declaration made on a new channel.
func TestARefusedDelayQueueDeclarationLeavesTheConsumersOtherDeliveries(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	open := func(queueTTL time.Duration) *mq.Client {
		t.Helper()
		cfg := base()
		expiring(queueTTL)(&cfg)
		c, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
		if err != nil {
			t.Fatalf("opening a client over the fake broker: %v", err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	// Another client declares the 3.2-second bucket with its own lease.
	other := open(20 * time.Minute)
	otherCons := consumeOn(t, other, "orders", "workers")
	if err := publishOn(t, other, "orders").Publish(t.Context(), &mq.Message{Body: []byte("other")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, otherCons); d.Nack(mq.NackDelay(3*time.Second)) != nil {
		t.Fatal("the other client's delayed nack failed")
	}
	if err := otherCons.Close(); err != nil {
		t.Fatal(err)
	}

	cl := open(10 * time.Minute)
	cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 8 })
	pub := publishOn(t, cl, "orders")
	for _, body := range []string{"X", "Y"} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	x, y := receiveOne(t, cons), receiveOne(t, cons)
	consuming := consumerChannel(t, broker)

	nerr := x.Nack(mq.NackDelay(3 * time.Second))
	code, _ := rq.ReplyCode(nerr)
	t.Logf("X's delayed nack: %v (reply code %d, portable code %v)", nerr, code, mq.CodeOf(nerr))
	if code != amqp.PreconditionFailed || mq.CodeOf(nerr) != mq.CodeConflict || mq.IsAmbiguous(nerr) {
		t.Fatalf("X's delayed nack reported %v, want the broker's 406 as a definite Conflict", nerr)
	}
	if consuming.IsClosed() {
		t.Fatalf("the refused declaration closed the consumer's channel (%v), and with it every delivery it held",
			consuming.Cause())
	}
	if err := y.Ack(); err != nil {
		t.Fatalf("Y's Ack after X's refused delayed nack: %v", err)
	}
	// X is still the caller's. A delay in another bucket is declared on a new
	// channel, because the refusal closed the one the declarations are made on.
	if err := x.Nack(mq.NackDelay(time.Second)); err != nil {
		t.Fatalf("a delayed nack of X after the refusal: %v", err)
	}
	broker.Sync()
	broker.Advance(2 * time.Second)
	broker.Sync()
	if back := receiveOne(t, cons); string(back.Body) != "X" {
		t.Fatalf("received %q after the delay, want X", back.Body)
	} else if err := back.Ack(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	dst := make([]mq.Delivery, 4)
	if n, _ := cons.Receive(ctx, dst); n != 0 {
		t.Fatalf("the consumer received %q again after Y was acknowledged", dst[0].Body)
	}
}

// TestADelayedNackDuringAnAlarmIsRefusedAtOnce is a delayed nack while the
// broker has blocked the connection under a memory or disk alarm.
//
// A delay queue's declaration is a round trip, which the broker does not
// answer until the alarm clears, and the client library's `queue.declare` has
// no deadline, so the nack waited for as long as the alarm lasted. A park's
// confirm does not come either, so on the plugin arm the nack waited for
// Config.PublishTimeout and reported an unknown outcome, and the broker stored
// the parked copy and requeued the original once it read the connection
// again: a duplicate. While the driver knows the connection is blocked, a
// delayed nack writes nothing and is refused with Backpressure, and the
// delivery stays the caller's.
func TestADelayedNackDuringAnAlarmIsRefusedAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name   string
		broker rabbitfake.Config
		shape  func(*rq.Config)
	}{
		{"a delay queue with x-expires", rabbitfake.Config{}, expiring(10 * time.Minute)},
		{"the delayed-message exchange", rabbitfake.Config{DelayedMessagePlugin: true}, func(c *rq.Config) {
			c.Declare, c.DetectDelayPlugin = true, true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker, cl := openFakeWith(t, tc.broker, tc.shape)
			b := backendOf(t, cl)
			cons := consumeOn(t, cl, "orders", "workers")
			pub := publishOn(t, cl, "orders")
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
				t.Fatal(err)
			}
			d := receiveOne(t, cons)

			conn := broker.LastConnection()
			conn.Alarm("low on disk")
			pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
			pcancel()
			waitFor(t, 5*time.Second, b.Blocked)

			nacked := make(chan error, 1)
			go func() { nacked <- d.Nack(mq.NackDelay(3 * time.Second)) }()
			var err error
			select {
			case err = <-nacked:
			case <-time.After(3 * time.Second):
				conn.ClearAlarm()
				err = <-nacked
				t.Fatalf("the delayed nack was still waiting 3s into the alarm, and returned %v once it cleared", err)
			}
			if mq.CodeOf(err) != mq.CodeBackpressure || !mq.IsRetryable(err) || mq.IsAmbiguous(err) {
				t.Fatalf("the delayed nack during the alarm reported %v, want a definite, retryable Backpressure", err)
			}

			conn.ClearAlarm()
			waitFor(t, 5*time.Second, func() bool { return !b.Blocked() })
			if err := d.Nack(mq.NackDelay(3 * time.Second)); err != nil {
				t.Fatalf("the delayed nack once the alarm cleared: %v", err)
			}
			broker.Sync()
			broker.Advance(4 * time.Second)
			broker.Sync()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var got []string
			dst := make([]mq.Delivery, 4)
			for !containsString(got, "held") {
				n, err := cons.Receive(ctx, dst)
				if err != nil {
					t.Fatalf("the parked copy of %q never came back (%v); received %q", "held", err, got)
				}
				for i := range n {
					got = append(got, string(dst[i].Body))
					_ = dst[i].Ack()
				}
			}
			if count := countOf(got, "held"); count != 1 {
				t.Fatalf("received %q: %q came back %d times, want once", got, "held", count)
			}
		})
	}
}

// TestADelayedNackWaitingBehindADeclarationIsRefusedOnceTheBrokerBlocks is two
// consumers' delayed nacks, the first of which the broker has not answered.
//
// The delay queues are declared one at a time, on one channel per client, so
// the second nack waits for the first's declaration. A declaration written
// in the moment before the broker stopped reading the connection waits for the alarm,
// because the broker's `connection.blocked` arrives after it has stopped, and
// the second nack must not wait with it once the driver knows: it is refused
// with Backpressure as soon as the connection is blocked.
func TestADelayedNackWaitingBehindADeclarationIsRefusedOnceTheBrokerBlocks(t *testing.T) {
	broker, cl := openFake(t, expiring(10*time.Minute))
	b := backendOf(t, cl)
	workers := consumeOn(t, cl, "orders", "workers")
	auditors := consumeOn(t, cl, "orders", "auditors")
	if err := publishOn(t, cl, "orders").Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	first, second := receiveOne(t, workers), receiveOne(t, auditors)

	gate := broker.HoldDeclarations()
	t.Cleanup(gate.Release)
	held := make(chan error, 1)
	go func() { held <- first.Nack(mq.NackDelay(3 * time.Second)) }()
	waitFor(t, 5*time.Second, func() bool { return gate.Parked() == 1 })
	queued := make(chan error, 1)
	go func() { queued <- second.Nack(mq.NackDelay(3 * time.Second)) }()
	time.Sleep(200 * time.Millisecond)
	if n := gate.Parked(); n != 1 {
		t.Errorf("%d declarations are waiting for the broker, want 1: the client declares its delay queues "+
			"one at a time, on one channel", n)
	}

	blockConnection(t, broker.LastConnection(), b, "low on disk")
	var err error
	select {
	case err = <-queued:
	case <-time.After(3 * time.Second):
		gate.Release()
		err = <-queued
		t.Fatalf("the second delayed nack was still waiting 3s after the broker blocked the connection, and "+
			"returned %v once the first declaration was answered", err)
	}
	if mq.CodeOf(err) != mq.CodeBackpressure || mq.IsAmbiguous(err) {
		t.Fatalf("the second delayed nack reported %v, want a definite Backpressure", err)
	}
	gate.Release()
	if err := <-held; err != nil {
		t.Fatalf("the first delayed nack, once its declaration was answered: %v", err)
	}
}

// TestADelayedNackAfterAReconnectionDeclaresOnTheNewConnection is the channel
// the delay queues are declared on, after the connection it was opened on was
// lost: the next delayed nack opens one on the new connection.
func TestADelayedNackAfterAReconnectionDeclaresOnTheNewConnection(t *testing.T) {
	broker, cl := openFake(t, func(c *rq.Config) {
		expiring(10 * time.Minute)(c)
		c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
	})
	cons := consumeOn(t, cl, "orders", "workers")
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if d := receiveOne(t, cons); d.Nack(mq.NackDelay(3*time.Second)) != nil {
		t.Fatal("the delayed nack before the reconnection failed")
	}
	lost := broker.LastConnection()
	lost.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
	waitFor(t, 10*time.Second, func() bool { return broker.LastConnection().ID() != lost.ID() })

	// The publisher is attached again once the dial loop gets to it.
	for deadline := time.Now().Add(10 * time.Second); ; {
		err := pub.Publish(t.Context(), &mq.Message{Body: []byte("second")})
		if err == nil {
			break
		}
		if !mq.IsRetryable(err) || time.Now().After(deadline) {
			t.Fatalf("publishing after the reconnection: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	var second mq.Delivery
	for second.Body == nil {
		d := receiveOne(t, cons)
		if string(d.Body) != "second" {
			_ = d.Ack()
			continue
		}
		second = d
	}
	if err := second.Nack(mq.NackDelay(time.Second)); err != nil {
		t.Fatalf("a delayed nack after the reconnection: %v", err)
	}
	broker.Sync()
	if got := broker.Ready("orders.workers.delay.1600"); len(got) != 1 {
		t.Fatalf("the delay queue holds %d message(s) after the delayed nack, want 1", len(got))
	}
}

// TestADelayedNackOfADeliveryAlreadyGoneIsNotRetryable is a delayed nack of a
// delivery that is no longer the caller's, answered before the alarm and the
// declaration had their say.
//
// A delayed nack checks the alarm and declares its delay queue before its
// settlement starts, and those answers were given as though the delivery were
// still the caller's: Backpressure or Unavailable, retryable, when a retry
// could only meet ErrLeaseLost. v1.1.1 answered a declaration whose connection
// went with ErrLeaseLost.
func TestADelayedNackOfADeliveryAlreadyGoneIsNotRetryable(t *testing.T) {
	t.Run("its connection lost during the declaration", func(t *testing.T) {
		broker, cl := openFake(t, func(c *rq.Config) {
			expiring(10 * time.Minute)(c)
			c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
		})
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
		broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		var err error
		select {
		case err = <-nacked:
		case <-time.After(5 * time.Second):
			gate.Release()
			t.Fatalf("the delayed nack waited 5s after its connection died, and returned %v once released", <-nacked)
		}
		if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
			t.Fatalf("the delayed nack whose connection died during the declaration reported %v (retryable %v), "+
				"want ErrLeaseLost: the delivery went with the connection", err, mq.IsRetryable(err))
		}
	})
	// The nack queued for the declaration turn behind one the broker has not
	// answered, when the connection goes. The first nack's declaration fails
	// with the connection and gives the turn up, and the dial loop's first
	// redial follows at once, whatever Config.ReconnectBase says. A queued
	// nack that took the turn before the replacement was in place found no
	// connection and was answered Unavailable and retryable, "the connection
	// is being re-established", although its delivery went with the
	// connection. One that took it after declared its delay queue on the
	// replacement, where this gate holds every declaration, and returned only
	// once the test released the gate, about one run in 150 under the cover
	// build at GOMAXPROCS=2. It now gives up the wait as soon as the client
	// hears its connection go, and looks again once it has the turn, so the
	// connection it would meet does not matter.
	t.Run("waiting for the declaration turn when its connection is lost", func(t *testing.T) {
		broker, cl := openFake(t, expiring(10*time.Minute))
		cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 4 })
		pub := publishOn(t, cl, "orders")
		for _, body := range []string{"a", "b"} {
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
				t.Fatal(err)
			}
		}
		first, second := receiveOne(t, cons), receiveOne(t, cons)
		gate := broker.HoldDeclarations()
		t.Cleanup(gate.Release)
		held := make(chan error, 1)
		go func() { held <- first.Nack(mq.NackDelay(3 * time.Second)) }()
		waitFor(t, 5*time.Second, func() bool { return gate.Parked() == 1 })
		queued := make(chan error, 1)
		go func() { queued <- second.Nack(mq.NackDelay(3 * time.Second)) }()
		time.Sleep(100 * time.Millisecond)
		if n := gate.Parked(); n != 1 {
			t.Fatalf("%d declarations are waiting for the broker, want 1: the second nack is not queued behind "+
				"the first", n)
		}

		broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		for i, ch := range []chan error{held, queued} {
			var err error
			select {
			case err = <-ch:
			case <-time.After(5 * time.Second):
				gate.Release()
				t.Fatalf("delayed nack %d had not returned 5s after its connection died, and returned %v once "+
					"released", i+1, <-ch)
			}
			if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
				t.Fatalf("delayed nack %d, whose connection died while it waited for its declaration, reported %v "+
					"(code %v, retryable %v), want ErrLeaseLost: the delivery went with the connection", i+1, err,
					mq.CodeOf(err), mq.IsRetryable(err))
			}
		}
	})
	// The same nack when the replacement connection is in place before the
	// first nack gives up the turn: the queued nack took the turn after it,
	// declared its delay queue on the new connection and reported ErrLeaseLost
	// only once the broker answered, and it waited behind the first nack's
	// declaration until then. It gives up as soon as the client hears its
	// connection go, while the first still holds the turn.
	t.Run("waiting for the declaration turn while its connection is replaced", func(t *testing.T) {
		ht := holdTheDeclarationTurn(t)
		gate := ht.broker.HoldDeclarations() // nothing is answered on the new connection
		t.Cleanup(gate.Release)
		lost := ht.broker.LastConnection()
		lost.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		waitFor(t, 5*time.Second, func() bool {
			return ht.broker.LastConnection().ID() != lost.ID() && gate.Parked() >= 1
		})
		select {
		case err := <-ht.queued:
			if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
				t.Fatalf("delayed nack 2, whose delivery went with connection 1, reported %v (code %v, retryable "+
					"%v), want ErrLeaseLost", err, mq.CodeOf(err), mq.IsRetryable(err))
			}
		case <-time.After(2 * time.Second):
			ht.letGo()
			gate.Release()
			t.Fatalf("delayed nack 2, whose delivery went with connection 1, was still waiting for the declaration "+
				"turn 2s after connection 2 was in place, and returned %v once the first nack gave the turn up and "+
				"the broker answered", <-ht.queued)
		}
		replay := gate.Parked()
		ht.letGo()
		select {
		case err := <-ht.held:
			if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
				t.Fatalf("delayed nack 1 reported %v (code %v, retryable %v), want ErrLeaseLost", err, mq.CodeOf(err),
					mq.IsRetryable(err))
			}
		case <-time.After(5 * time.Second):
			gate.Release()
			t.Fatalf("delayed nack 1 had not returned 5s after it was let go, and returned %v once released", <-ht.held)
		}
		if n := gate.Parked(); n != replay {
			t.Fatalf("%d declarations wait for the broker on connection 2 after the nacks, %d before: a nack whose "+
				"delivery had gone declared its delay queue there", n, replay)
		}
	})
	// The nack queued for the turn when the broker closes its consumer's
	// channel, the connection staying up: it waited for the first nack's
	// declaration, which the broker had not answered, before it reported
	// ErrLeaseLost.
	t.Run("waiting for the declaration turn when the broker closes its channel", func(t *testing.T) {
		broker, cl := openFake(t, expiring(10*time.Minute))
		cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 4 })
		pub := publishOn(t, cl, "orders")
		for _, body := range []string{"a", "b"} {
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
				t.Fatal(err)
			}
		}
		first, second := receiveOne(t, cons), receiveOne(t, cons)
		gate := broker.HoldDeclarations()
		t.Cleanup(gate.Release)
		held := make(chan error, 1)
		go func() { held <- first.Nack(mq.NackDelay(3 * time.Second)) }()
		waitFor(t, 5*time.Second, func() bool { return gate.Parked() == 1 })
		queued := make(chan error, 1)
		go func() { queued <- second.Nack(mq.NackDelay(3 * time.Second)) }()
		time.Sleep(100 * time.Millisecond)
		if n := gate.Parked(); n != 1 {
			t.Fatalf("%d declarations are waiting for the broker, want 1: the second nack is not queued behind "+
				"the first", n)
		}
		consumerChannel(t, broker).CloseWith(&amqp.Error{
			Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - unknown delivery tag 99", Server: true,
		})
		select {
		case err := <-queued:
			if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
				t.Fatalf("delayed nack 2, whose channel the broker closed, reported %v (code %v, retryable %v), "+
					"want ErrLeaseLost", err, mq.CodeOf(err), mq.IsRetryable(err))
			}
		case <-time.After(2 * time.Second):
			gate.Release()
			t.Fatalf("delayed nack 2, whose channel the broker closed, was still waiting 2s later for the first "+
				"nack's declaration, and returned %v once the broker answered it", <-queued)
		}
		gate.Release()
		if err := <-held; !errors.Is(err, mq.ErrLeaseLost) {
			t.Fatalf("delayed nack 1, whose channel the broker closed during its declaration, reported %v, want "+
				"ErrLeaseLost", err)
		}
	})
	// The nack queued for the turn when its connection is replaced and the
	// broker blocks the replacement before the first nack gives the turn up.
	// The wait gave up on the alarm and the nack reported Backpressure,
	// retryable, although its delivery had gone with connection 1 and a retry
	// could only meet ErrLeaseLost.
	t.Run("waiting for the declaration turn while its connection is replaced and the replacement blocked", func(t *testing.T) {
		ht := holdTheDeclarationTurn(t)
		lost := ht.broker.LastConnection()
		lost.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		waitFor(t, 5*time.Second, func() bool { return ht.broker.LastConnection().ID() != lost.ID() })
		replacement := ht.broker.LastConnection()
		blockConnection(t, replacement, ht.b, "low on memory")
		t.Cleanup(replacement.Unblock)
		var err error
		select {
		case err = <-ht.queued:
		case <-time.After(5 * time.Second):
			ht.letGo()
			t.Fatalf("delayed nack 2 was still waiting for the declaration turn 5s into the alarm, and returned %v "+
				"once the first nack gave the turn up", <-ht.queued)
		}
		ht.letGo()
		if first := <-ht.held; !errors.Is(first, mq.ErrLeaseLost) {
			t.Fatalf("delayed nack 1, whose connection was lost while it held the turn, reported %v, want "+
				"ErrLeaseLost", first)
		}
		if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
			t.Fatalf("delayed nack 2, whose delivery went with connection 1 while it waited for the turn, reported "+
				"%v (code %v, retryable %v) once connection 2 was blocked, want ErrLeaseLost", err, mq.CodeOf(err),
				mq.IsRetryable(err))
		}
	})
	t.Run("from before a reconnection, during an alarm", func(t *testing.T) {
		broker, cl := openFake(t, func(c *rq.Config) {
			expiring(10 * time.Minute)(c)
			c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
		})
		b := backendOf(t, cl)
		cons := consumeOn(t, cl, "orders", "workers")
		pub := publishOn(t, cl, "orders")
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		d := receiveOne(t, cons)
		lost := broker.LastConnection()
		lost.CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "restart", Server: true})
		waitFor(t, 10*time.Second, func() bool { return broker.LastConnection().ID() != lost.ID() })
		blockConnection(t, broker.LastConnection(), b, "low on disk")
		err := d.Nack(mq.NackDelay(3 * time.Second))
		if !errors.Is(err, mq.ErrLeaseLost) || mq.IsRetryable(err) {
			t.Fatalf("the delayed nack of a delivery from before the reconnection reported %v (retryable %v), "+
				"want ErrLeaseLost", err, mq.IsRetryable(err))
		}
	})
	t.Run("after BeginClose closed the blocked connection", func(t *testing.T) {
		broker, cl := openFake(t, expiring(10*time.Minute))
		b := backendOf(t, cl)
		cons := consumeOn(t, cl, "orders", "workers")
		pub := publishOn(t, cl, "orders")
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		d := receiveOne(t, cons)
		conn := broker.LastConnection()
		conn.Alarm("low on disk")
		t.Cleanup(conn.ClearAlarm)
		pctx, pcancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		_ = pub.Publish(pctx, &mq.Message{Body: []byte("caught")})
		pcancel()
		waitFor(t, 5*time.Second, b.Blocked)
		b.BeginClose()
		err := d.Nack(mq.NackDelay(3 * time.Second))
		if !errors.Is(err, mq.ErrClosed) || mq.IsRetryable(err) {
			t.Fatalf("the delayed nack after BeginClose reported %v (retryable %v), want ErrClosed: the client "+
				"is closing and nothing reconnects", err, mq.IsRetryable(err))
		}
	})
}

// heldTurn is a client over a fake broker whose first delayed nack holds the
// declaration turn and whose second waits for it, each nack's result to come
// on its channel.
type heldTurn struct {
	broker       *rabbitfake.Broker
	b            *rq.Backend
	held, queued chan error
	letGo        func()
}

// holdTheDeclarationTurn opens a client over a fake broker with
// Config.DelayQueueTTL set and nacks two deliveries with a delay. The first
// nack holds the declaration turn: the open of its declaration channel on
// connection 1 waits until letGo, so it keeps the turn across a loss of that
// connection while the dial loop puts in the next. The second waits for the
// turn.
//
// The hook holds the next channel opened on connection 1 once it is armed,
// which is the declaration channel because the consumer's and the
// publisher's are open by then and the client opens nothing else on its own.
// Should another channel come first, the first nack would not wait, and the
// helper fails rather than hand a test a turn nobody holds.
func holdTheDeclarationTurn(t *testing.T) heldTurn {
	t.Helper()
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := &openHookDialer{inner: broker.Dialer()}
	cfg := base()
	expiring(10 * time.Minute)(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	cons := consumeOnWith(t, cl, "orders", "workers", func(c *mq.ConsumerConfig) { c.Prefetch = 4 })
	pub := publishOn(t, cl, "orders")
	for _, body := range []string{"a", "b"} {
		if err := pub.Publish(t.Context(), &mq.Message{Body: []byte(body)}); err != nil {
			t.Fatal(err)
		}
	}
	first, second := receiveOne(t, cons), receiveOne(t, cons)

	holding, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	letGo := func() { once.Do(func() { close(release) }) }
	t.Cleanup(letGo)
	var armed atomic.Bool
	armed.Store(true)
	after := func(conn int) {
		if conn == 1 && armed.CompareAndSwap(true, false) {
			close(holding)
			<-release
		}
	}
	dialer.after.Store(&after)
	ht := heldTurn{
		broker: broker, b: backendOf(t, cl), held: make(chan error, 1), queued: make(chan error, 1), letGo: letGo,
	}
	go func() { ht.held <- first.Nack(mq.NackDelay(3 * time.Second)) }()
	select {
	case <-holding:
	case <-time.After(5 * time.Second):
		t.Fatal("the first delayed nack never opened its declaration channel")
	}
	go func() { ht.queued <- second.Nack(mq.NackDelay(3 * time.Second)) }()
	time.Sleep(100 * time.Millisecond) // the second nack is waiting for the turn
	for i, ch := range []chan error{ht.held, ht.queued} {
		select {
		case err := <-ch:
			t.Fatalf("delayed nack %d returned %v while the first should hold the declaration turn: the open "+
				"the hook held was not the first nack's declaration channel", i+1, err)
		default:
		}
	}
	return ht
}

// countOf is how many times x is in xs.
func countOf(xs []string, x string) int {
	n := 0
	for _, s := range xs {
		if s == x {
			n++
		}
	}
	return n
}

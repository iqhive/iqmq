package rabbitmq_test

import (
	"context"
	"errors"
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
	// answered, when the connection goes: it gets the turn as the first fails,
	// about when the supervision loop takes the dead connection out, and finds
	// none. It was answered Unavailable and retryable, "the connection is being
	// re-established", although its delivery went with the connection.
	t.Run("waiting for the declaration turn when its connection is lost", func(t *testing.T) {
		broker, cl := openFake(t, func(c *rq.Config) {
			expiring(10 * time.Minute)(c)
			// A slow redial keeps the client without a connection for a
			// while after the loss, as the default backoff does.
			c.ReconnectBase, c.ReconnectMax = 2*time.Second, 2*time.Second
		})
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

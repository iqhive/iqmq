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
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
)

// closeBounded closes c in the background and waits a bounded time for it.
//
// These cases are about the sink's lock, and a sink that deadlocked on it
// would also hang its own Close: a cleanup that waited for ever would turn the
// failure the case reports into a package timeout that reports nothing.
func closeBounded(t *testing.T, c interface{ Close() error }) {
	t.Helper()
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() { _ = c.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
}

// TestAPublishTheLibraryRefusesOnAnOpenChannelDoesNotDeadlock holds the open
// channel branch of `publishFailedLocked`: the library refused the write -- a
// context cancelled between the driver's own check and the write is the
// ordinary way -- and the channel is still up, so the tag is handed back and
// the pending value recycled. Recycling took the sink's lock, which `admit`
// already held, and the publisher hung for ever.
func TestAPublishTheLibraryRefusesOnAnOpenChannelDoesNotDeadlock(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	closeBounded(t, pub)

	publisherChannel(t, broker).FailNextPublishes(1, context.Canceled)
	published := make(chan error, 1)
	go func() { published <- pub.Publish(t.Context(), &mq.Message{Body: []byte("refused")}) }()
	select {
	case err := <-published:
		if err == nil {
			t.Fatal("a publication the library refused reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a publication the library refused on an open channel did not return: the sink deadlocked on its own lock")
	}

	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("after")}); err != nil {
		t.Fatalf("the next publication failed: %v", err)
	}
}

// TestAPublishThatMeetsAClosedChannelResolvesTheOthers holds the closed
// channel branch of `publishFailedLocked`: the write met a channel the broker
// had closed under it, so everything that channel was holding is ambiguous.
// The detach resolved those publications and then dropped the outcomes, so an
// asynchronous publication already in flight never had its callback run.
//
// The write is parked inside the library while the channel closes. `admit`
// no longer holds the sink's lock across the write, so the sink's own close
// notification may detach first, and then the failed write finds its
// publication already resolved; either route has to answer both
// publications, which is what this asserts.
func TestAPublishThatMeetsAClosedChannelResolvesTheOthers(t *testing.T) {
	broker := rabbitfake.New(rabbitfake.Config{Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual}})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cfg.ConfirmKind, cfg.ConfirmWindow = confirm.Async, 8
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	closeBounded(t, pub)

	outcome := make(chan error, 1)
	if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte("in flight")},
		func(r mq.PublishResult) { outcome <- r.Err }); err != nil {
		t.Fatalf("publishing the first message: %v", err)
	}
	ch := publisherChannel(t, broker)

	dialer.armPublish()
	published := make(chan error, 1)
	go func() { published <- pub.Publish(t.Context(), &mq.Message{Body: []byte("into the closing channel")}) }()
	dialer.waitPublishReached(t)
	go ch.CloseWith(&amqp.Error{Code: amqp.PreconditionFailed, Reason: "test: closed under a write", Server: true})
	waitFor(t, 5*time.Second, ch.IsClosed)
	dialer.releasePublish()

	select {
	case err := <-published:
		if err == nil {
			t.Fatal("a publication written into a closed channel reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a publication written into a closed channel did not return")
	}
	select {
	case err := <-outcome:
		if !mq.IsAmbiguous(err) {
			t.Fatalf("the in-flight publication resolved with %v, want an ambiguous outcome", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight publication's callback never ran: the detach the failed write made dropped its outcome")
	}
}

// TestAPublicationTheLibraryRefusesOnAClosedChannelIsNotAmbiguous is a
// publication that reaches the client library after the broker has closed its
// channel, with the publisher's own loop already detached from the channel
// and repairing it.
//
// amqp091 writes nothing on a channel it has marked closed and answers its
// ErrClosed, as the fake does, so the broker has nothing. The loop's detach
// answered every publication it found waiting as ambiguous, this one
// included, though its write had not returned; and a publication that met
// the closed channel before the loop did was answered ambiguous by the write
// itself. Either way a caller who must not duplicate could not publish again
// what the broker never saw. TestAnAckHandedOverBeforeAPublicationMetTheClosedChannelIsNotReportedAmbiguous
// is the second route.
//
// Under ConfirmNone the write's own answer is the whole outcome; the mapper
// reads the library's ErrClosed (a 504) as CodeClosed, retryable, and this
// holds it there.
func TestAPublicationTheLibraryRefusesOnAClosedChannelIsNotAmbiguous(t *testing.T) {
	for _, mode := range []mq.ConfirmationMode{mq.ConfirmDefault, mq.ConfirmNone} {
		t.Run(mode.String(), func(t *testing.T) { publishIntoAClosedChannel(t, mode) })
	}
}

// publishIntoAClosedChannel is
// TestAPublicationTheLibraryRefusesOnAClosedChannelIsNotAmbiguous under one
// confirmation mode.
func publishIntoAClosedChannel(t *testing.T, mode mq.ConfirmationMode) {
	broker := rabbitfake.New(rabbitfake.Config{})
	t.Cleanup(func() { _ = broker.Close() })
	dialer := newChannelGateDialer(t, broker.Dialer())
	cfg := base()
	declaring(&cfg)
	cl, err := rq.OpenWithDialer(t.Context(), dialer, cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	closeBounded(t, cl)
	cons := consumeOn(t, cl, "orders", "workers")
	pub, err := cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders", Confirmation: mode})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	closeBounded(t, pub)
	ch := broker.LastChannel()
	if mode != mq.ConfirmNone {
		ch = publisherChannel(t, broker)
	}
	if ch == nil || len(ch.Consumers()) != 0 || ch.IsConfirming() != (mode != mq.ConfirmNone) {
		t.Fatal("the publisher's channel was not the last one opened")
	}

	dialer.armPublish()
	published := make(chan error, 1)
	go func() { published <- pub.Publish(t.Context(), &mq.Message{Body: []byte("into a closed channel")}) }()
	dialer.waitPublishReached(t)
	// The repair's channel open is held, so the loop has detached by the time
	// it parks there.
	dialer.arm()
	ch.CloseWith(&amqp.Error{Code: amqp.NotFound, Reason: "test: channel closed before the write", Server: true})
	waitFor(t, 5*time.Second, ch.IsClosed)
	dialer.waitReached(t)
	dialer.releasePublish()

	var perr error
	select {
	case perr = <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("the publication did not return")
	}
	if got := len(broker.Publications("orders.workers")); got != 0 {
		t.Fatalf("the broker holds %d publications, want none: this test proves nothing", got)
	}
	if perr == nil || mq.IsAmbiguous(perr) || !mq.IsRetryable(perr) {
		t.Fatalf("a publication the library refused on a closed channel, with nothing at the broker, reported %v (code %v, ambiguous %v, retryable %v), want a retryable refusal that is not ambiguous",
			perr, mq.CodeOf(perr), mq.IsAmbiguous(perr), mq.IsRetryable(perr))
	}

	dialer.release()
	if err := publishRetrying(t.Context(), pub, "into a closed channel"); err != nil {
		t.Fatalf("publishing again once the channel was repaired: %v", err)
	}
	if d := receiveOne(t, cons); string(d.Body) != "into a closed channel" {
		t.Fatalf("the consumer received %q", d.Body)
	}
	if got := len(broker.Publications("orders.workers")); got != 1 {
		t.Fatalf("the broker holds %d copies of the publication, want 1", got)
	}
}

// TestACloseDuringTheConfirmWaitIsAmbiguous is a publisher closed while a
// publication it has already written waits for its confirm.
//
// Close resolves what the sink was holding as Ambiguous, because a close does
// not tell the broker to forget a frame it has read. But it closes the sink's
// closed signal first, and the confirm wait answered that signal with
// ErrClosed: a definite "not sent" for a publication the broker may hold, so a
// caller who retried it published it twice. Each case runs ten times because
// the wait and the close race, although the wrong answer won nearly every run.
// The windowed strategy is here for the wait it makes at the window, which met
// the same signal before the publication's own wait did.
func TestACloseDuringTheConfirmWaitIsAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		name  string
		shape func(*rq.Config)
	}{
		{"per-message", func(c *rq.Config) { declaring(c); c.ConfirmKind = confirm.PerMessage }},
		{"windowed, at the window", func(c *rq.Config) {
			declaring(c)
			c.ConfirmKind, c.ConfirmWindow = confirm.Windowed, 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := range 10 {
				broker, client := openFakeWith(t, rabbitfake.Config{
					Confirms: rabbitfake.ConfirmScript{Style: rabbitfake.ConfirmManual},
				}, tc.shape)
				_ = consumeOn(t, client, "orders", "workers")
				pub, err := client.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
				if err != nil {
					t.Fatalf("opening a publisher: %v", err)
				}
				res := make(chan error, 1)
				go func() { res <- pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}) }()
				waitForPendingConfirm(t, broker)
				_ = pub.Close()
				select {
				case err = <-res:
				case <-time.After(5 * time.Second):
					t.Fatal("the publication did not return after its publisher was closed")
				}
				if !mq.IsAmbiguous(err) {
					t.Fatalf("run %d: a publication already written was answered %v (code %v) when its publisher closed, want an ambiguous outcome",
						i, err, mq.CodeOf(err))
				}
			}
		})
	}
}

// waitForPendingConfirm waits until some open channel has written a
// publication whose confirm it is holding.
func waitForPendingConfirm(t *testing.T, b *rabbitfake.Broker) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b.Sync()
		for _, h := range b.Channels() {
			if !h.IsClosed() && len(h.PendingConfirms()) > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no publication reached the broker")
}

// TestASynchronousPublishWaitsForItsConfirmUnderEveryStrategy holds `Publish`
// to its confirmation boundary whatever Config.ConfirmKind says.
//
// The asynchronous strategy goes on publishing while its window has room,
// which is what `PublishAsync` is for. A synchronous `Publish` has no callback
// to hand a later outcome to, and under that strategy it used to return nil as
// soon as the frame was written: the `basic.nack`, or the `basic.return` of an
// unroutable mandatory publication, then arrived with nobody waiting, and the
// caller had been told the broker held a message it had refused. The core's
// batch loop calls `Publish` once per message, so a batch reported every
// refusal in it as a success as well.
func TestASynchronousPublishWaitsForItsConfirmUnderEveryStrategy(t *testing.T) {
	for _, kind := range []confirm.Kind{confirm.PerMessage, confirm.Windowed, confirm.Async} {
		t.Run(kind.String(), func(t *testing.T) {
			broker, cl := openFake(t, func(c *rq.Config) {
				c.Declare, c.Mandatory, c.ConfirmKind = true, true, kind
				if kind != confirm.PerMessage {
					c.ConfirmWindow = 8
				}
			})
			_ = consumeOn(t, cl, "orders", "a")
			pub := publishOn(t, cl, "orders")

			publisherChannel(t, broker).NackNextPublishes(1)
			if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("nacked")}); err == nil {
				t.Fatal("Publish reported success for a publication the broker nacked")
			}

			publisherChannel(t, broker).NackNextPublishes(1)
			err := pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("nacked")}, {Body: []byte("stored")}})
			var be *mq.BatchError
			if !errors.As(err, &be) || len(be.Errs) != 2 || be.Errs[0] == nil || be.Errs[1] != nil {
				t.Fatalf("a batch whose first message the broker nacked reported %v, want a *BatchError that "+
					"blames that message and no other", err)
			}

			unbound := publishOn(t, cl, "unbound")
			if err := unbound.Publish(t.Context(), &mq.Message{Body: []byte("returned")}); mq.CodeOf(err) != mq.CodeNotFound {
				t.Fatalf("Publish of a mandatory publication no queue is bound for reported %v, want CodeNotFound", err)
			}
		})
	}
}

// TestABasicNackIsAnAmbiguousOutcome is a publication the broker answers with
// `basic.nack` on a topic with two subscriptions.
//
// RabbitMQ nacks a publication when one queue it was routed to refuses it, a
// full queue whose overflow is reject-publish for example, and it stores it
// in every other queue that took it. The fake does the same here: both queues
// hold the message and the confirm is a nack. The driver reported the nack as
// a definite, retryable refusal, so a caller who retried, as x/outbox's relay
// does, put a second copy in each queue that had taken it.
func TestABasicNackIsAnAmbiguousOutcome(t *testing.T) {
	broker, cl := openFake(t, declaring)
	_ = consumeOn(t, cl, "orders", "a")
	_ = consumeOn(t, cl, "orders", "b")
	pub := publishOn(t, cl, "orders")
	check := func(how string, err error) {
		t.Helper()
		var oe *mq.OpError
		if !errors.As(err, &oe) || !mq.IsAmbiguous(err) || oe.Code != mq.CodeAmbiguous || oe.Retryable {
			t.Fatalf("%s of a publication the broker nacked reported %v (code %v, ambiguous %v, retryable flag %v), want CodeAmbiguous, ambiguous and not retryable: the broker may have stored it in another queue",
				how, err, mq.CodeOf(err), mq.IsAmbiguous(err), oe != nil && oe.Retryable)
		}
	}

	publisherChannel(t, broker).NackNextPublishes(1)
	check("Publish", pub.Publish(t.Context(), &mq.Message{Body: []byte("nacked")}))
	for _, q := range []string{"orders.a", "orders.b"} {
		if got := len(broker.Publications(q)); got != 1 {
			t.Fatalf("queue %s holds %d publications, want 1: the fake did not store the nacked publication, so this test proves nothing", q, got)
		}
	}

	publisherChannel(t, broker).NackNextPublishes(1)
	got := make(chan error, 1)
	if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte("nacked async")}, func(r mq.PublishResult) { got <- r.Err }); err != nil {
		t.Fatalf("PublishAsync: %v", err)
	}
	select {
	case err := <-got:
		check("PublishAsync's callback", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the callback of a publication the broker nacked never ran")
	}
}

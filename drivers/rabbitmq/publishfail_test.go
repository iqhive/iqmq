package rabbitmq_test

import (
	"context"
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

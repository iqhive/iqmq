package rabbitmq_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
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

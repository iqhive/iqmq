package rabbitmq_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// The Config fields that reach the wire only as a property or a flag, checked
// where the broker sees them. Each was documented and then read by nothing, so
// a caller who set one got a publication or a consumer without it and no
// error to say so.

// TestAppIDAndPublishTypeAreOnEveryPublication is [rq.Config.AppID] and
// [rq.Config.PublishType]: the `app-id` and `type` properties a management
// view and a consumer read to tell one application's publications from
// another's.
func TestAppIDAndPublishTypeAreOnEveryPublication(t *testing.T) {
	broker, client, admin := adminOf(t, func(c *rq.Config) {
		c.Declare = true
		c.AppID, c.PublishType = "orders-service", "OrderPlaced"
	})
	if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	broker.Sync()
	got := broker.Ready("orders.workers")
	if len(got) != 1 {
		t.Fatalf("the queue holds %d messages, want 1", len(got))
	}
	if got[0].AppID != "orders-service" {
		t.Errorf("app-id is %q, want %q", got[0].AppID, "orders-service")
	}
	if got[0].Type != "OrderPlaced" {
		t.Errorf("type is %q, want %q", got[0].Type, "OrderPlaced")
	}
}

// TestExclusiveConsumerIsAskedForAndRefusesASecond is
// [rq.Config.ExclusiveConsumer]: the consumer is registered exclusive, and a
// second consumer on the same subscription is refused by the broker.
func TestExclusiveConsumerIsAskedForAndRefusesASecond(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare, c.ExclusiveConsumer = true, true
	})
	if err := startConsuming(t, consumeOn(t, client, "orders", "workers")); err != nil {
		t.Fatalf("the first consumer did not attach: %v", err)
	}
	states := consumerStates(broker)
	if len(states) != 1 || !states[0].Exclusive {
		t.Fatalf("the consumers are %+v, want one exclusive consumer", states)
	}
	if err := startConsuming(t, consumeOn(t, client, "orders", "workers")); err == nil {
		t.Fatal("a second consumer attached to a queue with an exclusive consumer")
	}
}

// TestConsumerIsNotExclusiveByDefault is the default worth pinning: a
// subscription's meaning is competing consumers.
func TestConsumerIsNotExclusiveByDefault(t *testing.T) {
	broker, client := openFake(t, declaring)
	for range 2 {
		if err := startConsuming(t, consumeOn(t, client, "orders", "workers")); err != nil {
			t.Fatalf("a competing consumer did not attach: %v", err)
		}
	}
	states := consumerStates(broker)
	if len(states) != 2 {
		t.Fatalf("%d consumers, want 2", len(states))
	}
	for _, st := range states {
		if st.Exclusive {
			t.Errorf("consumer %s is exclusive without Config.ExclusiveConsumer", st.Tag)
		}
	}
}

// TestConsumeArgsAreSentWithTheConsume is [rq.Config.ConsumeArgs]: the
// arguments travel on `basic.consume`, which is where a consumer priority
// lives.
func TestConsumeArgsAreSentWithTheConsume(t *testing.T) {
	broker, client := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ConsumeArgs = map[string]any{"x-priority": int32(10)}
	})
	if err := startConsuming(t, consumeOn(t, client, "orders", "workers")); err != nil {
		t.Fatalf("the consumer did not attach: %v", err)
	}
	states := consumerStates(broker)
	if len(states) != 1 {
		t.Fatalf("%d consumers, want 1", len(states))
	}
	if got := states[0].Args["x-priority"]; got != int32(10) {
		t.Errorf("x-priority is %v (%T), want int32 10: the consume arguments were not sent", got, got)
	}
}

// startConsuming makes a consumer attach, which its first Receive does, and
// returns what that Receive reported other than running out of time on an
// empty queue.
func startConsuming(t *testing.T, cons *mq.ConsumerHandle) error {
	t.Helper()
	ctx, cancel := contextWithTimeout(t, 100*time.Millisecond)
	defer cancel()
	_, err := cons.Receive(ctx, make([]mq.Delivery, 1))
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// consumerStates is every live consumer on every open channel.
func consumerStates(b *rabbitfake.Broker) []rabbitfake.ConsumerState {
	var out []rabbitfake.ConsumerState
	for _, ch := range b.Channels() {
		if ch.IsClosed() {
			continue
		}
		for _, st := range ch.ConsumerStates() {
			if !st.Cancelled {
				out = append(out, st)
			}
		}
	}
	return out
}

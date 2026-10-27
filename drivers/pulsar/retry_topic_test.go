package pulsar_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// subscribeRecorder is the in-process model that keeps the consumer options
// the driver subscribed with, which is the only place a policy the client
// enforces on its own can be read.
type subscribeRecorder struct {
	pulsarapi.Client
	mu   sync.Mutex
	subs []pulsar.ConsumerOptions
}

func (c *subscribeRecorder) Subscribe(o pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	c.mu.Lock()
	c.subs = append(c.subs, o)
	c.mu.Unlock()
	return c.Client.Subscribe(o)
}

// last is the options of the most recent subscription.
func (c *subscribeRecorder) last(t *testing.T) pulsar.ConsumerOptions {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.subs) == 0 {
		t.Fatal("nothing subscribed")
	}
	return c.subs[len(c.subs)-1]
}

// TestARetryTopicWithoutADeadLetterTopicNeverDeadLetters is Config.RetryEnable
// with no Config.DeadLetterTopic. The driver used to subscribe with no DLQ
// policy at all, and pulsar-client-go then makes one up: a message redelivered
// or reconsumed 16 times went to `<topic>-<subscription>-DLQ`, a topic nobody
// named, although the configuration says an empty DeadLetterTopic means there
// is no dead-letter destination. The retry topic the configuration named was
// ignored too, because it was only set beside a dead-letter topic.
func TestARetryTopicWithoutADeadLetterTopicNeverDeadLetters(t *testing.T) {
	server := pulsarfake.New()
	cl := &subscribeRecorder{Client: server.Client()}
	cfg := rigConfig(server)
	cfg.RetryEnable, cfg.RetryTopic = true, "orders-later"
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	_ = sourceOf(t, b, "orders", "workers", mq.MetadataNativeStrict, 4)
	o := cl.last(t)
	if o.DLQ == nil {
		t.Fatal("the consumer was opened with RetryEnable and no DLQ policy, so pulsar-client-go invents " +
			"a dead-letter topic and routes to it after 16 deliveries")
	}
	if o.DLQ.MaxDeliveries != math.MaxUint32 {
		t.Errorf("the dead-letter bound is %d; with no destination configured no count may reach it",
			o.DLQ.MaxDeliveries)
	}
	if want := "persistent://public/default/orders-later"; o.DLQ.RetryLetterTopic != want {
		t.Errorf("the retry topic is %q, want the configured %q", o.DLQ.RetryLetterTopic, want)
	}
}

// TestAnExclusiveRetryTopicDoesNotIgnoreTheDelay is a delayed negative
// settlement on an Exclusive client with the retry topic enabled. The retry
// topic's delay is a DeliverAfter, which only the Shared and Key_Shared
// dispatchers honour, and the driver used to take the retry topic anyway: the
// requeued message came straight back, so a handler that asked for a minute's
// grace saw every attempt at once, and the capability table called the delay
// native.
func TestAnExclusiveRetryTopicDoesNotIgnoreTheDelay(t *testing.T) {
	b, server := newRig(t, func(c *pd.Config) {
		c.SubscriptionType = pd.SubscriptionExclusive
		c.RetryEnable, c.DeadLetterTopic = true, "excl-dlq"
	})
	if lvl := b.Capabilities().Support(mq.FeatureDelayedRedelivery).Level; lvl != mq.EmulatedVolatile {
		t.Errorf("FeatureDelayedRedelivery is advertised at %v on an Exclusive client, want EmulatedVolatile", lvl)
	}
	src := sourceOf(t, b, "excl", "workers", mq.MetadataNativeStrict, 4)
	// The model subscribes to one topic at a time, so the retry topic is
	// read the way the real multi-topic consumer would read it: by the same
	// subscription.
	retry := sourceOf(t, b, "excl-workers-RETRY", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "excl", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("later")})
	d := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: time.Minute}); err != nil {
		t.Fatalf("Nack(delay 1m): %v", err)
	}
	fetchNothing(t, retry, "a one-minute delayed requeue came back at once through the retry topic")
	fetchNothing(t, src, "a one-minute delayed requeue came back at once")
	server.Advance(time.Minute + time.Second)
	again, ok := fetchAdvancing(t, src, server, time.Second)
	if !ok {
		t.Fatal("the delayed requeue never came back")
	}
	if string(again.Message.Body) != "later" {
		t.Fatalf("the redelivery carries %q", again.Message.Body)
	}
}

// TestTheAttemptCountSurvivesARetryHopAfterANack is a message nacked once and
// then requeued with a delay through the retry topic. The broker's redelivery
// count starts again at zero on the retry topic and the client counts only the
// hops, so the third delivery used to report attempt 2, the same as the second,
// and a MaxAttempts policy above it saw one attempt fewer than there had been.
func TestTheAttemptCountSurvivesARetryHopAfterANack(t *testing.T) {
	b, server := newRig(t, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic = true, "attempts-dlq"
	})
	src := sourceOf(t, b, "attempts", "workers", mq.MetadataNativeStrict, 4)
	// The model subscribes to one topic at a time, so the retry topic is
	// read the way the real multi-topic consumer would read it: by the same
	// subscription.
	retry := sourceOf(t, b, "attempts-workers-RETRY", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "attempts", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("x")})
	first := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second, ok := fetchAdvancing(t, src, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no redelivery after the nack")
	}
	if err := src.Settler().Nack(context.Background(), &second, mq.NackOptions{Delay: time.Second}); err != nil {
		t.Fatalf("delayed Nack: %v", err)
	}
	third, ok := fetchAdvancing(t, retry, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no delivery through the retry topic")
	}
	got := []uint32{first.Info.Attempt, second.Info.Attempt, third.Info.Attempt}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("the three deliveries report attempts %v, want [1 2 3]", got)
	}
	if third.Info.Topic != "attempts" {
		t.Fatalf("the retry delivery reports topic %q, want the one the message was published to", third.Info.Topic)
	}
}

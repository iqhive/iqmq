package rabbitmq_test

import (
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// declaring is the configuration most tests want: the driver may create its
// own topology, because the conformance kit's and a test's own provisioning is
// the operator (the rule is about a deployment).
func declaring(c *rq.Config) { c.Declare = true }

// mandating is `declaring` plus the `mandatory` flag, which is opt-in: see
// TestMandatoryIsOffByDefault for why the default is off, and the specification's
// "Mandatory + NotifyReturn = Rejected" for what turning it on buys.
func mandating(c *rq.Config) { c.Declare, c.Mandatory = true, true }

// openFake opens a client over a fake broker with the default broker
// configuration.
func openFake(t *testing.T, shape func(*rq.Config)) (*rabbitfake.Broker, *mq.Client) {
	t.Helper()
	return openFakeWith(t, rabbitfake.Config{}, shape)
}

// openFakeWith is [openFake] with the broker configured too, for the tests
// whose subject is a broker behaviour rather than a driver one: the confirm
// framing, the delayed-message plugin, the delivery-count mode, the
// notification timeout.
func openFakeWith(t *testing.T, bcfg rabbitfake.Config, shape func(*rq.Config)) (*rabbitfake.Broker, *mq.Client) {
	t.Helper()
	broker := rabbitfake.New(bcfg)
	t.Cleanup(func() { _ = broker.Close() })

	cfg := base()
	if shape != nil {
		shape(&cfg)
	}
	client, err := rq.OpenWithDialer(t.Context(), broker.Dialer(), cfg)
	if err != nil {
		t.Fatalf("opening a client over the fake broker: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return broker, client
}

// consumeOn opens a consumer with the driver's default prefetch.
func consumeOn(t *testing.T, c *mq.Client, topic, sub string) *mq.ConsumerHandle {
	t.Helper()
	return consumeOnWith(t, c, topic, sub, nil)
}

// consumeOnWith is [consumeOn] with the consumer configuration shaped.
func consumeOnWith(t *testing.T, c *mq.Client, topic, sub string, shape func(*mq.ConsumerConfig)) *mq.ConsumerHandle {
	t.Helper()
	cfg := mq.ConsumerConfig{Topic: topic, Subscription: sub}
	if shape != nil {
		shape(&cfg)
	}
	cons, err := c.Consumer(t.Context(), cfg)
	if err != nil {
		t.Fatalf("opening a consumer on %s/%s: %v", topic, sub, err)
	}
	t.Cleanup(func() { _ = cons.Close() })
	return cons
}

// publishOn opens a publisher on a topic.
func publishOn(t *testing.T, c *mq.Client, topic string) *mq.PublisherHandle {
	t.Helper()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher on %s: %v", topic, err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	return pub
}

// receiveOne receives exactly one delivery, or fails the test.
//
// The bound is the test's own rather than the consumer's: a fake broker
// answers in microseconds, so a receive that takes seconds is a driver that
// is not delivering and the test should say so rather than time out at the
// package level.
func receiveOne(t *testing.T, cons *mq.ConsumerHandle) mq.Delivery {
	t.Helper()
	ctx, cancel := contextWithTimeout(t, 5*time.Second)
	defer cancel()
	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if err != nil {
		t.Fatalf("receiving: %v", err)
	}
	if n != 1 {
		t.Fatalf("Receive returned %d deliveries, want 1", n)
	}
	return dst[0]
}

// queueNames is the broker's queues, for a failure message.
func queueNames(b *rabbitfake.Broker) []string {
	qs := b.Queues()
	out := make([]string, 0, len(qs))
	for _, q := range qs {
		out = append(out, q.Name)
	}
	return out
}

// contains is strings.Contains, so that a failure message reads as a sentence.
func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// backendOf is the driver's backend behind a client.
func backendOf(t *testing.T, c *mq.Client) *rq.Backend {
	t.Helper()
	b, ok := rq.BackendForTest(c)
	if !ok {
		t.Fatal("the client's Native is not this driver's backend")
	}
	return b
}

// publisherChannel is the channel a publisher is using: the one in confirm
// mode with no consumers.
func publisherChannel(t *testing.T, b *rabbitfake.Broker) *rabbitfake.ChannelHandle {
	t.Helper()
	for _, ch := range b.Channels() {
		if ch.IsConfirming() && len(ch.Consumers()) == 0 && !ch.IsClosed() {
			return ch
		}
	}
	t.Fatal("no publisher channel is open")
	return nil
}

// consumerChannel is the channel a consumer is using: the one with a consumer
// registered on it.
func consumerChannel(t *testing.T, b *rabbitfake.Broker) *rabbitfake.ChannelHandle {
	t.Helper()
	for _, ch := range b.Channels() {
		if len(ch.Consumers()) > 0 && !ch.IsClosed() {
			return ch
		}
	}
	t.Fatal("no consumer channel is open")
	return nil
}

// consumerTags is how many consumers are registered across every open
// channel.
func consumerTags(b *rabbitfake.Broker) int {
	n := 0
	for _, ch := range b.Channels() {
		if !ch.IsClosed() {
			n += len(ch.Consumers())
		}
	}
	return n
}

// settlementCount is how many settlements the broker has recorded that a
// CLIENT asked for.
//
// A channel close settles what it was holding too, and those are the broker's
// own rather than the driver's -- so they are excluded, because the question
// every staleness test asks is "did the driver send a frame", not "did
// anything happen".
func settlementCount(b *rabbitfake.Broker) int {
	n := 0
	settled := b.Settlements()
	for i := range settled {
		if settled[i].Kind != rabbitfake.SettleChannelClosed {
			n++
		}
	}
	return n
}

// sum adds a slice of counts, for a failure message.
func sum(xs []int) int {
	n := 0
	for _, x := range xs {
		n += x
	}
	return n
}

// waitFor polls until the condition holds, or fails the test.
//
// It polls rather than parking on an event because the events it waits for are
// the DRIVER's own background work -- a reconnection, a re-attach -- which the
// portable API deliberately does not expose. A poll on a fake broker's
// in-process state costs microseconds.
func waitFor(t *testing.T, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the condition did not hold within %v", within)
}

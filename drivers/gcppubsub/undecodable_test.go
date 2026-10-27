package gcppubsub_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	gps "github.com/iqhive/iqmq/drivers/gcppubsub"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psfake"
)

// publishUndecodable writes a message an enveloping consumer cannot decode: a
// plain body published straight to the fake, which a MetadataPortableEnvelope
// consumer refuses because it is not an envelope.
func publishUndecodable(tb testing.TB, srv *psfake.Server, topic string) {
	tb.Helper()
	id, err := gps.Config{ProjectID: "iqmq-test"}.TopicID(topic)
	if err != nil {
		tb.Fatalf("TopicID: %v", err)
	}
	pub, err := srv.Publisher(psapi.PublisherConfig{Topic: id})
	if err != nil {
		tb.Fatalf("Publisher: %v", err)
	}
	if _, err := pub.Publish(context.Background(), &psapi.Outgoing{Data: []byte("not an envelope")}).Get(context.Background()); err != nil {
		tb.Fatalf("publishing the undecodable message: %v", err)
	}
}

// publishEnveloped writes a message the same consumer can decode.
func publishEnveloped(tb testing.TB, c *mq.Client, topic string) {
	tb.Helper()
	p, err := c.Publisher(context.Background(), mq.PublisherConfig{Topic: topic, Metadata: mq.MetadataPortableEnvelope})
	if err != nil {
		tb.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	if err := p.Publish(context.Background(), &mq.Message{Body: []byte("good")}); err != nil {
		tb.Fatalf("Publish: %v", err)
	}
}

// countNacks counts every negative settlement the fake receives and lets it
// through.
func countNacks(srv *psfake.Server) *atomic.Int64 {
	var n atomic.Int64
	srv.NackErr = func(string) error {
		n.Add(1)
		return nil
	}
	return &n
}

// TestAnUndecodableMessageIsNotRedeliveredInAHotLoop is the consumer that spun.
//
// On a subscription with no dead-letter policy an undecodable message used to
// be nacked at once, the service made it deliverable at once, and nothing ever
// ended it, so the callback went round more than a million times a second for
// as long as the message existed. It is now held for one acknowledgement
// deadline and nacked after it: no nack at all inside the deadline, one after
// it, and the message is still at the service rather than destroyed.
func TestAnUndecodableMessageIsNotRedeliveredInAHotLoop(t *testing.T) {
	c, srv := scripted(t)
	topic := newTopic("undecodable")
	provision(t, c, topic, "s")
	nacks := countNacks(srv)
	publishUndecodable(t, srv, topic)
	publishEnveloped(t, c, topic)

	cons, err := c.Consumer(context.Background(), mq.ConsumerConfig{
		Topic: topic, Subscription: "s", Metadata: mq.MetadataPortableEnvelope, AckDeadline: time.Second,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	// The fake delivers in publication order, so the good message arriving
	// means the undecodable one has been through the callback.
	d := receiveOne(t, cons, 5*time.Second)
	if string(d.Body) != "good" {
		t.Fatalf("received %q, want the decodable message", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := nacks.Load(); got != 0 {
		t.Fatalf("the undecodable message was nacked %d times inside its one-second deadline; "+
			"a nack makes it deliverable at once, so it is going round in a hot loop", got)
	}
	waitNacks(t, nacks, 1, 5*time.Second)
	sub, err := gps.Config{ProjectID: "iqmq-test"}.SubscriptionID(topic, "s")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	if got := srv.Backlog(sub) + srv.Outstanding(sub); got != 1 {
		t.Fatalf("the subscription holds %d messages after the undecodable one was nacked, want it still there", got)
	}
}

// TestAnUndecodableMessageReachesTheDeadLetterPolicyAtOnce is the other half:
// on a subscription with a dead-letter policy the service counts deliveries and
// moves the message after MaxDeliveryAttempts, so the nack is still sent at
// once rather than a deadline apart. The fake does not move messages, so the
// test stands in for the service: the fifth nack is refused, which leaves the
// message outstanding and ends its redelivery, as the policy would.
func TestAnUndecodableMessageReachesTheDeadLetterPolicyAtOnce(t *testing.T) {
	const attempts = 5
	dlq := newTopic("undecodable-dlq")
	c, srv := scripted(t, func(cfg *gps.Config) {
		cfg.DeadLetterTopic = dlq
		cfg.MaxDeliveryAttempts = attempts
	})
	topic := newTopic("undecodable-policy")
	provision(t, c, topic, "s")
	provision(t, c, dlq, "d")
	var nacks atomic.Int64
	srv.NackErr = func(string) error {
		if nacks.Add(1) >= attempts {
			return errors.New("injected: the dead-letter policy has moved the message")
		}
		return nil
	}
	publishUndecodable(t, srv, topic)

	cons, err := c.Consumer(context.Background(), mq.ConsumerConfig{
		Topic: topic, Subscription: "s", Metadata: mq.MetadataPortableEnvelope,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	// The deadline is the default ten seconds, so five deliveries a deadline
	// apart would take forty.
	waitNacks(t, &nacks, attempts, 5*time.Second)
}

// waitNacks waits for at least want nacks.
func waitNacks(t *testing.T, nacks *atomic.Int64, want int64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for nacks.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("%d nacks after %v, want %d", nacks.Load(), within, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

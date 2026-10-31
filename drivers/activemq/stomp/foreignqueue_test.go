package stomp_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// Artemis attaches a STOMP consumer to a queue by its name alone, and an
// Artemis queue name is unique across the broker, so a subscription whose
// queue name is taken on another address consumes that address's queue.
// Measured against the pinned image: a SUBSCRIBE to `orders::orders.billing`
// over a JMS queue `orders.billing` on its own address receives that queue's
// messages, each in a MESSAGE frame whose `destination` names the address it
// was published to. The in-process server's deliveries carry Artemis's
// `destination` the same way, and stompsrv.Message.Address stands for a queue
// of another address. Real-broker twins are in collision_test.go.

// TestASubscriptionOnAnotherAddresssQueueHandsNothingOut is the fan-out
// subscription "orders"/"billing" attached to another application's JMS queue
// `orders.billing`, and the AddressQueue topic "orders.billing" attached to
// that subscription's queue. The source handed out and acknowledged the other
// application's messages. Now the first one ends the source for good with a
// Conflict, nothing is handed out or acknowledged, and the subscription is
// given back with an UNSUBSCRIBE, which returns the messages to their owner as
// they were.
func TestASubscriptionOnAnotherAddresssQueueHandsNothingOut(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, topic, sub, from string
		addressing             stomp.Addressing
	}{
		{name: "fan-out over a JMS queue", topic: "orders", sub: "billing", from: "orders.billing"},
		// The JMS queue's address ends with the topic's address, so a check
		// that accepts any text in front of the address (an acceptor's
		// prefix) accepted it as the subscription's own.
		{name: "fan-out named like its topic over a JMS queue", topic: "orders", sub: "orders", from: "orders.orders"},
		{
			name: "AddressQueue over a subscription's queue", topic: "orders.billing", sub: "x", from: "orders",
			addressing: stomp.AddressQueue,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			cfg := artemisConfig(srv)
			cfg.Addressing = tc.addressing
			b := openBackend(t, cfg)
			src, dest := openSource(t, b, cfg, srv, tc.topic, tc.sub, 4)
			deliver(t, srv, dest, stompsrv.Message{Body: []byte("an invoice for the billing service"), Address: tc.from})

			err := fetchError(t, src, budget)
			if code := mq.CodeOf(err); code != mq.CodeConflict || mq.IsRetryable(err) {
				t.Errorf("the fetch returned %v (code %v, retryable %v), want a Conflict that is not retryable: "+
					"the subscription's queue is %s's", err, code, mq.IsRetryable(err), tc.from)
			}
			if n := countOf(srv, frame.ACK) + countOf(srv, frame.NACK); n != 0 {
				t.Errorf("the source settled %d deliveries of another address's queue\nframes:%s", n, frameLog(srv))
			}
			if n := countOf(srv, frame.UNSUBSCRIBE); n != 1 {
				t.Errorf("the source sent %d UNSUBSCRIBE frames, want 1: the subscription is given back, so the "+
					"broker returns the other application's messages\nframes:%s", n, frameLog(srv))
			}
			subscribes := countOf(srv, frame.SUBSCRIBE)
			again := fetchError(t, src, 100*time.Millisecond)
			if mq.CodeOf(again) != mq.CodeConflict {
				t.Errorf("the next fetch returned %v, want the same Conflict: every subscription this source "+
					"could make attaches to the same queue", again)
			}
			if n := countOf(srv, frame.SUBSCRIBE); n != subscribes {
				t.Errorf("the next fetch subscribed again (%d SUBSCRIBE frames, was %d)", n, subscribes)
			}
		})
	}
}

// TestAPublicationThroughAPrefixOrAnFQQNIsTheSubscriptionsOwn is the other
// side of that check: a delivery's `destination` names the address with a
// prefix in front when its publisher came through an acceptor that has one
// (`jms.topic.orders`, measured on the image's port 5445), and with the queue
// after it when it was sent to the fully qualified queue name. Both are this
// subscription's own messages.
func TestAPublicationThroughAPrefixOrAnFQQNIsTheSubscriptionsOwn(t *testing.T) {
	t.Parallel()
	// "orders" as well as "billing": a subscription named like its topic is
	// the one whose queue name ends with its own address.
	for _, sub := range []string{"billing", "orders"} {
		t.Run(sub, func(t *testing.T) {
			t.Parallel()
			srv := artemisServer(t)
			cfg := artemisConfig(srv)
			b := openBackend(t, cfg)
			src, dest := openSource(t, b, cfg, srv, "orders", sub, 4)
			deliver(t, srv, dest,
				stompsrv.Message{Body: []byte("plain")},
				stompsrv.Message{Body: []byte("prefixed"), Address: "jms.topic.orders"},
				stompsrv.Message{Body: []byte("fqqn"), Address: "orders::orders." + sub})
			got := fetchN(t, src, 3)
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			for i := range got {
				if err := src.Settler().Ack(ctx, &got[i]); err != nil {
					t.Errorf("Ack of %q: %v", got[i].Message.Body, err)
				}
			}
		})
	}
}

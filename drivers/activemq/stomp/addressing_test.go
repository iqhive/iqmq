package stomp_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	stomp "github.com/iqhive/iqmq/drivers/activemq/stomp"
	"github.com/iqhive/iqmq/drivers/activemq/stomp/internal/stompsrv"
)

// queueSettings is [settings] addressing JMS queues.
func queueSettings(b stomp.Broker) stomp.Config {
	cfg := settings(b)
	cfg.Addressing = stomp.AddressQueue
	return cfg
}

// TestQueueAddressingDestinations: a SEND and a SUBSCRIBE name the one JMS
// queue, in each broker's STOMP form, whatever subscription the consumer
// gives.
func TestQueueAddressingDestinations(t *testing.T) {
	for _, tc := range []struct {
		broker stomp.Broker
		want   string
	}{
		{stomp.Artemis, "ORDERS.IN"},
		{stomp.Classic, "/queue/ORDERS.IN"},
	} {
		t.Run(tc.broker.String(), func(t *testing.T) {
			publish, consume := destinations(t, queueSettings(tc.broker), "ORDERS.IN", "billing.v2")
			if publish != tc.want || consume != tc.want {
				t.Errorf("publish %q consume %q, want both %q", publish, consume, tc.want)
			}
		})
	}
}

// TestQueueAddressingRoutingHeaders pins the ANYCAST routing type on Artemis's
// frames, on both sides. Without it a SUBSCRIBE is read by the address's
// default and a SEND to an address carrying both routing types reaches its
// multicast queues instead of the JMS queue. Classic says "queue" in the path
// and carries neither header; fan-out keeps MULTICAST.
func TestQueueAddressingRoutingHeaders(t *testing.T) {
	sub := stomp.SubscribeHeadersForTest(queueSettings(stomp.Artemis), 1)
	if got := sub["subscription-type"]; got != "ANYCAST" {
		t.Errorf("Artemis SUBSCRIBE subscription-type %q, want ANYCAST", got)
	}
	if got := stomp.SubscribeHeadersForTest(settings(stomp.Artemis), 1)["subscription-type"]; got != "MULTICAST" {
		t.Errorf("fan-out SUBSCRIBE subscription-type %q, want MULTICAST", got)
	}
	if _, ok := stomp.SubscribeHeadersForTest(queueSettings(stomp.Classic), 1)["subscription-type"]; ok {
		t.Error("a Classic SUBSCRIBE carries Artemis's subscription-type")
	}
	m := &mq.Message{Body: []byte("x")}
	send, err := stomp.SendHeadersForTest(queueSettings(stomp.Artemis), m, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := send["destination-type"]; got != "ANYCAST" {
		t.Errorf("Artemis SEND destination-type %q, want ANYCAST", got)
	}
	for _, cfg := range []stomp.Config{queueSettings(stomp.Classic), settings(stomp.Artemis)} {
		send, err := stomp.SendHeadersForTest(cfg, m, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := send["destination-type"]; ok {
			t.Errorf("%v/%v SEND carries destination-type", cfg.Broker, cfg.Addressing)
		}
	}
}

// TestQueueAddressingDSN: the parameter round-trips on this scheme too, the
// default renders as it always did, and an unknown mode is refused.
func TestQueueAddressingDSN(t *testing.T) {
	cfg, err := stomp.ParseDSN("activemq+stomp://localhost:61613?broker=classic&addressing=queue")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if cfg.Addressing != stomp.AddressQueue {
		t.Fatalf("addressing %v, want queue", cfg.Addressing)
	}
	rendered := cfg.DSN()
	back, err := stomp.ParseDSN(rendered)
	if err != nil || back.Addressing != stomp.AddressQueue {
		t.Fatalf("ParseDSN(%q) = (%v, %v), want queue addressing", rendered, back.Addressing, err)
	}
	if s := settings(stomp.Classic).DSN(); strings.Contains(s, "addressing") {
		t.Errorf("the default renders %q, want no addressing parameter", s)
	}
	if _, err := stomp.ParseDSN("activemq+stomp://localhost:61613?broker=classic&addressing=topic"); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("an unknown mode gave %v, want CodeInvalidConfig", err)
	}
}

// TestQueueAddressingClaimsNoFanOut: a JMS queue gives each message to one
// consumer, and the table says so.
func TestQueueAddressingClaimsNoFanOut(t *testing.T) {
	for _, b := range []stomp.Broker{stomp.Artemis, stomp.Classic} {
		caps, err := stomp.CapabilitiesForTest(queueSettings(b))
		if err != nil {
			t.Fatal(err)
		}
		if caps.Has(mq.FeatureFanOut) {
			t.Errorf("%v: fan-out is claimed: %+v", b, caps.Support(mq.FeatureFanOut))
		}
		if !caps.Has(mq.FeatureConsumerGroups) {
			t.Errorf("%v: competing consumers are not claimed", b)
		}
	}
}

// TestQueueAddressingRoundTrip publishes to and consumes from one JMS queue on a
// real STOMP connection, on both brokers: the SEND goes to the queue, two
// consumers with different subscriptions SUBSCRIBE to that one destination,
// and a message delivered on it is fetched.
func TestQueueAddressingRoundTrip(t *testing.T) {
	for _, broker := range []stomp.Broker{stomp.Artemis, stomp.Classic} {
		t.Run(broker.String(), func(t *testing.T) {
			srv := brokerServer(t, broker)
			cfg := queueSettings(broker)
			cfg.Addrs = []string{srv.Addr()}
			b := openBackend(t, cfg)
			dest, _ := destinations(t, cfg, "ORDERS.IN", "billing")

			snk := openSink(t, b, "ORDERS.IN", mq.ConfirmAccepted)
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			if err := snk.Publish(ctx, &mq.Message{Body: []byte("order-1")}, nil); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			sends := srv.FramesOf(frame.SEND)
			if len(sends) != 1 || sends[0].Get(frame.Destination) != dest {
				t.Fatalf("SEND frames %d, destination %q; want one to %q", len(sends), firstDest(sends), dest)
			}

			_, _ = openSource(t, b, cfg, srv, "ORDERS.IN", "billing", 4)
			second, _ := openSource(t, b, cfg, srv, "ORDERS.IN", "audit", 4)
			// openSource waits for A subscription on the destination, which
			// the first consumer already satisfies, so the second one's
			// SUBSCRIBE is waited for here.
			if err := srv.WaitLive(dest, 2, budget); err != nil {
				t.Fatalf("both consumers did not subscribe to the one queue %s: %v\n%s", dest, err, frameLog(srv))
			}
			// The fake hands a delivery to the most recently subscribed
			// consumer, so the second one takes it, and the first shares
			// the queue, which is all a broker needs to make them compete.
			deliver(t, srv, dest, body("order-1"))
			got := fetchN(t, second, 1)
			if string(got[0].Message.Body) != "order-1" {
				t.Errorf("fetched %q, want order-1", got[0].Message.Body)
			}
		})
	}
}

// firstDest is the first frame's destination, for a failure message.
func firstDest(fs []stompsrv.Frame) string {
	if len(fs) == 0 {
		return ""
	}
	return fs[0].Get(frame.Destination)
}

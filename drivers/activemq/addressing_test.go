package activemq_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	amq "github.com/iqhive/iqmq/drivers/activemq"
)

// queueConfig is artemisConfig addressing JMS queues.
func queueConfig(broker amq.Broker) amq.Config {
	cfg := artemisConfig()
	cfg.Broker = broker
	cfg.Addressing = amq.AddressQueue
	return cfg
}

// TestQueueAddressingNamesTheQueueOnBothSides: a publisher and a consumer of a
// JMS queue name the one destination, whatever subscription the consumer
// gives, and the subscription is not checked, because it names nothing.
func TestQueueAddressingNamesTheQueueOnBothSides(t *testing.T) {
	for _, tc := range []struct {
		broker amq.Broker
		want   string
	}{
		{amq.Artemis, "ORDERS.IN"},
		{amq.Classic, "queue://ORDERS.IN"},
	} {
		t.Run(tc.broker.String(), func(t *testing.T) {
			sink, source := addresses(t, queueConfig(tc.broker), "ORDERS.IN", "billing.v2")
			if sink != tc.want || source != tc.want {
				t.Errorf("sink %q source %q, want both %q", sink, source, tc.want)
			}
		})
	}
}

// TestQueueAddressingAsksArtemisForAnycast pins the routing type on the links.
// A JMS queue's address can carry both routing types, and without the `queue`
// capability Artemis would route a publication by the address's default
// (multicast) and give a consumer a multicast queue.
func TestQueueAddressingAsksArtemisForAnycast(t *testing.T) {
	own := []string{"caller-cap"}
	cfg := queueConfig(amq.Artemis)
	cfg.Publisher.TargetCapabilities = own
	cc, err := cfg.CoreConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(cc.Publisher.TargetCapabilities, "queue") || !slices.Contains(cc.Publisher.TargetCapabilities, "caller-cap") {
		t.Errorf("target capabilities %v, want the caller's and \"queue\"", cc.Publisher.TargetCapabilities)
	}
	if !slices.Contains(cc.Consumer.SourceCapabilities, "queue") {
		t.Errorf("source capabilities %v, want \"queue\"", cc.Consumer.SourceCapabilities)
	}
	if len(own) != 1 {
		t.Errorf("the caller's own capability slice was written to: %v", own)
	}
	// Classic says "queue" in the address, and fan-out says nothing at all:
	// the default configuration's links are what they were.
	for _, other := range []amq.Config{queueConfig(amq.Classic), artemisConfig()} {
		cc, err := other.CoreConfig()
		if err != nil {
			t.Fatal(err)
		}
		if len(cc.Publisher.TargetCapabilities) != 0 || len(cc.Consumer.SourceCapabilities) != 0 {
			t.Errorf("%v/%v: capabilities %v and %v, want none", other.Broker, other.Addressing,
				cc.Publisher.TargetCapabilities, cc.Consumer.SourceCapabilities)
		}
	}
}

// TestQueueAddressingDSN: the parameter round-trips, a DSN that names no mode
// renders as it always did, and an unknown mode is refused rather than read
// as fan-out.
func TestQueueAddressingDSN(t *testing.T) {
	cfg, err := amq.ParseDSN("activemq://localhost:5672?broker=artemis&addressing=queue")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	if cfg.Addressing != amq.AddressQueue {
		t.Fatalf("addressing %v, want queue", cfg.Addressing)
	}
	rendered := cfg.DSN()
	if !strings.Contains(rendered, "addressing=queue") {
		t.Fatalf("rendered %q carries no addressing", rendered)
	}
	back, err := amq.ParseDSN(rendered)
	if err != nil || back.Addressing != amq.AddressQueue {
		t.Fatalf("ParseDSN(%q) = (%v, %v), want queue addressing", rendered, back.Addressing, err)
	}
	if s := artemisConfig().DSN(); strings.Contains(s, "addressing") {
		t.Errorf("the default renders %q, want no addressing parameter", s)
	}
	if _, err := amq.ParseDSN("activemq://localhost:5672?broker=artemis&addressing=anycast"); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("an unknown mode gave %v, want CodeInvalidConfig", err)
	}
	bad := artemisConfig()
	bad.Addressing = amq.Addressing(9)
	if err := bad.Validate(); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("Validate of an out-of-range mode gave %v, want CodeInvalidConfig", err)
	}
}

// TestQueueAddressingClaimsNoFanOut: a JMS queue gives each message to one
// consumer, so the table must not tell a caller that two subscriptions each
// receive it, and the table is still complete.
func TestQueueAddressingClaimsNoFanOut(t *testing.T) {
	for _, broker := range []amq.Broker{amq.Artemis, amq.Classic} {
		t.Run(broker.String(), func(t *testing.T) {
			cfg := queueConfig(broker)
			caps, err := amq.CapabilitiesForTest(cfg, "", "")
			if err != nil {
				t.Fatal(err)
			}
			if caps.Has(mq.FeatureFanOut) {
				t.Errorf("fan-out is claimed: %+v", caps.Support(mq.FeatureFanOut))
			}
			if !caps.Has(mq.FeatureConsumerGroups) {
				t.Error("competing consumers are not claimed, and they are what a JMS queue is")
			}
			missing, err := amq.MissingFeaturesForTest(cfg)
			if err != nil || len(missing) != 0 {
				t.Errorf("missing %v (%v)", missing, err)
			}
		})
	}
}

// TestQueueAddressingCreatesAnAnycastQueue: Admin creates a durable ANYCAST
// queue named after the topic on an address of the same name, which is what a
// JMS queue is on Artemis.
func TestQueueAddressingCreatesAnAnycastQueue(t *testing.T) {
	body, err := amq.QueueConfigurationForTest(queueConfig(amq.Artemis), "ORDERS.IN", "ignored")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{
		"name": "ORDERS.IN", "address": "ORDERS.IN", "routing-type": "ANYCAST",
		"durable": true, "purge-on-no-consumers": false,
	} {
		if got[k] != want {
			t.Errorf("%s = %v, want %v", k, got[k], want)
		}
	}
}

// TestQueueAddressingRoundTrip publishes to and consumes from one JMS queue over
// the scripted peer: the publication goes to the queue's own address, and two
// consumers with different subscriptions attach to that same address, which
// is how the broker comes to make them compete.
func TestQueueAddressingRoundTrip(t *testing.T) {
	b, p := openPeer(t, queueConfig(amq.Artemis), artemisProps())
	snk := openSink(t, b, "ORDERS.IN", mq.ConfirmAccepted)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := snk.Publish(ctx, &mq.Message{Body: []byte("order-1")}, nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if sent := p.Sent("ORDERS.IN"); len(sent) != 1 || string(sent[0].GetData()) != "order-1" {
		t.Fatalf("the peer received %d message(s) on ORDERS.IN, want the one publication", len(sent))
	}

	// Each source is warmed as it opens, because one warm of both attaches
	// them in the backend's map order: the first could attach last, take the
	// delivery, and leave the second's fetch to time out.
	first := openSource(t, b, "ORDERS.IN", "billing", 4)
	warm(t, b)
	second := openSource(t, b, "ORDERS.IN", "audit", 4)
	warm(t, b)
	if n := p.Receivers("ORDERS.IN"); n != 2 {
		t.Fatalf("%d receivers on ORDERS.IN, want both consumers on the one queue", n)
	}
	if err := p.Deliver("ORDERS.IN", tagged("t1", "order-1")); err != nil {
		t.Fatal(err)
	}
	// The peer hands a delivery to the most recently attached link, so the
	// second consumer takes it, and the first one shares the queue.
	d := fetchOne(t, second, 5*time.Second)
	if string(d.Message.Body) != "order-1" {
		t.Errorf("body %q, want order-1", d.Message.Body)
	}
	_ = first
}

// TestQueueAddressingWithTheCoreAPI is the user-facing shape: the driver's own
// constants, a Config literal and the portable consumer, with no internal
// package in sight.
func TestQueueAddressingWithTheCoreAPI(t *testing.T) {
	cfg := amq.Config{Broker: amq.Artemis, Addrs: []string{"broker:5672"}, Addressing: amq.AddressQueue}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	p, err := cfg.Profile()
	if err != nil {
		t.Fatal(err)
	}
	src, err := p.SourceAddress("ORDERS.IN", "app")
	if err != nil || src != "ORDERS.IN" {
		t.Fatalf("SourceAddress = (%q, %v), want ORDERS.IN", src, err)
	}
}

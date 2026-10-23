package activemq

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestDeriveQueueFourShapes is TestDeriveFourShapes under AddressQueue: one JMS
// queue, on two products, over two protocols.
//
// The publish and consume destinations are the SAME on every row, which is
// what a JMS queue is: a producer and a consumer name one destination, rather
// than a topic on one side and a subscription's own queue on the other. The
// subscription is one Derive would refuse under fan-out (it has
// a "."), because under AddressQueue it names nothing and is never read.
func TestDeriveQueueFourShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		dest string
	}{
		{"artemis/amqp10", Config{Broker: Artemis, Topic: "ORDERS.IN", Subscription: "app.one", Scheme: true}, "ORDERS.IN"},
		{"artemis/stomp", Config{Broker: Artemis, Topic: "ORDERS.IN", Subscription: "app.one", STOMP: true}, "ORDERS.IN"},
		{"classic/amqp10", Config{Broker: Classic, Topic: "ORDERS.IN", Subscription: "app.one", Scheme: true}, "queue://ORDERS.IN"},
		{"classic/stomp", Config{Broker: Classic, Topic: "ORDERS.IN", Subscription: "app.one", STOMP: true}, "/queue/ORDERS.IN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Addressing = AddressQueue
			n, err := Derive(tc.cfg)
			if err != nil {
				t.Fatalf("Derive: %v", err)
			}
			if n.Publish != tc.dest || n.Consume != tc.dest {
				t.Errorf("publish %q consume %q, want both %q", n.Publish, n.Consume, tc.dest)
			}
			if n.Address != "ORDERS.IN" || n.Queue != "ORDERS.IN" {
				t.Errorf("address %q queue %q, want both the queue's own name: a JMS queue's address and queue share it",
					n.Address, n.Queue)
			}
			// A publish-only name set is the same queue: the sink derives
			// with no subscription and must still find it.
			tc.cfg.Subscription = ""
			pub, err := Derive(tc.cfg)
			if err != nil {
				t.Fatalf("Derive with no subscription: %v", err)
			}
			if pub != n {
				t.Errorf("the name set with no subscription is %+v, want %+v", pub, n)
			}
		})
	}
}

// TestDeriveQueuePrefixAndDeadLetter pins the two fields the mode reads besides
// the topic: the prefix goes in front of the queue's name on both products
// (there is no virtual topic for it to sit inside), and a dead-letter topic is
// another JMS queue named the same way.
func TestDeriveQueuePrefixAndDeadLetter(t *testing.T) {
	for _, tc := range []struct {
		broker     Broker
		dest, dead string
	}{
		{Artemis, "app.ORDERS.IN", "app.ORDERS.DLQ"},
		{Classic, "queue://app.ORDERS.IN", "queue://app.ORDERS.DLQ"},
	} {
		t.Run(tc.broker.String(), func(t *testing.T) {
			n, err := Derive(Config{
				Broker: tc.broker, Prefix: "app.", Topic: "ORDERS.IN", DeadLetterTopic: "ORDERS.DLQ",
				Scheme: true, Addressing: AddressQueue,
			})
			if err != nil {
				t.Fatalf("Derive: %v", err)
			}
			if n.Publish != tc.dest || n.Consume != tc.dest {
				t.Errorf("publish %q consume %q, want %q", n.Publish, n.Consume, tc.dest)
			}
			if n.DeadLetter != tc.dead {
				t.Errorf("dead letter %q, want %q", n.DeadLetter, tc.dead)
			}
		})
	}
}

// TestDeriveQueueRefusals: the queue's name is still checked, because it is
// still a destination a broker parses, and an addressing mode outside the
// enumeration is a configuration error rather than a silent fan-out.
func TestDeriveQueueRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want error
		code mq.ErrorCode
	}{
		{"queue slash", Config{Broker: Artemis, Topic: "jms/orders", Addressing: AddressQueue}, ErrNameSlash, mq.CodeInvalidArgument},
		{"queue wildcard", Config{Broker: Classic, Topic: "orders.>", Addressing: AddressQueue}, ErrTopicWildcard, mq.CodeUnsupported},
		{"no topic", Config{Broker: Artemis, Addressing: AddressQueue}, ErrNoTopic, mq.CodeInvalidArgument},
		{"no broker", Config{Topic: "orders", Addressing: AddressQueue}, ErrNoBroker, mq.CodeInvalidConfig},
		{"bad dead letter", Config{Broker: Artemis, Topic: "orders", DeadLetterTopic: "a:b", Addressing: AddressQueue}, ErrNameColon, mq.CodeInvalidArgument},
		{"bad addressing", Config{Broker: Artemis, Topic: "orders", Addressing: addressingCount}, ErrBadAddressing, mq.CodeInvalidConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Derive(tc.cfg)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Derive(%+v) = %v, want %v", tc.cfg, err, tc.want)
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Errorf("code %v, want %v", got, tc.code)
			}
		})
	}
}

// TestAddressingNames: the zero value is valid and is fan-out, so a
// configuration that names nothing keeps the mapping it always had.
func TestAddressingNames(t *testing.T) {
	var zero Addressing
	if zero != AddressFanOut || !zero.Valid() || zero.String() != "fanout" {
		t.Fatalf("the zero value is %v valid=%v, want the valid fanout mode", zero, zero.Valid())
	}
	for _, a := range []Addressing{AddressFanOut, AddressQueue} {
		got, ok := ParseAddressing(a.String())
		if !ok || got != a {
			t.Errorf("ParseAddressing(%q) = (%v, %v), want (%v, true)", a.String(), got, ok, a)
		}
	}
	if _, ok := ParseAddressing("anycast"); ok {
		t.Error("ParseAddressing accepted a mode this package does not name")
	}
	if a := addressingCount; a.Valid() || a.String() != "addressing(?)" {
		t.Errorf("a mode outside the enumeration reports valid=%v slug=%q", a.Valid(), a.String())
	}
}

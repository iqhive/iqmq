package activemq

import (
	"errors"
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestDeriveFourShapes is the whole point of this package as one table: one
// topic and one subscription, on two products, over two protocols, and no two
// of the four answers share a name.
//
// "ActiveMQ" is two products with two topic mappings, and the four rows below
// are what a driver that derived its names ad hoc would get wrong in a way
// nothing reports: on Classic a destination's own SHAPE is what tells the
// broker whether a name is a queue or a topic, so a publication with the wrong
// decoration goes to a QUEUE called `VirtualTopic.orders` that every consumer
// queue is subscribed to and none of them receives anything from.
func TestDeriveFourShapes(t *testing.T) {
	for _, tc := range []struct {
		name             string
		cfg              Config
		publish, consume string
		address, queue   string
	}{
		{
			// Artemis over AMQP 1.0: a bare multicast address, and a
			// consumer attaches by fully qualified queue name so that it
			// gets the shared durable subscription's OWN queue rather than
			// whatever the broker would give it.
			name:    "artemis/amqp10",
			cfg:     Config{Broker: Artemis, Topic: "orders", Subscription: "workers", Scheme: true},
			publish: "orders",
			consume: "orders::orders.workers",
			address: "orders",
			queue:   "orders.workers",
		},
		{
			// Artemis over STOMP: the SAME names. Artemis addresses are bare
			// on both protocols, which is worth pinning: it is why the
			// protocol package's wrong-form check can only bite on Classic.
			name:    "artemis/stomp",
			cfg:     Config{Broker: Artemis, Topic: "orders", Subscription: "workers", STOMP: true},
			publish: "orders",
			consume: "orders::orders.workers",
			address: "orders",
			queue:   "orders.workers",
		},
		{
			// Classic over AMQP 1.0: the virtual-topic convention, with the
			// `topic://` and `queue://` scheme forms.
			name:    "classic/amqp10",
			cfg:     Config{Broker: Classic, Topic: "orders", Subscription: "workers", Scheme: true},
			publish: "topic://VirtualTopic.orders",
			consume: "queue://Consumer.workers.VirtualTopic.orders",
			address: "VirtualTopic.orders",
			queue:   "Consumer.workers.VirtualTopic.orders",
		},
		{
			// Classic over STOMP: the same convention in STOMP's path form.
			name:    "classic/stomp",
			cfg:     Config{Broker: Classic, Topic: "orders", Subscription: "workers", STOMP: true},
			publish: "/topic/VirtualTopic.orders",
			consume: "/queue/Consumer.workers.VirtualTopic.orders",
			address: "VirtualTopic.orders",
			queue:   "Consumer.workers.VirtualTopic.orders",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := Derive(tc.cfg)
			if err != nil {
				t.Fatalf("Derive: %v", err)
			}
			if n.Publish != tc.publish {
				t.Errorf("Publish = %q, want %q", n.Publish, tc.publish)
			}
			if n.Consume != tc.consume {
				t.Errorf("Consume = %q, want %q", n.Consume, tc.consume)
			}
			if n.Address != tc.address {
				t.Errorf("Address = %q, want %q", n.Address, tc.address)
			}
			if n.Queue != tc.queue {
				t.Errorf("Queue = %q, want %q", n.Queue, tc.queue)
			}
			// And a publish-only name set has no consumer, rather than a
			// consumer of some default queue.
			only := tc.cfg
			only.Subscription = ""
			bare, err := Derive(only)
			if err != nil {
				t.Fatalf("Derive without a subscription: %v", err)
			}
			if bare.Consume != "" || bare.Queue != "" {
				t.Errorf("a publish-only name set names the queue %q and the source %q", bare.Queue, bare.Consume)
			}
			if bare.Publish != tc.publish {
				t.Errorf("a publish-only name set publishes to %q, want %q", bare.Publish, tc.publish)
			}
		})
	}
}

// TestDerivePrefix is the one ordering decision in the Classic mapping that
// cannot be read off the convention, and it was settled by a broker rather
// than by reasoning.
//
// The prefix goes AFTER the virtual-topic prefix
// (`VirtualTopic.<prefix><topic>`), because Classic's default
// virtual-destination interceptor matches `VirtualTopic.>`: a destination
// called `iqmq.VirtualTopic.orders` is not a virtual topic at all. It is an
// ordinary topic, every publication to it is accepted, and no consumer queue
// receives anything -- with no error on either side.
//
// Measured against apache/activemq-classic:6.2.0, publishing to `topic://<X>`
// with a consumer on `queue://Consumer.A.<X>`:
//
//	iqmq.VirtualTopic.p   NOT DELIVERED
//	VirtualTopic.iqmq.q   delivered
//	VirtualTopic.r        delivered
//
// The other ordering was this package's first draft -- with a doc comment
// confidently asserting the opposite -- and the conformance suite is what
// found it: every Classic scenario timed out waiting for a delivery.
func TestDerivePrefix(t *testing.T) {
	n, err := Derive(Config{Broker: Classic, Prefix: "iqmq.", Topic: "orders", Subscription: "workers", Scheme: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := "VirtualTopic.iqmq.orders"; n.Address != want {
		t.Errorf("the Classic address is %q, want %q: the prefix goes INSIDE VirtualTopic. or the broker's default "+
			"VirtualTopic.> pattern does not match it and the fan-out stops silently", n.Address, want)
	}
	if want := "Consumer.workers.VirtualTopic.iqmq.orders"; n.Queue != want {
		t.Errorf("the Classic consumer queue is %q, want %q", n.Queue, want)
	}
	a, err := Derive(Config{Broker: Artemis, Prefix: "iqmq.", Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Address != "iqmq.orders" || a.Queue != "iqmq.orders.workers" {
		t.Errorf("the Artemis names are (%q, %q), want (%q, %q)", a.Address, a.Queue, "iqmq.orders", "iqmq.orders.workers")
	}
	// The Artemis queue name carries the TOPIC as well as the subscription,
	// and that is not decoration: an Artemis queue name is unique across the
	// whole broker, so two topics with a subscription called "workers" would
	// collide.
	other, err := Derive(Config{Broker: Artemis, Prefix: "iqmq.", Topic: "invoices", Subscription: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	if other.Queue == a.Queue {
		t.Errorf("two topics with the subscription \"workers\" both map to the queue %q: an Artemis queue name is unique across the whole broker, so the second createQueue is refused with AMQ229019 and a consumer that attached by FQQN would be refused with amqp:not-found while the queue it names exists on another address", a.Queue)
	}
}

// TestDeriveDeadLetter is the dead-letter topic's own name set, and the queue
// it deliberately does NOT declare.
func TestDeriveDeadLetter(t *testing.T) {
	for _, b := range Brokers() {
		n, err := Derive(Config{Broker: b, Topic: "orders", Subscription: "workers", DeadLetterTopic: "orders.dlq", Scheme: true})
		if err != nil {
			t.Fatalf("%v: %v", b, err)
		}
		dl, err := Derive(Config{Broker: b, Topic: "orders.dlq", Scheme: true})
		if err != nil {
			t.Fatal(err)
		}
		if n.DeadLetter != dl.Publish {
			t.Errorf("%v: the dead-letter destination is %q, want the dead-letter topic's own publish destination %q", b, n.DeadLetter, dl.Publish)
		}
		// The dead-letter destination belongs to ANOTHER topic, so its own
		// subscriptions declare their own queues. Declaring one here would
		// create a queue nothing consumes and dead-letter into it.
		if n.DeadLetterQueue != "" {
			t.Errorf("%v: a dead-letter queue %q was derived; the dead-letter topic's subscribers declare their own", b, n.DeadLetterQueue)
		}
		// A bad dead-letter topic is refused rather than producing a name
		// set with a working publish destination and a broken fallback.
		if _, err := Derive(Config{Broker: b, Topic: "orders", DeadLetterTopic: "a/b"}); err == nil {
			t.Errorf("%v: a dead-letter topic containing \"/\" was accepted", b)
		}
	}
}

// TestDeriveRefusals is every name a driver must not be allowed to build, with
// the reason the refusal is worth the code.
//
// A refusal at the broker costs more here than tidiness: on Artemis an attach
// to a name the broker will not accept detaches the LINK with amqp:not-found
// and takes the session with it, and on Classic a bad destination is ACCEPTED
// and created, so the caller's typo becomes a queue nothing consumes and a
// publication nobody receives. Neither failure names the typo.
func TestDeriveRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want error
		code mq.ErrorCode
	}{
		{"no broker", Config{Topic: "orders", Broker: Broker(9)}, ErrNoBroker, mq.CodeInvalidConfig},
		{"no topic", Config{Broker: Artemis}, ErrNoTopic, mq.CodeInvalidArgument},
		{"topic wildcard #", Config{Broker: Artemis, Topic: "orders.#"}, ErrTopicWildcard, mq.CodeUnsupported},
		{"topic wildcard *", Config{Broker: Artemis, Topic: "orders.*"}, ErrTopicWildcard, mq.CodeUnsupported},
		{"topic wildcard >", Config{Broker: Classic, Topic: "orders.>"}, ErrTopicWildcard, mq.CodeUnsupported},
		{"subscription wildcard", Config{Broker: Artemis, Topic: "orders", Subscription: "a*"}, ErrSubscriptionWildcard, mq.CodeUnsupported},
		{"subscription dot", Config{Broker: Classic, Topic: "orders", Subscription: "a.b"}, ErrSubscriptionDot, mq.CodeInvalidArgument},
		{"topic fqqn", Config{Broker: Artemis, Topic: "a::b"}, ErrNameFQQN, mq.CodeInvalidArgument},
		{"topic slash", Config{Broker: Artemis, Topic: "a/b"}, ErrNameSlash, mq.CodeInvalidArgument},
		{"topic comma", Config{Broker: Artemis, Topic: "a,b"}, ErrNameComma, mq.CodeInvalidArgument},
		{"topic newline", Config{Broker: Artemis, Topic: "a\nb"}, ErrNameControl, mq.CodeInvalidArgument},
		{"topic nul", Config{Broker: Artemis, Topic: "a\x00b"}, ErrNameControl, mq.CodeInvalidArgument},
		{"topic space", Config{Broker: Artemis, Topic: "a b"}, ErrNameSpace, mq.CodeInvalidArgument},
		{"topic tab", Config{Broker: Artemis, Topic: "a\tb"}, ErrNameSpace, mq.CodeInvalidArgument},
		{"topic colon", Config{Broker: Artemis, Topic: "a:b"}, ErrNameColon, mq.CodeInvalidArgument},
		{"topic too long", Config{Broker: Artemis, Topic: strings.Repeat("t", MaxNameLen+1)}, ErrNameTooLong, mq.CodeInvalidArgument},
		{"prefix slash", Config{Broker: Artemis, Prefix: "a/", Topic: "orders"}, ErrNameSlash, mq.CodeInvalidArgument},
		{"subscription colon", Config{Broker: Artemis, Topic: "orders", Subscription: "a:b"}, ErrNameColon, mq.CodeInvalidArgument},
		{"empty subscription is not a refusal", Config{Broker: Artemis, Topic: "orders"}, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Derive(tc.cfg)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Derive refused %+v: %v", tc.cfg, err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Derive = %v, want %v", err, tc.want)
			}
			if code := mq.CodeOf(err); code != tc.code {
				t.Errorf("the refusal's code is %v, want %v: a wildcard is Unsupported because the caller's request is well formed and this broker cannot serve it, while a control character is InvalidArgument because the name itself is wrong", code, tc.code)
			}
			// The refusal says which name it was about, which is the whole
			// reason NameError exists: three names are derived from one
			// config and "invalid name" would not say which.
			var ne *NameError
			if errors.As(err, &ne) {
				if ne.What == "" {
					t.Error("a NameError names nothing")
				}
				if !strings.Contains(ne.Error(), ne.What) {
					t.Errorf("the message %q does not name %q", ne.Error(), ne.What)
				}
			}
		})
	}
	// A derived name that is long enough to breach the ceiling ONLY after the
	// prefix and the convention are applied is refused too, which is the case
	// a check on the caller's input alone would miss: Classic's consumer
	// queue is `<prefix>Consumer.<sub>.VirtualTopic.<topic>`, so a topic well
	// under the ceiling can still produce a queue over it.
	long := strings.Repeat("t", MaxNameLen-20)
	if _, err := Derive(Config{Broker: Classic, Topic: long, Subscription: "workers"}); !errors.Is(err, ErrNameTooLong) {
		t.Errorf("a %d-byte topic produced a Classic consumer queue of %d bytes and was accepted: %v",
			len(long), len("Consumer.workers."+VirtualTopicPrefix)+len(long), err)
	}
}

// TestParseQueueIsDeriveInverse is the property Admin rests on: a driver that
// lists a broker's queues has to say which of them are its own, and the only
// way to know is to be able to take one apart.
func TestParseQueueIsDeriveInverse(t *testing.T) {
	for _, b := range Brokers() {
		for _, prefix := range []string{"", "iqmq.", "a-"} {
			for _, topic := range []string{"orders", "orders.eu", "a", "a.b.c"} {
				for _, sub := range []string{"workers", "w", "workers-2"} {
					n, err := Derive(Config{Broker: b, Prefix: prefix, Topic: topic, Subscription: sub})
					if err != nil {
						t.Fatalf("%v %q %q %q: %v", b, prefix, topic, sub, err)
					}
					gotTopic, gotSub, ok := ParseQueue(b, prefix, n.Queue)
					if !ok {
						t.Errorf("%v: ParseQueue(%q, %q) did not recognise a queue Derive produced", b, prefix, n.Queue)
						continue
					}
					if gotTopic != topic || gotSub != sub {
						t.Errorf("%v: ParseQueue(%q, %q) = (%q, %q), want (%q, %q)", b, prefix, n.Queue, gotTopic, gotSub, topic, sub)
					}
				}
			}
		}
	}
	// And a queue this package would NOT have produced is not claimed. That
	// is the half that matters for Admin: claiming somebody else's queue is
	// how a driver deletes a topology it does not own.
	for _, tc := range []struct {
		broker Broker
		prefix string
		name   string
	}{
		{Artemis, "iqmq.", "other.orders.workers"},
		{Artemis, "", "noseparator"},
		{Artemis, "", ".leading"},
		{Artemis, "", "trailing."},
		{Classic, "", "orders.workers"},
		{Classic, "", "Consumer.workers.orders"},
		{Classic, "", "Consumer.a.b.VirtualTopic.orders"},
		{Classic, "", "Consumer.workers.VirtualTopic."},
		{Classic, "iqmq.", "Consumer.workers.VirtualTopic.orders"},
		{Broker(9), "", "anything"},
	} {
		if _, _, ok := ParseQueue(tc.broker, tc.prefix, tc.name); ok {
			t.Errorf("ParseQueue(%v, %q, %q) claimed a queue this package would not have produced", tc.broker, tc.prefix, tc.name)
		}
	}
}

// TestBrokerNames holds the broker enumeration to its slugs, which a DSN, a
// benchmark name and a scorecard all read.
func TestBrokerNames(t *testing.T) {
	seen := map[string]Broker{}
	for _, b := range Brokers() {
		slug := b.String()
		if slug == "" || !b.Valid() {
			t.Fatalf("broker %d has slug %q valid=%v", b, slug, b.Valid())
		}
		if prev, dup := seen[slug]; dup {
			t.Fatalf("brokers %v and %v share the slug %q", prev, b, slug)
		}
		seen[slug] = b
		got, ok := ParseBroker(slug)
		if !ok || got != b {
			t.Fatalf("ParseBroker(%q) = (%v, %v), want (%v, true)", slug, got, ok, b)
		}
	}
	if len(seen) != int(brokerCount)-1 {
		t.Fatalf("Brokers() listed %d of %d, counting BrokerUnset out", len(seen), int(brokerCount)-1)
	}
	// The zero value names nothing, which is what makes the required field
	// enforceable rather than merely documented.
	if BrokerUnset.Valid() || BrokerUnset.String() != "broker(?)" {
		t.Errorf("the zero value reports valid=%v slug=%q; a configuration that omits the broker must be "+
			"refused rather than silently mapped the Artemis way", BrokerUnset.Valid(), BrokerUnset.String())
	}
	if _, ok := ParseBroker(""); ok {
		t.Error("ParseBroker accepted the empty slug, which is BrokerUnset's")
	}
	if _, ok := ParseBroker("rabbitmq"); ok {
		t.Error("ParseBroker accepted a broker this package does not name")
	}
	// There is deliberately no Auto: nothing on the wire says which product
	// a caller MEANT, and a driver that guessed would auto-create the wrong
	// topology on Artemis and publish into it for ever.
	if _, ok := ParseBroker("auto"); ok {
		t.Error("ParseBroker accepted \"auto\": the two products' topic mappings share no name, so guessing is what CheckProduct exists to stop")
	}
	if b := brokerCount; b.Valid() || b.String() != "broker(?)" {
		t.Errorf("a broker outside the enumeration reports valid=%v slug=%q", b.Valid(), b.String())
	}
}

// TestHasWildcard covers both brokers' wildcard alphabets, which is what makes
// the drivers' negative FeatureWildcardSubscribe claim provable rather than
// merely stated.
func TestHasWildcard(t *testing.T) {
	for _, s := range []string{"a.#", "a.*", "a.>", "#", "*", ">", "a#b"} {
		if !HasWildcard(s) {
			t.Errorf("HasWildcard(%q) = false: Artemis uses # and *, Classic uses > and *", s)
		}
	}
	for _, s := range []string{"", "orders", "orders.eu", "a-b_c", "a.b"} {
		if HasWildcard(s) {
			t.Errorf("HasWildcard(%q) = true", s)
		}
	}
}

package amqp10_test

import (
	"fmt"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "bitbucket.org/iqhive/iqmq"
	aq "bitbucket.org/iqhive/iqmq/drivers/amqp10"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// A profile for Apache ActiveMQ Artemis, which is the shape a broker-specific
// module built on this core supplies.
//
// It is the whole of what Artemis needs that AMQP 1.0 does not decide: the
// fully qualified queue name of a shared durable subscription, the annotation
// that delays a publication, the property that de-duplicates one, the durable
// boundary, and the claims about the broker that only somebody who knows
// Artemis can make.
func Example_profile() {
	profile := aq.Profile{
		Driver:  "activemq",
		Name:    "artemis",
		Version: "v0.1.0",

		// A topic is a multicast address.
		SinkAddress: func(topic string) (string, error) { return topic, nil },
		// A subscription is a shared durable subscription on that address,
		// named by the fully qualified queue name. The rule follows from
		// it: two consumers of ONE subscription attach to one queue and
		// compete, and two subscriptions are two queues and each receives
		// everything.
		SourceAddress: func(topic, sub string) (string, error) {
			return topic + "::" + sub, nil
		},

		// Artemis reads a delivery delay from a message annotation, in
		// milliseconds.
		Delay: aq.DelayRule{
			Slot: aq.SlotMessageAnnotation,
			Key:  "x-opt-delivery-delay",
			Max:  30 * 24 * time.Hour,
		},
		// A delayed redelivery is the `modified` outcome carrying the same
		// annotation, with the attempt counted.
		NackDelay: aq.NackDelayRule{
			Supported:      true,
			Slot:           aq.SlotMessageAnnotation,
			Key:            "x-opt-delivery-delay",
			DeliveryFailed: true,
		},
		// De-duplication is an application property.
		Dedup: aq.SlotRule{Slot: aq.SlotApplicationProperty, Key: "_AMQ_DUPL_ID"},
		// The durable boundary, which the specification requires every driver to state.
		Durable: aq.DurableRule{
			Supported: true,
			Note:      "the peer's `accepted` disposition for a durable message on a durable address, which Artemis writes to its journal before it accepts",
		},
		// Artemis's own annotations are x-opt-*, so a header under that
		// prefix becomes a message annotation rather than an application
		// property — which is how a broker-specific control stays reachable
		// from the portable API.
		AnnotationPrefix: "x-opt-",
		AutoCreates:      true,

		Capabilities: func(aq.Facts) []aq.Claim {
			return []aq.Claim{
				aq.Native(mq.FeatureDurableStorage, mq.ScopeTopic),
				aq.Native(mq.FeatureDeadLetter, mq.ScopeTopic),
				aq.Native(mq.FeatureConsumerGroups, mq.ScopeGroup),
				aq.Native(mq.FeatureDurableSubscriptions, mq.ScopeGroup),
				aq.Native(mq.FeatureFanOut, mq.ScopeTopic),
				aq.Conditional(mq.FeatureOrderedByKey, mq.ScopeKey,
					"Message.Key travels as the AMQP group-id property, which Artemis orders as a message group"),
			}
		},
	}
	if err := profile.Validate(); err != nil {
		fmt.Println("the profile is unusable:", err)
		return
	}
	sink, _ := profile.SinkAddress("orders")
	source, _ := profile.SourceAddress("orders", "workers")
	fmt.Println("publish to:", sink)
	fmt.Println("consume from:", source)
	// Output:
	// publish to: orders
	// consume from: orders::workers
}

// Opening a client. The profile is the broker's and the configuration is the
// deployment's; nothing else is needed.
func Example_open() {
	profile := aq.Profile{
		Driver:        "activemq",
		Name:          "artemis",
		SinkAddress:   func(topic string) (string, error) { return topic, nil },
		SourceAddress: func(topic, sub string) (string, error) { return topic + "::" + sub, nil },
	}
	cfg := aq.Config{
		Addrs:    []string{"amqps://broker.example:5671"},
		Username: "orders",
		Password: dsn.Secret("s3cr3t"),
		SASL:     aq.SASLPlain,
		// A settlement that costs a round trip and can therefore report an
		// UNKNOWN outcome rather than a lost lease. The default is `first`,
		// which costs nothing and reports a lost lease instead; see
		// [aq.ReceiverSettleMode].
		ReceiverSettle: aq.ReceiverSettleSecond,
	}
	if err := profile.Validate(); err != nil {
		fmt.Println("the profile is unusable:", err)
		return
	}
	if err := cfg.Validate(); err != nil {
		fmt.Println("the configuration is unusable:", err)
		return
	}
	// A configuration renders with the credential removed, so it is safe to
	// log and safe to put in an error.
	//
	// The rendering ends in `tls=true` although this Config sets no
	// *tls.Config: the TLS is in the ADDRESS SCHEME ("amqps://"), which
	// go-amqp dials on, and the DSN carries it as the parameter. Before
	// The specification the scheme was dropped and the parameter was not
	// written, so this very line was a DSN that ParseDSN refused -- SASL
	// PLAIN without TLS.
	fmt.Println(cfg)
	// Output:
	// amqp10://orders:redacted@broker.example:5671?sasl=plain&sendersettle=mixed&receiversettle=second&backoff=fulljitter&idletimeout=1m0s&maxframesize=65536&dialtimeout=10s&operationtimeout=30s&writetimeout=30s&reconnectbase=200ms&reconnectmax=30s&reconnectgrowth=2&maxpending=1000&tls=true
}

// A profile that refuses a broker it does not support. RabbitMQ's AMQP 1.0
// endpoint exists from 4.0 and its 3.13 plugin is a different protocol, so the
// gate belongs to the RabbitMQ profile rather than to this core.
func Example_brokerGate() {
	profile := aq.Profile{
		Driver:        "rabbitmq",
		Name:          "amqp10",
		SinkAddress:   func(topic string) (string, error) { return "/exchanges/amq.direct/" + topic, nil },
		SourceAddress: func(topic, sub string) (string, error) { return "/queues/" + topic + "." + sub, nil },
		CheckBroker: func(f aq.Facts) error {
			if f.Product == "rabbitmq" && f.Version < "4." {
				return fmt.Errorf("this driver needs RabbitMQ 4.0 or later; the peer says %q", f.Version)
			}
			return nil
		},
	}
	for _, facts := range []aq.Facts{
		{Product: "rabbitmq", Version: "4.3.5"},
		{Product: "rabbitmq", Version: "3.13.7"},
	} {
		if err := profile.CheckBroker(facts); err != nil {
			fmt.Println("refused:", err)
			continue
		}
		fmt.Println("accepted:", facts.Version)
	}
	// Output:
	// accepted: 4.3.5
	// refused: this driver needs RabbitMQ 4.0 or later; the peer says "3.13.7"
}

// The capability ceiling: a profile cannot claim a feature this core has no
// code for, and the refusal happens when the client is opened rather than being
// published in the capability matrix.
func Example_coreCeiling() {
	for _, f := range []mq.Feature{
		mq.FeatureDeadLetter,
		mq.FeatureExtendLease,
		mq.FeatureBatchSettle,
	} {
		fmt.Printf("%-12s claimable up to: %v\n", f, aq.CoreCeiling(f))
	}
	// Output:
	// DeadLetter   claimable up to: native
	// ExtendLease  claimable up to: unsupported
	// BatchSettle  claimable up to: unsupported
}

// The reserved header names, which are what make a receive-then-publish round
// trip lossless: a message this driver decoded carries them, and the specification refuses
// caller-supplied keys under its reserved prefix — so the mapping consumes
// exactly these back into the AMQP property they stand for.
func Example_reservedHeaders() {
	for _, key := range []string{
		aq.HeaderSubject,
		aq.HeaderTo,
		mq.ReservedHeaderPrefix + "something-invented",
		"x-opt-broker-thing",
	} {
		fmt.Printf("%-32s mapped by this driver: %v\n", key, aq.ReservedHeader(key))
	}
	// Output:
	// iqmq-amqp-subject                mapped by this driver: true
	// iqmq-amqp-to                     mapped by this driver: true
	// iqmq-something-invented          mapped by this driver: false
	// x-opt-broker-thing               mapped by this driver: false
}

// The AMQP condition behind a failure, which is the only thing an AMQP 1.0 peer
// really tells a client about a refusal — and which arrives in one of three
// wrappers a caller should not have to know about.
func Example_condition() {
	for _, tc := range []struct {
		what string
		err  error
	}{
		{"a rejection", &amqp.Error{Condition: amqp.ErrCondNotFound}},
		{"a forced detach", &amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondMessageSizeExceeded}}},
		{"a connection that just closed", &amqp.ConnError{}},
	} {
		cond, ok := aq.Condition(tc.err)
		fmt.Printf("%-30s %-38q said: %v\n", tc.what, cond, ok)
	}
	// Output:
	// a rejection                    "amqp:not-found"                       said: true
	// a forced detach                "amqp:link:message-size-exceeded"      said: true
	// a connection that just closed  ""                                     said: false
}

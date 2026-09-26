package amqp10_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	rmq10 "github.com/iqhive/iqmq/drivers/rabbitmq/amqp10"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
)

// TestVersionGateRefusesAnOldBroker is this module's headline behaviour and the specification's "never silently substituted" made into a test.
//
// A broker that reports 3.13 has an AMQP 1.0 PLUGIN whose feature set, address
// syntax and settlement behaviour differ from RabbitMQ 4's native endpoint, so
// opening against it must fail rather than succeed with different semantics
// under one name. Three things have to be true of the refusal and each is a
// different way for a gate to be useless:
//
//   - it is [mq.ErrUnsupported], not a configuration error — the broker is real
//     and reachable and does not have the feature;
//   - it NAMES BOTH versions, because "unsupported" without the numbers leaves
//     an operator guessing which of the two ends to change;
//   - no client comes back. A driver that returned a working client over AMQP
//     0-9-1 would be the silent substitution the specification forbids, and one that
//     returned a client at all would leave the caller publishing into a
//     protocol it did not ask for.
func TestVersionGateRefusesAnOldBroker(t *testing.T) {
	p := newPeer(t, rabbitProps("3.13.7"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := rmq10.Open(ctx, testConfig(p))
	if client != nil {
		_ = client.Close()
		t.Fatal("Open returned a client for a RabbitMQ 3.13 broker; the driver documentation says the AMQP 1.0 profile is " +
			"never silently substituted, so there is no client to hand back and nothing falls back to AMQP 0-9-1")
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Open against RabbitMQ 3.13 = %v, want ErrUnsupported: the broker is reachable and does not have "+
			"the feature, which the specification makes Unsupported rather than a configuration error", err)
	}
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("the refusal carries code %v, want CodeUnsupported", got)
	}
	msg := err.Error()
	for _, want := range [...]string{"3.13.7", "4.0.0"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not name %q: %q\n"+
				"a version gate that does not say which version it found and which it needs leaves an operator "+
				"guessing which end to change", want, msg)
		}
	}
}

// TestVersionGateAcceptsRabbitMQ4 is the other direction, and it is the half a
// gate written by hand usually gets wrong: a gate that refused everything would
// pass the test above and be useless.
//
// 4.0.0 is the boundary itself and 4.1.8-rc.1 is a release candidate, which is
// the shape `rabbit.ParseVersion` is deliberately tolerant of — a driver that
// refused an unexpected suffix would refuse to connect to a pre-release the
// operator is deliberately testing against.
func TestVersionGateAcceptsRabbitMQ4(t *testing.T) {
	for _, version := range [...]string{"4.0.0", "4.1.8-rc.1", "4.2.9 (Erlang 27)", "5.0.0"} {
		t.Run(version, func(t *testing.T) {
			p := newPeer(t, rabbitProps(version))
			client := openClient(t, testConfig(p))
			info := client.Info()
			if info.Name != "rabbitmq" || info.Profile != "amqp10" {
				t.Errorf("DriverInfo = %q/%q, want rabbitmq/amqp10: one broker, one driver name, two profiles, "+
					"which is what makes a report row readable against the protocol that produced it",
					info.Name, info.Profile)
			}
			if info.BrokerVersion != version {
				t.Errorf("DriverInfo.BrokerVersion = %q, want %q: the specification makes it the peer's own report and never a guess",
					info.BrokerVersion, version)
			}
		})
	}
}

// TestVersionGateRefusesASilentBroker holds the gate to the rule that keeps it
// from being open by omission.
//
// A peer that sent no `version` connection property has not said it is
// RabbitMQ 4; the specification makes "the peer did not say" distinct from any claim, and a
// driver that read an unknown version as new enough would gate nothing at all.
func TestVersionGateRefusesASilentBroker(t *testing.T) {
	for name, props := range map[string]map[string]any{
		"no properties at all": nil,
		"a product and no version": {
			"product": "RabbitMQ",
		},
		"a version that is not one": {
			"product": "RabbitMQ",
			"version": "the-best-one",
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := newPeer(t, props)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client, err := rmq10.Open(ctx, testConfig(p))
			if client != nil {
				_ = client.Close()
				t.Fatal("Open returned a client for a peer that never said it was RabbitMQ 4")
			}
			if !errors.Is(err, mq.ErrUnsupported) {
				t.Fatalf("Open = %v, want ErrUnsupported", err)
			}
			if !strings.Contains(err.Error(), "did not report its version") {
				t.Errorf("the refusal is %q; it should say the broker did not report its version, because that is a "+
					"different thing for an operator to fix than a broker that is too old", err.Error())
			}
		})
	}
}

// TestProductGateRefusesAnotherBroker is the version gate read backwards, and
// it exists because the version gate alone cannot do this job.
//
// Apache Artemis reports version 2.44, which satisfies no RabbitMQ gate; but it
// also AUTO-CREATES an address from an attach, so a client pointed at the wrong
// endpoint would not fail — it would create an address literally called
// "/exchanges/orders/orders" and publish into it for ever. Refusing a named
// product that is not RabbitMQ's is the specification's "never silently substituted" applied
// to the broker rather than to the protocol.
func TestProductGateRefusesAnotherBroker(t *testing.T) {
	props := map[string]any{"product": "apache-activemq-artemis", "version": "2.44.0"}
	p := newPeer(t, props)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := rmq10.Open(ctx, testConfig(p))
	if client != nil {
		_ = client.Close()
		t.Fatal("Open returned a client for an Apache Artemis peer; this profile's addressing means something else " +
			"on every other AMQP 1.0 broker")
	}
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("Open against Artemis = %v, want ErrUnsupported", err)
	}
	if !strings.Contains(err.Error(), "apache-activemq-artemis") {
		t.Errorf("the refusal does not name what the peer said it was: %q", err.Error())
	}
}

// TestProductGateAcceptsAPeerThatSaysNothingAboutItself is the deliberate hole
// in the product gate.
//
// An AMQP 1.0 proxy in front of RabbitMQ need not forward the broker's
// connection properties, so refusing an unnamed product would make this driver
// refuse to work behind a hop rather than because of the broker. Such a peer
// still has to satisfy the VERSION gate, which it cannot do silently — which is
// why the version it reports here is 4.0.0 and the test above, with no version,
// is refused.
func TestProductGateAcceptsAPeerThatSaysNothingAboutItself(t *testing.T) {
	if err := rmq10.CheckBrokerForTest("", "4.0.0"); err != nil {
		t.Fatalf("a peer that named no product and reported 4.0.0 was refused: %v", err)
	}
	if err := rmq10.CheckBrokerForTest("", "3.13.7"); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a peer that named no product and reported 3.13.7 = %v, want ErrUnsupported", err)
	}
}

// TestCheckBrokerTable drives the gate across the versions a container matrix
// would need one image each for.
//
// The gate is a pure function of what the peer said, which is why it can be
// tabled at all: [rmq10.CheckBrokerForTest] is the very function
// [rmq10.Config.Profile] installs as the core's CheckBroker, not a copy of it.
func TestCheckBrokerTable(t *testing.T) {
	for _, tc := range []struct {
		product string
		version string
		ok      bool
	}{
		{"RabbitMQ", "4.0.0", true},
		{"rabbitmq", "4.0.0", true},
		{"RABBITMQ", "4.0.1", true},
		{"RabbitMQ", "4.13.0", true},
		{"RabbitMQ", "10.0.0", true},
		{"RabbitMQ", "3.13.7", false},
		{"RabbitMQ", "3.99.99", false},
		{"RabbitMQ", "", false},
		{"RabbitMQ", "0.0.0", false},
		{"not-rabbitmq", "4.0.0", false},
		{"rabbitmq-proxy", "4.0.0", false},
		{"apache-activemq-artemis", "2.44.0", false},
		{"", "4.0.0", true},
	} {
		err := rmq10.CheckBrokerForTest(tc.product, tc.version)
		if tc.ok && err != nil {
			t.Errorf("CheckBroker(%q, %q) = %v, want nil", tc.product, tc.version, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("CheckBroker(%q, %q) = nil, want a refusal", tc.product, tc.version)
		}
		if !tc.ok && err != nil && !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("CheckBroker(%q, %q) = %v, want it to reach ErrUnsupported", tc.product, tc.version, err)
		}
	}
}

// TestAddressForm holds the addresses to RabbitMQ 4's own path format.
//
// The format is the authority's: `internal/rabbit/protocol` owns it for both
// arms, and what this test checks is that this profile asks for
// the right role and passes the right names — a sink that addressed a queue, or
// a source that addressed an exchange, would attach successfully to the wrong
// node and publish or consume nothing.
func TestAddressForm(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		topic  string
		sub    string
		sink   string
		source string
	}{
		{"the plain case", "", "orders", "workers", "/exchanges/orders/orders", "/queues/orders.workers"},
		{"a dotted topic", "", "orders.eu", "workers", "/exchanges/orders.eu/orders.eu", "/queues/orders.eu.workers"},
		{"a prefix", "acme.", "orders", "workers", "/exchanges/acme.orders/orders", "/queues/acme.orders.workers"},
		{"a slash in the topic", "", "orders/eu", "workers", "/exchanges/orders%2Feu/orders%2Feu", "/queues/orders%2Feu.workers"},
		{"a percent in the topic", "", "100%", "workers", "/exchanges/100%25/100%25", "/queues/100%25.workers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}, Prefix: tc.prefix}
			sink, source, err := rmq10.AddressForTest(cfg, tc.topic, tc.sub)
			if err != nil {
				t.Fatalf("deriving the addresses: %v", err)
			}
			if sink != tc.sink {
				t.Errorf("the sink address is %q, want %q", sink, tc.sink)
			}
			if source != tc.source {
				t.Errorf("the source address is %q, want %q", source, tc.source)
			}
		})
	}
}

// TestAddressEscapingCannotAddressAnotherNode is the address form's one
// security-shaped property.
//
// RabbitMQ 4's address format is PATH-shaped and a routing key is arbitrary
// caller-supplied text, so an unescaped "/" in a name would end a segment and
// address a different node — a publication to the topic "orders/eu" would
// otherwise reach the exchange "orders" with the routing key "eu". The escaping
// is `internal/rabbit/protocol`'s; what this test holds is that no address this
// profile produces has more segments than the form has.
func TestAddressEscapingCannotAddressAnotherNode(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	for _, topic := range [...]string{"orders/eu", "a/b/c/d", "/", "%2F", "%", "a%2Fb", "..", "orders/"} {
		sink, source, err := rmq10.AddressForTest(cfg, topic, "workers")
		if err != nil {
			// A name this broker cannot carry is refused, which is also an
			// acceptable answer: what must not happen is an address with a
			// segment the caller did not ask for.
			continue
		}
		if n := strings.Count(sink, "/"); n != 3 {
			t.Errorf("the sink address for topic %q is %q with %d separators; the form is "+
				"/exchanges/<exchange>/<routing-key> and has exactly three, so this address names a different node",
				topic, sink, n)
		}
		if n := strings.Count(source, "/"); n != 2 {
			t.Errorf("the source address for topic %q is %q with %d separators; the form is /queues/<queue> and has "+
				"exactly two", topic, source, n)
		}
	}
}

// TestNamingAgreesWithTheOtherProfile is what keeps the two RabbitMQ drivers
// from being two topologies wearing one name.
//
// The specification gives both profiles one topology — a durable exchange, a durable quorum
// queue `<topic>.<subscription>`, a dead-letter route derived from the queue's
// own name — and a caller who moves between them has to find it. So the names
// are derived once, by `internal/rabbit`, and what this test checks is that the
// AMQP 1.0 ADDRESS is built from exactly those names: the two arms of family
// The specification are asked for the same destination and their 0-9-1 fields have to match,
// which is the only way to compare a path address with an exchange-and-key
// pair.
func TestNamingAgreesWithTheOtherProfile(t *testing.T) {
	older, err := protocol.New(protocol.AMQP091)
	if err != nil {
		t.Fatalf("building the AMQP 0-9-1 arm: %v", err)
	}
	newer, err := protocol.New(protocol.AMQP10)
	if err != nil {
		t.Fatalf("building the AMQP 1.0 arm: %v", err)
	}
	for _, topic := range [...]string{"orders", "orders.eu", "a-b_c"} {
		names, err := rabbit.Derive(rabbit.Config{Topic: topic, Subscription: "workers"})
		if err != nil {
			t.Fatalf("deriving the names for %q: %v", topic, err)
		}
		for _, role := range [...]protocol.Role{protocol.Publish, protocol.Consume} {
			a091, err := older.Address(names, role)
			if err != nil {
				t.Fatalf("the AMQP 0-9-1 address for %q: %v", topic, err)
			}
			a10, err := newer.Address(names, role)
			if err != nil {
				t.Fatalf("the AMQP 1.0 address for %q: %v", topic, err)
			}
			if a091.Exchange != a10.Exchange || a091.RoutingKey != a10.RoutingKey || a091.Queue != a10.Queue {
				t.Errorf("the two profiles disagree about the destination for %q: 0-9-1 has exchange %q key %q queue %q "+
					"and 1.0 has exchange %q key %q queue %q",
					topic, a091.Exchange, a091.RoutingKey, a091.Queue, a10.Exchange, a10.RoutingKey, a10.Queue)
			}
			if a091.Node != "" {
				t.Errorf("the AMQP 0-9-1 arm produced an address node %q; the path form is AMQP 1.0's alone", a091.Node)
			}
			if a10.Node == "" {
				t.Error("the AMQP 1.0 arm produced no address node, so a link would have nothing to attach to")
			}
		}
		// And the addresses this profile hands the core are those very nodes.
		sink, source, err := rmq10.AddressForTest(rmq10.Config{Addrs: []string{"amqp://h:5672"}}, topic, "workers")
		if err != nil {
			t.Fatalf("deriving this profile's addresses for %q: %v", topic, err)
		}
		pub, _ := newer.Address(names, protocol.Publish)
		con, _ := newer.Address(names, protocol.Consume)
		if sink != pub.Node || source != con.Node {
			t.Errorf("this profile's addresses for %q are %q and %q, and the shared derivation says %q and %q",
				topic, sink, source, pub.Node, con.Node)
		}
	}
}

// TestAddressRefusesATopicThisBrokerCannotName holds the addressing to the
// refusals `internal/rabbit` makes, and to the rule that a refusal has to be
// reachable through errors.Is.
//
// The wildcard refusal is the one worth reading. A topic is its own routing key
// on RabbitMQ, so a topic literally called "orders.#" would bind a subscription
// to every topic under "orders" — a fan-out claim silently turned into
// somebody else's traffic.
func TestAddressRefusesATopicThisBrokerCannotName(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	for _, topic := range [...]string{"", "amq.orders", strings.Repeat("x", 256), "bad\nname"} {
		if _, _, err := rmq10.AddressForTest(cfg, topic, "workers"); err == nil {
			t.Errorf("the topic %q was accepted; `internal/rabbit` refuses it and this profile must not go round it", topic)
		} else if !errors.Is(err, mq.ErrInvalidTopic) && mq.CodeOf(err) != mq.CodeInvalidArgument {
			t.Errorf("the refusal of %q is %v, which reaches neither ErrInvalidTopic nor CodeInvalidArgument", topic, err)
		}
	}
	for _, sub := range [...]string{"", "dlq", "dlx", "delay", "a.b", "work#ers"} {
		if _, _, err := rmq10.AddressForTest(cfg, "orders", sub); err == nil {
			t.Errorf("the subscription %q was accepted; it would derive a queue name that is also one of this "+
				"driver's own derived names", sub)
		}
	}
}

// TestWildcardSubscriptionIsRefusedAsUnsupported is the negative half of a
// capability claim, and the reason the refusal is ErrUnsupported rather than an
// invalid name.
//
// the probe for a feature a driver does NOT claim runs the feature and
// requires ErrUnsupported. `internal/rabbit` would refuse a wildcard topic with
// CodeInvalidArgument — a topic is its own routing key, so a wildcard in one
// would bind a subscription to other topics' traffic — and a refusal with that
// code leaves the claim unverified in both directions, which is what
// drivers/sqs recorded for this very feature. So a wildcard SOURCE is refused
// as unsupported and a wildcard SINK keeps the invalid-name answer: there is no
// feature that would make a publication to a pattern meaningful.
func TestWildcardSubscriptionIsRefusedAsUnsupported(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	p, err := cfg.Profile()
	if err != nil {
		t.Fatalf("building the profile: %v", err)
	}
	for _, pattern := range [...]string{"orders.#", "orders.*", "#", "orders.*.eu"} {
		_, err := p.SourceAddress(pattern, "workers")
		if !errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("subscribing to the pattern %q = %v, want ErrUnsupported: this profile does not claim "+
				"FeatureWildcardSubscribe, and the negative probe reads that sentinel and no other", pattern, err)
		}
		if err != nil && !strings.Contains(err.Error(), pattern) {
			t.Errorf("the refusal of %q does not name the pattern: %q", pattern, err.Error())
		}
		if _, err := p.SinkAddress(pattern); errors.Is(err, mq.ErrUnsupported) {
			t.Errorf("publishing to the pattern %q was refused as unsupported; there is no feature that would make a "+
				"publication to a pattern meaningful, so the honest answer is an invalid topic", pattern)
		}
	}
}

// TestProfileShape is the profile value read back field by field, and every
// assertion here is a decision this module is making rather than a fact about
// the code.
//
// It is worth having as one test because the profile IS this module: a reviewer
// who wants to know what was decided reads this list and the doc comments it
// points at.
func TestProfileShape(t *testing.T) {
	cfg := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	p, err := cfg.Profile()
	if err != nil {
		t.Fatalf("building the profile: %v", err)
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("the profile does not validate: %v", err)
	}
	if p.Driver != "rabbitmq" || p.Name != "amqp10" {
		t.Errorf("the profile is %q/%q, want rabbitmq/amqp10", p.Driver, p.Name)
	}
	if p.AutoCreates {
		t.Error("the profile claims the broker auto-creates topology from an address. RabbitMQ 4's AMQP 1.0 endpoint " +
			"creates nothing: an attach to a queue that does not exist closes the link with amqp:not-found, which is " +
			"what makes the no-hidden-creation answer CodeNotFound here")
	}
	if p.AnnotationPrefix != "" {
		t.Errorf("the profile routes headers under %q into MESSAGE ANNOTATIONS. RabbitMQ converts between its two "+
			"protocols through APPLICATION PROPERTIES, so an annotated header would be in a different place on the "+
			"other profile and a message published through one RabbitMQ driver would read back without it through "+
			"the other", p.AnnotationPrefix)
	}
	if p.Delay.Supported() {
		t.Error("the profile claims a delayed publication. RabbitMQ has no AMQP 1.0 spelling of a delivery delay and " +
			"nothing here has verified which section the delayed-message plugin would read one from, so the specification forbids " +
			"the claim")
	}
	if p.NackDelay.Supported {
		t.Error("the profile claims a delayed redelivery. A `modified` disposition makes the message available again " +
			"at once and RabbitMQ has no annotation that holds one back")
	}
	if p.Dedup.Supported() {
		t.Error("the profile claims a de-duplication key. RabbitMQ has no broker-side de-duplication")
	}
	if p.Admin != nil {
		t.Error("the profile supplies a topology surface. AMQP 1.0 has none and RabbitMQ 4's management extension is " +
			"not implemented here; claiming one would publish a surface that answers ErrUnsupported")
	}
	if p.DeadLetterAddress != nil {
		t.Error("the profile supplies a dead-letter address, which would make the core re-publish a rejected message " +
			"itself. RabbitMQ routes `rejected` through the queue's own dead-letter exchange, so a second mechanism " +
			"would mean one more copy and one more window")
	}
	if p.Attempt.Source != core.AttemptFromHeader {
		t.Errorf("the attempt count is read from %v, and on this profile it is the AMQP header's `delivery-count` — a "+
			"protocol field rather than RabbitMQ's x-delivery-count header extension", p.Attempt.Source)
	}
	if p.CheckBroker == nil {
		t.Fatal("the profile has no version gate, which is the one thing this module exists for")
	}
	if p.ErrorRules == nil {
		t.Fatal("the profile adds no error rules, so an unroutable publication would be reported as retryable")
	}
}

// TestUnroutablePublicationIsNotFound is the rule this profile adds to the
// core table, and it is the specification's "Mandatory + NotifyReturn = Rejected"
// arriving through a different mechanism.
//
// RabbitMQ RELEASES a message that an exchange routes nowhere. The core's
// generic table reads `released` as the peer declining to act — CodeUnavailable
// and retryable, because another node might take it — and on RabbitMQ that is
// wrong in the way that costs a caller the most: the message is gone, the
// topology is what is wrong, and retrying throws the message away again. So the
// profile's rule makes it CodeNotFound and not retryable, which is the same
// answer the AMQP 0-9-1 driver gives for the same mistake.
func TestUnroutablePublicationIsNotFound(t *testing.T) {
	p := newPeer(t, rabbitProps("4.1.0"))
	b := openBackend(t, testConfig(p))
	released := &core.OutcomeError{Outcome: core.OutcomeReleased}

	got := core.MapForTest(b, mq.OpPublish, released)
	if code := mq.CodeOf(got); code != mq.CodeNotFound {
		t.Errorf("a `released` disposition for a publication maps to %v, want CodeNotFound: on RabbitMQ it means the "+
			"exchange routed the message nowhere, which the specification makes a rejection", code)
	}
	if mq.IsRetryable(got) {
		t.Error("the unroutable publication is reported retryable; retrying routes the message nowhere again and " +
			"throws away another copy")
	}
	if mq.IsAmbiguous(got) {
		t.Error("the unroutable publication is reported ambiguous. The disposition ARRIVED, so the outcome is known — " +
			"which is the half of the distinction a driver that marked everything ambiguous would destroy")
	}

	// The same outcome on the settlement path keeps the core's reading: a
	// dead-letter copy that was released was not stored, nothing was settled,
	// and the delivery is still the broker's, so a retry is the right advice.
	onNack := core.MapForTest(b, mq.OpNack, released)
	if code := mq.CodeOf(onNack); code == mq.CodeNotFound {
		t.Error("the profile's publish rule reached the settlement path: a released dead-letter copy is retryable, " +
			"because nothing was settled and the delivery is still the broker's")
	}
	// And a `rejected` disposition is still classified by its own condition
	// rather than by this rule.
	rejected := &core.OutcomeError{Outcome: core.OutcomeRejected}
	if code := mq.CodeOf(core.MapForTest(b, mq.OpPublish, rejected)); code == mq.CodeNotFound {
		t.Error("a `rejected` disposition was read as unroutable; the two are different answers and a caller acts on " +
			"the difference")
	}
}

// TestDurableBoundaryNeedsAQuorumQueue is the rule for this profile: either
// state the exact durable boundary or fail at construction.
//
// On a durable quorum queue the `accepted` disposition is returned once the
// message has been written to a majority of the queue's replicas, which is a
// boundary worth the name. On a classic queue the same disposition means the
// broker holds the message in memory — a boundary that READS like durability
// and is not one — so a publisher that asks for ConfirmDurable against a
// classic-queue configuration has to fail rather than succeed against a weaker
// promise.
func TestDurableBoundaryNeedsAQuorumQueue(t *testing.T) {
	base := rmq10.Config{Addrs: []string{"amqp://127.0.0.1:5672"}}
	quorum, err := base.Profile()
	if err != nil {
		t.Fatalf("building the quorum profile: %v", err)
	}
	if !quorum.Durable.Supported {
		t.Fatal("a quorum-queue configuration defines no durable boundary, so ConfirmDurable would be refused " +
			"against the strongest boundary RabbitMQ has")
	}
	if !strings.Contains(quorum.Durable.Note, "majority") {
		t.Errorf("the durable boundary is stated as %q and does not say what it is; the specification requires the exact boundary",
			quorum.Durable.Note)
	}
	classicCfg := base
	classicCfg.QueueType = rmq10.QueueClassic
	classic, err := classicCfg.Profile()
	if err != nil {
		t.Fatalf("building the classic profile: %v", err)
	}
	if classic.Durable.Supported {
		t.Error("a classic-queue configuration claims a durable confirmation boundary. The `accepted` disposition " +
			"from a classic queue means the broker holds the message in memory, so the specification wants ConfirmDurable to fail " +
			"when the sink is opened rather than succeed against a weaker promise")
	}
}

// TestProfileIsAFunctionOfTheConfiguration is why there is no package-level
// profile value.
//
// Four of the profile's answers depend on the deployment rather than on the
// broker: the addressing depends on the prefix, and three capability claims
// depend on the queue type, the dead-letter route and the priority ceiling. A
// constant profile would have to claim the union of every deployment's facts,
// which is exactly the capability table exists to prevent.
func TestProfileIsAFunctionOfTheConfiguration(t *testing.T) {
	plain, err := rmq10.Config{Addrs: []string{"amqp://h:5672"}}.Profile()
	if err != nil {
		t.Fatalf("building the plain profile: %v", err)
	}
	prefixed, err := rmq10.Config{Addrs: []string{"amqp://h:5672"}, Prefix: "acme."}.Profile()
	if err != nil {
		t.Fatalf("building the prefixed profile: %v", err)
	}
	a, err := plain.SinkAddress("orders")
	if err != nil {
		t.Fatalf("the plain sink address: %v", err)
	}
	b, err := prefixed.SinkAddress("orders")
	if err != nil {
		t.Fatalf("the prefixed sink address: %v", err)
	}
	if a == b {
		t.Errorf("both profiles address %q, so Config.Prefix reaches nothing and two deployments sharing a virtual "+
			"host would share a topology", a)
	}
}

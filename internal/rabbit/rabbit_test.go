package rabbit

import (
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestDeriveNames pins the whole topology one topic and one subscription map
// to (the specification's "topic→durable exchange; subscription→durable quorum queue
// <topic>.<subscription>; DLX→<queue>.dlq").
//
// It is an exact comparison of every field rather than a spot check of two,
// because the names have to point at EACH OTHER: the dead-letter exchange is
// named from the queue, the delay queues dead-letter back to the exchange, and
// a set where one name was derived from a slightly different input builds a
// topology whose parts miss.
func TestDeriveNames(t *testing.T) {
	got, err := Derive(Config{Prefix: "iqmq.", Topic: "orders", Subscription: "workers"})
	if err != nil {
		t.Fatalf("deriving the names: %v", err)
	}
	want := Names{
		Exchange:             "iqmq.orders",
		Queue:                "iqmq.orders.workers",
		RoutingKey:           "orders",
		DeadLetterExchange:   "iqmq.orders.workers.dlx",
		DeadLetterQueue:      "iqmq.orders.workers.dlq",
		DeadLetterRoutingKey: "dlq",
		DelayExchange:        "iqmq.orders.delayed",
	}
	if got != want {
		t.Errorf("Derive gave\n  %+v\nwant\n  %+v", got, want)
	}
}

// TestDeriveWithoutSubscription is the publish-only name set: a sink needs the
// exchange and the routing key and has no queue, and the fields that belong to
// a subscription stay empty rather than being filled with a queue nobody
// named.
func TestDeriveWithoutSubscription(t *testing.T) {
	got, err := Derive(Config{Topic: "orders"})
	if err != nil {
		t.Fatalf("deriving the names: %v", err)
	}
	if got.Queue != "" || got.DeadLetterQueue != "" || got.DeadLetterExchange != "" {
		t.Errorf("a publish-only name set names a queue: %+v", got)
	}
	if got.Exchange != "orders" || got.RoutingKey != "orders" {
		t.Errorf("Exchange=%q RoutingKey=%q, want both %q", got.Exchange, got.RoutingKey, "orders")
	}
}

// TestKeyAsRoutingKeyBindsEverything is WithKeyAsRoutingKey.
//
// When the message key is the routing key the topic is no longer, so a
// subscription that wants the whole topic has to bind with "#". A driver that
// left the binding at the topic name would publish under one key and bind
// under another, and the queue would receive nothing at all — which is the
// silent failure this pins.
func TestKeyAsRoutingKeyBindsEverything(t *testing.T) {
	got, err := Derive(Config{Topic: "orders", Subscription: "w", KeyAsRoutingKey: true})
	if err != nil {
		t.Fatalf("deriving the names: %v", err)
	}
	if got.RoutingKey != MatchAll {
		t.Errorf("RoutingKey=%q with KeyAsRoutingKey, want %q: the queue would bind to a key nothing is published with", got.RoutingKey, MatchAll)
	}
}

// TestCheckTopicRefusals holds every naming rule to an input that breaks it
// and to one that does not.
//
// Both directions are here on purpose. A validator tested only on bad input
// passes when it refuses everything, which is a driver that cannot publish.
func TestCheckTopicRefusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		topic   string
		wantErr error
	}{
		{"plain", "orders", nil},
		{"dotted", "orders.eu.west", nil},
		{"maximum length", strings.Repeat("a", MaxNameLen), nil},
		{"empty", "", errEmptyName},
		{"too long", strings.Repeat("a", MaxNameLen+1), errNameTooLong},
		{"not utf-8", "ord\xffers", errNameNotUTF8},
		{"newline", "orders\n", errNameControl},
		{"nul", "ord\x00ers", errNameControl},
		{"delete", "orders\x7f", errNameControl},
		{"star", "orders.*", errTopicWildcard},
		{"hash", "orders.#", errTopicWildcard},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckTopic(tc.topic)
			switch {
			case tc.wantErr == nil && err != nil:
				t.Fatalf("CheckTopic(%q) = %v, want nil", tc.topic, err)
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("CheckTopic(%q) = %v, want %v", tc.topic, err, tc.wantErr)
			}
			if tc.wantErr != nil && mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Errorf("CheckTopic(%q) carries code %v, want %v: a name this broker cannot name is the caller's argument and not its configuration",
					tc.topic, mq.CodeOf(err), mq.CodeInvalidArgument)
			}
		})
	}
}

// TestReservedPrefixIsCheckedOnTheDerivedName is the rule a per-input check
// would miss: RabbitMQ refuses a DECLARATION under "amq.", and what reaches
// the broker is the prefix plus the topic.
//
// So a topic of "orders" under a prefix of "amq." has to be refused even
// though neither half is reserved on its own.
func TestReservedPrefixIsCheckedOnTheDerivedName(t *testing.T) {
	if _, err := Derive(Config{Prefix: "amq.", Topic: "orders"}); !errors.Is(err, errNameReserved) {
		t.Fatalf("Derive with prefix \"amq.\" = %v, want the reserved-prefix refusal: the broker answers the declaration with ACCESS_REFUSED", err)
	}
	// And the topic alone is refused too, so neither route reaches the
	// broker.
	if _, err := Derive(Config{Topic: "amq.orders"}); !errors.Is(err, errNameReserved) {
		t.Fatalf("Derive with topic \"amq.orders\" = %v, want the reserved-prefix refusal", err)
	}
	// The negative direction: "amqp.orders" is not under the reserved
	// prefix, because the prefix is "amq." with the dot.
	if _, err := Derive(Config{Topic: "amqp.orders"}); err != nil {
		t.Fatalf("Derive with topic \"amqp.orders\" = %v, want nil: only \"amq.\" is reserved", err)
	}
}

// TestCheckSubscriptionRefusesADot is narrower than the broker and is what
// keeps ParseQueue unambiguous: the queue is "<topic>.<subscription>", and a
// dotted subscription makes the split guesswork.
func TestCheckSubscriptionRefusesADot(t *testing.T) {
	if err := CheckSubscription("eu.workers"); !errors.Is(err, errSubscriptionDot) {
		t.Fatalf("CheckSubscription(%q) = %v, want the dot refusal", "eu.workers", err)
	}
	if err := CheckSubscription("workers"); err != nil {
		t.Fatalf("CheckSubscription(%q) = %v, want nil", "workers", err)
	}
}

// TestDeriveRefusesANameTheSuffixWouldOverflow is the case a length check on
// the INPUT cannot catch.
//
// The longest name this driver builds is the dead-letter queue's, which is the
// exchange plus the subscription plus ".dlq" — so a topic that is itself
// exactly 255 bytes derives a queue name the broker refuses, and the refusal
// arrives as a channel exception that takes every in-flight confirm with it.
func TestDeriveRefusesANameTheSuffixWouldOverflow(t *testing.T) {
	topic := strings.Repeat("a", MaxNameLen)
	if err := CheckTopic(topic); err != nil {
		t.Fatalf("the topic itself must be valid for this test to mean anything: %v", err)
	}
	_, err := Derive(Config{Topic: topic, Subscription: "w"})
	if !errors.Is(err, errNameTooLong) {
		t.Fatalf("Derive with a 255-byte topic and a subscription = %v, want the length refusal: the derived queue name is %d bytes",
			err, len(topic)+len(".w")+len(DLQSuffix))
	}
}

// TestMatches is the topic-exchange binding oracle, held to AMQP 0-9-1's own
// rules.
//
// It matters because a driver uses it to answer the specification's "does this subscription
// really reach the topic the caller named", and an oracle that said yes too
// often would let a driver declare a binding that receives another topic's
// traffic.
func TestMatches(t *testing.T) {
	for _, tc := range []struct {
		pattern, key string
		want         bool
	}{
		{"orders", "orders", true},
		{"orders", "orders.eu", false},
		{"orders.*", "orders.eu", true},
		{"orders.*", "orders", false},
		{"orders.*", "orders.eu.west", false},
		{"orders.#", "orders", true},
		{"orders.#", "orders.eu", true},
		{"orders.#", "orders.eu.west", true},
		{"#", "", true},
		{"#", "anything.at.all", true},
		{"#.west", "orders.eu.west", true},
		{"#.west", "west", true},
		{"#.west", "orders.eu", false},
		{"*.*.west", "orders.eu.west", true},
		{"*.*.west", "orders.west", false},
		{"orders.#.west", "orders.eu.central.west", true},
		{"orders.#.west", "orders.west", true},
		{"orders.#.west", "orders.west.eu", false},
	} {
		t.Run(tc.pattern+"~"+tc.key, func(t *testing.T) {
			if got := Matches(tc.pattern, tc.key); got != tc.want {
				t.Errorf("Matches(%q, %q) = %v, want %v", tc.pattern, tc.key, got, tc.want)
			}
		})
	}
}

// TestCheckPatternAcceptsWhatCheckTopicRefuses is the asymmetry the two
// functions exist for: a wildcard is a legitimate SUBSCRIPTION and never a
// legitimate topic.
func TestCheckPatternAcceptsWhatCheckTopicRefuses(t *testing.T) {
	for _, p := range []string{"orders.*", "orders.#", "#", "*.west"} {
		if err := CheckPattern(p); err != nil {
			t.Errorf("CheckPattern(%q) = %v, want nil", p, err)
		}
		if err := CheckTopic(p); err == nil {
			t.Errorf("CheckTopic(%q) = nil, want a refusal", p)
		}
	}
	// A wildcard inside a word is neither: "#" and "*" match whole words.
	if err := CheckPattern("ord*rs"); !errors.Is(err, errPatternWord) {
		t.Errorf("CheckPattern(%q) = %v, want the in-word refusal", "ord*rs", err)
	}
}

// TestParseQueueRoundTrips is the reverse mapping, and the cases it must
// refuse.
func TestParseQueueRoundTrips(t *testing.T) {
	n, err := Derive(Config{Prefix: "iqmq.", Topic: "orders.eu", Subscription: "workers"})
	if err != nil {
		t.Fatalf("deriving: %v", err)
	}
	topic, sub, ok := ParseQueue("iqmq.", n.Queue)
	if !ok || topic != "orders.eu" || sub != "workers" {
		t.Fatalf("ParseQueue(%q) = (%q, %q, %v), want (%q, %q, true)", n.Queue, topic, sub, ok, "orders.eu", "workers")
	}
	for _, name := range []string{
		"",                               // nothing
		"iqmq.",                          // the prefix alone
		"iqmq.orders",                    // no subscription
		"iqmq.orders.workers.dlq",        // a dead-letter queue
		"iqmq.orders.workers.dlx",        // a dead-letter exchange
		"iqmq.orders.workers.delay.5000", // a delay queue
		"other.orders.workers",           // another deployment's prefix
		"iqmq..workers",                  // an empty topic
	} {
		if gotTopic, gotSub, ok := ParseQueue("iqmq.", name); ok {
			t.Errorf("ParseQueue(%q) = (%q, %q, true), want false: it is not a subscription's queue", name, gotTopic, gotSub)
		}
	}
}

// TestDerivedNamesCannotBeASubscriptionsQueue is the other half of the
// ambiguity: the split cannot tell "<topic>.dlq" from "<topic>.<sub>", so the
// three derived words are refused as subscription names.
//
// Without it a caller could subscribe as "dlq" and consume another
// subscription's dead-letter queue, with every name valid and nothing
// refused.
func TestDerivedNamesCannotBeASubscriptionsQueue(t *testing.T) {
	for _, sub := range []string{"dlq", "dlx", "delay"} {
		if err := CheckSubscription(sub); !errors.Is(err, errSubscriptionReserved) {
			t.Errorf("CheckSubscription(%q) = %v, want the reserved refusal: its queue would be another subscription's dead-letter queue", sub, err)
		}
		if _, err := Derive(Config{Topic: "orders", Subscription: sub}); err == nil {
			t.Errorf("Derive accepted the subscription %q", sub)
		}
	}
	// The negative direction: a name that merely contains one of them is
	// fine.
	if err := CheckSubscription("dlq-reader"); err != nil {
		t.Errorf("CheckSubscription(%q) = %v, want nil", "dlq-reader", err)
	}
}

// TestDelayQueueNamesTheBucket pins the one thing a delay queue's name has to
// carry: RabbitMQ fixes x-message-ttl at declaration, so the bucket is in the
// name and one queue serves one bucket.
func TestDelayQueueNamesTheBucket(t *testing.T) {
	got, err := DelayQueue("iqmq.orders.workers", 5000)
	if err != nil {
		t.Fatalf("DelayQueue: %v", err)
	}
	if got != "iqmq.orders.workers.delay.5000" {
		t.Errorf("DelayQueue = %q, want %q", got, "iqmq.orders.workers.delay.5000")
	}
	if _, err := DelayQueue(strings.Repeat("a", MaxNameLen), 5000); !errors.Is(err, errNameTooLong) {
		t.Errorf("DelayQueue on a 255-byte queue = %v, want the length refusal", err)
	}
}

// TestParseVersion covers the strings a RabbitMQ server really sends and the
// ones that mean "it did not say".
func TestParseVersion(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Version
		ok   bool
	}{
		{"4.3.5", Version{4, 3, 5}, true},
		{"3.13.7", Version{3, 13, 7}, true},
		{"4.1.8-rc.1", Version{4, 1, 8}, true},
		{"3.13.7+deb1", Version{3, 13, 7}, true},
		{"4.2.9 (Erlang 27)", Version{4, 2, 9}, true},
		{"4.0", Version{4, 0, 0}, true},
		{"4", Version{4, 0, 0}, true},
		{" 4.3.5 ", Version{4, 3, 5}, true},
		{"", Version{}, false},
		{"unknown", Version{}, false},
		{"v4.3.5", Version{}, false},
		// All three numbers zero is the zero Version, which is reserved for
		// "it did not say": accepting it would report a version String
		// renders as "unknown" (FuzzRabbitVersion's seed 00e15d22123489fd,
		// The specification). A 0.x that is not all zeroes is still a version.
		{"0", Version{}, false},
		{"0.0.0", Version{}, false},
		{"0.0.0-rc.1", Version{}, false},
		{"0.1", Version{0, 1, 0}, true},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, ok := ParseVersion(tc.in)
			if ok != tc.ok || got != tc.want {
				t.Errorf("ParseVersion(%q) = (%v, %v), want (%v, %v)", tc.in, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestUnknownVersionSatisfiesNoGate is the rule that keeps a version gate from
// being open by omission: a broker that did not say how old it is is refused,
// not waved through.
func TestUnknownVersionSatisfiesNoGate(t *testing.T) {
	var unknown Version
	if unknown.AtLeast(MinBrokerAMQP10) {
		t.Error("the zero Version satisfies the AMQP 1.0 gate: an unreported version would open every gate")
	}
	err := CheckAtLeast(unknown, MinBrokerAMQP10, "the AMQP 1.0 profile")
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("CheckAtLeast with an unknown version = %v, want ErrUnsupported", err)
	}
	if !strings.Contains(err.Error(), "did not report") {
		t.Errorf("the refusal does not say the broker was silent: %v", err)
	}
	// The positive and negative directions of the real gate.
	if err := CheckAtLeast(Version{4, 0, 0}, MinBrokerAMQP10, "x"); err != nil {
		t.Errorf("RabbitMQ 4.0.0 refused by the AMQP 1.0 gate: %v", err)
	}
	if err := CheckAtLeast(Version{3, 13, 7}, MinBrokerAMQP10, "x"); !errors.Is(err, mq.ErrUnsupported) {
		t.Errorf("RabbitMQ 3.13.7 accepted by the AMQP 1.0 gate: %v", err)
	}
}

// TestVersionCompare is the ordering, including the case a string comparison
// gets wrong: 3.9 is older than 3.13.
func TestVersionCompare(t *testing.T) {
	older, newer := Version{3, 9, 0}, Version{3, 13, 0}
	if older.Compare(newer) != -1 {
		t.Errorf("3.9.0 is not older than 3.13.0: a lexical comparison would say otherwise")
	}
	//nolint:gocritic // dupArg: comparing a version with ITSELF is the reflexivity half of the total order, which is the property under test
	if newer.Compare(older) != 1 || newer.Compare(newer) != 0 {
		//nolint:gocritic // dupArg: as above
		t.Errorf("Compare is not a total order: %d %d", newer.Compare(older), newer.Compare(newer))
	}
}

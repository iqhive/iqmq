package pulsar_test

import (
	"errors"
	"strings"
	"testing"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// TestTopicOfComposesTheConfiguredNamespace holds the mapping: a portable
// topic is a LOCAL name inside the configured tenant and namespace, and a
// fully qualified name is taken as one.
func TestTopicOfComposesTheConfiguredNamespace(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg   pd.Config
		topic string
		want  string
		code  mq.ErrorCode
	}{
		"the defaults": {
			topic: "orders", want: "persistent://public/default/orders",
		},
		"a configured tenant and namespace": {
			cfg:   pd.Config{Tenant: "acme", Namespace: "billing"},
			topic: "orders", want: "persistent://acme/billing/orders",
		},
		"a prefix": {
			cfg:   pd.Config{TopicPrefix: "iqmq-"},
			topic: "orders", want: "persistent://public/default/iqmq-orders",
		},
		"the non-persistent domain": {
			cfg:   pd.Config{Domain: "non-persistent"},
			topic: "orders", want: "non-persistent://public/default/orders",
		},
		"a fully qualified name escapes the configuration": {
			cfg:   pd.Config{Tenant: "acme", Namespace: "billing", TopicPrefix: "iqmq-"},
			topic: "persistent://other/ns/orders", want: "persistent://other/ns/orders",
		},
		// The containment rule: a short name with a slash is refused
		// rather than read as tenant/namespace/topic, which Pulsar itself
		// would accept. A deployment that configured a tenant has said
		// where its topics live.
		"a short name may not escape the namespace": {
			topic: "other/ns/orders", code: mq.CodeInvalidArgument,
		},
		"an empty topic":       {topic: "", code: mq.CodeInvalidArgument},
		"an illegal character": {topic: "orders/eu", code: mq.CodeInvalidArgument},
		"a space":              {topic: "orders eu", code: mq.CodeInvalidArgument},
		"a partition suffix": {
			topic: "orders-partition-3", code: mq.CodeInvalidArgument,
		},
		"a pattern": {topic: "orders.*", code: mq.CodeInvalidArgument},
		"a name too long": {
			topic: strings.Repeat("x", 256), code: mq.CodeInvalidArgument,
		},
		"the legacy four-part form": {
			topic: "persistent://t/cluster/ns/orders", code: mq.CodeInvalidArgument,
		},
		"an unknown domain": {
			topic: "ephemeral://t/ns/orders", code: mq.CodeInvalidArgument,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := pd.TopicOf(tc.cfg, tc.topic)
			if tc.code != mq.CodeUnknown {
				if err == nil {
					t.Fatalf("TopicOf(%q) produced %q, want a refusal", tc.topic, got)
				}
				if mq.CodeOf(err) != tc.code {
					t.Fatalf("TopicOf(%q) reported %v (code %v), want %v", tc.topic, err, mq.CodeOf(err), tc.code)
				}
				return
			}
			if err != nil {
				t.Fatalf("TopicOf(%q): %v", tc.topic, err)
			}
			if got.String() != tc.want {
				t.Fatalf("TopicOf(%q) produced %q, want %q", tc.topic, got, tc.want)
			}
		})
	}
}

// TestTopicNameRoundTrips is the target 3 for a name mapper: every name
// ParseTopic accepts renders back to itself, and every name TopicOf produces
// ParseTopic accepts.
func TestTopicNameRoundTrips(t *testing.T) {
	for _, topic := range []string{"orders", "orders.eu", "a", "x_1-2=3:4"} {
		got, err := pd.TopicOf(pd.Config{}, topic)
		if err != nil {
			t.Fatalf("TopicOf(%q): %v", topic, err)
		}
		back, err := pd.ParseTopic(got.String())
		if err != nil {
			t.Fatalf("ParseTopic(%q): %v", got, err)
		}
		if back.String() != got.String() {
			t.Fatalf("%q round-tripped to %q", got, back)
		}
		if back.Local != topic {
			t.Fatalf("the local name came back as %q, want %q", back.Local, topic)
		}
	}
}

// TestTopicMappingIsInjective is the property that keeps one subscription from
// receiving another's messages: two different portable topics never become one
// Pulsar topic.
func TestTopicMappingIsInjective(t *testing.T) {
	cfg := pd.Config{TopicPrefix: "p-"}
	seen := map[string]string{}
	for _, topic := range []string{"a", "b", "a.b", "a-b", "a_b", "ab", "A", "a:b", "a=b"} {
		got, err := pd.TopicOf(cfg, topic)
		if err != nil {
			t.Fatalf("TopicOf(%q): %v", topic, err)
		}
		if prev, dup := seen[got.String()]; dup {
			t.Fatalf("%q and %q both map to %q; a mapper that is not injective makes one "+
				"subscription receive another's messages", prev, topic, got)
		}
		seen[got.String()] = topic
	}
}

// TestPatternDetection holds the rule that decides whether a topic is a name or
// a pattern, and the one character that is deliberately not a metacharacter
// here.
func TestPatternDetection(t *testing.T) {
	for topic, want := range map[string]bool{
		"orders":    false,
		"orders.eu": false, // a dot is an ordinary character in a Pulsar topic name
		"orders.*":  true,
		"orders?":   true,
		"a|b":       true,
		"a[0-9]":    true,
		"^orders$":  true,
		"orders#":   true,
		"orders>":   true,
	} {
		if got := pd.HasPattern(topic); got != want {
			t.Errorf("HasPattern(%q) is %v, want %v", topic, got, want)
		}
	}
}

// TestPatternOfQualifiesTheNamespace is why a bare pattern cannot work: Pulsar
// matches a regex subscription's pattern against the WHOLE topic name.
func TestPatternOfQualifiesTheNamespace(t *testing.T) {
	got, ns, err := pd.PatternOf(pd.Config{Tenant: "acme", Namespace: "billing"}, "orders.*")
	if err != nil {
		t.Fatalf("PatternOf: %v", err)
	}
	if got != "persistent://acme/billing/orders.*" {
		t.Fatalf("PatternOf produced %q", got)
	}
	if ns != "acme/billing" {
		t.Fatalf("PatternOf reported namespace %q", ns)
	}
	if _, _, err := pd.PatternOf(pd.Config{}, "a/b"); err == nil {
		t.Fatal("a bare pattern containing a slash was accepted")
	}
}

// TestSubscriptionOfTakesTheTopicGrammar is why: Pulsar builds the retry and
// dead-letter topic names out of the subscription's name.
func TestSubscriptionOfTakesTheTopicGrammar(t *testing.T) {
	if _, err := pd.SubscriptionOf(""); !errors.Is(err, mq.ErrInvalidTopic) && mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("an empty subscription reported %v (code %v)", err, mq.CodeOf(err))
	}
	if _, err := pd.SubscriptionOf("work/ers"); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("a subscription with a slash reported %v (code %v); Pulsar would build the retry topic "+
			"<topic>-work/ers-RETRY out of it", err, mq.CodeOf(err))
	}
	got, err := pd.SubscriptionOf("workers")
	if err != nil || got != "workers" {
		t.Fatalf("SubscriptionOf(\"workers\") = %q, %v", got, err)
	}
}

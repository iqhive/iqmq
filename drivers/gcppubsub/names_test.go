package gcppubsub_test

import (
	"strings"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"

	gps "bitbucket.org/iqhive/iqmq/drivers/gcppubsub"
)

// The name mapping, which is pure and runs before any call.
//
// That is what turns a topic the service cannot hold into an immediate
// refusal naming the rule, rather than a request the API rejects with a
// message about a resource path. Every rule here is one a caller can trip
// and none of them ran.

// TestATopicIdentifierIsDerivedOrPassedThroughOrRefused.
//
// The three cases are deliberately different: a bare name is mapped, a fully
// qualified `projects/...` path is passed through UNTOUCHED -- it is a
// caller naming a resource in another project, and rewriting it would send
// their messages somewhere else -- and anything the service cannot hold is
// refused.
func TestATopicIdentifierIsDerivedOrPassedThroughOrRefused(t *testing.T) {
	t.Parallel()
	cfg := gps.Config{ProjectID: "iqmq-test"}

	// The CODE is what makes the wildcard cases discriminating. A pattern is
	// also rejected by the character rule, so a test that asked only whether
	// it failed would pass with the wildcard check deleted -- and the
	// difference is what the specification requires: an unadvertised FEATURE is
	// Unsupported, a name the caller got wrong is InvalidArgument.
	for _, tc := range []struct {
		name  string
		topic string
		want  string
		ok    bool
		code  mq.ErrorCode
	}{
		{"a bare name", "orders", "orders", true, 0},
		{"a fully qualified path", "projects/other/topics/orders", "projects/other/topics/orders", true, 0},
		{"the empty topic", "", "", false, mq.CodeInvalidArgument},
		{"a glob wildcard", "orders.*", "", false, mq.CodeUnsupported},
		{"an AMQP wildcard", "orders.#", "", false, mq.CodeUnsupported},
		{"a NATS wildcard", "orders.>", "", false, mq.CodeUnsupported},
		{"a name over the service's length", strings.Repeat("x", gps.MaxIDLen+1), "", false, mq.CodeInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := cfg.TopicID(tc.topic)
			if tc.ok {
				if err != nil {
					t.Fatalf("TopicID(%q) is %v, want %q", tc.topic, err, tc.want)
				}
				if got != tc.want {
					t.Errorf("TopicID(%q) is %q, want %q", tc.topic, got, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("TopicID(%q) returned %q; the service cannot hold it", tc.topic, got)
			}
			if code := mq.CodeOf(err); code != tc.code {
				t.Errorf("TopicID(%q) is %v (code %v), want %v", tc.topic, err, code, tc.code)
			}
		})
	}
}

// TestAFullyQualifiedPathIsNotRewrittenByThePrefix.
//
// It is the case a prefix would silently break: a deployment that sets one
// and a caller who names a topic in another project are both reasonable, and
// prepending the prefix to their path would send the messages to a resource
// that does not exist -- or, worse, to one that does.
func TestAFullyQualifiedPathIsNotRewrittenByThePrefix(t *testing.T) {
	t.Parallel()
	cfg := gps.Config{ProjectID: "iqmq-test", TopicPrefix: "app-"}

	const qualified = "projects/other/topics/orders"
	got, err := cfg.TopicID(qualified)
	if err != nil {
		t.Fatalf("TopicID(%q): %v", qualified, err)
	}
	if got != qualified {
		t.Errorf("TopicID(%q) is %q; a caller naming a resource in another project had their path "+
			"rewritten, which sends their messages somewhere else", qualified, got)
	}

	// A bare name IS prefixed, or the setting would do nothing.
	if got, err := cfg.TopicID("orders"); err != nil || !strings.HasPrefix(got, "app-") {
		t.Errorf("TopicID(\"orders\") is (%q, %v); the configured prefix has to reach a bare name", got, err)
	}
}

// TestASubscriptionIdentifierTakesItsOverrideFirst.
//
// The override map is how a deployment addresses a subscription somebody else
// named, and it has to win over the derivation -- otherwise the setting could
// only ever agree with what the driver would have computed anyway.
func TestASubscriptionIdentifierTakesItsOverrideFirst(t *testing.T) {
	t.Parallel()
	cfg := gps.Config{
		ProjectID:       "iqmq-test",
		SubscriptionIDs: map[string]string{"orders/workers": "the-existing-one"},
	}

	got, err := cfg.SubscriptionID("orders", "workers")
	if err != nil {
		t.Fatalf("SubscriptionID: %v", err)
	}
	if got != "the-existing-one" {
		t.Errorf("SubscriptionID is %q; the override map is how a deployment addresses a subscription "+
			"somebody else named, and a derivation that won would make the setting useless", got)
	}

	// One the map does not name is still derived.
	if got, err := cfg.SubscriptionID("orders", "audit"); err != nil || got == "" {
		t.Errorf("SubscriptionID for a pair the map does not name is (%q, %v)", got, err)
	}
}

// TestASubscriptionIdentifierIsRefusedWhenItCannotBeMade.
func TestASubscriptionIdentifierIsRefusedWhenItCannotBeMade(t *testing.T) {
	t.Parallel()
	cfg := gps.Config{ProjectID: "iqmq-test"}

	for _, tc := range []struct{ name, topic, sub string }{
		{"no subscription", "orders", ""},
		{"a topic that is not one", "orders.*", "workers"},
		{"a subscription over the service's length", "orders", strings.Repeat("s", gps.MaxIDLen+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, err := cfg.SubscriptionID(tc.topic, tc.sub); err == nil {
				t.Errorf("SubscriptionID(%q, %q) returned %q", tc.topic, tc.sub, got)
			}
		})
	}

	// A fully qualified subscription path is passed through, for the same
	// reason a topic path is.
	const qualified = "projects/other/subscriptions/workers"
	if got, err := cfg.SubscriptionID("orders", qualified); err != nil || got != qualified {
		t.Errorf("SubscriptionID with a qualified path is (%q, %v), want it passed through", got, err)
	}
}

package kafkago_test

import (
	"context"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// The Admin surface's refusals.
//
// Its happy paths run in every test that provisions a topic. What runs
// nowhere is what happens when the NAME is wrong or the cluster is gone --
// sixty blocks of one statement each, every one of them a `return err` a
// refactor could drop without any test noticing.

// adminOn is the admin surface of a backend against a running cluster.
func adminOn(t *testing.T, mutate ...func(*kq.Config)) mq.Admin {
	t.Helper()
	b := backendOn(t, cluster(t, 1, []string{"orders"}), mutate...)
	a, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return a
}

// adminCalls is every method, with the topic and subscription it takes.
func adminCalls(a mq.Admin, topic, sub string) []struct {
	name string
	call func(context.Context) error
} {
	return []struct {
		name string
		call func(context.Context) error
	}{
		{"EnsureTopic", func(ctx context.Context) error {
			return a.EnsureTopic(ctx, mq.TopicSpec{Name: topic})
		}},
		{"DeleteTopic", func(ctx context.Context) error { return a.DeleteTopic(ctx, topic) }},
		{"EnsureSubscription", func(ctx context.Context) error {
			return a.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: sub})
		}},
		{"DeleteSubscription", func(ctx context.Context) error { return a.DeleteSubscription(ctx, topic, sub) }},
		{"Purge", func(ctx context.Context) error { return a.Purge(ctx, topic, sub) }},
		{"Stats", func(ctx context.Context) error { _, err := a.Stats(ctx, topic, sub); return err }},
	}
}

// TestEveryAdminCallRefusesATopicNameKafkaCannotHold.
//
// The validation is in `Topic`, which every one of them routes through. One
// that forgot would send the name to the broker and get back an error naming
// a topic the caller never asked for -- or, for a name Kafka merely dislikes,
// create it.
func TestEveryAdminCallRefusesATopicNameKafkaCannotHold(t *testing.T) {
	t.Parallel()
	a := adminOn(t)

	for _, topic := range []string{"", ".", "..", "has spaces", strings.Repeat("x", kq.MaxTopicLen+1)} {
		for _, tc := range adminCalls(a, topic, "workers") {
			if tc.name == "DeleteSubscription" {
				// It validates the GROUP and not the topic, deliberately:
				// Kafka's DeleteGroups takes only the group id, so the topic
				// is there for the error message alone and a caller who got
				// it wrong still gets the group deleted.
				continue
			}
			if err := tc.call(t.Context()); err == nil {
				t.Errorf("%s accepted the topic %q, which Kafka cannot hold", tc.name, topic)
			}
		}
	}
}

// TestAWildcardIsRefusedAsUnsupportedAndNotAsInvalid.
//
// The difference is what the capability table claims: kafka-go takes a topic
// list and not a pattern, so a pattern is a thing this driver cannot do
// rather than a thing the caller got wrong, and the specification requires an unadvertised
// feature to be refused as unsupported.
func TestAWildcardIsRefusedAsUnsupportedAndNotAsInvalid(t *testing.T) {
	t.Parallel()
	a := adminOn(t)

	for _, tc := range adminCalls(a, "orders.*", "workers") {
		if tc.name == "DeleteSubscription" {
			continue // it takes the group alone; see the test above
		}
		err := tc.call(t.Context())
		if got := mq.CodeOf(err); got != mq.CodeUnsupported {
			t.Errorf("%s with a wildcard topic is code %v, want CodeUnsupported: a pattern is a thing this "+
				"driver cannot do, not a name the caller got wrong", tc.name, got)
		}
	}
}

// TestTheSubscriptionCallsRefuseAGroupNameKafkaCannotHold.
//
// The three that take a subscription check the GROUP as well as the topic,
// and they check it after the topic -- so a call with both wrong reports the
// topic, which is the one the caller is more likely to have got wrong.
func TestTheSubscriptionCallsRefuseAGroupNameKafkaCannotHold(t *testing.T) {
	t.Parallel()
	a := adminOn(t)

	for _, sub := range []string{"", "has spaces", strings.Repeat("g", 256)} {
		for _, tc := range adminCalls(a, "orders", sub) {
			switch tc.name {
			case "EnsureTopic", "DeleteTopic":
				continue // neither takes a subscription
			}
			if err := tc.call(t.Context()); err == nil {
				t.Errorf("%s accepted the group %q, which Kafka cannot hold", tc.name, sub)
			}
		}
	}
}

// TestTheConfiguredPrefixIsPartOfTheNameBeingValidated.
//
// A topic that fits on its own may not fit once the deployment's prefix is in
// front of it, and the refusal has to happen then rather than at the broker:
// the caller named a topic that is legal and the deployment made it illegal.
func TestTheConfiguredPrefixIsPartOfTheNameBeingValidated(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("p", kq.MaxTopicLen-2)
	a := adminOn(t, func(c *kq.Config) { c.TopicPrefix = long })

	if err := a.EnsureTopic(t.Context(), mq.TopicSpec{Name: "orders"}); err == nil {
		t.Error("EnsureTopic accepted a topic that is legal on its own and too long with the configured " +
			"prefix in front of it")
	}
}

// TestAdminIsRefusedOnAClosedBackend, rather than handing back a surface
// whose every call would fail at the broker.
func TestAdminIsRefusedOnAClosedBackend(t *testing.T) {
	t.Parallel()
	b := backendOn(t, cluster(t, 1, []string{"orders"}))
	if err := b.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}

	if _, err := b.Admin(); err == nil {
		t.Error("Admin() on a closed backend returned a surface; there is no client left to administer with")
	}
}

// TestOpenBackendRefusesWhenNoBrokerAnswers.
//
// It fails at OPEN rather than at first use, which is the behaviour worth
// having: a backend that opened against nothing and failed later would give
// its errors to whichever publish happened first, a long way from the
// configuration that caused them.
func TestOpenBackendRefusesWhenNoBrokerAnswers(t *testing.T) {
	t.Parallel()
	cfg := config(cluster(t, 1, []string{"orders"}), func(c *kq.Config) {
		c.Brokers = []string{"127.0.0.1:1"}
		c.RequestTimeout = time.Second
		c.WriteTimeout = time.Second
	})

	_, err := kq.OpenBackend(t.Context(), cfg)
	if err == nil {
		t.Fatal("OpenBackend succeeded with no broker answering")
	}
	if got := mq.CodeOf(err); got != mq.CodeUnavailable {
		t.Errorf("OpenBackend with no broker answering is code %v, want CodeUnavailable", got)
	}
	if _, err := kq.Open(t.Context(), cfg); err == nil {
		t.Error("Open succeeded with no broker answering")
	}
}

// TestEveryAdminCallFailsOnceTheClusterIsGone.
//
// It is the case a deployment meets -- the broker restarted, the network
// went -- and every one of these has a per-call error path for it that no
// test reached. A call that swallowed the failure would report success for
// topology that was never created.
//
// EnsureSubscription is NOT in the table, and the reason is worth knowing:
// kafka-go caches cluster metadata, so a group whose position is already
// where it would be committed needs no request at all and the call is
// correctly a no-op. Asserting it fails would be asserting that the client
// forgets what it knows.
func TestEveryAdminCallFailsOnceTheClusterIsGone(t *testing.T) {
	t.Parallel()
	c := cluster(t, 1, []string{"orders"})
	b := backendOn(t, c)
	a, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	c.Close()

	for _, tc := range adminCalls(a, "orders", "workers") {
		if tc.name == "EnsureSubscription" {
			continue
		}
		if err := tc.call(t.Context()); err == nil {
			t.Errorf("%s succeeded against a cluster that is gone", tc.name)
		}
	}
}

// TestListTopicsReportsTheTopicsTheClusterHas, and stops when the caller
// does.
func TestListTopicsReportsTheTopicsTheClusterHas(t *testing.T) {
	t.Parallel()
	a := adminOn(t)

	var found bool
	for topic, err := range a.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		if topic == "orders" {
			found = true
		}
	}
	if !found {
		t.Error("ListTopics did not report the topic the cluster was started with")
	}

	n := 0
	for _, err := range a.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		n++
		break
	}
	if n != 1 {
		t.Errorf("a caller that broke out of ListTopics saw %d topics, want 1", n)
	}
}

// TestDeletingASubscriptionThatNeverExistedIsSuccess.
//
// Kafka answers GroupIdNotFound, and the caller asked for the group to be
// gone -- which it is. A teardown that failed here would need a pre-check
// with the window a pre-check always has, and the conformance kit tears down
// on every run whether or not the run got as far as creating anything.
func TestDeletingASubscriptionThatNeverExistedIsSuccess(t *testing.T) {
	t.Parallel()
	a := adminOn(t)

	if err := a.DeleteSubscription(t.Context(), "orders", "never-consumed"); err != nil {
		t.Errorf("DeleteSubscription for a group that never existed is %v, want nil: GroupIdNotFound means "+
			"the group is not there, which is what the caller asked for", err)
	}
}

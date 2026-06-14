package rabbitmq_test

import (
	"context"
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	rq "bitbucket.org/iqhive/iqmq/drivers/rabbitmq"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/rabbitfake"
)

// The Admin surface, driven against the fake broker.
//
// Every one of these ran against a container before it ran here, and none of
// them ran in `make ci` at all: the whole of admin.go was uncovered by the
// unit suite, so a refactor of it broke nothing that a pull request could see.
// The fake speaks the same channel operations the real broker does --
// ExchangeDeclare, QueueDeclarePassive, QueuePurge, QueueDelete -- so the
// topology calls are testable here and only the wire framing is not.

// adminOf is the admin surface of a client over a fresh fake broker.
func adminOf(t *testing.T, shape func(*rq.Config)) (*rabbitfake.Broker, *mq.Client, mq.Admin) {
	t.Helper()
	broker, client := openFake(t, shape)
	admin, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return broker, client, admin
}

// TestAdminIsRefusedEveryCreationWhenDeclareIsOff is the split, from the
// admin side: a deployment that turned declaration off did so for a reason,
// and the two CREATING calls are the ones that have to honour it.
//
// The inspecting and destroying calls are in the same test because the split
// is the claim: a driver that refused all seven would honour the setting
// nowhere useful, and one that refused none of them would ignore it.
func TestAdminIsRefusedEveryCreationWhenDeclareIsOff(t *testing.T) {
	t.Parallel()
	_, _, admin := adminOf(t, nil) // Declare defaults to off.
	ctx := t.Context()

	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); mq.CodeOf(err) != mq.CodeUnsupported {
		t.Errorf("EnsureTopic with Declare off is %v (code %v), want CodeUnsupported", err, mq.CodeOf(err))
	}
	spec := mq.SubscriptionSpec{Topic: "orders", Name: "workers"}
	if err := admin.EnsureSubscription(ctx, spec); mq.CodeOf(err) != mq.CodeUnsupported {
		t.Errorf("EnsureSubscription with Declare off is %v (code %v), want CodeUnsupported", err, mq.CodeOf(err))
	}

	// The destroying and inspecting calls are still allowed: they are not
	// what the setting is about. They may fail because nothing was declared,
	// but they may not fail with Unsupported.
	if err := admin.DeleteTopic(ctx, "orders"); mq.CodeOf(err) == mq.CodeUnsupported {
		t.Errorf("DeleteTopic with Declare off answered Unsupported (%v); the setting governs creation only", err)
	}
	if err := admin.DeleteSubscription(ctx, "orders", "workers"); mq.CodeOf(err) == mq.CodeUnsupported {
		t.Errorf("DeleteSubscription with Declare off answered Unsupported (%v); the setting governs creation only", err)
	}
}

// TestAdminCreatesTheTopologyItIsAskedFor is the positive half: with Declare
// on, the two Ensure calls put an exchange and a queue on the broker.
//
// It asserts on the BROKER's state rather than on a nil error, because an
// EnsureTopic that returns nil and declares nothing is the failure this test
// exists to catch.
func TestAdminCreatesTheTopologyItIsAskedFor(t *testing.T) {
	t.Parallel()
	broker, _, admin := adminOf(t, declaring)
	ctx := t.Context()

	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if got := len(broker.Exchanges()); got == 0 {
		t.Fatal("EnsureTopic returned nil and the broker has no exchange; the call declared nothing")
	}

	if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	if len(broker.Queues()) == 0 {
		t.Fatalf("EnsureSubscription returned nil and the broker has no queue; declared: %v", queueNames(broker))
	}

	// Both are idempotent: the conformance kit calls them on every run and a
	// second call against existing topology is not a conflict.
	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Errorf("EnsureTopic a second time: %v; the call is not idempotent", err)
	}
	if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Errorf("EnsureSubscription a second time: %v; the call is not idempotent", err)
	}
}

// TestEveryAdminCallRefusesAWildcard is the refusal behind
// FeatureWildcardSubscribe being unsupported.
//
// A topic maps to its own exchange here, so a pattern over topic names would
// have to span exchanges. Every call that takes a name is in the table
// because the refusal lives in `names` and a call that forgot to route
// through it would accept a pattern and then create a queue literally named
// `orders.*`.
func TestEveryAdminCallRefusesAWildcard(t *testing.T) {
	t.Parallel()
	_, _, admin := adminOf(t, declaring)
	ctx := t.Context()

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"EnsureTopic", func() error { return admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders.*"}) }},
		{"EnsureSubscription", func() error {
			return admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders.#", Name: "workers"})
		}},
		{"DeleteTopic", func() error { return admin.DeleteTopic(ctx, "orders.*") }},
		{"DeleteSubscription", func() error { return admin.DeleteSubscription(ctx, "orders.*", "workers") }},
		{"Purge", func() error { return admin.Purge(ctx, "orders.#", "workers") }},
		{"Stats", func() error { _, err := admin.Stats(ctx, "orders.*", "workers"); return err }},
	} {
		if got := mq.CodeOf(tc.call()); got != mq.CodeUnsupported {
			t.Errorf("%s with a wildcard topic answered code %v, want CodeUnsupported", tc.name, got)
		}
	}
}

// TestPurgeDiscardsWhatNobodyConsumed is Purge, measured through
// Stats rather than through the return: a purge that reports success and
// leaves the queue full is the failure worth catching.
func TestPurgeDiscardsWhatNobodyConsumed(t *testing.T) {
	t.Parallel()
	_, client, admin := adminOf(t, declaring)
	ctx := t.Context()

	// The consumer is opened and closed first so the queue exists and is
	// bound, and so nothing holds a lease while the depth is read.
	cons := consumeOn(t, client, "orders", "workers")
	if err := cons.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	pub := publishOn(t, client, "orders")
	for range 3 {
		if err := pub.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}

	before, err := admin.Stats(ctx, "orders", "workers")
	if err != nil {
		t.Fatalf("Stats before the purge: %v", err)
	}
	if !before.Depth.Known || before.Depth.Value != 3 {
		t.Fatalf("the queue holds %d messages (known=%t) before the purge, want 3",
			before.Depth.Value, before.Depth.Known)
	}

	if err := admin.Purge(ctx, "orders", "workers"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	after, err := admin.Stats(ctx, "orders", "workers")
	if err != nil {
		t.Fatalf("Stats after the purge: %v", err)
	}
	if after.Depth.Value != 0 {
		t.Errorf("the queue still holds %d messages after Purge, want 0", after.Depth.Value)
	}
}

// TestStatsReportsWhatItKnowsAndSaysSoAboutLag is the rule that an unknown
// is never an overloaded zero.
//
// Depth and InFlight are measurable here and Lag is not: RabbitMQ has no
// cursor to be behind, so the driver has to answer Known=false rather than 0.
// A driver that reported `Lag: 0` would be claiming a subscription is caught
// up when nothing measured it.
func TestStatsReportsWhatItKnowsAndSaysSoAboutLag(t *testing.T) {
	t.Parallel()
	_, client, admin := adminOf(t, declaring)
	ctx := t.Context()

	cons := consumeOn(t, client, "orders", "workers")
	pub := publishOn(t, client, "orders")
	if err := pub.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	// Held, not settled: this is what InFlight has to count.
	d := receiveOne(t, cons)

	stats, err := admin.Stats(ctx, "orders", "workers")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Depth.Known {
		t.Error("Stats reports Depth as unknown; a passive declare returns the count")
	}
	if !stats.InFlight.Known {
		t.Error("Stats reports InFlight as unknown; the driver counts its own live leases")
	}
	if stats.InFlight.Value != 1 {
		t.Errorf("Stats reports %d in flight while one delivery is held unsettled, want 1", stats.InFlight.Value)
	}
	if stats.Lag.Known {
		t.Errorf("Stats claims to know the lag (%d); RabbitMQ has no cursor, so the specification requires Known=false "+
			"rather than an overloaded zero", stats.Lag.Value)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("acking: %v", err)
	}
}

// TestListTopicsReportsEachTopicOnceAndStopsWhenTheCallerDoes pins both
// halves of the iterator: the dedup, and that a caller's `break` ends it.
//
// What it lists is this CLIENT's own declarations and not the broker's
// inventory -- the driver has no management plugin to ask -- which is the
// part this driver satisfies narrowly and the reason the test names
// the topics it declared itself.
func TestListTopicsReportsEachTopicOnceAndStopsWhenTheCallerDoes(t *testing.T) {
	t.Parallel()
	_, client, admin := adminOf(t, declaring)
	ctx := t.Context()

	// Two subscriptions on one topic: the queue declarations are two and the
	// topic must still be listed once.
	for _, sub := range []string{"workers", "audit"} {
		if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: sub}); err != nil {
			t.Fatalf("EnsureSubscription %s: %v", sub, err)
		}
	}
	if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "payments", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription payments: %v", err)
	}

	var got []string
	for topic, err := range admin.ListTopics(ctx) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		got = append(got, topic)
	}
	sortStrings(got)
	if len(got) != 2 || got[0] != "orders" || got[1] != "payments" {
		t.Errorf("ListTopics yielded %v, want [orders payments]: two subscriptions on one topic list it once", got)
	}

	// A caller that stops early stops the iteration. Without the `return` on
	// a false yield, this loop would run to completion and the count would be
	// two -- the bug an iterator written as a slice append never has and one
	// written by hand always can.
	n := 0
	for range admin.ListTopics(ctx) {
		n++
		break
	}
	if n != 1 {
		t.Errorf("a caller that broke out of ListTopics saw %d topics, want 1", n)
	}
	_ = client
}

// TestAdminOnAClosedClientFailsEveryCall is the CheckOpen guard.
//
// A closed client's admin has no connection to use, and the failure has to be
// the error rather than a panic or a nil-deref inside the channel call.
func TestAdminOnAClosedClientFailsEveryCall(t *testing.T) {
	t.Parallel()
	_, client, admin := adminOf(t, declaring)
	ctx := t.Context()
	if err := client.Close(); err != nil {
		t.Fatalf("closing the client: %v", err)
	}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"EnsureTopic", func() error { return admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}) }},
		{"EnsureSubscription", func() error {
			return admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "workers"})
		}},
		{"DeleteTopic", func() error { return admin.DeleteTopic(ctx, "orders") }},
		{"DeleteSubscription", func() error { return admin.DeleteSubscription(ctx, "orders", "workers") }},
		{"Purge", func() error { return admin.Purge(ctx, "orders", "workers") }},
		{"Stats", func() error { _, err := admin.Stats(ctx, "orders", "workers"); return err }},
	} {
		if err := tc.call(); err == nil {
			t.Errorf("%s on a closed client returned nil; there is no connection to have done it on", tc.name)
		}
	}

	// The iterator reports the same failure through its error slot rather
	// than yielding nothing, which would read as "this broker has no topics".
	var yielded, failed int
	for _, err := range admin.ListTopics(ctx) {
		yielded++
		if err != nil {
			failed++
		}
	}
	if failed == 0 {
		t.Errorf("ListTopics on a closed client yielded %d topics and no error; an empty iteration reads as "+
			"\"this broker has no topics\"", yielded)
	}
}

// TestAdminCallsRespectACancelledContext is the context check every call
// makes before it takes a channel.
func TestAdminCallsRespectACancelledContext(t *testing.T) {
	t.Parallel()
	_, _, admin := adminOf(t, declaring)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"})
	if err == nil {
		t.Fatal("EnsureTopic with a cancelled context returned nil")
	}
	if !errors.Is(err, context.Canceled) && mq.CodeOf(err) != mq.CodeCanceled {
		t.Errorf("EnsureTopic with a cancelled context failed with %v (code %v), want a cancellation",
			err, mq.CodeOf(err))
	}
}

// TestDeleteRemovesWhatEnsureCreated closes the loop: the destroying calls
// have to actually destroy, or a conformance run leaves its topology behind
// and the next run inherits it.
func TestDeleteRemovesWhatEnsureCreated(t *testing.T) {
	t.Parallel()
	broker, _, admin := adminOf(t, declaring)
	ctx := t.Context()

	if err := admin.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	queuesBefore := len(broker.Queues())
	if queuesBefore == 0 {
		t.Fatal("EnsureSubscription declared no queue")
	}

	if err := admin.DeleteSubscription(ctx, "orders", "workers"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if got := len(broker.Queues()); got >= queuesBefore {
		t.Errorf("the broker still holds %d queues after DeleteSubscription (was %d): %v",
			got, queuesBefore, queueNames(broker))
	}

	if err := admin.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	exchangesBefore := len(broker.Exchanges())
	if err := admin.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := len(broker.Exchanges()); got >= exchangesBefore {
		t.Errorf("the broker still holds %d exchanges after DeleteTopic (was %d)", got, exchangesBefore)
	}
}

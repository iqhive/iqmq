package sqs_test

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// The Admin surface's refusals and error paths.
//
// SQS has no subscriptions -- a queue IS the subscription -- so this surface
// is smaller than most and three of its seven methods are about saying so.
// What no test reached is the rest: what each call does when the endpoint
// fails, and what the one method that cannot be supported answers.

// adminOf is the admin surface over a harness.
func adminOf(t *testing.T, h *harness) mq.Admin {
	t.Helper()
	a, err := h.back.Admin()
	if err != nil {
		t.Fatalf("Admin(): %v", err)
	}
	return a
}

// TestDeleteSubscriptionIsRefusedBecauseThereIsNoSuchThing.
//
// On SQS a queue is the subscription, so deleting "the subscription" and
// leaving "the topic" is not a distinction the service has. Answering
// Unsupported is the rule: the alternative -- deleting the queue -- would
// destroy a caller's messages on a call they expected to be narrow.
func TestDeleteSubscriptionIsRefusedBecauseThereIsNoSuchThing(t *testing.T) {
	t.Parallel()
	h := open(t)
	a := adminOf(t, h)

	err := a.DeleteSubscription(t.Context(), "orders", "workers")
	if got := mq.CodeOf(err); got != mq.CodeUnsupported {
		t.Errorf("DeleteSubscription is %v (code %v), want CodeUnsupported: a queue IS the subscription "+
			"here, and deleting it would destroy messages on a call the caller expected to be narrow",
			err, got)
	}
}

// TestEnsureSubscriptionIsEnsureTopic.
//
// The same fact from the other side: asking for a subscription on a topic
// asks for the queue, and a driver that refused it would make the portable
// provisioning path unusable against this broker.
func TestEnsureSubscriptionIsEnsureTopic(t *testing.T) {
	t.Parallel()
	h := open(t)
	a := adminOf(t, h)
	name := topic(t, "admin")

	if err := a.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: name, Name: "workers"}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	if _, err := a.Stats(t.Context(), name, "workers"); err != nil {
		t.Errorf("Stats after EnsureSubscription is %v; the queue it should have created is not there", err)
	}
}

// TestEveryAdminCallReportsAFailingEndpoint.
//
// Each of these has its own error mapping, and a call that swallowed the
// failure would report success for topology that was never created -- the
// worst outcome for a provisioning step, because the next thing to run is
// whatever assumed it worked.
//
// The fault is installed on the operation each call actually MAKES, not on
// the URL lookup: the driver caches a queue's URL, so faulting GetQueueUrl
// after the topic has been ensured fails nothing and the test would pass
// against a driver that ignored every error.
func TestEveryAdminCallReportsAFailingEndpoint(t *testing.T) {
	t.Parallel()
	boom := errors.New("the endpoint is unavailable")

	for _, tc := range []struct {
		name string
		op   string
		call func(mq.Admin, string) error
	}{
		{"EnsureTopic", "CreateQueue", func(a mq.Admin, n string) error {
			return a.EnsureTopic(t.Context(), mq.TopicSpec{Name: n})
		}},
		{"DeleteTopic", "DeleteQueue", func(a mq.Admin, n string) error {
			return a.DeleteTopic(t.Context(), n)
		}},
		{"Purge", "PurgeQueue", func(a mq.Admin, n string) error {
			return a.Purge(t.Context(), n, "workers")
		}},
		{"Stats", "GetQueueAttributes", func(a mq.Admin, n string) error {
			_, err := a.Stats(t.Context(), n, "workers")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			h := openOn(t, server)
			a := adminOf(t, h)
			name := topic(t, "admin")
			if err := a.EnsureTopic(t.Context(), mq.TopicSpec{Name: name}); err != nil {
				t.Fatalf("provisioning: %v", err)
			}

			server.Fault(sqsfake.Fault{Op: tc.op, Err: boom})
			if err := tc.call(a, name); err == nil {
				t.Errorf("%s returned nil while %s was failing", tc.name, tc.op)
			}
		})
	}
}

// TestPurgeAndStatsOnAQueueThatIsNotThere.
//
// The URL lookup fails first, and the failure has to reach the caller: a
// Stats that answered zeroes for a queue nobody made would say the
// subscription is drained when it does not exist.
func TestPurgeAndStatsOnAQueueThatIsNotThere(t *testing.T) {
	t.Parallel()
	h := open(t, func(c *sq.Config) { c.AutoCreate = false })
	a := adminOf(t, h)

	if err := a.Purge(t.Context(), "never-created", "workers"); err == nil {
		t.Error("Purge on a queue that does not exist returned nil")
	}
	stats, err := a.Stats(t.Context(), "never-created", "workers")
	if err == nil {
		t.Errorf("Stats on a queue that does not exist answered %+v; zeroes would say a subscription that "+
			"does not exist is drained", stats)
	}
}

// TestListTopicsWalksEveryPageAndStripsThePrefix.
//
// The listing is paginated, and a driver that read the first page only would
// report a subset as though it were everything -- which is worse than an
// error, because nothing looks wrong.
func TestListTopicsWalksEveryPageAndStripsThePrefix(t *testing.T) {
	t.Parallel()
	h := open(t, func(c *sq.Config) { c.QueuePrefix = "app-" })
	a := adminOf(t, h)

	want := map[string]bool{}
	for range 3 {
		n := topic(t, "admin")
		if err := a.EnsureTopic(t.Context(), mq.TopicSpec{Name: n}); err != nil {
			t.Fatalf("EnsureTopic(%s): %v", n, err)
		}
		want[n] = true
	}

	got := map[string]bool{}
	for name, err := range a.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		got[name] = true
	}
	for n := range want {
		if !got[n] {
			t.Errorf("ListTopics did not report %q; it reported %v, and the configured prefix has to be "+
				"stripped back off", n, got)
		}
	}
}

// TestAListedTopicCanBeUsedWithTheSurfaceItCameFrom is the regression for a
// listing whose output no other Admin call would accept.
//
// Every other method PREPENDS the configured QueuePrefix to the topic it is
// given. ListTopics yielded the raw queue NAME, prefix and all -- so a
// caller who listed the topics and passed one back was looking up
// `app-app-orders`, and was told the queue does not exist. With a prefix
// configured the listing was decorative.
//
// The test is a ROUND TRIP rather than a string comparison, because the
// comparison is the thing that was wrong: a test asserting ListTopics
// returned what EnsureTopic was given would have been written against the
// same misunderstanding.
func TestAListedTopicCanBeUsedWithTheSurfaceItCameFrom(t *testing.T) {
	t.Parallel()
	h := open(t, func(c *sq.Config) { c.QueuePrefix = "app-" })
	a := adminOf(t, h)
	name := topic(t, "roundtrip")
	if err := a.EnsureTopic(t.Context(), mq.TopicSpec{Name: name}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}

	var listed []string
	for got, err := range a.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics yielded an error: %v", err)
		}
		listed = append(listed, got)
	}
	if len(listed) == 0 {
		t.Fatal("ListTopics reported nothing for a topic it had just been asked to create")
	}
	for _, got := range listed {
		if _, err := a.Stats(t.Context(), got, "workers"); err != nil {
			t.Errorf("Stats(%q) on a topic ListTopics reported is %v; the listing's output has to be "+
				"addressable by the surface it came from, and with a prefix configured this looked up the "+
				"prefix twice", got, err)
		}
	}
}

// TestListTopicsReportsAFailureRatherThanEndingQuietly.
func TestListTopicsReportsAFailureRatherThanEndingQuietly(t *testing.T) {
	t.Parallel()
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	a := adminOf(t, h)
	server.Fault(sqsfake.Fault{Op: "ListQueues", Err: errors.New("the endpoint is unavailable")})

	var last error
	var n int
	for _, err := range a.ListTopics(t.Context()) {
		n++
		last = err
	}
	if last == nil {
		t.Errorf("ListTopics over a failing endpoint yielded %d topics and no error; an empty iteration "+
			"reads as \"this account has no queues\"", n)
	}
}

// TestAdminIsRefusedOnAClosedBackend.
func TestAdminIsRefusedOnAClosedBackend(t *testing.T) {
	t.Parallel()
	h := open(t)
	if err := h.back.Close(); err != nil {
		t.Fatalf("closing the backend: %v", err)
	}

	if _, err := h.back.Admin(); !errors.Is(err, mq.ErrClosed) {
		t.Errorf("Admin() on a closed backend is %v, want ErrClosed", err)
	}
}

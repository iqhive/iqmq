package snssqs_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// TestAdminEnsureIsIdempotent holds the specification's "ensure operations are
// idempotent" across all three resources a fan-out has.
func TestAdminEnsureIsIdempotent(t *testing.T) {
	h := open(t)
	tp := topic(t, "ensure")
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	for range 3 {
		if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: tp}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
		if err := admin.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: tp, Name: "s"}); err != nil {
			t.Fatalf("EnsureSubscription: %v", err)
		}
	}
	arn := h.sns.TopicARN(h.topicName(tp))
	if arn == "" {
		t.Fatalf("the topic was not created")
	}
	if got := h.sns.Subscriptions(arn); got != 1 {
		t.Fatalf("the topic has %d subscription(s) after three EnsureSubscription calls, want 1", got)
	}
	if got := h.queues.Depth(h.queue(tp, "s")); got != 0 {
		t.Fatalf("the subscription's queue was not created (depth %d)", got)
	}
}

// TestAdminListAndDeleteTopics holds the rest of the topic surface, and the
// one thing about DeleteTopic that is the service's rather than this driver's:
// it removes the subscriptions and leaves the queues.
func TestAdminListAndDeleteTopics(t *testing.T) {
	h := open(t)
	tp := topic(t, "list")
	h.provision(tp, "s")
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	found := false
	for name, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		if name == h.topicName(tp) {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListTopics does not name %q", h.topicName(tp))
	}

	// A message on the queue, so that the survival of the queue is
	// observable rather than theoretical.
	h.publish(tp, &mq.Message{Body: []byte("kept")})
	if err := admin.DeleteTopic(t.Context(), tp); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := h.sns.TopicARN(h.topicName(tp)); got != "" {
		t.Fatalf("the topic still exists after DeleteTopic")
	}
	if got := h.queues.Bodies(h.queue(tp, "s")); len(got) != 1 || got[0] != "kept" {
		t.Fatalf("the subscription's queue holds %v after the topic was deleted, want the message it already had: a portable DeleteTopic that destroyed undelivered messages would be the one operation that loses data silently", got)
	}
	// The cache was dropped with the topic, so a publisher opened afterwards
	// re-resolves rather than naming an ARN that no longer exists.
	if _, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp}); err != nil {
		t.Fatalf("opening a publisher after the topic was deleted and AutoCreate is on: %v", err)
	}
}

// TestAdminDeleteSubscriptionLeavesTheQueue is.
//
// The queue may hold messages nobody has consumed, so a portable "delete the
// subscription" that destroyed them would lose data silently. After the call
// the queue receives nothing new and whatever it holds is still there.
func TestAdminDeleteSubscriptionLeavesTheQueue(t *testing.T) {
	h := open(t)
	tp := topic(t, "unsub")
	h.provision(tp, "a", "b")
	h.publish(tp, &mq.Message{Body: []byte("first")})

	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), tp, "a"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if got := h.queues.Bodies(h.queue(tp, "a")); len(got) != 1 || got[0] != "first" {
		t.Fatalf("the queue holds %v after its subscription was deleted, want the message it already had", got)
	}
	// Nothing new arrives for the deleted subscription, and the other one is
	// untouched.
	h.publish(tp, &mq.Message{Body: []byte("second")})
	if got := h.queues.Depth(h.queue(tp, "a")); got != 1 {
		t.Fatalf("the deleted subscription's queue holds %d message(s), want the 1 it already had", got)
	}
	if got := h.queues.Depth(h.queue(tp, "b")); got != 2 {
		t.Fatalf("the surviving subscription's queue holds %d message(s), want 2", got)
	}
}

// TestAdminPurgeAndStatsActOnOneSubscription is what a fan-out makes of the
// two operations the specification defines per subscription.
//
// A topic has no depth of its own, and adding its queues' depths together
// would report a number no consumer experiences.
func TestAdminPurgeAndStatsActOnOneSubscription(t *testing.T) {
	h := open(t)
	tp := topic(t, "stats")
	h.provision(tp, "a", "b")
	for i := range 3 {
		h.publish(tp, &mq.Message{Body: []byte{byte('a' + i)}})
	}
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	stats, err := admin.Stats(t.Context(), tp, "a")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !stats.Depth.Known || stats.Depth.Value != 3 {
		t.Fatalf("Stats reports depth %d (known %v), want 3", stats.Depth.Value, stats.Depth.Known)
	}
	if err := admin.Purge(t.Context(), tp, "a"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if got := h.depth(tp, "a"); got != 0 {
		t.Fatalf("the purged subscription's queue holds %d message(s)", got)
	}
	if got := h.depth(tp, "b"); got != 3 {
		t.Fatalf("purging one subscription emptied another: b holds %d message(s), want 3", got)
	}
}

// TestTypedAdminNamesTheTopology is the bridge to `drivers/sqs`: with the
// queue name in hand a caller reaches that driver's own typed administration
// for the redrive policy and the message-move task.
func TestTypedAdminNamesTheTopology(t *testing.T) {
	h := open(t)
	tp := topic(t, "typed")
	h.provision(tp, "s")
	ta, err := h.back.TypedAdmin()
	if err != nil {
		t.Fatalf("TypedAdmin: %v", err)
	}
	name, url, arn, err := ta.Queue(t.Context(), tp, "s")
	if err != nil {
		t.Fatalf("Queue: %v", err)
	}
	if want := h.queue(tp, "s"); name != want {
		t.Errorf("Queue name = %q, want %q", name, want)
	}
	if url == "" || arn == "" {
		t.Errorf("Queue returned url %q and arn %q", url, arn)
	}
	topicARN, err := ta.TopicARN(t.Context(), tp)
	if err != nil {
		t.Fatalf("TopicARN: %v", err)
	}
	if topicARN != h.sns.TopicARN(h.topicName(tp)) {
		t.Errorf("TopicARN = %q", topicARN)
	}
	subARN, err := ta.SubscriptionARN(t.Context(), tp, "s")
	if err != nil {
		t.Fatalf("SubscriptionARN: %v", err)
	}
	if !strings.HasPrefix(subARN, topicARN) {
		t.Errorf("the subscription ARN %q is not under the topic ARN %q", subARN, topicARN)
	}
	attrs, err := ta.Attributes(t.Context(), tp, "s")
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if attrs["RawMessageDelivery"] != "true" {
		t.Errorf("the subscription this driver created has RawMessageDelivery %q, want true", attrs["RawMessageDelivery"])
	}
	endpoints, err := ta.Subscriptions(t.Context(), tp)
	if err != nil {
		t.Fatalf("Subscriptions: %v", err)
	}
	if len(endpoints) != 1 || endpoints[0] != arn {
		t.Errorf("Subscriptions = %v, want the one queue ARN %q", endpoints, arn)
	}
	// The inner driver is reachable, which is where everything the QUEUE
	// owns lives.
	if h.back.SQS() == nil {
		t.Fatalf("the inner SQS backend is not reachable")
	}
	if got := h.back.SQS().Info().Name; got != "sqs" {
		t.Errorf("the inner backend names the driver %q", got)
	}
}

// TestEnableRawDeliveryRepairsASubscriptionTheDriverRefuses is the other end
// of the headline refusal.
//
// The driver never flips RawMessageDelivery on by itself — a subscription
// somebody else made without it has consumers that expect SNS's envelope — so
// there has to be a place where the decision is a person's. This is it, and
// the test is the whole round: refused, repaired, consumed.
func TestEnableRawDeliveryRepairsASubscriptionTheDriverRefuses(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{Queues: queues})
	h := openOn(t, server, queues)
	tp := topic(t, "repair")
	h.provision(tp, "s")
	_, _, _, subARN, ok := sns.SubscriptionForTest(h.back, tp, "s")
	if !ok {
		t.Fatalf("the driver did not resolve a subscription")
	}
	server.SetRaw(subARN, false)

	broken := openOn(t, server, queues)
	if _, err := broken.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"}); err == nil {
		t.Fatalf("a consumer opened on a subscription without raw delivery")
	}
	ta, err := broken.back.TypedAdmin()
	if err != nil {
		t.Fatalf("TypedAdmin: %v", err)
	}
	if err := ta.EnableRawDelivery(t.Context(), tp, "s"); err != nil {
		t.Fatalf("EnableRawDelivery on the very subscription OpenSource refuses: %v", err)
	}
	c, err := broken.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
	if err != nil {
		t.Fatalf("opening a consumer after the repair: %v", err)
	}
	defer func() { _ = c.Close() }()
	broken.publish(tp, &mq.Message{Body: []byte("after")})
	d := broken.receiveOne(c, 5*time.Second)
	if string(d.Body) != "after" {
		t.Fatalf("the repaired subscription delivered %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestSetFilterPolicyWritesTheScopeAndThePolicy holds the runtime half of the conditional claim, including the order the two attributes are written
// in.
//
// The scope goes first, so that a policy is never in force under the wrong
// one: a policy written against header names and evaluated against a JSON body
// matches nothing, and a subscriber that silently received nothing is the
// worst of the outcomes here.
func TestSetFilterPolicyWritesTheScopeAndThePolicy(t *testing.T) {
	h := open(t)
	tp := topic(t, "setfilter")
	h.provision(tp, "s")
	c := h.consumer(tp, "s", 4)
	ta, err := h.back.TypedAdmin()
	if err != nil {
		t.Fatalf("TypedAdmin: %v", err)
	}
	if err := ta.SetFilterPolicy(t.Context(), tp, "s", `{"region":["eu"]}`, sns.FilterScopeAttributes); err != nil {
		t.Fatalf("SetFilterPolicy: %v", err)
	}
	attrs, err := ta.Attributes(t.Context(), tp, "s")
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if attrs["FilterPolicy"] != `{"region":["eu"]}` {
		t.Errorf("FilterPolicy = %q", attrs["FilterPolicy"])
	}
	if attrs["FilterPolicyScope"] != "MessageAttributes" {
		t.Errorf("FilterPolicyScope = %q", attrs["FilterPolicyScope"])
	}

	var eu, us mq.Message
	eu.Body = []byte("eu")
	eu.Headers.Set("region", []byte("eu"))
	us.Body = []byte("us")
	us.Headers.Set("region", []byte("us"))
	h.publish(tp, &eu)
	h.publish(tp, &us)
	d := h.receiveOne(c, 5*time.Second)
	if string(d.Body) != "eu" {
		t.Fatalf("the filtered subscription received %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	h.expectNothing(c, 500*time.Millisecond, "unmatched publication")

	// An empty policy removes the filter, which is how a subscription goes
	// back to receiving everything.
	if err := ta.SetFilterPolicy(t.Context(), tp, "s", "", sns.FilterScopeAttributes); err != nil {
		t.Fatalf("SetFilterPolicy(\"\"): %v", err)
	}
	h.publish(tp, &us)
	back := h.receiveOne(c, 5*time.Second)
	if string(back.Body) != "us" {
		t.Fatalf("after the filter was removed the subscription received %q", back.Body)
	}
	if err := back.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// And the refusals: a policy the service would reject is refused here,
	// before it can silently stop a subscription delivering.
	if err := ta.SetFilterPolicy(t.Context(), tp, "s", "not json", sns.FilterScopeAttributes); err == nil {
		t.Fatalf("a policy that is not JSON was accepted")
	}
	if err := ta.SetFilterPolicy(t.Context(), tp, "s", `{"a":["b"]}`, sns.FilterScope(9)); err == nil {
		t.Fatalf("a scope outside the enumeration was accepted")
	}
}

// TestAdminRefusesAfterClose holds a closed client's administration is
// ErrClosed rather than a request.
func TestAdminRefusesAfterClose(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{})
	server := snsfake.New(snsfake.Options{Queues: queues})
	back, err := sns.BackendWithAPI(t.Context(), server, queues, queues, server, ownBorrowed(), sns.Config{
		Region: "fake-region-1", AccountID: "1", AutoCreate: true, WaitTime: time.Second,
	})
	if err != nil {
		t.Fatalf("BackendWithAPI: %v", err)
	}
	if err := back.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := back.Admin(); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Admin on a closed backend reported %v, want ErrClosed", err)
	}
	if _, err := back.TypedAdmin(); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("TypedAdmin on a closed backend reported %v, want ErrClosed", err)
	}
}

// TestPingReachesBothServices is for a driver that is two drivers: a
// client that can publish and not consume is not a healthy fan-out client.
func TestPingReachesBothServices(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{})
	server := snsfake.New(snsfake.Options{Queues: queues})
	h := openOn(t, server, queues)
	if err := h.client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	// Fault the SNS half.
	server.Fault(snsfake.Fault{Op: "ListTopics", Err: errFaulted, Count: 1})
	if err := h.client.Ping(t.Context()); err == nil {
		t.Fatalf("Ping succeeded with the SNS half failing")
	}
	// Fault the SQS half, which is the inner driver's own Ping.
	queues.Fault(sqsfake.Fault{Op: "ListQueues", Err: errFaulted, Count: 1})
	if err := h.client.Ping(t.Context()); err == nil {
		t.Fatalf("Ping succeeded with the SQS half failing: a client that can publish and not consume is not a healthy fan-out client")
	}
	if err := h.client.Ping(t.Context()); err != nil {
		t.Fatalf("Ping after both faults cleared: %v", err)
	}
}

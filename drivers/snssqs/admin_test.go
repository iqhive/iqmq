package snssqs_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssns "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

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
		if name == tp {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListTopics does not name %q", tp)
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

// TestListedTopicsAreTheTopicsTheSurfaceTakes: ListTopics yielded SNS topic
// names, with Config.TopicPrefix and the "_u" and "_-" escapes still in them,
// while every other Admin method maps the portable topic it is given. So a
// name the listing reported could not be handed back: DeleteTopic of it
// mapped it a second time and answered NotFound, and `iqmq topology apply`
// read every topic under a DSN prefix, or holding "_" or "/", as missing
// after it had ensured it. The listing now yields the portable topic, and
// leaves out a name this configuration did not produce.
func TestListedTopicsAreTheTopicsTheSurfaceTakes(t *testing.T) {
	h := open(t, func(c *sns.Config) { c.TopicPrefix = "app-" })
	want := []string{topic(t, "plain"), topic(t, "under_score"), topic(t, "a") + "/b"}
	for _, tp := range want {
		h.provision(tp)
	}
	// Another deployment's topic in the same account, and one under this
	// prefix that no portable topic maps to ("_x" is no escape).
	foreign := "other-" + want[0]
	for _, name := range []string{foreign, "app-bad_x"} {
		if _, err := h.sns.CreateTopic(t.Context(), &awssns.CreateTopicInput{Name: aws.String(name)}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
	}
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	var listed []string
	for name, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		listed = append(listed, name)
	}
	slices.Sort(listed)
	slices.Sort(want)
	if !slices.Equal(listed, want) {
		t.Fatalf("ListTopics reported %q, want exactly the portable topics %q", listed, want)
	}
	for _, name := range listed {
		if err := admin.DeleteTopic(t.Context(), name); err != nil {
			t.Fatalf("DeleteTopic(%q), a name ListTopics reported: %v", name, err)
		}
		if arn := h.sns.TopicARN(h.topicName(name)); arn != "" {
			t.Errorf("DeleteTopic(%q) answered nil and %s is still there", name, arn)
		}
	}
	if h.sns.TopicARN(foreign) == "" {
		t.Errorf("the other deployment's topic is gone")
	}
}

// TestDeletingAListedTopicDeletesThatTopic: with no prefix the portable topics
// "<b>_" and "<b>_u" are the SNS topics "<b>_u" and "<b>_uu". The listing
// reported the first as "<b>_u", which is the second's portable name, so a
// cleanup that deleted what it listed deleted the other topic.
func TestDeletingAListedTopicDeletesThatTopic(t *testing.T) {
	h := open(t)
	base := topic(t, "wrong")
	a, b := base+"_", base+"_u"
	h.provision(a)
	h.provision(b)
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	listed := map[string]bool{}
	for name, err := range admin.ListTopics(t.Context()) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		listed[name] = true
	}
	if !listed[a] || !listed[b] {
		t.Fatalf("ListTopics reported %v, want both %q and %q", listed, a, b)
	}
	if err := admin.DeleteTopic(t.Context(), a); err != nil {
		t.Fatalf("DeleteTopic(%q): %v", a, err)
	}
	if h.sns.TopicARN(h.topicName(a)) != "" || h.sns.TopicARN(h.topicName(b)) == "" {
		t.Errorf("DeleteTopic(%q) did not delete exactly SNS topic %q (that one there: %v, %q there: %v)",
			a, h.topicName(a), h.sns.TopicARN(h.topicName(a)) != "", h.topicName(b), h.sns.TopicARN(h.topicName(b)) != "")
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

// TestSetFilterPolicyWritesTheScopeAndThePolicy holds the runtime half of the
// conditional claim. The order of the writes, which keeps a policy from ever
// being in force under the wrong scope, is
// TestSetFilterPolicyNeverDeliversLessWhileTheScopeChanges's.
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

// TestEnsureSubscriptionCreatesTheQueueWithAutoCreateOff holds the explicit
// form of the opt-in to what the driver page says it is.
//
// AutoCreate is off by default and Admin().EnsureSubscription is the explicit
// form, and its doc says it creates the subscription's queue and the SNS
// subscription. It resolved the queue through the inner driver, whose
// AutoCreate is this driver's, so with the default configuration it created
// the topic in EnsureTopic and then refused the queue. The queue it creates
// is the inner driver's, with that driver's attributes.
func TestEnsureSubscriptionCreatesTheQueueWithAutoCreateOff(t *testing.T) {
	h := open(t, func(c *sns.Config) { c.AutoCreate = false })
	tp := topic(t, "explicit")
	admin, err := h.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: tp}); err != nil {
		t.Fatalf("EnsureTopic with AutoCreate off: %v", err)
	}
	spec := mq.SubscriptionSpec{Topic: tp, Name: "s"}
	if err := admin.EnsureSubscription(t.Context(), spec); err != nil {
		t.Fatalf("EnsureSubscription with AutoCreate off: %v (code %v): the explicit form refused to create the queue", err, mq.CodeOf(err))
	}
	url := h.queues.QueueURL(h.queue(tp, "s"))
	if url == "" {
		t.Fatalf("no queue %s after EnsureSubscription", h.queue(tp, "s"))
	}
	attrs, err := h.queues.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url), AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameVisibilityTimeout},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	if got := attrs.Attributes[string(sqstypes.QueueAttributeNameVisibilityTimeout)]; got != "30" {
		t.Errorf("the created queue's VisibilityTimeout is %q, want the inner driver's 30 from the 30s AckDeadline", got)
	}
	if err := admin.EnsureSubscription(t.Context(), spec); err != nil {
		t.Fatalf("EnsureSubscription again: %v", err)
	}
	c := h.consumer(tp, "s", 1)
	h.publish(tp, &mq.Message{Body: []byte("explicit")})
	if d := h.receiveOne(c, 5*time.Second); string(d.Body) != "explicit" {
		t.Fatalf("the ensured subscription delivered %q", d.Body)
	}
}

// TestDeleteSubscriptionOfAMissingSubscriptionProvisionsNothing holds the
// portable rule that a destructive operation is explicit and never implies a
// creation.
//
// DeleteSubscription resolved the queue through the inner driver, which
// creates a missing queue whenever AutoCreate is on: deleting a subscription
// that never existed, a typo or a repeated delete, created its queue, which
// nothing then removes, and was refused saying AutoCreate was off. The answer
// is a NotFound that names no setting, and no queue and no SNS subscription
// is created, with AutoCreate on or off and whether or not the queue exists.
func TestDeleteSubscriptionOfAMissingSubscriptionProvisionsNothing(t *testing.T) {
	for _, auto := range []bool{true, false} {
		t.Run(map[bool]string{true: "autocreate", false: "no-autocreate"}[auto], func(t *testing.T) {
			h := open(t, func(c *sns.Config) { c.AutoCreate = auto })
			tp := topic(t, "ghost")
			admin, err := h.client.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: tp}); err != nil {
				t.Fatalf("EnsureTopic: %v", err)
			}
			topicARN := h.sns.TopicARN(h.topicName(tp))
			refused := func(sub string) {
				t.Helper()
				created := h.queues.Calls("CreateQueue")
				err := admin.DeleteSubscription(t.Context(), tp, sub)
				if mq.CodeOf(err) != mq.CodeNotFound {
					t.Errorf("DeleteSubscription(%s) of a subscription that does not exist = %v (code %v), want CodeNotFound",
						sub, err, mq.CodeOf(err))
				}
				if err != nil && strings.Contains(err.Error(), "AutoCreate") {
					t.Errorf("the refusal names AutoCreate, which a delete has nothing to do with: %v", err)
				}
				if n := h.queues.Calls("CreateQueue"); n != created {
					t.Errorf("DeleteSubscription(%s) created a queue (CreateQueue calls %d -> %d)", sub, created, n)
				}
				if n := h.sns.Subscriptions(topicARN); n != 0 {
					t.Errorf("DeleteSubscription(%s) left the topic with %d subscription(s), want none", sub, n)
				}
			}
			refused("ghost")
			if url := h.queues.QueueURL(h.queue(tp, "ghost")); url != "" {
				t.Fatalf("DeleteSubscription of a subscription that never existed created its queue %s", url)
			}
			// The queue exists and nothing delivers into it.
			if _, err := h.queues.CreateQueue(t.Context(), &awssqs.CreateQueueInput{QueueName: aws.String(h.queue(tp, "orphan"))}); err != nil {
				t.Fatalf("CreateQueue: %v", err)
			}
			refused("orphan")
		})
	}
}

// TestABackendWithoutAccountIDFindsATopicItDidNotResolve holds a backend with
// no Config.AccountID to that field's doc: it resolves a topic ARN by
// creating or listing.
//
// It never listed. A call that may not create a topic built its ARN from the
// AccountID, and without one refused, naming AutoCreate as a way out, so a
// cleanup job or a second process could not delete a topic or a subscription
// it had not resolved itself, AutoCreate on or not. The topic is past the
// first page of the listing here, as it is in an account with more than a
// hundred topics.
func TestABackendWithoutAccountIDFindsATopicItDidNotResolve(t *testing.T) {
	for _, auto := range []bool{true, false} {
		t.Run(map[bool]string{true: "autocreate", false: "no-autocreate"}[auto], func(t *testing.T) {
			first := open(t)
			for i := range 150 {
				name := "other-" + strconv.Itoa(i)
				if _, err := first.sns.CreateTopic(t.Context(), &awssns.CreateTopicInput{Name: aws.String(name)}); err != nil {
					t.Fatalf("CreateTopic(%s): %v", name, err)
				}
			}
			tp := topic(t, "noacct")
			first.provision(tp, "a", "b")

			second := openOn(t, first.sns, first.queues, func(c *sns.Config) { c.AccountID = ""; c.AutoCreate = auto })
			admin, err := second.client.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			ta, err := second.back.TypedAdmin()
			if err != nil {
				t.Fatalf("TypedAdmin: %v", err)
			}
			if subs, err := ta.Subscriptions(t.Context(), tp); err != nil || len(subs) != 2 {
				t.Errorf("Subscriptions with no AccountID = %v, %v; want the two queues", subs, err)
			}
			if err := admin.DeleteSubscription(t.Context(), tp, "a"); err != nil {
				t.Errorf("DeleteSubscription of an existing subscription with no AccountID: %v (code %v)", err, mq.CodeOf(err))
			}
			if err := admin.DeleteTopic(t.Context(), tp); err != nil {
				t.Errorf("DeleteTopic of an existing topic with no AccountID: %v (code %v)", err, mq.CodeOf(err))
			}
			if arn := first.sns.TopicARN(first.topicName(tp)); arn != "" {
				t.Errorf("the topic %s still exists", arn)
			}
			if err := admin.DeleteTopic(t.Context(), topic(t, "never")); mq.CodeOf(err) != mq.CodeNotFound {
				t.Errorf("DeleteTopic of a topic the account does not have = %v (code %v), want CodeNotFound", err, mq.CodeOf(err))
			}
		})
	}
}

// publishBetween is the fake with a publication made after each filter
// attribute write once armed, which is where another process's publication
// lands on the real service while a SetFilterPolicy is under way.
type publishBetween struct {
	*snsfake.Server
	topicARN string
	message  *awssns.PublishInput
	writes   []string
}

func (p *publishBetween) SetSubscriptionAttributes(ctx context.Context, in *awssns.SetSubscriptionAttributesInput, opts ...func(*awssns.Options)) (*awssns.SetSubscriptionAttributesOutput, error) {
	out, err := p.Server.SetSubscriptionAttributes(ctx, in, opts...)
	if err != nil || p.message == nil {
		return out, err
	}
	p.writes = append(p.writes, aws.ToString(in.AttributeName)+"="+aws.ToString(in.AttributeValue))
	if _, err := p.Publish(ctx, p.message); err != nil {
		return nil, err
	}
	return out, nil
}

// TestSetFilterPolicyNeverDeliversLessWhileTheScopeChanges holds the order
// of SetFilterPolicy's writes.
//
// The scope was written first, so the OLD policy was in force under the NEW
// scope until the policy write: changing a subscription from a header filter
// to a body filter dropped a message both configurations deliver. The old
// policy now goes first, so every publication made while the change is under
// way reaches the queue. A change of the policy alone opens no such window,
// so a message both policies keep out stays out throughout.
func TestSetFilterPolicyNeverDeliversLessWhileTheScopeChanges(t *testing.T) {
	order := func() *awssns.PublishInput {
		return &awssns.PublishInput{
			Message: aws.String(`{"type":"order"}`),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"kind": {DataType: aws.String("String"), StringValue: aws.String("order")},
			},
		}
	}
	setup := func(t *testing.T) (*harness, *publishBetween, *sns.TypedAdmin, string) {
		t.Helper()
		queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
		server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
		api := &publishBetween{Server: server}
		h := openOver(t, api, server, queues)
		tp := topic(t, "scopewin")
		h.provision(tp, "s")
		ta, err := h.back.TypedAdmin()
		if err != nil {
			t.Fatalf("TypedAdmin: %v", err)
		}
		if err := ta.SetFilterPolicy(t.Context(), tp, "s", `{"kind":["order"]}`, sns.FilterScopeAttributes); err != nil {
			t.Fatalf("SetFilterPolicy (attributes): %v", err)
		}
		arn, err := ta.TopicARN(t.Context(), tp)
		if err != nil {
			t.Fatalf("TopicARN: %v", err)
		}
		api.topicARN = arn
		return h, api, ta, tp
	}

	t.Run("scope and policy", func(t *testing.T) {
		h, api, ta, tp := setup(t)
		api.message = order()
		api.message.TopicArn = aws.String(api.topicARN)
		if err := ta.SetFilterPolicy(t.Context(), tp, "s", `{"type":["order"]}`, sns.FilterScopeBody); err != nil {
			t.Fatalf("SetFilterPolicy (body): %v", err)
		}
		if got, want := h.depth(tp, "s"), len(api.writes); got != want {
			t.Errorf("the queue holds %d of the %d message(s) published during the change (after the writes %q): "+
				"each matches the configuration before the call and the one after it", got, want, api.writes)
		}
		attrs, err := ta.Attributes(t.Context(), tp, "s")
		if err != nil {
			t.Fatalf("Attributes: %v", err)
		}
		if attrs["FilterPolicy"] != `{"type":["order"]}` || attrs["FilterPolicyScope"] != "MessageBody" {
			t.Errorf("after the change FilterPolicy = %q, FilterPolicyScope = %q", attrs["FilterPolicy"], attrs["FilterPolicyScope"])
		}
	})

	t.Run("policy alone", func(t *testing.T) {
		h, api, ta, tp := setup(t)
		api.message = &awssns.PublishInput{
			TopicArn: aws.String(api.topicARN),
			Message:  aws.String("neither"),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"kind": {DataType: aws.String("String"), StringValue: aws.String("refund")},
			},
		}
		if err := ta.SetFilterPolicy(t.Context(), tp, "s", `{"kind":["invoice"]}`, sns.FilterScopeAttributes); err != nil {
			t.Fatalf("SetFilterPolicy (attributes again): %v", err)
		}
		if len(api.writes) == 0 {
			t.Fatal("no attribute was written")
		}
		if got := h.depth(tp, "s"); got != 0 {
			t.Errorf("the queue holds %d message(s) that neither policy delivers, published after the writes %q: "+
				"a change of the policy alone removed the filter", got, api.writes)
		}
	})
}

// failFilterWrites is the fake with the filter writes of one SetFilterPolicy
// failing as fail says, which is how the service refuses a policy the new
// scope cannot express, or a write times out.
type failFilterWrites struct {
	*snsfake.Server
	fail   func(name, value string) bool
	writes []string
}

func (f *failFilterWrites) SetSubscriptionAttributes(ctx context.Context, in *awssns.SetSubscriptionAttributesInput, opts ...func(*awssns.Options)) (*awssns.SetSubscriptionAttributesOutput, error) {
	name, value := aws.ToString(in.AttributeName), aws.ToString(in.AttributeValue)
	if f.fail != nil {
		f.writes = append(f.writes, name+"="+value)
		if f.fail(name, value) {
			return nil, &snstypes.InvalidParameterException{Message: aws.String("injected: " + name)}
		}
	}
	return f.Server.SetSubscriptionAttributes(ctx, in, opts...)
}

// TestAFailedScopeChangePutsTheOldFilterBack holds a scope-changing
// SetFilterPolicy that fails part way to the configuration it started from.
//
// The old policy is removed first so that the change never delivers less
// than either configuration. A write after the removal that failed, the
// scope or the new policy, left the subscription with no filter at all, so a
// subscriber a filter kept messages from received everything until somebody
// noticed. On LocalStack the ordinary case is a nested policy refused under
// the attributes scope. The old scope and policy are put back, and when that
// fails as well the error says the subscription is left with no filter.
func TestAFailedScopeChangePutsTheOldFilterBack(t *testing.T) {
	const oldPolicy, newPolicy = `{"kind":["order"]}`, `{"type":["order"]}`
	for _, tc := range []struct {
		name string
		fail func(name, value string) bool
		open bool
	}{
		{"the scope write fails", func(name, value string) bool { return name == "FilterPolicyScope" && value == "MessageBody" }, false},
		{"the policy write fails", func(name, value string) bool { return name == "FilterPolicy" && value == newPolicy }, false},
		{"the policy write and the rollback fail", func(name, value string) bool {
			return name == "FilterPolicyScope" && value == "MessageAttributes" || name == "FilterPolicy" && value == newPolicy
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
			api := &failFilterWrites{Server: server}
			h := openOver(t, api, server, queues)
			tp := topic(t, "rollback")
			h.provision(tp, "s")
			ta, err := h.back.TypedAdmin()
			if err != nil {
				t.Fatalf("TypedAdmin: %v", err)
			}
			if err := ta.SetFilterPolicy(t.Context(), tp, "s", oldPolicy, sns.FilterScopeAttributes); err != nil {
				t.Fatalf("SetFilterPolicy (attributes): %v", err)
			}
			api.fail = tc.fail
			err = ta.SetFilterPolicy(t.Context(), tp, "s", newPolicy, sns.FilterScopeBody)
			if err == nil {
				t.Fatalf("SetFilterPolicy succeeded with a failing write (writes %q)", api.writes)
			}
			if mq.CodeOf(err) != mq.CodeInvalidArgument {
				t.Errorf("the failed change reports %v (code %v), want the failing write's CodeInvalidArgument", err, mq.CodeOf(err))
			}
			api.fail = nil
			attrs, aerr := ta.Attributes(t.Context(), tp, "s")
			if aerr != nil {
				t.Fatalf("Attributes: %v", aerr)
			}
			says := strings.Contains(err.Error(), "may have no filter policy")
			switch {
			case tc.open && !says:
				t.Errorf("the rollback failed and the error does not say the subscription may have no filter: %v", err)
			case !tc.open && says:
				t.Errorf("the rollback succeeded and the error says the subscription may have no filter: %v", err)
			case !tc.open && (attrs["FilterPolicy"] != oldPolicy || attrs["FilterPolicyScope"] != "MessageAttributes"):
				t.Errorf("after the failed change FilterPolicy = %q, FilterPolicyScope = %q (writes %q); want the old %q under MessageAttributes",
					attrs["FilterPolicy"], attrs["FilterPolicyScope"], api.writes, oldPolicy)
			}
		})
	}
}

// TestDeleteSubscriptionOfAQueueDeletedByHandIsNotFound: with AutoCreate
// off the queue is resolved through the inner driver, which caches its URL,
// so in a process that resolved the queue before an operator deleted it the
// queue's ARN read failed, and DeleteSubscription answered CodeUnknown. It
// is CodeNotFound, as for a queue a lookup does not find. The SNS
// subscription stays, which the method's doc says.
func TestDeleteSubscriptionOfAQueueDeletedByHandIsNotFound(t *testing.T) {
	for _, auto := range []bool{true, false} {
		t.Run(map[bool]string{true: "autocreate", false: "no-autocreate"}[auto], func(t *testing.T) {
			h := open(t, func(c *sns.Config) { c.AutoCreate = auto })
			tp := topic(t, "byhand")
			h.provision(tp, "x")
			url := h.queues.QueueURL(h.queue(tp, "x"))
			if _, err := h.queues.DeleteQueue(t.Context(), &awssqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil {
				t.Fatalf("DeleteQueue: %v", err)
			}
			admin, err := h.client.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			created := h.queues.Calls("CreateQueue")
			if err := admin.DeleteSubscription(t.Context(), tp, "x"); mq.CodeOf(err) != mq.CodeNotFound {
				t.Errorf("DeleteSubscription after the queue was deleted by hand = %v (code %v), want CodeNotFound", err, mq.CodeOf(err))
			}
			if n := h.queues.Calls("CreateQueue"); n != created {
				t.Errorf("DeleteSubscription created a queue (CreateQueue calls %d -> %d)", created, n)
			}
		})
	}
}

// TestAListingRefusedForPermissionNamesAccountID: a backend with no
// AccountID finds a topic by ListTopics, a permission a deployment that never
// pings may not have granted. The refusal is CodeForbidden, as before, and
// names the setting that avoids the call.
func TestAListingRefusedForPermissionNamesAccountID(t *testing.T) {
	first := open(t)
	tp := topic(t, "denied")
	first.provision(tp, "a")
	second := openOn(t, first.sns, first.queues, func(c *sns.Config) { c.AccountID = "" })
	first.sns.Fault(snsfake.Fault{Op: "ListTopics", Count: -1, Err: &snstypes.AuthorizationErrorException{
		Message: aws.String("User is not authorized to perform: SNS:ListTopics"),
	}})
	admin, err := second.client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	err = admin.DeleteTopic(t.Context(), tp)
	if mq.CodeOf(err) != mq.CodeForbidden {
		t.Errorf("DeleteTopic with ListTopics refused = %v (code %v), want CodeForbidden", err, mq.CodeOf(err))
	}
	if err == nil || !strings.Contains(err.Error(), "Config.AccountID") {
		t.Errorf("the refusal does not name Config.AccountID: %v", err)
	}
}

// listedPending is the fake with every SQS subscription listed under the
// placeholder a listing uses for one that is pending confirmation.
type listedPending struct{ *snsfake.Server }

func (l listedPending) ListSubscriptionsByTopic(ctx context.Context, in *awssns.ListSubscriptionsByTopicInput, opts ...func(*awssns.Options)) (*awssns.ListSubscriptionsByTopicOutput, error) {
	out, err := l.Server.ListSubscriptionsByTopic(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	for i := range out.Subscriptions {
		out.Subscriptions[i].SubscriptionArn = aws.String("PendingConfirmation")
	}
	return out, nil
}

// TestAPendingSubscriptionFoundByListingIsReportedPending holds
// findSubscription to an ARN.
//
// A pending subscription's ARN field holds a placeholder, "pending
// confirmation" in Subscribe's answer and "PendingConfirmation" where
// LocalStack's ListSubscriptions reports one. Only the first was recognised,
// so a pending subscription listed in the second form would have been used as
// an ARN. It is reported as pending instead, CodeConflict.
func TestAPendingSubscriptionFoundByListingIsReportedPending(t *testing.T) {
	queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	server := snsfake.New(snsfake.Options{Region: "fake-region-1", AccountID: "000000000000", Queues: queues})
	provisioned := openOn(t, server, queues)
	tp := topic(t, "pending")
	provisioned.provision(tp, "s")

	h := openOver(t, listedPending{server}, server, queues)
	_, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
	if mq.CodeOf(err) != mq.CodeConflict {
		t.Errorf("opening a consumer on a subscription listed as pending = %v (code %v), want CodeConflict", err, mq.CodeOf(err))
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

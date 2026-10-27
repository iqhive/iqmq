package snssqs_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
)

// The name tests prove that the default mapping keeps two identities apart;
// these prove what that is FOR, through the driver and the in-process
// servers. Each one is a pair of subscriptions whose names the old
// "/"-flattened, hyphen-joined composition folded into one resource, and each
// asserts on deliveries rather than on names, because the failure a shared
// queue or a shared topic produces is a message reaching the wrong consumer
// with no error anywhere.

// TestTwoSubscriptionsWhoseNamesFlattenAlikeEachGetACopy is two subscriptions
// of one topic, "eu/billing" and "eu-billing".
//
// Sharing one queue, they are one SNS subscription, because the driver finds
// a subscription by its endpoint and the second EnsureSubscription finds the
// first. One publication is then one copy, which the two consumers compete
// for, where a fan-out promises each subscription its own.
func TestTwoSubscriptionsWhoseNamesFlattenAlikeEachGetACopy(t *testing.T) {
	h := open(t)
	tp := topic(t, "subflatten")
	h.provision(tp, "eu/billing", "eu-billing")
	slash := h.consumer(tp, "eu/billing", 4)
	hyphen := h.consumer(tp, "eu-billing", 4)

	h.publish(tp, &mq.Message{Body: []byte("both")})

	for _, c := range []struct {
		sub string
		con *mq.ConsumerHandle
	}{{"eu/billing", slash}, {"eu-billing", hyphen}} {
		d := h.receiveOne(c.con, 3*time.Second)
		if string(d.Body) != "both" {
			t.Fatalf("subscription %q received %q", c.sub, d.Body)
		}
		if err := d.Ack(); err != nil {
			t.Fatalf("acking subscription %q's copy: %v", c.sub, err)
		}
	}
}

// TestASubscriptionDoesNotReceiveAnotherTopicsMessages is ("X-eu", "billing")
// and ("X", "eu-billing"): two topics, two subscriptions, and under a
// hyphen-joined composition one queue subscribed to BOTH topics.
//
// It runs under Config.NoQueuePolicy because that is where nothing else
// would catch a shared queue. When the driver writes the queue's policy, the
// policy lets only the first topic deliver, so the second pair is refused at
// open. Without it (or with a policy that admits both topics) the queue
// receives both topics' publications, and a consumer of one subscription
// settles the other's.
func TestASubscriptionDoesNotReceiveAnotherTopicsMessages(t *testing.T) {
	h := open(t, func(c *sns.Config) { c.NoQueuePolicy = true })
	base := topic(t, "joinmoves")
	h.provision(base+"-eu", "billing")
	h.provision(base, "eu-billing")
	billing := h.consumer(base+"-eu", "billing", 4)

	h.publish(base, &mq.Message{Body: []byte("for-eu-billing")})

	h.expectNothing(billing, time.Second, "publication to another topic")
	own := h.consumer(base, "eu-billing", 4)
	d := h.receiveOne(own, 3*time.Second)
	if string(d.Body) != "for-eu-billing" {
		t.Fatalf("subscription eu-billing received %q", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("acking: %v", err)
	}
}

// TestTwoTopicsWhoseNamesFlattenAlikeAreTwoSNSTopics is "X/eu" and "X-eu",
// one level up: sharing an SNS topic, every subscription of either receives
// every publication to both, even though each subscription has a queue of its
// own.
func TestTwoTopicsWhoseNamesFlattenAlikeAreTwoSNSTopics(t *testing.T) {
	h := open(t)
	base := topic(t, "topicflatten")
	slash, hyphen := base+"/eu", base+"-eu"
	h.provision(slash, "a")
	h.provision(hyphen, "b")
	b := h.consumer(hyphen, "b", 4)
	a := h.consumer(slash, "a", 4)

	h.publish(slash, &mq.Message{Body: []byte("for-slash")})

	d := h.receiveOne(a, 3*time.Second)
	if string(d.Body) != "for-slash" {
		t.Fatalf("the subscriber of %q received %q", slash, d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("acking: %v", err)
	}
	h.expectNothing(b, time.Second, "publication to "+slash)
}

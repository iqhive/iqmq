package snssqs_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
)

// The backend remembers what it resolved, and something other than this
// backend can change the topology underneath it: a second process, `iqmq
// topology`, the console, Terraform. These tests are about the calls whose
// whole job is to find out, Ensure and the open, asking the services rather
// than the memory.

// TestEnsureSubscriptionRepairsWhatAnotherProcessRemoved: the backend kept
// every (topic, subscription) it resolved, and only its own DeleteTopic,
// DeleteSubscription and EnableRawDelivery dropped an entry. So once a
// process had ensured a subscription, EnsureSubscription answered nil from
// that memory for ever, even after another process had removed the SNS
// subscription, and recreated nothing: the call a program makes before it
// publishes repaired nothing, every publication went to the other
// subscriptions only, and this one's consumers received nothing until the
// process restarted.
func TestEnsureSubscriptionRepairsWhatAnotherProcessRemoved(t *testing.T) {
	for _, auto := range []bool{true, false} {
		t.Run(map[bool]string{true: "autocreate", false: "no-autocreate"}[auto], func(t *testing.T) {
			a := open(t, func(c *sns.Config) { c.AutoCreate = auto })
			tp := topic(t, "ensure-again")
			a.provision(tp, "s")
			b := openOn(t, a.sns, a.queues, func(c *sns.Config) { c.AutoCreate = auto })
			adminB, err := b.client.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if err := adminB.DeleteSubscription(t.Context(), tp, "s"); err != nil {
				t.Fatalf("DeleteSubscription in the other process: %v", err)
			}
			adminA, err := a.client.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if err := adminA.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: tp, Name: "s"}); err != nil {
				t.Fatalf("EnsureSubscription: %v", err)
			}
			if n := a.sns.Subscriptions(a.sns.TopicARN(a.topicName(tp))); n != 1 {
				t.Fatalf("EnsureSubscription(%q, %q) answered nil, and the topic has %d subscriptions: the "+
					"answer came from what the backend remembered", tp, "s", n)
			}
			cons := a.consumer(tp, "s", 1)
			a.publish(tp, &mq.Message{Body: []byte("after the repair")})
			if d := a.receiveOne(cons, 5*time.Second); string(d.Body) != "after the repair" {
				t.Errorf("the repaired subscription delivered %q", d.Body)
			}
		})
	}
}

// TestEnsureTopicRecreatesWhatAnotherProcessDeleted is the same for the topic
// ARN: EnsureTopic answered from the backend's ARN cache before it would
// call CreateTopic, so after another process deleted the topic EnsureTopic
// answered nil and the topic stayed deleted. CreateTopic is idempotent and is
// the lookup, so EnsureTopic, and a consumer's open that may create, ask it
// every time.
func TestEnsureTopicRecreatesWhatAnotherProcessDeleted(t *testing.T) {
	// deleted provisions a topic and its subscription through a, and deletes
	// the topic, and with it the subscription, through another backend.
	deleted := func(t *testing.T, a *harness) string {
		t.Helper()
		tp := topic(t, "ensure-topic-again")
		a.provision(tp, "s")
		b := openOn(t, a.sns, a.queues)
		adminB, err := b.client.Admin()
		if err != nil {
			t.Fatalf("Admin: %v", err)
		}
		if err := adminB.DeleteTopic(t.Context(), tp); err != nil {
			t.Fatalf("DeleteTopic in the other process: %v", err)
		}
		return tp
	}
	t.Run("EnsureTopic", func(t *testing.T) {
		a := open(t)
		tp := deleted(t, a)
		adminA, err := a.client.Admin()
		if err != nil {
			t.Fatalf("Admin: %v", err)
		}
		if err := adminA.EnsureTopic(t.Context(), mq.TopicSpec{Name: tp}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
		if a.sns.TopicARN(a.topicName(tp)) == "" {
			t.Fatalf("EnsureTopic(%q) answered nil and the topic does not exist: the answer came from the "+
				"backend's ARN cache", tp)
		}
	})
	t.Run("a consumer under AutoCreate", func(t *testing.T) {
		a := open(t)
		tp := deleted(t, a)
		cons, err := a.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
		if err != nil {
			t.Fatalf("a consumer opened under AutoCreate after the topic was deleted: %v (the open looked the "+
				"topic up in the backend's ARN cache)", err)
		}
		defer func() { _ = cons.Close() }()
		a.publish(tp, &mq.Message{Body: []byte("after the delete")})
		if d := a.receiveOne(cons, 5*time.Second); string(d.Body) != "after the delete" {
			t.Errorf("the consumer delivered %q", d.Body)
		}
	})
}

// TestAReopenAfterRawDeliveryWasTurnedOffIsRefused: OpenSource refuses a
// subscription without RawMessageDelivery, the driver's headline refusal,
// because without it every body is SNS's JSON notification. The check ran
// only when the backend had not resolved the subscription before, so once it
// had, a consumer opened after an operator turned raw delivery off was
// accepted and handed the application the notification as the message body.
func TestAReopenAfterRawDeliveryWasTurnedOffIsRefused(t *testing.T) {
	h := open(t)
	tp := topic(t, "raw-off")
	h.provision(tp, "s")
	first := h.consumer(tp, "s", 1)
	if err := first.Close(); err != nil {
		t.Fatalf("closing the first consumer: %v", err)
	}
	typed, err := h.back.TypedAdmin()
	if err != nil {
		t.Fatalf("TypedAdmin: %v", err)
	}
	subARN, err := typed.SubscriptionARN(t.Context(), tp, "s")
	if err != nil {
		t.Fatalf("SubscriptionARN: %v", err)
	}
	h.sns.UnsetRaw(subARN)
	cons, err := h.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
	if err == nil {
		defer func() { _ = cons.Close() }()
		h.publish(tp, &mq.Message{Body: []byte("hello")})
		d := h.receiveOne(cons, 5*time.Second)
		t.Fatalf("a consumer opened after raw delivery was turned off was accepted, and received body %q for "+
			"the published %q", d.Body, "hello")
	}
	if mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Errorf("the refusal is %v (code %v), want CodeInvalidConfig, as a fresh backend answers", err, mq.CodeOf(err))
	}
}

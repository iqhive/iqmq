package kafkago_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// notReportedFor makes the broker leave topic out of every metadata answer
// until d has passed, as a broker does for a moment after CreateTopics
// returns, and for seconds when it is loaded, before it has applied the new
// topic. reported tells whether that time is over, and visible is closed once
// the broker reports the topic again.
func notReportedFor(t *testing.T, cl *kfake.Cluster, topic string, d time.Duration) (reported func() bool, visible <-chan struct{}) {
	t.Helper()
	f := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1})
	var over atomic.Bool
	ch := make(chan struct{})
	timer := time.AfterFunc(d, func() {
		over.Store(true)
		f.Remove()
		close(ch)
	})
	t.Cleanup(func() {
		if timer.Stop() {
			f.Remove()
		}
	})
	return over.Load, ch
}

// awaitListed waits until the client's metadata holds topic, so that a
// publication is not refused for a topic the client does not know yet.
func awaitListed(t *testing.T, adm mq.Admin, topic string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for name, err := range adm.ListTopics(t.Context()) {
			if err != nil {
				t.Fatalf("ListTopics: %v", err)
			}
			if name == topic {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the client's metadata never held %s", topic)
}

// TestARecordPublishedAfterEnsureTopicAndTheOpenArrives is kf8-4 on this
// driver. kafka-go's transport waits for a topic it has just created to reach
// its metadata, but reads the metadata once and does not wait for a topic
// CreateTopics answers already exists, such as one another process created a
// moment ago. EnsureTopic returned then, and a consumer opened next took the
// topic for one that does not exist, resolved no start position and started
// at the end of the log when its reader started, after the record the caller
// published once the consumer had opened. EnsureTopic now returns once the
// client's metadata holds the topic.
func TestARecordPublishedAfterEnsureTopicAndTheOpenArrives(t *testing.T) {
	for _, elsewhere := range []bool{false, true} {
		name := "created by EnsureTopic"
		if elsewhere {
			name = "created by another process"
		}
		t.Run(name, func(t *testing.T) {
			topic := newTopic("ensured")
			cl := cluster(t, 1, nil)
			c := openOn(t, cl)
			adm, err := c.Admin()
			if err != nil {
				t.Fatal(err)
			}
			reported, visible := notReportedFor(t, cl, topic, time.Second)
			if elsewhere {
				if err := cl.CreateTopic(topic, 1, nil); err != nil {
					t.Fatalf("creating %s: %v", topic, err)
				}
			}
			if err := adm.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
				t.Fatalf("EnsureTopic: %v", err)
			}
			if !reported() {
				t.Error("EnsureTopic returned while the broker did not report the topic yet")
			}
			release := holdAssignment(t, cl)
			cons := consumerOn(t, c, topic, "g-ensured")
			<-visible
			awaitListed(t, adm, topic)
			publish(t, c, topic, &mq.Message{Body: []byte("after-open")})
			release()
			receiveAfterOpen(t, cons)
		})
	}
}

// TestEnsureTopicThatTheBrokerNeverReportsIsRetryable: a topic the broker
// still does not report when EnsureTopic's bound ends was created, or already
// existed, and is not usable yet, so EnsureTopic fails with a reason a retry
// can cure rather than reporting success. kafka-go's own wait for the topic
// it created ends with the bound and says nothing.
func TestEnsureTopicThatTheBrokerNeverReportsIsRetryable(t *testing.T) {
	topic := newTopic("never-reported")
	cl := cluster(t, 1, nil)
	c := openOn(t, cl)
	adm, err := c.Admin()
	if err != nil {
		t.Fatal(err)
	}
	f := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1})
	defer f.Remove()
	err = adm.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic})
	if mq.CodeOf(err) != mq.CodeUnavailable || !mq.IsRetryable(err) {
		t.Fatalf("EnsureTopic of a topic the broker never reports = %v (code %v, retryable %v), want CodeUnavailable and retryable", err, mq.CodeOf(err), mq.IsRetryable(err))
	}
}

// TestARecordPublishedAfterEnsureSubscriptionArrives is kf8-4 through
// EnsureSubscription, where the loss was wider: a run that did not see the
// topic returned nil having committed nothing, so the subscription had no
// position until its first consumer opened, and everything published in
// between was skipped. EnsureSubscription now waits for the topic as
// EnsureTopic does.
func TestARecordPublishedAfterEnsureSubscriptionArrives(t *testing.T) {
	const sub = "g-ensured-sub"
	topic := newTopic("ensured-sub")
	cl := cluster(t, 1, nil)
	c := openOn(t, cl)
	adm, err := c.Admin()
	if err != nil {
		t.Fatal(err)
	}
	reported, visible := notReportedFor(t, cl, topic, time.Second)
	if err := cl.CreateTopic(topic, 1, nil); err != nil {
		t.Fatalf("creating %s: %v", topic, err)
	}
	if err := adm.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	if !reported() {
		t.Error("EnsureSubscription returned while the broker did not report the topic yet")
	}
	<-visible
	awaitListed(t, adm, topic)
	publish(t, c, topic, &mq.Message{Body: []byte("after-ensure")})
	cons := consumerOn(t, c, topic, sub)
	d, err := tryReceiveOne(t, cons, 10*time.Second)
	if err != nil {
		t.Fatalf("the record published after EnsureSubscription was never delivered (%v): the subscription had no position until its consumer opened", err)
	}
	if string(d.Body) != "after-ensure" {
		t.Errorf("first delivery = %q, want %q", d.Body, "after-ensure")
	}
}

// TestEnsureSubscriptionOfATopicThatDoesNotExistIsNotFound: a Kafka
// subscription is a position on its topic, and a topic the broker does not
// report within the bound has none to commit. EnsureSubscription returned nil
// for it, reporting a subscription that did not exist.
func TestEnsureSubscriptionOfATopicThatDoesNotExistIsNotFound(t *testing.T) {
	c := openOn(t, cluster(t, 1, nil))
	adm, err := c.Admin()
	if err != nil {
		t.Fatal(err)
	}
	err = adm.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: newTopic("no-such-topic"), Name: "g-no-topic"})
	if !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("EnsureSubscription on a topic that does not exist = %v, want ErrTopicNotFound", err)
	}
	// What is missing is the topic: an error that named the subscription
	// read as a missing subscription to the published predicates.
	if !mq.IsTopicNotFound(err) || mq.IsSubscriptionNotFound(err) {
		t.Errorf("EnsureSubscription on a topic that does not exist = %v: IsTopicNotFound %v, IsSubscriptionNotFound %v, want true and false", err, mq.IsTopicNotFound(err), mq.IsSubscriptionNotFound(err))
	}
}

// TestATopicWaitTheCallerEndsReturnsTheContextError: a caller whose own
// context is cancelled, or passes its deadline, while EnsureTopic or
// EnsureSubscription waits for the topic gets that context's error, at once.
// It used to get the last answer: Unavailable from EnsureTopic, whose
// transport's own wait inside CreateTopics ends silently with the context,
// and a NotFound no retry cures from EnsureSubscription, for a topic that
// exists.
func TestATopicWaitTheCallerEndsReturnsTheContextError(t *testing.T) {
	for _, call := range []string{"EnsureTopic", "EnsureSubscription"} {
		for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(call+" "+want.Error(), func(t *testing.T) {
				topic := newTopic("caller-ends")
				cl := cluster(t, 1, nil)
				c := openOn(t, cl)
				adm, err := c.Admin()
				if err != nil {
					t.Fatal(err)
				}
				f := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1})
				defer f.Remove()
				if call == "EnsureSubscription" {
					if err := cl.CreateTopic(topic, 1, nil); err != nil {
						t.Fatalf("creating %s: %v", topic, err)
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
				if errors.Is(want, context.Canceled) {
					cancel()
					ctx, cancel = context.WithCancel(t.Context())
					stop := time.AfterFunc(500*time.Millisecond, cancel)
					defer stop.Stop()
				}
				defer cancel()
				began := time.Now()
				switch call {
				case "EnsureTopic":
					err = adm.EnsureTopic(ctx, mq.TopicSpec{Name: topic})
				default:
					err = adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: topic, Name: "g-caller-ends"})
				}
				// The tests' Config.RequestTimeout is three seconds.
				if took := time.Since(began); took > 2*time.Second {
					t.Errorf("%s returned %v after the caller's context ended, want at once", call, took)
				}
				if !errors.Is(err, want) {
					t.Errorf("%s with the caller's context ended during the wait = %v (code %v), want %v", call, err, mq.CodeOf(err), want)
				}
			})
		}
	}
}

// TestAConsumerOfATopicTheBrokerDoesNotReportIsReported: a consumer opened
// while the client's metadata does not hold its topic still opens, and its
// partitions start where a group with no position starts once their readers
// start. It used to say nothing, with health Ready; the open now reports it,
// on the path of the consumer's commits, as for a start position it could not
// read.
func TestAConsumerOfATopicTheBrokerDoesNotReportIsReported(t *testing.T) {
	topic := newTopic("unreported")
	cl := cluster(t, 1, nil)
	// No commit round, so nothing but the open touches the path.
	c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.CommitInterval = -1 })
	f := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Metadata}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1})
	defer f.Remove()
	if err := cl.CreateTopic(topic, 1, nil); err != nil {
		t.Fatalf("creating %s: %v", topic, err)
	}
	transitions := b.DegradedTransitions()
	consumerOn(t, c, topic, "g-unreported")
	if h := b.Health(); b.DegradedTransitions() == transitions || h.State != mq.HealthDegraded {
		t.Errorf("health after an open that could not see its topic = %v (%v), want Degraded", h.State, h.Err)
	}
}

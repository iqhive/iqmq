package pulsar_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// A dead-letter copy reports the dead-letter topic it was delivered on, as a
// copy does on the memory and RabbitMQ drivers and as the core's
// mq.WithDeadLetter publication does, whichever route made it. A retry hop
// still reports the topic its message was published to, because the retry
// topic is the subscription's own business.
//
// The driver read REAL_TOPIC on every delivery, and both of pulsar-client-go's
// dead-letter routes keep the properties they find. So a copy of a message that
// had been round the retry topic, and any copy the client's redelivery bound
// made, reported the source's topic, while a Reject of a message that never
// hopped reported the dead-letter topic: a consumer of the dead-letter topic
// saw one or the other depending on how the copy was made.

// requeueThroughRetry requeues d with a delay and fetches it back through the
// retry topic.
func requeueThroughRetry(t *testing.T, src, retry mq.Source, server *pulsarfake.Server, d mq.DriverDelivery) mq.DriverDelivery {
	t.Helper()
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Delay: time.Second}); err != nil {
		t.Fatalf("delayed Nack: %v", err)
	}
	hop, ok := fetchAdvancing(t, retry, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no delivery through the retry topic")
	}
	return hop
}

// TestADeadLetterCopyReportsTheDeadLetterTopicOnEveryRoute drives each
// dead-letter route the in-process model runs.
func TestADeadLetterCopyReportsTheDeadLetterTopicOnEveryRoute(t *testing.T) {
	for _, tc := range []struct {
		name string
		// deadLetter moves the first delivery to the dead-letter topic and
		// returns the retry hop it went through on the way, if any.
		deadLetter func(t *testing.T, src, retry mq.Source, server *pulsarfake.Server, d mq.DriverDelivery) (mq.DriverDelivery, bool)
	}{
		{
			"a Reject of a message that never went round the retry topic",
			func(t *testing.T, src, _ mq.Source, _ *pulsarfake.Server, d mq.DriverDelivery) (mq.DriverDelivery, bool) {
				if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
					t.Fatalf("Reject: %v", err)
				}
				return mq.DriverDelivery{}, false
			},
		},
		{
			"a Reject of a message that came back through the retry topic",
			func(t *testing.T, src, retry mq.Source, server *pulsarfake.Server, d mq.DriverDelivery) (mq.DriverDelivery, bool) {
				hop := requeueThroughRetry(t, src, retry, server, d)
				if err := retry.Settler().Nack(context.Background(), &hop, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
					t.Fatalf("Reject: %v", err)
				}
				return hop, true
			},
		},
		{
			"a delayed requeue past the bound",
			func(t *testing.T, src, retry mq.Source, server *pulsarfake.Server, d mq.DriverDelivery) (mq.DriverDelivery, bool) {
				if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{}); err != nil {
					t.Fatalf("Nack: %v", err)
				}
				again, ok := fetchAdvancing(t, src, server, 200*time.Millisecond)
				if !ok {
					t.Fatal("no redelivery after the nack")
				}
				hop := requeueThroughRetry(t, src, retry, server, again)
				if err := retry.Settler().Nack(context.Background(), &hop, mq.NackOptions{Delay: time.Second}); err != nil {
					t.Fatalf("delayed Nack past the bound: %v", err)
				}
				return hop, true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const topic, dlqTopic = "routed", "routed-dlq"
			b, server := newRig(t, func(c *pd.Config) {
				c.RetryEnable, c.DeadLetterTopic, c.MaxAttempts = true, dlqTopic, 2
			})
			src := sourceOf(t, b, topic, "workers", mq.MetadataNativeStrict, 4)
			// The model subscribes to one topic at a time, so the retry
			// topic is read the way the real multi-topic consumer would
			// read it: by the same subscription.
			retry := sourceOf(t, b, topic+"-workers-RETRY", "workers", mq.MetadataNativeStrict, 4)
			publish(t, sinkOf(t, b, topic, mq.MetadataNativeStrict), &mq.Message{Body: []byte("poison")})

			hop, hopped := tc.deadLetter(t, src, retry, server, fetchOne(t, src))
			if hopped && hop.Info.Topic != topic {
				t.Fatalf("the retry hop reports topic %q, want the topic %q the message was published to",
					hop.Info.Topic, topic)
			}
			cp := fetchOne(t, sourceOf(t, b, dlqTopic, "auditors", mq.MetadataNativeStrict, 4))
			if cp.Info.Topic != dlqTopic || cp.Message.Topic != dlqTopic {
				t.Fatalf("the dead-letter copy reports topic %q (message topic %q), want the dead-letter topic %q "+
					"it was delivered on: the source's REAL_TOPIC was read as the copy's", cp.Info.Topic,
					cp.Message.Topic, dlqTopic)
			}
			if cp.Info.Attempt != 1 {
				t.Fatalf("the dead-letter copy's first delivery reports attempt %d, want 1", cp.Info.Attempt)
			}
			// The source's topic is not lost: a copy that has it keeps it on
			// the native message.
			native, ok := cp.Native.(pulsar.Message)
			if !ok {
				t.Fatalf("the delivery's native value is %T, want a pulsar.Message", cp.Native)
			}
			from, named := native.Properties()[pulsar.SysPropertyRealTopic]
			if hopped && from != "persistent://public/default/"+topic {
				t.Fatalf("the copy's native REAL_TOPIC is %q, want the source's topic", from)
			}
			if !hopped && named {
				t.Fatalf("the copy of a message that never hopped carries REAL_TOPIC %q, want none", from)
			}
		})
	}
}

// TestACopyTheRedeliveryBoundMadeReportsTheDeadLetterTopic is the client's own
// redelivery bound, which the in-process model does not run: the partition
// consumer's dispatcher hands a message whose redelivery count reached
// MaxDeliveries to the dead-letter router, with REAL_TOPIC and
// ORIGIN_MESSAGE_ID recorded under the message's own properties
// (consumer_partition.go in pulsar-client-go v0.21). The copies are published
// here exactly as the router publishes them.
//
// The hop another client made is the boundary: it carries a count and no
// RETRY_TOPIC, so it is read as a hop, as it always was.
func TestACopyTheRedeliveryBoundMadeReportsTheDeadLetterTopic(t *testing.T) {
	const (
		ns     = "persistent://public/default/"
		origin = ns + "bounded"
		retry  = ns + "bounded-workers-RETRY"
	)
	for _, tc := range []struct {
		name    string
		topic   string
		props   map[string]string
		want    string
		attempt uint32
	}{
		{"a message that never went round the retry topic", "bounded-dlq", map[string]string{
			pulsar.SysPropertyRealTopic:       origin,
			pulsar.SysPropertyOriginMessageID: "1:2:0",
			pulsar.PropertyOriginMessageID:    "1:2:0",
		}, "bounded-dlq", 1},
		{"a message that came back through the retry topic", "bounded-dlq", map[string]string{
			pulsar.SysPropertyRealTopic:       origin,
			pulsar.SysPropertyOriginMessageID: "1:2:0",
			pulsar.PropertyOriginMessageID:    "1:2:0",
			pulsar.SysPropertyReconsumeTimes:  "3",
			pulsar.SysPropertyDelayTime:       "1000",
			pulsar.SysPropertyRetryTopic:      retry,
		}, "bounded-dlq", 1},
		{"a retry hop another client made", "bounded-workers-RETRY", map[string]string{
			pulsar.SysPropertyRealTopic:       origin,
			pulsar.SysPropertyOriginMessageID: "1:2:0",
			pulsar.PropertyOriginMessageID:    "1:2:0",
			pulsar.SysPropertyReconsumeTimes:  "3",
			pulsar.SysPropertyDelayTime:       "1000",
		}, "bounded", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, server := newRig(t, nil)
			p, err := server.Client().CreateProducer(pulsar.ProducerOptions{Topic: ns + tc.topic})
			if err != nil {
				t.Fatalf("CreateProducer: %v", err)
			}
			defer p.Close()
			if _, err := p.Send(context.Background(), &pulsar.ProducerMessage{
				Payload: []byte("poison"), Properties: tc.props,
			}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			d := fetchOne(t, sourceOf(t, b, tc.topic, "auditors", mq.MetadataNativeStrict, 4))
			if d.Info.Topic != tc.want || d.Message.Topic != tc.want {
				t.Fatalf("the delivery reports topic %q (message topic %q), want %q",
					d.Info.Topic, d.Message.Topic, tc.want)
			}
			if d.Info.Attempt != tc.attempt {
				t.Fatalf("the delivery reports attempt %d, want %d", d.Info.Attempt, tc.attempt)
			}
		})
	}
}

// TestACopysRetryHopReportsTheDeadLetterTopic is a dead-letter subscription
// with a retry topic of its own, requeueing with a delay a copy of a message
// that had been round the source's retry topic. The copy carries the source's
// REAL_TOPIC, and the requeue left it there, so the copy reported the
// dead-letter topic and its hop the source's topic.
func TestACopysRetryHopReportsTheDeadLetterTopic(t *testing.T) {
	const topic, dlqTopic = "relayed", "relayed-dlq"
	server := pulsarfake.New()
	source := backendOn(t, server, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic = true, dlqTopic
	})
	auditor := backendOn(t, server, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic = true, dlqTopic+"-dlq"
	})
	src := sourceOf(t, source, topic, "workers", mq.MetadataNativeStrict, 4)
	srcRetry := sourceOf(t, source, topic+"-workers-RETRY", "workers", mq.MetadataNativeStrict, 4)
	publish(t, sinkOf(t, source, topic, mq.MetadataNativeStrict), &mq.Message{Body: []byte("poison")})
	hop := requeueThroughRetry(t, src, srcRetry, server, fetchOne(t, src))
	if err := srcRetry.Settler().Nack(context.Background(), &hop, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	dlq := sourceOf(t, auditor, dlqTopic, "auditors", mq.MetadataNativeStrict, 4)
	dlqRetry := sourceOf(t, auditor, dlqTopic+"-auditors-RETRY", "auditors", mq.MetadataNativeStrict, 4)
	cp := fetchOne(t, dlq)
	if cp.Info.Topic != dlqTopic {
		t.Fatalf("the dead-letter copy reports topic %q, want %q", cp.Info.Topic, dlqTopic)
	}
	again := requeueThroughRetry(t, dlq, dlqRetry, server, cp)
	if again.Info.Topic != dlqTopic || again.Message.Topic != dlqTopic {
		t.Fatalf("the copy's retry hop reports topic %q (message topic %q), want the dead-letter topic %q "+
			"its copy reported", again.Info.Topic, again.Message.Topic, dlqTopic)
	}
	if again.Info.Attempt != 2 {
		t.Fatalf("the copy's retry hop reports attempt %d, want 2", again.Info.Attempt)
	}
}

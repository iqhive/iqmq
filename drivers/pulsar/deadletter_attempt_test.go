package pulsar_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// A dead-letter copy is a new message on a new topic, and its attempt count is
// the dead-letter subscription's own: the first delivery of a copy is attempt
// 1, as it is on the memory and Redis drivers. pulsar-client-go's
// dead-letter router republishes the properties it finds, RECONSUMETIMES
// included, and AttemptOf used to add that to the broker's count, so at the
// default MaxAttempts of 16 a rejected message's copy arrived at attempt 18
// and a runner on the dead-letter topic with a MaxAttempts of 17 or less took
// every copy as exhausted at its first failure.

// backendOn is a backend over an existing model, for a test that needs two
// configurations on one broker.
func backendOn(t *testing.T, server *pulsarfake.Server, mutate func(*pd.Config)) *pd.Backend {
	t.Helper()
	cfg := rigConfig(server)
	mutate(&cfg)
	b, err := pd.BackendWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// TestARejectedCopyStartsAtTheFirstAttempt is the explicit Reject, which forces
// the reconsume count past the bound to reach the dead-letter router.
func TestARejectedCopyStartsAtTheFirstAttempt(t *testing.T) {
	b, server := newRig(t, func(c *pd.Config) { c.DeadLetterTopic = "rejected-dlq" })
	src := sourceOf(t, b, "rejected", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "rejected", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("poison")})
	first := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second, ok := fetchAdvancing(t, src, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no redelivery after the nack")
	}
	if second.Info.Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", second.Info.Attempt)
	}
	if err := src.Settler().Nack(context.Background(), &second, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	dlq := sourceOf(t, b, "rejected-dlq", "auditors", mq.MetadataNativeStrict, 4)
	cp := fetchOne(t, dlq)
	if cp.Info.Attempt != 1 || cp.Info.Redelivered {
		t.Fatalf("the dead-letter copy's first delivery reports attempt %d (redelivered %v), want 1: "+
			"the source subscription's forced reconsume count was read as this subscription's",
			cp.Info.Attempt, cp.Info.Redelivered)
	}
	// Nothing the copy carried is gone: the source's count is still on the
	// native message for a consumer that wants it.
	native, ok := cp.Native.(pulsar.Message)
	if !ok {
		t.Fatalf("the delivery's native value is %T, want a pulsar.Message", cp.Native)
	}
	if got := native.Properties()[pulsar.SysPropertyReconsumeTimes]; got != "17" {
		t.Fatalf("the copy's native RECONSUMETIMES is %q, want the source's %q", got, "17")
	}
	// What happens to the copy on the dead-letter topic counts.
	if err := dlq.Settler().Nack(context.Background(), &cp, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack on the dead-letter topic: %v", err)
	}
	again, ok := fetchAdvancing(t, dlq, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no redelivery of the copy after the nack")
	}
	if again.Info.Attempt != 2 {
		t.Fatalf("the copy's redelivery reports attempt %d, want 2", again.Info.Attempt)
	}
}

// TestARequeuePastTheBoundStartsTheCopyAtTheFirstAttempt is the dead-letter
// route a delayed requeue takes once the reconsume count passes MaxAttempts:
// the copy carries the retry topic's count and the retry topic's name.
//
// The attempts before it are the guard for the other half of the rule: the
// retry hop still counts, and so does the client's route, which moves the
// message after its third delivery with a bound of two.
func TestARequeuePastTheBoundStartsTheCopyAtTheFirstAttempt(t *testing.T) {
	const retryTopic = "persistent://public/default/deadhops-workers-RETRY"
	b, server := newRig(t, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic, c.MaxAttempts = true, "deadhops-dlq", 2
	})
	src := sourceOf(t, b, "deadhops", "workers", mq.MetadataNativeStrict, 4)
	// The model subscribes to one topic at a time, so the retry topic is
	// read the way the real multi-topic consumer would read it: by the same
	// subscription.
	retry := sourceOf(t, b, "deadhops-workers-RETRY", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "deadhops", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("x")})

	first := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &first, mq.NackOptions{}); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	second, ok := fetchAdvancing(t, src, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no redelivery after the nack")
	}
	if err := src.Settler().Nack(context.Background(), &second, mq.NackOptions{Delay: time.Second}); err != nil {
		t.Fatalf("delayed Nack: %v", err)
	}
	third, ok := fetchAdvancing(t, retry, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("no delivery through the retry topic")
	}
	got := []uint32{first.Info.Attempt, second.Info.Attempt, third.Info.Attempt}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("the three deliveries report attempts %v, want [1 2 3]", got)
	}
	if err := retry.Settler().Nack(context.Background(), &third, mq.NackOptions{Delay: time.Second}); err != nil {
		t.Fatalf("delayed Nack on the retry topic: %v", err)
	}
	if st := server.Stats(retryTopic, "workers"); st.DeadLettered != 1 || st.Retried != 0 {
		t.Fatalf("the third delivery's requeue was retried %d and dead-lettered %d time(s), want "+
			"dead-lettered once: the client's route counts every delivery", st.Retried, st.DeadLettered)
	}

	dlq := sourceOf(t, b, "deadhops-dlq", "auditors", mq.MetadataNativeStrict, 4)
	cp := fetchOne(t, dlq)
	if cp.Info.Attempt != 1 || cp.Info.Redelivered {
		t.Fatalf("the dead-letter copy's first delivery reports attempt %d (redelivered %v), want 1: "+
			"the retry topic's count was read as the dead-letter subscription's",
			cp.Info.Attempt, cp.Info.Redelivered)
	}
}

// TestADeadLetterSubscriptionRequeuesACopyFromItsOwnCount is a dead-letter
// subscription with a retry topic of its own, requeueing a copy with a delay.
// The copy reports attempt 1, and the client's route on this subscription has
// to agree with it: left to the router, the copy's requeue went out at the
// source's reconsume count plus one, past this subscription's bound, and
// straight on to the next dead-letter topic without being retried at all.
func TestADeadLetterSubscriptionRequeuesACopyFromItsOwnCount(t *testing.T) {
	const dlqTopic = "persistent://public/default/relay-dlq"
	server := pulsarfake.New()
	source := backendOn(t, server, func(c *pd.Config) { c.DeadLetterTopic = "relay-dlq" })
	auditor := backendOn(t, server, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic = true, "relay-dlq-dlq"
	})
	src := sourceOf(t, source, "relay", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, source, "relay", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("poison")})
	d := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	dlq := sourceOf(t, auditor, "relay-dlq", "auditors", mq.MetadataNativeStrict, 4)
	retry := sourceOf(t, auditor, "relay-dlq-auditors-RETRY", "auditors", mq.MetadataNativeStrict, 4)
	cp := fetchOne(t, dlq)
	if cp.Info.Attempt != 1 {
		t.Fatalf("the dead-letter copy's first delivery reports attempt %d, want 1", cp.Info.Attempt)
	}
	if err := dlq.Settler().Nack(context.Background(), &cp, mq.NackOptions{Delay: time.Second}); err != nil {
		t.Fatalf("delayed Nack of the copy: %v", err)
	}
	if st := server.Stats(dlqTopic, "auditors"); st.Retried != 1 || st.DeadLettered != 0 {
		t.Fatalf("the copy's first requeue was retried %d and dead-lettered %d time(s), want retried "+
			"once: the router counted the source subscription's attempts", st.Retried, st.DeadLettered)
	}
	hop, ok := fetchAdvancing(t, retry, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("the copy never came back through the dead-letter subscription's retry topic")
	}
	if hop.Info.Attempt != 2 {
		t.Fatalf("the copy's retry hop reports attempt %d, want 2", hop.Info.Attempt)
	}
}

// TestARejectedCopysRetryHopReportsTheDeadLetterTopic is the hop of the
// previous test's copy. A Reject's forced count stops the client's router
// recording REAL_TOPIC, so the copy carried a count and no topic, and its hop
// reported the dead-letter subscription's retry topic, which nobody published
// to, as the topic it came from.
func TestARejectedCopysRetryHopReportsTheDeadLetterTopic(t *testing.T) {
	server := pulsarfake.New()
	source := backendOn(t, server, func(c *pd.Config) { c.DeadLetterTopic = "hopped-dlq" })
	auditor := backendOn(t, server, func(c *pd.Config) {
		c.RetryEnable, c.DeadLetterTopic = true, "hopped-dlq-dlq"
	})
	src := sourceOf(t, source, "hopped", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, source, "hopped", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("poison")})
	d := fetchOne(t, src)
	if err := src.Settler().Nack(context.Background(), &d, mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	dlq := sourceOf(t, auditor, "hopped-dlq", "auditors", mq.MetadataNativeStrict, 4)
	retry := sourceOf(t, auditor, "hopped-dlq-auditors-RETRY", "auditors", mq.MetadataNativeStrict, 4)
	cp := fetchOne(t, dlq)
	if cp.Info.Topic != "hopped-dlq" {
		t.Fatalf("the dead-letter copy reports topic %q, want %q", cp.Info.Topic, "hopped-dlq")
	}
	if err := dlq.Settler().Nack(context.Background(), &cp, mq.NackOptions{Delay: time.Second}); err != nil {
		t.Fatalf("delayed Nack of the copy: %v", err)
	}
	hop, ok := fetchAdvancing(t, retry, server, 200*time.Millisecond)
	if !ok {
		t.Fatal("the copy never came back through the dead-letter subscription's retry topic")
	}
	if hop.Info.Topic != "hopped-dlq" {
		t.Fatalf("the copy's retry hop reports topic %q, want the dead-letter topic %q it was published to",
			hop.Info.Topic, "hopped-dlq")
	}
	if hop.Info.Attempt != 2 {
		t.Fatalf("the copy's retry hop reports attempt %d, want 2", hop.Info.Attempt)
	}
}

// TestARunnerOnTheDeadLetterTopicRetriesANativeCopy is the symptom end to end:
// a handler's mq.DeadLetter through the native route, and a runner on the
// dead-letter topic whose MaxAttempts is below the source's bound. The copy
// arrived at attempt 18, so the runner found it exhausted on its first failure
// and halted with a PoisonError instead of retrying it.
func TestARunnerOnTheDeadLetterTopicRetriesANativeCopy(t *testing.T) {
	const topic, dlqTopic = "native-orders", "native-orders-dlq"
	server := pulsarfake.New()
	cfg := rigConfig(server)
	cfg.DeadLetterTopic = dlqTopic
	client, err := pd.OpenWithClient(context.Background(), server.Client(), driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	consumer, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	defer func() { _ = consumer.Close() }()
	auditor, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: dlqTopic, Subscription: "audit"})
	if err != nil {
		t.Fatalf("dead-letter consumer: %v", err)
	}
	defer func() { _ = auditor.Close() }()
	if err := client.Publish(ctx, topic, &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	ran := make(chan error, 2)
	go func() {
		ran <- consumer.Run(runCtx, mq.HandlerFunc(func(context.Context, *mq.Delivery) error {
			return mq.DeadLetter(errors.New("unreadable"))
		}), mq.WithNativeDeadLetter())
	}()
	var mu sync.Mutex
	var attempts []int
	settled := make(chan struct{})
	go func() {
		ran <- auditor.Run(runCtx, mq.HandlerFunc(func(_ context.Context, d *mq.Delivery) error {
			mu.Lock()
			defer mu.Unlock()
			attempts = append(attempts, d.Attempt)
			if len(attempts) == 1 {
				return errors.New("not yet")
			}
			close(settled)
			return nil
		}), mq.WithMaxAttempts(3), mq.WithOrderingByKey(mq.OrderingStrict),
			mq.WithRetryBackoff(mq.ConstantBackoff(0)))
	}()

	select {
	case <-settled:
	case err := <-ran:
		t.Fatalf("a runner stopped before the copy was handled: %v", err)
	case <-ctx.Done():
		t.Fatal("the dead-letter copy was never handled")
	}
	stop()
	for range 2 {
		if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: %v", err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || attempts[0] != 1 {
		t.Fatalf("the dead-letter runner saw attempts %v, want the copy's first delivery at 1 and one retry", attempts)
	}
}

// TestAttemptOfTellsADeadLetterCopyFromARetryHop is the classification on
// its own, including the dead-letter route the in-process model does not run:
// the client's own redelivery bound, which copies the message's properties
// as they are (consumer_partition.go's dispatcher in pulsar-client-go v0.21),
// so a message that went round the retry topic arrives with that hop's
// RECONSUMETIMES and RETRY_TOPIC.
func TestAttemptOfTellsADeadLetterCopyFromARetryHop(t *testing.T) {
	const (
		origin = "persistent://public/default/orders"
		retry  = "persistent://public/default/orders-workers-RETRY"
		dlq    = "persistent://public/default/orders-dlq"
	)
	props := func(retryTopic string, marked bool) map[string]string {
		p := map[string]string{
			"kind":                            "order",
			pulsar.SysPropertyRealTopic:       origin,
			pulsar.SysPropertyReconsumeTimes:  "3",
			pulsar.SysPropertyDelayTime:       "1000",
			pulsar.SysPropertyOriginMessageID: "1:2:0",
			pulsar.PropertyOriginMessageID:    "1:2:0",
		}
		if marked {
			p[pulsar.SysPropertyRetryTopic] = retryTopic
		}
		return p
	}
	for _, tc := range []struct {
		name         string
		props        map[string]string
		topic        string
		redeliveries uint32
		want         uint32
	}{
		{"a retry hop", props(retry, true), retry, 1, 5},
		{"a retry hop from a partition of the retry topic", props(retry, true), retry + "-partition-2", 0, 4},
		{"the client's redelivery bound moved a hopped message", props(retry, true), dlq, 0, 1},
		{"a redelivery of that copy", props(retry, true), dlq, 2, 3},
		{"an explicit Reject moved it", props("", true), dlq, 0, 1},
		{"a topic that only looks like a partition of the retry topic", props(retry, true), retry + "-partition-x", 0, 1},
		{"a hop this driver did not mark", props("", false), retry, 1, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pm := pulsar.ProducerMessage{Payload: []byte("b"), Properties: tc.props}
			msg := &encoded{pm: &pm, topic: tc.topic, redeliveries: tc.redeliveries}
			if got := pd.AttemptOf(msg); got != tc.want {
				t.Fatalf("AttemptOf = %d, want %d", got, tc.want)
			}
		})
	}
}

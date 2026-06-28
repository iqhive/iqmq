package snssqs_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	sns "bitbucket.org/iqhive/iqmq/drivers/snssqs"
	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsfake"
	"bitbucket.org/iqhive/iqmq/drivers/sqs/sqsfake"
)

//
// Each one is a state the SERVICES decide and that no amount of correct code
// can produce on demand against a real endpoint, which is why they run against
// the in-process pair: a subscription whose raw delivery is off, a publication
// that reached two queues, a batch three entries of which failed, and a filter
// policy that kept a message out of one queue and not another.

// TestCriticalNonRawSubscriptionIsRefused is the refusal this driver exists to
// make.
//
// Without RawMessageDelivery, SNS wraps the publisher's body in a JSON
// notification and moves every message attribute inside it, so a consumer
// receives a body that is not the one that was published and a message with no
// headers, no key and no identifier. There is no reliable way to tell the two
// shapes apart after the fact — a publisher whose payload IS an SNS-shaped
// JSON object would be unwrapped by any driver that tried — so the only safe
// answer is to refuse at open, before one delivery.
//
// Both wrong states are exercised, because they are reached differently and a
// check that only knew one of them would pass for the other: the attribute
// ABSENT, which is the service's default and the state a subscription made
// without ticking the box is in, and the attribute PRESENT AND FALSE, which is
// what somebody turning it off looks like.
//
// The second half of the test is what makes the first half mean something: the
// same code path with raw delivery ON opens, delivers and round-trips. A
// driver that refused every subscription would pass the refusal and fail
// this.
func TestCriticalNonRawSubscriptionIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		// spoil turns a correctly made subscription into the wrong state.
		spoil func(server *snsfake.Server, subARN string)
	}{
		{"attribute absent", func(s *snsfake.Server, arn string) { s.UnsetRaw(arn) }},
		{"attribute false", func(s *snsfake.Server, arn string) { s.SetRaw(arn, false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queues := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			server := snsfake.New(snsfake.Options{Queues: queues})
			h := openOn(t, server, queues)
			tp := topic(t, "nonraw")
			h.provision(tp, "s")

			// The subscription exists and is correct, so the state below is
			// reached by changing it rather than by never having made it.
			_, _, queueARN, subARN, ok := sns.SubscriptionForTest(h.back, tp, "s")
			if !ok {
				t.Fatalf("the driver did not resolve a subscription for %s/s", tp)
			}
			if queueARN == "" || subARN == "" {
				t.Fatalf("resolved subscription is incomplete: queue %q, subscription %q", queueARN, subARN)
			}
			tc.spoil(server, subARN)

			// A NEW client, because the first one cached the resolution:
			// the refusal is an open-time check and the cache is what makes
			// it cost nothing per delivery.
			second := openOn(t, server, queues)
			_, err := second.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
			if err == nil {
				t.Fatalf("opening a consumer on a subscription without RawMessageDelivery succeeded; every body it delivered would be SNS's JSON notification wrapped around the published body")
			}
			if mq.CodeOf(err) != mq.CodeInvalidConfig {
				t.Fatalf("the refusal reported code %v (%v), want CodeInvalidConfig: a caller switching on the code has to be able to find it", mq.CodeOf(err), err)
			}
			if !containsAll(err.Error(), "RawMessageDelivery") {
				t.Fatalf("the refusal does not name the attribute: %v", err)
			}

			// And with raw delivery on, the same path works end to end.
			server.SetRaw(subARN, true)
			third := openOn(t, server, queues)
			c, err := third.client.Consumer(t.Context(), mq.ConsumerConfig{Topic: tp, Subscription: "s"})
			if err != nil {
				t.Fatalf("opening a consumer on a subscription WITH RawMessageDelivery: %v", err)
			}
			defer func() { _ = c.Close() }()
			third.publish(tp, &mq.Message{Body: []byte("raw")})
			d := third.receiveOne(c, 5*time.Second)
			if string(d.Body) != "raw" {
				t.Fatalf("the body is %q, want the published one: raw delivery means the queue's body IS the published body", d.Body)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestCriticalOneSubscriberSettlementLeavesTheOtherCopyAlone is the trap a
// fan-out driver exists to avoid.
//
// One publication reaches N queues. Each copy is a separate SQS message with
// its own receipt handle, its own visibility window and its own
// ApproximateReceiveCount, and nothing about SNS relates them. So a subscriber
// that acknowledges its copy has settled ITS copy and nothing else; a driver
// that treated a publication as settled when one subscriber settled would lose
// the message for every other subscription, and would do it silently.
//
// What the test holds:
//
//   - both subscriptions receive the publication;
//   - acking one empties ONE queue;
//   - the other copy is still there and is still settleable — and acking it
//     reports nil rather than ErrAlreadySettled or ErrLeaseLost;
//   - the two deliveries name different sources, because they are different
//     messages in different queues.
func TestCriticalOneSubscriberSettlementLeavesTheOtherCopyAlone(t *testing.T) {
	h := open(t)
	tp := topic(t, "isolation")
	h.provision(tp, "a", "b")
	ca := h.consumer(tp, "a", 4)
	cb := h.consumer(tp, "b", 4)

	h.publish(tp, &mq.Message{Body: []byte("both"), Key: []byte("k")})

	// Both copies are taken BEFORE either is settled, so that the claim
	// under test is "settling one leaves the other settleable" rather than
	// "the other copy is still in its queue" — which a driver that handed
	// one delivery to two subscriptions would also satisfy for a moment.
	da := h.receiveOne(ca, 5*time.Second)
	if string(da.Body) != "both" {
		t.Fatalf("subscription a received %q", da.Body)
	}
	db := h.receiveOne(cb, 5*time.Second)
	if string(db.Body) != "both" {
		t.Fatalf("subscription b received %q", db.Body)
	}
	if da.Info().Source == db.Info().Source {
		t.Fatalf("both deliveries name the source %q: the two copies are different messages in different queues, and a driver that handed one delivery to two subscriptions would settle them together", da.Info().Source)
	}

	if err := da.Ack(); err != nil {
		t.Fatalf("acking subscription a's copy: %v", err)
	}
	if err := db.Ack(); err != nil {
		t.Fatalf("acking subscription b's copy after a settled its own reported %v, want nil: the two settlements are unrelated, and a driver that treated the publication as settled would report ErrAlreadySettled or ErrLeaseLost here", err)
	}
	if got := h.depth(tp, "a"); got != 0 {
		t.Fatalf("subscription a's queue holds %d message(s) after its copy was acked, want 0", got)
	}
	if got := h.depth(tp, "b"); got != 0 {
		t.Fatalf("subscription b's queue holds %d message(s) after its own copy was acked, want 0", got)
	}

	// And the reverse direction of the same claim: a REJECTION by one
	// subscriber leaves the other's copy in flight. It is the half that
	// matters operationally — a poison message dead-lettered for one
	// subscription is still being processed by the others.
	h.publish(tp, &mq.Message{Body: []byte("second")})
	ra := h.receiveOne(ca, 5*time.Second)
	if err := ra.Nack(mq.NackDrop()); err != nil {
		t.Fatalf("dropping subscription a's copy: %v", err)
	}
	if got := h.depth(tp, "a"); got != 0 {
		t.Fatalf("subscription a's queue holds %d message(s) after its copy was dropped, want 0", got)
	}
	rb := h.receiveOne(cb, 5*time.Second)
	if string(rb.Body) != "second" {
		t.Fatalf("subscription b received %q after a dropped its copy, want the publication", rb.Body)
	}
	if err := rb.Ack(); err != nil {
		t.Fatalf("acking subscription b's copy after a dropped its own: %v", err)
	}
}

// TestCriticalPublishBatchPartialResultsAlignWithTheCallersSlice holds the one
// thing a batch publish must get right.
//
// The service answers with a Successful list and a Failed list, both keyed by
// the entry identifier the driver chose, and a result aligned with anything
// but the CALLER's slice reports one message's outcome for another. Ten
// entries go in, three fail, and every index has to carry its own answer — the
// seven that succeeded are in every subscribed queue and report nil, and the
// three that failed are in none and report their own error.
//
// The failures are injected by entry POSITION, because the identifier is the
// driver's private business: a test that knew it would be asserting on an
// implementation detail rather than on the contract.
func TestCriticalPublishBatchPartialResultsAlignWithTheCallersSlice(t *testing.T) {
	h := open(t)
	tp := topic(t, "batch")
	h.provision(tp, "a", "b")

	const n = 10
	failing := map[int]string{2: "InternalError", 5: "Throttled", 9: "InvalidParameter"}
	h.sns.Fault(snsfake.Fault{
		Op:           "PublishBatch",
		EntryIndexes: failing,
		Count:        1,
	})

	ms := make([]*mq.Message, n)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte("m" + strconv.Itoa(i))}
	}
	p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()

	err = p.PublishBatch(t.Context(), ms)
	var batch *mq.BatchError
	if !errors.As(err, &batch) {
		t.Fatalf("PublishBatch with three failing entries of ten reported %v, want a *mq.BatchError with a per-entry result", err)
	}
	if len(batch.Errs) != n {
		t.Fatalf("the batch error carries %d results for %d messages: the specification aligns them with the CALLER's slice", len(batch.Errs), n)
	}
	for i := range ms {
		_, shouldFail := failing[i]
		switch {
		case shouldFail && batch.Errs[i] == nil:
			t.Fatalf("entry %d failed at the service and reports nil: a success reported for a message that reached no queue is a lost message", i)
		case !shouldFail && batch.Errs[i] != nil:
			t.Fatalf("entry %d succeeded at the service and reports %v", i, batch.Errs[i])
		}
	}

	// The queues are the other half of the proof: an index-aligned result
	// that named the wrong indexes would still have the right COUNT.
	for _, sub := range []string{"a", "b"} {
		bodies := h.bodies(tp, sub)
		if len(bodies) != n-len(failing) {
			t.Fatalf("subscription %s's queue holds %d message(s), want %d", sub, len(bodies), n-len(failing))
		}
		got := map[string]bool{}
		for _, b := range bodies {
			got[b] = true
		}
		for i := range ms {
			body := "m" + strconv.Itoa(i)
			_, shouldFail := failing[i]
			if got[body] == shouldFail {
				t.Fatalf("subscription %s: message %d (%q) is %s in the queue, and its result says the opposite",
					sub, i, body, map[bool]string{true: "present", false: "absent"}[got[body]])
			}
		}
	}
}

// TestCriticalFilterPolicyKeepsAMessageOutOfOneQueueOnly is the conditional
// FeatureServerFilter claim, in both directions.
//
// A filter policy is the one thing in this driver that makes two subscriptions
// of one topic receive DIFFERENT messages, and the claim is conditional
// precisely because it needs the policy: without one every subscription
// receives everything. So the test needs both subscriptions and both messages
// — a policy that excluded everything, or one that excluded nothing, would
// pass a weaker test.
func TestCriticalFilterPolicyKeepsAMessageOutOfOneQueueOnly(t *testing.T) {
	h := open(t, func(c *sns.Config) {
		c.FilterPolicies = map[string]string{"eu": `{"region":["eu"]}`}
	})
	tp := topic(t, "filter")
	h.provision(tp, "eu", "all")
	ceu := h.consumer(tp, "eu", 4)
	call := h.consumer(tp, "all", 4)

	var euMsg, usMsg mq.Message
	euMsg.Body = []byte("for-eu")
	euMsg.Headers.Set("region", []byte("eu"))
	usMsg.Body = []byte("for-us")
	usMsg.Headers.Set("region", []byte("us"))
	h.publish(tp, &euMsg)
	h.publish(tp, &usMsg)

	// The filtered subscription sees the matching message and NOT the other.
	d := h.receiveOne(ceu, 5*time.Second)
	if string(d.Body) != "for-eu" {
		t.Fatalf("the filtered subscription received %q, want the matching publication", d.Body)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	h.expectNothing(ceu, 500*time.Millisecond, "unmatched publication on a filtered subscription")

	// The unfiltered subscription sees both, which is what makes the
	// exclusion above the filter's doing rather than a lost publication.
	got := map[string]bool{}
	for _, dd := range h.receive(call, 2, 5*time.Second) {
		got[string(dd.Body)] = true
		if err := dd.Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if !got["for-eu"] || !got["for-us"] {
		t.Fatalf("the unfiltered subscription received %v, want both publications", got)
	}
	if h.sns.Dropped() != 1 {
		t.Fatalf("the service dropped %d delivery, want exactly 1: a filter that excluded nothing is indistinguishable from no filter", h.sns.Dropped())
	}
}

// containsAll reports whether s contains every substring, which is how a test
// holds a refusal to naming what it refused.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}

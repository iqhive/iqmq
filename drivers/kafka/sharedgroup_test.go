package kafka_test

import (
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestASubscriptionNameReusedOnAnotherTopicLosesNothing is the
// conformance(kafka) failure of nightly run 35678720257, made deterministic.
//
// A Kafka consumer group spans topics and a portable subscription does not, so
// the subscription "conformance" on topic B joins the SAME group as
// "conformance" on topic A. Opening the second one resolves "latest" and tries
// to commit it for B -- and the coordinator refuses a commit from outside the
// group while the group has a live member, which A's consumer is. The refusal
// was taken to mean "the group already has a position"; it has none for B, so
// B's partition started at the end of the log as of its ASSIGNMENT, and every
// message published between Consumer returning and the rebalance completing
// was skipped. That is the nightly's four 45-second "no delivery arrived"
// failures, and its "message 10 arrived where 0 was expected".
//
// Every conformance scenario's subscription is called "conformance", so a
// member of an earlier scenario that has not yet left is all it takes. On a
// fast host none is ever left and the suite passes; one live member of
// "conformance" held open beside the suite reproduces the nightly's failures.
// The same defect, and the same test, is in drivers/kafka/kafkago.
func TestASubscriptionNameReusedOnAnotherTopicLosesNothing(t *testing.T) {
	first, second := newTopic("shared-a"), newTopic("shared-b")
	c := cluster(t, 1, []string{first, second})
	cl := openOn(t, c)

	// A live member of the group, consuming another topic.
	a := consumerOn(t, cl, first, "conformance")
	publish(t, cl, first, &mq.Message{Body: []byte("warm")})
	d := receiveOne(t, a, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// The same subscription NAME on a second topic, which Kafka makes the
	// same group. Everything published once Consumer has returned is owed.
	b := consumerOn(t, cl, second, "conformance")
	const n = 10
	for i := range n {
		publish(t, cl, second, &mq.Message{Body: []byte(strconv.Itoa(i))})
	}
	want := 0
	got := receiveN(t, b, n, 15*time.Second, func(d *mq.Delivery) error {
		if string(d.Body) != strconv.Itoa(want) {
			t.Errorf("message %s arrived where %d was expected", d.Body, want)
		}
		want++
		return d.Ack()
	})
	if got != n {
		t.Fatalf("%d of %d messages published after the subscription was opened arrived", got, n)
	}
}

// TestASequenceBeyondTheLogStartsWhereAnEmptyGroupWould is
// capability/SeekBySequence/works with a live group : the
// probe asks for sequence 1 of an empty topic and then publishes one record.
// An empty group has the sequence clamped into the log as it is at opening --
// offset 0 -- committed for it , so the record arrives. A
// live group refuses that commit, and the partition started wherever sequence
// 1 resolves at ASSIGNMENT: after the record, which was never delivered.
func TestASequenceBeyondTheLogStartsWhereAnEmptyGroupWould(t *testing.T) {
	first, second := newTopic("seqlog-a"), newTopic("seqlog-b")
	c := cluster(t, 1, []string{first, second})
	cl := openOn(t, c)

	a := consumerOn(t, cl, first, "conformance")
	publish(t, cl, first, &mq.Message{Body: []byte("warm")})
	if err := receiveOne(t, a, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	b := consumerFrom(t, cl, second, "conformance", mq.StartAtSequence(1))
	publish(t, cl, second, &mq.Message{Body: []byte("seq")})
	if got := receiveOne(t, b, 15*time.Second); string(got.Body) != "seq" {
		t.Fatalf("delivered %q", got.Body)
	}
}

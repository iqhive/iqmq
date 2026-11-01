package kafkago_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	mq "github.com/iqhive/iqmq"
)

// positionOn gives the subscription sub a committed position on topic: it
// consumes and acknowledges the first n of the records already there, in
// order, and closes the consumer.
func positionOn(t *testing.T, c *mq.Client, topic, sub string, n int) {
	t.Helper()
	cons := consumerFrom(t, c, topic, sub, mq.StartEarliest())
	for range n {
		if err := receiveOne(t, cons, 10*time.Second).Ack(); err != nil {
			t.Fatalf("Ack on %s: %v", topic, err)
		}
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close on %s: %v", topic, err)
	}
}

// TestDeleteSubscriptionKeepsTheOtherTopicsPosition is kf-3.
//
// DeleteSubscription(topic, sub) names one topic's subscription, and a Kafka
// group spans topics. The driver deleted the whole group, so deleting sub on
// topic A also deleted sub's committed position on topic B, and B's consumer
// restarted at its reset offset: with StartLatest it skipped B's unconsumed
// backlog. The delete=false subtest runs the same steps without the deletion.
func TestDeleteSubscriptionKeepsTheOtherTopicsPosition(t *testing.T) {
	for _, del := range []bool{false, true} {
		t.Run("delete="+strconv.FormatBool(del), func(t *testing.T) {
			a, b := newTopic("delsub-a"), newTopic("delsub-b")
			cl := cluster(t, 1, []string{a, b})
			c := openOn(t, cl)
			const sub = "g-delsub"
			for i := range 4 {
				publish(t, c, b, &mq.Message{Body: []byte("b" + strconv.Itoa(i))})
			}
			publish(t, c, a, &mq.Message{Body: []byte("a0")})
			positionOn(t, c, a, sub, 1)
			positionOn(t, c, b, sub, 2)
			admin, err := c.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if del {
				if err := admin.DeleteSubscription(t.Context(), a, sub); err != nil {
					t.Fatalf("DeleteSubscription(%s): %v", a, err)
				}
			}
			again := consumerFrom(t, c, b, sub, mq.StartLatest())
			d, err := tryReceiveOne(t, again, 5*time.Second)
			if err != nil {
				t.Fatalf("B's subscription received nothing (%v) after DeleteSubscription on topic A: its position on B "+
					"was deleted and it restarted at the end of the log, skipping b2 and b3", err)
			}
			if string(d.Body) != "b2" {
				t.Fatalf("B's subscription resumed at %q, want b2", d.Body)
			}
			// A's position is gone with the deletion, and only then.
			st, err := admin.Stats(t.Context(), a, sub)
			if err != nil {
				t.Fatalf("Stats(%s): %v", a, err)
			}
			if st.Lag.Known == del {
				t.Fatalf("Stats(%s) after delete=%t = lag %+v", a, del, st.Lag)
			}
		})
	}
}

// TestDeleteSubscriptionRefusesATopicALiveMemberConsumes: the coordinator
// refuses to delete a topic's offsets while a live member of the group
// consumes that topic, and the refusal is a conflict.
//
// kfake gives that answer only for a member whose subscription it can read,
// and it cannot read kafka-go's: kafka-go writes version 1 member metadata
// without the owned partitions version 1 carries. Kafka 4.3.1 reads it and
// refuses with GROUP_SUBSCRIBED_TO_TOPIC, so the test has kfake answer as
// that broker does.
func TestDeleteSubscriptionRefusesATopicALiveMemberConsumes(t *testing.T) {
	a, b := newTopic("delsub-live-a"), newTopic("delsub-live-b")
	cl := cluster(t, 1, []string{a, b})
	c := openOn(t, cl)
	const sub = "g-delsub-live"
	publish(t, c, b, &mq.Message{Body: []byte("b0")})
	positionOn(t, c, b, sub, 1)
	publish(t, c, a, &mq.Message{Body: []byte("a0")})
	ca := consumerFrom(t, c, a, sub, mq.StartEarliest())
	if err := receiveOne(t, ca, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack on A: %v", err)
	}
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.OffsetDelete}, Group: sub, Topic: a, Err: kerr.GroupSubscribedToTopic, Count: -1})
	defer fault.Remove()
	err = admin.DeleteSubscription(t.Context(), a, sub)
	if mq.CodeOf(err) != mq.CodeConflict {
		t.Fatalf("DeleteSubscription(%s) while a member consumes it = %v (code %v), want CodeConflict", a, err, mq.CodeOf(err))
	}
}

// TestDeleteSubscriptionWhileTheGroupConsumesAnotherTopic: a live member that
// consumes only topic B does not stop the subscription on topic A being
// deleted, because only A's offsets go. Deleting the whole group was refused
// as a group with live members.
func TestDeleteSubscriptionWhileTheGroupConsumesAnotherTopic(t *testing.T) {
	a, b := newTopic("delsub-other-a"), newTopic("delsub-other-b")
	cl := cluster(t, 1, []string{a, b})
	c := openOn(t, cl)
	const sub = "g-delsub-other"
	publish(t, c, a, &mq.Message{Body: []byte("a0")})
	positionOn(t, c, a, sub, 1)
	publish(t, c, b, &mq.Message{Body: []byte("b0")})
	cb := consumerFrom(t, c, b, sub, mq.StartEarliest())
	if err := receiveOne(t, cb, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack on B: %v", err)
	}
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.DeleteSubscription(t.Context(), a, sub); err != nil {
		t.Fatalf("DeleteSubscription(%s) while a member consumes only %s = %v, want nil", a, b, err)
	}
	st, err := admin.Stats(t.Context(), a, sub)
	if err != nil {
		t.Fatalf("Stats(%s): %v", a, err)
	}
	if st.Lag.Known {
		t.Errorf("after DeleteSubscription(%s) the group still has lag %+v on it", a, st.Lag)
	}
}

// TestDeleteSubscriptionWithoutOffsetDeleteIsUnsupported: a broker older than
// Kafka 2.4 has no OffsetDelete, so a group that spans topics cannot lose one
// topic's offsets alone there. The call is refused as unsupported and deletes
// nothing, rather than deleting the group and the other topic's position with
// it.
func TestDeleteSubscriptionWithoutOffsetDeleteIsUnsupported(t *testing.T) {
	a, b := newTopic("delsub-old-a"), newTopic("delsub-old-b")
	versions := kversion.Stable()
	versions.SetMaxKeyVersion(int16(kmsg.OffsetDelete), -1)
	cl := cluster(t, 1, []string{a, b}, kfake.MaxVersions(versions))
	c := openOn(t, cl)
	const sub = "g-delsub-old"
	publish(t, c, a, &mq.Message{Body: []byte("a0")})
	positionOn(t, c, a, sub, 1)
	for i := range 2 {
		publish(t, c, b, &mq.Message{Body: []byte("b" + strconv.Itoa(i))})
	}
	positionOn(t, c, b, sub, 1)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	err = admin.DeleteSubscription(t.Context(), a, sub)
	if mq.CodeOf(err) != mq.CodeUnsupported {
		t.Fatalf("DeleteSubscription(%s) on a broker without OffsetDelete = %v (code %v), want CodeUnsupported", a, err, mq.CodeOf(err))
	}
	again := consumerFrom(t, c, b, sub, mq.StartLatest())
	d, err := tryReceiveOne(t, again, 5*time.Second)
	if err != nil {
		t.Fatalf("B's subscription received nothing (%v) after the refused DeleteSubscription on %s: its position was deleted", err, a)
	}
	if string(d.Body) != "b1" {
		t.Fatalf("B's subscription resumed at %q, want b1", d.Body)
	}
}

package kafka_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	mq "github.com/iqhive/iqmq"
)

// TestDeleteSubscriptionKeepsTheOtherTopicsPosition: DeleteSubscription(topic,
// sub) names one topic's subscription, and a Kafka group spans topics. The
// driver deleted the whole group, so deleting sub on topic A also deleted
// sub's committed position on topic B, and B's consumer restarted at its
// reset offset: with StartLatest it skipped B's unconsumed backlog. The
// delete=false subtest runs the same steps without the deletion.
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
			// The group gets a position on A too.
			ca := consumerFrom(t, c, a, sub, mq.StartEarliest())
			if err := receiveOne(t, ca, 10*time.Second).Ack(); err != nil {
				t.Fatalf("Ack on A: %v", err)
			}
			if err := ca.Close(); err != nil {
				t.Fatalf("Close on A: %v", err)
			}
			cons := consumerFrom(t, c, b, sub, mq.StartEarliest())
			for i := range 2 {
				d := receiveOne(t, cons, 10*time.Second)
				if string(d.Body) != "b"+strconv.Itoa(i) {
					t.Fatalf("received %q, want b%d", d.Body, i)
				}
				if err := d.Ack(); err != nil {
					t.Fatalf("Ack: %v", err)
				}
			}
			if err := cons.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			admin, err := c.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if del {
				if err := admin.DeleteSubscription(t.Context(), a, sub); err != nil {
					t.Fatalf("DeleteSubscription(%s): %v", a, err)
				}
				// A's position is gone: A's subscription starts again
				// from its start position.
				st, err := admin.Stats(t.Context(), a, sub)
				if err != nil {
					t.Fatalf("Stats(%s): %v", a, err)
				}
				if st.Lag.Known {
					t.Fatalf("after DeleteSubscription(%s) the group still has lag %+v on it", a, st.Lag)
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
		})
	}
}

// TestDeleteSubscriptionOnABrokerWithoutOffsetDeleteIsUnsupported: a broker
// older than Kafka 2.4 has no OffsetDelete, so a group with a position on
// another topic cannot lose this topic's alone. The deletion is refused as
// unsupported and the other topic's position stays, where deleting the group
// would have lost it.
func TestDeleteSubscriptionOnABrokerWithoutOffsetDeleteIsUnsupported(t *testing.T) {
	old := kversion.Stable()
	old.SetMaxKeyVersion(int16(kmsg.OffsetDelete), -1)
	a, b := newTopic("delsub-old-a"), newTopic("delsub-old-b")
	cl := cluster(t, 1, []string{a, b}, kfake.MaxVersions(old))
	c := openOn(t, cl)
	const sub = "g-delsub-old"
	publish(t, c, a, &mq.Message{Body: []byte("a0")})
	publish(t, c, b, &mq.Message{Body: []byte("b0")})
	publish(t, c, b, &mq.Message{Body: []byte("b1")})
	for _, topic := range []string{a, b} {
		h := consumerFrom(t, c, topic, sub, mq.StartEarliest())
		if err := receiveOne(t, h, 10*time.Second).Ack(); err != nil {
			t.Fatalf("Ack on %s: %v", topic, err)
		}
		if err := h.Close(); err != nil {
			t.Fatalf("Close on %s: %v", topic, err)
		}
	}
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
		t.Fatalf("B's subscription received nothing (%v): its position was deleted with the group", err)
	}
	if string(d.Body) != "b1" {
		t.Fatalf("B's subscription resumed at %q, want b1", d.Body)
	}
}

// TestDeleteSubscriptionRefusesATopicALiveMemberConsumes: the coordinator
// refuses to delete a topic's offsets while a live member of the group
// consumes that topic, and the refusal is a conflict.
func TestDeleteSubscriptionRefusesATopicALiveMemberConsumes(t *testing.T) {
	a, b := newTopic("delsub-live-a"), newTopic("delsub-live-b")
	cl := cluster(t, 1, []string{a, b})
	c := openOn(t, cl)
	const sub = "g-delsub-live"
	publish(t, c, b, &mq.Message{Body: []byte("b0")})
	cb := consumerFrom(t, c, b, sub, mq.StartEarliest())
	if err := receiveOne(t, cb, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack on B: %v", err)
	}
	if err := cb.Close(); err != nil {
		t.Fatalf("Close on B: %v", err)
	}
	publish(t, c, a, &mq.Message{Body: []byte("a0")})
	ca := consumerFrom(t, c, a, sub, mq.StartEarliest())
	if err := receiveOne(t, ca, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack on A: %v", err)
	}
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	err = admin.DeleteSubscription(t.Context(), a, sub)
	if mq.CodeOf(err) != mq.CodeConflict {
		t.Fatalf("DeleteSubscription(%s) while a member consumes it = %v (code %v), want CodeConflict", a, err, mq.CodeOf(err))
	}
}

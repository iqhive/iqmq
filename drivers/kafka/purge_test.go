package kafka_test

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestPurgeOfASubscriptionWithNoPositionIsNotFound: on Kafka a subscription
// is a consumer group's committed position on the topic, and Purge committed
// the end offsets for whatever group it was named. A Purge of a subscription
// nobody had made (a typo, a purge run before the first consumer) made the
// group, at the end of the log, and a consumer opened on it with
// StartEarliest skipped every record already there without a word, because a
// start position applies only to a partition with no commit. The memory
// engine answers such a Purge NotFound and makes nothing. A group with a
// position on another topic alone has no subscription on this one: a group
// spans topics.
func TestPurgeOfASubscriptionWithNoPositionIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name      string
		elsewhere bool
	}{
		{name: "a group nobody made"},
		{name: "a group with a position on another topic", elsewhere: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic, other := newTopic("purge"), newTopic("purge-other")
			cl := cluster(t, 1, []string{topic, other})
			c := openOn(t, cl)
			const sub = "g-purge-none"
			if tc.elsewhere {
				publish(t, c, other, &mq.Message{Body: []byte("o0")})
				co := consumerFrom(t, c, other, sub, mq.StartEarliest())
				if err := receiveOne(t, co, 10*time.Second).Ack(); err != nil {
					t.Fatalf("Ack on the other topic: %v", err)
				}
				if err := co.Close(); err != nil {
					t.Fatalf("Close on the other topic: %v", err)
				}
			}
			publish(t, c, topic, &mq.Message{Body: []byte("r0")})
			admin, err := c.Admin()
			if err != nil {
				t.Fatalf("Admin: %v", err)
			}
			if err := admin.Purge(t.Context(), topic, sub); !mq.IsSubscriptionNotFound(err) {
				t.Errorf("Purge of a subscription with no position on the topic = %v (code %v), want NotFound naming the subscription",
					err, mq.CodeOf(err))
			}
			h := consumerFrom(t, c, topic, sub, mq.StartEarliest())
			d, err := tryReceiveOne(t, h, 5*time.Second)
			if err != nil {
				t.Fatalf("a consumer opened from the earliest record after the Purge received nothing (%v): "+
					"the Purge made the subscription at the end of the log", err)
			}
			if string(d.Body) != "r0" {
				t.Fatalf("first delivery = %q, want r0", d.Body)
			}
			if err := d.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		})
	}
}

// TestStatsOfASubscriptionTheBrokerDoesNotKnowIsNotFound: a broker that
// answers GROUP_ID_NOT_FOUND for the subscription's group, as kfake does for a
// group nobody made, has said the subscription does not exist, and Stats says
// so with CodeNotFound naming the subscription, as Purge does. It answered
// every figure unknown and no error, which `iqmq topology plan` can only read
// as "unknown". A group with a position on another topic alone still answers
// unknown figures (TestStatsCountsTheOneTopic).
func TestStatsOfASubscriptionTheBrokerDoesNotKnowIsNotFound(t *testing.T) {
	c, topic := openClient(t)
	publish(t, c, topic, &mq.Message{Body: []byte("r0")})
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	st, err := admin.Stats(t.Context(), topic, "g-never-made")
	if !mq.IsSubscriptionNotFound(err) {
		t.Errorf("Stats of a subscription the broker answers GROUP_ID_NOT_FOUND for = %+v, %v (code %v), want NotFound naming the subscription",
			st, err, mq.CodeOf(err))
	}
}

// TestPurgeDiscardsAPartitionAddedWhileNoConsumerRan: a subscription is the
// group on the topic, not on each partition, and Purge discards everything it
// has not consumed. A partition added to the topic while no consumer of the
// subscription ran has no commit of the group's, and round 1 of the kp9-4 fix
// moved only the partitions with one: Purge returned nil, and a consumer
// opened from the earliest record received the new partition's records.
func TestPurgeDiscardsAPartitionAddedWhileNoConsumerRan(t *testing.T) {
	topic := newTopic("purge-grown")
	cl := cluster(t, 1, []string{topic})
	const sub = "g-purge-grown"
	before, err := openOn(t, cl).Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := before.EnsureSubscription(t.Context(), mq.SubscriptionSpec{Topic: topic, Name: sub}); err != nil {
		t.Fatalf("EnsureSubscription: %v", err)
	}
	growTo(t, cl, topic, 2)
	// A client opened after the growth, so its metadata holds both
	// partitions.
	c := openOn(t, cl)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.Purge(t.Context(), topic, sub); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if st, err := admin.Stats(t.Context(), topic, sub); err != nil || !st.Depth.Known || st.Depth.Value != 0 {
		t.Errorf("Stats after the Purge = %+v, %v, want a known depth of 0", st, err)
	}
	h := consumerFrom(t, c, topic, sub, mq.StartEarliest())
	expectNothing(t, h, 3*time.Second, "a record the Purge was to discard, on the partition added while no consumer ran")
}

// growTo raises topic's partition count to n and puts one record, "p<i>", on
// each partition, through a client of the test's own.
func growTo(t *testing.T, cl *kfake.Cluster, topic string, n int32) {
	t.Helper()
	raw, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer raw.Close()
	req := kmsg.NewPtrCreatePartitionsRequest()
	req.TimeoutMillis = 5000
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic, rt.Count = topic, n
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(t.Context(), raw)
	if err == nil {
		for _, tr := range resp.Topics {
			err = errors.Join(err, kerr.ErrorForCode(tr.ErrorCode))
		}
	}
	if err != nil {
		t.Fatalf("raising %s to %d partitions: %v", topic, n, err)
	}
	for p := range n {
		rec := &kgo.Record{Topic: topic, Partition: p, Value: []byte{'p', byte('0' + p)}}
		if err := raw.ProduceSync(t.Context(), rec).FirstErr(); err != nil {
			t.Fatalf("producing to partition %d: %v", p, err)
		}
	}
}

// TestPurgeReportsACommittedOffsetItCouldNotRead: an answer that carries an
// error for one partition's committed offset is that failure, and Purge
// returns it. It was read as no position on the partition, so a Purge whose
// read had failed answered NotFound, as for a subscription nobody made.
func TestPurgeReportsACommittedOffsetItCouldNotRead(t *testing.T) {
	topic := newTopic("purge-unread")
	cl := cluster(t, 1, []string{topic})
	c := openCapped(t, cl)
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	cl.ControlKey(int16(kmsg.OffsetFetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cl.KeepControl()
		return offsetUnread(req, topic), nil, true
	})
	err = admin.Purge(t.Context(), topic, "g-purge-unread")
	if err == nil || mq.IsSubscriptionNotFound(err) {
		t.Errorf("Purge while the committed offset of a partition could not be read = %v (code %v), want that failure, not NotFound",
			err, mq.CodeOf(err))
	}
}

// offsetUnread answers an OffsetFetch with UNKNOWN_SERVER_ERROR for partition 0
// of topic, in the request's own version's shape.
func offsetUnread(req kmsg.Request, topic string) kmsg.Response {
	r := req.(*kmsg.OffsetFetchRequest)
	resp := r.ResponseKind().(*kmsg.OffsetFetchResponse)
	if len(r.Groups) == 0 {
		rp := kmsg.NewOffsetFetchResponseTopicPartition()
		rp.Offset, rp.ErrorCode = -1, kerr.UnknownServerError.Code
		rt := kmsg.NewOffsetFetchResponseTopic()
		rt.Topic, rt.Partitions = topic, append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp
	}
	for _, g := range r.Groups {
		rp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
		rp.Offset, rp.ErrorCode = -1, kerr.UnknownServerError.Code
		rt := kmsg.NewOffsetFetchResponseGroupTopic()
		rt.Topic, rt.Partitions = topic, append(rt.Partitions, rp)
		rg := kmsg.NewOffsetFetchResponseGroup()
		rg.Group, rg.Topics = g.Group, append(rg.Topics, rt)
		resp.Groups = append(resp.Groups, rg)
	}
	return resp
}

// openCapped opens the driver over a client capped at Kafka 3.9's request
// versions, whose OffsetFetch names topics: from v10 it names them by id.
func openCapped(t *testing.T, cl *kfake.Cluster) *mq.Client {
	t.Helper()
	kcl, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.MaxVersions(kversion.V3_9_0()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	c, err := kq.OpenWithClient(t.Context(), kcl, driverkit.ClientOwned, config(cl))
	if err != nil {
		kcl.Close()
		t.Fatalf("OpenWithClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

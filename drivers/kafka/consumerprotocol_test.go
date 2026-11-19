package kafka_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// describedAsMissing answers every DescribeGroups with GROUP_ID_NOT_FOUND for
// each group it names, as Kafka 4 answers it for a group of the consumer
// protocol (KIP-848), which is not a classic group.
func describedAsMissing(req kmsg.Request) kmsg.Response {
	r := req.(*kmsg.DescribeGroupsRequest)
	resp := r.ResponseKind().(*kmsg.DescribeGroupsResponse)
	for _, g := range r.Groups {
		rg := kmsg.NewDescribeGroupsResponseGroup()
		rg.Group, rg.ErrorCode = g, kerr.GroupIDNotFound.Code
		resp.Groups = append(resp.Groups, rg)
	}
	return resp
}

// TestStatsOfAGroupTheDescribeDoesNotFindReadsItsCommits: Kafka 4 answers
// DescribeGroups GROUP_ID_NOT_FOUND for a group nobody made and for a group of
// the consumer protocol (KIP-848) alike, and kadm's Lag reads only the
// describe, so Stats answered NotFound for a consumer-protocol group that
// exists and has a position. The committed offsets decide instead, as Purge
// reads them: a group with a position on the topic has its depth, one with
// positions elsewhere alone has unknown figures, and one with none at all is
// NotFound. The describe is answered here as Kafka 4 answers it for such a
// group; TestBrokerAdminOfAConsumerProtocolGroup is the real thing.
func TestStatsOfAGroupTheDescribeDoesNotFindReadsItsCommits(t *testing.T) {
	topic, other := newTopic("stats-848"), newTopic("stats-848-other")
	cl := cluster(t, 1, []string{topic, other})
	c := openOn(t, cl)
	const sub, elsewhere = "g-stats-848", "g-stats-848-elsewhere"
	for i, s := range []string{sub, elsewhere} {
		at := []string{topic, other}[i]
		h := consumerFrom(t, c, at, s, mq.StartEarliest())
		if err := h.Close(); err != nil {
			t.Fatalf("closing the consumer of %s: %v", s, err)
		}
	}
	for i := range 3 {
		publish(t, c, topic, &mq.Message{Body: []byte("r" + strconv.Itoa(i))})
	}
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	cl.ControlKey(int16(kmsg.DescribeGroups), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cl.KeepControl()
		return describedAsMissing(req), nil, true
	})
	st, err := admin.Stats(t.Context(), topic, sub)
	if err != nil || !st.Depth.Known || st.Depth.Value != 3 || !st.Lag.Known || st.Lag.Value != 3 {
		t.Errorf("Stats of a group with a position the describe does not find = %+v, %v (code %v), want a depth and lag of 3",
			st, err, mq.CodeOf(err))
	}
	st, err = admin.Stats(t.Context(), topic, elsewhere)
	if err != nil || st.Depth.Known || st.Lag.Known {
		t.Errorf("Stats of a group with a position on another topic alone = %+v, %v (code %v), want unknown figures and nil",
			st, err, mq.CodeOf(err))
	}
	st, err = admin.Stats(t.Context(), topic, "g-stats-848-never-made")
	if !mq.IsSubscriptionNotFound(err) {
		t.Errorf("Stats of a group with no commit at all = %+v, %v (code %v), want NotFound naming the subscription",
			st, err, mq.CodeOf(err))
	}
}

// TestBrokerAdminOfAConsumerProtocolGroup is Stats, Purge and
// DeleteSubscription of a consumer-protocol (KIP-848) group against a real
// broker named by IQMQ_KAFKA_ADDR (Kafka 4 or later), live and then empty,
// and Stats of a group nobody made.
func TestBrokerAdminOfAConsumerProtocolGroup(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	topic := "kip848-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	group := "g-" + topic
	c, err := kq.Open(t.Context(), kq.Config{Brokers: []string{addr}, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	admin, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := admin.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopic(context.Background(), topic) })
	for i := range 3 {
		publish(t, c, topic, &mq.Message{Body: []byte("r" + strconv.Itoa(i))})
	}
	// A consumer of the consumer protocol (ServerSideBalancer) that commits
	// the first record alone: a position of 1 and a lag of 2.
	gcl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.ServerSideBalancer(), kgo.Balancers(kgo.RangeBalancer()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for committed := false; !committed; {
		fs := gcl.PollFetches(ctx)
		if ctx.Err() != nil {
			gcl.Close()
			t.Fatalf("the consumer-protocol consumer received nothing: %v", fs.Errors())
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset == 0 && !committed {
				if err := gcl.CommitRecords(ctx, r); err != nil {
					t.Errorf("CommitRecords: %v", err)
				}
				committed = true
			}
		})
	}
	depth := func(what string, want int64) {
		t.Helper()
		st, err := admin.Stats(t.Context(), topic, group)
		if err != nil || !st.Depth.Known || st.Depth.Value != want {
			t.Errorf("Stats of the %s consumer-protocol group = %+v, %v (code %v), want a known depth of %d",
				what, st, err, mq.CodeOf(err), want)
		}
	}
	depth("live", 2)
	gcl.Close()
	depth("empty", 2)
	if err := admin.Purge(t.Context(), topic, group); err != nil {
		t.Errorf("Purge of the empty consumer-protocol group = %v (code %v)", err, mq.CodeOf(err))
	}
	depth("purged", 0)
	if err := admin.DeleteSubscription(t.Context(), topic, group); err != nil {
		t.Errorf("DeleteSubscription of the empty consumer-protocol group = %v (code %v)", err, mq.CodeOf(err))
	}
	raw, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer raw.Close()
	if offs, err := kadm.NewClient(raw).FetchOffsets(t.Context(), group); err == nil && len(offs) > 0 {
		t.Errorf("after DeleteSubscription the group still has committed offsets %v", offs)
	}
	if st, err := admin.Stats(t.Context(), topic, group); !mq.IsSubscriptionNotFound(err) {
		t.Errorf("Stats of the deleted group = %+v, %v (code %v), want NotFound naming the subscription", st, err, mq.CodeOf(err))
	}
	if st, err := admin.Stats(t.Context(), topic, "never-made-"+topic); !mq.IsSubscriptionNotFound(err) {
		t.Errorf("Stats of a group nobody made = %+v, %v (code %v), want NotFound naming the subscription", st, err, mq.CodeOf(err))
	}
}

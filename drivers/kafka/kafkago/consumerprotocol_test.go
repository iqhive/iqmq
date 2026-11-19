package kafkago_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestBrokerAdminOfAConsumerProtocolGroup is Stats, Purge and
// DeleteSubscription of a consumer-protocol (KIP-848) group against a real
// broker named by IQMQ_KAFKA_ADDR (Kafka 4 or later), live and then empty.
// This driver reads a group through its committed offsets, which such a group
// has as a classic one does.
func TestBrokerAdminOfAConsumerProtocolGroup(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	topic := "kip848-kgo-" + strconv.FormatInt(time.Now().UnixNano(), 36)
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
	// A consumer of the consumer protocol (franz-go's ServerSideBalancer)
	// that commits the first record alone: a position of 1 and a lag of 2.
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
	if st, err := admin.Stats(t.Context(), topic, group); st.Depth.Known {
		t.Errorf("Stats of the deleted group = %+v, %v: its position is still there", st, err)
	}
}

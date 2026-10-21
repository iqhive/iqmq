package kafkago_test

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
)

// TestAPartitionWhoseStartCannotBeResolvedIsRetried holds the reader to its
// generation: a partition whose start offset the broker will not give yet is
// looked up again until it does, rather than left assigned to this member and
// read by nobody until the next rebalance.
func TestAPartitionWhoseStartCannotBeResolvedIsRetried(t *testing.T) {
	topic := newTopic("resolve")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	code := *kerr.NotLeaderForPartition
	fault := cl.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.ListOffsets},
		Topic: topic,
		Err:   &code,
		Count: -1,
	})
	defer fault.Remove()
	h := consumerFrom(t, c, topic, "workers", mq.StartEarliest())
	if d, err := tryReceiveOne(t, h, time.Second); err == nil {
		t.Fatalf("a delivery arrived while every offset lookup was refused: %v", d.Body)
	}
	fault.Remove()
	d := receiveOne(t, h, 10*time.Second)
	if string(d.Body) != "x" {
		t.Fatalf("received %q, want the record published before the consumer opened", d.Body)
	}
}

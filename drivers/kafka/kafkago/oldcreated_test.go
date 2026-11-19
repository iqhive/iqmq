package kafkago_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	franzkgo "github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// rawRecords reads the first n records of topic with a plain franz-go client,
// keyed by body.
func rawRecords(t *testing.T, brokers []string, topic string, n int) map[string]*franzkgo.Record {
	t.Helper()
	cl, err := franzkgo.NewClient(franzkgo.SeedBrokers(brokers...), franzkgo.ConsumeTopics(topic),
		franzkgo.ConsumeResetOffset(franzkgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got := make(map[string]*franzkgo.Record, n)
	for len(got) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records on %s: %v", len(got), n, topic, fs.Errors())
		}
		fs.EachRecord(func(r *franzkgo.Record) { got[string(r.Value)] = r })
	}
	return got
}

// checkNative holds a record published with created to kafka-go's mapping,
// which has no delivery timeout counted from the timestamp: the record's
// timestamp is created however old it is, and the marker is empty.
func checkNative(t *testing.T, what string, rec *franzkgo.Record, created time.Time) {
	t.Helper()
	if !rec.Timestamp.Equal(created) {
		t.Errorf("%s: the record's timestamp is %v, want its CreatedAt %v", what, rec.Timestamp, created)
	}
	for _, h := range rec.Headers {
		if h.Key == kq.HeaderCreated && len(h.Value) != 0 {
			t.Errorf("%s: the marker carries %q, want it empty", what, h.Value)
		}
	}
}

// TestAnOldMessageIsPublishedWithItsCreatedAt: kafka-go counts no delivery
// timeout from a record's timestamp, so a message created an hour ago is
// published with its CreatedAt as the record's timestamp, as it always was,
// and reads back with it. The franz-go driver stamps the produce time instead
// for such a message, and this driver reads its marker
// ([TestTheCreationTimeSurvivesEveryMove]).
func TestAnOldMessageIsPublishedWithItsCreatedAt(t *testing.T) {
	topic := newTopic("oldcreated")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	created := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	old := func(body string) *mq.Message {
		return &mq.Message{Body: []byte(body), Properties: mq.Properties{CreatedAt: created}}
	}
	if err := c.Publish(t.Context(), topic, old("publish")); err != nil {
		t.Fatalf("Publish = %v, want nil", err)
	}
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	if err := p.PublishBatch(t.Context(), []*mq.Message{old("batch"), {Body: []byte("fresh")}}); err != nil {
		t.Fatalf("PublishBatch = %v, want nil", err)
	}
	recs := rawRecords(t, cl.ListenAddrs(), topic, 3)
	checkNative(t, "publish", recs["publish"], created)
	checkNative(t, "batch", recs["batch"], created)
	cons := consumerFrom(t, c, topic, "g-oldcreated", mq.StartEarliest())
	receiveN(t, cons, 3, 15*time.Second, func(d *mq.Delivery) error {
		want := created
		if string(d.Body) == "fresh" {
			want = time.Time{}
		}
		if !d.Properties.CreatedAt.Equal(want) {
			t.Errorf("%s reads back with CreatedAt %v, want %v", d.Body, d.Properties.CreatedAt, want)
		}
		return d.Ack()
	})
}

// TestBrokerAnOldMessageIsPublishedWithItsCreatedAt runs on the broker named by
// IQMQ_KAFKA_ADDR: a message created three minutes ago is published with its
// CreatedAt as the record's timestamp and reads back with it, and one created
// ten minutes ahead, past the topic's message.timestamp.after.max.ms of a
// minute, is the broker's INVALID_TIMESTAMP, a definite CodeInvalidArgument.
func TestBrokerAnOldMessageIsPublishedWithItsCreatedAt(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	topic := "oldcreated-kgo-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	acl, err := franzkgo.NewClient(franzkgo.SeedBrokers(addr))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer acl.Close()
	minute := "60000"
	create := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 1, 1
	rc := kmsg.NewCreateTopicsRequestTopicConfig()
	rc.Name, rc.Value = "message.timestamp.after.max.ms", &minute
	rt.Configs = append(rt.Configs, rc)
	create.Topics = append(create.Topics, rt)
	resp, err := create.RequestWith(t.Context(), acl)
	if err != nil {
		t.Fatalf("CreateTopics: %v", err)
	}
	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil {
		t.Fatalf("CreateTopics %s: %v", topic, err)
	}
	t.Cleanup(func() {
		del := kmsg.NewPtrDeleteTopicsRequest()
		del.TopicNames = []string{topic}
		dt := kmsg.NewDeleteTopicsRequestTopic()
		dt.Topic = kmsg.StringPtr(topic)
		del.Topics = append(del.Topics, dt)
		_, _ = del.RequestWith(context.Background(), acl)
	})
	c, err := kq.Open(t.Context(), kq.Config{Brokers: []string{addr}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	created := time.Now().Add(-3 * time.Minute).Truncate(time.Millisecond)
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("old"), Properties: mq.Properties{CreatedAt: created}}); err != nil {
		t.Fatalf("Publish of a message created three minutes ago = %v, want nil", err)
	}
	checkNative(t, "old", rawRecords(t, []string{addr}, topic, 1)["old"], created)
	d := receiveOne(t, consumerFrom(t, c, topic, "g-"+topic, mq.StartEarliest()), 30*time.Second)
	if !d.Properties.CreatedAt.Equal(created) {
		t.Fatalf("the consumer reads CreatedAt %v, want %v", d.Properties.CreatedAt, created)
	}

	err = c.Publish(t.Context(), topic, &mq.Message{Body: []byte("ahead"), Properties: mq.Properties{CreatedAt: time.Now().Add(10 * time.Minute)}})
	if mq.CodeOf(err) != mq.CodeInvalidArgument || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
		t.Fatalf("Publish of a message created ten minutes ahead = %v (code %v, ambiguous %v, retryable %v), want the broker's INVALID_TIMESTAMP as a definite CodeInvalidArgument",
			err, mq.CodeOf(err), mq.IsAmbiguous(err), mq.IsRetryable(err))
	}
}

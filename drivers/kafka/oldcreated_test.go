package kafka_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// rawRecords reads the first n records of topic with a plain franz-go client,
// committed records alone, keyed by body.
func rawRecords(t *testing.T, brokers []string, topic string, n int) map[string]*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	got := make(map[string]*kgo.Record, n)
	for len(got) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records on %s: %v", len(got), n, topic, fs.Errors())
		}
		fs.EachRecord(func(r *kgo.Record) { got[string(r.Value)] = r })
	}
	return got
}

// createdAtOf decodes rec as a consumer of this driver would.
func createdAtOf(rec *kgo.Record) time.Time {
	var m mq.Message
	kq.Decode(&m, rec, "")
	return m.Properties.CreatedAt
}

// markerOf is the value of rec's iqmq-created marker, and whether it has one.
func markerOf(rec *kgo.Record) (string, bool) {
	for _, h := range rec.Headers {
		if h.Key == kq.HeaderCreated {
			return string(h.Value), true
		}
	}
	return "", false
}

// checkStamped holds a record published with created to the produce time
// rule: the record's timestamp is the produce time (no earlier than start),
// the marker carries created, and a consumer reads created back.
func checkStamped(t *testing.T, what string, rec *kgo.Record, created, start time.Time) {
	t.Helper()
	if rec.Timestamp.Before(start) {
		t.Errorf("%s: the record's timestamp is %v, before the publication at %v", what, rec.Timestamp, start)
	}
	if v, ok := markerOf(rec); !ok || v != created.UTC().Format(time.RFC3339Nano) {
		t.Errorf("%s: the marker is %q (present %v), want the creation time %q", what, v, ok, created.UTC().Format(time.RFC3339Nano))
	}
	if got := createdAtOf(rec); !got.Equal(created) {
		t.Errorf("%s: CreatedAt reads back as %v, want %v", what, got, created)
	}
}

// TestAMessageOlderThanTheDeliveryTimeoutIsPublished: franz-go counts
// Config.DeliveryTimeout from a record's timestamp, which is the message's
// CreatedAt, so a message created longer ago than that was failed before it
// was sent: as an unknown outcome on e323771, and refused by the driver in
// round 5. The record now takes the produce time as its timestamp, the
// creation time travels in the iqmq-created marker's value, and the message
// is published on every path a publication takes. The helper's
// DeliveryTimeout is a second.
func TestAMessageOlderThanTheDeliveryTimeoutIsPublished(t *testing.T) {
	topic := newTopic("oldcreated")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	created := time.Now().Add(-5 * time.Second).Truncate(time.Millisecond)
	old := func(body string) *mq.Message {
		return &mq.Message{Body: []byte(body), Properties: mq.Properties{CreatedAt: created}}
	}
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	none, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher (ConfirmNone): %v", err)
	}
	defer func() { _ = none.Close() }()

	start := time.Now().Truncate(time.Millisecond)
	if err := c.Publish(t.Context(), topic, old("publish")); err != nil {
		t.Fatalf("Publish = %v (ambiguous %v), want nil", err, mq.IsAmbiguous(err))
	}
	if err := p.PublishBatch(t.Context(), []*mq.Message{old("batch"), {Body: []byte("fresh")}}); err != nil {
		t.Fatalf("PublishBatch = %v, want nil", err)
	}
	async := make(chan error, 1)
	if err := p.PublishAsync(t.Context(), old("async"), func(r mq.PublishResult) { async <- r.Err }); err != nil {
		t.Fatalf("PublishAsync = %v, want nil", err)
	}
	if err := <-async; err != nil {
		t.Fatalf("PublishAsync's outcome = %v (ambiguous %v), want nil", err, mq.IsAmbiguous(err))
	}
	if err := p.PublishOwned(t.Context(), mq.OwnedMessage{Message: old("owned")}); err != nil {
		t.Fatalf("PublishOwned = %v, want nil", err)
	}
	if err := none.Publish(t.Context(), old("none")); err != nil {
		t.Fatalf("ConfirmNone Publish = %v, want nil", err)
	}
	for _, pub := range []*mq.PublisherHandle{p, none} {
		if err := pub.Flush(t.Context()); err != nil {
			t.Fatalf("Flush = %v, want nil", err)
		}
	}

	recs := rawRecords(t, cl.ListenAddrs(), topic, 6)
	for _, body := range []string{"publish", "batch", "async", "owned", "none"} {
		checkStamped(t, body, recs[body], created, start)
	}
	if _, ok := markerOf(recs["fresh"]); ok || !createdAtOf(recs["fresh"]).IsZero() {
		t.Errorf("the fresh message gained a creation time: %v", createdAtOf(recs["fresh"]))
	}
}

// TestTheProduceTimeStandsInOnlyForAnOldCreatedAt holds the rule's edges, with
// a ten-second delivery timeout: a CreatedAt that leaves the record at least a
// second of the timeout stays its timestamp, as it always was; one that leaves
// less than a second, or none, is replaced by the produce time. A second is
// the shortest delivery timeout franz-go accepts.
func TestTheProduceTimeStandsInOnlyForAnOldCreatedAt(t *testing.T) {
	topic := newTopic("creatededge")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.DeliveryTimeout = 10 * time.Second })
	now := time.Now().Truncate(time.Millisecond)
	cases := []struct {
		body    string
		created time.Time
		stamped bool
	}{
		{"young", now.Add(-2 * time.Second), false},
		{"future", now.Add(time.Minute), false},
		{"edge", now.Add(-9500 * time.Millisecond), true},
		{"old", now.Add(-time.Hour), true},
	}
	start := time.Now().Truncate(time.Millisecond)
	for _, tc := range cases {
		if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte(tc.body), Properties: mq.Properties{CreatedAt: tc.created}}); err != nil {
			t.Fatalf("Publish %s = %v, want nil", tc.body, err)
		}
	}
	recs := rawRecords(t, cl.ListenAddrs(), topic, len(cases))
	for _, tc := range cases {
		rec := recs[tc.body]
		if tc.stamped {
			checkStamped(t, tc.body, rec, tc.created, start)
			continue
		}
		if !rec.Timestamp.Equal(tc.created) {
			t.Errorf("%s: the record's timestamp is %v, want its CreatedAt %v", tc.body, rec.Timestamp, tc.created)
		}
		if v, ok := markerOf(rec); !ok || v != "" {
			t.Errorf("%s: the marker is %q (present %v), want an empty one", tc.body, v, ok)
		}
		if got := createdAtOf(rec); !got.Equal(tc.created) {
			t.Errorf("%s: CreatedAt reads back as %v, want %v", tc.body, got, tc.created)
		}
	}
}

// TestAnOldMessageIsPublishedInsideATransaction: a transactional producer
// publishes through the same path, which is the one x/kafkatx's sessions use.
func TestAnOldMessageIsPublishedInsideATransaction(t *testing.T) {
	topic := newTopic("oldcreated-tx")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.TransactionalID = "oldcreated-" + topic })
	created := time.Now().Add(-5 * time.Second).Truncate(time.Millisecond)
	if err := b.Client().BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	start := time.Now().Truncate(time.Millisecond)
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("tx"), Properties: mq.Properties{CreatedAt: created}}); err != nil {
		t.Fatalf("Publish inside the transaction = %v (ambiguous %v), want nil", err, mq.IsAmbiguous(err))
	}
	if err := b.Client().EndTransaction(t.Context(), kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
	checkStamped(t, "tx", rawRecords(t, cl.ListenAddrs(), topic, 1)["tx"], created, start)
}

// TestABrokersInvalidTimestampIsADefiniteRefusal: INVALID_TIMESTAMP is the
// broker refusing a record before appending it (a timestamp outside the
// topic's message.timestamp.before.max.ms or after.max.ms), so it is
// CodeInvalidArgument, definite and not retryable, on a publication and on a
// nack's move, as the kafka-go driver maps it. It was CodeUnknown.
func TestABrokersInvalidTimestampIsADefiniteRefusal(t *testing.T) {
	topic := newTopic("badts")
	retry := nack.RetryTopicName(topic, 1)
	cl := cluster(t, 1, []string{topic, retry})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.Nack = nack.RetryTopic })
	definite := func(what string, err error) {
		t.Helper()
		if mq.CodeOf(err) != mq.CodeInvalidArgument || mq.IsAmbiguous(err) || mq.IsRetryable(err) {
			t.Fatalf("%s = %v (code %v, ambiguous %v, retryable %v), want a definite CodeInvalidArgument that is not retryable",
				what, err, mq.CodeOf(err), mq.IsAmbiguous(err), mq.IsRetryable(err))
		}
	}

	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.InvalidTimestamp, Count: 1})
	definite("a publication the broker refused with INVALID_TIMESTAMP", c.Publish(t.Context(), topic, &mq.Message{Body: []byte("refused")}))
	fault.Remove()

	publish(t, c, topic, &mq.Message{Body: []byte("moved")})
	d := receiveOne(t, consumerFrom(t, c, topic, "g-badts", mq.StartEarliest()), 10*time.Second)
	fault = cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: retry, Err: kerr.InvalidTimestamp, Count: 1})
	definite("a move the broker refused with INVALID_TIMESTAMP", d.Nack())
	fault.Remove()
	if err := d.Nack(); err != nil {
		t.Fatalf("the nack after the refused move = %v, want nil: a definite refusal leaves the delivery settleable", err)
	}
}

// TestBrokerAnOldMessageIsPublishedWithItsCreatedAt runs on the broker named by
// IQMQ_KAFKA_ADDR, with the default two-minute delivery timeout: a message
// created three minutes ago is published, its record's timestamp is the
// produce time, and it reads back with its CreatedAt. A message created ten
// minutes ahead, past the topic's message.timestamp.after.max.ms of a minute,
// is refused by the broker with INVALID_TIMESTAMP, which is a definite
// CodeInvalidArgument.
func TestBrokerAnOldMessageIsPublishedWithItsCreatedAt(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	topic := "oldcreated-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	acl, err := kgo.NewClient(kgo.SeedBrokers(addr))
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
	start := time.Now().Truncate(time.Millisecond)
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("old"), Properties: mq.Properties{CreatedAt: created}}); err != nil {
		t.Fatalf("Publish of a message created three minutes ago = %v (ambiguous %v), want nil", err, mq.IsAmbiguous(err))
	}
	checkStamped(t, "old", rawRecords(t, []string{addr}, topic, 1)["old"], created, start)
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

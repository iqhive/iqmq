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
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// firstRecord reads the first record of topic with a plain franz-go client, so
// that a test sees the record's own timestamp and headers.
func firstRecord(t *testing.T, brokers []string, topic string) *franzkgo.Record {
	t.Helper()
	cl, err := franzkgo.NewClient(franzkgo.SeedBrokers(brokers...), franzkgo.ConsumeTopics(topic),
		franzkgo.ConsumeResetOffset(franzkgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("no record on %s: %v", topic, fs.Errors())
		}
		if recs := fs.Records(); len(recs) > 0 {
			return recs[0]
		}
	}
}

// moveRoute names one way a nack moves a record and where the copy goes.
type moveRoute struct {
	name string
	dead bool
}

// moveIt nacks d along route and returns the topic the copy lands on.
func moveIt(d *mq.Delivery, route moveRoute, retry, dlq string) (string, error) {
	if route.dead {
		return dlq, d.Nack(mq.NackDeadLetter())
	}
	return retry, d.Nack()
}

// TestAMovedCopyIsStampedWithTheMoveTime: a nack that moves a record to a
// retry topic or a dead-letter topic stamps the copy with the move's own time,
// and a creation time the publisher set survives the move.
//
// The copy took the original record's timestamp, so a destination with a
// timestamp policy refused the move of an old record
// ([TestBrokerAMoveMeetsTheDestinationsTimestampPolicy]), and the copy's
// retention was counted from the original's time rather than from the move.
func TestAMovedCopyIsStampedWithTheMoveTime(t *testing.T) {
	for _, tc := range []struct {
		route   moveRoute
		created bool
	}{
		{moveRoute{name: "retry topic"}, false},
		{moveRoute{name: "retry topic"}, true},
		{moveRoute{name: "dead-letter topic", dead: true}, true},
	} {
		t.Run(tc.route.name+" created="+strconv.FormatBool(tc.created), func(t *testing.T) {
			topic, dlq := newTopic("movetime"), newTopic("movetime-dead")
			retry := nack.RetryTopicName(topic, 1)
			cl := cluster(t, 1, []string{topic, retry, dlq})
			c := openOn(t, cl, func(cfg *kq.Config) {
				cfg.Nack = nack.RetryTopic
				cfg.NackConfig = nack.Config{DeadLetterTopic: dlq}
			})
			m := &mq.Message{Body: []byte("old")}
			var created time.Time
			if tc.created {
				created = time.Now().Add(-time.Hour).Truncate(time.Millisecond)
				m.Properties.CreatedAt = created
			}
			publish(t, c, topic, m)
			cons := consumerFrom(t, c, topic, "g-movetime", mq.StartEarliest())
			d := receiveOne(t, cons, 15*time.Second)
			original := firstRecord(t, cl.ListenAddrs(), topic).Timestamp
			time.Sleep(10 * time.Millisecond)
			moved := time.Now().Truncate(time.Millisecond)
			dest, err := moveIt(d, tc.route, retry, dlq)
			if err != nil {
				t.Fatalf("the move = %v (ambiguous %v), want nil", err, mq.IsAmbiguous(err))
			}
			stamped := firstRecord(t, cl.ListenAddrs(), dest).Timestamp
			if stamped.Before(moved) {
				t.Fatalf("the copy's timestamp is %v, before the move at %v (the original's is %v)", stamped, moved, original)
			}
			got := receiveOne(t, consumerFrom(t, c, dest, "g-movetime-copy", mq.StartEarliest()), 15*time.Second)
			switch {
			case tc.created && !got.Properties.CreatedAt.Equal(created):
				t.Fatalf("the copy's CreatedAt is %v, want the publisher's %v", got.Properties.CreatedAt, created)
			case !tc.created && !got.Properties.CreatedAt.IsZero():
				t.Fatalf("the copy of a message with no CreatedAt has one: %v", got.Properties.CreatedAt)
			}
		})
	}
}

// TestBrokerAMoveMeetsTheDestinationsTimestampPolicy runs on the broker named
// by IQMQ_KAFKA_ADDR. A destination topic with message.timestamp.before.max.ms
// refuses a record whose timestamp is older than that, so a copy that took the
// original's timestamp could not be moved there at all once the original was
// old enough.
func TestBrokerAMoveMeetsTheDestinationsTimestampPolicy(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	for _, route := range []moveRoute{{name: "retry"}, {name: "dead", dead: true}} {
		t.Run(route.name, func(t *testing.T) {
			topic := "movetime-kgo-" + route.name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			retry, dlq := nack.RetryTopicName(topic, 1), topic+".dead"
			acl, err := franzkgo.NewClient(franzkgo.SeedBrokers(addr))
			if err != nil {
				t.Fatalf("kgo.NewClient: %v", err)
			}
			defer acl.Close()
			minute := "60000"
			create := kmsg.NewPtrCreateTopicsRequest()
			for _, name := range []string{topic, retry, dlq} {
				rt := kmsg.NewCreateTopicsRequestTopic()
				rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, 1, 1
				if name != topic {
					rc := kmsg.NewCreateTopicsRequestTopicConfig()
					rc.Name, rc.Value = "message.timestamp.before.max.ms", &minute
					rt.Configs = append(rt.Configs, rc)
				}
				create.Topics = append(create.Topics, rt)
			}
			resp, err := create.RequestWith(t.Context(), acl)
			if err != nil {
				t.Fatalf("CreateTopics: %v", err)
			}
			for _, rt := range resp.Topics {
				if err := kerr.ErrorForCode(rt.ErrorCode); err != nil {
					t.Fatalf("CreateTopics %s: %v", rt.Topic, err)
				}
			}
			t.Cleanup(func() {
				del := kmsg.NewPtrDeleteTopicsRequest()
				del.TopicNames = []string{topic, retry, dlq}
				for _, name := range del.TopicNames {
					dt := kmsg.NewDeleteTopicsRequestTopic()
					dt.Topic = kmsg.StringPtr(name)
					del.Topics = append(del.Topics, dt)
				}
				_, _ = del.RequestWith(context.Background(), acl)
			})
			c, err := kq.Open(t.Context(), kq.Config{
				Brokers: []string{addr}, Nack: nack.RetryTopic,
				NackConfig: nack.Config{DeadLetterTopic: dlq},
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			created := time.Now().Add(-90 * time.Second).Truncate(time.Millisecond)
			publish(t, c, topic, &mq.Message{Body: []byte("old"), Properties: mq.Properties{CreatedAt: created}})
			cons := consumerFrom(t, c, topic, "g-"+topic, mq.StartEarliest())
			d := receiveOne(t, cons, 30*time.Second)
			dest, err := moveIt(d, route, retry, dlq)
			if err != nil {
				t.Fatalf("the move to %s = %v (code %v), want nil", dest, err, mq.CodeOf(err))
			}
			got := receiveOne(t, consumerFrom(t, c, dest, "g-"+dest, mq.StartEarliest()), 30*time.Second)
			if !got.Properties.CreatedAt.Equal(created) {
				t.Fatalf("the copy's CreatedAt is %v, want the publisher's %v", got.Properties.CreatedAt, created)
			}
		})
	}
}

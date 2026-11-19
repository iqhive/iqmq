package kafka_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// firstRecord reads the first record of topic with a plain franz-go client, so
// that a test sees the record's own timestamp and headers.
func firstRecord(t *testing.T, brokers []string, topic string) *kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
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
// The copy took the original record's timestamp. franz-go counts
// Config.DeliveryTimeout from a record's timestamp, so the move of a record
// older than that failed before anything was sent, the failure read as an
// unknown outcome, and the delivery stayed in flight for good: the move after
// a long hold that the review found. The helper's DeliveryTimeout is a second.
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
				// Inside the delivery timeout when it is published, and past
				// it when it is moved.
				created = time.Now().Add(-500 * time.Millisecond).Truncate(time.Millisecond)
				m.Properties.CreatedAt = created
			}
			publish(t, c, topic, m)
			time.Sleep(1500 * time.Millisecond)
			cons := consumerFrom(t, c, topic, "g-movetime", mq.StartEarliest())
			d := receiveOne(t, cons, 10*time.Second)
			original := firstRecord(t, cl.ListenAddrs(), topic).Timestamp
			moved := time.Now().Truncate(time.Millisecond)
			dest, err := moveIt(d, tc.route, retry, dlq)
			if err != nil {
				t.Fatalf("the move of a record %v old = %v (ambiguous %v), want nil",
					time.Since(original).Round(time.Millisecond), err, mq.IsAmbiguous(err))
			}
			if n := cons.Stats().InFlight; n != 0 {
				t.Fatalf("InFlight after the move = %d, want 0", n)
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
// old enough, on either client.
func TestBrokerAMoveMeetsTheDestinationsTimestampPolicy(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	for _, route := range []moveRoute{{name: "retry"}, {name: "dead", dead: true}} {
		t.Run(route.name, func(t *testing.T) {
			topic := "movetime-" + route.name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			retry, dlq := nack.RetryTopicName(topic, 1), topic+".dead"
			acl, err := kgo.NewClient(kgo.SeedBrokers(addr))
			if err != nil {
				t.Fatalf("kgo.NewClient: %v", err)
			}
			defer acl.Close()
			adm := kadm.NewClient(acl)
			minute := "60000"
			if _, err := adm.CreateTopic(t.Context(), 1, 1, nil, topic); err != nil {
				t.Fatalf("CreateTopic %s: %v", topic, err)
			}
			for _, name := range []string{retry, dlq} {
				if _, err := adm.CreateTopic(t.Context(), 1, 1, map[string]*string{"message.timestamp.before.max.ms": &minute}, name); err != nil {
					t.Fatalf("CreateTopic %s: %v", name, err)
				}
			}
			t.Cleanup(func() { _, _ = adm.DeleteTopics(context.Background(), topic, retry, dlq) })
			c, err := kq.Open(t.Context(), kq.Config{
				Brokers: []string{addr}, Nack: nack.RetryTopic,
				NackConfig: nack.Config{DeadLetterTopic: dlq},
			})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			// Ninety seconds old: inside the two-minute DeliveryTimeout, and
			// past the destinations' one-minute policy.
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

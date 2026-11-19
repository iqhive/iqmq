package kafkago_test

import (
	"crypto/rand"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// randomBody is n random bytes, so that no compression can make a record
// smaller than its body.
func randomBody(n int64) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// TestABodyAtTheAdvertisedLimitIsPublished: MaxMessageBytes is what a caller
// sizes its messages by, so a body of exactly that size, with no key and no
// headers, is published, and one byte more is refused before I/O. The driver
// advertised the cluster's message.max.bytes (1,048,588 by default), and two
// bounds count the framing beside the body: the broker's (61 bytes of batch
// header and 11 of record framing for a body this size) and kafka-go's
// writer, which refuses a message larger than its BatchBytes (1 MiB by
// default, or Config.BatchMaxBytes) with 23 bytes of framing counted. The
// figure is the largest body within both. kfake accepts a record of exactly
// message.max.bytes, so the broker's own refusal is
// TestBrokerABodyAtTheAdvertisedLimitIsPublished.
func TestABodyAtTheAdvertisedLimitIsPublished(t *testing.T) {
	for _, tc := range []struct {
		name  string
		batch int64
		want  int64
	}{
		{"the defaults, the broker's bound", 0, 1048588 - 72},
		{"Config.BatchMaxBytes under the cluster's limit, the writer's bound", 64 << 10, 64<<10 - 23},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("maxbytes")
			cl := cluster(t, 1, []string{topic})
			atTheLimit(t, openOn(t, cl, func(cfg *kq.Config) { cfg.BatchMaxBytes = tc.batch }), topic, tc.want)
		})
	}
}

// atTheLimit checks that c advertises want as MaxMessageBytes, publishes a
// body of exactly that size, and refuses one byte more with
// ErrMessageTooLarge.
func atTheLimit(t *testing.T, c *mq.Client, topic string, want int64) {
	t.Helper()
	lim := c.Capabilities().Limit(mq.LimitMaxMessageBytes)
	if !lim.Known {
		t.Fatal("no MaxMessageBytes advertised; the cluster reports its message.max.bytes")
	}
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: randomBody(lim.Value)}); err != nil {
		t.Errorf("a body of exactly the advertised MaxMessageBytes, %d bytes, was refused: %v (code %v)",
			lim.Value, err, mq.CodeOf(err))
	}
	if lim.Value != want {
		t.Errorf("MaxMessageBytes = %d, want %d", lim.Value, want)
	}
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: randomBody(lim.Value + 1)}); !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Errorf("a body one byte over the advertised MaxMessageBytes = %v, want ErrMessageTooLarge", err)
	}
}

// TestBrokerABodyAtTheAdvertisedLimitIsPublished is the same against a real
// broker named by IQMQ_KAFKA_ADDR, with its default message.max.bytes
// (1,048,588): the broker refuses a batch larger than that, its 61 bytes of
// header and the record's framing counted, which kfake does not.
func TestBrokerABodyAtTheAdvertisedLimitIsPublished(t *testing.T) {
	addr := os.Getenv("IQMQ_KAFKA_ADDR")
	if addr == "" {
		t.Skip("IQMQ_KAFKA_ADDR is not set; this test asks a real broker")
	}
	topic := "maxbytes-kgo-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	c, err := kq.Open(t.Context(), kq.Config{Brokers: []string{addr}, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	adm, err := c.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.EnsureTopic(t.Context(), mq.TopicSpec{Name: topic}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	t.Cleanup(func() { _ = adm.DeleteTopic(t.Context(), topic) })
	atTheLimit(t, c, topic, 1048588-72)
}

package kafka_test

import (
	"crypto/rand"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// randomBody is n random bytes, so that no compression can make a record
// smaller than its body.
func randomBody(n int64) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// TestABodyAtTheAdvertisedLimitIsPublished: MaxMessageBytes is what a caller
// sizes its messages by, a claim check's threshold or a chunker's, so a body
// of exactly that size, with no key and no headers, is published, and one
// byte more is refused before I/O. The driver advertised the cluster's
// message.max.bytes (1,048,588 by default) while franz-go refuses on its own,
// before sending it, a record that does not fit in one batch of its
// ProducerBatchMaxBytes (1,000,012 by default, or Config.BatchMaxBytes), and
// both bounds count the framing beside the body: franz-go 65 bytes of batch
// and 11 of record for a body this size, the broker 61 and 11. The figure is
// the largest body within both, so the expected values are the bound less 76
// (franz-go's) or 72 (the broker's). kfake accepts a record of exactly
// message.max.bytes, so the broker's half is TestBrokerABodyAtTheAdvertisedLimitIsPublished.
func TestABodyAtTheAdvertisedLimitIsPublished(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T, cl *kfake.Cluster) *mq.Client
		want int64
	}{
		{
			name: "the defaults",
			open: func(t *testing.T, cl *kfake.Cluster) *mq.Client { return openOn(t, cl) },
			want: 1000012 - 76,
		},
		{
			name: "Config.BatchMaxBytes under the cluster's limit",
			open: func(t *testing.T, cl *kfake.Cluster) *mq.Client {
				return openOn(t, cl, func(cfg *kq.Config) { cfg.BatchMaxBytes = 64 << 10 })
			},
			want: 64<<10 - 76,
		},
		{
			name: "Config.BatchMaxBytes over the cluster's limit",
			open: func(t *testing.T, cl *kfake.Cluster) *mq.Client {
				return openOn(t, cl, func(cfg *kq.Config) { cfg.BatchMaxBytes = 2 << 20 })
			},
			want: 1048588 - 72,
		},
		{
			name: "a client the caller built",
			open: func(t *testing.T, cl *kfake.Cluster) *mq.Client {
				kcl, err := kgo.NewClient(kgo.SeedBrokers(cl.ListenAddrs()...), kgo.ProducerBatchMaxBytes(128<<10))
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
			},
			want: 128<<10 - 76,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("maxbytes")
			cl := cluster(t, 1, []string{topic})
			atTheLimit(t, tc.open(t, cl), topic, tc.want)
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
	for _, tc := range []struct {
		name  string
		batch int32
		want  int64
	}{
		{"the defaults, franz-go's bound", 0, 1000012 - 76},
		{"Config.BatchMaxBytes 2 MiB, the broker's bound", 2 << 20, 1048588 - 72},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := "maxbytes-" + strconv.FormatInt(time.Now().UnixNano(), 36)
			c, err := kq.Open(t.Context(), kq.Config{Brokers: []string{addr}, BatchMaxBytes: tc.batch, ReplicationFactor: 1})
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
			atTheLimit(t, c, topic, tc.want)
		})
	}
}

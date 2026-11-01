package kafka_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestUnconfirmedPublishOutsideATransactionGoesToHealth: on a transactional
// producer with no transaction open, franz-go refuses every record before
// buffering it, and runs the refusal's promise after Produce has returned
// with nothing the driver can read first: whether the producer is in a
// transaction is franz-go's own state. A ConfirmAccepted Publish waits for
// the promise and returns the refusal. A ConfirmNone Publish and a
// PublishOwned do not wait ([TestAnUnconfirmedPublishInsideAPromiseReturns]),
// so they return nil, or the refusal when the promise happened to run first,
// and the refusal reaches the caller through health. It is a usage error, and
// x/kafkatx is how a publication joins a transaction. docs/drivers/kafka.md
// says so, and this pins it.
func TestUnconfirmedPublishOutsideATransactionGoesToHealth(t *testing.T) {
	for _, tc := range []struct {
		name    string
		confirm mq.ConfirmationMode
		publish func(context.Context, *mq.PublisherHandle, *atomic.Int32) error
	}{
		{"ConfirmNone Publish", mq.ConfirmNone, func(ctx context.Context, p *mq.PublisherHandle, _ *atomic.Int32) error {
			return p.Publish(ctx, &mq.Message{Body: []byte("none")})
		}},
		{"PublishOwned", mq.ConfirmAccepted, func(ctx context.Context, p *mq.PublisherHandle, released *atomic.Int32) error {
			return p.PublishOwned(ctx, mq.OwnedMessage{
				Message: &mq.Message{Body: []byte("owned")},
				Release: func(*mq.Message) { released.Add(1) },
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("none-notx")
			cl := cluster(t, 1, []string{topic})
			c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.TransactionalID = "none-notx" })
			acc, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("opening a publisher: %v", err)
			}
			defer func() { _ = acc.Close() }()
			p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: tc.confirm})
			if err != nil {
				t.Fatalf("opening the publisher under test: %v", err)
			}
			defer func() { _ = p.Close() }()

			aerr := acc.Publish(t.Context(), &mq.Message{Body: []byte("accepted")})
			if aerr == nil {
				t.Fatal("the transactional producer took a record outside a transaction")
			}
			if mq.IsAmbiguous(aerr) {
				t.Fatalf("ConfirmAccepted Publish = %v, ambiguous; franz-go refused the record before buffering it", aerr)
			}
			if s := b.Health(); s.State != mq.HealthReady {
				t.Fatalf("Health before the unconfirmed publication = %v (%v), want Ready: a synchronous refusal is the caller's", s.State, s.Err)
			}

			var released atomic.Int32
			err = tc.publish(t.Context(), p, &released)
			// An error is the promise's refusal, when it ran before the
			// call finished.
			if err != nil && (mq.IsAmbiguous(err) || !strings.Contains(err.Error(), "transaction")) {
				t.Fatalf("%s = %v, want nil or the definite refusal", tc.name, err)
			}
			if err == nil {
				waitFor(t, 5*time.Second, "the refusal in health", func() bool {
					s := b.Health()
					return s.State == mq.HealthDegraded && s.Err != nil && strings.Contains(s.Err.Error(), "transaction")
				})
			}
			if tc.name == "PublishOwned" {
				waitFor(t, 5*time.Second, "the owned message to be released", func() bool { return released.Load() > 0 })
				if n := released.Load(); n != 1 {
					t.Fatalf("the owned message was released %d times, want once", n)
				}
			}
			if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 0 {
				t.Fatalf("the log holds %d records, want none", hw)
			}
		})
	}
}

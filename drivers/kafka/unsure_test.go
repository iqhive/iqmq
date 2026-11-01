package kafka_test

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestADefiniteAnswerAfterAnUnsureAttemptStaysDefinite: franz-go keeps
// retrying a batch whose earlier attempt it is unsure of (REQUEST_TIMED_OUT),
// and fails it with the bare answer of the first attempt it does not retry.
// It keeps the unsure state to itself, and no hook shows a produce response's
// per-partition codes, so the driver classifies that answer as it would the
// answer to a first attempt: a definite refusal, while the first attempt is in
// the log. docs/drivers/kafka.md ("The ambiguity") says so, and this pins it:
// a change that makes the answer ambiguous has to change the page too.
// unknownAfterRetries covers the unknown-topic answer alone.
func TestADefiniteAnswerAfterAnUnsureAttemptStaysDefinite(t *testing.T) {
	for _, tc := range []struct {
		ke   *kerr.Error
		code mq.ErrorCode
	}{
		{kerr.TopicAuthorizationFailed, mq.CodeForbidden},
		{kerr.CorruptMessage, mq.CodeCorruptMessage},
		{kerr.MessageTooLarge, mq.CodeMessageTooLarge},
	} {
		t.Run(tc.ke.Message, func(t *testing.T) {
			topic := newTopic("unsure")
			cl := cluster(t, 1, []string{topic})
			c, b := openPair(t, cl, func(cfg *kq.Config) { cfg.DeliveryTimeout = 20 * time.Second })
			// kfake appends a produce it answers with its own
			// REQUEST_TIMED_OUT, and answers the retry with ke.
			fault := cl.Fault(
				kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: 1},
				kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: tc.ke, Count: -1},
			)
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			hw := cl.PartitionInfo(topic, 0).HighWatermark
			t.Logf("Publish = %v (code %v, ambiguous %v); high watermark %d; ambiguous counted %d",
				err, mq.CodeOf(err), mq.IsAmbiguous(err), hw, b.AmbiguousPublications())
			if hw != 1 {
				t.Skipf("the timed-out attempt was not appended (high watermark %d)", hw)
			}
			if err == nil {
				t.Fatal("Publish = nil, want the retry's refusal")
			}
			if mq.IsAmbiguous(err) || mq.CodeOf(err) != tc.code {
				t.Fatalf("Publish = %v (code %v, ambiguous true): the documented answer is the definite %v; "+
					"if franz-go now shows the unsure attempt, change docs/drivers/kafka.md with it", err, mq.CodeOf(err), tc.code)
			}
		})
	}
}

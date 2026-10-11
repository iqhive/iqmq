package kafkago_test

import (
	"context"
	"errors"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// the ambiguous outcome, which is the one thing a caller cannot resolve on
// its own: the record may be in the log.
//
// It is WIDER on this client than on franz-go and the reason is worth stating
// once. kafka-go's WriteMessages returns the caller's ctx.Err() the moment the
// context is done and lets the produce carry on, on a goroutine of the
// writer's, with a deadline of its own — its own documentation says "there are
// no guarantees made on whether messages were written to kafka, they might
// also still be written after this method has already returned". And because
// the writer sends no producer id, a retry inside the writer's own budget can
// leave a SECOND copy that the broker does not de-duplicate
// , which is what DuplicablePublications counts.

// TestAmbiguousProduceOutcomes holds the three cases apart: the broker answers
// that mean "it may be written", the answer that means "it was not", and the
// caller's own deadline passing after the record was submitted.
func TestAmbiguousProduceOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		code      kerr.Error
		ambiguous bool
		want      mq.ErrorCode
	}{
		{"request-timed-out", *kerr.RequestTimedOut, true, mq.CodeSettlementUnknown},
		{"not-enough-replicas-after-append", *kerr.NotEnoughReplicasAfterAppend, true, mq.CodeSettlementUnknown},
		{"not-enough-replicas", *kerr.NotEnoughReplicas, false, mq.CodeUnavailable},
		{"topic-authorization-failed", *kerr.TopicAuthorizationFailed, false, mq.CodeForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("amb")
			cl := cluster(t, 1, []string{topic})
			c, b := openPair(t, cl, func(cfg *kq.Config) {
				// One attempt: the retryable answers would otherwise spend
				// the writer's whole budget with a backoff between attempts,
				// and what this test is about is the CLASSIFICATION.
				cfg.MaxAttempts = 1
			})
			code := tc.code
			fault := cl.Fault(kfake.Fault{
				Keys:  []kmsg.Key{kmsg.Produce},
				Topic: topic,
				Err:   &code,
				Count: -1,
			})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded while every produce was refused")
			}
			if got := mq.CodeOf(err); got != tc.want {
				t.Errorf("code = %v, want %v (%v)", got, tc.want, err)
			}
			if got := mq.IsAmbiguous(err); got != tc.ambiguous {
				t.Errorf("IsAmbiguous = %v, want %v (%v)", got, tc.ambiguous, err)
			}
			if tc.ambiguous {
				// The specification names the sentinel for exactly this condition, and a
				// caller that has to call IsAmbiguous to find out is a caller
				// who will not.
				if !errors.Is(err, mq.ErrSettlementUnknown) {
					t.Errorf("errors.Is(err, ErrSettlementUnknown) is false for an ambiguous outcome: %v", err)
				}
				if b.AmbiguousPublications() == 0 {
					t.Error("AmbiguousPublications is zero after an ambiguous publication")
				}
				// And on THIS client an ambiguous outcome may also have been
				// duplicated, because there is no producer id to
				// de-duplicate the writer's retries.
				if b.DuplicablePublications() == 0 {
					t.Error("DuplicablePublications is zero after an ambiguous publication on a client with no idempotent producer")
				}
			} else if b.AmbiguousPublications() != 0 {
				t.Errorf("AmbiguousPublications = %d after a definite refusal", b.AmbiguousPublications())
			}
			// The native error survives underneath, so a caller who needs the
			// broker's own code has it.
			var ke segkafka.Error
			if !errors.As(err, &ke) {
				t.Errorf("the broker's own error code is not reachable through errors.As: %v", err)
			}
		})
	}
}

// TestCallerDeadlineAfterSubmissionIsAmbiguous is the case kafka-go's own
// documentation calls out, and it is the sharpest difference from franz-go:
// the caller's context ends the WAIT and not the WRITE.
func TestCallerDeadlineAfterSubmissionIsAmbiguous(t *testing.T) {
	topic := newTopic("amb-ctx")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl, func(cfg *kq.Config) {
		// A retryable refusal and a generous attempt budget, so the writer is
		// still trying when the caller's deadline passes.
		cfg.MaxAttempts = 8
		cfg.WriteBackoffMin = 100 * time.Millisecond
		cfg.WriteBackoffMax = 200 * time.Millisecond
	})
	code := *kerr.RequestTimedOut
	fault := cl.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   &code,
		Count: -1,
	})
	defer fault.Remove()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	err := c.Publish(ctx, topic, &mq.Message{Body: []byte("x")})
	if err == nil {
		t.Fatal("the publication succeeded")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("a deadline that passed after the record was submitted to the writer = %v, and it is not reported as ambiguous: kafka-go lets the produce carry on after WriteMessages returns, so the record may still be written ", err)
	}
	if mq.CodeOf(err) != mq.CodeSettlementUnknown {
		t.Errorf("code = %v, want CodeSettlementUnknown", mq.CodeOf(err))
	}
	if b.AmbiguousPublications() == 0 {
		t.Error("AmbiguousPublications is zero")
	}
}

// TestAmbiguityReachesEveryPublicationSurface is the half a driver gets wrong
// by covering only Publish: a batch, an asynchronous publication and a
// nack's republication all go through the same classification, and a surface
// that reported a definite failure instead would send a caller into a
// duplicate.
func TestAmbiguityReachesEveryPublicationSurface(t *testing.T) {
	topic := newTopic("amb-all")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.MaxAttempts = 1 })
	code := *kerr.RequestTimedOut
	fault := cl.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   &code,
		Count: -1,
	})
	defer fault.Remove()
	pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	t.Run("batch", func(t *testing.T) {
		err := pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
		if err == nil {
			t.Fatal("the batch succeeded")
		}
		if !mq.IsAmbiguous(err) {
			t.Fatalf("a wholly refused batch = %v; REQUEST_TIMED_OUT on a produce is ambiguous whatever the surface", err)
		}
	})
	t.Run("async", func(t *testing.T) {
		done := make(chan mq.PublishResult, 1)
		if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte("c")}, func(r mq.PublishResult) { done <- r }); err != nil {
			t.Fatalf("PublishAsync: %v", err)
		}
		select {
		case res := <-done:
			if res.Err == nil {
				t.Fatal("the asynchronous publication succeeded")
			}
			if !mq.IsAmbiguous(res.Err) {
				t.Fatalf("the asynchronous outcome = %v, want ambiguous", res.Err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("the asynchronous callback never ran")
		}
	})
}

// TestAmbiguousRepublishNeverAcksTheOriginal is the strictest rule and the
// one a driver breaks by treating "unknown" as "done": a dead-letter
// publication whose outcome is ambiguous must NOT acknowledge the original,
// because the record may be nowhere.
func TestAmbiguousRepublishNeverAcksTheOriginal(t *testing.T) {
	topic, dlq := newTopic("amb-dl"), newTopic("amb-dl-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c, b := openPair(t, cl, func(cfg *kq.Config) {
		cfg.NackConfig.DeadLetterTopic = dlq
		cfg.MaxAttempts = 1
	})
	cons := consumerOn(t, c, topic, "amb-dl-g")
	publish(t, c, topic, &mq.Message{Body: []byte("must-not-vanish")})
	d := receiveOne(t, cons, 15*time.Second)
	code := *kerr.RequestTimedOut
	fault := cl.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: dlq,
		Err:   &code,
		Count: -1,
	})
	err := d.Reject()
	if err == nil {
		t.Fatal("Reject succeeded while the dead-letter publication's outcome was unknown: the specification forbids acknowledging the original on an AMBIGUOUS move as well as on a failed one")
	}
	if !mq.IsAmbiguous(err) {
		t.Errorf("the refused Reject = %v, want an ambiguous outcome", err)
	}
	if b.AmbiguousPublications() == 0 {
		t.Error("AmbiguousPublications is zero after an ambiguous republication")
	}
	fault.Remove()
	// A second settlement of a delivery whose first settlement's outcome is
	// UNKNOWN is refused by the core, and that is the correct end of the
	// story rather than a gap: iqmq cannot tell whether the record reached
	// the dead-letter topic, so it will not let the caller decide by
	// settling again. This is the difference from a DEFINITE refusal, which
	// leaves the delivery settleable —
	// TestCriticalDeadLetterPublicationFails is that case.
	if err := d.Reject(); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("the second Reject after an ambiguous one = %v, want ErrSettlementUnknown", err)
	}
	// What the driver owes is that the ORIGINAL offset was not committed, so
	// the record comes back to whoever reopens the subscription. Nothing was
	// destroyed.
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "amb-dl-g")
	back := receiveOne(t, again, 20*time.Second)
	if string(back.Body) != "must-not-vanish" {
		t.Fatalf("the reopened subscription holds %q, want the record whose dead-lettering was unknown", back.Body)
	}
	if err := back.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

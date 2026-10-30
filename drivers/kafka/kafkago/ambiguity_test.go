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
	"github.com/iqhive/iqmq/driverkit"
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

// TestRetriedOutProduceIsAmbiguous is the writer's own retries. kafka-go
// retries a retriable answer up to MaxAttempts times and returns only the
// last one, so the answer a publication reports says nothing about the
// attempts before it. Here the first attempt is appended and answered
// REQUEST_TIMED_OUT and the second is refused with NOT_ENOUGH_REPLICAS: the
// record is in the log, and the publication came back as the definite,
// retryable refusal, which a caller's retry turned into a second copy.
func TestRetriedOutProduceIsAmbiguous(t *testing.T) {
	for _, surface := range []string{"publish", "batch", "async"} {
		t.Run(surface, func(t *testing.T) {
			topic := newTopic("amb-retried")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl, func(cfg *kq.Config) {
				cfg.MaxAttempts = 2
				cfg.WriteBackoffMin = 10 * time.Millisecond
				cfg.WriteBackoffMax = 20 * time.Millisecond
			})
			pub, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
			if err != nil {
				t.Fatalf("Publisher: %v", err)
			}
			defer func() { _ = pub.Close() }()
			// Faults answer in the order they were added, and kfake appends
			// a produce it answers with its own REQUEST_TIMED_OUT, as a
			// broker can: the pointer is what it recognises, so it is not
			// copied.
			fault := cl.Fault(
				kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: 1},
				kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotEnoughReplicas, Count: -1},
			)
			defer fault.Remove()

			switch surface {
			case "publish":
				err = pub.Publish(t.Context(), &mq.Message{Body: []byte("x")})
			case "batch":
				err = pub.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("x")}})
			case "async":
				done := make(chan mq.PublishResult, 1)
				if err := pub.PublishAsync(t.Context(), &mq.Message{Body: []byte("x")}, func(r mq.PublishResult) { done <- r }); err != nil {
					t.Fatalf("PublishAsync: %v", err)
				}
				select {
				case res := <-done:
					err = res.Err
				case <-time.After(20 * time.Second):
					t.Fatal("the asynchronous callback never ran")
				}
			}
			if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 1 {
				t.Fatalf("the log holds %d records, want 1: the timed-out attempt was not appended", hw)
			}
			if err == nil {
				t.Fatal("the publication succeeded while its last attempt was refused")
			}
			if !mq.IsAmbiguous(err) || mq.CodeOf(err) != mq.CodeSettlementUnknown {
				t.Fatalf("the publication = %v (code %v, ambiguous %v), want CodeSettlementUnknown and ambiguous: the record is in the log", err, mq.CodeOf(err), mq.IsAmbiguous(err))
			}
			// The core hands an ambiguous asynchronous outcome back as its
			// own, so the native answer is checked where it is kept.
			if surface != "async" && !errors.Is(err, segkafka.NotEnoughReplicas) {
				t.Errorf("the last attempt's own answer is not reachable through errors.Is: %v", err)
			}
		})
	}
}

// TestABorrowedWritersOwnAttemptsDecideTheAmbiguity: whether a publication's
// answer followed a retry is decided by how many attempts the writer makes,
// and a writer handed to OpenWithWriter makes its own MaxAttempts, not the
// configuration's. The driver read the configuration's, so a writer that
// retried under a configuration of one attempt reported a definite refusal
// for a record in the log, and a writer that never retried under the default
// of three reported an outright refusal as an unknown outcome.
func TestABorrowedWritersOwnAttemptsDecideTheAmbiguity(t *testing.T) {
	for _, tc := range []struct {
		name           string
		writerAttempts int
		configAttempts int
		faults         []kfake.Fault
		records        int64
		code           mq.ErrorCode
		ambiguous      bool
	}{
		{
			// The first attempt is appended and answered REQUEST_TIMED_OUT,
			// and the second is refused.
			name: "a writer that retries", writerAttempts: 2, configAttempts: 1,
			faults: []kfake.Fault{
				{Keys: []kmsg.Key{kmsg.Produce}, Err: kerr.RequestTimedOut, Count: 1},
				{Keys: []kmsg.Key{kmsg.Produce}, Err: kerr.NotEnoughReplicas, Count: -1},
			},
			records: 1, code: mq.CodeSettlementUnknown, ambiguous: true,
		},
		{
			name: "a writer that does not retry", writerAttempts: 1, configAttempts: 0,
			faults: []kfake.Fault{
				{Keys: []kmsg.Key{kmsg.Produce}, Err: kerr.NotEnoughReplicas, Count: -1},
			},
			records: 0, code: mq.CodeUnavailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("amb-borrowed")
			cl := cluster(t, 1, []string{topic})
			tr := &segkafka.Transport{}
			t.Cleanup(tr.CloseIdleConnections)
			w := &segkafka.Writer{
				Addr:            segkafka.TCP(cl.ListenAddrs()...),
				Transport:       tr,
				RequiredAcks:    segkafka.RequireAll,
				BatchSize:       1,
				MaxAttempts:     tc.writerAttempts,
				WriteBackoffMin: 10 * time.Millisecond,
				WriteBackoffMax: 20 * time.Millisecond,
			}
			c, err := kq.OpenWithWriter(t.Context(), w, driverkit.ClientOwned, config(cl, func(cfg *kq.Config) {
				cfg.MaxAttempts = tc.configAttempts
			}))
			if err != nil {
				t.Fatalf("OpenWithWriter: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			for i := range tc.faults {
				tc.faults[i].Topic = topic
			}
			fault := cl.Fault(tc.faults...)
			defer fault.Remove()
			err = c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != tc.records {
				t.Fatalf("the log holds %d records, want %d", hw, tc.records)
			}
			if err == nil {
				t.Fatal("the publication succeeded while its last attempt was refused")
			}
			if mq.CodeOf(err) != tc.code || mq.IsAmbiguous(err) != tc.ambiguous {
				t.Fatalf("the publication = %v (code %v, ambiguous %v), want code %v and ambiguous %v",
					err, mq.CodeOf(err), mq.IsAmbiguous(err), tc.code, tc.ambiguous)
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

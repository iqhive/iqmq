package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// The Ambiguous publication, which is the one outcome a Kafka driver is most
// likely to get wrong and the most expensive to get wrong.
//
// Once a record has entered franz-go's buffer the client may have written it.
// Everything that goes wrong after that moment — the context being cancelled,
// the deadline passing, the broker answering REQUEST_TIMED_OUT or
// NOT_ENOUGH_REPLICAS_AFTER_APPEND — may be a failure to HEAR the answer
// rather than a failure to write the record. Reporting any of them as a
// definite failure is how a caller retries and publishes twice while believing
// it published once, and ErrSettlementUnknown exists for exactly this.
//
// Every case below asserts three things: that the error says the outcome is
// unknown ([github.com/iqhive/iqmq.IsAmbiguous]), that it matches
// ErrSettlementUnknown so a caller who reaches for the sentinel finds it, and
// that the native error is still underneath for a caller who needs the
// broker's own answer.

// TestAmbiguousPublishAfterRetries: a RETRYABLE broker error is retried by
// franz-go until the delivery timeout, and the record then fails with
// kgo.ErrRecordTimeout — which is the honest answer and an ambiguous one.
//
// The caller never sees the broker's own code for these, and that is not a
// mapping gap: by the time the record is failed there have been several
// attempts, and the client cannot know whether one of them landed before its
// reply was lost. REQUEST_TIMED_OUT and NOT_ENOUGH_REPLICAS_AFTER_APPEND are
// the two the broker answers AFTER writing, and NOT_ENOUGH_REPLICAS is one it
// answers before — retried, all three end in the same place, and reporting any
// of them as a definite failure is what makes a caller publish twice.
func TestAmbiguousPublishAfterRetries(t *testing.T) {
	for _, ke := range []*kerr.Error{
		kerr.RequestTimedOut,
		kerr.NotEnoughReplicasAfterAppend,
		kerr.NotEnoughReplicas,
	} {
		t.Run(ke.Message, func(t *testing.T) {
			topic := newTopic("ambig")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: ke, Count: -1})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded through the fault")
			}
			assertUnknown(t, err)
			// The native cause is still reachable, so a caller who wants
			// to know WHY the outcome is unknown can find out.
			if !errors.Is(err, kgo.ErrRecordTimeout) {
				t.Fatalf("the native cause is not reachable: %v", err)
			}
		})
	}
}

// TestAmbiguousPublishOnBrokenIdempotence: an idempotent producer whose
// sequence went wrong can no longer tell what the broker has.
//
// DUPLICATE_SEQUENCE_NUMBER is deliberately NOT here: franz-go treats it as a
// SUCCESS, because it means the broker already has this exact record — which
// is the idempotent producer working rather than failing, and the publication
// is not ambiguous at all. The driver's rule for it only matters for a
// caller who reaches the error through a path franz-go does not resolve.
func TestAmbiguousPublishOnBrokenIdempotence(t *testing.T) {
	for _, ke := range []*kerr.Error{
		kerr.OutOfOrderSequenceNumber,
		kerr.UnknownProducerID,
	} {
		t.Run(ke.Message, func(t *testing.T) {
			topic := newTopic("ambig-idem")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: ke, Count: -1})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded through the fault")
			}
			assertUnknown(t, err)
		})
	}
}

// TestDefinitePublishFailures are the other half of the distinction, and they
// matter as much: an error the broker gives once and does not retry is a
// definite refusal, and reporting it as ambiguous would make every caller
// reconcile a publication that certainly did not happen.
func TestDefinitePublishFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  *kerr.Error
		code mq.ErrorCode
	}{
		{"message too large", kerr.MessageTooLarge, mq.CodeMessageTooLarge},
		{"topic authorization failed", kerr.TopicAuthorizationFailed, mq.CodeForbidden},
		{"invalid required acks", kerr.InvalidRequiredAcks, mq.CodeInvalidArgument},
		{"invalid record", kerr.InvalidRecord, mq.CodeCorruptMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic := newTopic("definite")
			cl := cluster(t, 1, []string{topic})
			c := openOn(t, cl)
			fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: tc.err, Count: -1})
			defer fault.Remove()
			err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")})
			if err == nil {
				t.Fatal("the publication succeeded through the fault")
			}
			if mq.IsAmbiguous(err) {
				t.Fatalf("%v was reported as ambiguous; the broker refused it outright, so the caller may retry without duplicating", tc.err)
			}
			if got := mq.CodeOf(err); got != tc.code {
				t.Fatalf("%v mapped to %v, want %v", tc.err, got, tc.code)
			}
			var kerror *kerr.Error
			if !errors.As(err, &kerror) || kerror.Code != tc.err.Code {
				t.Fatalf("the native error is not reachable through errors.As: %v", err)
			}
		})
	}
}

// TestAmbiguousPublishOnCancellationAfterBuffering is the case the specification names and the hardest to see: the record is in the producer's buffer and the
// CALLER's context is cancelled.
//
// franz-go resolves the record's promise with the context's error, and the
// record may still be written — the client has it and the connection is open.
// So the answer is not "cancelled", it is "unknown", and a driver that
// returned context.Canceled as a plain failure would have a caller retry a
// record that is already in the log.
func TestAmbiguousPublishOnCancellationAfterBuffering(t *testing.T) {
	topic := newTopic("ambig-cancel")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)

	// Hold the produce request inside the broker until the test says so,
	// which is the window in which the record is buffered and its outcome
	// is not yet known.
	released := make(chan struct{})
	var once bool
	// (nil, nil, false) is kfake's own "I did not handle this request; let
	// the cluster answer it", which is why the two returns are both nil.
	cl.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if once {
			return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
		}
		once = true
		cl.KeepControl()
		cl.SleepControl(func() { <-released })
		return nil, nil, false //nolint:nilnil // kfake's "not handled" is (nil, nil, false)
	})

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	go func() { errc <- c.Publish(ctx, topic, &mq.Message{Body: []byte("in-flight")}) }()
	// Give the record time to reach the buffer and the produce time to
	// reach the broker, then cancel.
	time.Sleep(300 * time.Millisecond)
	cancel()

	var err error
	select {
	case err = <-errc:
	case <-time.After(20 * time.Second):
		close(released)
		t.Fatal("the publication never resolved after its context was cancelled")
	}
	close(released)

	if err == nil {
		t.Skip("the record was written before the cancellation was observed, so there was no ambiguity to report")
	}
	assertUnknown(t, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the native cause is no longer reachable: errors.Is(%v, context.Canceled) is false, so a caller cannot see WHY the outcome is unknown", err)
	}
}

// TestAmbiguousPublishCountsUp: a deployment that cannot resolve an ambiguous
// publication on its own at least has to be able to see that one happened.
func TestAmbiguousPublishCountsUp(t *testing.T) {
	topic := newTopic("ambig-count")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	if b.AmbiguousPublications() != 0 {
		t.Fatalf("the counter starts at %d", b.AmbiguousPublications())
	}
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: -1})
	defer fault.Remove()
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("x")}); err == nil {
		t.Fatal("the publication succeeded through the fault")
	}
	if b.AmbiguousPublications() == 0 {
		t.Fatal("an ambiguous publication was not counted, so a deployment cannot tell it happened")
	}
}

// TestAmbiguousBatchIsIndexAligned: an ambiguous batch reports its outcome per
// message, because a caller reconciling a batch has to know WHICH messages it
// cannot account for.
func TestAmbiguousBatchIsIndexAligned(t *testing.T) {
	topic := newTopic("ambig-batch")
	cl := cluster(t, 1, []string{topic})
	c := openOn(t, cl)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	defer func() { _ = p.Close() }()
	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut, Count: -1})
	defer fault.Remove()
	ms := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}, {Body: []byte("c")}}
	err = p.PublishBatch(t.Context(), ms)
	if err == nil {
		t.Fatal("the batch succeeded through the fault")
	}
	var batch *mq.BatchError
	if errors.As(err, &batch) {
		if len(batch.Errs) != len(ms) {
			t.Fatalf("the batch error carries %d outcomes for %d messages", len(batch.Errs), len(ms))
		}
		// EVERY message was refused by the fault, so every outcome has to
		// carry an error. A nil here is the misalignment this test exists
		// for: a driver that collected outcomes in completion order would
		// leave the later indices nil and a caller would believe those
		// messages were accepted.
		for i, e := range batch.Errs {
			if e == nil {
				t.Fatalf("message %d has no outcome, and the fault refused every produce: a caller would believe it was accepted", i)
			}
			assertUnknown(t, e)
		}
		return
	}
	// Every message failed identically, which the SPI reports as one error.
	assertUnknown(t, err)
}

// TestAmbiguousRepublicationNeverAcks is with the ambiguous half rather
// than the failed half: a dead-letter publication whose outcome is UNKNOWN
// must not acknowledge the original either, because the record may be in two
// places or in none and only one of those is safe to forget.
func TestAmbiguousRepublicationNeverAcks(t *testing.T) {
	topic, dlq := newTopic("ambig-dl"), newTopic("ambig-dl-dead")
	cl := cluster(t, 1, []string{topic, dlq})
	c := openOn(t, cl, func(cfg *kq.Config) { cfg.NackConfig.DeadLetterTopic = dlq })
	cons := consumerOn(t, c, topic, "g-ambig-dl")
	publish(t, c, topic, &mq.Message{Body: []byte("poison")})
	d := receiveOne(t, cons, 15*time.Second)

	fault := cl.Fault(kfake.Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: dlq, Err: kerr.RequestTimedOut, Count: -1})
	err := d.Reject()
	fault.Remove()
	if err == nil {
		t.Fatal("Reject succeeded while the dead-letter publication's outcome was unknown: the specification forbids acknowledging the original")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("the refused Reject = %v; a REQUEST_TIMED_OUT on the move is ambiguous, and a caller that retried it needs to know the record may already be there", err)
	}
	if err := cons.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	again := consumerOn(t, c, topic, "g-ambig-dl")
	back := receiveOne(t, again, 15*time.Second)
	if string(back.Body) != "poison" {
		t.Fatalf("the reopened subscription holds %q, want the record whose move was ambiguous", back.Body)
	}
	if err := back.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// assertUnknown holds one error to the answer for an outcome nobody knows.
func assertUnknown(t *testing.T, err error) {
	t.Helper()
	if !mq.IsAmbiguous(err) {
		t.Fatalf("%v is not reported as ambiguous: a caller that retries it may publish twice while believing it published once ", err)
	}
	if !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("%v does not match ErrSettlementUnknown (code %v): the specification names that sentinel for an outcome nobody knows, and a caller who reaches for it has to find it ",
			err, mq.CodeOf(err))
	}
	// The driver's judgement about a transient-but-unknown outcome, logged
	// rather than asserted because it is a judgement and not a defect. It is
	// read off the *OpError: since every error reaching
	// here is ambiguous -- the check above insists on it -- so mq.IsRetryable
	// is false for all of them and `if !mq.IsRetryable(err)` logged
	// unconditionally, and logged something that was false whenever the
	// driver HAD marked the rule retryable (kerr.RequestTimedOut does).
	var oe *mq.OpError
	if errors.As(err, &oe) && !oe.Retryable {
		t.Logf("%v is not marked retryable by the driver, which is a judgement rather than a defect", err)
	}
}

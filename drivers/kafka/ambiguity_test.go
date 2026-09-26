package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
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

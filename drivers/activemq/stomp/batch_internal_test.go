package stomp

import (
	"errors"
	"testing"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
)

// TestAStoppedBatchIsAnsweredPerMessage: PublishBatch is a loop that stops at
// the first failure. It answered with a *PartialBatchError{Published: n},
// which is not index-aligned, so the core gave that one error to EVERY
// message: the n messages already published read as failed, and a caller who
// republished the definite ones duplicated them. The answer is now
// index-aligned: nil for the published prefix, the failure itself, and a
// DEFINITE, retryable not-sent answer for the rest.
//
// The failures are the driver's own, mapped by its own mapper -- above all
// the receipt timeout, stomp's one ambiguous publication failure, which maps
// to CodeSettlementUnknown with the Ambiguous flag. The first version of the
// not-sent answer copied the failure's code, and since CodeSettlementUnknown
// IS ambiguity  a batch that stopped on a receipt timeout
// told every message it never sent "may be in the broker". A test built from
// MapNet(io.EOF) could not see that, because EOF maps to CodeUnavailable.
func TestAStoppedBatchIsAnsweredPerMessage(t *testing.T) {
	m := newErrorMapper()
	receipt := m.MapNet(mq.OpPublish, "t", "", gostomp.ErrMsgReceiptTimeout, true)
	sendTimeout := m.MapNet(mq.OpPublish, "t", "", gostomp.ErrMsgSendTimeout, true)
	if !mq.IsAmbiguous(receipt) || mq.CodeOf(receipt) != mq.CodeSettlementUnknown {
		t.Fatalf("the instrument is broken: the receipt timeout maps to %v (code %v), want an ambiguous SettlementUnknown",
			receipt, mq.CodeOf(receipt))
	}
	if mq.IsAmbiguous(sendTimeout) || !mq.IsRetryable(sendTimeout) {
		t.Fatalf("the instrument is broken: the send timeout maps to %v, want a definite, retryable failure", sendTimeout)
	}
	s := &Sink{topic: "t"}

	// notSent holds one entry to the not-sent answer: definite, retryable,
	// and the failure's code only where that code is not an unknown outcome.
	notSent := func(t *testing.T, name string, e error) {
		t.Helper()
		if e == nil || mq.IsAmbiguous(e) {
			t.Fatalf("%s = %v: IsAmbiguous = %v, want a definite answer: it was never sent", name, e, mq.IsAmbiguous(e))
		}
		if c := mq.CodeOf(e); c == mq.CodeSettlementUnknown || c == mq.CodeAmbiguous {
			t.Fatalf("%s = %v carries %v, a code whose meaning is an unknown outcome", name, e, c)
		}
		if !mq.IsRetryable(e) {
			t.Fatalf("%s = %v is not retryable; a message that never left is safe to send", name, e)
		}
	}

	// Stopped part-way on a receipt timeout.
	got := s.stoppedAt(2, 4, receipt)
	var be *mq.BatchError
	if !errors.As(got, &be) || len(be.Errs) != 4 {
		t.Fatalf("stoppedAt(2 of 4, receipt timeout) = %v, want an index-aligned *BatchError", got)
	}
	if be.Errs[0] != nil || be.Errs[1] != nil {
		t.Fatalf("the published prefix reads %v, want nil: those messages are in the broker", be.Errs[:2])
	}
	if be.Errs[2] != receipt || !mq.IsAmbiguous(got) { //nolint:errorlint // identity: the failure itself
		t.Fatalf("message 2 = %v, want the receipt timeout itself", be.Errs[2])
	}
	notSent(t, "stoppedAt(2 of 4, receipt timeout) message 3", be.Errs[3])
	if !errors.Is(got, mq.ErrPartial) {
		t.Fatalf("stoppedAt(2 of 4) = %v does not match ErrPartial", got)
	}

	// Stopped at the FIRST message on a receipt timeout: the first may be in
	// the broker and the rest were never sent, so the answer is per message.
	got = s.stoppedAt(0, 3, receipt)
	if !errors.As(got, &be) || len(be.Errs) != 3 || be.Errs[0] != receipt { //nolint:errorlint // identity: the failure itself
		t.Fatalf("stoppedAt(0 of 3, receipt timeout) = %v, want the per-message answer with the receipt timeout first", got)
	}
	notSent(t, "stoppedAt(0 of 3, receipt timeout) message 1", be.Errs[1])
	notSent(t, "stoppedAt(0 of 3, receipt timeout) message 2", be.Errs[2])

	// Stopped at the first message on a definite failure: every message has
	// one outcome, and the single error is the answer, as it always was.
	if got := s.stoppedAt(0, 3, sendTimeout); got != sendTimeout { //nolint:errorlint // identity: the failure itself
		t.Fatalf("stoppedAt(0 of 3, send timeout) = %v, want the failure itself", got)
	}
	got = s.stoppedAt(1, 3, sendTimeout)
	if !errors.As(got, &be) || len(be.Errs) != 3 {
		t.Fatalf("stoppedAt(1 of 3, send timeout) = %v, want the per-message answer", got)
	}
	notSent(t, "stoppedAt(1 of 3, send timeout) message 2", be.Errs[2])
	if mq.CodeOf(be.Errs[2]) != mq.CodeOf(sendTimeout) {
		t.Fatalf("message 2 carries %v, want the failure's own definite code %v", mq.CodeOf(be.Errs[2]), mq.CodeOf(sendTimeout))
	}
}

package stomp

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
)

// TestAnErrorFrameAnswersOnlyThePublicationItProvablyIs is [answers] case by
// case: a `receipt-id` decides it both ways, and without one only Classic's
// codec refusal, on a connection this backend dialled with nothing else
// unanswered, is the publication's.
func TestAnErrorFrameAnswersOnlyThePublicationItProvablyIs(t *testing.T) {
	const (
		ours  = "iqmq-7"
		codec = "The maximum frame size was exceeded"
		authz = "AMQ229032 User reader does not have permission='SEND' on address orders"
	)
	count := func(n int64) *atomic.Int64 {
		c := new(atomic.Int64)
		c.Store(n)
		return c
	}
	errorFrame := func(message string, receiptID ...string) *frame.Frame {
		f := frame.New(frame.ERROR, frame.Message, message)
		for _, id := range receiptID {
			f.Header.Set(frame.ReceiptId, id)
		}
		return f
	}
	for _, tc := range []struct {
		name       string
		f          *frame.Frame
		unanswered *atomic.Int64
		want       bool
	}{
		{"names-this-receipt", errorFrame(authz, ours), count(3), true},
		{"names-another-receipt", errorFrame(authz, "iqmq-6"), count(1), false},
		{"names-another-receipt-for-a-codec-refusal", errorFrame(codec, "iqmq-6"), count(1), false},
		{"no-receipt-id-codec-nothing-else-unanswered", errorFrame(codec), count(1), true},
		{"no-receipt-id-codec-another-frame-unanswered", errorFrame(codec), count(2), false},
		{"no-receipt-id-codec-on-the-callers-connection", errorFrame(codec), nil, false},
		{"no-receipt-id-not-a-codec-refusal", errorFrame(authz), count(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := answers(tc.f, ours, tc.unanswered); got != tc.want {
				t.Errorf("answers(%v) = %v, want %v", tc.f.Header, got, tc.want)
			}
		})
	}
}

// TestAStrayErrorFrameIsNotReadAsTheRefusalItNames: the stray error carries
// the broker's sentence, and the sentence rules would claim it -- as Forbidden
// here -- if `stray` did not come first. OpNack matters as much as OpPublish:
// a dead-letter copy's publication error is mapped again under OpNack, and a
// Forbidden there told the caller its dead-letter route was refused when the
// copy's outcome was simply unknown.
func TestAStrayErrorFrameIsNotReadAsTheRefusalItNames(t *testing.T) {
	m := newErrorMapper()
	stray := &strayErrorFrame{frame: ErrorFrameError{
		Message: "AMQ229032 User reader does not have permission='CREATE_ADDRESS' on address denied",
	}}
	published := m.MapNet(mq.OpPublish, "orders", "", stray, true)
	if !mq.IsAmbiguous(published) || mq.CodeOf(published) != mq.CodeUnavailable {
		t.Fatalf("the stray ERROR frame maps to %v (code %v, ambiguous=%v) on publish, want an ambiguous Unavailable",
			published, mq.CodeOf(published), mq.IsAmbiguous(published))
	}
	var oe *mq.OpError
	if !errors.As(published, &oe) || !oe.Retryable {
		t.Errorf("the stray ERROR frame is not retryable on publish: %v; the next connection can take it", published)
	}
	var ef *ErrorFrameError
	if !errors.As(published, &ef) || ef.Message != stray.frame.Message {
		t.Errorf("the mapped error %v does not reach the broker's sentence as an *ErrorFrameError", published)
	}
	nacked := m.Map(mq.OpNack, "orders", "workers", published)
	if got := mq.CodeOf(nacked); got != mq.CodeUnavailable {
		t.Errorf("the dead-letter copy's stray ERROR frame maps to %v under OpNack, want %v: the sentence refuses "+
			"another frame, not the dead-letter route", got, mq.CodeUnavailable)
	}
}

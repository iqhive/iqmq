package stomp

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	gostomp "github.com/go-stomp/stomp/v3"
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

// TestTheSentenceRulesReadEachBrokersOwnWording maps the ERROR frames captured
// from the pinned images, headers as they came off the wire (the bodies, and
// the content-length that goes with one, left out), through the rules a
// publication's own refusal reaches once [answers] has claimed it.
//
// Two of Classic's refusals mapped to CodeUnknown, definite and not
// retryable, so a caller could tell neither from a fault it knew nothing
// about: its authorisation refusal says "is not authorized to", lower case,
// where the rule matched "Not authorized", and its codec's "The maximum frame
// size was exceeded" contains neither of the phrases the MessageTooLarge rule
// matched. The cases marked CodeUnknown pin what the wider phrases must not
// claim: Classic's codec sentence about a frame it could not find the start
// of, a converter refusal no rule names, and Artemis's own sentences.
func TestTheSentenceRulesReadEachBrokersOwnWording(t *testing.T) {
	m := newErrorMapper()
	// refusal is what go-stomp hands a receipt wait for an ERROR frame.
	refusal := func(t *testing.T, headers string) error {
		t.Helper()
		f, err := frame.NewReader(strings.NewReader("ERROR\n" + headers + "\n\n\x00")).Read()
		if err != nil {
			t.Fatalf("reading the captured frame: %v", err)
		}
		return gostomp.Error{Message: f.Header.Get(frame.Message), Frame: f}
	}
	for _, tc := range []struct {
		name    string
		headers string
		op      string
		want    mq.ErrorCode
	}{
		// Captured from the pinned images.
		{
			"classic-authorisation",
			"content-type:text/plain\nreceipt-id:rcpt-refused-1\nmessage:User reader is not authorized to write to\\c queue\\c//denied",
			mq.OpPublish, mq.CodeForbidden,
		},
		{
			"classic-frame-size",
			"content-type:text/plain\nmessage:The maximum frame size was exceeded",
			mq.OpPublish, mq.CodeMessageTooLarge,
		},
		{
			"classic-command-length",
			"content-type:text/plain\nmessage:The maximum command length was exceeded",
			mq.OpPublish, mq.CodeUnknown,
		},
		{
			"classic-converter",
			"content-type:text/plain\nreceipt-id:rcpt-refused-1\nmessage:Invalid transaction id\\c nope",
			mq.OpPublish, mq.CodeUnknown,
		},
		{
			"artemis-authorisation",
			"message:AMQ229032 User reader does not have permission='CREATE_ADDRESS' on address denied\ncontent-type:text/plain\nreceipt-id:rcpt-refused-1",
			mq.OpPublish, mq.CodeForbidden,
		},
		{
			"artemis-malformed-send",
			"message:AMQ339031 Error handling send\nreceipt-id:rcpt-refused-1",
			mq.OpPublish, mq.CodeUnknown,
		},
		{
			"artemis-connection-not-valid",
			"message:AMQ339010 Connection is not valid.\nreceipt-id:rcpt-next-3",
			mq.OpPublish, mq.CodeUnknown,
		},
		// The same sources' other sentences of each kind, from activemq-broker
		// and activemq-stomp 6.2.0 rather than provoked.
		{
			"classic-read-authorisation",
			"content-type:text/plain\nmessage:User reader is not authorized to read from\\c queue\\c//Consumer.workers.VirtualTopic.orders",
			mq.OpFetch, mq.CodeForbidden,
		},
		{
			"classic-create-authorisation",
			"content-type:text/plain\nmessage:User reader is not authorized to create\\c topic\\c//VirtualTopic.orders",
			mq.OpFetch, mq.CodeForbidden,
		},
		{"classic-data-length", "content-type:text/plain\nmessage:The maximum data length was exceeded", mq.OpPublish, mq.CodeMessageTooLarge},
		{"classic-header-length", "content-type:text/plain\nmessage:The maximum header length was exceeded", mq.OpPublish, mq.CodeMessageTooLarge},
		{"classic-header-count", "content-type:text/plain\nmessage:The maximum number of headers was exceeded", mq.OpPublish, mq.CodeMessageTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := refusal(t, tc.headers)
			got := m.MapNet(tc.op, "orders", "workers", err, tc.op == mq.OpPublish)
			if mq.CodeOf(got) != tc.want {
				t.Errorf("the broker's %q maps to %v under %s, want %v", err, mq.CodeOf(got), tc.op, tc.want)
			}
			if mq.IsAmbiguous(got) {
				t.Errorf("the broker's %q maps to an ambiguous %v; a refusal the publication claimed is definite", err, got)
			}
			if !strings.Contains(got.Error(), err.Error()) {
				t.Errorf("the mapped error %v dropped the broker's sentence %q", got, err)
			}
		})
	}
}

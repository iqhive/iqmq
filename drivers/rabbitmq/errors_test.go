package rabbitmq_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
)

// TestErrorMapping drives this driver rule table with the values the
// CLIENT LIBRARY really produces.
//
// That is the lesson applied rather than restated: an error table written
// against the text a broker sends is a table nothing holds to what reaches the
// mapper. Every input below is either a real `*amqp091.Error` with a real AMQP
// reply code or one of the library's own package-level values, and the table
// is checked per OPERATION because the same code means different things in
// different places.
func TestErrorMapping(t *testing.T) {
	b := offlineBackend(t)
	for _, tc := range []struct {
		name      string
		op        string
		err       error
		code      mq.ErrorCode
		retryable bool
	}{
		// The rows where the operation changes the answer. These are the
		// reason the table is per operation at all, and a mapper with one
		// flat table would get one of each pair wrong.
		{"404 on publish is a missing exchange", mq.OpPublish, serverErr(amqp.NotFound, "no exchange 'orders'"), mq.CodeNotFound, false},
		{"404 on a settlement is a lost lease", mq.OpAck, serverErr(amqp.NotFound, "no queue 'orders.workers'"), mq.CodeLeaseLost, false},
		{"404 on a fetch is a missing queue", mq.OpFetch, serverErr(amqp.NotFound, "no queue"), mq.CodeNotFound, false},
		{"406 on a settlement is a lost lease", mq.OpAck, serverErr(amqp.PreconditionFailed, "unknown delivery tag 7"), mq.CodeLeaseLost, false},
		{"406 on open is a topology conflict", mq.OpOpen, serverErr(amqp.PreconditionFailed, "inequivalent arg 'x-queue-type'"), mq.CodeConflict, false},

		// The unroutable publication, which is Rejected.
		{"312 is an unroutable publication", mq.OpPublish, serverErr(amqp.NoRoute, "NO_ROUTE"), mq.CodeNotFound, false},

		// The shared rows.
		{"403 is forbidden", mq.OpPublish, serverErr(amqp.AccessRefused, "operation not permitted"), mq.CodeForbidden, false},
		{"405 is a locked resource", mq.OpFetch, serverErr(amqp.ResourceLocked, "exclusive consumer"), mq.CodeConflict, false},
		{"506 is resource exhaustion", mq.OpPublish, serverErr(amqp.ResourceError, "no memory"), mq.CodeResourceExhausted, true},
		{"311 is an oversized message", mq.OpPublish, serverErr(amqp.ContentTooLarge, "frame too large"), mq.CodeMessageTooLarge, false},
		{"530 is forbidden", mq.OpOpen, serverErr(amqp.NotAllowed, "vhost not allowed"), mq.CodeForbidden, false},
		{"540 is unsupported", mq.OpPublish, serverErr(amqp.NotImplemented, "not implemented"), mq.CodeUnsupported, false},
		{"320 is the broker going away", mq.OpPublish, serverErr(amqp.ConnectionForced, "broker forced connection closure"), mq.CodeUnavailable, true},
		{"503 is unsupported", mq.OpOpen, serverErr(amqp.CommandInvalid, "unknown exchange type 'x-delayed-message'"), mq.CodeUnsupported, false},
		{"541 is unknown and retryable", mq.OpPublish, serverErr(amqp.InternalError, "internal error"), mq.CodeUnknown, true},
		{"502 is a corrupt conversation", mq.OpPublish, serverErr(amqp.SyntaxError, "bad frame"), mq.CodeCorruptMessage, false},

		// The library's own values. They are the half a table written
		// against the specification alone would miss, and the 504 pair is
		// the sharpest case: `amqp.ErrClosed` is a *package-level* value
		// with code 504 and no `Is` method, so a table matching it with
		// errors.Is would miss a server-sent 504 and vice versa. Both
		// arrive here.
		{"the library's ErrClosed", mq.OpPublish, amqp.ErrClosed, mq.CodeClosed, true},
		{"a server-sent 504", mq.OpPublish, serverErr(amqp.ChannelError, "channel/connection is not open"), mq.CodeClosed, true},
		{"the library's ErrCredentials", mq.OpOpen, amqp.ErrCredentials, mq.CodeUnauthorized, false},
		{"the library's ErrVhost", mq.OpOpen, amqp.ErrVhost, mq.CodeUnauthorized, false},
		{"the library's ErrSASL", mq.OpOpen, amqp.ErrSASL, mq.CodeUnauthorized, false},
		{"the library's ErrChannelMax", mq.OpOpen, amqp.ErrChannelMax, mq.CodeResourceExhausted, true},
		{"the library's ErrFieldType", mq.OpPublish, amqp.ErrFieldType, mq.CodeInvalidArgument, false},

		// The transport, which has no reply code at all.
		{"a dial failure", mq.OpOpen, &net.OpError{Op: "dial", Err: errors.New("connection refused")}, mq.CodeUnavailable, true},
		{"an unexpected EOF", mq.OpFetch, io.ErrUnexpectedEOF, mq.CodeUnavailable, true},
		{"a cancelled context", mq.OpPublish, context.Canceled, mq.CodeCanceled, false},
		{"an expired deadline", mq.OpPublish, context.DeadlineExceeded, mq.CodeDeadlineExceeded, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rq.MapForTest(b, tc.op, tc.err)
			if code := mq.CodeOf(got); code != tc.code {
				t.Errorf("code = %v, want %v (%v)", code, tc.code, got)
			}
			var oe *mq.OpError
			if !errors.As(got, &oe) {
				t.Fatalf("the mapped error is not an *OpError: %T", got)
			}
			if oe.Retryable != tc.retryable {
				t.Errorf("retryable = %v, want %v", oe.Retryable, tc.retryable)
			}
			if oe.Driver != "rabbitmq" {
				t.Errorf("driver = %q", oe.Driver)
			}
			// The specification: the native error survives, so errors.As still reaches
			// the library's own type and a caller who needs the reply code
			// has it.
			if !errors.Is(got, tc.err) && !errorsAsAMQP(got) {
				t.Errorf("the native error did not survive the mapping: %v", got)
			}
		})
	}
	if rq.MapForTest(b, mq.OpPublish, nil) != nil {
		t.Error("a nil error mapped to something")
	}
}

// errorsAsAMQP reports whether an *amqp091.Error is still reachable.
func errorsAsAMQP(err error) bool {
	var aerr *amqp.Error
	return errors.As(err, &aerr)
}

// serverErr is an error as the library decodes one from the broker.
func serverErr(code int, reason string) *amqp.Error {
	return &amqp.Error{Code: code, Reason: reason, Server: true}
}

// TestAmbiguityOnTheWire is the distinction and the disqualifier.
//
// A publication whose frame may already have left the socket has an UNKNOWN
// outcome: the broker may have stored it. Reporting that as a definite failure
// invites a retry that duplicates the message, and the specification names doing so a
// settlement disqualifier. A broker REPLY is the opposite: it means the broker
// processed the command and refused it, so the outcome is definite however the
// caller reached it.
func TestAmbiguityOnTheWire(t *testing.T) {
	b := offlineBackend(t)
	for _, tc := range []struct {
		name        string
		err         error
		sentAlready bool
		ambiguous   bool
	}{
		{"a reply is definite even after a send", serverErr(amqp.NoRoute, "NO_ROUTE"), true, false},
		{"a 403 is definite even after a send", serverErr(amqp.AccessRefused, "denied"), true, false},
		{"an EOF before the send is definite", io.ErrUnexpectedEOF, false, false},
		{"an EOF after the send is ambiguous", io.ErrUnexpectedEOF, true, true},
		{"a deadline after the send is ambiguous", context.DeadlineExceeded, true, true},
		{"a closed connection after the send is ambiguous", net.ErrClosed, true, true},
		// A reply CODE means an answer, and the library's ErrClosed is the
		// answer "this handle was already closed": the frame was never
		// written, so nothing is ambiguous. The ambiguous case on this
		// driver's publish path is a channel that dies WHILE the frame is
		// in the socket, and that is not an error the mapper sees at all --
		// `sink.publishFailedLocked` recognises it from the channel's own
		// state and detaches, which resolves every publication the channel
		// was holding as Ambiguous. TestCriticalChannelCloseDuringConfirm
		// is where that is proved.
		{"the library's ErrClosed is a definite answer", amqp.ErrClosed, true, false},
		{"a net.Error after the send is ambiguous", &net.OpError{Op: "write", Err: errors.New("broken pipe")}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rq.MapNetForTest(b, mq.OpPublish, tc.err, tc.sentAlready)
			var oe *mq.OpError
			if !errors.As(got, &oe) {
				t.Fatalf("not an *OpError: %T", got)
			}
			if oe.Ambiguous != tc.ambiguous {
				t.Fatalf("ambiguous = %v, want %v (%v)", oe.Ambiguous, tc.ambiguous, got)
			}
		})
	}
}

// TestReplyCodeAndChannelException are the two pieces of AMQP detail this
// driver exports, because the portable codes deliberately do not carry
// them and a caller acts on both.
func TestReplyCodeAndChannelException(t *testing.T) {
	if code, ok := rq.ReplyCode(serverErr(amqp.PreconditionFailed, "x")); !ok || code != 406 {
		t.Errorf("ReplyCode = (%d, %v), want (406, true)", code, ok)
	}
	if _, ok := rq.ReplyCode(io.EOF); ok {
		t.Error("ReplyCode found a reply code in a transport error")
	}
	// A CHANNEL exception closes the channel and takes every in-flight
	// confirm with it; a CONNECTION exception takes the whole connection.
	// The two need different repairs, which is why the distinction is
	// exported.
	for _, code := range []int{amqp.NotFound, amqp.AccessRefused, amqp.PreconditionFailed, amqp.ResourceLocked, amqp.ContentTooLarge, amqp.NoRoute} {
		if !rq.IsChannelException(serverErr(code, "x")) {
			t.Errorf("reply code %d is not reported as a channel exception", code)
		}
	}
	for _, code := range []int{amqp.ConnectionForced, amqp.FrameError, amqp.ChannelError, amqp.InternalError} {
		if rq.IsChannelException(serverErr(code, "x")) {
			t.Errorf("reply code %d is reported as a channel exception, and it is a connection-level one", code)
		}
	}
	if rq.IsChannelException(nil) {
		t.Error("a nil error is a channel exception")
	}
}

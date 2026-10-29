package sqs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"

	mq "github.com/iqhive/iqmq"
	sq "github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/drivers/sqs/sqsfake"
)

// the ambiguity, which is the one classification a driver cannot infer
// from a native error type.
//
// Every SQS operation is one HTTP request. A failure BEFORE the request leaves
// the socket is a clean refusal the caller may retry; the same failure after it
// may have been accepted by the service, so a retry can duplicate and
// reporting it as a definite failure makes a duplicate look like a loss. The
// distinction is the driver's, and these are the tests that hold it.

// TestAmbiguousPublish is the publication half.
//
// A transport failure on the way back from a SendMessage is ambiguous: the
// message may be on the queue. A driver that reported it as a plain failure
// would let a caller retry and duplicate while believing it had recovered a
// loss.
func TestAmbiguousPublish(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	name := topic(t, "ambiguous-publish")
	// The queue is created first, so the failure below is the SEND's and not
	// the resolution's.
	h.publish(name, &mq.Message{Body: []byte("seed")})

	server.Fault(sqsfake.Fault{Op: "SendMessage", Err: io.ErrUnexpectedEOF, Count: 1})
	err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("maybe")})
	if err == nil {
		t.Fatal("the publication succeeded although its round trip failed")
	}
	if !mq.IsAmbiguous(err) {
		t.Fatalf("a publication whose round trip failed after the request left the socket = %v, and it is not marked ambiguous: the specification requires the distinction, and a caller that retried would duplicate", err)
	}
	// The driver's own judgement is unchanged -- a transport failure IS
	// transient -- but it is read where it lives, because mq.IsRetryable now
	// answers "safe to repeat" and an ambiguous publication never is.
	// This check used to be `!mq.IsRetryable(err)`,
	// which was the independent-flags reading of the same value.
	var e *mq.OpError
	if !errors.As(err, &e) || !e.Retryable {
		t.Fatalf("the failure is not marked retryable: %v", err)
	}
	if mq.IsRetryable(err) {
		t.Fatalf("IsRetryable reported an ambiguous publication safe to repeat (%v); a caller that repeated it would duplicate", err)
	}
}

// TestAmbiguousBatchPublish is the same rule for a batch: the whole call
// failed, so every entry of that chunk is ambiguous and every entry of another
// chunk is not affected.
func TestAmbiguousBatchPublish(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	name := topic(t, "ambiguous-batch")
	h.publish(name, &mq.Message{Body: []byte("seed")})
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	// The first chunk fails outright; the second is accepted.
	server.Fault(sqsfake.Fault{Op: "SendMessageBatch", Err: io.ErrUnexpectedEOF, Count: 1})
	ms := make([]*mq.Message, 12)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte("m" + string(rune('a'+i)))}
	}
	err = pub.PublishBatch(t.Context(), ms)
	var be *mq.BatchError
	if !errors.As(err, &be) {
		t.Fatalf("PublishBatch = %v, want a *BatchError", err)
	}
	if len(be.Errs) != len(ms) {
		t.Fatalf("%d outcome(s) for %d messages", len(be.Errs), len(ms))
	}
	ambiguous, accepted := 0, 0
	for i, e := range be.Errs {
		switch {
		case e == nil:
			accepted++
		case mq.IsAmbiguous(e):
			ambiguous++
		default:
			t.Errorf("message %d failed with %v, which is neither nil nor ambiguous", i, e)
		}
	}
	if ambiguous != 10 || accepted != 2 {
		t.Fatalf("%d ambiguous and %d accepted of 12; the first chunk of ten failed as a whole and the second was taken", ambiguous, accepted)
	}
}

// TestAmbiguousSettlementIsSticky is the settlement half, and the rule that
// an unknown outcome never becomes a success.
//
// A delete whose round trip failed may have been taken. The driver cannot
// tell, so the delivery is UNKNOWN for ever: a retry reports the unknown
// rather than deleting a message that may already be gone, and the message the
// service still holds is redelivered when its window passes.
func TestAmbiguousSettlementIsSticky(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "ambiguous-settle")
	h.publish(name, &mq.Message{Body: []byte("unknown")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)

	server.Fault(sqsfake.Fault{Op: "DeleteMessageBatch", Err: io.ErrUnexpectedEOF, Count: 1})
	err := d.Ack()
	if err == nil {
		t.Fatal("the settlement succeeded although its round trip failed")
	}
	if !mq.IsAmbiguous(err) && mq.CodeOf(err) != mq.CodeSettlementUnknown {
		t.Fatalf("a settlement whose round trip failed = %v; the specification requires an unknown outcome rather than a definite failure", err)
	}
	// Sticky: the retry reports the unknown rather than trying again.
	again := d.Ack()
	if mq.CodeOf(again) != mq.CodeSettlementUnknown && !errors.Is(again, mq.ErrSettlementUnknown) {
		t.Fatalf("the retried settlement = %v, want the sticky unknown ", again)
	}
}

// TestDefiniteSettlementRefusalLeavesItSettleable is the other side: a failure
// the driver CAN call definite leaves the delivery settleable, so a caller may
// try again. For a partial delete batch that is the only thing between the
// entries that failed and a redelivery.
func TestDefiniteSettlementRefusalLeavesItSettleable(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server, func(c *sq.Config) { c.AckDeadline = time.Hour })
	name := topic(t, "definite-refusal")
	h.publish(name, &mq.Message{Body: []byte("retry-me")})
	cons := h.consumer(name, 4)
	d := h.receiveOne(cons, 5*time.Second)

	server.Fault(sqsfake.Fault{
		Op:     "DeleteMessageBatch",
		Bodies: map[string]string{"retry-me": "InternalError"},
		Count:  1,
	})
	if err := d.Ack(); err == nil {
		t.Fatal("the settlement succeeded although the service refused the entry")
	} else if mq.IsAmbiguous(err) {
		t.Fatalf("a per-entry refusal was reported as ambiguous: %v; the service answered about this entry, so the outcome is known", err)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("the retried settlement = %v; a definite refusal leaves the delivery settleable ", err)
	}
	if got := h.depth(name); got != 0 {
		t.Fatalf("%d message(s) left after the retry succeeded", got)
	}
}

// TestErrorMappingTable holds the rule table row by row.
//
// It goes through the mapper directly, which is the only way to reach every
// row: half of them are typed errors the service raises for operations nothing
// in the suite performs, and the other half are code strings the SDK surfaces
// with no generated type at all.
func TestErrorMappingTable(t *testing.T) {
	h := open(t)
	for _, tc := range []struct {
		name      string
		op        string
		err       error
		code      mq.ErrorCode
		retryable bool
		ambiguous bool
	}{
		{"missing queue", mq.OpPublish, &types.QueueDoesNotExist{Message: aws.String("q")}, mq.CodeNotFound, false, false},
		{"resource not found", mq.OpAdmin, &types.ResourceNotFoundException{}, mq.CodeNotFound, false, false},
		{"invalid receipt handle", mq.OpAck, &types.ReceiptHandleIsInvalid{}, mq.CodeLeaseLost, false, false},
		{"message not in flight", mq.OpNack, &types.MessageNotInflight{}, mq.CodeLeaseLost, false, false},
		{"invalid security", mq.OpPublish, &types.InvalidSecurity{}, mq.CodeUnauthorized, false, false},
		{"unsupported operation", mq.OpAdmin, &types.UnsupportedOperation{}, mq.CodeUnsupported, false, false},
		{"bad batch entry id", mq.OpPublish, &types.InvalidBatchEntryId{}, mq.CodeInvalidArgument, false, false},
		{"batch ids not distinct", mq.OpPublish, &types.BatchEntryIdsNotDistinct{}, mq.CodeInvalidArgument, false, false},
		{"too many entries", mq.OpAck, &types.TooManyEntriesInBatchRequest{}, mq.CodeInvalidArgument, false, false},
		{"empty batch", mq.OpAck, &types.EmptyBatchRequest{}, mq.CodeInvalidArgument, false, false},
		{"batch too long", mq.OpPublish, &types.BatchRequestTooLong{}, mq.CodeMessageTooLarge, false, false},
		{"invalid contents", mq.OpPublish, &types.InvalidMessageContents{}, mq.CodeMessageTooLarge, false, false},
		{"over limit", mq.OpFetch, &types.OverLimit{}, mq.CodeResourceExhausted, true, false},
		{"queue name exists", mq.OpAdmin, &types.QueueNameExists{}, mq.CodeConflict, false, false},
		{"queue deleted recently", mq.OpAdmin, &types.QueueDeletedRecently{}, mq.CodeConflict, false, false},
		{"purge in progress", mq.OpAdmin, &types.PurgeQueueInProgress{}, mq.CodeConflict, false, false},
		{"kms disabled", mq.OpPublish, &types.KmsDisabled{}, mq.CodeUnavailable, false, false},
		{"kms access denied", mq.OpPublish, &types.KmsAccessDenied{}, mq.CodeUnauthorized, false, false},
		{"kms throttled on a publish is definite", mq.OpPublish, &types.KmsThrottled{}, mq.CodeThrottled, true, false},
		{"kms throttled on a fetch", mq.OpFetch, &types.KmsThrottled{}, mq.CodeThrottled, true, false},
		{"kms throttled on an ack", mq.OpAck, &types.KmsThrottled{}, mq.CodeThrottled, true, false},
		{"throttled on a publish is ambiguous", mq.OpPublish, &types.RequestThrottled{}, mq.CodeThrottled, true, true},
		{"throttled on a fetch is not", mq.OpFetch, &types.RequestThrottled{}, mq.CodeThrottled, true, false},
		{"internal error on a publish is ambiguous", mq.OpPublish, apiError("InternalError"), mq.CodeUnavailable, true, true},
		{"internal error on a fetch is not", mq.OpFetch, apiError("InternalError"), mq.CodeUnavailable, true, false},
		{"access denied", mq.OpPublish, apiError("AccessDenied"), mq.CodeForbidden, false, false},
		{"bad signature", mq.OpPublish, apiError("SignatureDoesNotMatch"), mq.CodeForbidden, false, false},
		{"unrecognised client", mq.OpFetch, apiError("UnrecognizedClientException"), mq.CodeForbidden, false, false},
		{"throttling exception", mq.OpAdmin, apiError("ThrottlingException"), mq.CodeThrottled, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mapped := sq.MapForTest(h.back, tc.op, tc.err)
			if got := mq.CodeOf(mapped); got != tc.code {
				t.Errorf("code = %v, want %v (%v)", got, tc.code, mapped)
			}
			// The table's retryable column is the DRIVER's judgement, which
			// lives on the *OpError; it used to be read through
			// mq.IsRetryable, which now answers "safe to repeat" -- the
			// judgement after the precedence, where ambiguity wins.
			// Both are asserted, so the two rows whose
			// verdict the precedence changes ("throttled on a publish is
			// ambiguous", "internal error on a publish is ambiguous") still
			// pin the flag the driver sets.
			var e *mq.OpError
			if !errors.As(mapped, &e) {
				t.Fatalf("the mapping is not an *mq.OpError: %v", mapped)
			}
			if e.Retryable != tc.retryable {
				t.Errorf("retryable = %v, want %v", e.Retryable, tc.retryable)
			}
			if got, want := mq.IsRetryable(mapped), tc.retryable && !tc.ambiguous; got != want {
				t.Errorf("IsRetryable = %v, want %v (flag %v, ambiguous %v)", got, want, tc.retryable, tc.ambiguous)
			}
			if got := mq.IsAmbiguous(mapped); got != tc.ambiguous {
				t.Errorf("ambiguous = %v, want %v", got, tc.ambiguous)
			}
			// The specification: the native error stays reachable.
			if !errors.Is(mapped, tc.err) && !errors.As(mapped, new(smithy.APIError)) {
				t.Errorf("the native error is no longer reachable through the mapping: %v", mapped)
			}
		})
	}
	if got := sq.MapForTest(h.back, mq.OpPublish, nil); got != nil {
		t.Fatalf("a nil error mapped to %v", got)
	}
}

// TestTransportClassification is the half no rule can answer: the same
// transport failure is a clean refusal before the write and ambiguous after
// it.
func TestTransportClassification(t *testing.T) {
	h := open(t)
	for _, err := range []error{
		io.EOF,
		io.ErrUnexpectedEOF,
		net.ErrClosed,
		&net.OpError{Op: "read", Err: timeoutError{}},
	} {
		clean := sq.MapNetForTest(h.back, mq.OpPublish, err, false)
		sent := sq.MapNetForTest(h.back, mq.OpPublish, err, true)
		if mq.IsAmbiguous(clean) {
			t.Errorf("%v before the write is marked ambiguous; nothing reached the service", err)
		}
		if !mq.IsAmbiguous(sent) {
			t.Errorf("%v after the write is not marked ambiguous; the service may have taken the request", err)
		}
		// The same transport failure before the write IS safe to repeat, and
		// that is the half mq.IsRetryable now answers: it used to report the
		// post-write one retryable too, because it read the flag alone.
		// The flag itself is still set on both and is
		// asserted below, so nothing about the driver's mapping is weakened.
		if !mq.IsRetryable(clean) {
			t.Errorf("%v before the write is not safe to repeat, although nothing reached the service", err)
		}
		if mq.IsRetryable(sent) {
			t.Errorf("%v after the write reported itself safe to repeat; repeating it is how the request is taken twice", err)
		}
		var e *mq.OpError
		if !errors.As(sent, &e) || !e.Retryable {
			t.Errorf("%v is not marked retryable by the driver", err)
		}
	}
}

// apiError is a smithy API error with a code and no generated type, which is
// how the service reports a whole class of failures.
func apiError(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: "synthetic"}
}

// timeoutError is a net.Error that timed out.
type timeoutError struct{}

func (timeoutError) Error() string   { return "synthetic timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// sdkTimeout is the shape the AWS SDK gives a request whose deadline passed
// after it was sent: an operation error whose chain ends in
// context.DeadlineExceeded.
func sdkTimeout(op string) error {
	return &smithy.OperationError{
		ServiceID: "SQS", OperationName: op,
		Err: fmt.Errorf("request send failed: %w", context.DeadlineExceeded),
	}
}

// TestASentPublishThatTimedOutIsAmbiguous is the timeout the SDK reports after
// the request left the socket. Its chain ends in context.DeadlineExceeded,
// which carries a portable code of its own, and the sink used to return it as
// a definite, retryable deadline: a caller that retried duplicated a message
// the service may already hold.
func TestASentPublishThatTimedOutIsAmbiguous(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	name := topic(t, "sent-timeout")
	h.publish(name, &mq.Message{Body: []byte("seed")})

	server.Fault(sqsfake.Fault{Op: "SendMessage", Err: sdkTimeout("SendMessage"), Count: 1})
	err := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("maybe")})
	if err == nil {
		t.Fatal("the publication succeeded although its round trip timed out")
	}
	if !mq.IsAmbiguous(err) || mq.IsRetryable(err) {
		t.Fatalf("a publication that timed out after it was sent = %v (ambiguous %v, retryable %v), want ambiguous and not retryable",
			err, mq.IsAmbiguous(err), mq.IsRetryable(err))
	}
}

// TestASentBatchThatTimedOutIsAmbiguous is the same rule for a chunk whose
// whole SendMessageBatch timed out after it was sent.
func TestASentBatchThatTimedOutIsAmbiguous(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	name := topic(t, "sent-timeout-batch")
	h.publish(name, &mq.Message{Body: []byte("seed")})
	pub, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: name})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	server.Fault(sqsfake.Fault{Op: "SendMessageBatch", Err: sdkTimeout("SendMessageBatch"), Count: 1})
	ms := []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}}
	// Every entry shares the one outcome, so the batch answer may collapse to
	// it; either shape must say the outcome is unknown.
	err = pub.PublishBatch(t.Context(), ms)
	if err == nil {
		t.Fatal("the batch succeeded although its round trip timed out")
	}
	if !mq.IsAmbiguous(err) || mq.IsRetryable(err) {
		t.Fatalf("a chunk that timed out after it was sent = %v, want ambiguous and not retryable", err)
	}
}

// TestAPublishWhoseContextWasDoneBeforeTheSendIsDefinite is the other side: a
// context that ended before the request was made means nothing was sent, so
// the failure is definite and not ambiguous.
func TestAPublishWhoseContextWasDoneBeforeTheSendIsDefinite(t *testing.T) {
	server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
	h := openOn(t, server)
	name := topic(t, "done-before-send")
	h.publish(name, &mq.Message{Body: []byte("seed")})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := h.client.Publish(ctx, name, &mq.Message{Body: []byte("never")})
	if err == nil {
		t.Fatal("a publication on a cancelled context succeeded")
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("a publication whose context ended before the send = %v, and it is marked ambiguous", err)
	}
}

// TestADeadLetterSendIsClassifiedAsAPublication holds a Reject's SendMessage
// to the dead-letter queue to the rules an ordinary publication is held to.
// The send was mapped by the SETTLEMENT rules, which read a throttle and a
// server error as definite, so an InternalError or a RequestThrottled (both
// answered after the service read the request, and both retried by the SDK)
// was a Reject that had not happened: the delivery stayed settleable and a
// retried Reject sent a second copy while the first was reported as never
// sent. A dead-letter send now answers what an ordinary publication that met
// the same failure answers, which is the oracle here: ambiguous for a server
// error, a timeout or a throttle, with the second settlement refused and the
// copy sent once, and definite for a refusal, with the delivery settleable
// again.
func TestADeadLetterSendIsClassifiedAsAPublication(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"InternalError", apiError("InternalError")},
		{"InternalFailure", apiError("InternalFailure")},
		{"ServiceUnavailable", apiError("ServiceUnavailable")},
		{"RequestTimeout", apiError("RequestTimeout")},
		{"RequestThrottled", &types.RequestThrottled{Message: aws.String("synthetic")}},
		{"ThrottlingException", apiError("ThrottlingException")},
		{"AccessDenied", apiError("AccessDenied")},
		{"KmsAccessDenied", &types.KmsAccessDenied{Message: aws.String("synthetic")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := sqsfake.New(sqsfake.Options{StaleHandleDeletes: true})
			h := openOn(t, server, func(c *sq.Config) {
				c.AckDeadline = time.Hour
				c.DeadLetterQueue = "dead"
				c.MaxReceiveCount = 5
			})
			name := topic(t, "dlq-class")
			h.publish(name, &mq.Message{Body: []byte("seed")})

			server.Fault(sqsfake.Fault{Op: "SendMessage", Queue: h.queue(name), Err: tc.err, Count: 1})
			perr := h.client.Publish(t.Context(), name, &mq.Message{Body: []byte("probe")})
			if perr == nil {
				t.Fatal("the injected publication failure was not reported")
			}
			ambiguous := mq.IsAmbiguous(perr)

			cons := h.consumer(name, 4)
			d := h.receiveOne(cons, 5*time.Second)
			server.Fault(sqsfake.Fault{Op: "SendMessage", Queue: "dead", Err: tc.err, Count: 1})
			rerr := d.Reject()
			if rerr == nil {
				t.Fatal("the injected dead-letter failure was not reported")
			}
			if got := mq.IsAmbiguous(rerr); got != ambiguous {
				t.Fatalf("Reject after a dead-letter send that failed with %s = %v (ambiguous %v), "+
					"but an ordinary publication that failed the same way = %v (ambiguous %v): "+
					"the dead-letter copy is a publication and may be stored exactly when that one may",
					tc.name, rerr, got, perr, ambiguous)
			}
			var e *mq.OpError
			if !errors.As(rerr, &e) || e.Operation != mq.OpNack {
				t.Fatalf("Reject = %v, want an *mq.OpError for %s: the failure is the negative settlement's", rerr, mq.OpNack)
			}
			if got := h.depth(name); got != 1 {
				t.Fatalf("the source queue holds %d message(s) after a failed dead-letter send; the original must stay", got)
			}

			sends := server.Calls("SendMessage")
			again := d.Reject()
			if ambiguous {
				if mq.CodeOf(again) != mq.CodeSettlementUnknown && !errors.Is(again, mq.ErrSettlementUnknown) {
					t.Fatalf("a second Reject after an ambiguous one = %v, want the sticky unknown", again)
				}
				if got := server.Calls("SendMessage") - sends; got != 0 {
					t.Fatalf("the retried Reject made %d more SendMessage call(s): a second dead-letter copy while the first may be stored", got)
				}
				return
			}
			if again != nil {
				t.Fatalf("a second Reject after a definite refusal = %v: the delivery is not settleable again", again)
			}
			if got := h.server.Bodies("dead"); len(got) != 1 || got[0] != "seed" {
				t.Fatalf("the dead-letter queue holds %v after the retried Reject", got)
			}
		})
	}
}

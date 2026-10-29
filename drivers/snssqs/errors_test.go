package snssqs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/smithy-go"

	mq "github.com/iqhive/iqmq"
	sns "github.com/iqhive/iqmq/drivers/snssqs"
	"github.com/iqhive/iqmq/drivers/snssqs/internal/snsfake"
)

// sdkTimeout is the shape the AWS SDK gives a request whose deadline passed
// after it was sent: an operation error whose chain ends in
// context.DeadlineExceeded.
func sdkTimeout(op string) error {
	return &smithy.OperationError{
		ServiceID: "SNS", OperationName: op,
		Err: fmt.Errorf("request send failed: %w", context.DeadlineExceeded),
	}
}

// TestASentPublishThatTimedOutIsAmbiguous is the timeout the SDK reports after
// the request left the socket. Its chain ends in context.DeadlineExceeded,
// which carries a portable code of its own, and the sink used to return it as
// a definite deadline: on a fan-out an accepted publication is already on its
// way to every subscribed queue, and a caller that retried duplicated it in
// each of them.
func TestASentPublishThatTimedOutIsAmbiguous(t *testing.T) {
	h := open(t)
	tp := topic(t, "sent-timeout")
	h.provision(tp, "a")

	h.sns.Fault(snsfake.Fault{Op: "Publish", Err: sdkTimeout("Publish"), Count: 1})
	err := h.client.Publish(t.Context(), tp, &mq.Message{Body: []byte("maybe")})
	if err == nil {
		t.Fatal("the publication succeeded although its round trip timed out")
	}
	if !mq.IsAmbiguous(err) || mq.IsRetryable(err) {
		t.Fatalf("a publication that timed out after it was sent = %v (ambiguous %v, retryable %v), want ambiguous and not retryable",
			err, mq.IsAmbiguous(err), mq.IsRetryable(err))
	}
}

// TestASentBatchThatTimedOutIsAmbiguous is the same rule for a chunk whose
// whole PublishBatch timed out after it was sent.
func TestASentBatchThatTimedOutIsAmbiguous(t *testing.T) {
	h := open(t)
	tp := topic(t, "sent-timeout-batch")
	h.provision(tp, "a")
	p, err := h.client.Publisher(t.Context(), mq.PublisherConfig{Topic: tp})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	defer func() { _ = p.Close() }()

	h.sns.Fault(snsfake.Fault{Op: "PublishBatch", Err: sdkTimeout("PublishBatch"), Count: 1})
	// Every entry shares the one outcome, so the batch answer may collapse to
	// it; either shape must say the outcome is unknown.
	err = p.PublishBatch(t.Context(), []*mq.Message{{Body: []byte("a")}, {Body: []byte("b")}})
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
	h := open(t)
	tp := topic(t, "done-before-send")
	h.provision(tp, "a")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := h.client.Publish(ctx, tp, &mq.Message{Body: []byte("never")})
	if err == nil {
		t.Fatal("a publication on a cancelled context succeeded")
	}
	if mq.IsAmbiguous(err) {
		t.Fatalf("a publication whose context ended before the send = %v, and it is marked ambiguous", err)
	}
}

// TestErrorMappingTable holds every row of the rule table for the SNS
// half to its code and its retryability.
//
// Driving each one through a real request would need a fault per row and would
// still not reach the rows whose operation nothing in the suite performs,
// which is why the mapper is fed the error directly (the same reason
// `drivers/sqs` exports MapForTest). What the table must never do is throw the
// native error away, which the last assertion in each row checks.
func TestErrorMappingTable(t *testing.T) {
	h := open(t)
	for _, tc := range []struct {
		name      string
		op        string
		err       error
		code      mq.ErrorCode
		retryable bool
	}{
		{"topic that names nothing", mq.OpPublish, &snstypes.NotFoundException{Message: aws.String("no topic")}, mq.CodeNotFound, false},
		{"resource not found", mq.OpAdmin, &snstypes.ResourceNotFoundException{}, mq.CodeNotFound, false},
		{"authorization", mq.OpPublish, &snstypes.AuthorizationErrorException{}, mq.CodeForbidden, false},
		{"invalid security", mq.OpPublish, &snstypes.InvalidSecurityException{}, mq.CodeUnauthorized, false},
		{"invalid parameter", mq.OpPublish, &snstypes.InvalidParameterException{}, mq.CodeInvalidArgument, false},
		{"invalid parameter value", mq.OpPublish, &snstypes.InvalidParameterValueException{}, mq.CodeInvalidArgument, false},
		{"duplicate batch ids", mq.OpPublish, &snstypes.BatchEntryIdsNotDistinctException{}, mq.CodeInvalidArgument, false},
		{"empty batch", mq.OpPublish, &snstypes.EmptyBatchRequestException{}, mq.CodeInvalidArgument, false},
		{"too many batch entries", mq.OpPublish, &snstypes.TooManyEntriesInBatchRequestException{}, mq.CodeInvalidArgument, false},
		{"batch too long", mq.OpPublish, &snstypes.BatchRequestTooLongException{}, mq.CodeMessageTooLarge, false},
		{"subscription limit", mq.OpAdmin, &snstypes.SubscriptionLimitExceededException{}, mq.CodeResourceExhausted, false},
		{"topic limit", mq.OpAdmin, &snstypes.TopicLimitExceededException{}, mq.CodeResourceExhausted, false},
		{"filter policy limit", mq.OpAdmin, &snstypes.FilterPolicyLimitExceededException{}, mq.CodeResourceExhausted, false},
		{"concurrent access", mq.OpAdmin, &snstypes.ConcurrentAccessException{}, mq.CodeConflict, false},
		{"invalid state", mq.OpAdmin, &snstypes.InvalidStateException{}, mq.CodeConflict, false},
		{"kms disabled", mq.OpPublish, &snstypes.KMSDisabledException{}, mq.CodeUnavailable, false},
		{"kms not found", mq.OpPublish, &snstypes.KMSNotFoundException{}, mq.CodeUnavailable, false},
		// A throttled KMS key is a throttle that a retry can get past, and
		// definite even on a publication, like the rest of the KMS family.
		{"kms throttling on a publish", mq.OpPublish, &snstypes.KMSThrottlingException{}, mq.CodeThrottled, true},
		{"kms throttling on an admin call", mq.OpAdmin, &snstypes.KMSThrottlingException{}, mq.CodeThrottled, true},
		{"endpoint disabled", mq.OpPublish, &snstypes.EndpointDisabledException{}, mq.CodeUnavailable, false},
		{"throttled on an admin call", mq.OpAdmin, &snstypes.ThrottledException{}, mq.CodeThrottled, true},
		{"internal error on an admin call", mq.OpAdmin, &snstypes.InternalErrorException{}, mq.CodeUnavailable, true},
		{"smithy throttling code", mq.OpAdmin, &smithy.GenericAPIError{Code: "ThrottlingException"}, mq.CodeThrottled, true},
		{"smithy service unavailable", mq.OpAdmin, &smithy.GenericAPIError{Code: "ServiceUnavailable"}, mq.CodeUnavailable, true},
		{"smithy access denied", mq.OpPublish, &smithy.GenericAPIError{Code: "AccessDenied"}, mq.CodeForbidden, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sns.MapForTest(h.back, tc.op, tc.err)
			if code := mq.CodeOf(got); code != tc.code {
				t.Errorf("code = %v, want %v (%v)", code, tc.code, got)
			}
			if r := mq.IsRetryable(got); r != tc.retryable {
				t.Errorf("retryable = %v, want %v (%v)", r, tc.retryable, got)
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("the mapping lost the native error: errors.Is does not reach %T", tc.err)
			}
		})
	}
}

// TestAThrottledPublicationIsAmbiguous is the one rule whose answer depends on
// the OPERATION rather than on the error.
//
// The service answers after it has read the request, so a publication that was
// throttled may or may not have been accepted — and on a fan-out an accepted
// one is already on its way to every subscribed queue. Reporting it as a
// definite failure would make N duplicates look like a loss; reporting it as
// ambiguous on a read would make a harmless retry look dangerous.
func TestAThrottledPublicationIsAmbiguous(t *testing.T) {
	h := open(t)
	publish := sns.MapForTest(h.back, mq.OpPublish, &snstypes.ThrottledException{})
	if !mq.IsAmbiguous(publish) {
		t.Errorf("a throttled publication is not ambiguous: %v", publish)
	}
	admin := sns.MapForTest(h.back, mq.OpAdmin, &snstypes.ThrottledException{})
	if mq.IsAmbiguous(admin) {
		t.Errorf("a throttled administrative call is ambiguous: %v", admin)
	}
	if !mq.IsRetryable(admin) {
		t.Errorf("a throttled administrative call is not retryable: %v", admin)
	}
}

// TestATransportFailureAfterTheWriteIsAmbiguous is the distinction that no
// native error type can make.
//
// The same net.Error is a clean refusal before the request left the socket and
// an ambiguous outcome after it. driverkit.ClassifyNet is what makes the call,
// and the flag is a parameter rather than a guess because every call site
// knows which it is.
func TestATransportFailureAfterTheWriteIsAmbiguous(t *testing.T) {
	h := open(t)
	for _, err := range []error{
		io.ErrUnexpectedEOF,
		&net.OpError{Op: "read", Err: errors.New("connection reset by peer")},
	} {
		before := sns.MapNetForTest(h.back, mq.OpPublish, err, false)
		after := sns.MapNetForTest(h.back, mq.OpPublish, err, true)
		if mq.IsAmbiguous(before) {
			t.Errorf("%v before the write is ambiguous: %v", err, before)
		}
		if !mq.IsAmbiguous(after) {
			t.Errorf("%v after the write is NOT ambiguous: %v", err, after)
		}
	}
}

// TestAFailedBatchEntryCarriesTheServicesOwnCodeAndFault holds the per-entry
// translation, which cannot go through the rule table at all: a batch response
// reports a code string and a sender-fault flag rather than a typed error.
//
// The shape that matters is the fault flag. SenderFault false means the
// SERVICE failed, which is retryable; true means the request was wrong, which
// is not. Reading them the same way would make a driver retry a malformed
// entry for ever, or give up on a transient failure.
func TestAFailedBatchEntryCarriesTheServicesOwnCodeAndFault(t *testing.T) {
	for _, tc := range []struct {
		code        string
		senderFault bool
		want        mq.ErrorCode
		retryable   bool
	}{
		{"InternalError", false, mq.CodeUnavailable, true},
		{"ServiceUnavailable", false, mq.CodeUnavailable, true},
		{"Throttled", false, mq.CodeThrottled, true},
		{"KMSThrottling", true, mq.CodeThrottled, true},
		{"InvalidParameter", true, mq.CodeInvalidArgument, false},
		{"BatchRequestTooLong", true, mq.CodeMessageTooLarge, false},
		{"NotFound", true, mq.CodeNotFound, false},
		{"AuthorizationError", true, mq.CodeForbidden, false},
		// A code nobody anticipated falls back to the fault flag, which is
		// the only thing the service said that a driver can act on.
		{"SomethingNew", true, mq.CodeInvalidArgument, false},
		{"SomethingNew", false, mq.CodeUnavailable, true},
	} {
		got := sns.BatchEntryErrorForTest(mq.OpPublish, "t", tc.code, "why", tc.senderFault)
		if code := mq.CodeOf(got); code != tc.want {
			t.Errorf("entry code %q (senderFault %v) mapped to %v, want %v", tc.code, tc.senderFault, code, tc.want)
		}
		if r := mq.IsRetryable(got); r != tc.retryable {
			t.Errorf("entry code %q (senderFault %v) is retryable=%v, want %v", tc.code, tc.senderFault, r, tc.retryable)
		}
		var api smithy.APIError
		if !errors.As(got, &api) {
			t.Errorf("entry code %q does not reach the service's own code through smithy.APIError", tc.code)
			continue
		}
		if api.ErrorCode() != tc.code {
			t.Errorf("the entry error reports the code %q, want the service's %q", api.ErrorCode(), tc.code)
		}
	}
}

// TestCollapseAlignsWithTheInput is the batch answer, which is the whole
// contract a partial result rests on.
func TestCollapseAlignsWithTheInput(t *testing.T) {
	boom := errors.New("boom")
	other := errors.New("other")
	if got := sns.CollapseForTest([]error{nil, nil}); got != nil {
		t.Errorf("a batch every entry of which succeeded reported %v", got)
	}
	if got := sns.CollapseForTest([]error{boom, boom}); !errors.Is(got, boom) {
		t.Errorf("a batch every entry of which failed reported %v, want the single error", got)
	}
	got := sns.CollapseForTest([]error{nil, boom, nil, other})
	var batch *mq.BatchError
	if !errors.As(got, &batch) {
		t.Fatalf("a partial batch reported %v, want a *mq.BatchError", got)
	}
	if len(batch.Errs) != 4 {
		t.Fatalf("the batch error carries %d results for 4 entries", len(batch.Errs))
	}
	if batch.Errs[0] != nil || batch.Errs[2] != nil {
		t.Errorf("a succeeding entry carries an error")
	}
	if !errors.Is(batch.Errs[1], boom) || !errors.Is(batch.Errs[3], other) {
		t.Errorf("the failures are not at their own indexes: %v", batch.Errs)
	}
}

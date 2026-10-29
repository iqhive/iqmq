package sqs

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	mq "github.com/iqhive/iqmq"
)

// TestAFailedBatchEntryIsReadFromItsFaultFlag holds the one translation in this
// driver that cannot go through the rule table at all.
//
// A batch response reports a per-entry CODE STRING and a sender-fault flag
// rather than a typed error, so there is no native type for a rule to match.
// The shape that matters is the flag: SenderFault false means the SERVICE
// failed, which is retryable, and SenderFault true means the request was wrong,
// which is not. Reading them the same way makes a driver retry a malformed entry
// for ever, or give up on a transient failure — and a batch is exactly where
// that is invisible, because the other entries succeeded.
//
// The `sqs` half of the AWS pair had no test for this; `drivers/snssqs`'s
// TestAFailedBatchEntryCarriesTheServicesOwnCodeAndFault is the same check on
// the other half, and the two codes tables are NOT the same — SQS prefixes its
// own with "AWS.SimpleQueueService.", which is the part a copied table gets
// wrong.
func TestAFailedBatchEntryIsReadFromItsFaultFlag(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		code        string
		senderFault bool
		want        mq.ErrorCode
		retryable   bool
	}{
		// The service's own failures are retryable whatever it says about
		// fault, because a retry can succeed.
		{"InternalError", false, mq.CodeUnavailable, true},
		{"InternalFailure", false, mq.CodeUnavailable, true},
		{"ServiceUnavailable", false, mq.CodeUnavailable, true},
		{"RequestThrottled", false, mq.CodeThrottled, true},
		{"ThrottlingException", false, mq.CodeThrottled, true},
		// A throttled KMS key is a throttle, although the service files it
		// as the sender's fault.
		{"KMS.ThrottlingException", true, mq.CodeThrottled, true},
		{"KmsThrottled", true, mq.CodeThrottled, true},
		// The request's own failures are not, and each names what to fix.
		{"AWS.SimpleQueueService.NonExistentQueue", true, mq.CodeNotFound, false},
		{"AWS.SimpleQueueService.BatchRequestTooLong", true, mq.CodeMessageTooLarge, false},
		{"InvalidMessageContents", true, mq.CodeMessageTooLarge, false},
		{"AWS.SimpleQueueService.UnsupportedOperation", true, mq.CodeUnsupported, false},
		// A code nobody anticipated falls back to the fault flag, which is the
		// only thing the service said that a driver can act on.
		{"SomethingNew", true, mq.CodeInvalidArgument, false},
		{"SomethingNew", false, mq.CodeUnavailable, true},
	} {
		got := batchEntryError(mq.OpPublish, "orders", tc.code, "why", tc.senderFault)
		if code := mq.CodeOf(got); code != tc.want {
			t.Errorf("entry code %q (senderFault %v) mapped to %v, want %v", tc.code, tc.senderFault, code, tc.want)
		}
		if r := mq.IsRetryable(got); r != tc.retryable {
			t.Errorf("entry code %q (senderFault %v) is retryable=%v, want %v: reading the fault flag the other "+
				"way retries a malformed entry for ever, or gives up on a transient failure",
				tc.code, tc.senderFault, r, tc.retryable)
		}
		var op *mq.OpError
		if !errors.As(got, &op) {
			t.Fatalf("entry code %q did not produce an *mq.OpError", tc.code)
		}
		if op.Topic != "orders" || op.Operation != mq.OpPublish || op.Driver != DriverName {
			t.Errorf("the entry error says driver=%q op=%q topic=%q, so an operator cannot tell which batch it "+
				"came from", op.Driver, op.Operation, op.Topic)
		}
	}
}

// TestALostLeaseInABatchIsNotARetry is the entry code that is not a failure of
// the request at all.
//
// ReceiptHandleIsInvalid and MessageNotInflight are the service saying "the
// lease you are settling is not yours any more": the message has been
// redelivered to somebody else, or the handle is from a receive this consumer has
// superseded. A retry can NEVER succeed — the handle will not become valid
// again — and reporting it as a plain failure leaves the core believing the
// delivery is still settleable, so it keeps trying to settle a delivery another
// consumer now owns.
func TestALostLeaseInABatchIsNotARetry(t *testing.T) {
	t.Parallel()
	for _, code := range []string{
		"ReceiptHandleIsInvalid",
		"AWS.SimpleQueueService.MessageNotInflight",
	} {
		got := batchEntryError(mq.OpAck, "orders", code, "the handle is not valid", false)
		if !errors.Is(got, mq.ErrLeaseLost) {
			t.Errorf("the entry code %q reported %v, want mq.ErrLeaseLost: a retry can never succeed and the core "+
				"would keep settling a delivery another consumer owns ", code, got)
		}
		if mq.IsRetryable(got) {
			t.Errorf("the entry code %q is retryable; the receipt handle will never become valid again", code)
		}
	}
	// SenderFault false is what the service reports for a lost lease, and it is
	// the one case where the flag must NOT decide: read as "the service
	// failed", it would come back CodeUnavailable and retryable.
	fault := batchEntryError(mq.OpAck, "orders", "ReceiptHandleIsInvalid", "", true)
	if !errors.Is(fault, mq.ErrLeaseLost) {
		t.Errorf("a lost lease with the fault flag set reported %v; the code decides this one and the flag does "+
			"not", fault)
	}
}

// TestAFailedEntryStillCarriesTheServicesOwnCodeAndMessage is what makes the
// entry error diagnosable.
//
// The portable code says what class of failure it was; only the service's own
// code and message say WHICH. They travel through smithy.APIError, which is the
// interface the rest of the SDK uses, so a caller matching on an AWS code
// reaches this entry the same way it reaches a top-level refusal.
func TestAFailedEntryStillCarriesTheServicesOwnCodeAndMessage(t *testing.T) {
	t.Parallel()
	got := batchEntryError(mq.OpPublish, "orders", "AWS.SimpleQueueService.BatchRequestTooLong",
		"Batch requests cannot be longer than 262144 bytes", true)
	var api smithy.APIError
	if !errors.As(got, &api) {
		t.Fatalf("a failed entry does not reach the service's own code through smithy.APIError: %v", got)
	}
	if api.ErrorCode() != "AWS.SimpleQueueService.BatchRequestTooLong" {
		t.Errorf("the entry reports the code %q, want the service's own", api.ErrorCode())
	}
	if api.ErrorMessage() != "Batch requests cannot be longer than 262144 bytes" {
		t.Errorf("the entry reports the message %q, want the service's own: the portable code says the class and "+
			"only this says which limit", api.ErrorMessage())
	}
	if api.ErrorFault() != smithy.FaultUnknown {
		t.Errorf("the entry reports fault %v; the batch response's own flag has already been read into the "+
			"retryability, and repeating it here as a smithy fault would be a second, unsynchronised answer",
			api.ErrorFault())
	}
	// The rendering is what reaches a log. It names the service's code, and it
	// carries no payload -- the specification forbids that, and a batch entry is exactly
	// where a driver is tempted to include the message body.
	if !strings.Contains(got.Error(), "AWS.SimpleQueueService.BatchRequestTooLong") {
		t.Errorf("the rendering %q does not name the service's code", got.Error())
	}
	if !strings.Contains(got.Error(), "262144") {
		t.Errorf("the rendering %q drops the service's message", got.Error())
	}
	// An entry the service refused with no message renders without a dangling
	// separator, because the code alone is the whole answer.
	bare := batchEntryError(mq.OpPublish, "orders", "InvalidMessageContents", "", true)
	if !strings.Contains(bare.Error(), "InvalidMessageContents") {
		t.Errorf("an entry with no message renders as %q and loses the code", bare.Error())
	}
	if strings.Contains(bare.Error(), "InvalidMessageContents:") {
		t.Errorf("an entry with no message renders as %q, with a separator and nothing after it", bare.Error())
	}
}

// TestCollapseAlignsWithTheInput is the batch answer, which is the whole
// contract a partial result rests on: a caller reads its own entry's outcome by
// INDEX, so a collapse that compacted the successes would hand every later
// entry another entry's answer.
func TestCollapseAlignsWithTheInput(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	other := errors.New("other")
	if got := collapse([]error{nil, nil}); got != nil {
		t.Errorf("a batch every entry of which succeeded reported %v", got)
	}
	if got := collapse([]error{boom, boom}); !errors.Is(got, boom) {
		t.Errorf("a batch every entry of which failed reported %v, want the single error", got)
	}
	got := collapse([]error{nil, boom, nil, other})
	var batch *mq.BatchError
	if !errors.As(got, &batch) {
		t.Fatalf("a partial batch reported %v, want a *mq.BatchError", got)
	}
	if len(batch.Errs) != 4 {
		t.Fatalf("the batch error carries %d results for 4 entries, so a caller cannot index its own",
			len(batch.Errs))
	}
	if batch.Errs[0] != nil || batch.Errs[2] != nil {
		t.Error("a succeeding entry carries an error")
	}
	if !errors.Is(batch.Errs[1], boom) || !errors.Is(batch.Errs[3], other) {
		t.Errorf("the failures are not at their own indexes: %v", batch.Errs)
	}
}

package kafkatx

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	mq "github.com/iqhive/iqmq"
)

// TestAnEndFranzGoNeverSentIsDefinite: franz-go refuses two commits before it
// sends anything, and an ending it refused that way certainly did not
// happen. Every other failure of an ending may have reached the broker, so
// its outcome stays unknown.
func TestAnEndFranzGoNeverSentIsDefinite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		ambiguous bool
	}{
		{name: "a producer id that failed", err: kerr.OperationNotAttempted},
		{
			// franz-go's own wording, with both codes wrapped.
			name: "a topic recreated inside the transaction",
			err: fmt.Errorf("the transaction produced to %v before the topic was deleted and recreated; the records the old topic acknowledged are gone, abort the transaction: %w, %w",
				[]string{"orders"}, kerr.TransactionAbortable, kerr.UnknownTopicID),
		},
		{name: "the broker's own TRANSACTION_ABORTABLE", err: kerr.TransactionAbortable, ambiguous: true},
		{name: "an end nobody confirmed", err: kerr.UnknownServerError, ambiguous: true},
		{name: "a request cancelled in flight", err: context.Canceled, ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mq.IsAmbiguous(wrap(tc.err, endSent(tc.err))); got != tc.ambiguous {
				t.Fatalf("an ending that failed with %v is ambiguous %v, want %v", tc.err, got, tc.ambiguous)
			}
		})
	}
}

// TestARecordFailureTheDriverCannotNameIsRetryable: a commit refused for a
// record failure takes the code the driver gives that failure, and one the
// driver has no code for is unavailable and retryable, as every other
// failure franz-go reports to this package is. It kept CodeUnknown, which
// no caller retries.
func TestARecordFailureTheDriverCannotNameIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		code      mq.ErrorCode
		retryable bool
	}{
		{name: "a record the broker refused", err: kerr.InvalidRecord, code: mq.CodeCorruptMessage},
		{
			// franz-go's own wording for a record with no topic.
			name: "a failure the driver has no code for",
			err:  errors.New("cannot produce record with no topic and no default topic"),
			code: mq.CodeUnavailable, retryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := refused(tc.err)
			if mq.CodeOf(err) != tc.code || mq.IsRetryable(err) != tc.retryable || mq.IsAmbiguous(err) {
				t.Fatalf("refused(%v) = %v (code %v, retryable %v, ambiguous %v), want code %v, retryable %v, definite",
					tc.err, err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err), tc.code, tc.retryable)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("refused(%v) = %v, which does not wrap the record's failure", tc.err, err)
			}
		})
	}
}

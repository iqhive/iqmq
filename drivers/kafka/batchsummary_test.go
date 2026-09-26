package kafka_test

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestBatchSummaryKeepsAMixedOutcome is the rule for a batch none of whose
// records was accepted. Kafka answers per partition, so one partition can
// refuse definitely (MESSAGE_TOO_LARGE) while another times out after the
// append may have happened (REQUEST_TIMED_OUT, ambiguous). BatchSummary
// returned the first record's error whatever the others said, so such a batch
// read as a definite refusal and a caller who republished it duplicated the
// records that may be in the log. It now answers through
// driverkit.CollapseBatch: the single error only when every record has the
// same outcome.
func TestBatchSummaryKeepsAMixedOutcome(t *testing.T) {
	mixed := kq.BatchOutcome([]error{kerr.MessageTooLarge, kerr.RequestTimedOut, kerr.RequestTimedOut})
	if mq.IsAmbiguous(mixed[0]) || !mq.IsAmbiguous(mixed[1]) {
		t.Fatalf("the instrument is broken: %v should be definite and %v ambiguous", mixed[0], mixed[1])
	}
	got := kq.BatchSummary(mixed)
	var be *mq.BatchError
	if !errors.As(got, &be) || len(be.Errs) != 3 {
		t.Fatalf("BatchSummary(refused, ambiguous, ambiguous) = %v, want the index-aligned *BatchError", got)
	}
	if !mq.IsAmbiguous(got) || mq.IsAmbiguous(be.Errs[0]) || !mq.IsAmbiguous(be.Errs[2]) {
		t.Fatalf("BatchSummary(refused, ambiguous, ambiguous) = %v: the per-record outcomes were lost", got)
	}
	for name, errs := range map[string][]error{
		"every record refused":   kq.BatchOutcome([]error{kerr.MessageTooLarge, kerr.InvalidRecord}),
		"every record ambiguous": kq.BatchOutcome([]error{kerr.RequestTimedOut, kerr.RequestTimedOut}),
	} {
		got := kq.BatchSummary(errs)
		if errors.As(got, &be) || got != errs[0] { //nolint:errorlint // identity: the first record's own error
			t.Errorf("%s: BatchSummary = %v, want the first record's own error: every record has one outcome", name, got)
		}
	}
}

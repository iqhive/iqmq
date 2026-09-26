package kafkago_test

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	kq "github.com/iqhive/iqmq/drivers/kafka/kafkago"
)

// TestBatchSummaryKeepsAMixedOutcome is the rule for a batch none of whose
// messages was accepted: NOT_ENOUGH_REPLICAS (19) refused before the append,
// NOT_ENOUGH_REPLICAS_AFTER_APPEND (20) after it, so the second message is in
// the leader's log. BatchSummary returned the first message's error whatever
// the others said; it now answers through driverkit.CollapseBatch.
func TestBatchSummaryKeepsAMixedOutcome(t *testing.T) {
	mixed := kq.BatchOutcome([]error{kafkaGoError(19), kafkaGoError(20)})
	if mq.IsAmbiguous(mixed[0]) || !mq.IsAmbiguous(mixed[1]) {
		t.Fatalf("the instrument is broken: %v should be definite and %v ambiguous", mixed[0], mixed[1])
	}
	got := kq.BatchSummary(mixed)
	var be *mq.BatchError
	if !errors.As(got, &be) || len(be.Errs) != 2 || !mq.IsAmbiguous(got) || mq.IsAmbiguous(be.Errs[0]) {
		t.Fatalf("BatchSummary(19, 20) = %v, want an ambiguous, index-aligned *BatchError", got)
	}
	same := kq.BatchOutcome([]error{kafkaGoError(19), kafkaGoError(19)})
	if got := kq.BatchSummary(same); errors.As(got, &be) || got != same[0] { //nolint:errorlint // identity: the first message's own error
		t.Fatalf("BatchSummary(19, 19) = %v, want the first message's own error", got)
	}
}

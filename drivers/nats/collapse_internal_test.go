package nats

import (
	"errors"
	"io"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestBatchCollapseKeepsAMixedOutcome is the rule for a batch none of
// whose messages was accepted: one refused before the request left, one lost
// after it -- the same code, CodeUnavailable, and two different outcomes.
// PublishBatch returned the first message's error whatever the others said;
// it now answers through collapse, which is driverkit.CollapseBatch.
func TestBatchCollapseKeepsAMixedOutcome(t *testing.T) {
	m := newErrorMapper()
	refused := m.MapNet(mq.OpPublish, "t", "", io.EOF, false)
	ambiguous := m.MapNet(mq.OpPublish, "t", "", io.EOF, true)
	if mq.IsAmbiguous(refused) || !mq.IsAmbiguous(ambiguous) {
		t.Fatalf("the instrument is broken: %v should be definite and %v ambiguous", refused, ambiguous)
	}
	got := collapse([]error{refused, ambiguous})
	var be *mq.BatchError
	if !errors.As(got, &be) || len(be.Errs) != 2 || !mq.IsAmbiguous(got) || mq.IsAmbiguous(be.Errs[0]) {
		t.Fatalf("collapse(refused, ambiguous) = %v, want an ambiguous, index-aligned *BatchError", got)
	}
	if got := collapse([]error{refused, refused}); got != refused { //nolint:errorlint // identity: the first message's own error
		t.Fatalf("collapse(refused, refused) = %v, want the first message's own error", got)
	}
	if got := collapse([]error{nil, nil}); got != nil {
		t.Fatalf("collapse(nil, nil) = %v, want nil", got)
	}
}

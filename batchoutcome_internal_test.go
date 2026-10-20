package iqmq

import (
	"errors"
	"testing"
)

// TestBatchOutcomeNeverHandsOnABlamelessBatchError pins batchOutcome's
// failed == 0 arm, which alignPiece keeps every path from reaching and which
// therefore no end-to-end test can (the review's rigour-5): were it ever
// reached, the answer must not be the driver's own *BatchError, whose entries
// all read "accepted".
func TestBatchOutcomeNeverHandsOnABlamelessBatchError(t *testing.T) {
	counters, err := newPublisherCounters()
	if err != nil {
		t.Fatal(err)
	}
	p := &PublisherHandle{abandoned: &OpError{Driver: "d", Topic: "t"}, counters: counters}
	blameless := &BatchError{Errs: []error{nil, nil}}
	err = p.batchOutcome([]*Message{{}, {}}, []error{nil, nil}, blameless)
	var be *BatchError
	if err == nil || errors.As(err, &be) {
		t.Fatalf("batchOutcome(no entry failed, driver said %v) = %v: want a failure errors.As cannot read as accepted", blameless, err)
	}
	if !IsAmbiguous(err) {
		t.Fatalf("batchOutcome(...) = %v: IsAmbiguous = false; nobody can say which messages the driver took", err)
	}
	if st := p.counters.snapshot(); st.Published != 0 || st.Failed != 2 {
		t.Fatalf("counters = %+v, want 0 published and 2 failed", st)
	}
}

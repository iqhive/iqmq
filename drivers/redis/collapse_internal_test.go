package redis

import (
	"errors"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestBatchCollapseKeepsAMixedOutcome is the rule for the two answers the
// sink gives a batch with a failure in it :
//
//   - every message failed, some refused by a reply and some stored but short
//     of the durable boundary -- the WAITAOF that came back short, which is
//     errcode.Ambiguous mapped through MapNet; and
//   - some messages failed and the rest are stored, and WAITAOF for them came
//     back short, so the fill gives them the short answer and the batch can be
//     wholly failed with mixed outcomes.
//
// PublishBatch returned the first error in the first case and a *BatchError
// built by hand in the second; both now go through batchAnswer, which is
// driverkit.CollapseBatch after the fill.
func TestBatchCollapseKeepsAMixedOutcome(t *testing.T) {
	m := newErrorMapper()
	refused := m.MapNet(mq.OpPublish, "t", "", errors.New("READONLY You can't write against a read only replica"), true)
	refused2 := m.MapNet(mq.OpPublish, "t", "", errors.New("READONLY You can't write against a read only replica"), true)
	short := m.MapNet(mq.OpPublish, "t", "", errDurableShort, true)
	if mq.IsAmbiguous(refused) || !mq.IsAmbiguous(short) {
		t.Fatalf("the instrument is broken: %v should be definite and %v ambiguous", refused, short)
	}
	var be *mq.BatchError
	// Every message failed, with mixed outcomes.
	if got := batchAnswer([]error{refused, short}, nil); !errors.As(got, &be) || len(be.Errs) != 2 || !mq.IsAmbiguous(got) {
		t.Fatalf("batchAnswer(refused, short) = %v, want an ambiguous, index-aligned *BatchError", got)
	}
	// Every message failed with one outcome: the first message's own error.
	if got := batchAnswer([]error{refused, refused2}, nil); got != refused { //nolint:errorlint // identity: the first message's own error
		t.Fatalf("batchAnswer(refused, refused) = %v, want the first message's own error", got)
	}
	// Some were refused and the rest came back short after the fill.
	got := batchAnswer([]error{refused, nil}, short)
	if !errors.As(got, &be) || len(be.Errs) != 2 || be.Errs[0] != refused || be.Errs[1] != short { //nolint:errorlint // identity
		t.Fatalf("batchAnswer(refused, stored; short) = %v, want [refused, short]", got)
	}
	if !mq.IsAmbiguous(got) {
		t.Fatalf("batchAnswer(refused, stored; short) = %v: IsAmbiguous = false, and one message is in the stream", got)
	}
	// Some were refused, and the boundary's own failure for the rest is
	// DEFINITE: every message failed with one outcome, so the single error
	// is the answer -- not a *BatchError built by hand after the fill.
	unsupported := m.Map(mq.OpPublish, "t", "", errors.New("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled"))
	if mq.IsAmbiguous(unsupported) {
		t.Fatalf("the instrument is broken: %v should be definite", unsupported)
	}
	if got := batchAnswer([]error{refused, nil}, unsupported); got != refused { //nolint:errorlint // identity: the first message's own error
		t.Fatalf("batchAnswer(refused, stored; definite) = %v, want the first message's own error", got)
	}
	// Some were refused and the durable boundary was reached for the rest.
	got = batchAnswer([]error{refused, nil}, nil)
	if !errors.As(got, &be) || be.Errs[1] != nil {
		t.Fatalf("batchAnswer(refused, stored) = %v, want a partial *BatchError with the stored message accepted", got)
	}
}

package redis

import (
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/streams"
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
	// Some were refused, and the WAITAOF failed outright for the rest, as
	// durableReply answers it. Their entries are in the stream, so the
	// failure is ambiguous whatever its reply says, and the answer is per
	// message: the refusal may be retried, and the stored message may not.
	// A LOADING reply used to be mapped as definite, with the refusal's code
	// and retryability, so the batch collapsed to the refusal and a caller
	// who retried it duplicated the stored message.
	s := &sink{b: &Backend{errs: m}, topic: "t"}
	for _, reply := range []error{
		errors.New("LOADING Redis is loading the dataset in memory"),
		errors.New("ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled"),
	} {
		failed := s.durableReply(streams.Result{Err: reply})
		got := batchAnswer([]error{refused, nil}, failed)
		if !errors.As(got, &be) || len(be.Errs) != 2 || be.Errs[0] != refused || !mq.IsAmbiguous(be.Errs[1]) { //nolint:errorlint // identity
			t.Fatalf("batchAnswer(refused, stored; WAITAOF answered %q) = %v, want [refused, an ambiguous outcome]", reply, got)
		}
	}
	// Some were refused and the durable boundary was reached for the rest.
	got = batchAnswer([]error{refused, nil}, nil)
	if !errors.As(got, &be) || be.Errs[1] != nil {
		t.Fatalf("batchAnswer(refused, stored) = %v, want a partial *BatchError with the stored message accepted", got)
	}
}

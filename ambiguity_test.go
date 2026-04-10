package iqmq_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The ambiguity of a composite error ("Misclassifying ambiguous as
// rejected/accepted is a disqualifying correctness defect").
//
// IsAmbiguous used to stop at the FIRST *OpError it met, including across the
// branches of a multi-error. For one *OpError in a chain that is the right
// reading; for a batch it is not, because a batch's entries are independent
// outcomes. A *BatchError whose first failed item was definitely refused and
// whose last was ambiguous answered "not ambiguous", and a caller who
// republished the refused items republished the ambiguous one too.

// refusedItem is a definite refusal: the broker answered and said no.
var refusedItem = &mq.OpError{
	Driver: "test", Operation: mq.OpPublish, Topic: "orders",
	Code: mq.CodeInvalidArgument,
}

// ambiguousItem is what driverkit.ErrorMapper.MapNet reports for an EOF after
// the request left the socket: retryable, and nobody knows whether the
// broker has it.
var ambiguousItem = &mq.OpError{
	Driver: "test", Operation: mq.OpPublish, Topic: "orders",
	Code: mq.CodeUnavailable, Retryable: true, Ambiguous: true, Err: io.EOF,
}

// codeOnlyItem carries its ambiguity in its code and not in its flag, which is
// what driverkit.ErrorMapper.MapNet made of drivers/redis's short WAITAOF
// (errcode.Ambiguous) before ClassifyNet read the code.
var codeOnlyItem = &mq.OpError{Driver: "test", Operation: mq.OpPublish, Topic: "orders", Code: mq.CodeAmbiguous}

// TestIsAmbiguousReadsEveryBranch pins the rule: along one chain the nearest
// *OpError decides, and an error with several branches is ambiguous when ANY
// branch is.
func TestIsAmbiguousReadsEveryBranch(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"a definite refusal", refusedItem, false},
		{"an ambiguous failure", ambiguousItem, true},
		{"ambiguous, wrapped", fmt.Errorf("publishing: %w", ambiguousItem), true},
		{"joined, ambiguous first", errors.Join(ambiguousItem, refusedItem), true},
		{"joined, ambiguous second", errors.Join(refusedItem, ambiguousItem), true},
		{"joined, ambiguous under a wrap", errors.Join(refusedItem, fmt.Errorf("x: %w", ambiguousItem)), true},
		{"joined, nothing ambiguous", errors.Join(refusedItem, errors.New("plain")), false},
		{"a batch, ambiguous item last", &mq.BatchError{Errs: []error{refusedItem, nil, nil, ambiguousItem}}, true},
		{"a batch, ambiguous item first", &mq.BatchError{Errs: []error{ambiguousItem, refusedItem}}, true},
		{"a batch, nothing ambiguous", &mq.BatchError{Errs: []error{refusedItem, nil, refusedItem}}, false},
		{"a wrapped batch, ambiguous item last", fmt.Errorf("orders: %w", &mq.BatchError{Errs: []error{refusedItem, ambiguousItem}}), true},
		// Along one chain the nearest *OpError still decides: an operation
		// that re-classified its cause as definite has said so.
		{"a definite *OpError over an ambiguous cause", &mq.OpError{Code: mq.CodeInvalidArgument, Err: ambiguousItem}, false},
		{"a definite *OpError over a code-only cause", &mq.OpError{Code: mq.CodeInvalidArgument, Err: codeOnlyItem}, false},

		// Ambiguity carried in the CODE alone. CodeAmbiguous is "a publish
		// whose acceptance is unknown" and CodeSettlementUnknown "an
		// ambiguous native settlement result": an error carrying either IS
		// the ambiguous outcome, whatever its flag says. drivers/redis maps
		// its short WAITAOF to exactly this shape, and x/outbox, the
		// runner's settlement path and drivers/sqs already read the code.
		{"a flagless CodeAmbiguous *OpError", codeOnlyItem, true},
		{"a flagless CodeSettlementUnknown *OpError", &mq.OpError{Code: mq.CodeSettlementUnknown}, true},
		{"the bare ErrSettlementUnknown sentinel", mq.ErrSettlementUnknown, true},
		{"an unmapped CodeAmbiguous error", errcode.New(errcode.Ambiguous, "short WAITAOF"), true},
		{"code-only, wrapped", fmt.Errorf("publishing: %w", codeOnlyItem), true},
		{"joined, code-only second", errors.Join(refusedItem, codeOnlyItem), true},
		{"a batch, code-only item last", &mq.BatchError{Errs: []error{refusedItem, nil, codeOnlyItem}}, true},
		{"a CodeAmbiguous *OpError whose flag is set", &mq.OpError{Code: mq.CodeAmbiguous, Ambiguous: true}, true},
	}
	for _, c := range cases {
		if got := mq.IsAmbiguous(c.err); got != c.want {
			t.Errorf("%s: IsAmbiguous(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}

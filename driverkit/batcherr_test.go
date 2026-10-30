package driverkit_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestCollapseBatchFollowsSection6 pins the three arms two drivers had each
// written for themselves.
//
// The middle arm is the one with a rule behind it: a batch every entry of
// which failed WITH ONE OUTCOME collapses to the first error rather than to a
// *BatchError nobody can act on differently (narrowed -- TestCollapseBatchNeverReportsAnAmbiguousEntryAsRefused is the other half). The partial case must stay index-aligned, because that
// is the only thing that tells a caller WHICH message failed.
func TestCollapseBatchFollowsSection6(t *testing.T) {
	a, b := errors.New("a"), errors.New("b")

	if err := driverkit.CollapseBatch([]error{nil, nil, nil}); err != nil {
		t.Errorf("a batch that wholly succeeded collapsed to %v, want nil", err)
	}
	if err := driverkit.CollapseBatch(nil); err != nil {
		t.Errorf("an empty batch collapsed to %v, want nil", err)
	}

	// Every entry failed: the first error, unwrapped.
	err := driverkit.CollapseBatch([]error{a, b})
	if !errors.Is(err, a) {
		t.Errorf("a wholly failed batch collapsed to %v, want the first error", err)
	}
	var be *mq.BatchError
	if errors.As(err, &be) {
		t.Errorf("a wholly failed batch whose entries share one outcome collapsed to a *BatchError; a caller "+
			"then needs two code paths for one outcome: %v", err)
	}

	// Some failed: a *BatchError whose indices still line up with the items
	// the caller submitted.
	err = driverkit.CollapseBatch([]error{nil, a, nil, b})
	if !errors.As(err, &be) {
		t.Fatalf("a partly failed batch collapsed to %v, want a *BatchError", err)
	}
	if len(be.Errs) != 4 {
		t.Fatalf("the *BatchError carries %d entries for a batch of 4: a compacted slice attributes every "+
			"failure to the wrong message", len(be.Errs))
	}
	for i, want := range []error{nil, a, nil, b} {
		if be.Errs[i] != want { //nolint:errorlint // identity is the question: the entry must be the caller's own error at its own index
			t.Errorf("entry %d is %v, want %v", i, be.Errs[i], want)
		}
	}
}

// TestCollapseBatchNeverReportsAnAmbiguousEntryAsRefused is the rule for
// the middle arm. "A single error when none were accepted" is only honest
// when that one error is every entry's outcome, and a batch in which one
// entry was refused and another may have reached the broker has two
// outcomes. Collapsing it to the first entry answered IsAmbiguous = false for
// a batch holding an ambiguous publication, and a caller who republished the
// "refused" batch published that entry twice; collapsing the other way
// round reported a definite refusal as "maybe accepted", which a caller that
// does not republish ambiguous work turns into a lost message.
//
// The ambiguous entry is built by MapNet, the mapper drivers/sqs and
// drivers/snssqs -- this helper's two callers -- use on their publish path.
func TestCollapseBatchNeverReportsAnAmbiguousEntryAsRefused(t *testing.T) {
	m := &driverkit.ErrorMapper{Driver: "test"}
	ambiguous := m.MapNet(mq.OpPublish, "orders", "", io.EOF, true)
	ambiguous2 := m.MapNet(mq.OpPublish, "orders", "", net.ErrClosed, true)
	refused := &mq.OpError{Driver: "test", Operation: mq.OpPublish, Code: mq.CodeInvalidArgument}
	refused2 := &mq.OpError{
		Driver: "test", Operation: mq.OpPublish, Code: mq.CodeInvalidArgument,
		Err: errors.New("the service refused this entry"),
	}
	if !mq.IsAmbiguous(ambiguous) || !mq.IsAmbiguous(ambiguous2) || mq.IsAmbiguous(refused) || mq.IsAmbiguous(refused2) {
		t.Fatal("the instrument is broken: the entries do not carry the ambiguity the test is about")
	}

	// Mixed outcomes: an index-aligned *BatchError that says, entry by entry,
	// which is which -- and is ambiguous as a whole.
	for _, errs := range [][]error{
		{refused, refused, refused, ambiguous},
		{ambiguous, refused},
		{refused2, ambiguous, refused, ambiguous2},
	} {
		err := driverkit.CollapseBatch(errs)
		var be *mq.BatchError
		if !errors.As(err, &be) {
			t.Errorf("CollapseBatch(%v) = %v, want a *BatchError: its entries have different outcomes and one error "+
				"cannot say both", errs, err)
			continue
		}
		if len(be.Errs) != len(errs) {
			t.Fatalf("CollapseBatch(%v) carries %d entries, want %d", errs, len(be.Errs), len(errs))
		}
		for i := range errs {
			if be.Errs[i] != errs[i] { //nolint:errorlint // identity: the entry must be the caller's own error at its own index
				t.Errorf("CollapseBatch(%v): entry %d is %v, want %v", errs, i, be.Errs[i], errs[i])
			}
		}
		if !mq.IsAmbiguous(err) {
			t.Errorf("CollapseBatch(%v) = %v: IsAmbiguous = false for a batch holding an ambiguous entry", errs, err)
		}
	}

	// One outcome, whichever it is: the single error, unwrapped.
	for _, errs := range [][]error{
		{ambiguous, ambiguous2},
		{refused, refused2},
	} {
		err := driverkit.CollapseBatch(errs)
		var be *mq.BatchError
		if errors.As(err, &be) {
			t.Errorf("CollapseBatch(%v) = %v, want the single error: every entry has the same outcome", errs, err)
		}
		if err != errs[0] { //nolint:errorlint // identity: the first entry itself
			t.Errorf("CollapseBatch(%v) = %v, want the first entry", errs, err)
		}
		if mq.IsAmbiguous(err) != mq.IsAmbiguous(errs[0]) {
			t.Errorf("CollapseBatch(%v) changed the batch's ambiguity", errs)
		}
	}
}

// TestCollapseBatchKeepsDefiniteFailuresThatDisagree is the middle arm for
// definite failures. A refusal is not one outcome: a caller retries a
// retryable one and drops the rest, and it reads the code to learn why. So a
// batch every entry of which was definitely refused collapses only when every
// entry carries the same code and the same retryability. Comparing ambiguity
// alone collapsed [refused, throttled] to the refusal, and the caller dropped
// the throttled entry it should have retried; through a BatchSettler, one
// caller's lost lease became another caller's answer although the driver had
// told that caller to retry.
func TestCollapseBatchKeepsDefiniteFailuresThatDisagree(t *testing.T) {
	refused := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeInvalidArgument, Err: errors.New("entry refused")}
	tooBig := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeInvalidArgument, Err: errors.New("entry too large")}
	throttled := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeThrottled, Retryable: true, Err: errors.New("throttled")}
	loading := &mq.OpError{Operation: mq.OpAck, Code: mq.CodeUnavailable, Retryable: true, Err: errors.New("LOADING")}
	readonly := &mq.OpError{Operation: mq.OpAck, Code: mq.CodeUnavailable, Retryable: true, Err: errors.New("READONLY")}
	down := &mq.OpError{Operation: mq.OpAck, Code: mq.CodeUnavailable, Err: errors.New("the replica is gone")}

	for _, errs := range [][]error{
		{refused, throttled},           // the code and the retryability differ
		{mq.ErrLeaseLost, loading},     // a sentinel beside a retryable failure
		{loading, down},                // one code, and only one is retryable
		{refused, errors.New("plain")}, // a code beside none
		{refused, tooBig, throttled},
	} {
		err := driverkit.CollapseBatch(errs)
		var be *mq.BatchError
		if !errors.As(err, &be) {
			t.Errorf("CollapseBatch(%v) = %v (code %v, IsRetryable %v), want a *BatchError: its entries are "+
				"definite failures with different codes or retryability", errs, err, mq.CodeOf(err), mq.IsRetryable(err))
			continue
		}
		if len(be.Errs) != len(errs) {
			t.Fatalf("CollapseBatch(%v) carries %d entries, want %d", errs, len(be.Errs), len(errs))
		}
		for i := range errs {
			if be.Errs[i] != errs[i] { //nolint:errorlint // identity: the entry must be the caller's own error at its own index
				t.Errorf("CollapseBatch(%v): entry %d is %v, want %v", errs, i, be.Errs[i], errs[i])
			}
		}
	}

	// One code and one retryability, whatever the text: the first, unwrapped.
	for _, errs := range [][]error{
		{refused, tooBig},
		{loading, readonly},
	} {
		if err := driverkit.CollapseBatch(errs); err != errs[0] { //nolint:errorlint // identity: the first entry itself
			t.Errorf("CollapseBatch(%v) = %v, want the first entry: every entry has the same code and retryability", errs, err)
		}
	}
}

// notSent is the answer a driver's loop gives the messages behind the one that
// stopped it, as drivers/activemq/stomp's does: the stopping error's code,
// retryable, because they never left the process.
func notSent(stop error) error {
	return &mq.OpError{
		Operation: mq.OpPublish, Code: mq.ErrorCode(errcode.NotAttempted(errcode.Code(mq.CodeOf(stop)))),
		Retryable: true, Err: errcode.ErrNotAttempted,
	}
}

// TestCollapseBatchAnswersABatchStoppedByAClosedSinkOrAnEndedContextWithTheStop
// is a batch that stopped at its first entry because the sink was closed or
// the context ended. The entries behind it never left the process and are
// retryable, which the stop is not, but the batch has the one outcome the
// stop names, so it is that error. Counting the retryability as a second
// outcome made it a *BatchError, CodePartial, with nothing accepted, and
// IsRetryable true when the stop was a bare context error. A batch stopped by
// a refusal stays per entry: the refused entry is not safe to repeat and the
// ones behind it are.
func TestCollapseBatchAnswersABatchStoppedByAClosedSinkOrAnEndedContextWithTheStop(t *testing.T) {
	m := &driverkit.ErrorMapper{Driver: "test"}
	for _, stop := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		mq.ErrClosed,
		m.Map(mq.OpPublish, "orders", "", context.Canceled),
	} {
		errs := []error{stop, notSent(stop), notSent(stop)}
		if err := driverkit.CollapseBatch(errs); err != stop { //nolint:errorlint // identity: the stop itself
			t.Errorf("CollapseBatch(%v) = %v (code %v, IsRetryable %v), want the stop itself",
				errs, err, mq.CodeOf(err), mq.IsRetryable(err))
		}
	}
	refused := &mq.OpError{Operation: mq.OpPublish, Code: mq.CodeInvalidArgument, Err: errors.New("entry refused")}
	var be *mq.BatchError
	if err := driverkit.CollapseBatch([]error{refused, notSent(refused)}); !errors.As(err, &be) {
		t.Errorf("CollapseBatch(refused, not sent) = %v, want a *BatchError: the entry behind the refusal is safe to send again", err)
	}
}

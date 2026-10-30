package driverkit

import (
	"errors"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// CollapseBatch turns one error per item into the single value the API contract
// The specification asks a batch operation to return.
//
// The rule has three arms and only the middle one is interesting:
//
//   - nothing failed, so the batch succeeded and the error is nil;
//   - EVERY entry failed, and every entry has the same outcome -- all
//     ambiguous, or all definitely refused with one code and one
//     retryability -- so the first one is returned unwrapped. Nothing is
//     misattributed by collapsing a batch whose entries all say the same
//     thing, and a caller whose whole call was refused gets the error
//     handling it already has for a single operation;
//   - otherwise the result is an index-aligned [mq.BatchError] and the caller
//     can tell which entry is which. That includes a batch every entry of
//     which failed when some were refused and some are ambiguous: one error
//     cannot say both, and the first entry's used to be returned whichever it
//     was. A batch whose first entry was refused and whose last may already
//     be in the broker then answered [mq.IsAmbiguous] false, and a caller who
//     republished the "refused" batch duplicated the last entry, which the specification
//     names a disqualifying defect. [mq.IsAmbiguous] reads every entry of a
//     BatchError, so the batch is ambiguous when any entry is. It also
//     includes refusals whose codes or retryability differ: a caller retries
//     a retryable refusal and drops the rest, so [refused, throttled]
//     collapsed to the refusal dropped the entry it should have retried, and
//     a [BatchSettler] handed one caller's lost lease to a caller the driver
//     had told to retry.
//
// An ambiguous entry's outcome is unknown whatever its code, and no caller
// repeats one ([mq.IsRetryable] is false for every one of them), so a batch
// of ambiguous entries collapses whatever their codes say.
//
// A batch stopped at its first entry by a closed sink or an ended context is
// that one error too. The entries behind it are answered as never attempted
// (errcode.NotAttempted, as drivers/activemq/stomp's loop answers them), with
// its code, and are retryable because they never left the process, which the
// stop is not; counting that as a second outcome made such a batch a
// [mq.BatchError], a partial success with nothing accepted.
//
// errs must be index-aligned with the items the caller submitted, including
// the nil entries: [mq.BatchError] is read by index, so a compacted slice
// attributes each failure to the wrong message.
//
// It lives here because two drivers wrote it identically -- `drivers/sqs` and
// `drivers/snssqs`, differing in one comment -- and the third driver to need
// a batch settlement would have written it a third time.
// The root package's publisher applies the same rule to the results it
// gathers itself; it cannot call this one, which imports it.
func CollapseBatch(errs []error) error {
	failed, first := 0, -1
	for i, err := range errs {
		if err != nil {
			failed++
			if first < 0 {
				first = i
			}
		}
	}
	switch {
	case failed == 0:
		return nil
	case failed == len(errs) && sameOutcome(errs):
		return errs[first]
	default:
		return &mq.BatchError{Errs: errs}
	}
}

// sameOutcome reports whether every entry of errs, none of them nil, has the
// same outcome: all ambiguous, or all definite with one code and one
// retryability, or the never-attempted entries behind a first entry that
// stopped the batch.
func sameOutcome(errs []error) bool {
	first := errs[0]
	ambiguous := mq.IsAmbiguous(first)
	for _, err := range errs[1:] {
		if mq.IsAmbiguous(err) != ambiguous {
			return false
		}
		if ambiguous || mq.CodeOf(err) == mq.CodeOf(first) && mq.IsRetryable(err) == mq.IsRetryable(first) {
			continue
		}
		if !stoppedBy(first, err) {
			return false
		}
	}
	return true
}

// stoppedBy reports whether err is the never-attempted answer of an entry the
// batch did not reach because stop, a closed sink or an ended context, ended it
// first.
func stoppedBy(stop, err error) bool {
	switch mq.CodeOf(stop) {
	case mq.CodeClosed, mq.CodeCanceled, mq.CodeDeadlineExceeded:
		return mq.CodeOf(err) == mq.CodeOf(stop) && errors.Is(err, errcode.ErrNotAttempted)
	default:
		return false
	}
}

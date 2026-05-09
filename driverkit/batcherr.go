package driverkit

import (
	mq "bitbucket.org/iqhive/iqmq"
)

// CollapseBatch turns one error per item into the single value the API contract
// The specification asks a batch operation to return.
//
// The rule has three arms and only the middle one is interesting:
//
//   - nothing failed, so the batch succeeded and the error is nil;
//   - EVERY entry failed, and every entry has the same outcome -- all
//     definitely refused, or all ambiguous -- so the first one is returned
//     unwrapped. Nothing is misattributed by collapsing a batch whose entries
//     all say the same thing, and a caller whose whole call was refused gets
//     the error handling it already has for a single operation;
//   - otherwise the result is an index-aligned [mq.BatchError] and the caller
//     can tell which entry is which. That includes a batch every entry of
//     which failed when some were refused and some are ambiguous: one error
//     cannot say both, and the first entry's used to be returned whichever it
//     was. A batch whose first entry was refused and whose last may already
//     be in the broker then answered [mq.IsAmbiguous] false, and a caller who
//     republished the "refused" batch duplicated the last entry, which the specification
//     names a disqualifying defect. [mq.IsAmbiguous] reads every entry of a
//     BatchError, so the batch is ambiguous when any entry is.
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
// same outcome: all ambiguous, or none.
func sameOutcome(errs []error) bool {
	ambiguous := mq.IsAmbiguous(errs[0])
	for _, err := range errs[1:] {
		if mq.IsAmbiguous(err) != ambiguous {
			return false
		}
	}
	return true
}

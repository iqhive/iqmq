package kafkatx

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
)

// TestAnEndFranzGoNeverSentIsDefinite: franz-go refuses two commits before it
// sends anything, and an ending it refused that way certainly did not
// happen. Every other failure of an ending may have reached the broker, so
// its outcome stays unknown.
func TestAnEndFranzGoNeverSentIsDefinite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		ambiguous bool
	}{
		{name: "a producer id that failed", err: kerr.OperationNotAttempted},
		{
			// franz-go's own wording, with both codes wrapped.
			name: "a topic recreated inside the transaction",
			err: fmt.Errorf("the transaction produced to %v before the topic was deleted and recreated; the records the old topic acknowledged are gone, abort the transaction: %w, %w",
				[]string{"orders"}, kerr.TransactionAbortable, kerr.UnknownTopicID),
		},
		{name: "the broker's own TRANSACTION_ABORTABLE", err: kerr.TransactionAbortable, ambiguous: true},
		{name: "an end nobody confirmed", err: kerr.UnknownServerError, ambiguous: true},
		{name: "a request cancelled in flight", err: context.Canceled, ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := mq.IsAmbiguous(wrap(tc.err, endSent(tc.err))); got != tc.ambiguous {
				t.Fatalf("an ending that failed with %v is ambiguous %v, want %v", tc.err, got, tc.ambiguous)
			}
		})
	}
}

// TestAFailedEndIsUnknownOnlyWhenItMayHaveReachedTheBroker: an end that
// never reached the broker certainly did not happen, and neither did one the
// broker refused outright, as long as the refusal answers the only EndTxn
// written and is the answer read to it. The EndTxn count comes from
// TrackRecords' ledger or the Kafka driver's, found by its method. A client
// without either, or with the ledger of a driver release that predates the
// count, cannot count its EndTxn requests, so on one every failure franz-go
// did not refuse itself stays unknown, unless the client was closed before
// the end began.
func TestAFailedEndIsUnknownOnlyWhenItMayHaveReachedTheBroker(t *testing.T) {
	open, err := kgo.NewClient()
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer open.Close()
	closed, err := kgo.NewClient()
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	closed.Close()
	// counted is a TrackRecords ledger that counted written EndTxn requests,
	// and answered answers to them, since the mark.
	counted := func(written, answered uint64) ledger {
		l := &recordLedger{}
		l.ends.Store(written)
		l.answers.Store(answered)
		return l
	}
	// driver is the Kafka driver's ledger, which answers the same two
	// methods and none of TrackRecords' own.
	driver := func(written, answered uint64) ledger { return driverLedger{written: written, answered: answered} }
	// lookedUp is a ledger of either kind that also saw a coordinator
	// lookup written after its only EndTxn: the retry of an end answered
	// COORDINATOR_NOT_AVAILABLE, whose lookup franz-go makes first.
	lookedUp := func(driver bool) ledger {
		if driver {
			return driverLedger{written: 1, answered: 1, lookedUp: true}
		}
		l := &recordLedger{}
		l.ends.Store(1)
		l.answers.Store(1)
		l.endAt.Store(l.writes.Add(1))
		l.lookupAt.Store(l.writes.Add(1))
		return l
	}
	for _, tc := range []struct {
		name      string
		cl        *kgo.Client
		records   ledger
		before    bool // the client was closed before the end began
		err       error
		ambiguous bool
		code      mq.ErrorCode
	}{
		{name: "franz-go refused it unsent", cl: open, err: kerr.OperationNotAttempted, code: mq.CodeUnavailable},
		{name: "a client closed before the end", cl: closed, before: true, err: context.Canceled, code: mq.CodeClosed},
		{name: "a client that closed while the end was out", cl: closed, err: kgo.ErrClientClosed, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a client that closed before any EndTxn was written", cl: closed, records: counted(0, 0), err: context.Canceled, code: mq.CodeClosed},
		{name: "a ctx that ended before any EndTxn was written", cl: open, records: counted(0, 0), err: context.Canceled, code: mq.CodeCanceled},
		{name: "a ctx that ended, uncounted", cl: open, err: context.Canceled, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a fence answered to the only EndTxn", cl: open, records: counted(1, 1), err: kerr.ProducerFenced, code: mq.CodeConflict},
		{name: "an old epoch answered to the only EndTxn", cl: open, records: counted(1, 1), err: kerr.InvalidProducerEpoch, code: mq.CodeConflict},
		{name: "an authorisation failure answered to the only EndTxn", cl: open, records: counted(1, 1), err: kerr.TransactionalIDAuthorizationFailed, code: mq.CodeForbidden},
		{name: "an authorisation failure after the only EndTxn's answer was lost", cl: open, records: counted(1, 0), err: kerr.TransactionalIDAuthorizationFailed, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a coordinator lookup refused as unauthorised before any EndTxn", cl: open, records: counted(0, 0), err: kerr.TransactionalIDAuthorizationFailed, code: mq.CodeForbidden},
		{name: "a fence answered to an EndTxn written again", cl: open, records: counted(2, 1), err: kerr.ProducerFenced, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "an authorisation failure from the lookup after the only EndTxn's answer", cl: open, records: lookedUp(false), err: kerr.TransactionalIDAuthorizationFailed, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "an authorisation failure from the lookup after the only EndTxn's answer, on the driver's ledger", cl: open, records: lookedUp(true), err: kerr.TransactionalIDAuthorizationFailed, ambiguous: true, code: mq.CodeSettlementUnknown},
		// Every EndTxn answered and the last one a fence is still unknown:
		// franz-go writes the end again after a retriable answer, and
		// this package cannot see which. CONCURRENT_TRANSACTIONS is the
		// usual one (the producer that fenced this one is still finishing
		// its commit) and left nothing behind, but COORDINATOR_NOT_AVAILABLE
		// can follow a log append that timed out and still completes.
		{name: "a fence answered to the second of two EndTxn, both answered", cl: open, records: counted(2, 2), err: kerr.ProducerFenced, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a fence, uncounted", cl: open, err: kerr.ProducerFenced, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "an end nobody confirmed", cl: open, records: counted(1, 1), err: kerr.UnknownServerError, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a fence answered to the only EndTxn, on the driver's ledger", cl: open, records: driver(1, 1), err: kerr.ProducerFenced, code: mq.CodeConflict},
		{name: "an authorisation failure answered to the only EndTxn, on the driver's ledger", cl: open, records: driver(1, 1), err: kerr.TransactionalIDAuthorizationFailed, code: mq.CodeForbidden},
		{name: "a ctx that ended before any EndTxn was written, on the driver's ledger", cl: open, records: driver(0, 0), err: context.Canceled, code: mq.CodeCanceled},
		{name: "a fence answered to an EndTxn written again, on the driver's ledger", cl: open, records: driver(2, 1), err: kerr.ProducerFenced, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a fence answered to the second of two EndTxn, both answered, on the driver's ledger", cl: open, records: driver(2, 2), err: kerr.ProducerFenced, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a fence, on an older driver's ledger", cl: open, records: recordsOnly{}, err: kerr.ProducerFenced, ambiguous: true, code: mq.CodeSettlementUnknown},
		{name: "a ctx that ended, on an older driver's ledger", cl: open, records: recordsOnly{}, err: context.Canceled, ambiguous: true, code: mq.CodeSettlementUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := failedEnd(tc.cl, tc.records, endMark{closed: tc.before}, tc.err)
			if mq.IsAmbiguous(got) != tc.ambiguous || mq.CodeOf(got) != tc.code {
				t.Fatalf("an end that failed with %v = %v (code %v, ambiguous %v), want code %v, ambiguous %v",
					tc.err, got, mq.CodeOf(got), mq.IsAmbiguous(got), tc.code, tc.ambiguous)
			}
			if !errors.Is(got, tc.err) {
				t.Fatalf("an end that failed with %v = %v, which does not wrap the failure", tc.err, got)
			}
			if mq.CodeOf(got) == mq.CodeClosed && !errors.Is(got, kgo.ErrClientClosed) {
				t.Fatalf("an end on a closed client = %v, which does not say the client closed", got)
			}
		})
	}
}

// driverLedger stands for the Kafka driver's ledger, which this package
// cannot name: it answers RecordLedger and EndTxnLedger, and its EndTxn count
// is fixed at written and answered, with a lookup after the latest when
// lookedUp is set.
type driverLedger struct {
	written, answered uint64
	lookedUp          bool
}

func (driverLedger) RecordLedger() (taken, finished, failed uint64, last error) {
	return 0, 0, 0, nil
}

func (l driverLedger) EndTxnLedger() (written, answered uint64, lookedUp bool) {
	return l.written, l.answered, l.lookedUp
}

// recordsOnly stands for the ledger of a Kafka driver release that predates
// the EndTxn count: it answers RecordLedger alone.
type recordsOnly struct{}

func (recordsOnly) RecordLedger() (taken, finished, failed uint64, last error) {
	return 0, 0, 0, nil
}

// TestARecordFailureTheDriverCannotNameIsRetryable: a commit refused for a
// record failure takes the code the driver gives that failure, and one the
// driver has no code for is unavailable and retryable, as every other
// failure franz-go reports to this package is. It kept CodeUnknown, which
// no caller retries.
func TestARecordFailureTheDriverCannotNameIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		code      mq.ErrorCode
		retryable bool
	}{
		{name: "a record the broker refused", err: kerr.InvalidRecord, code: mq.CodeCorruptMessage},
		{
			// franz-go's own wording for a record with no topic.
			name: "a failure the driver has no code for",
			err:  errors.New("cannot produce record with no topic and no default topic"),
			code: mq.CodeUnavailable, retryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := refused(tc.err)
			if mq.CodeOf(err) != tc.code || mq.IsRetryable(err) != tc.retryable || mq.IsAmbiguous(err) {
				t.Fatalf("refused(%v) = %v (code %v, retryable %v, ambiguous %v), want code %v, retryable %v, definite",
					tc.err, err, mq.CodeOf(err), mq.IsRetryable(err), mq.IsAmbiguous(err), tc.code, tc.retryable)
			}
			if !errors.Is(err, tc.err) {
				t.Fatalf("refused(%v) = %v, which does not wrap the record's failure", tc.err, err)
			}
		})
	}
}

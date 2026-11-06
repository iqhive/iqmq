package kafka

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// recordLedger counts a transactional producer's records and their failures,
// and its EndTxn requests and the answers to them, for x/kafkatx.
//
// franz-go keeps a transaction committable after the broker refused one of
// its records outright (INVALID_RECORD, MESSAGE_TOO_LARGE): the producer id
// stays valid, and EndTransaction makes the records it accepted visible
// without the refused one. A record's failure reaches only its promise, which
// is this driver's, and an asynchronous publication's promise runs during the
// commit's own flush. So kafkatx cannot refuse that commit unless the client
// carries a hook that saw the failure.
//
// The EndTxn count is for a commit whose end failed. kafkatx reports that
// commit as unknown unless it can tell that no EndTxn reached the broker, or
// that the broker refused the only one written (fenced, or unauthorised).
// Only a count of the EndTxn requests written, and of the answers read to
// them, tells it that, with whether a coordinator lookup was written after
// the latest EndTxn: franz-go looks the coordinator up again before it writes
// an EndTxn again after an answer it retries (COORDINATOR_NOT_AVAILABLE,
// which can follow an end that still commits), and when the broker refuses
// that lookup, the refusal comes back as the end's with one EndTxn written
// and one answer read.
//
// kafkatx finds this hook among the client's hooks by its RecordLedger
// method, and the EndTxn count by its EndTxnLedger method, not by its type:
// kafkatx is built against published releases of this module, and one older
// than the ledger, or than the count, has no type or method to name. Its own
// TrackRecords hook, for a client a caller built, answers the same methods.
type recordLedger struct {
	taken    atomic.Uint64
	finished atomic.Uint64
	failed   atomic.Uint64

	// ends counts the EndTxn requests the producer wrote, and answers the
	// answers to them it read in full.
	ends    atomic.Uint64
	answers atomic.Uint64
	// writes numbers the EndTxn and FindCoordinator requests the producer
	// wrote, and endAt and lookupAt are the number of the latest of each.
	writes   atomic.Uint64
	endAt    atomic.Uint64
	lookupAt atomic.Uint64

	mu   sync.Mutex
	last error
}

// compile-time proof of which of franz-go's hooks the ledger takes.
var (
	_ kgo.HookProduceRecordBuffered   = (*recordLedger)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*recordLedger)(nil)
	_ kgo.HookBrokerWrite             = (*recordLedger)(nil)
	_ kgo.HookBrokerRead              = (*recordLedger)(nil)
)

// OnProduceRecordBuffered counts a record the producer has taken. franz-go
// calls it for every record, including one it then refuses before buffering.
func (l *recordLedger) OnProduceRecordBuffered(*kgo.Record) { l.taken.Add(1) }

// OnProduceRecordUnbuffered counts a record whose promise is about to run.
// The failure is recorded before the record is counted finished, so a reader
// that sees it finished also sees its failure.
func (l *recordLedger) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err != nil {
		l.mu.Lock()
		l.last = err
		l.mu.Unlock()
		l.failed.Add(1)
	}
	l.finished.Add(1)
}

// RecordLedger reports how many records the producer has taken, how many of
// them have finished, how many of those failed, and the latest failure.
//
// finished is read before taken, so finished >= taken means every record
// taken by the time of the call has finished and its failure is counted.
func (l *recordLedger) RecordLedger() (taken, finished, failed uint64, last error) {
	finished = l.finished.Load()
	taken = l.taken.Load()
	failed = l.failed.Load()
	l.mu.Lock()
	last = l.last
	l.mu.Unlock()
	return taken, finished, failed, last
}

// OnBrokerWrite counts an EndTxn request the producer wrote, and numbers it
// and every coordinator lookup. franz-go calls it once a request is written,
// before it reads the answer, so an EndTxn written for an EndTransaction is
// counted before that returns. It calls it for a write that failed too, and
// never for a request it gave up before writing, so an end it counts none for
// certainly did not reach the broker. A lookup franz-go makes before it
// writes an EndTxn again is written after the first one's answer is read, so
// it is numbered after that EndTxn.
func (l *recordLedger) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	switch key {
	case kmsg.EndTxn.Int16():
		l.ends.Add(1)
		l.endAt.Store(l.writes.Add(1))
	case kmsg.FindCoordinator.Int16():
		l.lookupAt.Store(l.writes.Add(1))
	}
}

// OnBrokerRead counts an answer to an EndTxn the producer read in full.
// franz-go calls it before it hands the answer on, so an answer EndTransaction
// read is counted before that returns. An end whose only answer was lost has
// none counted.
func (l *recordLedger) OnBrokerRead(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if key == kmsg.EndTxn.Int16() && err == nil {
		l.answers.Add(1)
	}
}

// EndTxnLedger reports how many EndTxn requests the producer has written and
// how many answers to them it has read in full, and whether it has written a
// coordinator lookup since the latest EndTxn.
func (l *recordLedger) EndTxnLedger() (written, answered uint64, lookedUp bool) {
	return l.ends.Load(), l.answers.Load(), l.lookupAt.Load() > l.endAt.Load()
}

package kafka

import (
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

// recordLedger counts a transactional producer's records and their failures,
// for x/kafkatx.
//
// franz-go keeps a transaction committable after the broker refused one of
// its records outright (INVALID_RECORD, MESSAGE_TOO_LARGE): the producer id
// stays valid, and EndTransaction makes the records it accepted visible
// without the refused one. A record's failure reaches only its promise, which
// is this driver's, and an asynchronous publication's promise runs during the
// commit's own flush. So kafkatx cannot refuse that commit unless the client
// carries a hook that saw the failure.
//
// kafkatx finds this hook among the client's hooks by its RecordLedger
// method, not by its type: kafkatx is built against published releases of
// this module, and one older than the ledger has no type to name. Its own
// TrackRecords hook, for a client a caller built, answers the same method.
type recordLedger struct {
	taken    atomic.Uint64
	finished atomic.Uint64
	failed   atomic.Uint64

	mu   sync.Mutex
	last error
}

// compile-time proof of which of franz-go's hooks the ledger takes.
var (
	_ kgo.HookProduceRecordBuffered   = (*recordLedger)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*recordLedger)(nil)
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

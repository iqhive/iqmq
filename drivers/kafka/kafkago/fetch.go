package kafkago

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	fetchapi "github.com/segmentio/kafka-go/protocol/fetch"
)

// abortMarker is the control record type a transaction coordinator writes
// when it aborts a transaction.
const abortMarker = 0

// fetchPartition is one fetch of one partition at a concrete offset.
//
// It is [kgo.Client.Fetch] one level down, and it has to be: a read_committed
// broker answers with the LAST STABLE OFFSET as its bound and the list of
// transactions aborted within the range, and leaves dropping the aborted
// records to the consumer. Client.Fetch decodes that list and throws it away,
// so a reader on it hands out every aborted record as though it were
// committed. Everything else matches Client.Fetch: the same transport, the
// same address, the same wait bounded by half the client timeout.
func (s *source) fetchPartition(ctx context.Context, partition int, off int64) (*kgo.FetchResponse, []fetchapi.ResponseTransaction, error) {
	cl := s.b.cl
	wait := s.b.cfg.FetchMaxWait
	if half := halfBudget(ctx, cl.Timeout); half > 0 && half < wait {
		wait = half
	}
	if cl.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cl.Timeout)
		defer cancel()
	}
	tr := cl.Transport
	if tr == nil {
		tr = kgo.DefaultTransport
	}
	m, err := tr.RoundTrip(ctx, cl.Addr, &fetchapi.Request{
		ReplicaID:      -1,
		MaxWaitTime:    int32(wait / time.Millisecond),
		MinBytes:       int32(s.b.cfg.FetchMinBytes),
		MaxBytes:       int32(s.b.cfg.FetchMaxBytes),
		IsolationLevel: int8(isolation(s.b.cfg)),
		SessionID:      -1,
		SessionEpoch:   -1,
		Topics: []fetchapi.RequestTopic{{
			Topic: s.topic,
			Partitions: []fetchapi.RequestPartition{{
				Partition:          int32(partition),
				CurrentLeaderEpoch: -1,
				FetchOffset:        off,
				LogStartOffset:     -1,
				PartitionMaxBytes:  int32(s.b.cfg.FetchMaxBytes),
			}},
		}},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("kafka.(*Client).Fetch: %w", err)
	}
	res, ok := m.(*fetchapi.Response)
	if !ok || len(res.Topics) == 0 {
		return nil, nil, fmt.Errorf("kafka.(*Client).Fetch: %w", protocol.ErrNoTopic)
	}
	topic := &res.Topics[0]
	if len(topic.Partitions) == 0 {
		return nil, nil, fmt.Errorf("kafka.(*Client).Fetch: %w", protocol.ErrNoPartition)
	}
	p := &topic.Partitions[0]
	resp := &kgo.FetchResponse{
		Topic:            topic.Topic,
		Partition:        int(p.Partition),
		HighWatermark:    p.HighWatermark,
		LastStableOffset: p.LastStableOffset,
		LogStartOffset:   p.LogStartOffset,
		Records:          p.RecordSet.Records,
	}
	switch {
	case p.ErrorCode != 0:
		resp.Error = kgo.Error(p.ErrorCode)
	case res.ErrorCode != 0:
		resp.Error = kgo.Error(res.ErrorCode)
	}
	return resp, p.AbortedTransactions, nil
}

// halfBudget is the server-side wait kafka-go's Client allows a request: half
// of the client timeout or of what is left of the context, whichever is
// shorter, because the rest is the round trip's.
func halfBudget(ctx context.Context, timeout time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if remain := time.Until(deadline); remain < timeout {
			timeout = remain
		}
	}
	return timeout / 2
}

// batchesOf is a response's record batches in log order. kafka-go hands a
// multi-batch response back as a [protocol.RecordStream], whose own
// ReadRecord hides which batch a record came from and skips control batches
// silently, and both are what aborted-transaction filtering needs to see.
func batchesOf(r kgo.RecordReader) []kgo.RecordReader {
	if r == nil {
		return nil
	}
	if rs, ok := r.(*protocol.RecordStream); ok {
		return rs.Records
	}
	return []kgo.RecordReader{r}
}

// abortFilter decides which batches of one fetch response belong to aborted
// transactions, the way the Java consumer does: an aborted transaction
// becomes active for its producer at the transaction's first offset and stays
// active until that producer's ABORT marker, and a transactional batch of a
// producer with an active abort is dropped whole.
type abortFilter struct {
	pending []fetchapi.ResponseTransaction
	active  map[int64]struct{}
}

func newAbortFilter(aborted []fetchapi.ResponseTransaction) abortFilter {
	slices.SortFunc(aborted, func(a, b fetchapi.ResponseTransaction) int {
		return cmp.Compare(a.FirstOffset, b.FirstOffset)
	})
	return abortFilter{pending: aborted}
}

// drops reports whether a data batch is part of an aborted transaction.
//
// A transaction starts on a batch boundary of its own producer, so comparing
// against the batch's base offset activates an abort no later than its first
// batch.
func (f *abortFilter) drops(batch kgo.RecordReader) bool {
	rb, ok := batch.(*protocol.RecordBatch)
	if !ok || !rb.Attributes.Transactional() {
		return false
	}
	for len(f.pending) > 0 && f.pending[0].FirstOffset <= rb.BaseOffset {
		if f.active == nil {
			f.active = make(map[int64]struct{}, len(f.pending))
		}
		f.active[f.pending[0].ProducerID] = struct{}{}
		f.pending = f.pending[1:]
	}
	_, aborted := f.active[rb.ProducerID]
	return aborted
}

// ended reads a control batch, ends the producer's active abort on an ABORT
// marker, and returns the offset after the batch's last marker, or -1 when it
// holds none it could read.
func (f *abortFilter) ended(cb *protocol.ControlBatch) int64 {
	last := int64(-1)
	for {
		cr, err := cb.ReadControlRecord()
		if err != nil {
			return last
		}
		last = cr.Offset + 1
		if cr.Type == abortMarker {
			delete(f.active, cb.ProducerID)
		}
	}
}

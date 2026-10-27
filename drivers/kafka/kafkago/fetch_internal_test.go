package kafkago

import (
	"testing"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

// A fetch response holding only a transaction marker must still move the
// reader past it. kafka-go's record stream skips control batches without a
// word, so the fetch loop saw no record, fetched the marker's offset again,
// and spun on it for as long as nothing was published after it.
func TestAbsorbFetchAdvancesPastAControlBatch(t *testing.T) {
	const marker = 7
	cb := protocol.NewControlBatch(protocol.ControlRecord{Offset: marker, Type: 1})
	cb.ProducerID = 42
	resp := &kgo.FetchResponse{Records: &protocol.RecordStream{Records: []protocol.RecordReader{cb}}}
	s := &source{}
	next, fatal := s.absorbFetch(t.Context(), resp, nil, 0, marker, 1, 0)
	if fatal {
		t.Fatalf("absorbFetch reported the generation over")
	}
	if next != marker+1 {
		t.Fatalf("next fetch offset is %d, want %d (past the marker)", next, marker+1)
	}
}

// A fetch response holding only an empty record batch must move the reader
// past it as well. Compaction keeps a batch whose records it removed when the
// batch is its producer's last, so the producer's state survives, and the
// loop saw no record in it, fetched the same offset again and was answered
// with the same batch for as long as nothing followed it. kafka-go drops the
// batch's last offset delta when it decodes it, so the reader can only step
// past the first offset the batch covers, which is the fetch offset or the
// batch's base, whichever is later.
func TestAbsorbFetchAdvancesPastAnEmptyBatch(t *testing.T) {
	const base = 7
	for _, from := range []int64{base - 3, base, base + 2} {
		empty := &protocol.RecordBatch{BaseOffset: base, Records: protocol.NewRecordReader()}
		resp := &kgo.FetchResponse{Records: &protocol.RecordStream{Records: []protocol.RecordReader{empty}}}
		s := &source{}
		next, fatal := s.absorbFetch(t.Context(), resp, nil, 0, from, 1, 0)
		if fatal {
			t.Fatalf("absorbFetch from %d reported the generation over", from)
		}
		if want := max(from, base) + 1; next != want {
			t.Errorf("from %d the next fetch offset is %d, want %d (past the empty batch's first covered offset)", from, next, want)
		}
	}
}

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

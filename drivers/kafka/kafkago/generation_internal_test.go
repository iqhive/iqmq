package kafkago

import (
	"sync"
	"testing"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// TestEndingGenerationAbsorbsNothing pins the window between the start of
// endGeneration and the next beginGeneration. The partitions are stale and
// the generation counter has not moved, so a record of the ending generation
// that a Fetch was already holding passed the generation check, and the stale
// partition made it re-base the tracker past the offsets still in flight,
// which the safe-boundary commit then skipped.
func TestEndingGenerationAbsorbsNothing(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	p := &partState{partition: 0, tr: tr, inflight: 1, committed: 10}
	s := &source{parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1}
	s.cond = sync.NewCond(&s.mu)
	gen := tr.Gen()

	s.stopDispatch()
	dst := make([]mq.DriverDelivery, 1)
	n, err := s.absorb(fetched{msg: &kgo.Message{Partition: 0, Offset: 11}, gen: 1}, dst, 0)
	if err != nil || n != 0 {
		t.Fatalf("absorb during an ending generation = (%d, %v), want (0, nil)", n, err)
	}
	if tr.Gen() != gen || tr.Watermark() != 10 || !p.stale {
		t.Fatalf("the ending generation's record re-based the partition: gen %d (want %d), watermark %d (want 10), stale %t",
			tr.Gen(), gen, tr.Watermark(), p.stale)
	}
}

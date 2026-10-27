package kafkago

import (
	"sync"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// A record the reader queued from the old log before it rewound must not
// reach the re-based tracker. Absorbed after the re-base, its offset, which is
// the old log's, moved the tracker past the new log's first records, and
// those were then refused as out of order and dropped.
func TestRewindDiscardsTheOldLogsQueuedRecords(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	for off := range int64(5) {
		if err := tr.Track(tr.Gen(), off); err != nil {
			t.Fatal(err)
		}
		tr.Complete(tr.Gen(), off)
	}
	p := &partState{partition: 0, tr: tr, committed: 5, firstAt: map[int64]time.Time{}}
	s := &source{b: &Backend{}, topic: "t", parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1}
	s.cond = sync.NewCond(&s.mu)

	rewinds := s.rewind(0, 0)
	dst := make([]mq.DriverDelivery, 2)
	n, err := s.absorb(fetched{msg: &kgo.Message{Partition: 0, Offset: 5}, gen: 1}, dst, 0)
	if err != nil || n != 0 {
		t.Fatalf("absorb of a record queued from the old log = (%d, %v), want (0, nil)", n, err)
	}
	n, err = s.absorb(fetched{msg: &kgo.Message{Partition: 0, Offset: 0}, gen: 1, rewinds: rewinds}, dst, n)
	if err != nil || n != 1 {
		t.Fatalf("absorb of the new log's first record = (%d, %v), want (1, nil)", n, err)
	}
	if got := dst[0].Info.Offset; got != 0 {
		t.Fatalf("the delivery is offset %d, want 0", got)
	}
}

// A commit whose boundary was read before the reader rewound the partition
// must not be sent after it. The commit loop reads the boundary and then
// sends it, and a rewind in between left it sending an offset of the old
// log: the group's position then sat past the new log's first records, and
// a member that took the partition skipped every one of them it had not
// settled.
func TestABoundaryReadBeforeARewindIsNotCommitted(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	for off := range int64(5) {
		if err := tr.Track(tr.Gen(), off); err != nil {
			t.Fatal(err)
		}
		tr.Complete(tr.Gen(), off)
	}
	p := &partState{partition: 0, tr: tr, committed: 0, dirty: true, firstAt: map[int64]time.Time{}}
	s := &source{b: &Backend{}, topic: "t", parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1}
	s.cond = sync.NewCond(&s.mu)

	// The commit loop's first half: the boundary is 5, and not yet sent.
	offsets, parts := s.pendingCommits(nil)
	if got := offsets["t"][0]; got != 5 {
		t.Fatalf("the boundary read is %d, want 5", got)
	}
	s.rewind(0, 0)
	// The second half. The generation is a zero value, so a boundary that
	// reached it would fail the call: nothing may be sent at all.
	sent := func() (sent bool) {
		defer func() {
			if recover() != nil {
				sent = true
			}
		}()
		_ = s.commit(&kgo.Generation{}, offsets, parts)
		return false
	}()
	if sent {
		t.Fatal("the old log's boundary was sent after the rewind")
	}
}

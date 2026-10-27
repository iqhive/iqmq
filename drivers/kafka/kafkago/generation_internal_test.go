package kafkago

import (
	"errors"
	"sync"
	"testing"
	"time"

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

// TestEndingGenerationSettlesInflight pins step (3) of the sequence. The wait
// is for the in-flight deliveries to settle, so a settlement that arrives
// during it must be accepted: refusing it as lease lost left the wait to run
// out the whole RevokeWait, and the boundary committed in step (4) then
// omitted a record the handler had finished.
func TestEndingGenerationSettlesInflight(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	p := &partState{partition: 0, tr: tr, inflight: 1, committed: 10, firstAt: map[int64]time.Time{}}
	b := &Backend{cfg: Config{RevokeWait: 5 * time.Second}}
	s := &source{b: b, topic: "t", parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1}
	s.cond = sync.NewCond(&s.mu)
	d := &mq.DriverDelivery{Token: 0, Gen: tr.Gen(), Info: mq.DeliveryInfo{Offset: 10}}
	assigned := map[int]bool{0: true}

	s.stopDispatch()
	settled := make(chan bool, 1)
	start := time.Now()
	go func() { settled <- s.waitInflight(assigned) }()
	if err := (&settler{s: s}).Ack(t.Context(), d); err != nil {
		t.Errorf("Ack during the generation's wait = %v, want nil", err)
	}
	if !<-settled {
		t.Errorf("the wait ran out after %v: it did not see the settlement", time.Since(start))
	}
	offsets, _ := s.pendingCommits(assigned)
	if got := offsets["t"][0]; got != 11 {
		t.Errorf("the safe boundary to commit = %d, want 11", got)
	}
}

// TestACompletionAfterTheLastBoundaryIsRefused is the half of the end of a
// generation that a settlement can straddle. resolve answers for the delivery
// and lets go of the lock, the end of the generation reads the boundary it
// commits, and the completion lands after that read: no commit carries it,
// so the settlement must not be answered nil.
func TestACompletionAfterTheLastBoundaryIsRefused(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	p := &partState{partition: 0, tr: tr, inflight: 1, committed: 10, firstAt: map[int64]time.Time{}}
	s := &source{b: &Backend{}, topic: "t", parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1}
	s.cond = sync.NewCond(&s.mu)
	d := &mq.DriverDelivery{Token: 0, Gen: tr.Gen(), Info: mq.DeliveryInfo{Offset: 10}}
	assigned := map[int]bool{0: true}

	s.stopDispatch()
	resolved, err := s.resolve(d)
	if err != nil {
		t.Fatalf("resolve during the generation's wait = %v, want nil", err)
	}
	if offsets, _ := s.lastBoundary(assigned); len(offsets) != 0 {
		t.Fatalf("the boundary read before the completion is %v, want nothing to commit", offsets)
	}
	if err := s.complete(resolved, d); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a completion after the boundary was read = %v, want ErrLeaseLost: the commit carries nothing of it", err)
	}
	if wm := tr.Watermark(); wm != 10 {
		t.Fatalf("the refused completion moved the watermark to %d, want 10", wm)
	}
}

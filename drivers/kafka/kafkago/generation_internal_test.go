package kafkago

import (
	"errors"
	"sync"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/nack"
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

// stallingStrategy is a strategy whose Plan waits to be released, which holds
// a nack between resolve and apply for as long as a test needs it held.
type stallingStrategy struct {
	nack.Strategy
	planning, release chan struct{}
}

func (s stallingStrategy) Plan(d nack.Delivery, o mq.NackOptions) (nack.Plan, error) {
	close(s.planning)
	<-s.release
	return s.Strategy.Plan(d, o)
}

// nackAfterTheGeneration nacks the delivery at offset 10 of a source's one
// partition under a strategy of kind, and holds the nack between resolve and
// apply while the generation ends, the next one begins and its reader reads
// offset 10 again, which it does because nothing committed past it. It
// returns the source, the partition and what the nack answered.
func nackAfterTheGeneration(t *testing.T, kind nack.Kind) (*source, *partState, error) {
	t.Helper()
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	strategy, err := nack.New(kind, nack.Config{})
	if err != nil {
		t.Fatal(err)
	}
	stall := stallingStrategy{Strategy: strategy, planning: make(chan struct{}), release: make(chan struct{})}
	p := &partState{partition: 0, tr: tr, inflight: 1, committed: 10, firstAt: map[int64]time.Time{}}
	// The wait runs out, because the nack is still on its way.
	b := &Backend{cfg: Config{RevokeWait: 20 * time.Millisecond}, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	s := &source{
		b: b, topic: "t", strategy: stall, recs: newRecQueue(1), wake: make(chan struct{}, 1),
		parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1,
	}
	s.cond = sync.NewCond(&s.mu)
	d := &mq.DriverDelivery{
		Token: 0, Gen: tr.Gen(), Native: &kgo.Message{Topic: "t", Offset: 10},
		Info: mq.DeliveryInfo{Source: "t", Offset: 10, Attempt: 1},
	}

	nacked := make(chan error, 1)
	go func() { nacked <- (&settler{s: s}).Nack(t.Context(), d, mq.NackOptions{}) }()
	select {
	case <-stall.planning:
	case <-time.After(10 * time.Second):
		t.Fatal("the nack never got past resolve")
	}
	s.endGeneration(nil, map[int]bool{0: true}, &sync.WaitGroup{})
	s.beginGeneration(t.Context(), &kgo.Generation{Assignments: map[string][]kgo.PartitionAssignment{"t": {{ID: 0, Offset: 10}}}})
	dst := make([]mq.DriverDelivery, 1)
	if n, err := s.absorb(fetched{msg: &kgo.Message{Topic: "t", Offset: 10}, gen: s.gens}, dst, 0); n != 1 || err != nil {
		t.Fatalf("the next generation's first record = (%d, %v), want (1, nil)", n, err)
	}
	close(stall.release)
	return s, p, <-nacked
}

// A retry-in-place nack that resolve accepted and that applies only after the
// generation has ended must be refused, and must hold nothing. The handler
// was told nil, and the record it held went out again in the next generation
// beside the copy that generation's reader had read, so one generation handed
// offset 10 out twice and kept every later record of the partition back until
// the stale copy had gone.
func TestARetryInPlaceNackAfterTheGenerationEndedIsRefused(t *testing.T) {
	s, p, err := nackAfterTheGeneration(t, nack.BlockPartition)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a retry-in-place nack applied after the generation ended = %v, want ErrLeaseLost: the generation it was handed out in is over", err)
	}
	again := make([]mq.DriverDelivery, 1)
	if n := s.takeLocal(again); n != 0 {
		t.Fatalf("the next generation handed offset %d out a second time", again[0].Info.Offset)
	}
	s.mu.Lock()
	held, inflight := len(p.held), p.inflight
	s.mu.Unlock()
	if held != 0 || inflight != 1 {
		t.Fatalf("the refused nack left %d records held and %d in flight, want none held and the new generation's 1 in flight", held, inflight)
	}
}

// A stop nack that applies only after the generation has ended is refused the
// way resolve refuses one that arrives after it, and releases nothing of the
// next generation's in-flight count.
func TestAStopNackAfterTheGenerationEndedIsRefused(t *testing.T) {
	s, p, err := nackAfterTheGeneration(t, nack.Stop)
	if !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a stop nack applied after the generation ended = %v, want ErrLeaseLost: the generation it was handed out in is over", err)
	}
	if err := s.state(t.Context()); err != nil {
		t.Fatalf("the refused stop nack stopped the source: %v", err)
	}
	s.mu.Lock()
	inflight := p.inflight
	s.mu.Unlock()
	if inflight != 1 {
		t.Fatalf("the refused stop nack left %d in flight, want the new generation's 1", inflight)
	}
}

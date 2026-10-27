package kafka

import (
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// lostFixture is a source owning one partition with one delivery in flight at
// offset 10, whose offset 9 has completed and is not yet committed.
func lostFixture(t *testing.T, revokeWait time.Duration) (*source, *partState) {
	t.Helper()
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 9, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	for _, off := range []int64{9, 10} {
		if err := tr.Track(tr.Gen(), off); err != nil {
			t.Fatal(err)
		}
	}
	if wm, _ := tr.Complete(tr.Gen(), 9); wm != 10 {
		t.Fatalf("watermark after completing 9 = %d, want 10", wm)
	}
	key := tp{"t", 0}
	p := &partState{tp: key, tr: tr, inflight: 1, committed: 9, dirty: true, firstAt: map[int64]time.Time{}}
	b := &Backend{cfg: Config{RevokeWait: revokeWait}, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	s := &source{b: b, topic: "t", parts: map[tp]*partState{key: p}, order: []*partState{p}}
	s.cond = sync.NewCond(&s.mu)
	return s, p
}

// TestLostPartitionDoesNotWaitOutRevokeWait: a lost partition's settlements
// are refused as lease lost and never release the in-flight count, so the
// revocation's wait for them could only end at RevokeWait — holding up the
// rebalance and reporting a revoke timeout for a wait that had nothing to wait
// for.
func TestLostPartitionDoesNotWaitOutRevokeWait(t *testing.T) {
	s, p := lostFixture(t, 3*time.Second)
	d := &mq.DriverDelivery{Token: 0, Gen: p.tr.Gen(), Info: mq.DeliveryInfo{Offset: 10}}

	start := time.Now()
	s.onLost(t.Context(), nil, map[string][]int32{"t": {0}})
	if took := time.Since(start); took > time.Second {
		t.Errorf("losing a partition took %v: it waited for settlements it refuses", took)
	}
	if n := s.b.DegradedTransitions(); n != 0 {
		t.Errorf("losing a partition reported %d degraded transitions, want 0", n)
	}
	if err := (&settler{s: s}).Ack(t.Context(), d); err == nil {
		t.Error("a settlement for a lost partition was accepted")
	}
}

// TestCommitLoopSkipsALostPartition: once a partition is lost its position
// belongs to the member that now owns it, and the commit loop committing the
// watermark this member had reached would overwrite that member's position.
func TestCommitLoopSkipsALostPartition(t *testing.T) {
	s, p := lostFixture(t, time.Second)
	if offsets, _ := s.pendingCommits(nil); len(offsets) != 1 {
		t.Fatalf("an owned partition with an uncommitted watermark: %d topics pending, want 1", len(offsets))
	}
	s.mu.Lock()
	p.stale, p.draining = true, false
	s.mu.Unlock()
	if offsets, _ := s.pendingCommits(nil); len(offsets) != 0 {
		t.Errorf("the commit loop would commit a lost partition: %v", offsets)
	}
	if offsets, _ := s.pendingCommits(map[tp]bool{p.tp: true}); len(offsets) != 0 {
		t.Errorf("a revocation would commit a lost partition: %v", offsets)
	}
}

// TestPauseFlushAppliesTheLatestDecision: the pause and resume decisions are
// made under the source mutex and applied to the client after it, and they
// were applied as queued deltas, every pause before every resume. A resume
// decided before a pause then left the client fetching a partition the source
// had paused, for good.
func TestPauseFlushAppliesTheLatestDecision(t *testing.T) {
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 0, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	key := tp{"t", 0}
	p := &partState{tp: key, tr: tr, firstAt: map[int64]time.Time{}}
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	s := &source{cl: cl, topic: "t", parts: map[tp]*partState{key: p}, order: []*partState{p}}
	s.cond = sync.NewCond(&s.mu)

	// Three settlements decide pause, resume and pause again before any
	// of their flushes reaches the client.
	s.mu.Lock()
	s.pauseLocked(p)
	s.maybeResumeLocked(p)
	s.pauseLocked(p)
	want := p.paused
	s.mu.Unlock()
	s.flushPauses()
	s.flushPauses()

	got := len(cl.PauseFetchPartitions(nil)["t"]) == 1
	if got != want {
		t.Errorf("client paused = %v, source decided paused = %v", got, want)
	}
}

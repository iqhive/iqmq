package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
)

// The rebalance callbacks are the hardest thing this driver does, and the
// order of operations in [source.onRevoked] is the whole for Kafka.
//
// A rebalance can take a partition away while a handler still holds a delivery
// from it. From that moment two processes believe they own the same record, and
// exactly one of them may acknowledge it. The sequence that keeps that true:
//
//  1. STOP DISPATCH for the revoked partitions, so nothing more of them is
//     handed out and nothing held is redelivered.
//  2. WAIT, BOUNDED, for their in-flight deliveries to settle. Bounded
//     because the coordinator is waiting: a callback that overruns the group's
//     rebalance timeout takes the member out of the group, which turns a
//     rebalance into an outage.
//  3. COMMIT the safe boundary SYNCHRONOUSLY. The safe boundary is the
//     tracker's watermark and not the highest offset seen: every offset below
//     the watermark has completed, and committing anything higher would tell
//     the member that takes the partition to skip records this one never
//     finished.
//  4. INVALIDATE the generation. Every delivery handed out before now is
//     stale, so a settlement that arrives afterwards reports ErrLeaseLost
//     rather than acknowledging a record the new owner is processing.
//
// Step 4 is what the tracker's generation is for, and it is why a partition's
// state is kept for the life of the source rather than deleted on revoke: a
// generation that restarted at one would make an old handle match a new
// assignment.
//
// [source.onLost] is the same sequence without step 3. A lost partition is one
// the coordinator has already taken away — the session expired, the member was
// fenced — and a commit for it either fails or overwrites the position of the
// member that now owns it. So the boundary is NOT committed, which means the
// records completed since the last commit are redelivered: that is the
// at-least-once window opening, it is reported through
// [Backend.LostPartitions], and it is the honest outcome rather than a
// dangerous commit.

// onAssigned records a new assignment.
//
// It creates no tracker: a partition's tracking window is based at the offset
// of its first record, which is the only number that is certainly the offset
// the broker is delivering from. An assignment with no records to follow it
// has nothing to commit and nothing to lose.
func (s *source) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, parts := range assigned {
		for _, partition := range parts {
			if p := s.parts[tp{topic, partition}]; p != nil {
				// The partition is back. Its state is kept so that its
				// generation keeps rising, and the re-base happens when
				// the first record arrives and says where the broker is
				// delivering from.
				p.stale = true
			}
		}
	}
}

// onRevoked is the sequence: stop, wait, commit, invalidate.
func (s *source) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.handleRevocation(ctx, revoked, true)
}

// onLost is the same sequence without the commit.
func (s *source) onLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	n := 0
	for _, parts := range lost {
		n += len(parts)
	}
	s.b.lost.Add(uint64(n))
	s.handleRevocation(ctx, lost, false)
}

// handleRevocation performs the sequence for one set of partitions.
func (s *source) handleRevocation(ctx context.Context, set map[string][]int32, mayCommit bool) {
	only := make(map[tp]bool, len(set))
	for topic, parts := range set {
		for _, partition := range parts {
			only[tp{topic, partition}] = true
		}
	}
	if len(only) == 0 {
		return
	}
	s.b.rebalances.Add(uint64(len(only)))

	// (1) Stop dispatch. Nothing more of these partitions is handed out,
	// and the redeliveries they were holding are dropped: the records
	// belong to whoever takes the partition next, and holding them here
	// would be this process redelivering a record another member owns.
	s.mu.Lock()
	held := 0
	for key := range only {
		p := s.parts[key]
		if p == nil {
			continue
		}
		// A revoked partition's deliveries are still settled during the
		// wait; a lost one's are not, because it is already somebody
		// else's and there is no commit for a settlement to reach.
		p.stale, p.draining = true, mayCommit
		held += len(p.held)
		p.held, p.backlog = nil, nil
	}
	s.mu.Unlock()

	// (2) Wait, bounded, for the in-flight deliveries to settle. On a
	// revoke they are still settled while it waits: refusing them would
	// leave nothing to wait for, and would leave the commit without records
	// that finished. A lost partition's ARE refused, so nothing would ever
	// end its wait but RevokeWait, and there is nothing for it to wait for.
	settled := true
	if mayCommit {
		settled = s.waitInflight(ctx, only)
	}

	// (3) Commit the safe boundary, synchronously.
	if mayCommit {
		offsets, parts := s.pendingCommits(only)
		if len(offsets) > 0 {
			cctx, cancel := s.revokeCtx(ctx)
			if err := s.commit(cctx, offsets, parts); err != nil {
				// The commit is the last chance to narrow the
				// at-least-once window for these partitions, so a
				// failure is worth reporting even though there is
				// nothing to do about it: the records are redelivered.
				s.b.health.Degraded(err)
			}
			cancel()
		}
	}

	// (4) Invalidate. Every handle from before this moment is stale.
	s.mu.Lock()
	for key := range only {
		p := s.parts[key]
		if p == nil {
			continue
		}
		p.tr.Reset(p.tr.Watermark())
		p.draining = false
		p.inflight = 0
		p.dirty = false
		clear(p.firstAt)
		if p.paused {
			// The pause is the client's own state and franz-go keeps it
			// per topic partition, so a partition paused when it was
			// revoked would come back paused and never be fetched
			// again.
			p.paused = false
			s.toFlush = append(s.toFlush, p.tp)
		}
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	s.flushPauses()
	if !settled {
		s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.group, errRevokeTimeout))
	}
	_ = held
}

// waitInflight waits, bounded, for the in-flight deliveries of these
// partitions to settle, and reports whether they all did.
//
// The bound is [Config.RevokeWait] and it is not optional. The coordinator is
// waiting for this callback: a member that waits longer than the group's
// rebalance timeout is removed from the group, and the partitions it was
// trying to hand over cleanly are taken from it instead — which is the
// OnPartitionsLost path, where the boundary cannot be committed at all. So
// overrunning the wait makes the outcome strictly worse than giving up on it.
func (s *source) waitInflight(ctx context.Context, only map[tp]bool) bool {
	deadline := s.b.now().Add(s.b.cfg.RevokeWait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	// sync.Cond has no deadline, so a waker converts the deadline into a
	// broadcast. It is one timer per revocation, not one per delivery,
	// which is the rule.
	stop := time.AfterFunc(time.Until(deadline), func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop.Stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.inflightLocked(only) == 0 {
			return true
		}
		if !s.b.now().Before(deadline) || s.closed {
			return s.inflightLocked(only) == 0
		}
		s.cond.Wait()
	}
}

// inflightLocked is how many deliveries of these partitions are outstanding.
func (s *source) inflightLocked(only map[tp]bool) int {
	n := 0
	for key := range only {
		if p := s.parts[key]; p != nil {
			n += p.inflight
		}
	}
	return n
}

// revokeCtx bounds the commit a revocation makes.
//
// franz-go hands the callback a context that is cancelled when the client
// closes, and a commit that inherited no deadline from it would wait for the
// rebalance timeout. The bound is the request timeout, which is well inside
// it.
func (s *source) revokeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || s.b.cfg.RequestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, s.b.cfg.RequestTimeout)
}

// resolve turns a settlement's token and generation back into the partition it
// names, and refuses one whose generation a rebalance has superseded.
//
// This is the driver's own half. The core already refuses a second
// settlement of one delivery; what it cannot know is that the partition has
// changed hands, and this is where that is caught.
func (s *source) resolve(d *mq.DriverDelivery) (*partState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, mq.ErrClosed
	}
	slot := int(d.Token)
	if slot < 0 || slot >= len(s.order) {
		return nil, errNotSettleable
	}
	p := s.order[slot]
	if (p.stale && !p.draining) || d.Gen != p.tr.Gen() {
		return nil, errLeaseLost
	}
	return p, nil
}

// kerrForCode is kerr.ErrorForCode with a nil for success, so a response's
// per-partition error code reads as an error or as nothing.
func kerrForCode(code int16) error {
	if code == 0 {
		return nil
	}
	return kerr.ErrorForCode(code)
}

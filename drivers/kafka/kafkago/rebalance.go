package kafkago

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "bitbucket.org/iqhive/iqmq"
)

// The generation loop is the hardest thing this driver does, and the order of
// operations in [source.endGeneration] is the whole for kafka-go.
//
// A rebalance can take a partition away while a handler still holds a delivery
// from it. From that moment two processes believe they own the same record, and
// exactly one of them may acknowledge it. The sequence that keeps that true:
//
//  1. STOP DISPATCH, so nothing more is handed out and nothing held is
//     redelivered.
//  2. STOP THE READERS and discard what they had already read but not handed
//     out. Those records belong to whoever takes the partition next.
//  3. WAIT, BOUNDED, for the in-flight deliveries to settle. Bounded because
//     the coordinator is waiting: kafka-go will not rejoin the group until
//     every function this generation started has returned, so a wait that
//     overruns the group's rebalance timeout takes the member out of the
//     group — which turns a rebalance into an outage.
//  4. COMMIT the safe boundary SYNCHRONOUSLY. The safe boundary is each
//     tracker's watermark and not the highest offset seen: every offset below
//     the watermark has completed, and committing anything higher would tell
//     the member that takes the partition to skip records this one never
//     finished.
//  5. INVALIDATE the generations. Every delivery handed out before now is
//     stale, so a settlement that arrives afterwards reports ErrLeaseLost
//     rather than acknowledging a record the new owner is processing.
//
// Step 5 is what the tracker's generation is for, and it is why a partition's
// state is kept for the life of the source rather than deleted when a
// generation ends: a generation that restarted at one would make an old handle
// match a new assignment.
//
// THREE THINGS ARE DIFFERENT FROM THE FRANZ-GO DRIVER, and all three come from
// the client rather than from a choice made here.
//
// First, the sequence runs for the WHOLE ASSIGNMENT and not for the partitions
// that moved. kafka-go has no cooperative assignor — range and round-robin are
// both eager — so every generation revokes everything and hands it back. A
// member that gains one partition therefore reports ErrLeaseLost for every
// delivery it was holding, where the franz-go driver keeps the ones whose
// partitions did not move. That is a real cost of the
// client, it is what TestCriticalRebalanceWithDeliveriesInFlight asserts, and
// it is on the driver page.
//
// Second, there is NO REVOKED-VERSUS-LOST DISTINCTION to be had. franz-go
// calls OnPartitionsRevoked when the handover is cooperative and
// OnPartitionsLost when the coordinator has already taken the partitions away,
// and commits only in the first case. kafka-go ends a generation the same way
// whichever it was — its heartbeat loop exits and the generation's context is
// cancelled — and does not report why. So this driver ALWAYS attempts the
// safe-boundary commit and reads the answer: a commit the coordinator refuses
// IS the lost case, and [Backend.LostPartitions] counts it.
// Attempting it is safe because a refused commit changes nothing; not
// attempting it would turn every cooperative handover into redelivery.
//
// Third, the commit is made through the ENDING generation, inside the function
// kafka-go is waiting for. `Generation.CommitOffsets` carries the generation
// and member id, and kafka-go closes the coordinator connection only after
// every started function has returned — so this is the last moment at which
// the boundary can be committed at all, and a driver that deferred the commit
// to the next generation would commit nothing.

// run is the generation loop: join, read, hand over, repeat.
func (s *source) run() {
	defer s.wg.Done()
	for {
		gen, err := s.group.Next(s.ctx)
		if err != nil {
			if errors.Is(err, kgo.ErrGroupClosed) || s.ctx.Err() != nil {
				return
			}
			// A join or sync failure. kafka-go's own loop backs off by
			// JoinGroupBackoff before it offers the next one, so this cannot
			// spin; what the driver owes the caller is that the failure is
			// visible.
			s.b.health.Degraded(s.b.errs.Map(mq.OpOpen, TopicOf(s.prefix, s.topic), s.groupID, err))
			continue
		}
		// One started function for the whole generation, not one per
		// partition. kafka-go ends a generation as soon as ANY started
		// function returns, and waits for all of them before it rejoins — so
		// a driver that started one per partition could not run the
		// sequence after the last reader stopped and before the next
		// generation began. With one, the sequence is simply what this
		// function does before it returns.
		gen.Start(func(ctx context.Context) { s.runGeneration(ctx, gen) })
	}
}

// runGeneration owns one consumer group generation from end to end.
func (s *source) runGeneration(ctx context.Context, gen *kgo.Generation) {
	assigned := s.beginGeneration(ctx, gen)
	var wg sync.WaitGroup
	for _, a := range gen.Assignments[s.topic] {
		wg.Add(1)
		go func(a kgo.PartitionAssignment) {
			defer wg.Done()
			s.readPartition(ctx, a)
		}(a)
	}
	<-ctx.Done()
	s.endGeneration(gen, assigned, &wg)
}

// beginGeneration records a new assignment and returns the partitions it
// covers.
//
// It creates no tracker: a partition's tracking window is based at the offset
// of its first record, which is the only number that is certainly the offset
// the broker is delivering from. An assignment with no records to follow it
// has nothing to commit and nothing to lose.
func (s *source) beginGeneration(ctx context.Context, gen *kgo.Generation) map[int]bool {
	assigned := make(map[int]bool, len(gen.Assignments[s.topic]))
	s.mu.Lock()
	s.cur = gen
	s.gens++
	first := s.gens == 1
	for _, a := range gen.Assignments[s.topic] {
		assigned[a.ID] = true
		if p := s.parts[a.ID]; p != nil {
			// The partition is back. Its state is kept so that its generation
			// keeps rising, and the re-base happens when the first record
			// arrives and says where the broker is delivering from.
			p.stale = true
		}
	}
	s.mu.Unlock()
	s.b.generationUp(ctx, first)
	return assigned
}

// endGeneration is the sequence: stop, drain, wait, commit, invalidate.
func (s *source) endGeneration(gen *kgo.Generation, assigned map[int]bool, readers *sync.WaitGroup) {
	// (1) Stop dispatch. Nothing more is handed out, and the redeliveries the
	// partitions were holding are dropped: the records belong to whoever takes
	// the partition next, and holding them here would be this process
	// redelivering a record another member owns.
	s.mu.Lock()
	for _, p := range s.order {
		p.stale = true
		p.held, p.backlog = nil, nil
	}
	s.cond.Broadcast()
	s.mu.Unlock()

	// (2) Stop the readers and discard what they had already read. The
	// generation tag on every queued record makes the discard sufficient even
	// for a Fetch running concurrently with this , and
	// draining here is what keeps the channel from handing the next
	// generation a record of this one.
	readers.Wait()
	s.drainRecs()

	// (3) Wait, bounded, for the in-flight deliveries to settle.
	settled := s.waitInflight(assigned)

	// (4) Commit the safe boundary, synchronously and through the ENDING
	// generation. A refusal here is the lost case: the coordinator has already
	// given the partitions away, so the records completed since the last
	// commit are redelivered.
	offsets, parts := s.pendingCommits(assigned)
	if len(offsets) > 0 {
		if err := s.commit(gen, offsets, parts); err != nil {
			s.b.lost.Add(uint64(len(offsets[s.topic])))
			s.b.health.Degraded(err)
		}
	}

	// (5) Invalidate. Every handle from before this moment is stale.
	s.mu.Lock()
	s.cur = nil
	n := 0
	for _, p := range s.order {
		if !assigned[p.partition] {
			continue
		}
		n++
		p.tr.Reset(p.tr.Watermark())
		p.inflight = 0
		p.dirty = false
		clear(p.firstAt)
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	s.b.rebalances.Add(uint64(n))
	if !settled {
		s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.groupID, errRevokeTimeout))
	}
	s.b.generationDown()
}

// drainRecs discards every record the readers had queued and nobody took.
func (s *source) drainRecs() {
	for {
		select {
		case <-s.recs:
		default:
			return
		}
	}
}

// readPartition reads one assigned partition for the life of the generation.
//
// It fetches with [kgo.Client.Fetch] and NOT with [kgo.Reader], and that is a
// correctness requirement rather than a preference. A Kafka record value is a
// nullable byte string and the specification makes the difference between a nil body and
// an empty one observable; `kgo.Reader` reads a record through kafka-go's
// legacy `Conn`/`Batch` path, whose `readNewBytes` returns nil for any length
// at or below zero, so it collapses a zero-length value and a null one into
// the same nil `Body`. The modern protocol path keeps them apart —
// `readFromVersion2` sets the value's page reference whenever the length is at
// or above zero, and `protocol.ReadAll` answers nil only for a nil reader — so
// the driver that wants to hand a caller back what it published has to fetch
// this way. It cost the `publish/roundtrip-bodies` scenario, which this
// driver failed until the read path moved.
//
// Fetching directly also puts the consume path on the same [kgo.Transport] as
// the producer and the administrative client, so the connection pool, the
// metadata cache ([Config.MetadataTTL]) and the TLS and SASL configuration are
// one thing rather than two. What the Reader provided and this loop provides
// for itself is a bounded read-ahead (the source's own record channel), a
// backoff on a failing partition (one fetch wait) and an explicit answer to
// OFFSET_OUT_OF_RANGE.
//
// A goroutine per partition is not a goroutine per message: one fetch returns
// a batch.
func (s *source) readPartition(ctx context.Context, a kgo.PartitionAssignment) {
	s.mu.Lock()
	gen := s.gens
	s.mu.Unlock()
	// The assignment's offset is a committed position, or kafka-go's
	// FirstOffset/LastOffset sentinel when the group has none. The sentinel is
	// resolved ONCE, here: `Fetch` would resolve it on every call, and a
	// LastOffset re-resolved on each fetch tracks the moving end of the log
	// and skips everything published in between.
	//
	// And it is resolved to the position the subscription was OPENED at when
	// there is one. The commit that normally makes that position the group's
	// is refused by a live group -- the same subscription name on another
	// topic is enough -- and LastOffset resolved now, at assignment, skips
	// whatever the caller published since Consumer returned.
	want := a.Offset
	if at, has := s.startAt[a.ID]; has && want < 0 {
		want = at
	}
	off, ok := s.resolveOffset(ctx, a.ID, want)
	if !ok {
		return
	}
	for ctx.Err() == nil {
		resp, err := s.b.cl.Fetch(ctx, &kgo.FetchRequest{
			Topic:          s.topic,
			Partition:      a.ID,
			Offset:         off,
			MinBytes:       int64(s.b.cfg.FetchMinBytes),
			MaxBytes:       int64(s.b.cfg.FetchMaxBytes),
			MaxWait:        s.b.cfg.FetchMaxWait,
			IsolationLevel: isolation(s.b.cfg),
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.groupID, err))
			if !s.backoff(ctx) {
				return
			}
			continue
		}
		next, fatal := s.absorbFetch(ctx, resp, a.ID, off, gen)
		closeRecords(resp)
		if fatal {
			return
		}
		if resp.Error != nil {
			if errors.Is(resp.Error, kgo.OffsetOutOfRange) {
				// The position is outside the log: compaction, a retention
				// sweep, or a topic deleted and recreated under the group.
				// the answer for an out-of-range position is the log's own
				// range, and the earliest offset is the one that loses
				// nothing that is still there.
				if off, ok = s.resolveOffset(ctx, a.ID, kgo.FirstOffset); !ok {
					return
				}
				continue
			}
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.groupID, resp.Error))
			if !s.backoff(ctx) {
				return
			}
			continue
		}
		off = next
	}
}

// absorbFetch hands one fetch response's records to the source, and returns
// the offset to fetch from next and whether the generation is over.
//
// A record at or below `from` is DISCARDED rather than handed out: kafka-go
// documents that "kafka may return record batches that start at an offset
// before the one that was requested", and the offset tracker requires
// strictly increasing offsets — so handing one out would be a duplicate the
// tracker then refuses.
func (s *source) absorbFetch(ctx context.Context, resp *kgo.FetchResponse, partition int, from int64, gen uint64) (int64, bool) {
	next := from
	for {
		rec, rerr := resp.Records.ReadRecord()
		if rec != nil {
			if rec.Offset >= next {
				next = rec.Offset + 1
			}
			if rec.Offset >= from {
				msg, cerr := MessageFrom(s.topic, partition, resp.HighWatermark, rec)
				if cerr != nil {
					// A record whose key or value could not be read out of
					// the response is a corrupt fetch, not a settlement: the specification
					// makes it a background failure and the offset moves on,
					// because stopping here would stall the partition for
					// ever.
					s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.groupID, cerr))
				} else {
					select {
					case s.recs <- fetched{msg: msg, gen: gen}:
					case <-ctx.Done():
						return next, true
					}
				}
			}
		}
		if rerr != nil {
			if !errors.Is(rerr, io.EOF) {
				s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.groupID, rerr))
			}
			return next, false
		}
	}
}

// closeRecords releases the page buffers a fetch response's records point at.
//
// [kgo.RecordReader] does not declare Close — the interface is one method —
// but kafka-go's own documentation says "the program is expected to call the
// RecordSet's Close method when it finished reading the records", and the
// concrete reader implements [io.Closer]. Not closing it holds the response's
// pages until the garbage collector reaches them, which on a busy partition is
// the whole fetch size per fetch.
func closeRecords(resp *kgo.FetchResponse) {
	if resp == nil || resp.Records == nil {
		return
	}
	if c, ok := resp.Records.(io.Closer); ok {
		_ = c.Close()
	}
}

// resolveOffset turns an assignment's offset into a concrete one, resolving
// kafka-go's FirstOffset and LastOffset sentinels against the log's own range.
func (s *source) resolveOffset(ctx context.Context, partition int, want int64) (int64, bool) {
	if want >= 0 {
		return want, true
	}
	bounds, err := s.b.logBounds(ctx, s.topic, []int{partition})
	if err != nil {
		if ctx.Err() == nil {
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, s.topic), s.groupID, err))
		}
		return 0, false
	}
	lb, ok := bounds[partition]
	if !ok || lb.first < 0 || lb.last < 0 {
		return 0, false
	}
	if want == kgo.FirstOffset {
		return lb.first, true
	}
	return lb.last, true
}

// backoff waits one fetch wait before a failing partition is fetched again,
// and reports whether the generation is still live.
//
// The unit is [Config.FetchMaxWait] because that is what a healthy fetch of an
// empty partition already costs: a partition that is answering errors then
// costs no more requests than one that is answering nothing, and there is no
// new knob for it.
func (s *source) backoff(ctx context.Context) bool {
	t := time.NewTimer(s.b.cfg.FetchMaxWait)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// isolation is the read isolation one partition reader uses.
func isolation(cfg Config) kgo.IsolationLevel {
	if cfg.ReadCommitted {
		return kgo.ReadCommitted
	}
	return kgo.ReadUncommitted
}

// waitInflight waits, bounded, for the in-flight deliveries of these
// partitions to settle, and reports whether they all did.
//
// The bound is [Config.RevokeWait] and it is not optional. kafka-go will not
// rejoin the group until this function returns, and a member that takes longer
// than the group's rebalance timeout is removed from the group — so
// overrunning the wait makes the outcome strictly worse than giving up on it:
// a removed member cannot commit its boundary at all.
func (s *source) waitInflight(assigned map[int]bool) bool {
	deadline := s.b.now().Add(s.b.cfg.RevokeWait)
	// sync.Cond has no deadline, so a waker converts the deadline into a
	// broadcast. It is one timer per generation, not one per delivery, which
	// is the rule.
	stop := time.AfterFunc(time.Until(deadline), func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop.Stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.inflightLocked(assigned) == 0 {
			return true
		}
		if !s.b.now().Before(deadline) || s.closed {
			return s.inflightLocked(assigned) == 0
		}
		s.cond.Wait()
	}
}

// inflightLocked is how many deliveries of these partitions are outstanding.
func (s *source) inflightLocked(assigned map[int]bool) int {
	n := 0
	for id := range assigned {
		if p := s.parts[id]; p != nil {
			n += p.inflight
		}
	}
	return n
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
	if p.stale || d.Gen != p.tr.Gen() {
		return nil, errLeaseLost
	}
	return p, nil
}

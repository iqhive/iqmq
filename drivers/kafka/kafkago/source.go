package kafkago

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// The source implements the mandatory consumption surface, and its settler
// implements the batched one.
var _ mq.Source = (*source)(nil)

// partState is everything this source knows about one assigned partition.
//
// One partition is owned by one reader goroutine, and every field here is read
// and written under the source's own mutex, which is what lets the tracker be
// a non-concurrent type.
type partState struct {
	partition int
	// tr is the partition's offset tracker (ADR-019). Its
	// generation is the delivery generation the specification makes stale settlements
	// detectable with: the end of a consumer group generation calls Reset,
	// which bumps it.
	tr offset.Tracker
	// slot is this partition's index in the source's stable slice, and is the
	// DriverDelivery.Token every delivery of this partition carries.
	slot int
	// inflight is how many deliveries of this partition have been handed out
	// and not settled. It is what the end of a generation waits on.
	inflight int
	// held are the records a block-partition nack put back, in the order they
	// were held, each with the time it becomes deliverable again.
	held []heldRecord
	// backlog are records this source polled and could not track, because the
	// partition's window was full. They are handed out on a later fetch
	// rather than re-read, which is what keeps the tracker from ever seeing
	// an offset twice.
	backlog []*kgo.Message
	// stale reports that this partition's assignment has ended or has just
	// begun again, so nothing of it is dispatched or settled until its next
	// record says where the broker is delivering from. It is what stops a
	// delivery from the previous generation being settled against this one
	stale bool
	// dirty reports that the watermark has advanced since the last commit.
	dirty bool
	// committed is the watermark the last successful commit carried.
	committed int64
	// firstAt is when the oldest in-flight record was first delivered, which
	// the iqmq-first-delivered-at header reports.
	firstAt map[int64]time.Time
}

// heldRecord is a record a block-partition nack put back into the partition.
type heldRecord struct {
	msg     *kgo.Message
	due     time.Time
	attempt uint32
}

// fetched is one record a partition reader has handed to the source, tagged
// with the generation it was read under.
//
// The tag is not decoration. A record can sit in the channel while its
// generation ends, and a Fetch that absorbed it afterwards would hand the
// NEXT generation's tracker an offset from the previous assignment — which
// re-bases the tracker at the wrong place and invalidates handles that are
// still live. So a record whose generation is not the current one is dropped:
// it belongs to whoever takes the partition next.
type fetched struct {
	msg *kgo.Message
	gen uint64
}

// source is one open subscription: its own consumer group, one reader per
// assigned partition, one tracker per assigned partition, and the commit loop.
type source struct {
	b        *Backend
	group    *kgo.ConsumerGroup
	topic    string // the Kafka topic, with the configured prefix
	groupID  string
	prefix   string
	meta     metadataMode
	strategy nack.Strategy
	cfg      mq.SourceConfig
	trcfg    offset.Config

	// ctx is cancelled by Close, which is how the specification's "Close unblocks blocked
	// operations" reaches a parked Fetch.
	ctx    context.Context
	cancel context.CancelFunc

	// recs carries records from the per-partition fetch loops to Fetch. It is
	// bounded at the larger of the caller's prefetch and
	// [Config.QueueCapacity], which is the specification's "all queues bounded" and is
	// also how a partition is paused: a fetch loop whose records nobody is
	// consuming blocks on the push, and kafka-go has no pause to call.
	recs *recQueue

	mu    sync.Mutex
	cond  *sync.Cond
	parts map[int]*partState
	order []*partState
	// cur is the live consumer group generation, and nil between generations.
	// It is what a commit needs: kafka-go's OffsetCommit carries the
	// generation and member id, so an offset simply cannot be committed while
	// this is nil — the coordinator would refuse it.
	cur *kgo.Generation
	// gens counts the generations this source has been through, so that the
	// first is told apart from a rejoin. It is also the tag every record
	// carries, which is what lets a record left over from an ending
	// generation be told from a record of the new one.
	gens uint64
	// stopped is the reason Fetch refuses, set by a stop-the-consumer nack.
	stopped error
	// startAt is the start position resolved when the subscription was
	// opened, per partition the group had no committed offset for. It is
	// written before the group exists and only read afterwards, by
	// readPartition.
	startAt map[int]int64

	// commitMu serialises the commits, which the ticker, a caller's
	// Backend.Commit and the end of a generation all make.
	commitMu sync.Mutex

	closeOnce sync.Once
	closed    bool
	wg        sync.WaitGroup
}

// newSource builds the consumer group and starts the generation loop.
func (b *Backend) newSource(ctx context.Context, cfg mq.SourceConfig, topic string, meta metadataMode) (*source, error) {
	strategy, err := nack.New(b.cfg.Nack, b.cfg.NackConfig)
	if err != nil {
		return nil, err
	}
	s := &source{
		b: b, topic: topic, groupID: cfg.Subscription, prefix: b.cfg.TopicPrefix,
		meta: meta, strategy: strategy, cfg: cfg,
		trcfg: b.trackerFor(cfg.Prefetch),
		parts: map[int]*partState{},
		recs:  newRecQueue(max(cfg.Prefetch, b.cfg.QueueCapacity)),
	}
	s.cond = sync.NewCond(&s.mu)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	// A Kafka group has a durable position, and this is what makes it exist
	// before the first record is consumed: the resolved start position is
	// committed for every partition that has none, so a subscription that was
	// opened and closed without consuming anything still remembers where it
	// was. It is also the ONLY way a start position by time or by sequence can
	// be honoured at all on this client, because kafka-go's group start offset
	// is FirstOffset or LastOffset and nothing else.
	resolved, err := b.ensureGroupPosition(ctx, topic, cfg.Subscription, cfg.Start.Resolve())
	if err != nil {
		s.cancel()
		return nil, err
	}
	for _, c := range resolved {
		if s.startAt == nil {
			s.startAt = make(map[int]int64, len(resolved))
		}
		s.startAt[c.Partition] = c.Offset
	}
	gcfg := kgo.ConsumerGroupConfig{
		ID:                     cfg.Subscription,
		Brokers:                b.cfg.Brokers,
		Dialer:                 b.dialer,
		Topics:                 []string{topic},
		GroupBalancers:         groupBalancers(b.cfg),
		HeartbeatInterval:      b.cfg.HeartbeatInterval,
		SessionTimeout:         b.cfg.SessionTimeout,
		RebalanceTimeout:       b.cfg.RebalanceTimeout,
		JoinGroupBackoff:       b.cfg.JoinBackoff,
		PartitionWatchInterval: b.cfg.PartitionWatchInterval,
		WatchPartitionChanges:  b.cfg.WatchPartitionChanges,
		StartOffset:            startOffset(cfg.Start.Resolve()),
		Timeout:                b.cfg.RequestTimeout,
		// RetentionTime is left zero, which kafka-go resolves to its own -1ms
		// sentinel meaning "the broker's offsets.retention.minutes". Writing
		// -1 here would be minus one NANOSECOND, which kafka-go's own
		// Validate refuses with "RetentionTime out of bounds" — and refuses
		// it when the consumer group is built, which is the first thing a
		// caller's Consumer call does.
	}
	group, err := kgo.NewConsumerGroup(gcfg)
	if err != nil {
		s.cancel()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	s.group = group
	s.wg.Add(1)
	// The loop's context is the SOURCE's, not the caller's: it outlives the
	// call that opened the source and is cancelled by Close, which is what
	// makes Close able to stop it. Passing the opening context would tie a
	// background loop to a construction call's deadline.
	go s.run()
	if b.cfg.CommitInterval > 0 {
		s.wg.Add(1)
		go s.commitLoop()
	}
	return s, nil
}

// trackerFor is the tracking window one partition gets.
//
// The pause thresholds default to twice the prefetch rather than to the
// window, which is the difference between a pause that protects the driver and
// one that never fires: the window's own default is 65,536 offsets, and a
// partition with 65,536 uncompleted deliveries has already cost more memory
// than any prefetch asked for. Twice the prefetch is what a caller who took a
// batch and has not settled it yet is holding.
func (b *Backend) trackerFor(prefetch int) offset.Config {
	cfg := b.cfg.trackerConfig(0)
	if cfg.MaxPending == 0 {
		cfg.MaxPending = 2 * prefetch
	}
	if cfg.Window == 0 {
		cfg.Window = offset.DefaultWindow
	}
	if cfg.MaxPending > cfg.Window {
		cfg.MaxPending = cfg.Window
	}
	return cfg
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The order it looks in is the order that keeps a partition's records in
// order: the records a nack is holding for redelivery first, then the records
// a full tracking window made it keep back, then the readers. A fetch that
// went to the readers first would hand out a later offset before the earlier
// one it is already holding.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		if err := s.state(ctx); err != nil {
			return 0, err
		}
		if n := s.takeLocal(dst); n > 0 {
			return n, nil
		}
		m, err := s.poll(ctx, dst)
		if err != nil {
			return 0, err
		}
		if m > 0 {
			return m, nil
		}
		// Nothing came back: either the context is done, the source is
		// closing, or a held record became due while the poll waited. The
		// loop re-checks all three.
	}
}

// poll takes one record from the readers and then drains whatever else is
// already queued, which is the specification's "one blocking wait then a non-blocking drain".
//
// The drain is what makes a batch a batch. kafka-go's Reader serves ONE record
// per FetchMessage — there is no "give me everything you have buffered" call at
// all — so a driver that returned after the first record would fill one slot
// of a caller's batch and look as though it could not batch, which is what the BatchReceive scenario catches. The per-partition readers do the
// buffering instead, into a queue this drains without blocking.
func (s *source) poll(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	var (
		timer *time.Timer
		wake  <-chan time.Time
	)
	if due, ok := s.nextDue(); ok {
		// A held record is waiting: the wait must not outlast it, or a
		// partition whose only work is a redelivery would wait for a record
		// that never comes instead of for the delay the nack asked for.
		timer = time.NewTimer(time.Until(due))
		defer timer.Stop()
		wake = timer.C
	}
	first, ok := s.recs.tryPop(false)
	for !ok {
		select {
		case <-s.recs.ready:
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.ctx.Done():
			return 0, mq.ErrClosed
		case <-wake:
			return 0, nil
		}
		first, ok = s.recs.tryPop(true)
	}
	n, err := s.absorb(first, dst, 0)
	for err == nil && n < len(dst) {
		f, ok := s.recs.tryPop(false)
		if !ok {
			return n, nil
		}
		n, err = s.absorb(f, dst, n)
	}
	return n, err
}

// absorb turns one polled record into a delivery, or into the partition's
// backlog, tracking its offset as it goes.
//
// The tracker is what decides which: a partition whose window is full keeps
// its remaining records in the source's own backlog, so that the tracker never
// sees an offset twice. Re-reading them instead would mean rewinding the
// partition, which would mean resetting the tracker, which would invalidate
// the generation of every delivery of that partition that is still in flight —
// a rebalance's worth of ErrLeaseLost for a full window.
func (s *source) absorb(f fetched, dst []mq.DriverDelivery, n int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.gen != s.gens {
		// A record from a generation that has ended. It is not this member's
		// to hand out any more.
		return n, nil
	}
	p, err := s.ensurePartLocked(f.msg)
	if err != nil {
		return n, err
	}
	if p == nil {
		return n, nil
	}
	if n >= len(dst) || p.tr.Paused() || len(p.backlog) > 0 {
		// Behind a backlog the record must queue rather than overtake: the
		// tracker requires strictly increasing offsets, so handing this one
		// out would make the backlog untrackable for ever.
		p.backlog = append(p.backlog, f.msg)
		return n, nil
	}
	switch s.trackLocked(p, f.msg) {
	case tracked:
		dst[n] = s.deliveryLocked(p, f.msg, 1)
		return n + 1, nil
	case dropped:
		// An offset this tracker has already resolved: the broker is
		// redelivering after an uncommitted rebalance and the record is not
		// news.
		return n, nil
	case deferred:
		p.backlog = append(p.backlog, f.msg)
		return n, nil
	default:
		return n, nil
	}
}

// takeLocal hands out what the source is already holding: the redeliveries a
// nack is holding whose delay has passed, and then the backlog.
func (s *source) takeLocal(dst []mq.DriverDelivery) int {
	now := s.b.now()
	n := 0
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.order {
		if p.stale {
			continue
		}
		for len(p.held) > 0 && n < len(dst) {
			h := p.held[0]
			if h.due.After(now) {
				break
			}
			p.held = p.held[1:]
			// The offset is already in the tracker: a redelivery of a record
			// whose offset never completed must NOT be tracked again, because
			// Track requires strictly increasing offsets and would refuse it
			// as out of order.
			dst[n] = s.deliveryLocked(p, h.msg, h.attempt)
			n++
		}
		for len(p.backlog) > 0 && n < len(dst) && !p.tr.Paused() {
			msg := p.backlog[0]
			out := s.trackLocked(p, msg)
			if out == deferred {
				break
			}
			p.backlog = p.backlog[1:]
			if out == dropped {
				continue
			}
			dst[n] = s.deliveryLocked(p, msg, 1)
			n++
		}
	}
	return n
}

// nextDue is when the earliest held redelivery becomes deliverable.
func (s *source) nextDue() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due time.Time
	for _, p := range s.order {
		if p.stale || len(p.held) == 0 {
			continue
		}
		if due.IsZero() || p.held[0].due.Before(due) {
			due = p.held[0].due
		}
	}
	return due, !due.IsZero()
}

// ensurePartLocked returns the state of a partition, creating or re-basing it
// when the generation is new.
//
// The re-base is the half that matters. A partition this source has owned
// before keeps its state — and therefore its monotonically increasing tracker
// generation — so that a delivery handed out under a previous generation can
// never be mistaken for one handed out under this generation, whatever the
// offsets happen to be.
func (s *source) ensurePartLocked(msg *kgo.Message) (*partState, error) {
	p := s.parts[msg.Partition]
	first := msg.Offset
	if p == nil {
		tr, err := offset.New(s.b.cfg.Tracker, s.trackerBase(first))
		if err != nil {
			return nil, fmt.Errorf("kafkago: offset tracker for partition %d: %w", msg.Partition, err)
		}
		p = &partState{
			partition: msg.Partition,
			tr:        tr,
			slot:      len(s.order),
			firstAt:   map[int64]time.Time{},
			committed: first,
		}
		s.parts[msg.Partition] = p
		s.order = append(s.order, p)
		return p, nil
	}
	if p.stale {
		// The partition has come back. Re-basing at the offset the broker is
		// delivering from bumps the tracker's generation, so every handle from
		// the previous generation is stale for good — which is why this state
		// is kept for the life of the source rather than deleted when a
		// generation ends: a generation that restarted at one would make an
		// old handle match a new assignment.
		p.tr.Reset(first)
		p.stale = false
		p.backlog, p.held = nil, nil
		p.committed, p.dirty = first, false
		clear(p.firstAt)
	}
	return p, nil
}

// trackerBase is the tracking window based at off.
func (s *source) trackerBase(off int64) offset.Config {
	cfg := s.trcfg
	cfg.Base = off
	if cfg.Base < 0 {
		cfg.Base = 0
	}
	return cfg
}

// trackOutcome is what happened to one record the tracker was offered.
type trackOutcome uint8

const (
	// tracked: the offset is in flight and the record may be handed out.
	tracked trackOutcome = iota
	// dropped: the offset is one this tracker has already resolved, so the
	// record is a redelivery of something finished and is not news.
	dropped
	// deferred: the tracking window has no room, so the record is kept for a
	// later fetch.
	deferred
)

// trackLocked records one delivered offset and says what happened to it.
//
// The three answers are genuinely different and conflating any two of them is
// a defect: dropping a deferred record loses it, deferring a dropped one
// stalls the partition on a record that will never be trackable, and handing
// out either is a duplicate.
func (s *source) trackLocked(p *partState, msg *kgo.Message) trackOutcome {
	err := p.tr.Track(p.tr.Gen(), msg.Offset)
	switch {
	case err == nil:
		return tracked
	case errors.Is(err, offset.ErrOutOfOrder):
		return dropped
	case errors.Is(err, offset.ErrWindowFull):
		return deferred
	default:
		// A stale generation here would mean the tracker was reset between
		// the read and this call, which is possible on this client — the
		// readers run on the generation's goroutines and a record may be in
		// the channel when the generation ends. Dropping it is right: the
		// record belongs to whoever takes the partition next.
		return dropped
	}
}

// deliveryLocked builds the DriverDelivery for one record.
//
// Token is the partition's slot and Gen is the tracker's generation, which
// together are what a settlement is resolved through: a token names the
// partition for as long as the source lives, and a generation names the
// assignment. Native is the record itself, which is what a retry-topic
// republication needs and what the escape hatch surfaces.
func (s *source) deliveryLocked(p *partState, msg *kgo.Message, attempt uint32) mq.DriverDelivery {
	m := &mq.Message{}
	switch s.meta {
	case metaEnvelope, metaAdaptive:
		// The two modes differ in ONE place: adaptive asks whether this
		// record is an envelope and portable already knows that it is. What
		// they must not differ in is what happens to a record that IS one,
		// so the decode and its failure are shared.
		var raw mq.Message
		Decode(&raw, msg, s.prefix)
		// The marker and never the body: a native value that happens to be
		// a valid envelope is a payload, and decoding it as metadata would
		// hand the application bytes nobody published.
		if s.meta == metaAdaptive && !Enveloped(msg) {
			*m = raw
			break
		}
		if err := s.b.env.Unwrap(m, &raw); err != nil {
			// The record says it is an envelope and it will not decode, so
			// it is corrupt or written under wider limits than this
			// consumer accepts. There is no settlement that can express
			// "undecodable", so the specification makes it a background failure and the
			// record is surfaced as it arrived — which is the honest answer
			// only because it is REPORTED: a marked record delivered as its
			// own bytes with nothing counting it is the mis-decode this
			// marker exists to prevent, wearing the other hat.
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, msg.Topic), s.groupID, err))
			*m = raw
		} else {
			m.Topic = raw.Topic
		}
	case metaStrict, metaDrop:
		Decode(m, msg, s.prefix)
	}
	if a := Attempt(msg); a > attempt {
		attempt = a
	}
	now := s.b.now()
	if _, ok := p.firstAt[msg.Offset]; !ok {
		p.firstAt[msg.Offset] = now
	}
	p.inflight++
	return mq.DriverDelivery{
		Message: m,
		Info: mq.DeliveryInfo{
			Topic:        m.Topic,
			Source:       msg.Topic,
			Subscription: s.groupID,
			ReceivedAt:   now,
			Attempt:      attempt,
			Redelivered:  attempt > 1,
			Partition:    int32(msg.Partition),
			HasPartition: true,
			Offset:       msg.Offset,
			HasOffset:    true,
			// LeaseExpires is the zero time and Sequence is unset: the specification makes
			// both "the broker does not say", which is the truth here. Kafka
			// grants no per-record lease and its sequence IS the offset.
		},
		Token:  uint64(p.slot),
		Native: msg,
		Gen:    p.tr.Gen(),
	}
}

// state reports whether this source may hand out anything at all.
func (s *source) state(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closed:
		return mq.ErrClosed
	case s.stopped != nil:
		return s.stopped
	default:
		return nil
	}
}

// Settler settles this source's deliveries.
func (s *source) Settler() mq.Settler { return &settler{s: s} }

// Close stops the consumer, commits what is safe, wakes a parked Fetch and
// abandons everything it still held. It is idempotent.
//
// The order is the one that gets the last commit made. Closing the consumer
// group makes kafka-go end the live generation and WAIT for the functions that
// generation started — which is where this driver's own sequence runs, so
// the final commit happens inside group.Close() while the coordinator
// connection is still open. Cancelling the source's context first would kill
// the generation loop before that, and everything completed since the last
// tick would be redelivered.
func (s *source) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()
		if s.group != nil {
			err = s.group.Close()
		}
		s.cancel()
		s.wg.Wait()
		s.b.forget(s, nil)
	})
	return err
}

// commitLoop commits the watermark every CommitInterval.
//
// It is what makes the at-least-once window a bounded, configurable thing
// rather than "until the process ends". A commit per completion would cost a
// round trip per record; a commit only at Close would redeliver a whole
// process's worth of work.
func (s *source) commitLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.b.cfg.CommitInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			if err := s.commitNow(s.ctx); err != nil && !errors.Is(err, mq.ErrClosed) {
				s.b.health.Degraded(err)
			}
		}
	}
}

// commitNow commits every partition whose watermark has advanced, and waits
// for the answer.
//
// A source with no live generation commits nothing, and that is not a
// shortcoming to work around: kafka-go's OffsetCommit carries the generation
// and the member id, so an offset committed between generations would be
// refused by the coordinator — or, worse, accepted for a generation this
// member no longer holds.
func (s *source) commitNow(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	gen := s.cur
	s.mu.Unlock()
	if gen == nil {
		return nil
	}
	offsets, parts := s.pendingCommits(nil)
	if len(offsets) == 0 {
		return nil
	}
	return s.commit(gen, offsets, parts)
}

// pendingCommits collects the offsets a commit would carry. With only non-nil
// it collects just those partitions, which is what the end of a generation
// needs.
func (s *source) pendingCommits(only map[int]bool) (map[string]map[int]int64, []*partState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		offsets map[string]map[int]int64
		parts   []*partState
	)
	for _, p := range s.order {
		if !p.dirty || (only != nil && !only[p.partition]) {
			continue
		}
		wm := p.tr.Watermark()
		if wm <= p.committed {
			continue
		}
		if offsets == nil {
			offsets = map[string]map[int]int64{}
		}
		if offsets[s.topic] == nil {
			offsets[s.topic] = map[int]int64{}
		}
		offsets[s.topic][p.partition] = wm
		parts = append(parts, p)
	}
	return offsets, parts
}

// commit performs one synchronous offset commit and records what it carried.
//
// [kgo.Generation.CommitOffsets] takes no context: the round trip is bounded by
// the coordinator connection's own timeout, which is
// [Config.RequestTimeout]. So a caller's deadline does not shorten a commit
// here, which is recorded in the maintenance notes.
func (s *source) commit(gen *kgo.Generation, offsets map[string]map[int]int64, parts []*partState) error {
	s.commitMu.Lock()
	err := gen.CommitOffsets(offsets)
	s.commitMu.Unlock()
	if err != nil {
		if errors.Is(err, kgo.ErrGroupClosed) {
			return mq.ErrClosed
		}
		return s.b.errs.Map(mq.OpAck, TopicOf(s.prefix, s.topic), s.groupID, err)
	}
	s.b.commits.Add(1)
	s.mu.Lock()
	for _, p := range parts {
		if o, ok := offsets[s.topic][p.partition]; ok {
			p.committed = o
			p.dirty = p.tr.Watermark() > o
		}
	}
	s.mu.Unlock()
	return nil
}

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// The source implements the mandatory consumption surface, and its settler
// implements the batched one.
var _ mq.Source = (*source)(nil)

// tp names one topic partition.
type tp struct {
	topic     string
	partition int32
}

// partState is everything this source knows about one assigned partition.
//
// One partition is owned by one fetch loop, which is what lets the tracker be
// a non-concurrent type: every field here is read and written under the
// source's own mutex.
type partState struct {
	tp tp
	// tr is the partition's offset tracker (ADR-019). Its
	// generation is the delivery generation the specification makes stale settlements
	// detectable with: a revoke or a loss calls Reset, which bumps it.
	tr offset.Tracker
	// slot is this partition's index in the source's stable slice, and is
	// the DriverDelivery.Token every delivery of this partition carries.
	slot int
	// inflight is how many deliveries of this partition have been handed
	// out and not settled. It is what OnPartitionsRevoked waits on.
	inflight int
	// held are the records a block-partition nack put back, in the order
	// they were held, each with the time it becomes deliverable again.
	held []heldRecord
	// backlog are records this source polled and could not track, because
	// the partition's window was full. They are handed out on a later
	// fetch rather than re-fetched, which is what keeps the tracker from
	// ever seeing an offset twice.
	backlog []*kgo.Record
	// paused reports whether the partition is paused at the client.
	paused bool
	// stale reports that this partition's assignment has ended or has just
	// begun again, so nothing of it is dispatched or settled until its
	// next record says where the broker is delivering from. It is what
	// stops a delivery from the previous assignment being settled against
	// this one.
	stale bool
	// draining reports that a revocation has stopped dispatch for this
	// partition and is waiting for its in-flight deliveries. They are still
	// settled — the wait is for exactly that — until the revocation reads
	// the boundary it commits ([source.lastBoundary]).
	draining bool
	// dirty reports that the watermark has advanced since the last commit.
	dirty bool
	// committed is the watermark the last successful commit carried.
	committed int64
	// epoch is the leader epoch of the newest record seen, which a commit
	// carries so that the broker can detect a truncation.
	epoch int32
	// firstAt is when the oldest in-flight record was first delivered,
	// which the iqmq-first-delivered-at header reports.
	firstAt map[int64]time.Time
}

// heldRecord is a record a block-partition nack put back into the partition.
type heldRecord struct {
	rec     *kgo.Record
	due     time.Time
	attempt uint32
	// pause is the plan's Pause: nothing later of the partition is handed
	// out until this record has been handed out again.
	pause bool
}

// source is one open subscription: its own group client, one tracker per
// assigned partition, and the commit loop.
type source struct {
	b        *Backend
	cl       *kgo.Client
	topic    string // the Kafka topic, with the configured prefix
	group    string
	prefix   string
	meta     metadataMode
	strategy nack.Strategy
	cfg      mq.SourceConfig
	trcfg    offset.Config

	// ctx is cancelled by Close, which is how the specification's "Close unblocks
	// blocked operations" reaches a parked poll.
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	cond  *sync.Cond
	parts map[tp]*partState
	order []*partState
	// toFlush are the partitions whose client-side pause a locked section
	// changed. flushPauses applies each one's paused flag as it stands when
	// the flush runs, with the mutex NOT held: a franz-go call made under
	// s.mu could deadlock against a rebalance callback that is waiting for
	// it.
	toFlush []tp
	// flushMu orders the flushes. A flush reads the decisions under s.mu
	// and applies them after it, so two flushes left unordered could apply
	// an older decision last and leave the client paused for good.
	flushMu sync.Mutex
	// commitMu orders the commits the same way, for the same reason: see
	// [source.stillAhead].
	commitMu sync.Mutex
	// stopped is the reason Fetch refuses, set by a stop-the-consumer nack.
	stopped error
	// wakePoll cancels the poll that is waiting in PollFetches, and is nil
	// when none is. A settlement that frees local work calls it: the waiting
	// poll is the only thing that would hand that work out, and a partition
	// with a backlog stays paused, so the broker never ends the wait itself.
	wakePoll context.CancelFunc
	// woken records a wake-up that came while no poll was waiting, so the
	// next poll returns at once instead of waiting past it. takeLocal clears
	// it: it sees everything a settlement before it freed.
	woken bool

	// startAt is the start position resolved when the subscription was
	// opened, per partition the group had no committed offset for, and
	// reset is the offset franz-go substitutes for a partition with no
	// commit. Both are written before the group client exists and only
	// read afterwards, by adjustStart.
	startAt map[int32]int64
	reset   kgo.Offset

	closeOnce sync.Once
	closed    bool
	wg        sync.WaitGroup
}

// newSource builds the group client and starts the commit loop.
func (b *Backend) newSource(ctx context.Context, cfg mq.SourceConfig, topic string, meta metadataMode) (*source, error) {
	strategy, err := nack.New(b.cfg.Nack, b.cfg.NackConfig)
	if err != nil {
		return nil, err
	}
	s := &source{
		b: b, topic: topic, group: cfg.Subscription, prefix: b.cfg.TopicPrefix,
		meta: meta, strategy: strategy, cfg: cfg,
		trcfg: b.trackerFor(cfg.Prefetch),
		parts: map[tp]*partState{},
	}
	s.cond = sync.NewCond(&s.mu)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	// A Kafka group has a durable position, and this is what makes it exist
	// before the first record is consumed: the resolved start position is
	// committed for every partition that has none, so a subscription that
	// was opened and closed without consuming anything still remembers
	// where it was.
	start := cfg.Start.Resolve()
	resolved, err := b.ensureGroupPosition(ctx, topic, cfg.Subscription, start)
	if err != nil {
		s.cancel()
		return nil, err
	}
	s.reset = resetOffset(start)
	// Kept for every start kind, so that a live group starts exactly where
	// an empty one would have had committed for it: "latest" as of now
	// rather than as of an assignment that may land after the caller has
	// published, and a sequence clamped into the log as it was when the
	// subscription was opened.
	for _, o := range resolved {
		if o.Topic != topic {
			continue
		}
		if s.startAt == nil {
			s.startAt = make(map[int32]int64, len(resolved))
		}
		s.startAt[o.Partition] = o.At
	}
	opts, err := b.groupOptions(s, cfg, topic)
	if err != nil {
		s.cancel()
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		s.cancel()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	s.cl = cl
	if b.cfg.CommitInterval > 0 {
		s.wg.Add(1)
		// The loop's context is the SOURCE's, not the caller's: it
		// outlives the call that opened the source and is cancelled by
		// Close, which is what makes Close able to stop it. Passing the
		// opening context would tie a background loop to a construction
		// call's deadline.
		go s.commitLoop() //nolint:contextcheck // the loop's lifetime is the source's, not the opening call's
	}
	return s, nil
}

// trackerFor is the tracking window one partition gets.
//
// The pause thresholds default to twice the prefetch rather than to the
// window, which is the difference between a pause that protects the driver
// and one that never fires: the window's own default is 65,536 offsets, and a
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

// groupOptions turns the configuration into the group client's options.
//
// Two of them are the row's own requirements and neither is franz-go's
// default: cooperative-sticky, so that a rebalance does not stop the world for
// partitions nobody is taking away, and auto-commit OFF, because the commit
// is the acknowledgement and a client that committed on a timer would
// acknowledge records the application has not finished.
//
// BlockRebalanceOnPoll is the third, and it is what makes the rest of this
// file safe: the rebalance callbacks run only while this source is NOT inside
// a poll, so they can take the source's mutex without racing the fetch that
// is filling it.
func (b *Backend) groupOptions(s *source, cfg mq.SourceConfig, topic string) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(b.cfg.Brokers...),
		kgo.ClientID(b.cfg.ClientID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(cfg.Subscription),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(s.onAssigned),
		kgo.OnPartitionsRevoked(s.onRevoked),
		kgo.OnPartitionsLost(s.onLost),
		kgo.ConsumeResetOffset(resetOffset(cfg.Start.Resolve())),
		kgo.AdjustFetchOffsetsFn(s.adjustStart),
		kgo.FetchMaxWait(b.cfg.FetchMaxWait),
		kgo.SessionTimeout(b.cfg.SessionTimeout),
		kgo.RebalanceTimeout(b.cfg.RebalanceTimeout),
		kgo.HeartbeatInterval(b.cfg.HeartbeatInterval),
		kgo.WithHooks(&clientHooks{b: b}),
	}
	if b.cfg.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(b.cfg.FetchMaxBytes))
	}
	if b.cfg.InstanceID != "" {
		opts = append(opts, kgo.InstanceID(b.cfg.InstanceID))
	}
	if b.cfg.ReadCommitted {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}
	if b.cfg.RequestTimeout > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(b.cfg.RequestTimeout))
	}
	common, err := commonOptions(b.cfg)
	if err != nil {
		return nil, err
	}
	return append(opts, common...), nil
}

// adjustStart puts a partition the group has no committed offset for at the
// position resolved when the subscription was OPENED, rather than at the reset
// offset franz-go would resolve when the partition is assigned.
//
// For "latest" those are different records. Assignment follows a rebalance,
// and a caller that opened a consumer and then published is owed what it
// published; resolving the end of the log afterwards skips it. The commit
// [Backend.ensureGroupPosition] makes normally hides the difference, and a
// live group refuses that commit: a Kafka group spans topics, so reusing a
// subscription name on a second topic is enough. Every conformance scenario's
// subscription is called "conformance", and nightly run 35678720257 lost a
// scenario's only message, or the start of a burst, whenever an earlier
// scenario's member was still in the group. For a sequence
// it is the clamp: an empty group commits the sequence clamped into the log as
// it was at opening , and a live group now starts there too.
//
// A partition whose fetched offset is anything but the reset offset HAS a
// committed position and is left alone: franz-go substitutes the configured
// reset offset itself for a partition with no commit. The one value a commit
// can share with a reset offset is a sequence start's At(n), and only from a
// commit that carries no leader epoch; substituting then moves the start to
// the clamp of n taken at opening, which re-reads records and never skips one.
func (s *source) adjustStart(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	if len(s.startAt) == 0 {
		return offsets, nil
	}
	parts := offsets[s.topic]
	for p, o := range parts {
		if o != s.reset {
			continue
		}
		if at, ok := s.startAt[p]; ok {
			parts[p] = kgo.NewOffset().At(at)
		}
	}
	return offsets, nil
}

// resetOffset maps the start position onto the offset a group with no
// committed position starts at.
//
// It is the fallback. The primary mechanism is the commit
// [Backend.ensureGroupPosition] makes when the subscription is opened, which
// is what turns a start position into a DURABLE one; this covers the case
// where the topic did not exist yet and there was nothing to resolve against.
func resetOffset(start mq.StartPosition) kgo.Offset {
	o := kgo.NewOffset()
	switch start.Kind() {
	case mq.StartKindEarliest:
		return o.AtStart()
	case mq.StartKindTime:
		return o.AfterMilli(start.Time().UnixMilli())
	case mq.StartKindSequence:
		return o.At(int64(start.Sequence()))
	case mq.StartKindLatest, mq.StartKindUnset:
		return o.AtEnd()
	default:
		return o.AtEnd()
	}
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The order it looks in is the order that keeps a partition's records in
// order: the records a nack is holding for redelivery first, then the records
// a full tracking window made it keep back, then the broker. A fetch that went
// to the broker first would hand out a later offset before the earlier one it
// is already holding.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		if err := s.state(ctx); err != nil {
			return 0, err
		}
		n := s.takeLocal(dst)
		s.flushPauses()
		if n > 0 {
			return n, nil
		}
		m, err := s.poll(ctx, dst)
		s.flushPauses()
		if err != nil {
			return 0, err
		}
		if m > 0 {
			return m, nil
		}
		// Nothing came back: either the context is done, the source is
		// closing, a held record became due while the poll waited, or a
		// settlement freed records the source was already holding. The
		// loop re-checks all four.
	}
}

// poll asks the broker once and absorbs what it returns.
//
// AllowRebalance is the obligation BlockRebalanceOnPoll creates, and it is
// deferred rather than called at the end of the happy path: a member that
// forgot it under an error would hold the group's rebalance until the
// coordinator's rebalance timeout and then be kicked out of the group, which
// looks to the application like its consumer stopped for no reason.
func (s *source) poll(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	pctx, cancel := context.WithCancel(ctx)
	// Go has no way to derive a context from two parents, and the specification
	// requires Close to unblock a parked Fetch as well as the caller's own
	// cancellation. AfterFunc on the source's context is the merge, and it
	// costs no goroutine.
	stop := context.AfterFunc(s.ctx, cancel) //nolint:contextcheck // the merge of the caller's cancellation and Close's
	defer func() { stop(); cancel() }()
	// Armed before nextDue is read, so that a redelivery held after this
	// point either wakes the poll or is in the deadline below.
	if !s.armWake(cancel) {
		return 0, nil
	}
	defer s.disarmWake()
	if due, ok := s.nextDue(); ok {
		// A held record is waiting: the poll must not outlast it, or a
		// partition whose only work is a redelivery would wait for the
		// fetch timeout instead of for the delay the nack asked for.
		var dcancel context.CancelFunc
		pctx, dcancel = context.WithDeadline(pctx, due)
		defer dcancel()
	}
	// PollFetches rather than PollRecords, and the difference is the whole
	// of the specification's "one blocking wait then a non-blocking drain".
	//
	// franz-go buffers each fetch RESPONSE separately, and PollRecords
	// serves one response at a time: a Kafka fetch returns as soon as one
	// byte is available, so the first response of a burst routinely holds
	// a single record and the second holds the rest. A driver built on
	// PollRecords therefore fills one slot of a caller's batch and looks
	// as though it cannot batch at all — which is what the
	// BatchReceive scenario caught. PollFetches takes everything buffered,
	// and what will not fit in the caller's slice goes into the
	// per-partition backlog the tracking window already needed.
	fetches := s.cl.PollFetches(pctx)
	defer s.cl.AllowRebalance()
	if err := s.fetchErr(fetches); err != nil {
		return 0, err
	}
	return s.absorb(fetches, dst)
}

// fetchErr decides what a fetch's errors mean.
//
// A closed client is the only one that ends the fetch: everything else is
// either a condition franz-go is already recovering from (a rebalance, a
// coordinator move, a leader election) or one partition's problem, and both
// belong in health rather than in the caller's Receive — the rule that a
// background failure surfaces through hooks and health and not only in logs.
func (s *source) fetchErr(fetches kgo.Fetches) error {
	if errors.Is(fetches.Err0(), kgo.ErrClientClosed) {
		return mq.ErrClosed
	}
	for _, fe := range fetches.Errors() {
		if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
			continue
		}
		if errors.Is(fe.Err, kgo.ErrClientClosed) {
			return mq.ErrClosed
		}
		// Handled before absorb sees any record of this poll: franz-go
		// reports the loss before it fetches from where it reset to, but a
		// poll can carry that fetch and the report together, the report
		// last.
		var lost *kgo.ErrDataLoss
		if errors.As(fe.Err, &lost) && lost.ResetTo < lost.ConsumedTo {
			s.rewind(tp{fe.Topic, fe.Partition}, lost.ResetTo, lost.ResetToEpoch)
		}
		s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, fe.Topic), s.group, fe.Err))
	}
	return nil
}

// rewind re-bases a partition the client has moved back to off, below the
// offset it had consumed to, because the log lost records under it: the topic
// was recreated without topic IDs to tell, or the log was truncated.
//
// Left as it was, the tracker refused every record from off up to its newest
// offset as out of order, and the record was dropped unseen. The reset also
// bumps the tracker's generation: a delivery handed out before the loss
// carries an offset the log may since have given to another record, and
// completing it could move the watermark, and the commit, past a record
// nobody has processed. After the reset it reports ErrLeaseLost instead. The
// records the source kept back and the redeliveries a nack held are from
// before the loss too, and are discarded. The leader epoch the next commit
// carries is the one the client reset to, because a recreated log numbers its
// epochs again from the start.
func (s *source) rewind(key tp, off int64, epoch int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.parts[key]
	if p == nil {
		return
	}
	p.tr.Reset(off)
	p.backlog, p.held = nil, nil
	p.inflight = 0
	p.committed, p.dirty = off, false
	p.epoch = epoch
	clear(p.firstAt)
	// A partition paused for a full window is fetched by nothing until it is
	// resumed, and the window it was paused for is gone.
	s.maybeResumeLocked(p)
}

// absorb turns polled records into deliveries, tracking each offset as it
// goes.
//
// The tracker is what decides how many: a partition whose window is full is
// paused and its remaining records are kept in the source's own backlog, so
// that the tracker never sees an offset twice. Re-fetching them instead would
// mean rewinding the partition, which would mean resetting the tracker, which
// would invalidate the generation of every delivery of that partition that is
// still in flight — a rebalance's worth of ErrLeaseLost for a full window.
func (s *source) absorb(fetches kgo.Fetches, dst []mq.DriverDelivery) (int, error) {
	n := 0
	var err error
	s.mu.Lock()
	defer s.mu.Unlock()
	fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
		if err != nil {
			return
		}
		p, perr := s.ensurePartLocked(tp{ftp.Topic, ftp.Partition}, ftp.Records)
		if perr != nil {
			err = perr
			return
		}
		if p == nil {
			return
		}
		for i, rec := range ftp.Records {
			// A partition a nack is blocking takes what the poll brought
			// back into its backlog too: those records come after the
			// held one, and takeLocal hands the backlog out only once
			// the held record has gone out again.
			if n >= len(dst) || n >= s.cfg.Prefetch || p.tr.Paused() || p.blockedLocked() {
				p.backlog = append(p.backlog, ftp.Records[i:]...)
				s.pauseLocked(p)
				return
			}
			switch s.trackLocked(p, rec) {
			case tracked:
				dst[n] = s.deliveryLocked(p, rec, 1)
				n++
			case dropped:
				// An offset this tracker has already resolved: the
				// broker is redelivering after an uncommitted
				// rebalance and the record is not news.
				continue
			case deferred:
				p.backlog = append(p.backlog, ftp.Records[i:]...)
				s.pauseLocked(p)
				return
			}
		}
	})
	return n, err
}

// armWake lets a settlement end the coming poll, and reports false, arming
// nothing, when a settlement has freed local work since takeLocal looked.
//
// The check and the arming are one locked section, and that is what loses no
// wake-up: a settlement either lands before it and has left woken set, or
// finds the poll armed and cancels it.
func (s *source) armWake(cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.woken {
		s.woken = false
		return false
	}
	s.wakePoll = cancel
	return true
}

// disarmWake forgets the poll that has ended.
func (s *source) disarmWake() {
	s.mu.Lock()
	s.wakePoll = nil
	s.mu.Unlock()
}

// wakeLocked ends the waiting poll, or makes the next one not wait, so that
// Fetch goes back to takeLocal.
func (s *source) wakeLocked() {
	if s.wakePoll == nil {
		s.woken = true
		return
	}
	s.wakePoll()
	s.wakePoll = nil
}

// backlogReadyLocked reports whether takeLocal could hand out a partition's
// backlog now.
func backlogReadyLocked(p *partState) bool {
	return !p.stale && len(p.backlog) > 0 && !p.tr.Paused() && !p.blockedLocked()
}

// blockedLocked reports whether a nack is holding one of the partition's
// records under a plan that pauses the partition: block-partition's, or a
// custom plan that asked for the pause.
func (p *partState) blockedLocked() bool {
	for _, h := range p.held {
		if h.pause {
			return true
		}
	}
	return false
}

// takeLocal hands out what the source is already holding: the redeliveries a
// nack is holding whose delay has passed, and then the backlog.
//
// A blocked partition hands out no backlog, and absorb puts everything a poll
// brings back for it into that backlog, so this one gate is what stops a
// partition's later records overtaking the record its nack is holding: when
// the backlog waited only for the tracker's pause, a record the source already
// held was handed out microseconds after the nack. The block ends when the
// held record is handed out again, which is what [nack.Plan]'s Pause asks for,
// and not when it is acknowledged. What it keeps is therefore the order
// records are handed out in: the redelivery goes out ahead of every later
// record of its partition that had not been handed out when it was nacked, and
// those may follow it in the same Fetch. Nothing handed out is called back: a
// record that was out before the nack, such as the rest of the same batch, can
// be handled before the redelivery, and one that went out with the redelivery
// can be handled before the next attempt if the redelivery fails again.
func (s *source) takeLocal(dst []mq.DriverDelivery) int {
	now := s.b.now()
	n := 0
	s.mu.Lock()
	defer s.mu.Unlock()
	s.woken = false
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
			// The offset is already in the tracker: a redelivery of a
			// record whose offset never completed must NOT be tracked
			// again, because Track requires strictly increasing offsets
			// and would refuse it as out of order.
			dst[n] = s.deliveryLocked(p, h.rec, h.attempt)
			n++
		}
		for len(p.backlog) > 0 && n < len(dst) && !p.tr.Paused() && !p.blockedLocked() {
			rec := p.backlog[0]
			out := s.trackLocked(p, rec)
			if out == deferred {
				break
			}
			p.backlog = p.backlog[1:]
			if out == dropped {
				continue
			}
			dst[n] = s.deliveryLocked(p, rec, 1)
			n++
		}
		s.maybeResumeLocked(p)
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
// when the assignment is new.
//
// The re-base is the half that matters. A partition this source has owned
// before keeps its state — and therefore its monotonically increasing tracker
// generation — so that a delivery handed out under the previous assignment can
// never be mistaken for one handed out under this assignment, whatever the
// offsets happen to be.
func (s *source) ensurePartLocked(key tp, recs []*kgo.Record) (*partState, error) {
	p := s.parts[key]
	if len(recs) == 0 {
		return p, nil
	}
	first := recs[0].Offset
	if p == nil {
		tr, err := offset.New(s.b.cfg.Tracker, s.trackerBase(first))
		if err != nil {
			return nil, fmt.Errorf("kafka: offset tracker for %s/%d: %w", key.topic, key.partition, err)
		}
		p = &partState{
			tp:      key,
			tr:      tr,
			slot:    len(s.order),
			firstAt: map[int64]time.Time{},
		}
		p.committed = first
		s.parts[key] = p
		s.order = append(s.order, p)
		return p, nil
	}
	if p.stale {
		// The partition has come back. Re-basing at the offset the broker
		// is delivering from bumps the tracker's generation, so every
		// handle from the previous assignment is stale for good — which
		// is why this state is kept for the life of the source rather
		// than deleted on revoke: a generation that restarted at one
		// would make an old handle match a new assignment.
		p.tr.Reset(first)
		p.stale = false
		p.backlog, p.held = nil, nil
		p.committed, p.dirty = first, false
		clear(p.firstAt)
	}
	// A record at or below the newest offset this tracker has seen is a
	// redelivery of something already resolved, which happens after an
	// uncommitted rebalance. trackLocked drops it rather than re-basing:
	// re-basing would invalidate the generation of every delivery of this
	// partition that is still in flight.
	return p, nil
}

// trackerBase is the tracking window based at off.
func (s *source) trackerBase(off int64) offset.Config {
	cfg := s.trcfg
	cfg.Base = off
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
	// deferred: the tracking window has no room, so the record and
	// everything after it in this batch is kept for a later fetch.
	deferred
)

// trackLocked records one delivered offset and says what happened to it.
//
// The three answers are genuinely different and conflating any two of them is
// a defect: dropping a deferred record loses it, deferring a dropped one
// stalls the partition on a record that will never be trackable, and handing
// out either is a duplicate.
func (s *source) trackLocked(p *partState, rec *kgo.Record) trackOutcome {
	err := p.tr.Track(p.tr.Gen(), rec.Offset)
	switch {
	case err == nil:
		if rec.LeaderEpoch > p.epoch {
			p.epoch = rec.LeaderEpoch
		}
		return tracked
	case errors.Is(err, offset.ErrOutOfOrder):
		return dropped
	case errors.Is(err, offset.ErrWindowFull):
		return deferred
	default:
		// A stale generation here would mean the tracker was reset
		// between the fetch and this call, which BlockRebalanceOnPoll
		// makes impossible; reporting it is how that assumption gets
		// checked in production rather than assumed.
		s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, rec.Topic), s.group, err))
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
func (s *source) deliveryLocked(p *partState, rec *kgo.Record, attempt uint32) mq.DriverDelivery {
	m := &mq.Message{}
	switch s.meta {
	case metaEnvelope, metaAdaptive:
		// The two modes differ in ONE place: adaptive asks whether this
		// record is an envelope and portable already knows that it is. What
		// they must not differ in is what happens to a record that IS one,
		// so the decode and its failure are shared.
		var raw mq.Message
		Decode(&raw, rec, s.prefix)
		// The marker and never the body: a native value that happens to be
		// a valid envelope is a payload, and decoding it as metadata would
		// hand the application bytes nobody published.
		if s.meta == metaAdaptive && !Enveloped(rec) {
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
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, TopicOf(s.prefix, rec.Topic), s.group, err))
			*m = raw
		} else {
			m.Topic = raw.Topic
		}
	case metaStrict, metaDrop:
		Decode(m, rec, s.prefix)
	}
	if a := Attempt(rec); a > attempt {
		attempt = a
	}
	now := s.b.now()
	if _, ok := p.firstAt[rec.Offset]; !ok {
		p.firstAt[rec.Offset] = now
	}
	p.inflight++
	return mq.DriverDelivery{
		Message: m,
		Info: mq.DeliveryInfo{
			Topic:        m.Topic,
			Source:       rec.Topic,
			Subscription: s.group,
			ReceivedAt:   now,
			Attempt:      attempt,
			Redelivered:  attempt > 1,
			Partition:    rec.Partition,
			HasPartition: true,
			Offset:       rec.Offset,
			HasOffset:    true,
			// LeaseExpires is the zero time and Sequence is unset:
			// The specification makes both "the broker does not say", which is the
			// truth here. Kafka grants no per-record lease and its
			// sequence IS the offset.
		},
		Token:  uint64(p.slot),
		Native: rec,
		Gen:    p.tr.Gen(),
	}
}

// pauseLocked marks a partition for pausing, which is what stops the broker
// handing this source more of a partition it cannot track. The client call
// itself is flushPauses's, outside the mutex.
func (s *source) pauseLocked(p *partState) {
	if p.paused {
		return
	}
	p.paused = true
	s.toFlush = append(s.toFlush, p.tp)
}

// maybeResumeLocked marks a partition for resuming once its window has room
// and its redeliveries have all been handed back out.
func (s *source) maybeResumeLocked(p *partState) {
	if !p.paused || p.tr.Paused() || len(p.held) > 0 || len(p.backlog) > 0 {
		return
	}
	p.paused = false
	s.toFlush = append(s.toFlush, p.tp)
}

// flushPauses performs the pauses and resumes the locked sections decided on.
//
// It must run with s.mu NOT held. franz-go's PauseFetchPartitions takes the
// consumer's own locks, and a rebalance callback takes s.mu, so a client call
// made while holding s.mu is a lock cycle: the settlement path holds s.mu and
// waits for franz-go, and franz-go's callback waits for s.mu.
//
// It applies each partition's paused flag as it stands now rather than the
// changes queued since the last flush. Queued changes were applied every
// pause before every resume, and by whichever flush reached the client last,
// so a resume decided before a pause, or a pause flushed after a resume that
// came later, left the client paused while the source believed the partition
// was fetching. Nothing would resume it again.
func (s *source) flushPauses() {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.Lock()
	var pause, resume []tp
	for _, key := range s.toFlush {
		p := s.parts[key]
		switch {
		case p == nil:
		case p.paused:
			pause = append(pause, key)
		default:
			resume = append(resume, key)
		}
	}
	s.toFlush = nil
	s.mu.Unlock()
	if len(pause) > 0 {
		s.cl.PauseFetchPartitions(byTopic(pause))
	}
	if len(resume) > 0 {
		s.cl.ResumeFetchPartitions(byTopic(resume))
	}
}

// byTopic groups topic partitions the way franz-go's pause and resume take
// them.
func byTopic(tps []tp) map[string][]int32 {
	out := make(map[string][]int32, 1)
	for _, t := range tps {
		out[t.topic] = append(out[t.topic], t.partition)
	}
	return out
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
func (s *source) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cond.Broadcast()
		s.mu.Unlock()
		// The commit comes first and it is synchronous: a Close that
		// closed the client before committing would redeliver everything
		// the application completed since the last tick, which is the
		// at-least-once window opening as wide as CommitInterval for no
		// reason.
		//
		// It is belt and braces rather than the only path. Closing the
		// group client makes franz-go leave the group, which fires
		// OnPartitionsRevoked and commits the same boundary — so a
		// mutation that removes this line is not observable through the
		// portable API. What it covers is the case where no revoke
		// fires: a source that was never assigned a partition, and a
		// close whose leave-group does not complete.
		cctx, cancel := context.WithTimeout(context.Background(), s.b.cfg.RequestTimeout)
		err = s.commitNow(cctx)
		cancel()
		s.cancel()
		if s.cl != nil {
			s.cl.Close()
		}
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
			cctx, cancel := context.WithTimeout(s.ctx, s.b.cfg.RequestTimeout)
			if err := s.commitNow(cctx); err != nil && !errors.Is(err, mq.ErrClosed) {
				s.b.health.Degraded(err)
			}
			cancel()
		}
	}
}

// commitNow commits every partition whose watermark has advanced, and waits
// for the answer.
func (s *source) commitNow(ctx context.Context) error {
	offsets, parts := s.pendingCommits(nil)
	if len(offsets) == 0 {
		return nil
	}
	return s.commit(ctx, offsets, parts, false)
}

// pendingCommits collects the offsets a commit would carry. With only non-nil
// it collects just those partitions, which is what a revoke needs.
func (s *source) pendingCommits(only map[tp]bool) (map[string]map[int32]kgo.EpochOffset, []*partState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pendingCommitsLocked(only)
}

// pendingCommitsLocked is pendingCommits with s.mu held.
func (s *source) pendingCommitsLocked(only map[tp]bool) (map[string]map[int32]kgo.EpochOffset, []*partState) {
	var (
		offsets map[string]map[int32]kgo.EpochOffset
		parts   []*partState
	)
	for _, p := range s.order {
		if !p.dirty || (only != nil && !only[p.tp]) || p.disownedLocked() {
			continue
		}
		wm := p.tr.Watermark()
		if wm <= p.committed {
			continue
		}
		if offsets == nil {
			offsets = map[string]map[int32]kgo.EpochOffset{}
		}
		if offsets[p.tp.topic] == nil {
			offsets[p.tp.topic] = map[int32]kgo.EpochOffset{}
		}
		offsets[p.tp.topic][p.tp.partition] = kgo.EpochOffset{Epoch: p.epoch, Offset: wm}
		parts = append(parts, p)
	}
	return offsets, parts
}

// disownedLocked reports that the partition's position is no longer this
// member's to commit: a stale partition that no revocation is draining was
// lost, or has been handed over, and a commit for it would overwrite the
// position of the member that owns it now. A revocation's own commit is sent
// while its partitions are disowned, and [source.stillAhead] says why.
func (p *partState) disownedLocked() bool { return p.stale && !p.draining }

// commit performs one synchronous offset commit and records what it carried.
//
// handover is set only by a revocation committing the boundary
// [source.lastBoundary] read for it, which is the one commit a disowned
// partition still makes: see [source.stillAhead].
func (s *source) commit(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset, parts []*partState, handover bool) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if offsets, parts = s.stillAhead(offsets, parts, handover); len(parts) == 0 {
		return nil
	}
	var cerr error
	s.cl.CommitOffsetsSync(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			cerr = err
			return
		}
		cerr = commitResponseErr(resp)
	})
	if cerr != nil {
		if errors.Is(cerr, kgo.ErrClientClosed) {
			return mq.ErrClosed
		}
		return s.b.errs.Map(mq.OpAck, TopicOf(s.prefix, s.topic), s.group, cerr)
	}
	s.b.commits.Add(1)
	s.mu.Lock()
	for _, p := range parts {
		if o, ok := offsets[p.tp.topic][p.tp.partition]; ok {
			p.committed = o.Offset
			p.dirty = p.tr.Watermark() > o.Offset
		}
	}
	s.mu.Unlock()
	return nil
}

// stillAhead leaves in a commit only the partitions whose boundary, read
// before the commit was sent, would still move the group forward, and runs
// with commitMu held so that no other commit lands between the check and the
// send.
//
// The boundary is read and then sent, and the commit loop, Backend.Commit and
// a revocation each do both. A revocation that read a later boundary and sent
// it while the loop was between the two halves had its commit overwritten by
// the loop's older one, and the member that took the partition redelivered
// the records between them. A boundary at or below the one last committed is
// that older commit. A boundary above the tracker's watermark was read before
// the tracker was re-based below it, at a log that lost records, and it names
// an offset of the log as it was before.
//
// A partition lost or handed over since its boundary was read is no longer
// this member's to commit ([partState.disownedLocked]), which the read alone
// checked. franz-go stamps a commit with the membership it holds when the
// commit is sent, so a commit that waited out the loss and the rejoin went out
// under the new generation, the coordinator took it, and the position of the
// member that owns the partition now went back to where this one had been. The
// revocation's own commit is the exception, and handover says it is that one:
// reading its boundary ended the wait's acceptance of settlements, which is
// what disowns the partitions, and they are still this member's until the
// revocation returns.
func (s *source) stillAhead(offsets map[string]map[int32]kgo.EpochOffset, parts []*partState, handover bool) (map[string]map[int32]kgo.EpochOffset, []*partState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := make([]*partState, 0, len(parts))
	for _, p := range parts {
		o, ok := offsets[p.tp.topic][p.tp.partition]
		if !ok {
			continue
		}
		disowned := p.disownedLocked() && !handover
		if disowned || o.Offset <= p.committed || o.Offset > p.tr.Watermark() {
			delete(offsets[p.tp.topic], p.tp.partition)
			continue
		}
		kept = append(kept, p)
	}
	return offsets, kept
}

// commitResponseErr is the first per-partition error an offset commit
// reported. A commit whose response carried an error code committed nothing
// for that partition, and treating the round trip as a success would make the
// driver believe an offset is durable when it is not.
func commitResponseErr(resp *kmsg.OffsetCommitResponse) error {
	if resp == nil {
		return nil
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerrForCode(p.ErrorCode); err != nil {
				return err
			}
		}
	}
	return nil
}

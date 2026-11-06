package redis

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
)

// The source implements the mandatory consumption surface, and its settler
// implements the optional batched settlement and the optional lease
// extension.
var _ mq.Source = (*source)(nil)

// source is one open subscription: the consumer group, this instance's
// consumer name, the shared consume logic in `internal/streams`, and the two
// background loops this broker cannot do without.
//
// The loops are the part worth reading. A Redis consumer group hands an entry
// to a consumer and then does nothing about it for ever — nothing expires,
// nothing redelivers — so the redelivery is the driver's, and it is the
// reclaim sweep. The second loop is the delayed mover, which only the zset
// nack strategy needs; it is not started for the other three.
type source struct {
	b     *Backend
	cons  *streams.Consumer
	env   *driverkit.Envelope
	keys  streams.Keys
	set   *settler
	topic string
	group string
	name  string
	meta  metadataMode

	// pending holds the deliveries the sweep and the read produced but that
	// Fetch has not handed over yet, so a Fetch never loses a delivery the
	// sweep reclaimed.
	//
	// It holds at most ONE delivery per entry, and that is not an
	// optimisation. A buffered delivery that Fetch has not handed out yet can
	// be re-claimed by a later sweep, which supersedes its generation — and a
	// buffer that appended would then hand out the OLD one first, whose
	// settlement is refused with ErrLeaseLost. Against a real server that
	// broke lease/expiry-redelivery, settle/stale-rejection and
	// capability/AtLeastOnce/works, and every unit test passed because they
	// drove one sweep at a time.
	mu      sync.Mutex
	pending []buffered
	// collecting is the reads and sweeps whose deliveries are not in pending
	// yet, each with the entries Stop nacks have failed since it started.
	// Each can return entries behind those, which handBack could not find in
	// the buffer because they were not in it yet. collects numbers them.
	collecting map[uint64][]streams.EntryKey
	collects   uint64

	// scratch is the entry buffer the read and the sweep fill, reused so
	// that a fetch allocates the deliveries and nothing else.
	scratch []streams.Entry

	// wake tells a Fetch waiting on held shards that a settlement or a sweep
	// round may have changed what it would find.
	wake chan struct{}

	// interval is how often the sweep runs: see sweepInterval.
	interval time.Duration
	// inFetch counts the Fetch calls in progress and fetches every Fetch call
	// made. swept is fetches as the previous sweep round saw it. Together
	// they say whether a round claims anything: see sweep.
	inFetch atomic.Int32
	fetches atomic.Uint64
	swept   atomic.Uint64

	// failing has a bit set for each background loop whose last round failed
	// (sweepFailing, moverFailing), which is what the backend's health is
	// made from. See Backend.loopRound.
	failing atomic.Uint32
	// sweepErr and moverErr are each loop's latest failure. Backend.mu
	// guards them.
	sweepErr, moverErr loopFailure

	wg     sync.WaitGroup
	stop   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

// buffered is a delivery the read or the sweep produced, with when it did, or
// when an extension last restarted its idle clock while it waited (see
// refresh and unexpired).
//
// The time is what the delivery's lease is reported from. The read or the
// claim restarted the entry's idle clock, and the reclaim sweep takes the
// entry back a deadline after that whether or not Fetch has handed it out, so
// a lease counted from the hand-out was longer than the one the sweep
// enforces: a handler told it had a whole deadline could lose the entry to
// another consumer at once.
type buffered struct {
	streams.Delivery
	at time.Time
}

// newSource provisions the group, opens the source and starts its loops.
func (b *Backend) newSource(ctx context.Context, cfg mq.SourceConfig, keys streams.Keys, group, start string, meta metadataMode) (*source, error) {
	name := b.consumerName(cfg.Subscription)
	deadline := cfg.AckDeadline
	if deadline <= 0 {
		deadline = b.cfg.AckDeadline
	}
	dlq, err := b.cfg.deadLetterKey(cfg.Topic)
	if err != nil {
		return nil, err
	}
	interval := sweepInterval(b.cfg.ClaimInterval, deadline, b.cfg.Nack.Resolve())
	cons, err := streams.NewConsumer(streams.ConsumerConfig{
		Keys:     keys,
		Group:    group,
		Name:     name,
		Prefetch: cfg.Prefetch,
		// No longer than a sweep interval, because what a sweep round
		// claims waits in the buffer until the read a Fetch is parked in
		// returns: see Fetch.
		Block:            min(b.cfg.Block, interval),
		AckDeadline:      deadline,
		ClaimCount:       b.cfg.ClaimCount,
		Guard:            b.cfg.AckGuard,
		Nack:             b.cfg.Nack,
		MaxLen:           b.cfg.MaxLen,
		DeadLetterStream: dlq,
	})
	if err != nil {
		return nil, err
	}
	env, err := b.envelope(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	if err := cons.Provision(ctx, b.cmder, start, b.cfg.AutoCreate); err != nil {
		if streams.IsNoGroup(err) && !b.cfg.AutoCreate {
			// The specification: the stream does not exist and this driver may not create
			// it. Naming the setting is what turns a mystery into a
			// configuration error.
			return nil, errNoAutoCreate
		}
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	src := &source{
		b:     b,
		cons:  cons,
		env:   env,
		keys:  keys,
		topic: cfg.Topic,
		group: group,
		name:  name,
		meta:  meta,
		stop:  make(chan struct{}),
		wake:  make(chan struct{}, 1),

		interval: interval,

		collecting: map[uint64][]streams.EntryKey{},
	}
	src.set = &settler{src: src}
	src.wg.Add(1)
	// The loops deliberately do NOT take the opening context. They outlive
	// the call that started them — a source lives until Close — so a loop
	// bound to the open's context would stop the moment a caller who used a
	// bounded context for construction let it expire, and the mandatory
	// reclaim sweep stopping is a subscription that has silently stopped
	// being at-least-once. Each round derives its own bounded context from
	// Background instead, and `stop` is what ends the loop.
	go src.sweepLoop() //nolint:gosec,contextcheck // G118 and contextcheck both want the opening context; the loop outlives it by design, see above
	if b.cfg.Nack.Resolve() == streams.ZSetMover {
		// Only one of the four strategies parks anything, so only one needs
		// a mover. Starting it unconditionally would be a goroutine and a
		// ZRANGEBYSCORE per shard per interval for a sorted set that is
		// always empty.
		src.wg.Add(1)
		go src.moverLoop() //nolint:gosec,contextcheck // as above
	}
	return src, nil
}

// consumerName is this source instance's consumer name.
//
// It is ALWAYS unique: the configured prefix, the subscription, and a nonce
// this source allocates. The nonce is not optional and the reason is the
// guarded acknowledgement — it tells "somebody else claimed this entry" from
// "we still hold it" by comparing the pending row's consumer name to this
// one, so two sources sharing a name would each believe the other's
// deliveries were their own, and each would happily acknowledge the other's
// work. That is the loss [streams.Reclaimer] documents, and a configurable
// name would put it one typo away.
//
// The cost is a consumer row per source instance in XINFO CONSUMERS, which
// Close removes when the source holds nothing pending.
func (b *Backend) consumerName(sub string) string {
	prefix := b.cfg.ConsumerPrefix
	if prefix == "" {
		prefix = defaultConsumerPrefix()
	}
	return prefix + "-" + sub + "-" + strconv.FormatUint(nextNonce(), 36)
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The deliveries come from two places and both are drained here: the blocking
// XREADGROUP, and whatever the reclaim sweep put aside. The sweep's output
// goes through this buffer rather than being read directly, because a sweep
// runs on its own schedule and a redelivery it produced must not wait for the
// next read to be handed over.
//
// A read parked at the server cannot be woken without closing its
// connection, so what a sweep round buffers meanwhile waits for the read to
// return. The read's block is no longer than the sweep interval for that
// reason: Config.Block (5 s by default) is longer than the interval whenever
// the sweep runs more often, which by default it does for any deadline under
// 15 s, and the redelivery waited out the whole block. The block is also no
// longer than ctx allows (see [streams.Consumer.Read]), and once what is left
// of ctx is too short for a read to be answered in, Fetch waits for the
// sweep, ctx or Close instead of reading again.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	s.inFetch.Add(1)
	defer s.inFetch.Add(-1)
	s.fetches.Add(1)
	for {
		if s.closed.Load() {
			return 0, mq.ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if n := s.drain(ctx, dst); n > 0 {
			return n, nil
		}
		// The read blocks at the server, which is what keeps an idle
		// consumer from polling. Its bound is Config.Block, never zero:
		// BLOCK 0 in Redis means "for ever", and a fetch that cannot be
		// woken is what the specification forbids.
		n, err := s.collect(ctx, s.read)
		if err != nil {
			if s.closed.Load() {
				return 0, mq.ErrClosed
			}
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, s.b.errs.Map(mq.OpFetch, s.topic, s.group, err)
		}
		if n == 0 {
			// Nothing arrived within the block. Looping is right: Fetch's
			// contract is to block until something is available, ctx is done
			// or the source is closed, and the block is the driver's own
			// bound rather than the caller's.
			//
			// With every shard held under the Stop strategy, though, the read
			// sent nothing and returned at once, so the block has not passed.
			// Going straight round spun a core until the sweep brought the
			// failed message back. The same goes for a read that had too
			// little of ctx left to block and only polled.
			if s.cons.Held() >= s.keys.Shards || nearDeadline(ctx) {
				s.awaitChange(ctx)
			}
		}
	}
}

// fetchTail is how near its deadline a Fetch stops reading and only waits:
// a little more than the 150ms [streams.Consumer.Read] leaves between the end
// of its block and the deadline, for the server's timer tick and the answer,
// so that a read started then could only poll.
const fetchTail = 160 * time.Millisecond

// nearDeadline reports whether ctx's deadline is within fetchTail.
func nearDeadline(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return ok && time.Until(deadline) < fetchTail
}

// read is one read from the consumer.
func (s *source) read(ctx context.Context) ([]streams.Delivery, error) {
	ds, scratch, err := s.cons.Read(ctx, s.b.cmder, s.scratch, nil)
	s.scratch = scratch
	return ds, err
}

// sweep is one round of the reclaim sweep, buffered as collect buffers it.
//
// A round in which one shard fails still returns what the others claimed, and
// those are buffered with the error reported. The server has already given
// them to this source, so dropping them left them pending in its name: they
// came back a deadline later with an attempt too many, and a shard that kept
// failing stopped the redelivery of every other shard.
//
// The round claims for the caller, not for the buffer. fetching says whether
// the caller is fetching, which the loop decides with active. A round for a
// caller that is fetching claims no more than Prefetch less what the buffer
// already holds, after restarting the idle clocks of what the buffer holds
// (see refresh); a round for one that is not claims nothing, and asks the
// server about the pending list instead (see probe). So does a round with no
// room left, which a read of a partitioned topic can leave behind (its COUNT
// is per shard): it sent nothing whose failure it reported, so during an
// outage it reported a good round and Health went back to Ready for it. The round used to claim
// up to ClaimCount entries per shard whatever the caller was doing, and a
// claimed entry nobody fetched was claimed again each deadline it waited,
// each claim a delivery attempt no handler made: a busy runner's source took
// a crashed consumer's messages away from the sources that were fetching and
// handed them out, once it had room, already past MaxAttempts.
func (s *source) sweep(ctx context.Context, fetching bool) (int, error) {
	room := 0
	if fetching {
		s.refresh(ctx)
		room = s.room()
	}
	n, err := s.collect(ctx, func(ctx context.Context) ([]streams.Delivery, error) {
		ds, _, err := s.cons.SweepAtMost(ctx, s.b.cmder, room, nil, nil)
		return ds, err
	})
	if err == nil && room <= 0 {
		err = s.probe(ctx)
	}
	return n, err
}

// active reports whether a Fetch is in progress or one has started since the
// previous call, which is the sweep loop's previous round.
func (s *source) active() bool {
	seen := s.fetches.Load()
	return s.swept.Swap(seen) != seen || s.inFetch.Load() > 0
}

// probe asks the server for this source's first pending entry on each shard,
// in a round that claims nothing, for a caller that is not fetching or for
// one whose buffer is full.
//
// It claims nothing and changes nothing. It is there because the sweep is how
// a lost server shows in Health while nothing else is happening (see
// [Backend.Health]), and a round that sent no command would leave Health
// Ready through an outage for as long as the caller was busy.
func (s *source) probe(ctx context.Context) error {
	cmds := make([]streams.Cmd, 0, s.keys.Shards)
	for i := range s.keys.Shards {
		cmd, err := streams.XPending(streams.XPendingArgs{
			Key: s.keys.Stream(i), Group: s.group, Consumer: s.name, Count: 1,
		})
		if err != nil {
			return err
		}
		cmds = append(cmds, cmd)
	}
	results, err := s.b.cmder.DoMulti(ctx, cmds)
	if err != nil {
		return err
	}
	for _, res := range results {
		if res.Err != nil {
			return res.Err
		}
	}
	return nil
}

// room is how many more deliveries the buffer may take: Prefetch, the most
// the source may hold that its caller has not taken, less what it holds.
func (s *source) room() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cons.Config().Prefetch - len(s.pending)
}

// refresh restarts the idle clocks of the deliveries waiting in the buffer,
// before a round that claims.
//
// A buffered entry is pending to this source and idle while it waits, so a
// claim would take it once it had waited a deadline, this source's own claim
// included, and count a delivery no handler saw. A round claims only for a
// caller that is fetching, and that caller takes what the buffer holds next,
// so keeping it takes nothing from the consumers that are fetching. A source
// whose caller has stopped fetching does not refresh: what it holds can then
// be claimed by those consumers, and drain checks it before handing it out.
func (s *source) refresh(ctx context.Context) {
	s.mu.Lock()
	ds := append([]buffered(nil), s.pending...)
	s.mu.Unlock()
	if len(ds) == 0 {
		return
	}
	asked := make(map[streams.EntryKey]uint32, len(ds))
	for _, d := range ds {
		asked[d.Key] = d.Gen
	}
	fresh := s.restart(ctx, ds)
	s.mu.Lock()
	defer s.mu.Unlock()
	kept := s.pending[:0]
	for _, d := range s.pending {
		// A delivery the round replaced while the extension was out is
		// newer than the one extended, and is kept as it is.
		if gen, ok := asked[d.Key]; ok && gen == d.Gen {
			at, live := fresh[d.Key]
			if !live {
				continue
			}
			d.at = at
		}
		kept = append(kept, d)
	}
	s.pending = kept
}

// restart restarts the idle clocks of buffered deliveries with the guarded
// extension, which counts no delivery, and returns when each one's lease now
// starts: when the extension was sent, or the delivery's own stamp when its
// extension failed for a reason other than another consumer having claimed
// the entry, as a read hands out a queued delivery it could not restart. A
// delivery another consumer has claimed is not in it.
func (s *source) restart(ctx context.Context, ds []buffered) map[streams.EntryKey]time.Time {
	at := s.b.now()
	plain := make([]streams.Delivery, len(ds))
	for i := range ds {
		plain[i] = ds[i].Delivery
	}
	fresh := make(map[streams.EntryKey]time.Time, len(ds))
	for i, err := range s.cons.Extend(ctx, s.b.cmder, plain) {
		switch {
		case err == nil:
			fresh[ds[i].Key] = at
		case !errors.Is(err, mq.ErrLeaseLost):
			fresh[ds[i].Key] = ds[i].at
		}
	}
	return fresh
}

// collect runs one read or sweep round and buffers what it returned, except
// what is behind a Stop nack made while it ran, which it hands back as
// handBack hands back the buffer. It returns how many it buffered.
//
// The check and the buffering are one step under the lock. A nack after the
// round returned but before its deliveries were buffered found them in
// neither place, and the entry behind the failed one went out before its
// retry.
//
// The round's deliveries are stamped with the time it returned, which is when
// the read or the claim restarted their idle clocks: see buffered.
func (s *source) collect(ctx context.Context, round func(context.Context) ([]streams.Delivery, error)) (int, error) {
	s.mu.Lock()
	id := s.collects
	s.collects++
	s.collecting[id] = nil
	s.mu.Unlock()
	ds, err := round(ctx)
	got := stamp(ds, s.b.now())
	var back []buffered
	s.mu.Lock()
	for _, k := range s.collecting[id] {
		var behind []buffered
		got, behind = splitBehind(got, k)
		back = append(back, behind...)
	}
	delete(s.collecting, id)
	s.putLocked(got)
	s.mu.Unlock()
	s.giveBack(ctx, back)
	return len(got), err
}

// stamp pairs each of a round's deliveries with the time the round returned.
func stamp(ds []streams.Delivery, at time.Time) []buffered {
	if len(ds) == 0 {
		return nil
	}
	out := make([]buffered, len(ds))
	for i := range ds {
		out[i] = buffered{Delivery: ds[i], at: at}
	}
	return out
}

// awaitChange waits, for at most one block, for something that can change
// what a fetch with every shard held would find: a settlement or a sweep
// round, which may lift a hold or bring the failed message back, ctx, or
// Close.
func (s *source) awaitChange(ctx context.Context) {
	t := time.NewTimer(s.b.cfg.Block)
	defer t.Stop()
	select {
	case <-s.wake:
	case <-t.C:
	case <-ctx.Done():
	case <-s.stop:
	}
}

// nudge wakes a Fetch waiting in awaitChange, if there is one. A nudge with
// nobody waiting is kept, and costs the next wait one more round of the
// loop.
func (s *source) nudge() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// handBack returns to the consumer what this source buffered from behind k,
// whose shard a Stop nack has just held.
//
// The hold stops the shard's new reads, and these were read before it: the
// read took up to Prefetch entries and the fetches have handed out only as
// many as their callers had room for. Handing them out would deliver them
// before the failed message's retry. They are given back to the consumer
// rather than dropped from the buffer (see giveBack), so they stay pending and
// come back behind k, and the shard reads nothing newer until they too are
// settled. A read or a sweep in flight is told about k through collecting,
// because whatever it returns from behind k is not in the buffer yet.
func (s *source) handBack(ctx context.Context, k streams.EntryKey) {
	s.mu.Lock()
	var back []buffered
	s.pending, back = splitBehind(s.pending, k)
	for id, late := range s.collecting {
		s.collecting[id] = append(late, k)
	}
	s.mu.Unlock()
	s.giveBack(ctx, back)
}

// giveBack gives the consumer back deliveries this source is handing back
// without having handed them out.
//
// The consumer queues each one behind the hold, as it was: the same
// generation, and the same attempt, because no handler has tried it. One the
// consumer will not take, because the hold in front of it has lifted since,
// goes back into the buffer to be handed out.
//
// An entry that cannot be decoded is answered as Fetch answers it instead.
// It never reaches a handler, so it has no order to keep, and giving it back
// would hold its shard for a settlement nothing will ever make: with no
// dead-letter stream, poison only leaves it pending, and every entry behind
// it waited for good.
//
// A delivery a sweep has since superseded is skipped, as drain skips it. The
// newer one is the entry's live delivery and is on its way through the
// consumer or a round, and poison's Lost would forget that one instead.
func (s *source) giveBack(ctx context.Context, ds []buffered) {
	var due []buffered
	for i := range ds {
		d := &ds[i].Delivery
		if s.cons.Reclaimer().Check(d.Key, d.Gen) != nil {
			continue
		}
		if _, err := s.decode(d); err != nil {
			s.poison(ctx, d, err)
			continue
		}
		if !s.cons.Return(*d) {
			due = append(due, ds[i])
		}
	}
	if len(due) == 0 {
		return
	}
	s.mu.Lock()
	s.putLocked(due)
	s.mu.Unlock()
}

// splitBehind moves the deliveries of ds that are behind k in its shard to
// back, keeping the rest in order in ds's own array.
func splitBehind(ds []buffered, k streams.EntryKey) (kept, back []buffered) {
	kept = ds[:0]
	for _, d := range ds {
		if d.Key.Shard == k.Shard && k.ID.Before(d.Key.ID) {
			back = append(back, d)
			continue
		}
		kept = append(kept, d)
	}
	return kept, back
}

// drain moves buffered deliveries into dst.
func (s *source) drain(ctx context.Context, dst []mq.DriverDelivery) int {
	s.mu.Lock()
	n := min(len(s.pending), len(dst))
	batch := s.pending[:n]
	s.pending = s.pending[n:]
	if len(s.pending) == 0 {
		s.pending = nil
	}
	s.mu.Unlock()
	batch = s.unexpired(ctx, batch)
	out := 0
	for i := range batch {
		// The generation is re-checked here and not only when the delivery
		// was buffered. Between the two, a sweep may have re-claimed the
		// entry and superseded this generation; handing it out anyway would
		// give the caller a delivery whose settlement is refused, and the
		// message would look lost when it is about to be redelivered.
		if err := s.cons.Reclaimer().Check(batch[i].Key, batch[i].Gen); err != nil {
			continue
		}
		d, ok := s.delivery(ctx, &batch[i].Delivery, batch[i].at)
		if !ok {
			continue
		}
		dst[out] = d
		out++
	}
	return out
}

// unexpired is a batch drain is about to hand out, with the leases of the
// deliveries whose leases ran out while they waited restarted, and without
// the ones another consumer has claimed meanwhile.
//
// A source whose caller stopped fetching keeps nothing fresh (see refresh),
// so another consumer's sweep may have claimed what it holds and delivered
// it. Handing out the copy left here gave a second handler the message, under
// a lease that had already run out, and its settlement was refused. One that
// nobody claimed is handed out with its attempt as it was: waiting in the
// buffer is not an attempt.
func (s *source) unexpired(ctx context.Context, batch []buffered) []buffered {
	now := s.b.now()
	lease := s.cons.Config().AckDeadline
	var stale []buffered
	for _, d := range batch {
		if leaseRunOut(d, now, lease) {
			stale = append(stale, d)
		}
	}
	if len(stale) == 0 {
		return batch
	}
	fresh := s.restart(ctx, stale)
	out := batch[:0]
	for _, d := range batch {
		if leaseRunOut(d, now, lease) {
			at, live := fresh[d.Key]
			if !live {
				continue
			}
			d.at = at
		}
		out = append(out, d)
	}
	return out
}

// leaseRunOut reports whether a buffered delivery's lease had run out by now:
// its entry had been idle for the deadline, so any consumer's sweep could
// have claimed it.
func leaseRunOut(d buffered, now time.Time, lease time.Duration) bool {
	return !now.Before(d.at.Add(lease))
}

// poison answers an entry this driver cannot decode.
//
// It is reported through the health surface, and then dead-lettered when the
// subscription has a dead-letter stream: the same move a Reject makes, with
// the raw entry as the copy. Leaving it pending was this driver's first
// answer, on the reasoning that MaxAttempts would dead-letter it, and it
// cannot: MaxAttempts is counted by the core on the deliveries a handler sees,
// and this one never reaches a handler, so the reclaim sweep claimed it again
// every AckDeadline for ever. The decode is deterministic, so a later attempt
// cannot succeed and nothing is gained by waiting for one.
//
// With no dead-letter stream, or when the move fails, the entry stays pending
// and the sweep brings it back, which is the one place it is still visible.
func (s *source) poison(ctx context.Context, d *streams.Delivery, cause error) {
	// It counts as a failed sweep, so the source's next good sweep round
	// clears it: the entry is dead-lettered by then, or it is still pending
	// and the sweep brings it back to be reported again.
	s.b.loopRound(s, sweepFailing, s.b.errs.Map(mq.OpFetch, s.topic, s.group, cause))
	if s.cons.Config().DeadLetterStream != "" {
		o := mq.NackOptions{Action: mq.NackActionDeadLetter, Reason: "undecodable: " + cause.Error()}
		if s.cons.Nack(ctx, s.b.cmder, *d, o, s.b.now()) == nil {
			return
		}
	}
	s.cons.Reclaimer().Lost(d.Key)
}

// putLocked buffers deliveries the read or the sweep produced, replacing a
// buffered delivery of the same entry.
//
// The replacement is what keeps the buffer from holding two generations of one
// entry: a sweep that re-claims a buffered entry produces a NEW generation,
// and the old one is refused by the guard the moment anything settles it.
// Replacing rather than appending means the caller is handed the generation
// that can actually be settled. The caller holds s.mu.
func (s *source) putLocked(ds []buffered) {
	for _, d := range ds {
		replaced := false
		for i := range s.pending {
			if s.pending[i].Key == d.Key {
				s.pending[i] = d
				replaced = true
				break
			}
		}
		if !replaced {
			s.pending = append(s.pending, d)
		}
	}
}

// delivery maps one entry onto a DriverDelivery.
//
// An entry this driver cannot decode is the poison-message case the
// record is in the stream and pending in this group, so it cannot be ignored
// — something has to settle it or it comes back for ever. It is NOT delivered
// as an empty message: a handler given a message that is not the one that was
// published is worse than a message that never arrives. See [source.poison]
// for what happens to it instead.
//
// at is when the read or the claim that produced d returned, which is where
// its lease starts: see buffered.
func (s *source) delivery(ctx context.Context, d *streams.Delivery, at time.Time) (mq.DriverDelivery, bool) {
	m, err := s.decode(d)
	if err != nil {
		s.poison(ctx, d, err)
		return mq.DriverDelivery{}, false
	}
	ref := *d
	// One reading for both times: the lease scheduler extends by
	// LeaseExpires-ReceivedAt, and two readings made that a little longer
	// than AckDeadline, which Extend refuses.
	return mq.DriverDelivery{
		Message: m,
		Info: mq.DeliveryInfo{
			Topic:        m.Topic,
			Source:       s.keys.Stream(d.Key.Shard),
			Subscription: s.group,
			ReceivedAt:   at,
			// The lease is the DRIVER's: Redis grants none, and the reclaim
			// sweep is what enforces this one. Reporting it is what lets a
			// caller's lease scheduler decide when to extend.
			LeaseExpires: at.Add(s.cons.Config().AckDeadline),
			Attempt:      d.Attempt,
			Redelivered:  d.Attempt > 1,
			Partition:    int32(d.Key.Shard),
			HasPartition: s.keys.Shards > 1,
			// A stream identifier is two 64-bit numbers and Sequence
			// is one, so it is the packed form: the millisecond in the high
			// bits and the within-millisecond sequence in the low twenty.
			// Monotone, so comparing two deliveries' sequences gives the
			// publication order, and exact, so StartAtSequence resumes at
			// the entry rather than at the start of its millisecond.
			Sequence:    d.Key.ID.Pack(),
			HasSequence: true,
		},
		Ref: &ref,
		Native: nativeDelivery{
			Key:     s.keys.Stream(d.Key.Shard),
			Group:   s.group,
			ID:      d.Key.ID.String(),
			Attempt: d.Attempt,
		},
		Gen: d.Gen,
	}, true
}

// decode is the message an entry carries, or why this driver cannot read it.
func (s *source) decode(d *streams.Delivery) (*mq.Message, error) {
	m := &mq.Message{}
	if err := streams.Decode(m, d.Entry.Fields); err != nil {
		return nil, err
	}
	if s.meta == metaEnvelope || s.meta == metaAdaptive {
		var out mq.Message
		if err := s.env.Unwrap(&out, m); err != nil {
			return nil, err
		}
		topic := m.Topic
		*m = out
		if m.Topic == "" {
			m.Topic = topic
		}
	}
	if m.Topic == "" {
		m.Topic = s.topic
	}
	return m, nil
}

// nativeDelivery is what a caller asking for the escape hatch gets: the
// stream key, the group and the entry identifier, which is everything needed
// to look the entry up in redis-cli.
type nativeDelivery struct {
	// Key is the stream key the entry is in.
	Key string
	// Group is the consumer group.
	Group string
	// ID is the entry identifier, as Redis spells it.
	ID string
	// Attempt is the delivery attempt.
	Attempt uint32
}

// Settler settles this source's deliveries. There is one per source.
func (s *source) Settler() mq.Settler { return s.set.settler() }

// Close unsubscribes, wakes a parked Fetch and abandons whatever this source
// still holds. It is idempotent.
//
// "Abandon" is the only hand-back this broker offers, and it is honest rather
// than convenient: an unsettled entry stays in the pending-entries list and
// becomes claimable once it has been idle for the acknowledgement deadline,
// so another consumer's reclaim loop picks it up. The one command that would
// look like a prompt hand-back — XGROUP DELCONSUMER — deletes the entries
// along with the consumer, which is message loss, so it runs only when the
// pending list is empty ([streams.Consumer.Close]).
func (s *source) Close() error {
	var err error
	s.once.Do(func() {
		s.closed.Store(true)
		close(s.stop)
		s.wg.Wait()
		s.set.close()
		// A bounded context, because Close has none and this is a
		// round trip: a tidy-up that hung would make Close unbounded, which
		// is worse than a consumer row left behind. The specification gives Close no
		// context on purpose — a prompt close is not an operation a caller
		// cancels, and Shutdown is the bounded form — so there is no caller
		// context to pass down here.
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		if cerr := s.cons.Close(ctx, s.b.cmder); cerr != nil {
			err = s.b.errs.Map(mq.OpClose, s.topic, s.group, cerr)
		}
		s.b.forget(s, nil)
		s.b.loopsGone(s)
	})
	return err
}

// closeTimeout bounds the round trip Close makes.
const closeTimeout = 5 * time.Second

// sweepLoop is the mandatory pending-entries reclaim loop.
//
// It is not an optimisation and it is not a background nicety: it is the only
// thing on this broker that redelivers. A Redis consumer group hands an entry
// to a consumer and forgets about it, so a consumer that died holding a
// hundred entries holds them until something claims them away. XAUTOCLAIM with
// min-idle equal to the acknowledgement deadline is that something, and a
// driver without this loop has an at-least-once claim that is false.
func (s *source) sweepLoop() {
	defer s.wg.Done()
	interval := s.interval
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), interval)
		_, err := s.sweep(ctx, s.active())
		cancel()
		if err != nil {
			// The specification: a background failure surfaces through health, not only
			// through a log. A sweep that keeps failing is a subscription
			// that has stopped redelivering, which is exactly what a caller
			// polling Health needs to know.
			s.b.loopRound(s, sweepFailing, s.b.errs.Map(mq.OpFetch, s.topic, s.group, err))
			// Even a failed round may have lifted a Stop hold, and a Fetch
			// waiting on one hears about it here.
			s.nudge()
			continue
		}
		// A good round is what brings the health back once the sweep
		// recovers.
		s.b.loopRound(s, sweepFailing, nil)
		s.nudge()
	}
}

// sweepInterval is how often a source sweeps: the configured interval, or
// under the Stop strategy a third of the source's own deadline when that is
// shorter, but no less than minStopSweep.
//
// Under Stop each round also restarts the idle clocks of the entries held
// back behind a failed message, and one left idle for a deadline is
// claimable: another consumer's sweep takes it and delivers it before the
// retry it was waiting behind. The default interval is a third of the
// backend's deadline, and a source can have a shorter one of its own.
//
// The floor is because a round is bounded by the interval too. A deadline of
// a few milliseconds would bound each round of pipelined commands by one, and
// against a real server every round would time out and report the source
// degraded. An interval configured below the floor is kept.
func sweepInterval(interval, deadline time.Duration, nack streams.Kind) time.Duration {
	if interval <= 0 {
		interval = DefaultClaimInterval
	}
	if nack != streams.Stop {
		return interval
	}
	return min(interval, max(deadline/3, minStopSweep))
}

// minStopSweep is the shortest interval sweepInterval derives from a
// source's deadline: the floor the conformance harness gives the sweep too.
const minStopSweep = 10 * time.Millisecond

// moverLoop releases the entries the zset nack strategy parked.
//
// It runs only under that strategy. Each pass is one ZRANGEBYSCORE per shard
// and one script call per due entry, and the script's ZREM decides the winner
// — so two processes' movers racing over one due entry produce exactly one
// redelivery.
func (s *source) moverLoop() {
	defer s.wg.Done()
	interval := s.b.cfg.MoverInterval
	if interval <= 0 {
		interval = DefaultMoverInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), interval*4)
		err := s.movePass(ctx)
		cancel()
		// A failed pass is a parked redelivery that is not happening, and a
		// good one is what brings the health back. Map of nil is nil.
		s.b.loopRound(s, moverFailing, s.b.errs.Map(mq.OpNack, s.topic, s.group, err))
	}
}

// movePass releases every parked entry whose time has come, one shard at a
// time.
func (s *source) movePass(ctx context.Context) error {
	now := s.b.now().UnixMilli()
	var first error
	for shard := range s.keys.Shards {
		zset := s.keys.Delayed(shard)
		v, err := s.b.cmder.Do(ctx, streams.ZRangeDue(zset, now, s.b.cfg.MoverCount))
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		members, err := streams.ParseMembers(v)
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		for _, member := range members {
			d, uerr := streams.UnpackDelayed(member)
			if uerr != nil {
				// A member this driver cannot decode is left in the sorted
				// set for an operator to look at rather than removed: the
				// alternative is deleting a message nobody has seen.
				if first == nil {
					first = uerr
				}
				continue
			}
			args := make([]string, 0, 2+len(d.Fields))
			args = append(args, member, strconv.FormatInt(s.b.cfg.MaxLen, 10))
			args = append(args, d.Fields...)
			cmd := streams.EvalSHA(streams.SHAOf(streams.ScriptDelayRelease),
				[]string{zset, s.keys.Stream(d.Shard)}, args)
			if _, rerr := s.b.cmder.Do(ctx, cmd); rerr != nil {
				if first == nil {
					first = rerr
				}
			}
		}
	}
	return first
}

// nextNonce is the random number that makes a consumer name unique. Sixty-four
// random bits keep two sources apart without a counter shared by every
// backend in the process, and keep two processes apart as well.
func nextNonce() uint64 { return rand.Uint64() } //nolint:gosec // uniqueness, not secrecy

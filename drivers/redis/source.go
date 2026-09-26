package redis

import (
	"context"
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
	pending []streams.Delivery

	// scratch is the entry buffer the read and the sweep fill, reused so
	// that a fetch allocates the deliveries and nothing else.
	scratch []streams.Entry

	wg     sync.WaitGroup
	stop   chan struct{}
	once   sync.Once
	closed atomic.Bool
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
	cons, err := streams.NewConsumer(streams.ConsumerConfig{
		Keys:             keys,
		Group:            group,
		Name:             name,
		Prefetch:         cfg.Prefetch,
		Block:            b.cfg.Block,
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
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for {
		if s.closed.Load() {
			return 0, mq.ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if n := s.drain(dst); n > 0 {
			return n, nil
		}
		// The read blocks at the server, which is what keeps an idle
		// consumer from polling. Its bound is Config.Block, never zero:
		// BLOCK 0 in Redis means "for ever", and a fetch that cannot be
		// woken is what the specification forbids.
		ds, scratch, err := s.cons.Read(ctx, s.b.cmder, s.scratch, nil)
		s.scratch = scratch
		if err != nil {
			if s.closed.Load() {
				return 0, mq.ErrClosed
			}
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, s.b.errs.Map(mq.OpFetch, s.topic, s.group, err)
		}
		if len(ds) == 0 {
			// Nothing arrived within the block. Looping is right: Fetch's
			// contract is to block until something is available, ctx is done
			// or the source is closed, and the block is the driver's own
			// bound rather than the caller's.
			continue
		}
		s.put(ds)
	}
}

// drain moves buffered deliveries into dst.
func (s *source) drain(dst []mq.DriverDelivery) int {
	s.mu.Lock()
	n := min(len(s.pending), len(dst))
	batch := s.pending[:n]
	s.pending = s.pending[n:]
	if len(s.pending) == 0 {
		s.pending = nil
	}
	s.mu.Unlock()
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
		d, ok := s.delivery(&batch[i])
		if !ok {
			continue
		}
		dst[out] = d
		out++
	}
	return out
}

// put buffers deliveries the read or the sweep produced, replacing a buffered
// delivery of the same entry.
//
// The replacement is what keeps the buffer from holding two generations of one
// entry: a sweep that re-claims a buffered entry produces a NEW generation,
// and the old one is refused by the guard the moment anything settles it.
// Replacing rather than appending means the caller is handed the generation
// that can actually be settled.
func (s *source) put(ds []streams.Delivery) {
	if len(ds) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
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
// — something has to settle it or it comes back for ever. The driver's answer
// is to report it through the health surface and leave it pending, which
// hands it to MaxAttempts and therefore to the dead-letter route. It
// is NOT delivered as an empty message: a handler given a message that is not
// the one that was published is worse than a message that never arrives.
func (s *source) delivery(d *streams.Delivery) (mq.DriverDelivery, bool) {
	m := &mq.Message{}
	if err := streams.Decode(m, d.Entry.Fields); err != nil {
		s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, s.topic, s.group, err))
		s.cons.Reclaimer().Lost(d.Key)
		return mq.DriverDelivery{}, false
	}
	if s.meta == metaEnvelope || s.meta == metaAdaptive {
		var out mq.Message
		if err := s.env.Unwrap(&out, m); err != nil {
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, s.topic, s.group, err))
			s.cons.Reclaimer().Lost(d.Key)
			return mq.DriverDelivery{}, false
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
	ref := *d
	return mq.DriverDelivery{
		Message: m,
		Info: mq.DeliveryInfo{
			Topic:        m.Topic,
			Source:       s.keys.Stream(d.Key.Shard),
			Subscription: s.group,
			ReceivedAt:   s.b.now(),
			// The lease is the DRIVER's: Redis grants none, and the reclaim
			// sweep is what enforces this one. Reporting it is what lets a
			// caller's lease scheduler decide when to extend.
			LeaseExpires: s.b.now().Add(s.cons.Config().AckDeadline),
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
	interval := s.b.cfg.ClaimInterval
	if interval <= 0 {
		interval = DefaultClaimInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), interval)
		ds, _, err := s.cons.Sweep(ctx, s.b.cmder, nil, nil)
		cancel()
		if err != nil {
			// The specification: a background failure surfaces through health, not only
			// through a log. A sweep that keeps failing is a subscription
			// that has stopped redelivering, which is exactly what a caller
			// polling Health needs to know.
			s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, s.topic, s.group, err))
			continue
		}
		if len(ds) > 0 {
			s.put(ds)
		}
	}
}

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
		if err := s.movePass(ctx); err != nil {
			s.b.health.Degraded(s.b.errs.Map(mq.OpNack, s.topic, s.group, err))
		}
		cancel()
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

// nonce is the source-instance counter that makes a consumer name unique.
//
// It is combined with the process's own start time in
// [defaultConsumerPrefix], so two processes on one host do not collide either.
var nonce atomic.Uint64

func nextNonce() uint64 { return nonce.Add(1) }

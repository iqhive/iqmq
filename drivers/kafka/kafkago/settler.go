package kafkago

import (
	"context"
	"errors"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/nack"
)

// The settler implements the mandatory settlement surface and the optional
// batched one.
//
// It does NOT implement [github.com/iqhive/iqmq.DeadlineExtender], and that is
// a decision rather than an omission: Kafka grants no per-record lease. What a
// Kafka consumer has is the group's own session timeout, which belongs to the
// member and not to a delivery, so there is nothing an Extend could move and a
// settler that pretended otherwise would let a handler believe it had bought
// time it did not have.
var (
	_ mq.Settler      = (*settler)(nil)
	_ mq.BatchSettler = (*settler)(nil)
)

// settler settles one source's deliveries (one per source).
type settler struct{ s *source }

// Ack settles a delivery positively.
//
// Positively means "this offset has completed", not "this offset is
// committed": the tracker records the completion and the watermark moves to
// the offset after the highest record whose completion can no longer lose
// work. A failed or unresolved offset below it blocks the commit, which is
// what makes the commit safe rather than merely recent.
//
// The commit itself is the commit loop's, [Backend.Commit]'s or the end of a
// generation's. Acknowledging is therefore cheap — no round trip — and the
// price is the at-least-once window, which is [Config.CommitInterval] wide and
// is documented on the driver page.
//
// It does NOT consult ctx, and that is deliberate. Acknowledging is a LOCAL
// operation on this driver — the offset completes in a tracker and the
// watermark moves — so there is no round trip for a context to bound, and
// refusing a completion because a context is cancelled would lose it and cause
// a redelivery for no reason. It also produces a wrong answer:
// `Delivery.Ack` takes no context, so the one the core supplies is cancelled
// when the CONSUMER closes, and a settler that checked it would report
// CodeCanceled for what is in fact a closed consumer. The answer for that
// is ErrClosed, and [source.resolve] is what gives it (the franz-go driver's
// The specification found this with 1,207 failures in 2,000 rounds).
func (st *settler) Ack(_ context.Context, d *mq.DriverDelivery) error {
	p, err := st.s.resolve(d)
	if err != nil {
		return err
	}
	st.s.complete(p, d)
	return nil
}

// AckBatch settles every delivery in ds positively.
//
// It is not an optimisation of a loop: a cumulative acknowledgement means the
// watermark after a batch is computed once and committed once, so a batch of a
// thousand costs the same commit as a batch of one. The per-item results are
// index-aligned with ds, which is the contract a partial response needs.
//
// The answer comes from [driverkit.CollapseBatch], which is the settlement
// rule as well as the publication one: it returns the single error only
// when EVERY delivery failed with the same outcome. This
// used to return the first failure whenever every delivery failed, and
// driverkit's batching settler gives a single error to every delivery of the
// batch — so a delivery whose settlement outcome was unknown would have been
// told it was definitely refused, which the specification's "ambiguous settlement stays
// explicitly unknown" forbids. Every error this loop can produce today is a
// definite one ([source.resolve] answers ErrClosed, not-settleable or
// lease-lost), so the collapse was sound here by accident rather than by
// construction; the rule is applied anyway, because the next error added to
// resolve should not have to remember it. NackBatch's mixture is reachable
// now.
func (st *settler) AckBatch(_ context.Context, ds []*mq.DriverDelivery) error {
	if len(ds) == 0 {
		return nil
	}
	errs := make([]error, len(ds))
	for i, d := range ds {
		p, err := st.s.resolve(d)
		if err != nil {
			errs[i] = err
			continue
		}
		st.s.complete(p, d)
	}
	return driverkit.CollapseBatch(errs)
}

// Nack settles a delivery negatively under o.
//
// Kafka has no per-record negative acknowledgement, so what this does is the
// strategy ([github.com/iqhive/iqmq/driverkit/nack]) — the same
// strategies the franz-go driver applies, with the same default. The plan it
// returns is followed literally, and the ORDER within a republishing plan is the specification: the record is published to its destination and confirmed FIRST, and
// only then is the original offset completed. A driver that completed the
// offset first would lose the record whenever the publication failed, which is
// the defect the specification calls out by name ("a failed or ambiguous DLQ publish never
// acks the original").
//
// Like Ack it does not consult ctx for its own sake — see Ack for why — but it
// does pass it on: a plan that republishes performs a produce, and that round
// trip is bounded by the context and by [Config.WriteTimeout].
func (st *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	s := st.s
	p, err := s.resolve(d)
	if err != nil {
		return err
	}
	plan, err := s.strategy.Plan(nack.Delivery{
		Topic:     d.Info.Source,
		Partition: d.Info.Partition,
		Offset:    d.Info.Offset,
		Attempt:   d.Info.Attempt,
	}, o)
	if err != nil {
		if errors.Is(err, mq.ErrUnsupported) && o.Action == mq.NackActionDeadLetter {
			return errNoDeadLetter
		}
		return err
	}
	return s.apply(ctx, p, d, plan)
}

// NackBatch settles every delivery in ds negatively under o.
//
// The answer comes from [driverkit.CollapseBatch], and here the mixture is
// reachable: a nack under a republishing strategy performs a produce, and a
// produce that runs out of [Config.WriteTimeout] is AMBIGUOUS while a
// delivery whose generation a rebalance superseded is definitely lease-lost.
// This returned the first failure whenever every delivery failed, and
// driverkit's batching settler gives one error to every delivery of the
// batch: the delivery whose dead-letter copy may already be in the log was
// told "lease lost", which the specification's "ambiguous settlement stays explicitly
// unknown" forbids.
func (st *settler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) error {
	if len(ds) == 0 {
		return nil
	}
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = st.Nack(ctx, d, o)
	}
	return driverkit.CollapseBatch(errs)
}

// apply performs one nack plan.
func (s *source) apply(ctx context.Context, p *partState, d *mq.DriverDelivery, plan nack.Plan) error {
	switch plan.Op {
	case nack.OpRetryInPlace:
		msg, ok := d.Native.(*kgo.Message)
		if !ok {
			return errNotSettleable
		}
		s.mu.Lock()
		// kafka-go has no pause to call, so plan.Pause is the hold itself:
		// takeLocal and absorb hand out nothing later of the partition
		// while the record is held, those records wait in the backlog, and
		// the partition's fetch loop stops in waitBacklogRoom once that
		// backlog is full, which is the pause a full tracking window gets.
		p.held = append(p.held, heldRecord{
			msg: msg, attempt: plan.Attempt,
			due:   s.b.now().Add(plan.Delay),
			pause: plan.Pause,
		})
		s.releaseLocked(p)
		s.mu.Unlock()
		// The waiting poll's timer was set before this record was held.
		signal(s.wake)
		return nil
	case nack.OpRepublish, nack.OpDeadLetter:
		// The publication first, the completion second, and nothing at all if
		// the publication fails: the ordering, and the reason
		// TestCriticalRetryTopicPublicationFails exists.
		if err := s.republish(ctx, p, d, plan); err != nil {
			return err
		}
		s.complete(p, d)
		return nil
	case nack.OpDrop:
		s.complete(p, d)
		return nil
	case nack.OpStop:
		s.mu.Lock()
		if s.stopped == nil {
			s.stopped = errStopped
		}
		s.releaseLocked(p)
		s.mu.Unlock()
		// The offset is deliberately NOT completed: the record is
		// unacknowledged, so whoever reopens this subscription gets it again.
		// Fetch reports errStopped from now on, which is how the halt reaches
		// the application (ADR-011).
		return nil
	default:
		return errNotSettleable
	}
}

// complete records that an offset has finished and advances the watermark.
//
// There is no partition to resume here, and that is a real difference from the
// franz-go driver rather than an omission: kafka-go offers no per-partition
// pause on either of its consumer surfaces, so a partition whose tracking
// window is full is held back by the source's own backlog, whose fetch loop
// waits while that backlog is full ([source.waitBacklogRoom]). A completion
// therefore makes room rather than lifting a pause, and the room is taken by
// the next Fetch.
func (s *source) complete(p *partState, d *mq.DriverDelivery) {
	s.mu.Lock()
	_, advanced := p.tr.Complete(d.Gen, d.Info.Offset)
	if advanced {
		p.dirty = true
	}
	delete(p.firstAt, d.Info.Offset)
	p.dropHeldLocked(d.Info.Offset)
	s.releaseLocked(p)
	ready := !p.stale && len(p.backlog) > 0 && !p.tr.Paused() && !p.blockedLocked()
	s.mu.Unlock()
	if ready {
		// Only Fetch hands a backlog out, and a Fetch already waiting
		// would otherwise wait for its whole deadline.
		signal(s.wake)
	}
}

// releaseLocked accounts one settled delivery and wakes a generation that is
// waiting for the partition to drain.
func (s *source) releaseLocked(p *partState) {
	if p.inflight > 0 {
		p.inflight--
	}
	if p.inflight == 0 {
		s.cond.Broadcast()
	}
}

// dropHeldLocked removes a redelivery this offset was waiting for, which is
// what a completion after a nack means: the caller nacked the delivery, then
// something completed the offset, and the held record must not be handed out
// for an offset the tracker has resolved.
func (p *partState) dropHeldLocked(off int64) {
	out := p.held[:0]
	for _, h := range p.held {
		if h.msg.Offset != off {
			out = append(out, h)
		}
	}
	p.held = out
}

// republish publishes the record to the plan's destination and waits for the
// confirmation.
//
// It goes through the backend's own SYNCHRONOUS writer, with acks=all, so a
// record moved to a retry topic or a dead-letter topic is as durable as one a
// caller published — and, more importantly, the settlement does not complete
// until the broker has said so. The provenance headers are stamped on
// a COPY of the record's headers: the fetched record is still the delivery's
// Native and other goroutines may be reading it.
func (s *source) republish(ctx context.Context, p *partState, d *mq.DriverDelivery, plan nack.Plan) error {
	msg, ok := d.Native.(*kgo.Message)
	if !ok {
		return errNotSettleable
	}
	dest, err := Topic(s.b.cfg.TopicPrefix, plan.Topic)
	if err != nil {
		return err
	}
	buf := s.b.getMsgbuf()
	// One reference is this goroutine's and one is the writer completion's,
	// exactly as on the publication paths: the produce below is bounded by a
	// context, and a context that ends the WAIT does not end the WRITE.
	defer buf.release()
	buf.route.buf = buf
	buf.msg.Topic = dest
	buf.msg.Key = msg.Key
	buf.msg.Value = msg.Value
	buf.msg.Headers = append(buf.msg.Headers[:0], msg.Headers...)
	buf.msg.Time = msg.Time
	buf.msg.WriterData = &buf.route
	s.mu.Lock()
	firstAt := p.firstAt[d.Info.Offset]
	s.mu.Unlock()
	stampProvenance(&buf.msg, &d.Info, plan.Attempt, plan.Reason, firstAt)

	// The produce is bounded. kafka-go retries a retryable produce error
	// MaxAttempts times with a backoff between them, and a settlement that
	// blocked a handler for that long would take the consumer out of its group
	// long before it returned. The bound is the request timeout, and the
	// outcome of a produce that runs out of it is ambiguous rather than
	// failed.
	pctx, cancel := s.b.bound(ctx)
	defer cancel()
	if err := s.b.w.WriteMessages(pctx, buf.msg); err != nil {
		var werr kgo.WriteErrors
		if errors.As(err, &werr) && len(werr) == 1 && werr[0] != nil {
			err = werr[0]
		}
		// The publication failed or its outcome is unknown. Either way the
		// original offset stays uncommitted and the delivery stays settleable,
		// so the caller may nack again: the specification forbids acknowledging the
		// original on a failed or ambiguous move.
		mapped := mapSent(s.b.errs, mq.OpNack, TopicOf(s.b.cfg.TopicPrefix, dest), s.groupID, err)
		var oe *mq.OpError
		if errors.As(mapped, &oe) && oe.Ambiguous {
			s.b.ambiguous.Add(1)
			s.b.duplicable.Add(1)
		}
		return mapped
	}
	return nil
}

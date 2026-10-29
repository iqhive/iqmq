package redis

import (
	"context"
	"errors"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/streams"
)

// The settler implements the mandatory settlement surface, the optional
// batched one and the optional lease extension.
//
// The batched surface is the one that earns its keep here: a settlement on
// this broker is a round trip, so acknowledging a hundred deliveries one at a
// time is a hundred round trips and acknowledging them as a batch is one
// pipelined command per shard. [driverkit.BatchSettler] is what coalesces
// them, and it goes through AckBatch below because this type implements it.
var (
	_ mq.Settler          = (*settler)(nil)
	_ mq.BatchSettler     = (*settler)(nil)
	_ mq.DeadlineExtender = (*settler)(nil)
)

// settler settles one source's deliveries.
type settler struct {
	src *source

	once  sync.Once
	batch *driverkit.BatchSettler
	wrap  mq.Settler
}

// settler returns the value Source.Settler hands the core: this settler
// wrapped in driverkit's coalescing one.
//
// The coalescing strategy is Immediate, which is [driverkit]'s default and is
// deliberate rather than lazy: the other three hold a settlement the caller
// believes has happened, and on a broker whose acknowledgement is one
// pipelined command the round trip saved is not worth the window in which a
// crash loses an acknowledgement the application already saw succeed. What
// the wrapper still buys is the batch route: a core that settles a whole
// fetch's worth of deliveries at once reaches AckBatch in one call.
//
// The kit's own value is what goes to the core now. This driver used to wrap
// it in a facade of its own because driverkit.BatchSettler.Settler() returned
// something that implemented Settler and DeadlineExtender and NOT
// [github.com/iqhive/iqmq.BatchSettler] — so a driver whose settler had a
// native batch could not expose it through the kit, which was the omission one surface along. Driverkit.WrapSettler
// now preserves every optional settlement surface the specification defines, with a batched
// settlement going straight to the native batch and a single one through the
// coalescing queue, which is exactly what the facade did.
func (s *settler) settler() mq.Settler {
	s.once.Do(func() {
		b, err := driverkit.NewBatchSettler(s, driverkit.BatchConfig{
			Batching: driverkit.SettleImmediate,
			Clock:    s.src.b.cfg.Clock,
		})
		if err != nil {
			// NewBatchSettler refuses only a nil settler and an invalid
			// configuration, neither of which is reachable from here; using
			// the bare settler rather than panicking keeps a future
			// configuration mistake from taking the process down.
			s.wrap = driverkit.WrapSettler(s, s)
			return
		}
		s.batch = b
		s.wrap = b.Settler()
	})
	return s.wrap
}

// close releases the coalescing settler's goroutine, if it has one.
func (s *settler) close() {
	if s.batch != nil {
		_ = s.batch.Close()
	}
}

// Ack settles d positively.
func (s *settler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	errs := s.AckBatchResults(ctx, []*mq.DriverDelivery{d})
	return errs[0]
}

// AckBatch settles every delivery in ds positively, reporting per-delivery
// outcomes through a *BatchError whose Errs are index-aligned with ds.
//
// The answer comes from [driverkit.CollapseBatch], which is the settlement
// rule as well as the publication one: the single error only when EVERY
// delivery failed with the same outcome. It used to be
// the first failure whenever every delivery failed, and the mixture is
// reachable here — [settler.ref] refuses a tampered delivery with a definite
// ErrLeaseLost without touching the server, while an XACK whose round trip
// failed after the command was written is CodeSettlementUnknown. Since
// [driverkit.BatchSettler] gives one error to every delivery of the batch,
// that told the delivery whose acknowledgement may have landed that its lease
// was lost, which the specification's "ambiguous settlement stays explicitly unknown"
// forbids.
func (s *settler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	return driverkit.CollapseBatch(s.AckBatchResults(ctx, ds))
}

// AckBatchResults is AckBatch with the per-item results exposed, which is
// what Ack needs and what the driver's own tests assert on.
func (s *settler) AckBatchResults(ctx context.Context, ds []*mq.DriverDelivery) []error {
	errs := make([]error, len(ds))
	batch := make([]streams.Delivery, 0, len(ds))
	at := make([]int, 0, len(ds))
	for i, d := range ds {
		ref, err := s.ref(d)
		if err != nil {
			errs[i] = err
			continue
		}
		batch = append(batch, *ref)
		at = append(at, i)
	}
	if len(batch) == 0 {
		return errs
	}
	inner := s.src.cons.Ack(ctx, s.src.b.cmder, batch)
	// An acknowledgement can lift a Stop hold.
	s.src.nudge()
	for n, i := range at {
		errs[i] = s.mapSettle(mq.OpAck, inner[n])
	}
	return errs
}

// Nack settles d negatively under o.
func (s *settler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	ref, err := s.ref(d)
	if err != nil {
		return err
	}
	if o.Action == mq.NackActionDeadLetter && s.src.cons.Config().DeadLetterStream == "" {
		return errNoDeadLetter
	}
	err = s.src.cons.Nack(ctx, s.src.b.cmder, *ref, o, s.src.b.now())
	if err == nil && o.Action == mq.NackActionRequeue && s.src.cons.Strategy().Kind() == streams.Stop {
		s.src.handBack(ctx, ref.Key)
	}
	// A drop or a dead-letter can lift a Stop hold.
	s.src.nudge()
	return s.mapSettle(mq.OpNack, err)
}

// NackBatch settles every delivery in ds negatively under o.
//
// It is a loop and not one command, and that is a property of the strategies
// rather than an omission: two of the four send nothing at all, and the other
// two send a script call per entry because each one moves ONE entry
// atomically. A script that moved a batch would either be one transaction per
// entry anyway or lose the per-entry ownership check that keeps it from
// settling another consumer's delivery.
func (s *settler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) error {
	errs := make([]error, len(ds))
	for i, d := range ds {
		errs[i] = s.Nack(ctx, d, o)
	}
	return driverkit.CollapseBatch(errs)
}

// Extend moves d's lease deadline by at least by.
//
// It is XCLAIM ... JUSTID inside an ownership-checking script, and both halves
// matter. JUSTID resets the entry's idle clock WITHOUT incrementing its
// delivery counter, so extending a lease twenty times does not make a slow
// handler look like a message that keeps failing. The script is because XCLAIM
// is a CLAIM: a bare `XCLAIM key group me 0 <id> JUSTID` takes the entry from
// whoever holds it, so an extension written without the check steals an entry
// another consumer is processing and reports success.
//
// `by` is not sent. Redis has no "extend by": the lease is this driver's, its
// length is the configured deadline, and what the extension does is reset the
// idle clock the deadline is measured from. Honouring a longer `by` would
// require a per-entry deadline the broker has nowhere to keep, so the driver
// resets the lease it has rather than pretending to a lease it does not. A
// `by` longer than that deadline is refused before anything is sent, because
// the sweep would reclaim the entry one deadline later underneath a handler
// that was told it had `by`.
func (s *settler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	ref, err := s.ref(d)
	if err != nil {
		return err
	}
	// The local lease check runs first so that a delivery the sweep has
	// already handed out again answers ErrLeaseLost whatever it was asked
	// for: that is the answer that tells the handler to stop. Extend repeats
	// it, which costs a map lookup.
	if err := s.src.cons.Reclaimer().Check(ref.Key, ref.Gen); err != nil {
		return err
	}
	if by > s.src.cons.Config().AckDeadline {
		return errExtendTooLong
	}
	errs := s.src.cons.Extend(ctx, s.src.b.cmder, []streams.Delivery{*ref})
	// An extension that finds the entry taken lifts its Stop hold.
	s.src.nudge()
	return s.mapSettle(mq.OpExtend, errs[0])
}

// ref recovers the delivery bookkeeping the source put in DriverDelivery.Ref.
//
// A delivery whose Ref is not one of ours is not settleable: it came from
// another source, or from a zero DriverDelivery. The specification makes that
// ErrNotSettleable rather than a panic.
func (s *settler) ref(d *mq.DriverDelivery) (*streams.Delivery, error) {
	if d == nil {
		return nil, mq.ErrNotSettleable
	}
	ref, ok := d.Ref.(*streams.Delivery)
	if !ok || ref == nil {
		return nil, mq.ErrNotSettleable
	}
	if ref.Gen != d.Gen {
		// The core hands back its own copy of the struct, so the generation
		// in the Ref and the one in the delivery are the same value written
		// once. A mismatch is a delivery that was tampered with, and
		// applying a settlement from it would settle a record this source
		// cannot vouch for.
		return nil, mq.ErrLeaseLost
	}
	return ref, nil
}

// mapSettle turns the command layer's error into the model.
//
// ErrLeaseLost passes through unchanged: it is already the answer the specification asks
// for, and wrapping it in an *OpError whose code says something else is how a
// caller comes to retry a settlement that can never succeed.
func (s *settler) mapSettle(op string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mq.ErrLeaseLost):
		return err
	case errors.Is(err, mq.ErrUnsupported):
		return err
	case errors.Is(err, mq.ErrSettlementInProgress):
		return err
	}
	// A settlement whose round trip failed after the command was written has
	// an UNKNOWN outcome, and the specification makes an unknown outcome sticky. Every
	// failure here is post-write, so sentAlready is true and the rule table
	// plus driverkit.ClassifyNet decide the rest — see [sink.fail] for why
	// that needs no "is this a reply?" branch.
	return s.src.b.errs.MapNet(op, s.src.topic, s.src.group, err, true)
}

// collapse is gone. It answered a wholly failed batch with its first error
// whatever the other entries said, which is the rule driverkit.CollapseBatch
// replaced on the publish side  and now on the settlement
// side too. Its absence is deliberate: a call site that
// reverted to the old rule would no longer compile.

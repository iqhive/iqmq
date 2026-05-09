package driverkit

import (
	"context"
	"sync"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// SettleBatching is how a [BatchSettler] decides when to send.
type SettleBatching uint8

// The batching strategies. All four ship: which one wins is a bake-off, and a
// driver whose broker charges per round trip picks a different one from a driver
// whose broker does not.
const (
	// SettleImmediate sends every settlement on its own. It is the zero
	// value because it is the only strategy that adds no latency and holds
	// no settlement the caller believes has happened.
	//
	// It is a LATENCY guarantee and not a one-request-per-settlement one,
	// and the name does not say so: submit appends to the queue and then
	// flushes everything in it, so two settlements that arrive before either
	// flush runs travel together under every strategy, this one included.
	// Ten concurrent acknowledgements really do take fewer than ten requests
	// here. What this strategy promises is that no settlement WAITS for
	// another.
	SettleImmediate SettleBatching = iota
	// SettleCount sends once MaxItems settlements have accumulated.
	SettleCount
	// SettleTime sends every Interval.
	SettleTime
	// SettleHybrid sends at whichever of the two comes first.
	SettleHybrid

	settleBatchingCount
)

var settleBatchingNames = [settleBatchingCount]string{"immediate", "count", "time", "hybrid"}

// String returns the strategy's lower-case name.
func (s SettleBatching) String() string {
	if s < settleBatchingCount {
		return settleBatchingNames[s]
	}
	return "SettleBatching(?)"
}

// Valid reports whether s names a strategy.
func (s SettleBatching) Valid() bool { return s < settleBatchingCount }

// BatchConfig configures a [BatchSettler].
type BatchConfig struct {
	// Batching is when to send. The zero value is SettleImmediate.
	Batching SettleBatching
	// MaxItems is the batch size SettleCount and SettleHybrid send at, and
	// the ceiling every strategy splits a larger batch on. Zero means
	// DefaultSettleBatch.
	MaxItems int
	// Interval is how often SettleTime and SettleHybrid send. Zero means
	// DefaultSettleInterval.
	Interval time.Duration
	// Clock is the time source the ticker reads. Nil means the wall clock.
	Clock mq.Clock
	// OnResult observes each settlement's outcome as the batch resolves,
	// which is the per-item result callback a partial response needs. It is
	// called outside the settler's lock and must not block.
	OnResult func(d *mq.DriverDelivery, err error)
}

// The batching defaults.
const (
	// DefaultSettleBatch is the batch size a driver gets by not saying. Ten
	// is SQS's DeleteMessageBatch limit, which is the smallest of the
	// brokers that batch settlement at all.
	DefaultSettleBatch = 10
	// DefaultSettleInterval is how often a time-based settler sends.
	DefaultSettleInterval = 50 * time.Millisecond
)

// BatchSettler coalesces settlements into the batched round trip a broker
// charges per call for.
//
// It wraps a driver's own [bitbucket.org/iqhive/iqmq.Settler]. When that settler is
// also a [bitbucket.org/iqhive/iqmq.BatchSettler] the coalesced batch goes through
// AckBatch and NackBatch, whose per-item results are index-aligned with the
// batch; when it is not, the settler loops, which costs the round trips the
// broker was going to charge anyway.
//
// Ack and Nack return at the configured boundary: a caller's Ack does not
// return until the batch it joined has been sent and its own item resolved.
// That is what keeps true through the coalescing — a settlement that has
// returned nil has happened at the broker — and it is why the immediate
// strategy is the default: the others trade that latency for round trips
// deliberately.
type BatchSettler struct {
	inner   mq.Settler
	batch   mq.BatchSettler
	cfg     BatchConfig
	clock   mq.Clock
	maxItem int
	every   time.Duration

	mu      sync.Mutex
	pending []settleItem
	closed  bool

	wg   sync.WaitGroup
	stop chan struct{}
}

// settleItem is one settlement waiting for its batch.
type settleItem struct {
	d    *mq.DriverDelivery
	opts mq.NackOptions
	done chan error
	nack bool
}

// NewBatchSettler wraps a driver's settler in a coalescing one.
func NewBatchSettler(inner mq.Settler, cfg BatchConfig) (*BatchSettler, error) {
	if inner == nil {
		return nil, errNilSettler
	}
	if !cfg.Batching.Valid() {
		return nil, errBatching
	}
	if cfg.MaxItems < 0 || cfg.Interval < 0 {
		return nil, errBatchBounds
	}
	b := &BatchSettler{
		inner:   inner,
		cfg:     cfg,
		clock:   cfg.Clock,
		maxItem: cfg.MaxItems,
		every:   cfg.Interval,
		stop:    make(chan struct{}),
	}
	if bs, ok := inner.(mq.BatchSettler); ok {
		b.batch = bs
	}
	if b.maxItem == 0 {
		b.maxItem = DefaultSettleBatch
	}
	if b.every == 0 {
		b.every = DefaultSettleInterval
	}
	if b.clock == nil {
		b.clock = wallClock{}
	}
	if cfg.Batching == SettleTime || cfg.Batching == SettleHybrid {
		b.wg.Add(1)
		go b.tick()
	}
	return b, nil
}

// Settler returns the value a driver's Source.Settler should hand the core:
// this settler, carrying every optional settlement surface the driver's own
// settler has.
//
// A batch that arrives here already batched goes straight to the driver's
// native AckBatch or NackBatch rather than back through the coalescing queue:
// coalescing exists to CREATE a batch out of single settlements, and
// re-splitting one the caller has already assembled would cost the round trips
// the surface exists to save.
func (b *BatchSettler) Settler() mq.Settler { return WrapSettler(b, b.inner) }

// Ack settles a delivery positively, at the configured boundary.
func (b *BatchSettler) Ack(ctx context.Context, d *mq.DriverDelivery) error {
	return b.submit(ctx, settleItem{d: d})
}

// Nack settles a delivery negatively, at the configured boundary.
func (b *BatchSettler) Nack(ctx context.Context, d *mq.DriverDelivery, o mq.NackOptions) error {
	return b.submit(ctx, settleItem{d: d, opts: o, nack: true})
}

// submit queues one settlement and waits for its outcome.
func (b *BatchSettler) submit(ctx context.Context, item settleItem) error {
	item.done = make(chan error, 1)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return mq.ErrClosed
	}
	b.pending = append(b.pending, item)
	send := b.cfg.Batching == SettleImmediate ||
		((b.cfg.Batching == SettleCount || b.cfg.Batching == SettleHybrid) && len(b.pending) >= b.maxItem)
	b.mu.Unlock()
	if send {
		b.flush(ctx)
	}
	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush sends everything queued and waits for it, which is what a source does
// before it closes and what a caller does when it wants its settlements at the
// broker now.
func (b *BatchSettler) Flush(ctx context.Context) error {
	b.flush(ctx)
	return ctx.Err()
}

// Close flushes and stops the ticker. It is idempotent, and a settlement
// afterwards reports ErrClosed.
//
// The closed flag and the queue are taken under one lock, so a settlement that
// arrives while Close is draining either lands before the drain and is sent, or
// is refused: there is no window in which one is queued behind a drain that has
// already run. That is the engine's recurring defect class applied here.
func (b *BatchSettler) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.wg.Wait()
		return nil
	}
	b.closed = true
	held := b.pending
	b.pending = nil
	b.mu.Unlock()
	close(b.stop)
	b.wg.Wait()
	if len(held) > 0 {
		b.send(context.Background(), held)
	}
	return nil
}

// tick is the shared ticker of the time-based strategies. There is one per
// settler and never one timer per delivery.
func (b *BatchSettler) tick() {
	defer b.wg.Done()
	timer := b.clock.NewTimer(b.every)
	defer timer.Stop()
	for {
		select {
		case <-timer.C():
			b.flush(context.Background())
			timer.Reset(b.every)
		case <-b.stop:
			return
		}
	}
}

// flush takes everything queued and sends it in batches of at most MaxItems.
func (b *BatchSettler) flush(ctx context.Context) {
	b.mu.Lock()
	held := b.pending
	b.pending = nil
	b.mu.Unlock()
	for len(held) > 0 {
		n := min(len(held), b.maxItem)
		b.send(ctx, held[:n])
		held = held[n:]
	}
}

// send settles one batch, grouping consecutive items that can travel together: a
// run of acks, or a run of nacks that ask for the same thing. A nack carries its
// own action and delay, and NackOptions is per batch, so two nacks that
// disagree are two batches.
func (b *BatchSettler) send(ctx context.Context, items []settleItem) {
	for len(items) > 0 {
		n := 1
		for n < len(items) && items[n].nack == items[0].nack && items[n].opts == items[0].opts {
			n++
		}
		b.sendRun(ctx, items[:n])
		items = items[n:]
	}
}

// sendRun settles one homogeneous run.
func (b *BatchSettler) sendRun(ctx context.Context, items []settleItem) {
	if b.batch == nil || len(items) == 1 {
		for i := range items {
			var err error
			if items[i].nack {
				err = b.inner.Nack(ctx, items[i].d, items[i].opts)
			} else {
				err = b.inner.Ack(ctx, items[i].d)
			}
			b.resolve(&items[i], err)
		}
		return
	}
	ds := make([]*mq.DriverDelivery, len(items))
	for i := range items {
		ds[i] = items[i].d
	}
	var err error
	if items[0].nack {
		err = b.batch.NackBatch(ctx, ds, items[0].opts)
	} else {
		err = b.batch.AckBatch(ctx, ds)
	}
	b.distribute(items, err)
}

// distribute hands each item its own outcome, which for a partial batch is the
// entry of the *BatchError at its index.
//
// Index alignment is the whole contract. A result aligned with anything but the
// batch reports one delivery's outcome for another, which is how a batch
// settlement acknowledges a message nobody processed.
func (b *BatchSettler) distribute(items []settleItem, err error) {
	var be *mq.BatchError
	if asBatchError(err, &be) && len(be.Errs) == len(items) {
		for i := range items {
			b.resolve(&items[i], be.Errs[i])
		}
		return
	}
	for i := range items {
		b.resolve(&items[i], err)
	}
}

// resolve completes one item.
func (b *BatchSettler) resolve(item *settleItem, err error) {
	if b.cfg.OnResult != nil {
		b.cfg.OnResult(item.d, err)
	}
	item.done <- err
}

// asBatchError is errors.As for a *BatchError without the reflection.
func asBatchError(err error, dst **mq.BatchError) bool {
	for err != nil {
		if be, ok := err.(*mq.BatchError); ok { //nolint:errorlint // the walk below is the unwrap
			*dst = be
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// WrapSettler returns wrapper carrying every optional settlement surface the specification
// defines that wrapper or inner has: the mandatory Ack and Nack are wrapper's,
// and [bitbucket.org/iqhive/iqmq.DeadlineExtender] and
// [bitbucket.org/iqhive/iqmq.BatchSettler] are preserved from whichever of the two
// implements them, wrapper first.
//
// The surfaces are preserved and never invented. A driver with no lease to
// extend still presents a settler that is not a DeadlineExtender, and one with
// no native batch still presents one that is not a BatchSettler, so the core
// answers ErrUnsupported at open and offers the broker nothing it has not got.
//
// The result presents the SPI's surfaces and nothing else, whatever else
// wrapper happens to have: a [BatchSettler]'s own Flush(ctx) error would
// otherwise satisfy [bitbucket.org/iqhive/iqmq.FlushSink] by accident, and a
// settler telling the core it buffers publications is the same kind of
// dishonest surface in the other direction. A driver that wants the kit's
// Flush holds the [BatchSettler] itself.
func WrapSettler(wrapper, inner mq.Settler) mq.Settler {
	if wrapper == nil {
		return nil
	}
	ex, _ := wrapper.(mq.DeadlineExtender)
	if ex == nil {
		ex, _ = inner.(mq.DeadlineExtender)
	}
	bs, _ := wrapper.(mq.BatchSettler)
	if bs == nil {
		bs, _ = inner.(mq.BatchSettler)
	}
	base := settlerFacade{Settler: wrapper}
	switch {
	case ex == nil && bs == nil:
		return base
	case bs == nil:
		return extendingSettler{settlerFacade: base, extender: ex}
	case ex == nil:
		return batchingSettler{settlerFacade: base, batch: bs}
	default:
		return extendingBatchingSettler{
			batchingSettler: batchingSettler{settlerFacade: base, batch: bs},
			extender:        ex,
		}
	}
}

// settlerFacade presents exactly [bitbucket.org/iqhive/iqmq.Settler] over the
// settler it wraps. The three types below add one optional surface each on top
// of it, which is how the method set says what the driver underneath can do:
// Go decides an interface assertion from the dynamic type, so the surfaces have
// to be spelled as types rather than chosen at run time.
type settlerFacade struct {
	mq.Settler
}

// Unwrap returns the settler the facade was built over, so a driver can reach
// past the adapter.
func (s settlerFacade) Unwrap() mq.Settler { return s.Settler }

// extendingSettler is a settler wrapper that delegates Extend.
type extendingSettler struct {
	settlerFacade
	extender mq.DeadlineExtender
}

// Extend delegates to the extender the wrapper was built with.
func (e extendingSettler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	return e.extender.Extend(ctx, d, by)
}

// batchingSettler is a settler wrapper that delegates the batched surface.
type batchingSettler struct {
	settlerFacade
	batch mq.BatchSettler
}

// AckBatch delegates to the driver's own batch, whose per-delivery results are
// index-aligned with ds.
func (b batchingSettler) AckBatch(ctx context.Context, ds []*mq.DriverDelivery) error {
	return b.batch.AckBatch(ctx, ds)
}

// NackBatch delegates to the driver's own batch.
func (b batchingSettler) NackBatch(ctx context.Context, ds []*mq.DriverDelivery, o mq.NackOptions) error {
	return b.batch.NackBatch(ctx, ds, o)
}

// extendingBatchingSettler is the settler of a broker that has both: a
// visibility timeout and a batched settlement call. SQS and Redis are both.
type extendingBatchingSettler struct {
	batchingSettler
	extender mq.DeadlineExtender
}

// Extend delegates to the extender the wrapper was built with.
func (e extendingBatchingSettler) Extend(ctx context.Context, d *mq.DriverDelivery, by time.Duration) error {
	return e.extender.Extend(ctx, d, by)
}

// wallClock is the default time source: package time, which follows the bubble
// clock inside testing/synctest.
type wallClock struct{}

// Now returns the current time.
func (wallClock) Now() time.Time { return timeNow() }

// NewTimer returns a timer that fires once after d.
func (wallClock) NewTimer(d time.Duration) mq.Timer { return &wallTimer{t: time.NewTimer(d)} }

// wallTimer adapts *time.Timer to mq.Timer.
type wallTimer struct{ t *time.Timer }

func (w *wallTimer) C() <-chan time.Time        { return w.t.C }
func (w *wallTimer) Stop() bool                 { return w.t.Stop() }
func (w *wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// The batch settler's failures.
var (
	errNilSettler  = errcode.New(errcode.InvalidArgument, "iqmq/driverkit: NewBatchSettler was given no settler")
	errBatching    = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: the batching strategy is not one of immediate, count, time or hybrid")
	errBatchBounds = errcode.New(errcode.InvalidConfig, "iqmq/driverkit: a BatchSettler's MaxItems and Interval cannot be negative")
)

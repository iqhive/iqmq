package iqmq

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/lifecycle"
)

// The prefetch defaults and limits.
const (
	// DefaultPrefetch is the portable default. A driver may prefetch
	// fewer; none prefetches more than it is told.
	DefaultPrefetch = 128
	// MaxPrefetch bounds what a caller may ask for. A prefetch is a promise
	// to hold that many unsettled messages in memory, so it is bounded like
	// every other buffer.
	MaxPrefetch = 4096
)

// ConsumerConfig is what a consumer is opened with.
//
// It is a struct and not a list of positional parameters, which is the specification's
// winner (ADR-006, Struct): every parameter is named at the call site, and
// the two required strings cannot be exchanged by accident the way
// Consumer(ctx, "orders", "billing") can.
//
// Topic and Subscription are required. Everything else has a documented
// default, and the zero value of every field is that default.
type ConsumerConfig struct {
	// Topic is the topic, subject or queue to consume. A driver that
	// supports FeatureWildcardSubscribe accepts its pattern syntax here.
	Topic string
	// Subscription is the durable subscription, consumer group or queue
	// name. It is what makes two processes share a stream rather than each
	// receive all of it.
	Subscription string
	// Start is where a subscription with no committed position begins. The
	// zero value is StartLatest.
	Start StartPosition
	// Prefetch is how many messages the driver may hold unclaimed. Zero
	// means DefaultPrefetch. It also sizes the consumer's own fetch buffer,
	// so a Receive never asks the driver for more than this many messages
	// at once.
	Prefetch int

	// MaxInFlight bounds unsettled deliveries; zero follows the prefetch.
	MaxInFlight int
	// AckDeadline is how long a delivery's lease should last. Zero means
	// the broker's own default. A driver that cannot set it says so in
	// FeatureExtendLease and on its page.
	AckDeadline time.Duration
	// Metadata is the metadata mode. The zero value is
	// MetadataNativeStrict.
	Metadata MetadataMode
	// BufferReuse lets the consumer recycle a delivery's settlement storage
	// once the delivery is settled, which is the reuse mode: it takes
	// the receive path to zero allocations per message, and a Delivery must
	// not be retained past its settlement. The default is the safe
	// mode, where nothing is recycled (ADR-009).
	BufferReuse bool
	// Requirements are the capabilities the consumer needs. They are checked
	// before the source is opened, so a consumer that cannot work fails at
	// construction rather than at the first message.
	Requirements []Requirement
}

// resolve validates the configuration and fills in every default, so that
// nothing below this point has to ask what a zero means.
func (cfg ConsumerConfig) resolve() (ConsumerConfig, error) {
	switch {
	case cfg.Topic == "":
		return cfg, errConsumerNoTopic
	case cfg.Subscription == "":
		return cfg, errConsumerNoSubscription
	case cfg.Prefetch < 0 || cfg.Prefetch > MaxPrefetch:
		return cfg, errConsumerPrefetch
	case cfg.MaxInFlight < 0:
		return cfg, errConsumerMaxInFlight
	case cfg.AckDeadline < 0:
		return cfg, errConsumerAckDeadline
	case !cfg.Metadata.Valid():
		return cfg, errConsumerMetadata
	case !cfg.Start.Kind().Valid():
		return cfg, errConsumerStart
	}
	if cfg.Prefetch == 0 {
		cfg.Prefetch = DefaultPrefetch
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = cfg.Prefetch
	}
	cfg.Start = cfg.Start.Resolve()
	return cfg, nil
}

// ConsumerHandle is one open subscription.
//
// It is a concrete struct and not an interface, which is the winner
// (ADR-003, Concrete): the narrow interfaces are for callers who want to
// substitute a consumer, and the handle is what a driver's Open returns.
//
// Receive is called by one goroutine at a time — the specification's "one active Receive
// per consumer" — and settlement may happen on any goroutine, concurrently
// with a fetch and with other settlements. Close and Shutdown are idempotent
// and safe from any goroutine.
type ConsumerHandle struct {
	client *Client
	cfg    ConsumerConfig
	src    Source
	// settler is the source's settler, read once at open. A nil settler is AckNone model and makes every settlement ErrNotSettleable.
	settler Settler
	// extender is the settler's optional DeadlineExtender, asserted
	// once at open and never on a delivery's path (rule 4). It is nil
	// for a driver that cannot extend a lease, which is what makes
	// Delivery.Extend answer ErrUnsupported.
	extender DeadlineExtender
	life     *lifecycle.Lifecycle
	// settleCtx bounds every settlement. It is the handle's own context and
	// not the caller: the specification forbids retaining a caller's context beyond
	// the operation it was given for, and the operation that opened this
	// consumer is over. It is cancelled when the handle shuts down, which
	// is what unblocks a settlement parked in a driver.
	settleCtx    context.Context
	cancelSettle context.CancelFunc
	clock        clock.Clock
	hooks        Hooks

	// running is Run's exclusion: exactly one runner may be active on a
	// consumer at a time. It is on the handle and
	// not on the runner because it is the handle a second caller has, and
	// the refusal has to be reachable from there.
	running atomic.Bool
	// run is the active runner, or nil. It is what the shutdown order
	// reaches: "stop fetching" and "wait for in-flight handlers within
	// DrainTimeout" are the runner's, and a consumer that closed without
	// them would close its source under a handler still holding a delivery.
	run atomic.Pointer[runner]

	// fetchGate serialises Receive. The specification allows one active Receive per
	// consumer; serialising rather than refusing keeps a second caller
	// correct instead of racing on the fetch buffer.
	//
	// It is a one-token channel and not a mutex, because sync.Mutex.Lock
	// cannot be cancelled: a second caller behind a fetch parked in a driver
	// would wait for that fetch however short its own context was, which is
	// the defect the specification found and named. The token is put in
	// at open, so the zero value of the handle is never a usable gate — and
	// nothing constructs a ConsumerHandle but newConsumer.
	fetchGate chan struct{}
	// buf is the fetch buffer, allocated once at open and reused for ever:
	// Fetch fills a caller slice precisely so that a fetch allocates
	// nothing.
	buf []DriverDelivery

	// reuse is ConsumerConfig.BufferReuse, compiled into a boolean at
	// construction (rule 4).
	reuse bool
	free  recordFreeList

	counters consumerCounters
}

// Receive fills dst with up to len(dst) deliveries and returns how many.
//
// It blocks until at least one delivery is available, ctx is done or the
// consumer is closed. It is the winner (ADR-006, Value): the caller owns
// the slice and the deliveries live in its backing array, so a receive costs
// no allocation per message and no container per batch.
//
// The deliveries are addressable and must be settled where they lie:
// dst[i].Ack() settles delivery i. A copy taken out of the slice is a copy of
// a delivery — it can settle it, and it cannot tell that something else
// already has — which is what the specification's "exactly one settlement wins" answers.
//
// A short return is normal: Receive returns what the driver had rather than
// waiting for the slice to fill. A non-nil error may accompany a non-zero
// count, and the deliveries returned with it are valid.
//
// After the consumer is closed Receive returns ErrClosed, and it returns
// ErrClosed rather than deliveries for a fetch that completed while the
// consumer was closing: the source requeues what it had handed over, so the
// messages are not lost — they are redelivered.
func (c *ConsumerHandle) Receive(ctx context.Context, dst []Delivery) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if err := c.enterFetch(ctx); err != nil {
		return 0, err
	}
	defer func() { c.fetchGate <- struct{}{} }()
	// The fetch itself is not bracketed by a lifecycle ticket: it blocks,
	// and a phase transition waits for every admitted operation to leave
	// before it drains, so a ticket held across a parked Fetch would park
	// the shutdown with it. The specification requires Source.Close to unblock a parked
	// Fetch, and the materialisation below — which is what actually hands a
	// delivery over — takes the ticket.
	if !c.life.Allow(lifecycle.OpFetch) {
		return 0, ErrClosed
	}
	n := min(len(dst), len(c.buf))
	var start time.Time
	if c.hooks.OnReceive != nil {
		start = c.clock.Now()
	}
	got, err := c.src.Fetch(ctx, c.buf[:n])
	switch {
	case got < 0 || got > n:
		// The driver reported more deliveries than the buffer holds, so
		// there is nothing here that can be read: dst[:n] is what a Fetch
		// fills and n is what it was given.
		got, err = 0, errDriverFetchCount
	case got > 0:
		var ferr error
		got, ferr = c.admit(dst, got)
		if ferr != nil {
			err = ferr
		}
	}
	if c.hooks.OnReceive != nil {
		c.hooks.OnReceive(ctx, ReceiveEvent{
			Topic:        c.cfg.Topic,
			Subscription: c.cfg.Subscription,
			Messages:     got,
			Duration:     c.clock.Now().Sub(start),
			Err:          err,
		})
	}
	return got, err
}

// enterFetch takes the one fetch token, or reports why it could not: the
// caller's context is done, or the consumer closed while it waited.
//
// The closed case is checked on the way in as well as on the way out, so that
// a caller queued behind a fetch is told ErrClosed rather than waiting for a
// gate a closing consumer will never hand over.
func (c *ConsumerHandle) enterFetch(ctx context.Context) error {
	select {
	case <-c.fetchGate:
		return nil
	default:
	}
	select {
	case <-c.fetchGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.life.Done():
		return ErrClosed
	}
}

// admit turns the driver's deliveries into the caller's, under a lifecycle
// ticket so that a consumer which closed while the fetch was in flight hands
// nothing over.
func (c *ConsumerHandle) admit(dst []Delivery, got int) (int, error) {
	t := c.life.Enter(lifecycle.OpFetch)
	if !t.OK() {
		// The consumer closed while the fetch was in flight. The specification makes the
		// source responsible for requeueing what it handed over and nothing
		// here settled any of it, so the messages are redelivered rather
		// than lost. Handing them to a caller who can only be told
		// ErrClosed would be the worse answer.
		return 0, ErrClosed
	}
	defer c.life.Leave(t)
	redelivered := int64(0)
	for i := range got {
		dd := &c.buf[i]
		if dd.Message == nil {
			// A driver protocol failure. Everything before i has been
			// admitted and is the caller's; this delivery and the ones
			// after it are not, and the source still holds them.
			c.counters.received.Add(int64(i))
			c.counters.inflight.Add(int64(i))
			c.counters.redelivered.Add(redelivered)
			return i, errDriverNilMessage
		}
		d := &dst[i]
		d.Message = *dd.Message
		d.Attempt = int(dd.Info.Attempt)
		d.handle, d.gen = c.claim(dd)
		if dd.Info.Redelivered {
			redelivered++
		}
	}
	c.counters.received.Add(int64(got))
	c.counters.inflight.Add(int64(got))
	c.counters.redelivered.Add(redelivered)
	return got, nil
}

// claim takes a settlement record for one delivery.
//
// In the default safe mode every delivery gets its own record and nothing is
// ever recycled, which is ADR-009's decision and the reason a Delivery
// retained past its settlement still answers correctly. Under BufferReuse the
// record comes from the free list, which is what takes the path to zero
// allocations per message.
//
// It returns the generation it leased the record under, which the Delivery
// carries so that it can never answer for the record's next delivery.
func (c *ConsumerHandle) claim(dd *DriverDelivery) (*settlementRecord, uint32) {
	var r *settlementRecord
	if c.reuse {
		r = c.free.pop()
	}
	if r == nil {
		r = &settlementRecord{owner: c, st: c.settler}
	}
	r.dd = *dd
	gen, ok := r.cell.Lease()
	if !ok {
		// A record is only ever recycled out of a state the cell can lease
		// again, so this cannot happen; a fresh record rather than a panic
		// is the answer that cannot corrupt a caller's delivery.
		r = &settlementRecord{owner: c, st: c.settler}
		r.dd = *dd
		gen, _ = r.cell.Lease()
	}
	r.slot.Store(packSlot(uint32(gen), releaseLive))
	return r, uint32(gen)
}

// release takes the record of generation gen out of circulation, for the one
// reason it left.
//
// It runs exactly once per delivery, because only the settlement that won the
// delivery's cell reaches it. That is the terminal-release invariant, and
// the counter it decrements is why it matters: a release that ran twice, or
// never, would leave InFlight wrong for ever.
func (c *ConsumerHandle) release(r *settlementRecord, gen, reason uint32) {
	r.last.Store(packSlot(gen, reason))
	r.slot.Store(packSlot(0, reason))
	c.counters.inflight.Add(-1)
	// The driver's delivery is deliberately NOT cleared here. Clearing it
	// was a write to a shared, non-atomic struct at the one moment another
	// goroutine may be reading it — the losing settler of the same
	// delivery, building its hook event, and every caller-facing accessor
	// (Delivery.Info, Native, Extend) — so a settled delivery stopped
	// naming its own topic and -race reported the write against the read.
	// dd is written once, by claim, before the
	// delivery exists for anyone to read.
	//
	// the memory plateau survives it: in the default safe mode the
	// record is garbage the moment the caller drops the Delivery, and a
	// caller that keeps one keeps the message's own storage with it; under
	// BufferReuse the free list is bounded by Prefetch and each of its
	// records pins one delivery until the next claim overwrites it, which
	// is the bound the fetch buffer c.buf already holds.
	// Under BufferReuse that claim is a later delivery's, so an extension
	// still reading dd parks the record rather than letting it reach the
	// free list (settlementRecord.recyclable).
	if c.reuse && r.recyclable() {
		c.free.push(r)
	}
}

// recordSettled counts a successful settlement and reports it.
func (c *ConsumerHandle) recordSettled(op settleOp, o NackOptions) {
	if op == settleAck {
		c.counters.acked.Add(1)
		return
	}
	c.counters.nacked.Add(1)
	if o.Action == NackActionDeadLetter {
		c.counters.deadLettered.Add(1)
	}
}

// Stats returns what this consumer has counted.
func (c *ConsumerHandle) Stats() ConsumerStats { return c.counters.snapshot() }

// Topic is the topic the consumer subscribed to.
func (c *ConsumerHandle) Topic() string { return c.cfg.Topic }

// Subscription is the subscription the consumer reads.
func (c *ConsumerHandle) Subscription() string { return c.cfg.Subscription }

// Shutdown closes the consumer gracefully, bounded by ctx (the specification's
// winner ADR-006, Shutdown): it stops fetching, lets the settlements already
// in flight finish, and then closes the source, which requeues or abandons
// whatever was never settled.
//
// It is bounded by ctx and by the run's drain timeout, WithDrainTimeout or
// DefaultDrainTimeout, measured from when it began; a consumer with no runner
// is bounded by DefaultDrainTimeout. When the drain timeout passes first, the
// handlers' contexts are cancelled and Shutdown reports an error whose code
// is CodeDeadlineExceeded; when ctx ends first, they are cancelled then and
// Shutdown returns ctx's error. Either way the source closes under any
// handler still running, so its delivery comes back. A driver call that
// ignores its context does not hold Shutdown past the bound either: Shutdown
// returns and reports the bound, and the close finishes behind it once the
// call has returned.
//
// It is idempotent: a second call, and a concurrent one, observe the first
// one's outcome rather than a half-closed handle.
//
// Called from inside a handler while this consumer has a runner, it does not
// wait. The drain waits for every handler in flight, and the caller may be
// one of them, which cannot finish until Shutdown returns. It begins the
// shutdown and returns nil, or the shutdown's error once one has finished.
// The drain then waits for the calling handler as for any other, so a
// settlement it makes after Shutdown returns still reaches the broker, and a
// Shutdown or Close from anywhere else waits for the shutdown and reports the
// outcome. A handler of any runner counts, because the call cannot tell whose
// handler it is running in.
//
// The shutdown begun there keeps ctx's values but not its deadline or its
// cancellation, so the drain timeout alone bounds it. That ctx is most often
// the handler's own, which ends before the drain can finish: as the handler
// returns, under WithHandlerTimeout or WithLeaseDeadline, and as the drain
// ends otherwise. A bound that ended there would cancel the other handlers
// and close the source under them, or report a clean drain as cancelled.
// WithDrainTimeout sets a nearer bound.
func (c *ConsumerHandle) Shutdown(ctx context.Context) error {
	if c.drainMayWaitForCaller() {
		// The client forgets the consumer at the end of the run and not
		// here: see closeNative.
		return c.life.StartShutdown(context.WithoutCancel(ctx))
	}
	err := c.life.Shutdown(ctx)
	c.client.forget(c)
	return err
}

// drainMayWaitForCaller reports whether the drain of c may be waiting for the
// calling goroutine: c has a runner, whose drain waits for its handlers, and
// the caller is running a handler. A consumer driven by Receive has no
// handlers for its drain to wait for, so its Shutdown and Close wait,
// whoever calls them.
func (c *ConsumerHandle) drainMayWaitForCaller() bool {
	return c.run.Load() != nil && insideHandler()
}

// insideHandler reports whether the calling goroutine is running a handler,
// a Handler under Run or a BatchHandler under RunBatch, of any runner. A
// consumer's Shutdown, Drain and Close ask it before they wait, and so do a
// client's Shutdown and Close, which drain or close every consumer it opened,
// because a drain waits for the handlers in flight and the caller may be one
// of them.
//
// The goroutine is recognised by its stack, as insideDone recognises a
// PublishAsync callback, and for the same reason: the cost falls on the
// shutdown and not on every delivery. A stack names the function a goroutine
// is running and not the runner it belongs to, so it answers for every
// runner's handlers. A handler that shuts down another consumer with a
// runner does not wait for that one either.
func insideHandler() bool {
	handle := runtime.FuncForPC(reflect.ValueOf((*runner).invoke).Pointer()).Name()
	handleBatch := runtime.FuncForPC(reflect.ValueOf((*runner).invokeBatch).Pointer()).Name()
	var buf [64]uintptr
	pcs := buf[:]
	n := runtime.Callers(2, pcs)
	for n == len(pcs) {
		// Deeper than the buffer: middleware and the handler's own calls
		// may nest past it. Rare, and only ever on a call that would
		// otherwise wait.
		pcs = make([]uintptr, 2*len(pcs))
		n = runtime.Callers(2, pcs)
	}
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if f.Function == handle || f.Function == handleBatch {
			return true
		}
		if !more {
			return false
		}
	}
}

// startShutdown begins the graceful shutdown Shutdown performs and returns
// without waiting for it. The run stops fetching before it drains, so a
// client that starts every consumer's shutdown before it waits for any has
// none of them fetching while another drains. Shutdown, called afterwards,
// waits for the same run.
func (c *ConsumerHandle) startShutdown(ctx context.Context) {
	_ = c.life.StartShutdown(ctx)
}

// Drain stops receiving and waits for the handlers already in flight, bounded
// by ctx (the graceful trio).
//
// It is the middle of the three: Flush is the publisher's, Shutdown drains and
// then closes, and this one drains and leaves the consumer OPEN. A runner
// stops as it would for a shutdown — Run returns once its lanes have drained —
// and nothing else about the handle changes, so a caller may start another
// Run afterwards. A consumer with no runner has nothing in flight and Drain
// returns at once.
//
// It is idempotent and safe from any goroutine. A ctx that is done before the
// handlers are reports its error; the handlers keep running, because nothing
// can abandon an unsettled delivery (see drainRun).
//
// Called from inside a handler, it stops the run and returns nil without
// waiting, for the reason Shutdown gives. Run returns once the calling
// handler and the others in flight have finished.
func (c *ConsumerHandle) Drain(ctx context.Context) error {
	c.stopRun()
	if c.drainMayWaitForCaller() {
		return nil
	}
	return c.drainRun(ctx)
}

// Close closes the consumer promptly: it unblocks a parked Receive,
// cancels the handlers' contexts and the settlements in flight, and closes
// the source without waiting. The source closes under any handler still
// running, so its delivery comes back and a settlement it makes is refused.
// It is idempotent and safe for concurrent use.
//
// Called from inside a handler while this consumer has a runner, it does not
// wait either. A Shutdown already under way, begun by that handler or from
// anywhere else, waits for the handler, and a Close joins it rather than
// starting a close of its own. It begins the close and returns nil, or the
// close's error once one has finished.
func (c *ConsumerHandle) Close() error {
	if c.drainMayWaitForCaller() {
		// The client forgets the consumer at the end of the run and not
		// here: see closeNative.
		return c.life.StartClose()
	}
	err := c.life.Close()
	c.client.forget(c)
	return err
}

// closed reports whether the handle is closing or closed.
func (c *ConsumerHandle) closed() bool { return c.life.Closing() }

// stopRun stops the runner's fetch loop, if there is one. It does not wait:
// The specification stops receiving BEFORE it waits on the workers, and the waiting is
// drainRun's for Drain and drainStep's for Shutdown.
func (c *ConsumerHandle) stopRun() {
	if r := c.run.Load(); r != nil {
		r.stopFetch()
	}
}

// drainRun waits for the runner's handlers to finish, bounded by ctx. It is
// Drain's wait, and Drain leaves the consumer open, so a ctx that ends first
// leaves the handlers running under their own contexts: nothing is about to
// close under them.
func (c *ConsumerHandle) drainRun(ctx context.Context) error {
	r := c.run.Load()
	if r == nil {
		return nil
	}
	select {
	case <-r.drained:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// drainTimeout is the bound of a graceful Shutdown that begins now: the drain
// timeout of the run being served, or DefaultDrainTimeout with no runner.
//
// The lifecycle's own 30 seconds used to be the bound whatever the run said,
// and closed the source under handlers the run still allowed to finish. The
// bound cannot simply be lifted either: the barriers behind the drain wait
// for a settlement inside the driver, and the drain itself waits for a fetch
// loop that only CloseNative ends when the driver's Fetch ignores its
// context. Without a bound, a driver like that held Shutdown for as long as
// it liked.
func (c *ConsumerHandle) drainTimeout() time.Duration {
	if r := c.run.Load(); r != nil {
		return r.cfg.drainTimeout
	}
	return DefaultDrainTimeout
}

// drainStep is Shutdown's Draining step: it waits for the runner's handlers
// to finish.
//
// Returning early leaves the handlers running while the source closes under
// them, which turns every settlement they were about to make into a
// redelivery. So the wait ends at the run's own drain timeout and no sooner,
// unless the caller's deadline comes first. ctx carries both: it is the
// caller's, cut short at the run's drain timeout from when Shutdown began
// (drainTimeout). expired is the runner's own timer, which ends the wait
// sooner when Run was already draining before Shutdown began.
//
// When ctx ends first, the source is about to close under the handlers, so
// their contexts are cancelled now rather than at the runner's own drain
// timeout: a handler that reads its context stops working on a delivery that
// is going back to the broker.
func (c *ConsumerHandle) drainStep(ctx context.Context) error {
	r := c.run.Load()
	if r == nil {
		return nil
	}
	select {
	case <-r.drained:
	case <-r.expired:
	case <-ctx.Done():
		r.cutHandlers()
		return ctx.Err()
	}
	// The timeout is reported even when the cancelled handlers returned in
	// time to close drained as well: they were cut short.
	select {
	case <-r.expired:
		return lifecycle.ErrDrainTimeout
	default:
		return nil
	}
}

// recordFreeList is the consumer's stock of recycled settlement records: a
// bounded LIFO behind one mutex, in the shape internal/pool's FreeList uses
// for the same job.
//
// Records are pushed by whichever goroutine settled a delivery and popped by
// the one that fetches, so the mutex is what orders a record's re-use after
// its release — and it is why a recycled record's fields can be written by
// the fetch without racing the settlement that released it.
type recordFreeList struct {
	mu   sync.Mutex
	head *settlementRecord
	n    int
	max  int
}

// push returns a record to the list, or drops it when the list is full.
func (l *recordFreeList) push(r *settlementRecord) {
	l.mu.Lock()
	if l.n < l.max {
		r.next = l.head
		l.head = r
		l.n++
	}
	l.mu.Unlock()
}

// pop takes a record, or returns nil when the list is empty.
func (l *recordFreeList) pop() *settlementRecord {
	l.mu.Lock()
	r := l.head
	if r != nil {
		l.head = r.next
		r.next = nil
		l.n--
	}
	l.mu.Unlock()
	return r
}

// newConsumer opens a consumer over the backend's source. It is
// Client.Consumer's body; the client owns the registration and the closing of
// child handles.
func newConsumer(ctx context.Context, cl *Client, cfg ConsumerConfig) (*ConsumerHandle, error) {
	resolved, err := cfg.resolve()
	if err != nil {
		return nil, err
	}
	if err := cl.caps.Require(resolved.Requirements...); err != nil {
		return nil, err
	}
	counters, err := newConsumerCounters()
	if err != nil {
		return nil, err
	}
	src, err := cl.backend.OpenSource(ctx, SourceConfig{
		Topic:        resolved.Topic,
		Subscription: resolved.Subscription,
		Prefetch:     resolved.Prefetch,
		Start:        resolved.Start,
		AckDeadline:  resolved.AckDeadline,
		Metadata:     resolved.Metadata,
	})
	if err != nil {
		return nil, err
	}
	if src == nil {
		return nil, errDriverNilSource
	}
	c := &ConsumerHandle{
		client:    cl,
		cfg:       resolved,
		src:       src,
		settler:   src.Settler(),
		clock:     cl.clock,
		hooks:     cl.hooks,
		buf:       make([]DriverDelivery, resolved.Prefetch),
		fetchGate: make(chan struct{}, 1),
		reuse:     resolved.BufferReuse,
		counters:  counters,
	}
	// The specification asserts the optional interfaces once, when the source is opened.
	// The assertion is on the settler the handle keeps, not on a second
	// Settler() call: a source is free to answer that call with a new
	// object each time.
	if ex, ok := c.settler.(DeadlineExtender); ok {
		c.extender = ex
	}
	// The gate starts open: the token is the right to fetch, and it is in the
	// channel from the moment the handle exists.
	c.fetchGate <- struct{}{}
	c.free.max = resolved.Prefetch
	// The handle's own context, rooted at Background because the specification forbids
	// retaining the caller's: the context that opened a consumer governs the
	// opening, and a settlement that happens an hour later is not part of
	// it. The Settling step is what cancels it.
	c.settleCtx, c.cancelSettle = context.WithCancel(context.Background())
	c.life = lifecycle.New(lifecycle.Config{
		Clock:          cl.clock,
		DrainTimeoutOf: c.drainTimeout,
		Steps: lifecycle.Steps{
			// the specification's "stop fetching", which for a consumer with a runner is
			// the runner's fetch loop. A caller driving Receive itself is
			// stopped by the OpFetch gate instead.
			StopFetch: c.stopRun,
			// the specification's "wait for in-flight handlers within DrainTimeout".
			// Close does not call it — a prompt close waits for nothing —
			// and Shutdown bounds it by its own context.
			Drain: c.drainStep,
			// Settling is where the specification puts "settle, requeue or let leases
			// expire per policy", and it is after the drain: a graceful
			// shutdown has already waited by the time the settlements in
			// flight are cancelled.
			Settle: c.settleStep,
			Commit: func() error {
				// Nothing is in flight any more: OpSettle's barrier is
				// behind us and the source is about to be closed, so the
				// in-flight counter returns to zero rather than recording
				// deliveries nothing can settle.
				c.counters.inflight.Add(-c.counters.inflight.Load())
				return nil
			},
			CloseNative: c.closeNative,
		},
	})
	return c, nil
}

// settleStep is the Settling step: the handlers still running have their
// contexts cancelled, and then the settlements in flight are.
//
// A graceful Shutdown has already drained the handlers by now, or cancelled
// them when its bound passed, so the first half changes nothing for it. A
// prompt Close skips the drain, and the source closes under its handlers
// next and gives their deliveries back. Their contexts used to stay live
// until the run's own drain timeout ran out, while they worked on messages
// the broker already had back and could no longer settle.
func (c *ConsumerHandle) settleStep() {
	if r := c.run.Load(); r != nil {
		r.cutHandlers()
	}
	c.cancelSettle()
}

// closeNative is the Closed phase's step: it closes the source, and then the
// client forgets the consumer.
//
// The client forgets it here, once the source is closed, as well as when
// Close or Shutdown returns, because of the Shutdown and Close called from
// inside a handler. Those begin the run and return, and the client's own
// close waits only for the children it still lists. Forgotten as that call
// returned, the consumer would be a shutdown nobody waited for, and
// Client.Close would close the backend under its open source while its drain
// was still going. Forgotten here, it is listed for as long as its shutdown
// has anything left to do.
func (c *ConsumerHandle) closeNative() error {
	err := c.src.Close()
	c.client.forget(c)
	return err
}

// The consumer's construction and protocol failures. The specification names no sentinel
// for an invalid configuration or for a driver that broke the SPI's contract,
// and errors.Is matches by code, so a caller compares CodeOf(err) against
// CodeInvalidConfig.
var (
	errConsumerNoTopic        = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.Topic is required")
	errConsumerNoSubscription = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.Subscription is required")
	errConsumerPrefetch       = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.Prefetch is negative or above MaxPrefetch")
	errConsumerMaxInFlight    = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.MaxInFlight is negative")
	errConsumerAckDeadline    = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.AckDeadline is negative")
	errConsumerMetadata       = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.Metadata is not a metadata mode")
	errConsumerStart          = errcode.New(errcode.InvalidConfig, "iqmq: ConsumerConfig.Start is not a start position")
	errDriverFetchCount       = errcode.New(errcode.Unknown, "iqmq: driver returned a delivery count outside the buffer")
	errDriverNilMessage       = errcode.New(errcode.Unknown, "iqmq: driver returned a delivery with no message")
	errDriverNilSource        = errcode.New(errcode.Unknown, "iqmq: driver returned a nil Source and no error")
	errDriverNilSink          = errcode.New(errcode.Unknown, "iqmq: driver returned a nil Sink and no error")
)

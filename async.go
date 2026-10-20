package iqmq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/completion"
	"github.com/iqhive/iqmq/internal/lifecycle"
)

// AsyncPublisher is the asynchronous publication interface, for library
// authors who take a publisher that does not wait. *PublisherHandle
// implements it (ADR-003).
type AsyncPublisher interface {
	Publisher
	// PublishAsync admits one message and reports its outcome to done.
	PublishAsync(ctx context.Context, m *Message, done func(PublishResult), opts ...PublishOption) error
}

var _ AsyncPublisher = (*PublisherHandle)(nil)

// CallbackMode is where an asynchronous publication's callback runs (
// "inline-vs-deferred callback behaviour fixed by publisher config and
// documented"). It is the inline-versus-deferred axis, exposed as
// publisher configuration rather than decided per call.
type CallbackMode uint8

// The callback modes.
const (
	// CallbackInline runs the callback on the goroutine that completed the
	// publication, which is the driver's I/O goroutine for a driver with a
	// native asynchronous sink. It is the zero value and the default: it
	// costs nothing, and the specification already forbids a callback that blocks the
	// driver's progress.
	CallbackInline CallbackMode = iota
	// CallbackDeferred hands the outcome to one completion goroutine per
	// publisher, so a slow callback cannot stall the driver. It costs one
	// goroutine per publisher — never one per message — and one bounded
	// hand-over ring.
	CallbackDeferred

	callbackModeCount
)

var callbackModeNames = [callbackModeCount]string{"inline", "deferred"}

// String returns the mode's lower-case name, which is the name the specification uses
// for the async timing dimension.
func (c CallbackMode) String() string {
	if c < callbackModeCount {
		return callbackModeNames[c]
	}
	return "CallbackMode(?)"
}

// Valid reports whether c names a callback mode.
func (c CallbackMode) Valid() bool { return c < callbackModeCount }

// kind maps the mode onto the completion kind.
func (c CallbackMode) kind() completion.Kind {
	if c == CallbackDeferred {
		return completion.Deferred
	}
	return completion.Inline
}

// The stages of one asynchronous publication's state.
const (
	// stageLive is a publication whose outcome nobody has claimed.
	stageLive uint32 = iota
	// stageSettled is a publication whose outcome came from the driver (or
	// from the core on a path that will never reach the driver), so the
	// driver will not call done again and the state may be recycled.
	stageSettled
	// stageDropped is a publication a drop mode discarded while it was still
	// queued in the batching buffer. The buffer still holds a reference to
	// the state — the flusher is what takes it out — so the state must not be
	// recycled: it would be handed to the next publication while the buffer
	// still pointed at it, and the flusher would then publish that
	// publication's message a second time in the dropped one's place. It is
	// the same rule the abandoned publication follows.
	stageDropped
	// stageSettling is the one-instruction window in which the goroutine
	// that won the outcome is reading the state and nothing may recycle it
	// yet. Only settle and abort pass through it, and only they move it on
	// to stageSettled.
	//
	// It exists because recycling is keyed on the stage and a recycle can
	// happen on ANOTHER goroutine: an abandoned publication's deliverResult
	// runs on the publisher's closing goroutine, so a state marked settled
	// could go back to the pool while its winner was still reading the
	// completion table out of it. deliverResult recycles on stageSettled and
	// on nothing else, so a state in this stage is safe to read and a state
	// abandoned during it is simply never pooled.
	stageSettling
)

// asyncPub is the state of one asynchronous publication: what the callback
// needs, what the driver borrows, and the place it holds in the publisher's
// bounded buffer.
//
// It is pooled, and the two closures the machinery needs — the completion
// callback and the driver's done — are bound to it once, when the pool first
// builds it, and reused for every publication that state serves. That is what
// makes an asynchronous publish allocate nothing in steady state: a closure
// built per publication would be an allocation per message, and a method
// value is a closure.
//
// Binding the closures to a reused object has one consequence that is the
// whole reason the stages exist: the driver's done identifies the publication
// *by the object*, so the object must not be recycled while any driver may
// still hold it. A publication the driver completed is safe to recycle; one
// that a close abandoned while the driver still held done is not, and is
// dropped for the collector instead.
type asyncPub struct {
	// ctx is the publication's context, kept for the done hook alone and
	// stripped of cancellation: a span started by the caller must be able to
	// parent the one the hook records, and the completion runs long after
	// the call returned. Set only when a hook is watching.
	ctx context.Context
	// seq pairs the done event with the start event of this publication.
	seq uint64

	p    *PublisherHandle
	cb   completion.Callback
	done func(error)
	// tbl is the completion table this publication's token belongs to,
	// recorded at admission. A completion reads it here rather than asking
	// the publisher, so no path from a driver's done reaches the table's
	// lazy constructor — and a completion costs no atomic load.
	tbl completion.Completer

	// tok is the completion token, read by done from whatever goroutine the
	// driver completes on. It is atomic because an abandoned publication's
	// state may be re-read after the state was handed back.
	tok atomic.Uint64

	msg   *Message
	user  func(PublishResult)
	opts  PublishOptions
	bytes int64
	start time.Time

	stage atomic.Uint32
	// suppress stops the caller's callback from running at all, which is
	// what a publication rejected by the driver needs: the specification says a non-nil
	// immediate error means the callback will not run. It is written by
	// whichever of settle, abort and drop won the stage, and read by
	// deliver, which the same call chain orders after it.
	suppress bool

	// prev, next, queued and dropped are the queued-list membership
	// BackpressureDropOldest walks. Every one of them is guarded by
	// pendingBound.mu.
	prev, next *asyncPub
	queued     bool
	dropped    bool
}

// newAsyncPub builds a state and binds its two closures to it, once and for
// the life of the state. The publisher's asyncPool has no New and this is
// called by hand: a New that built the state would be an initialisation
// cycle, because the state's closures reach back into the pool when they
// hand it over.
func newAsyncPub() *asyncPub {
	ap := new(asyncPub)
	ap.cb = ap.deliverResult
	ap.done = ap.settle
	return ap
}

// getAsyncPub takes a state out of the pool and binds it to one publication.
func getAsyncPub(p *PublisherHandle, m *Message, user func(PublishResult), bytes int64) *asyncPub {
	ap, _ := p.asyncPool.Get().(*asyncPub)
	if ap == nil {
		ap = newAsyncPub()
	}
	ap.p, ap.msg, ap.user, ap.bytes = p, m, user, bytes
	ap.suppress, ap.dropped = false, false
	ap.stage.Store(stageLive)
	return ap
}

// release empties the state and hands it back. It is called only where the
// publication never started — a validation failure, a full completion
// table — and by deliverResult for a publication the driver finished.
func (ap *asyncPub) release() {
	owner := ap.p
	ap.p, ap.msg, ap.user, ap.tbl = nil, nil, nil, nil
	ap.opts = PublishOptions{}
	ap.bytes, ap.start, ap.ctx, ap.seq = 0, time.Time{}, nil, 0
	owner.asyncPool.Put(ap)
}

// settle delivers the driver's outcome. It is the func(error) the driver is
// handed, and it is bound to this state once: exactly one of settle, abort
// and drop ever reaches the completion table, so the callback runs once.
//
// It takes the stage in TWO steps — live → settling → settled — and reads
// everything it needs in between. That is not tidiness; it is the ordering
// the recycling rule needs, and getting it wrong was a live data race with a
// cross-message consequence. See [asyncPub.claim].
func (ap *asyncPub) settle(err error) {
	tbl, tok, ok := ap.claim(stageSettled)
	if !ok {
		return
	}
	complete(tbl, tok, err)
}

// abort is settle's path for a publication the driver refused outright: the
// caller is told by the return value, so the callback must not run.
func (ap *asyncPub) abort(err error) {
	if !ap.stage.CompareAndSwap(stageLive, stageSettling) {
		return
	}
	// suppress is written inside the settling window, where nothing may
	// recycle the state: it is read by deliverResult, which only ever runs
	// after the Complete below.
	ap.suppress = true
	tbl, tok := ap.tbl, completion.Token(ap.tok.Load())
	ap.stage.Store(stageSettled)
	complete(tbl, tok, err)
}

// drop is settle's path for a publication a drop mode discarded. The
// callback does run — the caller was told the publication was admitted — and
// reports ErrBackpressure.
//
// It needs no settling window: [asyncPub.deliverResult] recycles on
// stageSettled and on nothing else, so a state this function won can never be
// pooled and is safe to read afterwards.
func (ap *asyncPub) drop() {
	if !ap.stage.CompareAndSwap(stageLive, stageDropped) {
		return
	}
	ap.p.counters.drops.Add(1)
	complete(ap.tbl, completion.Token(ap.tok.Load()), errDropped)
}

// claim takes the outcome of this publication and returns the completion table
// and token to deliver it through, or reports that somebody else took it.
//
// The two-step transition is the whole of the fix for a data race that
// `drivers/kafka/kafkago`'s `TestCloseRacesPublication` found — intermittently,
// and only under load — and whose consequence was worse than the detector's
// complaint. Recycling is keyed on the stage by
// [asyncPub.deliverResult], and for a publication the publisher's Close
// abandons, deliverResult runs on the CLOSING goroutine. So the sequence that
// used to happen was
//
//  1. the driver's goroutine CASes stageLive → stageSettled;
//  2. the publisher's Close abandons the same slot; its deliverResult reads
//     the stage, sees stageSettled and calls release() — nilling `tbl` and
//     handing the object to the publisher's asyncPool;
//  3. the driver's goroutine, still inside complete, reads ap.tbl.
//
// A recycled state is handed to the NEXT asynchronous publication by
// getAsyncPub, so step 3 could complete a token belonging to a different
// message: the specification's "the callback runs exactly once, with its own result"
// broken as a cross-message swap under load, which is the shape
// reached through the pool rather than through the option set, and which
// nothing reports.
//
// stageSettling is what closes it. Nothing recycles a state in that stage, so
// the reads below are safe; the stage moves on to `final` only once they are
// done; and a caller that loses the CAS reads nothing at all — which is the
// half a "capture before the CAS" fix would have got wrong, because `drop`
// and `settle` really can race for one publication through
// BackpressureDropOldest.
func (ap *asyncPub) claim(final uint32) (completion.Completer, completion.Token, bool) {
	if !ap.stage.CompareAndSwap(stageLive, stageSettling) {
		return nil, 0, false
	}
	tbl, tok := ap.tbl, completion.Token(ap.tok.Load())
	ap.stage.Store(final)
	return tbl, tok, true
}

// complete hands the outcome to the completion table, which runs the
// callback exactly once and frees the table slot before it does.
//
// It takes the table and the token rather than reading them off a state,
// because by the time it runs that state may belong to another publication:
// see [asyncPub.claim].
func complete(tbl completion.Completer, tok completion.Token, err error) {
	tbl.Complete(tok, err)
}

// deliverResult is the completion table's callback, bound to this state once.
// It is where the caller's callback runs, and it is the only place the state
// is recycled.
func (ap *asyncPub) deliverResult(res completion.Result) {
	p, m, user, suppressed := ap.p, ap.msg, ap.user, ap.suppress
	bytes, start := ap.bytes, ap.start
	// A publication the driver never finished may still have its done called
	// later, and done identifies the publication by this object, so the
	// object may not go back to the pool. Only a publication whose outcome
	// the driver has already given up is recycled.
	recycle := ap.stage.Load() == stageSettled

	// The buffer place is given back before the callback runs, so a callback
	// that publishes again finds the room its own completion freed, and no
	// lock of ours is held while user code runs (the rule for Release,
	// and the same discipline here).
	p.bound.release(bytes)
	err := res.Err
	if err != nil && errors.Is(err, completion.ErrAbandoned) {
		// The publication was outstanding at the driver when the publisher
		// closed. The answer for an outcome nobody knows is an *OpError
		// with Ambiguous set, preallocated per handle.
		err = p.abandoned
	}
	hookCtx := ap.ctx
	if hookCtx == nil {
		hookCtx = context.Background()
	}
	p.finishAsync(hookCtx, start, ap.seq, bytes, err)
	if !suppressed && user != nil {
		out := PublishResult{Message: m, Err: err}
		if err == nil && p.receipts && m != nil && m.Receipt != nil {
			out.Receipt = *m.Receipt
		}
		user(out)
	}
	if recycle {
		ap.release()
	}
}

// asyncTable returns the publisher's completion table, building it on the
// first asynchronous publication.
//
// It is built lazily because the table is the publisher's whole async budget
// — MaxPendingMessages slots — and a publisher that only ever publishes
// synchronously should not pay for it. Client.Publish opens one bound
// publisher per topic, so an eager table would be tens of kilobytes per
// topic a process ever published to.
//
// Every reader goes through the atomic pointer, and the only writer holds
// compMu, so the table is safe to reach from any goroutine. A close after
// the publication gate is barred reads the pointer and finds either nothing
// or a complete table: a publication that would build one cannot be admitted
// any more.
func (p *PublisherHandle) asyncTable() (completion.Completer, error) {
	if t := p.comp.Load(); t != nil {
		return t.c, nil
	}
	p.compMu.Lock()
	defer p.compMu.Unlock()
	if t := p.comp.Load(); t != nil {
		return t.c, nil
	}
	c, err := completion.New(p.cfg.Callbacks.kind(), completion.Config{Capacity: p.maxPending})
	if err != nil {
		return nil, fmt.Errorf("iqmq: completion table: %w", err)
	}
	p.comp.Store(&asyncCompleter{c: c})
	return c, nil
}

// asyncCompleter boxes the completion table so that it can live behind an
// atomic pointer.
type asyncCompleter struct{ c completion.Completer }

// PublishAsync admits one message and reports its outcome to done.
//
// The contract is the specification, and every clause of it is a promise about done:
//
//   - A non-nil error means done will not run. The caller keeps ownership of
//     the message and nothing was published.
//   - A nil return means done runs exactly once — including when the
//     publisher closes with the publication still outstanding, in which case
//     it reports an error whose code is CodeSettlementUnknown.
//   - The message, its Key, its Body and its header storage are borrowed
//     until done runs. So is the option set: see AsyncSink.
//   - There is no goroutine and no timer per message. A publisher configured
//     with CallbackDeferred has one completion goroutine; one configured with
//     Batching has one flusher.
//   - done must not block: it runs on the driver's completion goroutine
//     under CallbackInline, which is the default.
//
// done may be nil for a caller that only wants the publication bounded and
// counted.
//
// A driver with no native asynchronous sink is adapted rather than refused,
// which is the promise that a driver implementing only the mandatory
// surface works: the core publishes synchronously and runs done before
// returning. Batching is what gives such a driver a publication that really
// does not wait.
func (p *PublisherHandle) PublishAsync(ctx context.Context, m *Message, done func(PublishResult), opts ...PublishOption) error {
	if m == nil {
		return errNilMessage
	}
	// The closed check comes before the buffer so that the specification's "after close
	// all operations return ErrClosed" holds without exception, and the
	// buffer comes before the gate because taking a place may park: see
	// pendingBound.admit.
	if p.life.Closing() {
		return ErrClosed
	}
	bytes := int64(len(m.Body))
	dropped, err := p.bound.admit(ctx, bytes, newTimeoutBound(p.clock, p.cfg.Timeout))
	if err != nil {
		if !errors.Is(err, ErrClosed) {
			p.counters.backpressure.Add(1)
		}
		if dropped {
			p.counters.drops.Add(1)
		}
		p.counters.failed.Add(1)
		return err
	}
	t := p.life.Enter(lifecycle.OpPublish)
	if !t.OK() {
		// The publisher closed while this publication was taking its place.
		// Nothing was admitted anywhere, so the place goes straight back.
		p.bound.release(bytes)
		return ErrClosed
	}
	defer p.life.Leave(t)
	ap := getAsyncPub(p, m, done, bytes)
	ap.opts = p.cfg.Defaults
	if len(opts) > 0 {
		if err := ap.opts.Apply(opts...); err != nil {
			return p.refuseAsync(ap, err)
		}
	}
	p.stamp(m)
	if err := p.check(m, &ap.opts); err != nil {
		return p.refuseAsync(ap, err)
	}
	// The table is claimed before the driver is told anything, so that a
	// publication the table cannot hold is refused with the caller still
	// owning its message.
	//nolint:contextcheck // the table's delivery goroutine lives as long as the publisher and is stopped by its close, not by this publication; the specification makes an async publish's context govern admission only
	tbl, err := p.asyncTable()
	if err != nil {
		return p.refuseAsync(ap, err)
	}
	tok, _, err := tbl.Start(ap.cb)
	if err != nil {
		return p.refuseAsync(ap, err)
	}
	ap.tbl = tbl
	ap.tok.Store(uint64(tok))
	if p.hooks.OnPublishStart != nil || p.hooks.OnPublishDone != nil {
		ap.ctx = context.WithoutCancel(ctx)
	}
	ap.start, ap.seq = p.beginAsync(ctx, bytes)
	p.resetReceipt(m)
	switch {
	case p.flusher != nil:
		return p.flusher.add(ap)
	case p.async != nil:
		if err := p.async.PublishAsync(ctx, m, &ap.opts, ap.done); err != nil {
			// The specification: a non-nil immediate error means the callback will not
			// run. The table slot is freed and the buffer place given back,
			// and the caller is told once — by this return.
			ap.abort(err)
			return err
		}
		return nil
	default:
		// The adapter for a driver with no AsyncSink. done runs before this
		// returns, which is the honest cost of publishing synchronously: the
		// alternative is a goroutine per message, which rule 3 forbids.
		ap.settle(p.sink.Publish(ctx, m, &ap.opts))
		return nil
	}
}

// refuseAsync unwinds a publication that never started: the buffer place is
// given back, the state is recycled and no callback runs.
func (p *PublisherHandle) refuseAsync(ap *asyncPub, err error) error {
	p.bound.release(ap.bytes)
	p.counters.failed.Add(1)
	ap.release()
	return err
}

// beginAsync counts an asynchronous publication as pending and reports it to
// the start hook, returning the instant it started — the zero time when no
// hook is watching (rule 4).
func (p *PublisherHandle) beginAsync(ctx context.Context, bytes int64) (time.Time, uint64) {
	return p.begin(ctx, 1, bytes)
}

// finishAsync is beginAsync's other half, run from the completion.
func (p *PublisherHandle) finishAsync(ctx context.Context, start time.Time, seq uint64, bytes int64, err error) {
	if err != nil {
		p.counters.failed.Add(1)
	} else {
		p.counters.published.Add(1)
	}
	p.finish(ctx, start, seq, 1, bytes, err)
}

// stamp applies PublisherConfig.AutoID and Timestamp to the caller's message.
//
// Both write into the message the caller still owns, and both write only into
// a field the caller left empty. The specification makes them publisher configuration, so
// a publisher that has neither performs two branches and no writes at all —
// which is what "Publish never generates IDs or timestamps on the hot path
// unless configured" means. Why they mutate the caller's message rather than
// a copy is.
func (p *PublisherHandle) stamp(m *Message) {
	if p.cfg.AutoID != nil && m.ID == "" {
		m.ID = p.cfg.AutoID.NextID()
	}
	if p.cfg.Timestamp && m.Properties.CreatedAt.IsZero() {
		m.Properties.CreatedAt = p.clock.Now()
	}
}

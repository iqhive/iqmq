package iqmq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iqhive/iqmq/internal/batch"
	"github.com/iqhive/iqmq/internal/clock"
)

// BatchingConfig is the publisher's own batching: how many
// messages it accumulates before handing them to the driver in one call, and
// how long a partly filled batch may wait.
//
// Batching applies to PublishAsync. A synchronous Publish is never held back
// by it: a synchronous publication that waited out Linger would add that
// latency to every call, and the single-message budget is stated against
// a publication that goes straight out. PublishBatch and
// PublishItems are already batches and are handed to the driver as they are.
//
// The zero value disables batching, which is why a publisher that does not
// set it has no flusher goroutine.
type BatchingConfig struct {
	// MaxMessages is the number of messages that makes a batch leave. Zero
	// means the count never triggers one.
	MaxMessages int
	// MaxBytes is the accumulated body size that makes a batch leave. Zero
	// means the size never triggers one.
	MaxBytes int
	// Linger is how long a partly filled batch may wait before it leaves
	// anyway. Zero means no time bound at all: such a batch leaves when it
	// reaches MaxMessages or MaxBytes, or when Flush, Shutdown or Close
	// asks for it.
	Linger time.Duration
}

// enabled reports whether the configuration asks for batching.
func (b BatchingConfig) enabled() bool {
	return b.MaxMessages > 0 || b.MaxBytes > 0 || b.Linger > 0
}

// kind is the batching trigger the configuration asks for.
//
// A publisher that named a Linger asked for a latency bound, and only a
// timed kind honours one, so it gets batch.PerBatchTimer — the kind whose
// deadline is exact. A publisher that named none gets batch.SizeOnly, which
// is the measured winner and arms nothing at all. The choice is the
// configuration's and not the bake-off's because the bake-off raced the
// triggers at equal latency promises, and these two configurations do not
// make the same promise.
func (b BatchingConfig) kind() batch.Kind {
	if b.Linger > 0 {
		return batch.PerBatchTimer
	}
	return batch.SizeOnly
}

// publishFlusher accumulates asynchronous publications into batches and hands
// each ready batch to the driver.
//
// There is one per publisher and never one per message: the specification rule 3 forbids
// a goroutine or a timer per message, and the batcher's own timer is one per
// batch. Every field but the scratch buffers is safe for concurrent use; the
// scratch buffers belong to the flusher goroutine.
type publishFlusher struct {
	p *PublisherHandle
	b batch.Batcher[*asyncPub]

	// ctx is the context every send runs under. It is the publisher's and
	// not the publication: the specification says an async publish's context "governs
	// admission only unless the driver documents otherwise", and a batch
	// carries messages from many callers whose contexts have nothing to say
	// about each other. For the same reason a Flush's context bounds only
	// the Flush's wait for a send, never the send itself.
	ctx    context.Context
	cancel context.CancelFunc

	// wake is the flusher's doorbell, buffered so that a publication never
	// blocks on it and so that many publications coalesce into one wake-up.
	wake chan struct{}
	// asked is closed by halt; done is closed by the goroutine.
	asked chan struct{}
	done  chan struct{}
	// draining asks the goroutine to send the whole batch at its next
	// wake-up, ready or not. It is the publisher's bounded buffer asking:
	// see pendingBound.queued.
	draining atomic.Bool

	// flushes counts the flushes asked of the goroutine, and flushed is the
	// count it has served: every batch it took after reading flushes has
	// been handed to the driver and settled. Only the goroutine writes
	// flushed. sent is closed and replaced each time flushed moves, under
	// flushMu, which guards nothing else.
	flushes atomic.Uint64
	flushed atomic.Uint64
	flushMu sync.Mutex
	sent    chan struct{}

	// own is the flusher goroutine's own scratch, reused across batches so
	// that a batch costs no container.
	//
	// It belongs to that goroutine and to nothing else. Sharing one set of
	// buffers between the flusher and a caller's Flush is a data race that
	// publishes one message twice and another not at all: both take from the
	// batcher — which is safe, the batcher hands each item to one taker — and
	// then both write the taken items into the same backing array.
	own sendBufs
}

// sendBufs are the containers one batch needs on its way to the driver. A
// sender that is not the flusher goroutine brings its own, which costs a few
// allocations per Flush and none per message.
type sendBufs struct {
	items []*asyncPub
	live  []*asyncPub
	msgs  []*Message
	errs  []error
}

// newPublishFlusher starts a publisher's flusher.
func newPublishFlusher(p *PublisherHandle, cfg BatchingConfig, clk clock.Clock) (*publishFlusher, error) {
	b, err := batch.New[*asyncPub](cfg.kind(), batch.Config{
		MaxMessages: cfg.MaxMessages,
		MaxBytes:    cfg.MaxBytes,
		Linger:      cfg.Linger,
		Clock:       clk,
	})
	if err != nil {
		return nil, fmt.Errorf("iqmq: batcher: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &publishFlusher{
		p:      p,
		b:      b,
		ctx:    ctx,
		cancel: cancel,
		wake:   make(chan struct{}, 1),
		asked:  make(chan struct{}),
		done:   make(chan struct{}),
		sent:   make(chan struct{}),
	}
	go f.run()
	return f, nil
}

// add puts one admitted publication into the batch.
//
// The publication is linked into the publisher's queued list first, so that
// BackpressureDropOldest can reach it for as long as it is only queued, and
// the batcher's own Add comes second: a publication that is in the batch but
// not in the list could not be recalled, and one that is in the list but not
// in the batch could be recalled twice.
func (f *publishFlusher) add(ap *asyncPub) error {
	f.p.bound.link(ap)
	if _, err := f.b.Add(ap, int(ap.bytes)); err != nil {
		// The batcher is closed, so this publication will never leave. It
		// was never handed to the driver, so the specification's "a non-nil immediate
		// error means the callback will not run" is the honest answer.
		f.p.bound.unlink(ap)
		ap.abort(err)
		return err
	}
	// Every add rings the doorbell, not only the one that fills the batch: a
	// partly filled batch arms its linger deadline when its first item
	// arrives, and the flusher can only select on that deadline once it has
	// re-read the batch's wait channel.
	f.signal()
	return nil
}

// drain asks the flusher goroutine to send its partial batch. It is what
// pendingBound.stall calls when the buffer is full of queued publications that
// would otherwise wait for a batch that can never fill.
func (f *publishFlusher) drain() {
	f.draining.Store(true)
	f.signal()
}

// signal rings the doorbell without ever blocking on it.
func (f *publishFlusher) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// run is the flusher goroutine: wait for the doorbell or the batch's linger
// deadline, then send whatever is ready, or everything when a Flush or the
// bounded buffer asked for it.
func (f *publishFlusher) run() {
	defer close(f.done)
	var served uint64
	for {
		// The wait channel belongs to the current batch, so it is re-read
		// every round: the batch that armed it may already have left.
		linger := f.b.Wait()
		select {
		case <-f.asked:
			return
		case <-f.wake:
		case <-linger:
		}
		// A select with the doorbell rung as well picks at random, and a
		// send after halt goes to a sink that is closing.
		if f.halted() {
			return
		}
		// Read before the batch is taken, so a Flush counted here is
		// served by a take that came after it asked.
		asked := f.flushes.Load()
		if f.draining.Swap(false) || asked != served {
			f.sendAll()
			if asked != served {
				served = asked
				f.serve(served)
			}
			continue
		}
		f.sendReady()
	}
}

// halted reports whether halt has been called.
func (f *publishFlusher) halted() bool {
	select {
	case <-f.asked:
		return true
	default:
		return false
	}
}

// serve records that every flush up to n has been served and wakes the
// Flush calls waiting for it. The count is stored before the channel is
// replaced, so a waiter that took the old channel is woken and one that
// takes the new one finds the count already there.
func (f *publishFlusher) serve(n uint64) {
	f.flushed.Store(n)
	f.flushMu.Lock()
	close(f.sent)
	f.sent = make(chan struct{})
	f.flushMu.Unlock()
}

// sendAll hands everything the batch holds to the driver, whether or not it
// was ready. It runs only on the flusher goroutine, so it uses that
// goroutine's own scratch.
func (f *publishFlusher) sendAll() {
	items := f.b.Flush(f.own.items[:0])
	f.own.items = items
	if len(items) > 0 {
		f.send(items, &f.own)
	}
}

// sendReady hands the batch to the driver when it is ready to leave. It runs
// only on the flusher goroutine, so it uses that goroutine's own scratch.
func (f *publishFlusher) sendReady() {
	items, ok := f.b.TakeReady(f.p.clock.Now(), f.own.items[:0])
	f.own.items = items
	if !ok {
		return
	}
	f.send(items, &f.own)
}

// flush has the flusher goroutine send everything pending, whether or not the
// batch was ready, and waits until it has. It is what Flush and a graceful
// shutdown ask for.
//
// The goroutine sends it, and not the caller, for two reasons. A batch the
// goroutine took before the call may still be on its way to the driver, and
// the goroutine finishes that send before it serves the flush, so when a
// flush returns nil every publication admitted before it has reached the
// driver and been settled. A caller that sent the batch itself found the
// buffer empty and went on to flush the driver ahead of that send. And the
// caller waits under its own ctx, while the send runs under the publisher's:
// a broker that stops answering holds the send, not the caller, and the close
// that follows ends the send.
//
// It returns ctx's error when ctx ends first and ErrClosed when the
// goroutine stops first. Either way the publications go on to be settled, by
// the send or by the close.
func (f *publishFlusher) flush(ctx context.Context) error {
	want := f.flushes.Add(1)
	f.signal()
	for {
		f.flushMu.Lock()
		sent := f.sent
		f.flushMu.Unlock()
		if f.flushed.Load() >= want {
			return nil
		}
		select {
		case <-sent:
		case <-ctx.Done():
			return f.unserved(want, ctx.Err())
		case <-f.done:
			return f.unserved(want, ErrClosed)
		}
	}
}

// unserved is flush's answer when it stops waiting: nil when the flush it
// asked for was served after all, which a select that found both ready may
// not have seen, and err otherwise.
func (f *publishFlusher) unserved(want uint64, err error) error {
	if f.flushed.Load() >= want {
		return nil
	}
	return err
}

// handOver sends everything pending on the calling goroutine and returns once
// the driver has it. It is Flush called from inside a PublishAsync callback,
// which may be running on the flusher goroutine itself and so cannot wait for
// it; it may be called from any goroutine, so it brings scratch of its own.
func (f *publishFlusher) handOver() {
	var bufs sendBufs
	bufs.items = f.b.Flush(nil)
	if len(bufs.items) > 0 {
		f.send(bufs.items, &bufs)
	}
}

// send publishes one batch and settles every publication in it exactly once.
//
// It groups the batch into runs of publications that resolved to the same
// option set, because a native batch carries one option set for every message
// in it. A publisher whose publications all share the defaults — the
// common case — is one run.
func (f *publishFlusher) send(items []*asyncPub, bufs *sendBufs) {
	live := bufs.live[:0]
	for _, ap := range items {
		// A publication a drop mode took is skipped here and nowhere else:
		// unlink reports the drop under the same lock that marked it, so the
		// dropping publisher and this flusher cannot both own one message.
		if f.p.bound.unlink(ap) {
			continue
		}
		live = append(live, ap)
	}
	bufs.live = live
	if len(live) == 0 {
		return
	}
	for lo := 0; lo < len(live); {
		hi := lo + 1
		for hi < len(live) && live[hi].opts == live[lo].opts {
			hi++
		}
		if fatal := f.sendRun(live[lo:hi], bufs); fatal != nil {
			// The later runs would be offered to the same dead sink or
			// expired context; they never reached the driver, which is
			// the definite answer they get instead.
			na := f.p.notAttempted(fatal)
			for _, ap := range live[hi:] {
				ap.settle(na)
			}
			return
		}
		lo = hi
	}
}

// sendRun publishes one run of publications that share an option set, and
// reports the first error in it that means the sink cannot continue.
func (f *publishFlusher) sendRun(run []*asyncPub, bufs *sendBufs) error {
	msgs := bufs.msgs[:0]
	for _, ap := range run {
		msgs = append(msgs, ap.msg)
	}
	bufs.msgs = msgs
	errs := bufs.errs
	if cap(errs) < len(run) {
		errs = make([]error, len(run))
	}
	errs = errs[:len(run)]
	for i := range errs {
		errs[i] = nil
	}
	bufs.errs = errs
	f.p.sendAligned(f.ctx, msgs, &run[0].opts, errs)
	fatal := firstFatal(errs)
	// The settlements run last and outside every take: each one may run the
	// caller's callback, and a callback may publish or flush again.
	for i, ap := range run {
		ap.settle(errs[i])
	}
	return fatal
}

// halt tells the flusher goroutine to stop and cancels the context its sends
// run under. It does not wait: a send already under way may be one only the
// sink's close can end, and the close step closes the sink between halt and
// stop for that reason.
func (f *publishFlusher) halt() {
	close(f.asked)
	f.cancel()
}

// stop waits for the flusher goroutine, which halt has told to stop, and
// settles whatever the batch still holds.
//
// Everything left in the batch was admitted and never handed to the driver,
// so each publication is settled with ErrClosed: it definitely did not reach
// the broker, which is a stronger and more useful answer than the unknown
// outcome a publication abandoned at the driver gets.
func (f *publishFlusher) stop() {
	<-f.done
	_ = f.b.Close()
	leftover := f.b.Flush(nil)
	for _, ap := range leftover {
		if f.p.bound.unlink(ap) {
			continue
		}
		ap.settle(ErrClosed)
	}
}

// sendAligned publishes ms under o and writes each message's outcome into
// errs at the same index.
//
// It is the per-index half of PublishBatch's contract, which the batch
// flusher and PublishItems both need and which an aggregated error cannot
// give them: a publication's callback reports that publication's outcome.
// Splitting a batch larger than the broker's maximum, re-aligning each
// piece's results at the piece's offset, holding back a key whose message
// failed in an earlier piece, and stopping early on an error that means the
// sink cannot continue are all the same rules PublishBatch follows
func (p *PublisherHandle) sendAligned(ctx context.Context, ms []*Message, o *PublishOptions, errs []error) {
	if p.batch == nil || len(ms) == 1 {
		for i, m := range ms {
			errs[i] = p.sink.Publish(ctx, m, o)
			if errs[i] != nil && isFatalBatch(errs[i]) {
				fillNotAttempted(errs[i+1:], p.notAttempted(errs[i]))
				return
			}
		}
		return
	}
	// errs is not nil, so every outcome lands in it, the driver's first
	// error among them.
	_, _ = p.splitBatch(ctx, ms, o, errs)
}

package kafka

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The sink implements the mandatory publication surface and all four optional
// ones.
//
// All four are real here, which is unusual and is why each is worth a line:
//
//   - [github.com/iqhive/iqmq.BatchSink] because a Kafka produce request
//     carries many records and franz-go batches them itself, so a batch of a
//     hundred is one round trip rather than a hundred promises;
//   - [github.com/iqhive/iqmq.AsyncSink] because franz-go's own produce IS
//     asynchronous — the promise is the callback — so the asynchronous surface
//     costs no goroutine per message, which the specification forbids;
//   - [github.com/iqhive/iqmq.OwnedSink] because the record's storage is
//     released the moment the promise runs, which is earlier than a
//     synchronous Publish can release it;
//   - [github.com/iqhive/iqmq.FlushSink] because a publication under
//     ConfirmNone is in a buffer, and a caller has to be able to wait for it.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
	_ mq.AsyncSink = (*sink)(nil)
	_ mq.OwnedSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
)

// sink is one open publisher, bound to one topic for its lifetime (ADR-021).
type sink struct {
	b        *Backend
	topic    string // the portable topic
	name     string // the Kafka topic, with the configured prefix
	meta     metadataMode
	confirm  mq.ConfirmationMode
	receipts bool
	// path is the health path of the publications nobody waits for, a
	// ConfirmNone Publish's and a PublishOwned's: a failure of one is
	// reported to health, and the sink's next success clears it ([paths]).
	path healthPath

	once   sync.Once
	closed chan struct{}
	// life ends when Close runs, and with it a synchronous publication's
	// admission ([admission]) and its wait ([sink.await]), as its caller's
	// context ends them.
	life context.Context
	end  context.CancelFunc

	// pending is what Flush waits for: this sink's records whose promise
	// has not run.
	pending pending
}

// Publish admits one message.
//
// The order is fixed and it is the specification: the per-call controls Kafka cannot
// honour are refused before anything is written, then the size limit is
// checked, then the metadata is mapped, and only then does the record go to
// the producer.
//
// Under ConfirmAccepted and ConfirmDurable — the same boundary here — the call
// waits for the record's promise, which fires when every in-sync replica has
// the record in its log, or until ctx ends or the sink closes
// ([sink.produceWaited]). Under ConfirmNone it returns as soon as the record
// is in the producer's buffer. In every mode the message's storage is copied
// first, because the specification lets the caller reuse its buffers the
// instant Publish returns, and franz-go may still be reading them.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	if s.confirm == mq.ConfirmNone {
		return s.publishBuffered(ctx, m, o)
	}
	adm := s.admit(ctx)
	defer adm.done()
	buf, detached, err := s.handOver(ctx, adm, m, o)
	if err != nil {
		return err
	}
	if err := s.await(ctx, s.life, detached, buf); err != nil {
		return err
	}
	s.fillReceipt(m, &buf.rec)
	s.b.putRecbuf(buf)
	return nil
}

// handOver builds m's record into a buffer and hands it to the producer
// ([sink.produceWaited]) under the next [placement] adm gives it. It returns
// the buffer and whether the record is out of the reach of ctx and the sink's
// Close ([admission.leave]), or the refusal of a record it did not hand over,
// which is definite because nothing was sent.
func (s *sink) handOver(ctx context.Context, adm *admission, m *mq.Message, o *mq.PublishOptions) (*recbuf, bool, error) {
	// The admission learns of ctx's end and of Close on another goroutine,
	// a moment later, so they are read here as well: a record handed over
	// in that moment would come back unknown rather than held back.
	if ctx.Err() != nil || s.isClosed() {
		return nil, false, s.heldBack(ctx)
	}
	rctx, ok := adm.enter()
	if !ok {
		return nil, false, s.heldBack(ctx)
	}
	buf := s.b.getRecbuf()
	if err := s.build(rctx, buf, m, o, true); err != nil { //nolint:contextcheck // rctx is ctx without its cancellation (admission.base)
		adm.leave(rctx)
		s.b.putRecbuf(buf)
		return nil, false, err
	}
	// ctx or Close may have ended rctx while build ran. franz-go reads a
	// record's context only while it waits for buffer room, so with room
	// it would buffer the record under a context that has ended, and fail
	// the records queued with it.
	if rctx.Err() != nil {
		adm.leave(rctx)
		s.b.putRecbuf(buf)
		return nil, false, s.heldBack(ctx)
	}
	s.produceWaited(rctx, buf) //nolint:contextcheck // rctx is ctx without its cancellation (admission.base)
	return buf, adm.leave(rctx), nil
}

// heldBack is the answer for a record not handed to the producer because ctx
// ended or the sink closed first.
func (s *sink) heldBack(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, false)
	}
	return mq.ErrClosed
}

// produceWaited hands buf's record to the producer for a call that waits for
// its promise in [sink.await], and may stop waiting.
//
// The record is produced under a context the caller cannot end once franz-go
// has placed it, or Produce has returned for it ([admission], [placement]),
// because franz-go fails a partition's
// whole buffer when the first record of its head batch has a context that is
// done: that is its gapless ordering, and with
// AllowIdempotentProduceCancellation one record's context is also the produce
// request's. A caller who gave up, whose deadline passed, or whose publisher
// closed therefore failed every other caller's publication queued on that
// partition, with a cancellation those callers never made. So the caller's
// context, and the sink's Close, end only the WAIT; the record stays with
// franz-go, bounded by [Config.DeliveryTimeout], and its storage is the
// buffer's own copy ([sink.build] with own set), because the call may return
// first. The record is still this sink's, and Flush waits for it.
func (s *sink) produceWaited(ctx context.Context, buf *recbuf) {
	e := s.pending.admit()
	s.b.cl.Produce(ctx, &buf.rec, func(_ *kgo.Record, err error) {
		if err == nil {
			s.b.recovered(&s.path)
		}
		switch buf.finish(err) {
		case handoffRan:
			buf.ran <- struct{}{}
		default:
			s.b.putRecbuf(buf)
		}
		s.pending.done(e)
	})
}

// await waits for the promise of a record [sink.produceWaited] produced, and
// answers from it: nil, or the produce failure mapped. When ctx or life ends
// first, the call stops waiting and its outcome is unknown, and the promise
// only frees the record when it runs. On nil the call owns buf and frees it
// after reading the record; on an error buf is freed already.
//
// detached false means ctx ended, or the sink closed, while Produce ran and
// before franz-go placed the record ([placement]), so the record was cut
// before it could be sent: franz-go refused it while it waited for room in a
// full buffer, or the driver's partitioner refused it. Either way its promise
// runs at once and its failure is definite, so neither ctx nor life cuts that
// wait short. The answer is the cut, a cancellation, whatever franz-go said
// for the refusal. A borrowed client whose partitioner is not this driver's
// never places a record for a [placement], so there the race is decided only
// when Produce returns, and a record cut while Produce ran may still be
// buffered under its ended context and wait for the broker.
func (s *sink) await(ctx, life context.Context, detached bool, buf *recbuf) error {
	stop, closing := ctx.Done(), life.Done()
	if !detached {
		stop, closing = nil, nil
	}
	select {
	case <-buf.ran:
	case <-stop:
		if buf.leave(true) {
			return s.produceFailed(nil, ctx.Err())
		}
		<-buf.ran
	case <-closing:
		if buf.leave(true) {
			return s.produceFailed(nil, context.Canceled)
		}
		<-buf.ran
	}
	if err := buf.err; err != nil {
		if !detached && !s.b.maySend(&buf.rec) {
			err = context.Canceled
		}
		err = s.produceFailed(&buf.rec, err)
		s.b.putRecbuf(buf)
		return err
	}
	return nil
}

// publishBuffered is the ConfirmNone path: the record's storage is the
// driver's own copy, the record is handed to the producer, and the call
// returns without waiting.
//
// The copy is not avoidable and is the price of the mode. The specification says a
// synchronous Publish may not retain the caller's key, body or header storage
// after it returns, and franz-go holds the record until it has been written —
// so either the call waits for the promise or the bytes are copied. The other
// modes copy as well, because their wait can end first ([sink.produceWaited]).
// The copy is a pooled buffer, so the steady state allocates nothing.
//
// A record franz-go may have refused before buffering it is the exception:
// the call answers that its outcome is unknown ([sink.unadmitted]).
func (s *sink) publishBuffered(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	actx, admitted := s.admitForRoom(ctx, m)
	buf := s.b.getRecbuf()
	if err := s.build(actx, buf, m, o, true); err != nil {
		admitted()
		s.b.putRecbuf(buf)
		return err
	}
	// A receipt cannot be filled: nothing is known about the record yet.
	// The specification makes Receipts a publisher configuration, and a publisher
	// configured with both Receipts and ConfirmNone is asking for two
	// things that cannot both be true; the receipt slot is left as the
	// core zeroed it.
	e := s.pending.admit()
	s.b.cl.Produce(actx, &buf.rec, func(_ *kgo.Record, err error) {
		if err == nil {
			s.b.recovered(&s.path)
		}
		switch buf.finish(err) {
		case handoffLeft:
			if err != nil {
				// Nobody is waiting, so the specification makes this a background
				// failure: it goes to health, which is the only place a
				// caller can see it.
				s.b.failed(&s.path, s.produceFailed(&buf.rec, err))
				s.closedSinkFailed()
			}
			s.b.putRecbuf(buf)
		case handoffAnswered:
			s.b.putRecbuf(buf)
		}
		s.pending.done(e)
	})
	unsure := s.unadmitted(admitted)
	if buf.leave(unsure) {
		return s.unadmittedErr(ctx, unsure)
	}
	// The promise ran before the call finished, and its outcome is the
	// call's answer.
	err := buf.err
	if err != nil {
		err = s.produceFailed(&buf.rec, err)
	}
	s.b.putRecbuf(buf)
	return err
}

// unadmitted reports, once Produce has returned, that franz-go may have
// refused the record before buffering it: the caller's context ended, or the
// sink closed ([sink.admitForRoom]), while Produce waited for room in a full
// buffer, which admitted then finds it can no longer stop, or the client is
// closed. It ends the admission either way.
//
// franz-go runs the promise of such a refusal after Produce has returned, so
// a call that returned nil told its caller "admitted" for a record that is
// nowhere. The call cannot wait for that promise either: franz-go runs every
// promise on one goroutine, and a publication made from inside a PublishAsync
// callback runs on that goroutine, so it would wait for ever for a promise
// only it could run. So the answer is that the outcome is unknown
// ([sink.unadmittedErr]), which it is: the record may also have been buffered
// just before the context ended.
//
// A transactional producer with no transaction open refuses every record the
// same way, and that one this cannot see: whether a transaction is open is
// franz-go's own state. It is a usage error, so the call returns nil and the
// refusal goes to health, as docs/drivers/kafka.md says.
func (s *sink) unadmitted(admitted func() bool) bool {
	return !admitted() || s.b.cl.Context().Err() != nil
}

// unadmittedErr is the answer of a call that returns before its record's
// promise has run: nil, or for a record franz-go may have refused before
// buffering it ([sink.unadmitted]), an unknown outcome whose cause is ctx's
// end, the sink's Close (a cancellation, as for a synchronous publication's
// wait, [sink.await]) or the closed client. The promise only frees the
// record then.
func (s *sink) unadmittedErr(ctx context.Context, unsure bool) error {
	if !unsure {
		return nil
	}
	cause := ctx.Err()
	if cause == nil && s.isClosed() {
		cause = context.Canceled
	}
	if cause == nil {
		cause = kgo.ErrClientClosed
	}
	return s.produceFailed(nil, cause)
}

// admitCtx is the context a call that does not wait for its record's promise
// produces it under, and the function that ends the call admitting it. A
// synchronous publication uses an [admission], which the sink's Close ends as
// well, and a nack's move a [placement] of its own.
//
// The record is the producer's once that call has returned nil, and franz-go
// fails a buffered record whose context is cancelled — so a record bound to
// the caller's context was dropped by the usual deferred cancel, after the
// caller had been told it was admitted. The record keeps ctx's values, for
// tracing and for [RouteToPartition], and ctx cancels it only until admitted
// runs: while Produce may still be blocked waiting for buffer room, which is
// a wait the caller can give up on. A ctx already done never gets here,
// because check refuses it.
//
// admitted reports true when it stopped ctx from ending the record, as it
// always does for a ctx that can never end, and false when ctx had ended
// first.
func admitCtx(ctx context.Context) (context.Context, func() bool) {
	if ctx.Done() == nil {
		return ctx, func() bool { return true }
	}
	actx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	return actx, context.AfterFunc(ctx, cancel)
}

// admitForRoom is [admitCtx] for a call that does not wait for its record's
// promise: a ConfirmNone Publish, a PublishAsync and a PublishOwned.
//
// The core ends a publication it is waiting for by closing the sink, and a
// record produced under a context only ctx could end, while Produce waited
// for room in a full buffer, held the publisher's Close until the buffer had
// room: Config.DeliveryTimeout, or for ever under UnboundedDelivery. So when
// the buffer is at its bound, the sink's Close ends that wait as ctx does.
// While the buffer has room, Produce does not wait, and admitCtx is all the
// call needs, which for a ctx that cannot end costs nothing. The buffer is
// read before Produce runs, so a record that finds room and then a full
// buffer waits under ctx alone, as it did before.
func (s *sink) admitForRoom(ctx context.Context, m *mq.Message) (context.Context, func() bool) {
	if !s.b.bufferFull(m) {
		return admitCtx(ctx)
	}
	actx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stopLife := context.AfterFunc(s.life, cancel)
	if ctx.Done() == nil {
		return actx, stopLife
	}
	stopCtx := context.AfterFunc(ctx, cancel)
	return actx, func() bool {
		ctxStopped, lifeStopped := stopCtx(), stopLife()
		return ctxStopped && lifeStopped
	}
}

// bufferFull reports whether the producer's buffer is at its bound, so that
// Produce would wait for room for m's record. Under a byte bound the record's
// size is m's key and body, a little under what franz-go counts; a record
// that misses the bound by its headers waits as it did before
// ([sink.admitForRoom]).
func (b *Backend) bufferFull(m *mq.Message) bool {
	if b.cl.BufferedProduceRecords() >= b.bufRecords {
		return true
	}
	return b.bufBytes > 0 && b.cl.BufferedProduceBytes()+int64(len(m.Key)+len(m.Body)) > b.bufBytes
}

// admission is how a synchronous Publish or PublishBatch lets its caller's
// context and its sink's Close end the one record Produce may still hold,
// waiting for room in a full buffer, and none franz-go has placed or Produce
// has returned for.
//
// The core's Close ends a publication it is waiting for by closing the sink,
// so Close has to reach a record waiting for room as well as ctx does: one
// produced under a context only ctx could end held the publisher's Close
// until the buffer had room, which is DeliveryTimeout, or for ever under
// UnboundedDelivery. Each record has a [placement] of its own, derived from
// base, which keeps ctx's values for tracing and [RouteToPartition] and
// cannot end any other way. ctx and the sink's life are watched once for the
// call, not once for each record of a batch, so a record costs one context.
type admission struct {
	base context.Context

	mu    sync.Mutex
	cur   *placement // the record inside Produce; nil between records
	ended bool       // ctx ended or the sink closed

	stopCtx, stopLife func() bool
}

// admit starts the admission of a call's records. The call ends it with done.
func (s *sink) admit(ctx context.Context) *admission {
	a := &admission{base: context.WithoutCancel(ctx)}
	a.stopLife = context.AfterFunc(s.life, a.end) //nolint:contextcheck // the sink's Close ends the admission as ctx does
	if ctx.Done() != nil {
		a.stopCtx = context.AfterFunc(ctx, a.end)
	}
	return a
}

// end is what ctx's end and the sink's Close run: it cuts the record inside
// Produce unless franz-go has placed it ([placement.cut]), and
// [admission.enter] hands over no other.
func (a *admission) end() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ended = true
	if a.cur != nil {
		a.cur.cut()
		a.cur = nil
	}
}

// enter returns the placement the next record is produced under, and false
// once ctx has ended or the sink has closed: that record is held back.
func (a *admission) enter() (*placement, bool) {
	p := newPlacement(a.base)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended {
		p.cancel()
		return nil, false
	}
	a.cur = p
	return p, true
}

// leave reports, once Produce has returned for p, the record enter gave it
// to, whether that record is out of reach: true when franz-go placed it
// first, or nothing ended it before now, and false when ctx or Close cut it
// first, and franz-go then refuses it at once ([placement]).
func (a *admission) leave(p *placement) bool {
	a.mu.Lock()
	a.cur = nil
	a.mu.Unlock()
	return p.take()
}

// placement is the context a record a call waits for is produced under, and
// the one place that decides which came first: the end of the call, by its
// context or its sink's Close ([placement.cut]), or franz-go placing the
// record in a partition's batch ([placement.take]).
//
// franz-go fails a batch for a context only through its first record. A
// record whose context ended inside Produce, after it had taken its room in
// the buffer, joined a batch under that context: it failed the records queued
// behind it when it began the batch, and when it joined one another call
// began, nothing failed it, so its call waited for the broker's answer,
// Config.DeliveryTimeout, or for ever under UnboundedDelivery. So the
// driver's partitioner, which franz-go runs inside Produce after the room
// wait and before the record joins a batch, takes the record first
// ([TopicPartitioner.Partition]): from then on nothing ends it, and its call
// may stop waiting with an unknown outcome. A record cut before that the
// partitioner refuses, so franz-go fails it before it is placed, and its
// call's answer comes at once and is definite.
//
// It rides in the record's context because that is the only per-record thing
// franz-go hands its partitioner, and the field franz-go documents for
// carrying a record's enrichment to the client's hooks, as
// [RouteToPartition]'s partition does. It IS the record's context, carrying
// the record's requested partition itself, so the partitioner finds it with a
// type assertion and never walks a caller's context for it.
//
// It is allocated for each record, and not kept in the record's pooled
// buffer: franz-go derives request contexts from a record's context (the
// producer id's, for one) that it can still be cancelling after the record's
// promise has run and the buffer has gone back to the pool, so a placement
// reused for the next record was read by the old one's derived context.
type placement struct {
	context.Context
	cancel context.CancelFunc
	state  atomic.Int32
	// partition is the partition the call requested, when routed is set
	// ([sink.build]); [requestedPartition] reads it through Value.
	partition int32
	routed    bool
}

// The states of a [placement].
const (
	placeOpen  int32 = iota // neither has happened
	placeTaken              // franz-go placed the record, or its call's Produce returned first
	placeCut                // the call ended first, and the record's context is ended
)

// newPlacement returns a placement for one record, derived from base.
func newPlacement(base context.Context) *placement {
	p := &placement{}
	p.Context, p.cancel = context.WithCancel(base)
	return p
}

// Value is the record's context's, and the requested partition for
// [RouteToPartition]'s key when the call asked for one.
func (p *placement) Value(key any) any {
	if _, ok := key.(partitionKey); ok && p.routed {
		return p.partition
	}
	return p.Context.Value(key)
}

// cut ends the record's context unless the record was taken first. The state
// moves before the context ends, so a partitioner that reads the state after
// the cut refuses the record, whether or not franz-go has seen the end yet.
func (p *placement) cut() {
	if p.state.CompareAndSwap(placeOpen, placeCut) {
		p.cancel()
	}
}

// take keeps the record's context from ending, and reports false when the
// record was cut first.
func (p *placement) take() bool {
	return p.state.CompareAndSwap(placeOpen, placeTaken) || p.state.Load() == placeTaken
}

// placed is the partitioner's half of a [placement]: it takes r for
// franz-go, and reports false for a record whose call ended first, which the
// partitioner then refuses. A record produced under no placement, a caller's
// own on a client it built with [Partitioner], is always placed.
func placed(r *kgo.Record) bool {
	p, ok := r.Context.(*placement)
	return !ok || p.take()
}

// done stops watching ctx and the sink's Close.
func (a *admission) done() {
	a.stopLife()
	if a.stopCtx != nil {
		a.stopCtx()
	}
}

// PublishBatch admits a batch.
//
// Every record is handed to the producer and the call waits for all of their
// promises, so a batch of a hundred costs the round trips franz-go's own
// batching decides on rather than a hundred. ctx and the sink's Close end only
// the wait, as they do for a Publish ([sink.produceWaited]), so the messages'
// storage is copied.
//
// The result is index-aligned with ms, which is the whole contract. A result
// aligned with anything else, such as the completion order franz-go's own
// ProduceSync returns, reports one message's outcome for another, and that is
// how a batch publish loses a message silently. FuzzKafkaBatchOutcome exists
// for it.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	for _, m := range ms {
		if err := s.check(ctx, m, o); err != nil {
			return err
		}
	}
	errs := make([]error, len(ms))
	// waiting holds, by index, the buffers of the records handed to the
	// producer; the other entries of errs hold this driver's own refusal.
	// Each record's outcome comes back in its own buffer, never in the
	// order franz-go's promises fire, which is whatever order the
	// partitions resolve in.
	waiting := make([]*recbuf, len(ms))
	// detached says, by index, that neither ctx nor the sink's Close could
	// reach the record once Produce had returned for it ([admission]). Each
	// record is admitted on its own, because one admission for the whole
	// batch left the records already buffered under ctx while a later one
	// waited for room in a full buffer: a caller who gave up then failed
	// every publication queued on their partitions ([sink.produceWaited]).
	// The records not handed over once ctx has ended or the sink has closed
	// are held back, with a definite refusal.
	detached := make([]bool, len(ms))
	adm := s.admit(ctx)
	defer adm.done()
	for i, m := range ms {
		waiting[i], detached[i], errs[i] = s.handOver(ctx, adm, m, o)
	}
	for i, buf := range waiting {
		if buf == nil {
			continue
		}
		if errs[i] = s.await(ctx, s.life, detached[i], buf); errs[i] == nil {
			s.fillReceipt(ms[i], &buf.rec)
			s.b.putRecbuf(buf)
		}
	}
	return BatchSummary(errs)
}

// BatchOutcome turns a produce response's per-record answers into the
// index-aligned error slice the specification requires, mapping each one through.
//
// It is exported and pure because it is the one piece of batch arithmetic that
// is easy to get wrong in a way no ordinary test notices: franz-go's promises
// fire in whatever order the partitions resolve, so a driver that collected
// results in COMPLETION order would report message seven's failure against
// message two. A caller reading an index-aligned result would then retry the
// wrong message and believe the right one succeeded. FuzzKafkaBatchOutcome
// scrambles the outcomes on purpose and checks the alignment.
//
// A nil entry in native is a record the broker accepted.
//
// It applies the rules alone, because it cannot tell whether franz-go placed
// a record, as a publication can. So the answers a publication reads as
// unknown only for a record franz-go may have sent are each the rule's here:
// UNKNOWN_TOPIC_OR_PARTITION on its own is CodeNotFound, and
// kgo.ErrClientClosed is CodeClosed.
func BatchOutcome(native []error) []error {
	if len(native) == 0 {
		return nil
	}
	m := newErrorMapper()
	out := make([]error, len(native))
	for i, err := range native {
		if err == nil {
			continue
		}
		out[i] = m.MapNet(mq.OpPublish, "", "", err, true)
	}
	return out
}

// BatchSummary is the single value the specification asks a batch publication to return: nil
// when every message was accepted, the one error when none were and every
// message has the same outcome, and a *BatchError carrying the
// index-aligned outcomes otherwise -- when some were accepted, or when none
// were but some were refused and some may be in the log.
//
// It is driverkit.CollapseBatch, which is where the rule lives. It returned
// the first message's error whenever every message failed, so a batch one
// partition refused and another timed out on read as a definite refusal, and
// a caller who republished it duplicated the records that may have been
// appended.
func BatchSummary(errs []error) error {
	return driverkit.CollapseBatch(errs)
}

// PublishAsync admits one message and reports its outcome to done.
//
// m and o are borrowed until done runs, which is exactly franz-go's own
// contract for a record and its promise, so this path copies nothing and
// starts no goroutine: the promise IS the callback. The rules hold as
// written — a non-nil immediate error means done will not run, a nil return
// means done runs exactly once.
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return errNilCallback
	}
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	ctx, admitted := s.admitForRoom(ctx, m)
	defer admitted()
	buf := s.b.getRecbuf()
	if err := s.build(ctx, buf, m, o, false); err != nil {
		s.b.putRecbuf(buf)
		return err
	}
	e := s.pending.admit()
	s.b.cl.Produce(ctx, &buf.rec, func(rec *kgo.Record, err error) {
		if err == nil {
			s.b.recovered(&s.path)
			s.fillReceipt(m, rec)
			done(nil)
		} else {
			done(s.produceFailed(rec, err))
		}
		s.b.putRecbuf(buf)
		s.pending.done(e)
	})
	return nil
}

// PublishOwned takes ownership of om's message at invocation and releases it
// exactly once.
//
// The release happens in the promise, which is the earliest moment it can: the
// record points at the message's own key, body and header storage, so nothing
// is copied and the storage goes back to whoever owned it as soon as franz-go
// has written it. Every path releases — a refusal before the produce, a
// refusal by the broker, a closed sink — and none of them releases while
// holding a lock. A record franz-go may have refused before buffering it is
// answered as an unknown outcome, as a ConfirmNone Publish's is
// ([sink.unadmitted]), and released when its promise runs.
func (s *sink) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	release := func() {
		if om.Release != nil {
			om.Release(om.Message)
		}
	}
	if err := s.check(ctx, om.Message, o); err != nil {
		release()
		return err
	}
	actx, admitted := s.admitForRoom(ctx, om.Message)
	buf := s.b.getRecbuf()
	if err := s.build(actx, buf, om.Message, o, false); err != nil {
		admitted()
		s.b.putRecbuf(buf)
		release()
		return err
	}
	e := s.pending.admit()
	s.b.cl.Produce(actx, &buf.rec, func(rec *kgo.Record, err error) {
		if err == nil {
			s.b.recovered(&s.path)
		}
		switch buf.finish(err) {
		case handoffLeft:
			if err == nil {
				s.fillReceipt(om.Message, rec)
			} else {
				s.b.failed(&s.path, s.produceFailed(rec, err))
				s.closedSinkFailed()
			}
			s.b.putRecbuf(buf)
			release()
		case handoffAnswered:
			s.b.putRecbuf(buf)
			release()
		}
		s.pending.done(e)
	})
	unsure := s.unadmitted(admitted)
	if buf.leave(unsure) {
		return s.unadmittedErr(ctx, unsure)
	}
	// The promise ran before the call finished, and its outcome is the
	// call's answer.
	err := buf.err
	if err == nil {
		s.fillReceipt(om.Message, &buf.rec)
	} else {
		err = s.produceFailed(&buf.rec, err)
	}
	s.b.putRecbuf(buf)
	release()
	return err
}

// Flush completes the publications already admitted, bounded by ctx.
//
// They are this sink's own, admitted before the call ([pending]), and not
// the client's: kgo.Client.Flush waits until the CLIENT has nothing
// buffered, and the client is shared by every sink of the backend and by the
// nack moves. Flush used to be that call alone, so a publisher with nothing
// left waited for another publisher's records, answered
// ErrSettlementUnknown when its context ended and counted an ambiguous
// publication that did not exist; under steady traffic from anyone else its
// Flush, and so its Shutdown, never succeeded.
//
// A record whose synchronous caller stopped waiting is still this sink's
// ([sink.produceWaited]), so Flush waits for it too: until franz-go writes it
// or fails it, at the latest after Config.DeliveryTimeout, or until ctx ends.
func (s *sink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	e, waiting := s.pending.begin(cancel)
	if !waiting {
		return nil
	}
	// kgo.Client.Flush is still what sends a record that lingers, which it
	// does for the whole client, and it returns when fctx ends: the
	// promise that answers the last of this sink's records ends it. It also
	// returns when the client has nothing buffered, which can come before
	// the promise of a record franz-go refused before buffering it, so the
	// wait goes on to fctx's end.
	if s.b.cl.Flush(fctx) == nil {
		<-fctx.Done()
	}
	if s.pending.end(e) {
		return nil
	}
	// No one record is named, and a Flush cut short leaves every record it
	// was waiting for in the buffer, where franz-go may still send it.
	return s.produceFailed(nil, ctx.Err())
}

// pending counts a sink's records whose promise has not run, by the Flush
// they were admitted before, so that a Flush waits for the records admitted
// before it and not for those admitted while it waits: under steady traffic
// on the sink, a Flush that waited for none to be outstanding would never
// return.
//
// Each Flush closes an epoch and waits until no record of that epoch, or of
// an earlier one, is outstanding. There are as many epochs with records
// outstanding as there are Flushes waiting, plus the open one, so the
// slices stay short.
type pending struct {
	mu sync.Mutex
	// epoch is the open one, which a record admitted now is counted in.
	epoch uint64
	// counts holds the epochs that have records outstanding, oldest first.
	counts []epochCount
	// waiters are the Flushes waiting, each with the epoch it closed and
	// the function that ends its wait.
	waiters []flushWaiter
}

// epochCount is how many records of one epoch are outstanding.
type epochCount struct {
	epoch uint64
	n     int
}

// flushWaiter is one waiting Flush.
type flushWaiter struct {
	epoch uint64
	wake  context.CancelFunc
}

// admit counts one record handed to the producer, and returns the epoch its
// promise passes to done.
func (p *pending) admit() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.epoch
	if n := len(p.counts); n > 0 && p.counts[n-1].epoch == e {
		p.counts[n-1].n++
		return e
	}
	p.counts = append(p.counts, epochCount{epoch: e, n: 1})
	return e
}

// done is a promise's report that a record of epoch e has been answered. It
// ends the wait of every Flush that has nothing left outstanding.
func (p *pending) done(e uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.counts {
		if p.counts[i].epoch != e {
			continue
		}
		if p.counts[i].n--; p.counts[i].n > 0 {
			return
		}
		p.counts = slices.Delete(p.counts, i, i+1)
		if i == 0 {
			p.wakeLocked()
		}
		return
	}
}

// begin closes the open epoch for a Flush. It returns that epoch and whether
// a record of it, or of an earlier one, is outstanding; while one is, the
// Flush is waiting and wake ends its wait once none is.
func (p *pending) begin(wake context.CancelFunc) (uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.epoch
	p.epoch++
	if !p.outstandingLocked(e) {
		return e, false
	}
	p.waiters = append(p.waiters, flushWaiter{epoch: e, wake: wake})
	return e, true
}

// end removes the Flush that closed epoch e, and reports whether every
// record admitted before it has been answered.
func (p *pending) end(e uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.waiters {
		if p.waiters[i].epoch == e {
			p.waiters = slices.Delete(p.waiters, i, i+1)
			break
		}
	}
	return !p.outstandingLocked(e)
}

// outstandingLocked reports whether a record of epoch e, or of an earlier
// one, has not been answered.
func (p *pending) outstandingLocked(e uint64) bool {
	return len(p.counts) > 0 && p.counts[0].epoch <= e
}

// wakeLocked ends the wait of every Flush with nothing left outstanding.
func (p *pending) wakeLocked() {
	kept := p.waiters[:0]
	for _, w := range p.waiters {
		if p.outstandingLocked(w.epoch) {
			kept = append(kept, w)
			continue
		}
		w.wake()
	}
	clear(p.waiters[len(kept):])
	p.waiters = kept
}

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It does not flush: the specification gives Close no context, and a flush is a round trip
// a caller who wants it asks for with Shutdown. What it does not do is touch
// the client — many sinks share one. It does end the synchronous
// publications still waiting on this sink: a record still waiting for buffer
// room is refused ([admission]), and the wait for one already buffered ends
// ([sink.await]) while the record stays with franz-go.
func (s *sink) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.end()
		s.b.forget(nil, s)
		// A failure of a publication nobody waited for is the sink's, and
		// a closed sink will not publish again to clear it.
		s.b.recovered(&s.path)
	})
	return nil
}

// closedSinkFailed clears the failure a publication nobody waited for has
// just reported when the sink has closed since. Close does not flush, so such
// a record can fail after Close cleared the sink's path, and the sink will
// not publish again to clear it. The failure still reached the health, for
// the moment. Close marks the sink closed before it clears the path, so one
// of the two always runs after the failure.
func (s *sink) closedSinkFailed() {
	if s.isClosed() {
		s.b.recovered(&s.path)
	}
}

// isClosed reports whether Close has run.
func (s *sink) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// check is "enforced before I/O": the controls Kafka has no way to
// honour are refused here, with ErrUnsupported, before a byte is written.
//
// A driver that ignored them instead would publish a message the caller asked
// to be delayed, to expire, or to be de-duplicated, and the caller would have
// no way to find out. The four refusals stand in every metadata mode, because
// none of them is metadata: a delay is behaviour the broker does not have, and
// an envelope cannot add it.
func (s *sink) check(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() || s.b.cl.Context().Err() != nil {
		// The client's own Close is the second half: franz-go places a
		// record produced on a closed client before failing it, so its
		// failure would read as one that may have been sent
		// ([closedHolding]).
		return mq.ErrClosed
	}
	if m == nil {
		return errNilMessage
	}
	if o != nil {
		switch {
		case o.Delay > 0 || !o.DeliverAt.IsZero():
			return errNoSchedule
		case o.TTL > 0:
			return errNoTTL
		case o.PrioritySet:
			return errNoPriority
		case o.DedupKey != "":
			return errNoDedup
		}
	}
	if lim := s.b.caps.Limit(mq.LimitMaxMessageBytes); lim.Known && int64(len(m.Body)) > lim.Value {
		return mq.ErrMessageTooLarge
	}
	if s.meta == metaStrict {
		return Representable(m)
	}
	return nil
}

// build fills buf's record with what this sink publishes, enveloping the
// message when the metadata mode says to and copying its storage when own
// says the record has to outlive the call.
func (s *sink) build(ctx context.Context, buf *recbuf, m *mq.Message, o *mq.PublishOptions, own bool) error {
	wrapped := m
	if s.meta == metaEnvelope {
		var err error
		if wrapped, err = s.b.env.Wrap(m); err != nil {
			return err
		}
	}
	buf.sbuf = Encode(&buf.rec, wrapped, s.name, buf.sbuf)
	if wrapped != m {
		// The record's value is the envelope, and the marker is where that
		// is recorded: a consumer must never have to read the value to find
		// out.
		MarkEnveloped(&buf.rec)
		// The record key is the message key even so. The broker partitions,
		// orders and compacts by it without reading the value, and the
		// message Wrap returns has no key of its own, so a record built from
		// it alone went out keyless. The consumer reads the key back from
		// the envelope.
		buf.rec.Key = m.Key
		if own {
			// The envelope is freshly allocated storage nobody else holds,
			// so only the key, which is still the caller's, needs a copy.
			buf.rec.Key, buf.key = copyInto(buf.key, m.Key)
			own = false
		}
	}
	if o != nil && o.PartitionSet {
		if p, ok := ctx.(*placement); ok {
			p.partition, p.routed = o.Partition, true
		} else {
			RouteToPartition(ctx, &buf.rec, o.Partition)
		}
	}
	if s.b.marks {
		buf.rec.Partition = unplaced
	}
	if own {
		buf.retain()
	}
	return nil
}

// unplaced is the partition a record carries until franz-go places it.
//
// franz-go writes the partition it chose into the record before the record
// can join a batch (`pr.Partition = recBuf.partition` in sink.go's
// bufferRecord), and every produce request is made of batches. So a record
// whose promise runs while it still carries this was never in a request, and
// the broker cannot have it. That is the case of a record whose context ended
// while Produce waited for room in a full buffer, or while its topic's
// partitions were unknown: both fail before the record is placed.
const unplaced int32 = -1

// maySend reports whether franz-go may have put rec in a produce request,
// which is what makes a failure reported for it ambiguous. It answers yes for
// a nil rec and for a client whose records [sink.build] does not mark.
func (b *Backend) maySend(rec *kgo.Record) bool {
	return rec == nil || !b.marks || rec.Partition != unplaced
}

// produceFailed maps a produce failure onto the model, and is the one
// place this driver decides that an outcome is unknown rather than bad.
//
// Once franz-go has placed rec in a partition's buffer it may write it, and a
// context cancellation or a deadline after that point is not a failed
// publication: it is a publication whose outcome nobody knows. A record
// franz-go failed before placing it was never sent, and its failure is
// definite. Produce waits for room in a full buffer under the caller's
// context, and a caller who gave up on that wait was told "unknown" for a
// record that cannot be in the log, so it reconciled a publication that did
// not happen. [Backend.maySend] tells the two apart; a nil rec is a failure
// that names no one record, and counts as sent. The refusals `check` and
// `build` make never get here. A rule that calls an error ambiguous, such as
// franz-go's record timeout, is written for a record that may have been sent,
// so for one that was not the outcome is made definite ([neverSent]). The
// other way round, a record that may have been sent and that franz-go failed
// with an unknown topic was retried into that answer, so its outcome is
// unknown although the rule reads the answer as a refusal
// ([unknownAfterRetries]), and so is one that franz-go's Close failed while
// it held it ([closedHolding]).
//
// The code is rewritten to CodeSettlementUnknown for every ambiguous outcome,
// so that `errors.Is(err, mq.ErrSettlementUnknown)` answers yes as well as
// [github.com/iqhive/iqmq.IsAmbiguous]. The specification names the sentinel for exactly
// this condition, and a caller that has to call IsAmbiguous to find out is a
// caller who will not. The native error is preserved
// underneath, so `errors.Is(err, context.Canceled)` still answers yes too.
func (s *sink) produceFailed(rec *kgo.Record, err error) error {
	sent := s.b.maySend(rec)
	mapped := s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, sent)
	var oe *mq.OpError
	if !errors.As(mapped, &oe) {
		return mapped
	}
	if sent && (unknownAfterRetries(err) || closedHolding(err)) {
		oe.Ambiguous = true
	}
	if !oe.Ambiguous {
		return mapped
	}
	if !sent {
		return neverSent(oe)
	}
	s.b.ambiguous.Add(1)
	if oe.Code == mq.CodeSettlementUnknown {
		return mapped
	}
	return &mq.OpError{
		Driver:    oe.Driver,
		Operation: oe.Operation,
		Topic:     oe.Topic,
		Code:      mq.CodeSettlementUnknown,
		Retryable: oe.Retryable,
		Ambiguous: true,
		Err:       err,
	}
}

// fillReceipt writes what the broker said about this publication into the
// caller's own receipt slot.
//
// The storage is the caller's and the core has already zeroed it, so filling
// it in place is what keeps "no result struct allocation on success" true.
func (s *sink) fillReceipt(m *mq.Message, rec *kgo.Record) {
	if !s.receipts || m == nil || m.Receipt == nil {
		return
	}
	m.Receipt.Partition = rec.Partition
	m.Receipt.Offset = rec.Offset
	m.Receipt.Confirmation = s.confirm
	m.Receipt.ConfirmedAt = rec.Timestamp
	// Kafka assigns no message identifier: the coordinates ARE the
	// identifier, and the specification makes an empty MessageID "the broker assigns
	// none" rather than a claim.
}

// firstNonNil is gone. It existed only to answer a wholly failed batch with
// its first error, which is the rule driverkit.CollapseBatch replaced on the
// publish side and on the settlement side. Its absence is deliberate: a call site that reverted to the old
// rule would no longer compile.

// recbuf is a pooled record and the storage a buffered publication copies
// into.
//
// It exists and for the allocation budget at the same time. A
// publication that returns before its record has been written has to own the
// bytes, and a driver that allocated them per message would allocate three
// times per publication under ConfirmNone. The buffers are reused, so the
// steady state allocates nothing.
type recbuf struct {
	rec kgo.Record
	// sbuf holds the reserved header values Encode writes, so that the
	// identifier and the typed properties cost no conversion per field.
	sbuf []byte
	key  []byte
	val  []byte
	hbuf []byte

	// handoff decides who finishes a publication whose call does not wait
	// for the promise, or may stop waiting, and neither side ever blocks
	// the other: the promise, when the call has returned first, or the
	// call, when the promise has run first and left its outcome in err.
	handoff atomic.Int32
	err     error
	// ran is where the promise tells a waiting call ([sink.await]) that it
	// has run. It holds one signal, which the call always takes, so a
	// buffer goes back to the pool with it empty.
	ran chan struct{}
}

// The states of a [recbuf]'s handoff.
const (
	handoffOpen     int32 = iota // neither the call nor the promise has finished
	handoffLeft                  // the call returned nil; the promise reports the outcome and frees
	handoffAnswered              // the call returned an unknown outcome; the promise only frees
	handoffRan                   // the promise ran first; the call answers and frees
)

// finish is the promise's half of the handoff. It returns the state that says
// what the promise still has to do: report the outcome and free the record
// (handoffLeft), only free it (handoffAnswered), or nothing (handoffRan).
func (b *recbuf) finish(err error) int32 {
	b.err = err
	if b.handoff.CompareAndSwap(handoffOpen, handoffRan) {
		return handoffRan
	}
	return b.handoff.Load()
}

// leave is the call's half: it returns without the outcome, as nil, or as an
// answer of its own when answered is true, and the promise finishes. It
// reports false when the promise has run already; the call then answers from
// err and frees the record.
func (b *recbuf) leave(answered bool) bool {
	to := handoffLeft
	if answered {
		to = handoffAnswered
	}
	return b.handoff.CompareAndSwap(handoffOpen, to)
}

// emptyBytes is the non-nil empty slice, which is what keeps the specification's
// distinction between a nil body and an empty one through a copy.
var emptyBytes = []byte{}

// retain copies everything the record points at into the buffer's own storage,
// so the record may outlive the call that built it.
func (b *recbuf) retain() {
	b.rec.Key, b.key = copyInto(b.key, b.rec.Key)
	b.rec.Value, b.val = copyInto(b.val, b.rec.Value)
	b.hbuf = b.hbuf[:0]
	for i := range b.rec.Headers {
		v := b.rec.Headers[i].Value
		if v == nil {
			continue
		}
		start := len(b.hbuf)
		b.hbuf = append(b.hbuf, v...)
		b.rec.Headers[i].Value = b.hbuf[start:len(b.hbuf):len(b.hbuf)]
	}
}

// copyInto copies src into buf's storage, preserving the difference between a
// nil slice and an empty one. It returns the copy and buf, which the copy may
// have grown.
func copyInto(buf, src []byte) (dup, grown []byte) {
	switch {
	case src == nil:
		return nil, buf
	case len(src) == 0:
		return emptyBytes, buf
	default:
		buf = append(buf[:0], src...)
		return buf, buf
	}
}

// getRecbuf takes a record buffer out of the backend's pool, or builds one
// when the pool is empty.
func (b *Backend) getRecbuf() *recbuf {
	buf, _ := b.recbufs.Get().(*recbuf)
	if buf == nil {
		return &recbuf{ran: make(chan struct{}, 1)}
	}
	return buf
}

// putRecbuf hands a record buffer back to the backend's pool.
func (b *Backend) putRecbuf(buf *recbuf) {
	if buf == nil {
		return
	}
	buf.rec = kgo.Record{Headers: buf.rec.Headers[:0]}
	buf.handoff.Store(handoffOpen)
	buf.err = nil
	b.recbufs.Put(buf)
}

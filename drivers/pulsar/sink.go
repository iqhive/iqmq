package pulsar

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this sink is.
//
// All five publication surfaces are here, and each of them is a DIFFERENT
// pulsar-client-go call rather than a loop over the first:
//
//   - Sink.Publish is Producer.Send, which returns when the broker's send
//     receipt arrives — for a persistent topic, after the entry is written to
//     the write quorum of bookies.
//   - AsyncSink.PublishAsync is Producer.SendAsync, whose native callback is the done. There is no goroutine per message: the callback runs on the
//     client's own connection goroutine.
//   - BatchSink.PublishBatch is one SendAsync per message and one wait for all
//     of them, which is what lets the producer's own batching put them in ONE
//     entry — Pulsar has no "publish these together" verb, and the batch is a
//     property of the producer rather than of the call.
//   - OwnedSink.PublishOwned is SendAsync whose callback releases the caller's
//     storage.
//   - FlushSink.Flush is Producer.FlushWithCtx, which completes what batching
//     is still holding.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.AsyncSink = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
	_ mq.OwnedSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
)

// sink publishes to one Pulsar topic through the backend's cached producer.
//
// It holds the cache entry rather than the producer, because the producer in
// the entry is replaced when the client closes it for good (see
// [producerEntry]), and every sink on the topic has to move to the new one.
type sink struct {
	b        *Backend
	entry    *producerEntry
	topic    TopicName
	name     string
	iqTopic  string
	meta     mq.MetadataMode
	env      *driverkit.Envelope
	forced   *driverkit.Envelope
	receipts bool
	confirm  mq.ConfirmationMode
	props    map[string]string

	// mu guards closed and the admission of an asynchronous publication.
	//
	// The two are one decision and that is the point. inflight counts the
	// asynchronous publications this sink has admitted and not yet
	// completed, so that Close can wait for their callbacks rather than
	// leaving a goroutine holding the caller's message — and a WaitGroup's
	// Add must not race its own Wait. Taking the closed flag and the Add
	// under one mutex is what makes that impossible: once Close has set the
	// flag no publication is admitted, so every Add falls strictly before
	// the Wait.
	//
	// It is this repository's recurring defect class in the publication
	// direction: a placement that lands after the drain
	// leaves its obligation — here a native callback still holding the
	// caller's message — with nobody waiting for it.
	// TestConcurrentPublishAndClose found it under -race.
	//
	// handing counts the admitted publications that have not yet returned
	// from SendAsync, which is where pulsar-client-go puts the request in
	// the producer's queue. A close must not begin while one is on its way
	// in: a request that reaches the queue after the flush is dropped with
	// its callback by the close, and one that reaches it after the close
	// sends on a closed channel and panics (v0.21, internalSendAsync and
	// doClose). It is added with inflight, under the same mutex. A
	// synchronous Publish and a Flush have the same window, and since they
	// return only with an answer, the producer's close ends and waits for
	// them itself ([liveProducer.calls]).
	//
	// stopping is closed with closed set, and ends a publication's wait for
	// room in the producer's queue ([room]), which no close waits for.
	mu       sync.Mutex
	closed   bool
	inflight sync.WaitGroup
	handing  sync.WaitGroup
	stopping chan struct{}
}

// isClosed reports whether this sink has been closed.
func (s *sink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// admit reserves a place for one asynchronous publication, refusing once the
// sink is closed. Its caller calls [sink.handed] once SendAsync has returned.
//
// The reservation and the closed check are one atomic step: see [sink.mu].
func (s *sink) admit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.inflight.Add(1)
	s.handing.Add(1)
	return true
}

// handed records that an admitted publication has returned from SendAsync, so
// pulsar-client-go has it, answered or queued.
func (s *sink) handed() { s.handing.Done() }

// pending is one publication's scratch: the native message and the property
// map it points at.
//
// It is pooled because the map is the only allocation a publication would
// otherwise make, and because the client RETAINS the map until the send
// completes — so a scratch map shared by one sink would be read by one
// publication while another cleared it, which is a cross-message metadata swap
// under load rather than a crash (the shape the specification records for
// pooled option sets).
type pending struct {
	pm    pulsar.ProducerMessage
	props map[string]string
	// phase and refusal belong to a hand-off under BlockIfQueueFull
	// ([sink.send]): phase says whether the hand-off is under way and
	// whether the producer answered during it, and refusal records the
	// client's refusal for want of room ([refusalOf]). They live here, rather
	// than on the hand-off's stack, so that the callback reaching them costs
	// nothing.
	phase   atomic.Int32
	refusal atomic.Int32
}

// The phases of a hand-off under BlockIfQueueFull ([sink.send]).
const (
	// phaseSettled is no hand-off under way: the producer's answer pools p.
	phaseSettled int32 = iota
	// phaseHanding is inside SendAsync: an answer leaves p to the hand-off.
	phaseHanding
	// phaseAnswered is answered by the producer inside SendAsync: the
	// hand-off pools p once it has read refusal.
	phaseAnswered
)

// handing reports whether p's hand-off is still inside SendAsync.
func (p *pending) handing() bool { return p.phase.Load() == phaseHanding }

// answeredInside records that the producer answered p, and reports whether its
// hand-off was still inside SendAsync, which then pools p itself.
func (p *pending) answeredInside() bool {
	return p.phase.CompareAndSwap(phaseHanding, phaseAnswered)
}

// take borrows a scratch publication from the backend's pool, or builds one
// when the pool is empty.
func (b *Backend) take() *pending {
	if p, ok := b.pendings.Get().(*pending); ok {
		return p
	}
	return &pending{props: make(map[string]string, 8)}
}

// give returns a scratch publication, clearing what it pointed at so that a
// pooled message never keeps a caller's body alive.
func (b *Backend) give(p *pending) {
	p.pm = pulsar.ProducerMessage{}
	clear(p.props)
	p.phase.Store(phaseSettled)
	p.refusal.Store(notRefused)
	b.pendings.Put(p)
}

// Publish admits one message and waits for the broker's send receipt.
//
// The order is the contract: every refusal this driver can make without I/O
// is made before anything is written, and the envelope's own admission check is
// one of them.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	p, err := s.prepare(ctx, m, o)
	if err != nil {
		return err
	}
	h, err := s.b.live(ctx, s.entry)
	if err != nil {
		s.b.give(p)
		return err
	}
	if s.confirm == mq.ConfirmNone {
		// the weakest boundary: the client has admitted the
		// publication and this call does not wait for the broker. The
		// outcome still reaches the driver — the callback records a
		// failure in the health state and the counters — because a
		// publication whose failure nobody can see is a lost message
		// with no evidence.
		//
		// This call returns before the client has read the payload.
		detach(p, m)
		err := s.enqueue(ctx, h, p, func(_ pulsar.MessageID, serr error) {
			defer h.done()
			if serr != nil {
				h.lost(serr)
				// A publication a Close failed is the close's doing and
				// not a background failure of the broker's.
				if !s.isClosed() {
					s.b.degrade(s.fail(serr))
				}
			}
		})
		if err != nil {
			h.done()
			s.b.give(p)
			return err
		}
		return nil
	}
	if abandonable(ctx) {
		// A Send that gives up on its context returns while the
		// request may still be queued, payload unread.
		detach(p, m)
	}
	sendCtx, end, ok := h.call(ctx)
	if !ok {
		h.done()
		s.b.give(p)
		return mq.ErrClosed
	}
	id, sent, err := s.sendWithRoom(sendCtx, h, p)
	stopped := err != nil && halted(ctx, sendCtx, err)
	end()
	h.done()
	switch {
	case !sent:
		// It waited for room and never reached the client, which makes
		// any end of the wait a definite refusal.
		s.b.give(p)
		if stopped {
			return s.unsent()
		}
		return err
	case stopped:
		// The producer's close ended this Send, which may have queued
		// the request: the close fails it or drops it. Once the producer
		// has closed the client reads the payload no more, so the
		// caller's Body is free from then on, as when Send returns with
		// an answer.
		<-h.shut
		return s.failSent(pulsar.ErrProducerClosed, true)
	}
	if err != nil {
		h.lost(err)
		// A Send abandoned on its context leaves the request in the
		// client's send queue, whose event loop still reads p.pm and its
		// property map. Pooling them now would let the next publication
		// rewrite a message the client may yet send, so they are left
		// to the garbage collector instead. The payload is not the
		// caller's: detach copied it before the Send.
		if !sendAbandoned(err) {
			s.b.give(p)
		}
		return s.failSend(err)
	}
	s.fillReceipt(m, id)
	s.b.give(p)
	return nil
}

// sendWithRoom is a synchronous Publish's Send on h, under ctx, with the
// room h's queue has for it under Config.BlockIfQueueFull ([room]): it waits
// for a place first, and gives it back once Send has returned. sent reports
// whether the client was given the message, rather than the wait ending first.
//
// The client can still refuse it for want of room ([refusalOf]), which under
// BlockIfQueueFull is waited out too ([sink.waitOut]) before the Send is made
// again, until ctx ends or the sink is closed.
func (s *sink) sendWithRoom(ctx context.Context, h *liveProducer, p *pending) (id pulsar.MessageID, sent bool, err error) {
	if err := h.room.take(ctx, s.stopping); err != nil {
		return nil, false, err
	}
	var e *retrier
	for wait := roomRetry; ; wait = min(2*wait, roomRetryMax) {
		id, err = h.prod.Send(ctx, &p.pm)
		refusal := notRefused
		if h.room != nil {
			refusal = refusalOf(err)
		}
		if refusal == notRefused {
			if err != nil && h.room != nil && sendAbandoned(err) {
				// The client may still hold the request, out of the
				// room's count once its place goes back.
				h.room.abandoned()
			}
			h.room.leave(e)
			h.room.give()
			return id, true, err
		}
		if e, err = s.waitOut(ctx, h, refusal, e, wait); err != nil {
			return nil, false, err
		}
	}
}

// waitOut waits out the client's refusal of a publication for want of room
// under BlockIfQueueFull, until the publication is to be handed to the client
// again, holding a place in h's room. e is its place in the room's line, if it
// has one, and waitOut returns the one it has now. When ctx ends or the sink
// closes, waitOut gives up the place and the turn and returns ctx's error or
// ErrClosed.
//
// The refused publication keeps its place and its turn in the line
// ([room.retry]), except for a full queue while callers are giving up on
// synchronous publications on their deadlines ([room.abandoned]): the room
// they leave behind in the client comes back within a round trip, and with
// deadlines that short the line, which serves the oldest first, let almost
// nothing through, as the oldest were the nearest their deadlines. Then the
// refused publication gives its place back for a pause and waits for one
// again, and the publications that came after it, with their deadlines still
// ahead of them, try meanwhile.
func (s *sink) waitOut(ctx context.Context, h *liveProducer, refusal int32, e *retrier, wait time.Duration) (*retrier, error) {
	if refusal == refusedMemory || !h.room.abandonedLately() {
		e = h.room.refused(e)
		if err := h.room.retry(ctx, s.stopping, e, wait); err != nil {
			h.room.leave(e)
			h.room.give()
			return nil, err
		}
		return e, nil
	}
	h.room.leave(e)
	h.room.give()
	if err := pause(ctx, s.stopping, wait); err != nil {
		return nil, err
	}
	return nil, h.room.take(ctx, s.stopping)
}

// unsent is the answer for a publication the close of its producer ended
// before it reached the client: ErrClosed when the sink is what closed, and
// otherwise the definite ProducerClosed of a producer a replacement retired,
// which the next publication does not go through.
func (s *sink) unsent() error {
	if s.isClosed() {
		return mq.ErrClosed
	}
	return s.failSent(pulsar.ErrProducerClosed, false)
}

// enqueue hands p to h's producer for an asynchronous publication: it waits
// for room in h's queue under Config.BlockIfQueueFull ([room]), admits the
// publication ([sink.admit]) and sends it ([sink.send]), and answer runs once
// with its outcome. A non-nil return is a publication that was not handed
// over, for which answer never runs: the sink closed, or ctx ended while it
// waited for room.
//
// The client can still refuse it for want of room ([refusalOf]), which under
// BlockIfQueueFull is waited out too ([sink.waitOut]). Its admission is given
// back meanwhile, so that no close waits for it, and it is admitted again for
// each try.
func (s *sink) enqueue(ctx context.Context, h *liveProducer, p *pending, answer func(pulsar.MessageID, error)) error {
	if err := h.room.take(ctx, s.stopping); err != nil {
		return err
	}
	var e *retrier
	for wait := roomRetry; ; wait = min(2*wait, roomRetryMax) {
		if !s.admit() {
			h.room.leave(e)
			h.room.give()
			return mq.ErrClosed
		}
		refusal := s.send(ctx, h, p, answer)
		if refusal == notRefused {
			// The place is the publication's until it is answered.
			h.room.leave(e)
			return nil
		}
		s.inflight.Done()
		var err error
		if e, err = s.waitOut(ctx, h, refusal, e, wait); err != nil {
			return err
		}
	}
}

// send hands p to h's producer with SendAsync, for a publication [sink.admit]
// has counted, and runs answer once with its outcome: the producer's callback,
// or the failure the close of the producer gives a publication the close
// dropped ([liveProducer.close]); the place the publication took in h's room
// goes back first. It then ends the publication's count, and pools p only when
// the producer itself answered, which is the client's word that it has
// finished with p: after the close's own answer a racing callback can still
// read it.
//
// The hand-off is the producer's too ([liveProducer.hand]), so no close of h
// overtakes it. One refused because h's close has begun never reaches the
// producer, and gets the answer v0.21 gives a send on a closed producer,
// ProducerClosed.
//
// Under BlockIfQueueFull a refusal for want of room ([refusalOf]) is not the
// publication's answer: send reports it and leaves the publication's count and
// place to its caller ([sink.enqueue]), which waits it out. The callback
// records the refusal in p while the hand-off is inside SendAsync
// ([liveProducer.track]), and p.phase keeps p from going back to the pool
// while the hand-off may still read it: an answer that comes inside SendAsync
// leaves the pooling to the hand-off. send reports notRefused for a
// publication handed over.
func (s *sink) send(ctx context.Context, h *liveProducer, p *pending, answer func(pulsar.MessageID, error)) int32 {
	cb := h.track(p, func(id pulsar.MessageID, err error, byProducer bool) {
		defer s.inflight.Done()
		answer(id, err)
		if byProducer && !p.answeredInside() {
			s.b.give(p)
		}
	})
	if !h.hand() {
		cb(nil, &p.pm, pulsar.ErrProducerClosed)
		s.handed()
		return notRefused
	}
	if h.room == nil {
		h.prod.SendAsync(ctx, &p.pm, cb)
		h.handed()
		s.handed()
		return notRefused
	}
	p.refusal.Store(notRefused)
	p.phase.Store(phaseHanding)
	h.prod.SendAsync(ctx, &p.pm, cb)
	refusal := p.refusal.Load()
	if !p.phase.CompareAndSwap(phaseHanding, phaseSettled) {
		// The producer answered inside SendAsync and left p to this.
		s.b.give(p)
	}
	if refusal != notRefused {
		// Taken out of unanswered while the hand-off still holds the
		// producer's close off, which would otherwise answer it.
		h.answer(p)
	}
	h.handed()
	s.handed()
	return refusal
}

// detach gives the client its own copy of a publication's payload, for the
// paths that return to the caller before the client has finished with it.
//
// The Sink contract lets a caller reuse its Body the moment Publish returns.
// pulsar-client-go does not promise to have read the payload by then, and v0.21
// has not for a batched message: SendAsync and Send queue the request, and the
// producer's event loop copies the payload into the batch afterwards
// (internalSendAsync, dataChan, internalSend, batchBuilder.Add). Publish used to
// hand the client the caller's Body on those paths, so a caller that reused its
// buffer published whatever it wrote there next.
//
// The paths that wait for the native callback keep the caller's Body, because
// the callback is the client's word that it has finished with the message:
// PublishAsync and PublishOwned hold the message until it runs, and
// PublishBatch waits for every one.
//
// Only the caller's storage is copied. An enveloped payload is this driver's
// own and nobody else writes it, and an empty one has nothing to read.
func detach(p *pending, m *mq.Message) {
	if !aliases(p.pm.Payload, m.Body) {
		return
	}
	p.pm.Payload = bytes.Clone(p.pm.Payload)
}

// aliases reports whether payload is body's storage.
func aliases(payload, body []byte) bool {
	return len(payload) > 0 && len(body) > 0 && &payload[0] == &body[0]
}

// abandonable reports whether a Send on ctx can return before the client has
// finished with its message. Send gives up only when its context ends, and a
// context with no Done channel never does, so that publication keeps the
// caller's Body.
func abandonable(ctx context.Context) bool { return ctx.Done() != nil }

// PublishAsync admits one message and reports its outcome to done.
//
// done runs on pulsar-client-go's own callback goroutine, which is the native
// path the specification asks for: there is no goroutine per message and no completion pool
// of this driver's own. A non-nil return means done will not run, and a nil
// return means it runs exactly once.
//
// done must not wait for the driver, as AsyncSink says. It can run inside the
// close of a producer a replacement retired ([liveProducer.close]), and a
// Flush it made there would wait for that close to finish answering, done
// included, until its context ended ([sink.flushRetiring]).
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return errNilCallback
	}
	p, err := s.prepare(ctx, m, o)
	if err != nil {
		return err
	}
	h, err := s.b.live(ctx, s.entry)
	if err != nil {
		s.b.give(p)
		return err
	}
	err = s.enqueue(ctx, h, p, func(id pulsar.MessageID, serr error) {
		h.done()
		if serr != nil {
			h.lost(serr)
			done(s.fail(serr))
			return
		}
		s.fillReceipt(m, id)
		done(nil)
	})
	if err != nil {
		h.done()
		s.b.give(p)
		return err
	}
	return nil
}

// PublishOwned takes ownership of om's message and releases it exactly once
//
// The release happens in the native callback, which is the moment the client
// has finished with the body: SendAsync reads the payload on the producer's own
// goroutine after it returns, so releasing at invocation would hand the storage
// back while the client was still serialising it.
func (s *sink) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	if om.Release == nil {
		return errNoRelease
	}
	m := om.Message
	p, err := s.prepare(ctx, m, o)
	if err != nil {
		// Ownership transferred at invocation, so the release is this
		// sink's obligation even when it refuses the publication.
		om.Release(m)
		return err
	}
	h, err := s.b.live(ctx, s.entry)
	if err != nil {
		s.b.give(p)
		om.Release(m)
		return err
	}
	err = s.enqueue(ctx, h, p, func(id pulsar.MessageID, serr error) {
		h.done()
		h.lost(serr)
		if serr == nil {
			s.fillReceipt(m, id)
		}
		// Never under an internal lock, and exactly once: this is the
		// only place the callback can reach.
		om.Release(m)
	})
	if err != nil {
		h.done()
		s.b.give(p)
		om.Release(m)
		return err
	}
	return nil
}

// PublishBatch admits a batch.
//
// Pulsar has no batch verb: a batch is the PRODUCER's, formed from
// publications that arrive within BatchingMaxPublishDelay of one another. So
// this is one SendAsync per message and one wait for every callback, which is
// what lets the producer coalesce them into one entry — and the *BatchError it
// returns is index-aligned with ms, because a result aligned with anything else
// reports one message's outcome for another.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	errs := make([]error, len(ms))
	var wg sync.WaitGroup
	// The producer is looked up once, at the first message there is to
	// send, so a batch waits for a replacement once and every message it
	// could not send is refused with the same answer.
	var (
		h    *liveProducer
		herr error
	)
	for i, m := range ms {
		p, err := s.prepare(ctx, m, o)
		if err != nil {
			errs[i] = err
			continue
		}
		if h == nil && herr == nil {
			h, herr = s.b.live(ctx, s.entry)
		}
		if herr != nil {
			s.b.give(p)
			errs[i] = herr
			continue
		}
		wg.Add(1)
		err = s.enqueue(ctx, h, p, func(id pulsar.MessageID, serr error) {
			defer wg.Done()
			if serr != nil {
				h.lost(serr)
				errs[i] = s.fail(serr)
			} else {
				s.fillReceipt(m, id)
			}
		})
		if err != nil {
			wg.Done()
			s.b.give(p)
			errs[i] = err
		}
	}
	wg.Wait()
	if h != nil {
		// One admission covers the batch, which has finished sending.
		h.done()
	}
	return batchResult(errs)
}

// Flush completes the publications the producer's batching is still holding
//
// A Flush still inside the producer when the producer closes is ended by the
// close (see [liveProducer.calls]), which flushes the producer itself and
// answers each publication. When the sink or the backend is what closed, the
// Flush reports ErrClosed. When the producer closed is one the client closed for
// good and a replacement has retired, the sink is still open, and the Flush
// reports what v0.21 gives a flush whose producer closes under it, and what
// this sink gives a Flush on a producer the client has closed: an ambiguous
// ProducerClosed. The next Flush goes through the replacement.
//
// That next Flush, and any Flush after a replacement, also waits for the
// publications still unanswered on the producers the replacement is retiring
// from this sink's topic ([sink.flushRetiring]). It used to flush the
// replacement alone and return nil at once, while a publication admitted
// before it was still unanswered on the producer the client had closed.
func (s *sink) Flush(ctx context.Context) error {
	if s.isClosed() {
		return mq.ErrClosed
	}
	h, err := s.b.live(ctx, s.entry)
	if err != nil {
		return err
	}
	flushCtx, end, ok := h.call(ctx)
	if !ok {
		h.done()
		return s.endedFlush()
	}
	err = h.prod.FlushWithCtx(flushCtx)
	stopped := err != nil && halted(ctx, flushCtx, err)
	end()
	h.done()
	switch {
	case stopped:
		return s.endedFlush()
	case err != nil:
		h.lost(err)
		return s.fail(err)
	}
	return s.flushRetiring(ctx)
}

// flushRetiring is the rest of a Flush once the producer the sink publishes
// through is flushed: each producer a replacement is retiring from the sink's
// topic is flushed too, and its publications are waited for, until ctx ends.
//
// Such a producer is one the client closed for good, which v0.21 can no longer
// flush, or one partition of which it closed, whose flush stops at that
// partition. A publication on it is answered by its receipt, by its
// SendTimeout, or, for one the client dropped from its batch, only by the
// close the replacement makes once the retirement's bound is up, as ambiguous
// ([Backend.replace]). So the wait is for every publication handed to it
// ([liveProducer.settle]), as the backend's own flush waits, and when the close
// answered some, for their callbacks too. The Flush then reports the ambiguous
// ProducerClosed of a flush whose producer closed; and ctx's error, when ctx
// ends with some still unanswered.
func (s *sink) flushRetiring(ctx context.Context) error {
	s.b.mu.Lock()
	hs := s.b.retiringFrom(s.entry)
	s.b.mu.Unlock()
	for _, h := range hs {
		if flushCtx, end, ok := h.call(ctx); ok {
			// Its answer says nothing the wait below does not.
			_ = h.prod.FlushWithCtx(flushCtx)
			end()
		}
		h.settle(ctx)
		unanswered, dropped := h.outcome(ctx)
		switch {
		case unanswered && ctx.Err() != nil:
			return s.fail(ctx.Err())
		case dropped:
			return s.endedFlush()
		}
	}
	return nil
}

// endedFlush is a Flush's answer when the close of its producer ended it or
// refused it: see [sink.Flush].
func (s *sink) endedFlush() error {
	if s.isClosed() {
		return mq.ErrClosed
	}
	return s.fail(pulsar.ErrProducerClosed)
}

// batchResult turns per-message outcomes into the result: nil when every
// message was accepted, a *BatchError when some were, and a single error when
// none were.
//
// It is driverkit.CollapseBatch, so "a single error" means one only when every
// message has the same outcome; it used to return the first error
// whenever none was accepted, so a batch of one definite refusal and one
// ambiguous failure read as a refusal.
func batchResult(errs []error) error { return driverkit.CollapseBatch(errs) }

// prepare is every refusal this sink can make without I/O, and the native
// message to publish.
func (s *sink) prepare(ctx context.Context, m *mq.Message, o *mq.PublishOptions) (*pending, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.isClosed() {
		return nil, mq.ErrClosed
	}
	if m == nil {
		return nil, errNilMessage
	}
	delay, err := s.delayOf(o)
	if err != nil {
		return nil, err
	}
	out, err := wrapForPublish(s.env, s.forced, s.b.limit, s.meta, m)
	if err != nil {
		return nil, err
	}
	if s.meta == mq.MetadataLenientDrop && out != m {
		s.b.dropped.Add(uint64(droppedMetadata(m)))
	}
	// The size check is on what will actually be written, which for an
	// enveloped message is the envelope: a body just under the broker's
	// maximum plus its headers is over it, and finding that out from the
	// broker costs a round trip and a MessageTooBig.
	if lim := s.b.cfg.MaxMessageBytes; lim > 0 && int64(len(out.Body)) > lim {
		return nil, mq.ErrMessageTooLarge
	}
	// A pulsar.ProducerMessage carries no topic: the producer is bound to
	// one, which is why this driver caches a producer per topic.
	p := s.b.take()
	Encode(&p.pm, out, p.props)
	if delay > 0 {
		p.pm.DeliverAfter = delay
	}
	return p, nil
}

// delayOf is the publication delay, and every refusal about it.
func (s *sink) delayOf(o *mq.PublishOptions) (time.Duration, error) {
	if o == nil {
		return 0, nil
	}
	switch {
	case o.TTL > 0:
		// Pulsar's time to live is a NAMESPACE policy, applied to every
		// message in the namespace, and there is no per-message field
		// for it at all.
		return 0, errNoTTL
	case o.PrioritySet:
		// Pulsar's PriorityLevel is a CONSUMER's priority, which decides
		// who gets a message when several are waiting. It is not a
		// message priority and a driver that mapped one onto the other
		// would give a caller the opposite of what it asked for.
		return 0, errNoPriority
	case o.DedupKey != "":
		return 0, errNoDedup
	case o.PartitionSet:
		return 0, errNoPartition
	}
	delay := o.Delay
	if !o.DeliverAt.IsZero() {
		delay = o.DeliverAt.Sub(s.b.now())
		if delay < 0 {
			// A time already past is "deliver now", not an error: the
			// caller asked for a message to be deliverable at an
			// instant, and that instant has come.
			delay = 0
		}
	}
	switch {
	case delay == 0:
		return 0, nil
	case delay < 0:
		return 0, errNegativeDelay
	case delay > MaxDelay:
		return 0, errDelayTooLong
	case s.b.cfg.SubscriptionType.Exclusive():
		// Pulsar's delayed delivery lives in the broker's Shared and
		// Key_Shared dispatchers. An Exclusive or Failover subscription
		// receives a delayed message as soon as it reaches the topic, so
		// publishing one from a client configured for those subscription
		// types would be a schedule that silently did not happen.
		return 0, errNoDelayForExclusive
	}
	return delay, nil
}

// fillReceipt records what the broker said about an accepted publication
//
// The storage is the caller's and the core has already zeroed it, so filling it
// in place is what keeps "no result struct allocation on success" true. It is
// the one field of m a sink may write.
func (s *sink) fillReceipt(m *mq.Message, id pulsar.MessageID) {
	if !s.receipts || m == nil || m.Receipt == nil || id == nil {
		return
	}
	m.Receipt.MessageID = id.String()
	m.Receipt.Confirmation = s.confirm
	m.Receipt.ConfirmedAt = s.b.now()
	if p := id.PartitionIdx(); p >= 0 {
		m.Receipt.Partition = p
	}
	m.Receipt.Native = id
}

// fail maps a publication's native error and records a background failure when
// the link is what went wrong.
//
// MapNet rather than Map, and the sentAlready argument is [PublishAdmitted]:
// the ambiguity is about whether the publication had left this process, and
// no Pulsar result code says so on its own. The kit classifies the transport
// FIRST, so a rule can refine the code and cannot discard the ambiguity.
func (s *sink) fail(err error) error { return s.failSent(err, PublishAdmitted(err)) }

// failSend is fail for Producer.Send's own answer.
//
// A bare context error from Send is not the client's refusal: Send returns
// ctx.Err() the moment the context ends, while the request may still be in
// the client's send queue or already on the wire, so the publication may yet
// reach the broker. prepare checked the context before the hand-off, so this
// one ended after it.
func (s *sink) failSend(err error) error {
	return s.failSent(err, PublishAdmitted(err) || sendAbandoned(err))
}

// sendAbandoned reports whether Send gave up waiting on its context rather
// than the client answering.
func sendAbandoned(err error) bool {
	if _, ok := ResultOf(err); ok {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// failSent is fail with the ambiguity decided by the caller.
func (s *sink) failSent(err error, sentAlready bool) error {
	mapped := s.b.errs.MapNet(mq.OpPublish, s.iqTopic, "", err, sentAlready)
	if sentAlready {
		markAmbiguous(mapped)
	}
	if mq.CodeOf(mapped) == mq.CodeUnavailable {
		s.b.degrade(mapped)
	}
	return mapped
}

// markAmbiguous makes a mapped publication failure ambiguous.
//
// MapNet turns sentAlready into ambiguity only for a transport error: a context
// that ended, an EOF, a net.Error. pulsar-client-go fails a publication with a
// *pulsar.Error result code instead, which the kit classifies by its rules
// alone, so a code [PublishAdmitted] said may have been written came back
// definite unless its rule said otherwise, and only TimeoutError's did. The
// error is the one MapNet has just built, so setting the flag changes nobody
// else's.
func markAmbiguous(err error) {
	var oe *mq.OpError
	if errors.As(err, &oe) {
		oe.Ambiguous = true
	}
}

// Close releases the sink and drops its reference to the topic's producer.
//
// It waits for the asynchronous publications this sink admitted, because each
// of their callbacks still holds the caller's message: the specification says the callback
// runs exactly once and the specification says the message is borrowed until it does, so a
// Close that returned first would leave a caller unable to know when its
// storage came back.
//
// It first waits for every admitted publication to return from SendAsync (see
// [sink.handing]), and then drops the reference, before it waits for the
// callbacks. When the reference was the producer's last, the producer is
// flushed, for at most OperationTimeout, and closed, which fails what is still
// waiting for its receipt, or was dropped unanswered, as ambiguous, so that
// wait ends whether or not the broker answers ([liveProducer.close]). The close
// first ends each synchronous Publish and Flush still inside the producer, on
// any sink. While other sinks still hold the producer it stays open, and the
// wait is for each publication's receipt or its SendTimeout: with SendTimeout
// disabled and a broker that stays silent, that wait has no bound. A
// publication still waiting for room in the producer's queue under
// Config.BlockIfQueueFull has not reached the client, and Close ends its wait
// at once: it reports ErrClosed. Close used to wait for an asynchronous one,
// for as long as the broker kept the queue full.
//
// It is idempotent through the closed flag rather than through a sync.Once,
// because the flag is what a publication has to read anyway: the first caller
// takes it under the mutex and every later one returns at once.
func (s *sink) Close() error {
	if !s.stop() {
		return nil
	}
	s.handing.Wait()
	s.b.releaseProducer(context.Background(), s.name)
	// Outside the mutex, and after the flag: every Add fell before this
	// Wait, and no publication can be admitted from here on.
	s.inflight.Wait()
	s.b.forget(nil, s)
	return nil
}

// stop refuses every later publication, reporting whether this call is the one
// that closed the sink.
func (s *sink) stop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.closed = true
	close(s.stopping)
	return true
}

// The sink's refusals.
var (
	errNoDurableConfirm = errcode.New(errcode.Unsupported,
		"iqmq/pulsar: a non-persistent topic is acknowledged by a broker that holds the message in memory and will never write it down, so ConfirmDurable cannot be honoured (the message contract); a persistent:// topic's send receipt IS the durable boundary")
	errNoTTL               = errcode.New(errcode.Unsupported, "iqmq/pulsar: Pulsar's time to live is a namespace policy and has no per-message field")
	errNoPriority          = errcode.New(errcode.Unsupported, "iqmq/pulsar: Pulsar's PriorityLevel is a consumer's priority and not a message's")
	errNoDedup             = errcode.New(errcode.Unsupported, "iqmq/pulsar: Pulsar de-duplicates by (producer name, sequence number) and not by a caller's key, so a DedupKey cannot be honoured")
	errNoPartition         = errcode.New(errcode.Unsupported, "iqmq/pulsar: a Pulsar partition is chosen by the producer's router from the message key; there is no per-publication partition field")
	errNegativeDelay       = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: a publication delay cannot be negative")
	errDelayTooLong        = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the delay is longer than pulsar.MaxDelay; Pulsar's delayed-delivery tracker holds an index entry per delayed message, so a very distant delivery time is a broker-side leak rather than a schedule")
	errNoDelayForExclusive = errcode.New(errcode.Unsupported,
		"iqmq/pulsar: Pulsar's delayed delivery belongs to the broker's Shared and Key_Shared dispatchers, so an Exclusive or Failover subscription receives a delayed message at once; this client is configured for one of those subscription types and refuses the schedule rather than making one that does not happen")
	errNilCallback = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: PublishAsync needs a callback")
	errNoRelease   = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: PublishOwned needs a Release function; ownership with no way to give the storage back is a leak by construction")
)

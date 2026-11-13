package gcppubsub

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The sink implements the mandatory publication surface, the optional batched,
// asynchronous, owned and flushable ones.
//
// All four optional surfaces are here because the client library already has
// them: its Publish is asynchronous by construction, so the synchronous path
// is the one that costs a wait and the asynchronous path is the library's own
// shape. What the driver adds is the completion pool ([completionPool]) and
// the ordering-key resume ([keyLedger]), neither of which the library does for
// you.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
	_ mq.AsyncSink = (*sink)(nil)
	_ mq.OwnedSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
)

// sink is one open publisher.
type sink struct {
	b     *Backend
	cfg   mq.SinkConfig
	enc   *encoder
	pub   psapi.Publisher
	topic string
	keys  keyLedger

	// handed is set once a publication has been handed to the library, and
	// until then Flush leaves the library's Flush alone (see [sink.Flush]).
	handed atomic.Bool

	// mu orders a Flush that starts the library's flush against Close:
	// either the Flush registers its goroutine in flushes before Close sets
	// closed, and Close waits for it, or it sees closed and starts nothing.
	mu      sync.Mutex
	flushes sync.WaitGroup
	once    sync.Once
	closed  atomic.Bool
}

// Publish admits one message and waits for the service's answer.
//
// The wait happens on the CALLER's goroutine and not in the completion pool,
// because the caller is already blocked: see [completionPool] for why that is
// not an inconsistency.
//
// A context that ends during the wait ends the call, and the library still
// holds the message, which it may send later. The SPI lets the caller reuse
// the body as soon as the call returns, so a body the library could still
// read then is copied first ([sink.ownBody]). An ordered publication under a
// context that can end first takes a place in the completion queue, for its
// key's count should the wait end first ([sink.holdPlace]): the wait for that
// place is bounded by ctx, as PublishAsync's is, and a refusal there is one
// nothing was sent for.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	out, err := s.prepare(m, o)
	if err != nil {
		return err
	}
	key := out.Out.OrderingKey
	place, err := s.holdPlace(ctx, key)
	if err != nil {
		return err
	}
	s.ownBody(ctx, &out)
	res := s.handOver(ctx, &out.Out)
	id, perr := res.Get(ctx)
	s.answered(ctx, place, res, key, perr)
	if perr != nil {
		return s.mapPublish(perr)
	}
	s.writeReceipt(m, id)
	return nil
}

// PublishBatch admits every message in ms, reporting per-message outcomes
// through a *BatchError whose Errs are index-aligned with ms.
//
// Every message is handed to the library's bundler first and every result is
// waited for afterwards, which is what makes the batch one round trip rather
// than len(ms) of them: the bundler is what decides how many publish requests
// it becomes.
//
// A message this driver refuses before any I/O — an option the broker cannot
// honour, metadata the attributes cannot carry — fails at its own index and
// the rest of the batch is still published. That is the contract and it is
// the reason the encoding happens in a first pass: a refusal discovered
// half-way through a batch would leave the earlier messages published and the
// later ones not, with one error to describe both.
//
// Under [Config.Ordering] the exception is the rest of a refused message's
// ordering key: each later message of the key is answered as not attempted
// and is not published. The client library pauses a key only when a
// publication of it fails, and a refused message never reached it, so the
// later message of the key was published, and the caller's retry of the
// refused one, once fixed, landed behind it. The core holds a key back the same
// way across the pieces of a split batch. A message that reached the library
// and failed there holds the rest of its key back through the library's own
// pause, which the driver keeps until the library has answered every message
// of the key ([keyLedger]): each later message fails with the paused-key
// error, which is retryable.
//
// Each body the library could still read once the call has returned is copied
// first, as Publish copies one, and the batch takes one place in the
// completion queue before its first ordered message, as Publish does for one:
// a refusal there answers every ordered message of the batch as not attempted
// (the refusal's code, retryable), as BatchSink asks for messages never sent,
// and the messages without a key are still published.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	errs := make([]error, len(ms))
	results := make([]psapi.Result, len(ms))
	keys := make([]string, len(ms))
	var stopped map[string]error
	var place, tried bool
	var refused error
	for i, m := range ms {
		out, err := s.prepare(m, o)
		if err != nil {
			errs[i] = err
			if key := s.orderingKey(m); key != "" {
				if stopped == nil {
					stopped = make(map[string]error)
				}
				stopped[key] = err
			}
			continue
		}
		key := out.Out.OrderingKey
		if err, ok := stopped[key]; ok {
			errs[i] = s.notAttempted(err)
			continue
		}
		if key != "" && !tried {
			tried = true
			place, refused = s.holdPlace(ctx, key)
		}
		if key != "" && refused != nil {
			errs[i] = s.notAttempted(refused)
			continue
		}
		s.ownBody(ctx, &out)
		keys[i] = key
		results[i] = s.handOver(ctx, &out.Out)
	}
	var later []int
	for i, res := range results {
		if res == nil {
			continue
		}
		id, err := res.Get(ctx)
		if place && waitEnded(ctx, keys[i], err) {
			later = append(later, i)
		} else {
			s.keys.answer(s.pub, keys[i], err != nil)
		}
		if err != nil {
			errs[i] = s.mapPublish(err)
			continue
		}
		s.writeReceipt(ms[i], id)
	}
	if place {
		s.answerLater(later, results, keys)
	}
	return collapse(errs)
}

// PublishAsync admits one message and reports its outcome to done.
//
// done runs on a completion worker, exactly once, and never on the calling
// goroutine. A refused submission returns its error and done never runs, and
// the refusal comes before the library sees the publication, so it is a
// definite one: that is the one shape that keeps the specification's "exactly once" true
// without the core having to guess whether the driver took the callback.
//
// The body is not copied: the message is borrowed until done runs, and done
// runs once the library has answered, when it no longer reads the body.
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	out, err := s.prepare(m, o)
	if err != nil {
		return err
	}
	// The place in the completion queue is taken before the library sees
	// the publication, so a refusal here is one nothing was sent for.
	if err := s.b.pool.reserve(ctx); err != nil {
		return err
	}
	key := out.Out.OrderingKey
	s.b.pool.commit(completion{
		res: s.handOver(ctx, &out.Out),
		done: func(id string, perr error) {
			s.keys.answer(s.pub, key, perr != nil)
			if perr != nil {
				done(s.mapPublish(perr))
				return
			}
			s.writeReceipt(m, id)
			done(nil)
		},
	})
	return nil
}

// PublishOwned takes ownership of om's message and releases it exactly once.
//
// The release happens before the call returns, on the caller's goroutine, on
// every path. The body is copied first when the library could still read it
// after that ([sink.ownBody]), exactly as Publish copies one, so a release
// never hands back storage the library still reads; ownership saves the copy
// only under a context that cannot end, when the call returns once the
// library has answered. The release used to happen on return with the
// caller's own body still in the library, which then sent whatever the
// owner's pool had put there.
//
// The release is never left to the completion pool, though OwnedSink would
// allow a later one: a Release may publish again, and a Release on a
// completion worker that waited for room in the completion queue would wait
// for itself.
func (s *sink) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	release := func() {
		if om.Release != nil {
			om.Release(om.Message)
		}
	}
	if s.closed.Load() {
		release()
		return mq.ErrClosed
	}
	out, err := s.prepare(om.Message, o)
	if err != nil {
		release()
		return err
	}
	key := out.Out.OrderingKey
	place, err := s.holdPlace(ctx, key)
	if err != nil {
		release()
		return err
	}
	s.ownBody(ctx, &out)
	res := s.handOver(ctx, &out.Out)
	id, perr := res.Get(ctx)
	s.answered(ctx, place, res, key, perr)
	if perr != nil {
		release()
		return s.mapPublish(perr)
	}
	// The receipt is in the message's storage, so it is written before the
	// release hands that storage back.
	s.writeReceipt(om.Message, id)
	release()
	return nil
}

// Flush completes the publications already admitted.
//
// The library's Flush takes no context, so the wait is performed on a
// goroutine and the context bounds this call rather than the flush: a Flush
// that outlives its caller's deadline is one the library is still finishing,
// and the publications it is finishing are still outstanding. The goroutine is
// per FLUSH and not per message, which is the distinction rule 3 draws, and
// the sink owns it: Close waits for it before it stops the publisher, because
// the library's Flush reads the publisher's stopped flag without the lock its
// Stop writes it under.
//
// A context that had ended before the call gets ctx's error every time, as
// from the other drivers whose flush reads an ended context. The library's
// flush is still started, so an ended Flush still sends. It used to depend on
// a select between the flush goroutine and ctx.Done, and an idle publisher
// answered nil now and then.
//
// Until a publication has been handed to the library there is nothing to
// flush, and the library's Flush is not called: the library builds its bundler
// on the first Publish, under its lock, and its Flush reads the bundler
// without that lock.
func (s *sink) Flush(ctx context.Context) error {
	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return mq.ErrClosed
	}
	var done chan struct{}
	if s.handed.Load() {
		done = make(chan struct{})
		s.flushes.Go(func() {
			defer close(done)
			s.pub.Flush()
		})
	}
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close releases the publisher, flushing what it holds. It is idempotent.
func (s *sink) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed.Store(true)
		s.mu.Unlock()
		// A Flush that returned on its context left its goroutine in the
		// library's Flush, and Stop beside it is a data race inside the
		// library. Stop flushes anyway, so waiting for those flushes first
		// costs Close nothing.
		s.flushes.Wait()
		// Stop flushes and waits, which is what makes a publication admitted
		// before Close one that is still published: the specification's "Close completes
		// or fails what was admitted", and the reason the completion pool is
		// closed after every sink.
		s.pub.Stop()
		s.b.forget(nil, s)
	})
	return nil
}

// ownBody copies out's body when the library could still read it after the
// call that hands it over has returned, which the SPI forbids for a body that
// is the caller's: Sink.Publish's caller may reuse it the moment the call
// returns.
//
// A synchronous publication returns before the library has answered only when
// its context ends during the wait, so a context that can never end needs no
// copy. An enveloped body is the driver's own encoding and needs none either,
// and nor does an empty one. PublishOwned copies as Publish does, because it
// releases the storage on return. PublishAsync never copies: it borrows the
// message until done runs, after the library's answer.
func (s *sink) ownBody(ctx context.Context, out *encoded) {
	if out.Enveloped || len(out.Out.Data) == 0 || ctx.Done() == nil {
		return
	}
	out.Out.Data = bytes.Clone(out.Out.Data)
}

// holdPlace reserves a place in the completion queue for a synchronous
// publication of key under a context that can end, and reports whether it
// did. The place is for what [sink.answered] may have to leave to the pool,
// and it is taken before the library sees the publication: the wait for it is
// bounded by ctx, as PublishAsync's is, and a refusal is the definite answer
// of a publication nothing was sent for. A publication without a key, or
// under a context that cannot end, needs no place and takes none.
func (s *sink) holdPlace(ctx context.Context, key string) (bool, error) {
	if key == "" || ctx.Done() == nil {
		return false, nil
	}
	if err := s.b.pool.reserve(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// handOver gives one publication to the library, counting it against its
// ordering key first.
func (s *sink) handOver(ctx context.Context, out *psapi.Outgoing) psapi.Result {
	s.keys.admit(out.OrderingKey)
	res := s.pub.Publish(ctx, out)
	if !s.handed.Load() {
		s.handed.Store(true)
	}
	return res
}

// waitEnded reports whether a Get of an ordered publication that returned err
// may have returned before the library answered: it failed after ctx had
// ended, and the library may then still hold the publication and send it.
func waitEnded(ctx context.Context, key string, err error) bool {
	return key != "" && err != nil && ctx.Err() != nil
}

// answered counts the answer to one synchronous publication of key against
// the key ([keyLedger]) once its result's Get has returned err, and gives back
// the place [sink.holdPlace] took for it, if any.
//
// A wait that ended (waitEnded) is not the library's answer: counted now, the
// key could be resumed while the library still held the publication. Its
// count is left to the completion pool instead, in the place held for it,
// and the pool counts it once the library has answered. Nothing else runs
// there: the caller's own code, a release among it, stays on the caller's
// goroutine.
func (s *sink) answered(ctx context.Context, place bool, res psapi.Result, key string, err error) {
	if place && waitEnded(ctx, key, err) {
		s.b.pool.commit(completion{res: res, done: func(_ string, err error) {
			s.keys.answer(s.pub, key, err != nil)
		}})
		return
	}
	s.keys.answer(s.pub, key, err != nil)
	if place {
		s.b.pool.unreserve()
	}
}

// answerLater uses the place a synchronous call holds in the completion
// queue: for the publications at the indexes later, whose waits ended, it
// commits one completion that waits for the library's answer to each and
// counts it against its key; with none, it gives the place back.
func (s *sink) answerLater(later []int, results []psapi.Result, keys []string) {
	if len(later) == 0 {
		s.b.pool.unreserve()
		return
	}
	held := &heldAnswers{res: make([]psapi.Result, len(later)), errs: make([]error, len(later))}
	heldKeys := make([]string, len(later))
	for j, i := range later {
		held.res[j], heldKeys[j] = results[i], keys[i]
	}
	s.b.pool.commit(completion{res: held, done: func(string, error) {
		for j, key := range heldKeys {
			s.keys.answer(s.pub, key, held.errs[j] != nil)
		}
	}})
}

// heldAnswers is the outcome of the publications a synchronous call left to
// the completion pool, as one [psapi.Result]: Get waits for each in turn and
// keeps its error.
type heldAnswers struct {
	res  []psapi.Result
	errs []error
}

// Get waits for the library's answer to every publication.
func (h *heldAnswers) Get(ctx context.Context) (string, error) {
	for i, r := range h.res {
		_, h.errs[i] = r.Get(ctx)
	}
	return "", nil
}

// prepare encodes one message, refusing before any I/O.
func (s *sink) prepare(m *mq.Message, o *mq.PublishOptions) (encoded, error) {
	if m == nil {
		return encoded{}, errNilMessage
	}
	return s.enc.encode(m, o)
}

// orderingKey is the ordering key m would be published under, and "" when it
// has none: ordering is off, m has no key, or the key is one the service
// refuses, which every message of it is refused for anyway.
func (s *sink) orderingKey(m *mq.Message) string {
	if m == nil {
		return ""
	}
	key, err := s.enc.orderingKey(m)
	if err != nil {
		return ""
	}
	return key
}

// notAttempted is the answer for a message held back because an earlier
// message of its ordering key failed with err: DEFINITE, because it never
// reached the client library, and retryable, with errcode.NotAttempted's
// code, as the core's own not-attempted answer is.
func (s *sink) notAttempted(err error) error {
	return &mq.OpError{
		Driver:    DriverName,
		Operation: mq.OpPublish,
		Topic:     s.cfg.Topic,
		Code:      mq.ErrorCode(errcode.NotAttempted(errcode.Code(mq.CodeOf(err)))),
		Retryable: true,
		Err:       errcode.ErrNotAttempted,
	}
}

// writeReceipt fills the caller's receipt, in place, on success.
//
// Nothing is allocated for it: the storage is the caller's, and a sink that
// was not configured with Receipts never touches it.
func (s *sink) writeReceipt(m *mq.Message, id string) {
	if !s.cfg.Receipts || m == nil || m.Receipt == nil {
		return
	}
	m.Receipt.MessageID = id
	// PublishResult.Get answers once the service has stored the message in
	// at least two zones, so the boundary really is the durable one whatever
	// the publisher asked for — and the specification wants the boundary the publication
	// REACHED rather than the one it wanted.
	m.Receipt.Confirmation = mq.ConfirmDurable
	m.Receipt.ConfirmedAt = s.b.now()
}

// mapPublish maps a publication failure to the model.
//
// A context that ended while the driver waited on PublishResult.Get does not
// un-send the message: the library's bundler holds it and the service may
// store it, so the failure is mapped as one after the request may have left --
// ambiguous, with its Canceled or DeadlineExceeded code kept. Map reported it
// as a definite refusal, and a caller who republishes definite refusals
// duplicated the message. The one context error from Get
// that IS definite is flow control refusing the message before the bundler
// took it (pubsub's Publish sets the result's error then); the result cannot
// tell the two apart, and the specification makes the ambiguous answer the safe one.
func (s *sink) mapPublish(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return s.b.errs.MapNet(mq.OpPublish, s.cfg.Topic, "", err, true)
	}
	return s.b.errs.Map(mq.OpPublish, s.cfg.Topic, "", err)
}

// collapse is the batch answer: nil, the single error when every entry failed
// with the same outcome, or the index-aligned *BatchError. It is
// driverkit.CollapseBatch; it used to return the first error whenever every
// entry failed, so a batch of one definite refusal and one ambiguous failure
// read as a refusal.
func collapse(errs []error) error { return driverkit.CollapseBatch(errs) }

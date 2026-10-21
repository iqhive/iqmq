package gcppubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The sink implements the mandatory publication surface, the optional batched,
// asynchronous, owned and flushable ones.
//
// All four optional surfaces are here because the client library already has
// them: its Publish is asynchronous by construction, so the synchronous path
// is the one that costs a wait and the asynchronous path is the library's own
// shape. What the driver adds is the completion pool ([completionPool]) and
// the ordering-key resume, neither of which the library does for you.
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

	once   sync.Once
	closed atomic.Bool
}

// Publish admits one message and waits for the service's answer.
//
// The wait happens on the CALLER's goroutine and not in the completion pool,
// because the caller is already blocked: see [completionPool] for why that is
// not an inconsistency.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	out, err := s.prepare(m, o)
	if err != nil {
		return err
	}
	res := s.pub.Publish(ctx, &out.Out)
	id, perr := res.Get(ctx)
	if perr != nil {
		resumeKey(s.pub, out.Out.OrderingKey)
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
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	errs := make([]error, len(ms))
	results := make([]psapi.Result, len(ms))
	keys := make([]string, len(ms))
	for i, m := range ms {
		out, err := s.prepare(m, o)
		if err != nil {
			errs[i] = err
			continue
		}
		keys[i] = out.Out.OrderingKey
		results[i] = s.pub.Publish(ctx, &out.Out)
	}
	for i, res := range results {
		if res == nil {
			continue
		}
		id, err := res.Get(ctx)
		if err != nil {
			resumeKey(s.pub, keys[i])
			errs[i] = s.mapPublish(err)
			continue
		}
		s.writeReceipt(ms[i], id)
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
	s.b.pool.commit(completion{
		res: s.pub.Publish(ctx, &out.Out), pub: s.pub, key: out.Out.OrderingKey,
		done: func(id string, perr error) {
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

// PublishOwned takes ownership of om's message and releases it exactly once
//
// The release happens once the publication's outcome is known, because that is
// the first moment the message's storage is provably no longer needed: the
// encoding copies the body into the library's own message, so in principle the
// release could happen sooner — and doing it sooner would hand the caller's
// storage back while this function still holds a pointer to it, which is the
// alias bug exists to prevent.
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
	res := s.pub.Publish(ctx, &out.Out)
	id, perr := res.Get(ctx)
	if perr != nil {
		resumeKey(s.pub, out.Out.OrderingKey)
		release()
		return s.mapPublish(perr)
	}
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
// per FLUSH and not per message, which is the distinction rule 3 draws.
func (s *sink) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.pub.Flush()
	}()
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
		s.closed.Store(true)
		// Stop flushes and waits, which is what makes a publication admitted
		// before Close one that is still published: the specification's "Close completes
		// or fails what was admitted", and the reason the completion pool is
		// closed after every sink.
		s.pub.Stop()
		s.b.forget(nil, s)
	})
	return nil
}

// prepare encodes one message, refusing before any I/O.
func (s *sink) prepare(m *mq.Message, o *mq.PublishOptions) (encoded, error) {
	if m == nil {
		return encoded{}, errNilMessage
	}
	return s.enc.encode(m, o)
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

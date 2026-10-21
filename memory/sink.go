package memory

import (
	"context"
	"sync"
	"sync/atomic"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/wire"
)

// The sink implements the mandatory publication surface and the three
// optional ones the engine can honour: a native batch, a flush, an ownership
// transfer and an asynchronous borrow. It does NOT implement
// mq.AtomicBatchSink or the receipt surface, which the engine cannot offer —
// a driver that implemented the method without the guarantee would make the
// core skip its own adapter and report a promise nothing keeps.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
	_ mq.OwnedSink = (*sink)(nil)
	_ mq.AsyncSink = (*sink)(nil)
)

// sink is one open publisher, bound to one topic for its lifetime.
//
// It holds the engine topic directly rather than a name, which is the specification's
// "publisher handles hold *topic directly (no lookup on publish)": a
// publication costs no registry lookup.
type sink struct {
	b     *Broker
	topic *engine.Topic[*mq.Message]
	name  string
	meta  metadataMode
	// wildcards is the topic's subject routing, compiled into a field at
	// open so that a publication reads one boolean.
	wildcards bool
	// batchPool holds the publication slices a native batch is built in, so
	// that a repeated batch publish allocates nothing per batch in steady
	// state.
	batchPool sync.Pool

	closed atomic.Bool
}

// Publish admits one message.
//
// It is ADR-009's safe mode: the engine clones the message on admission and
// hands the caller's own storage straight back, so the Key, the Body and the
// header storage may all be reused the instant Publish returns. A
// caller that would rather transfer the storage uses PublishOwned, and one
// that would rather lend it uses PublishAsync.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	if m == nil {
		return errNilMessage
	}
	if o == nil {
		return errNilOptions
	}
	p, err := s.record(m, o, engine.CopySafe)
	if err != nil {
		return err
	}
	return s.topic.Publish(ctx, p)
}

// PublishBatch admits a batch through the engine's own batch call.
//
// The result is index-aligned with ms, which is the whole contract: the
// engine's batch stops at the first element it refuses and names its index,
// so the loop below records that element's outcome and offers the rest rather
// than reporting one message's refusal for the messages behind it. A batch
// that reported the wrong index is how a batch publish loses a message
// silently, which is what FuzzBatchResults exists for.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	if len(ms) == 0 {
		return nil
	}
	if o == nil {
		return errNilOptions
	}
	buf, _ := s.batchPool.Get().(*publishBatch)
	if buf == nil {
		buf = &publishBatch{}
	}
	ps := buf.ps[:0]
	defer func() {
		clear(ps)
		buf.ps = ps[:0]
		s.batchPool.Put(buf)
	}()
	var errs []error
	var first error
	for _, m := range ms {
		if m == nil {
			return errNilMessage
		}
		p, err := s.record(m, o, engine.CopySafe)
		if err != nil {
			// A message this driver cannot represent fails the batch
			// before anything is published, which is where the specification puts it.
			return err
		}
		ps = append(ps, p)
	}
	for lo := 0; lo < len(ps); {
		n, err := s.topic.PublishBatch(ctx, ps[lo:])
		if err == nil {
			break
		}
		idx := lo + n
		if first == nil {
			first = err
		}
		if errs == nil {
			errs = make([]error, len(ms))
		}
		if idx >= len(ps) {
			// Every element was offered and the batch still reports an
			// error: a partial fan-out, which the engine reports with the
			// whole length rather than against an element. It blames no
			// message, so it stays the batch's own error.
			break
		}
		errs[idx] = err
		if fatalPublish(err) {
			// Nothing after it can be offered either, and leaving those
			// results nil would report success for a message the engine was
			// never told about.
			for i := idx + 1; i < len(errs); i++ {
				errs[i] = err
			}
			break
		}
		lo = idx + 1
	}
	return batchOutcome(errs, first)
}

// batchOutcome turns the per-message results into the batch's one error.
//
// It is the batch answer every in-tree sink gives, driverkit.CollapseBatch:
// nil when every message was accepted, a single error when none was and every
// message has the same outcome, and a *BatchError naming which otherwise.
// The engine has no ambiguous publication outcome, so its
// messages always agree when none was accepted. A failure that blames no
// message — a partial fan-out, which the engine reports against the batch
// rather than against an element — stays the batch's own error, because a
// BatchError that blamed every message for it would tell the caller to retry
// records the engine has taken.
func batchOutcome(errs []error, first error) error {
	if first == nil {
		return nil
	}
	if errs == nil {
		return first
	}
	for _, err := range errs {
		if err != nil {
			return driverkit.CollapseBatch(errs)
		}
	}
	return first
}

// fatalPublish reports whether an error means the batch cannot continue: a
// closed engine or a finished context stops it, while a refused record is one
// record's outcome.
func fatalPublish(err error) bool {
	switch mq.CodeOf(err) {
	case mq.CodeClosed, mq.CodeCanceled, mq.CodeDeadlineExceeded:
		return true
	default:
		return false
	}
}

// PublishOwned takes ownership of om's message and releases it exactly once
//
// The engine releases what it admitted, and the adapter completes the
// contract for what it did not: a nil error and ErrPartial mean the engine
// took the record and will release it, and every other outcome means nothing
// was admitted and the payload is still the caller's — so this method
// releases it here, which is the half the specification asks for and the engine cannot
// give.
func (s *sink) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	release := om.Release
	if release == nil {
		release = func(*mq.Message) {}
	}
	if s.closed.Load() {
		release(om.Message)
		return mq.ErrClosed
	}
	if om.Message == nil {
		release(om.Message)
		return errNilMessage
	}
	if o == nil {
		release(om.Message)
		return errNilOptions
	}
	p, err := s.record(om.Message, o, engine.CopyOwned)
	if err != nil {
		release(om.Message)
		return err
	}
	p.Release = release
	err = s.topic.Publish(ctx, p)
	if err != nil && mq.CodeOf(err) != mq.CodePartial {
		// Ownership never left the caller, so nothing has released the
		// payload and this is the release the specification promises even on rejection.
		release(om.Message)
	}
	return err
}

// PublishAsync admits one message and reports its outcome to done.
//
// It is ADR-009's borrowed mode: the message stays the caller's and must stay
// valid and unmutated until done runs, which is the completion notification
// of the borrow. done runs exactly once when this method returns nil, and
// does not run at all when it returns an error — the contract the specification fixes.
//
// The engine admits synchronously, so the callback is not what tells the
// caller the record was accepted; it is what tells the caller the storage is
// free again, which for this broker is when the last subscription that was
// admitted has settled the record.
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return errNilCallback
	}
	if s.closed.Load() {
		return mq.ErrClosed
	}
	if m == nil {
		return errNilMessage
	}
	if o == nil {
		return errNilOptions
	}
	p, err := s.record(m, o, engine.CopyBorrowed)
	if err != nil {
		return err
	}
	p.Release = func(*mq.Message) { done(nil) }
	if err := s.topic.Publish(ctx, p); err != nil {
		if mq.CodeOf(err) != mq.CodePartial {
			// Nothing was admitted, so the engine will not call Release and
			// the callback must not run: a non-nil immediate error means
			// done will not run.
			return err
		}
		// A partial fan-out: the engine took the record and will release
		// it, so the callback WILL run — and the specification makes a non-nil return
		// mean it will not. The admission is reported, and the refusal the
		// other subscriptions made is visible in Snapshot.Rejected rather
		// than here.
	}
	return nil
}

// Flush completes the publications this sink has already admitted.
//
// The engine admits synchronously — Publish returns once the record is in
// every target lane — so there is never anything buffered to complete and
// Flush is a barrier that has already passed. It is implemented rather than
// omitted so that the boundary is stated here and on the driver page instead
// of being inferred from a missing method.
func (s *sink) Flush(ctx context.Context) error {
	if s.closed.Load() {
		return mq.ErrClosed
	}
	return ctx.Err()
}

// Close releases the sink. It is idempotent, and a Publish afterwards returns
// ErrClosed.
func (s *sink) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.b.forget(nil, s)
	return nil
}

// record maps a message and its resolved options onto one engine publication.
//
// It performs no allocation for a native-metadata publication: every field of
// the returned struct is a copy of a scalar or of a slice header the caller
// already holds. The enveloping mode is the exception and says so.
func (s *sink) record(m *mq.Message, o *mq.PublishOptions, mode engine.CopyMode) (engine.Publish[*mq.Message], error) {
	// The key is taken before enveloping: the envelope carries it in its
	// body, but the engine still partitions by it, and a keyless record
	// would leave its key's order to whichever partition it landed on. The
	// size stays the enveloped message's, whose body already holds the key.
	key := m.Key
	if s.meta == metaEnvelope {
		var err error
		m, err = envelope(s.b.envelope, m)
		if err != nil {
			return engine.Publish[*mq.Message]{}, err
		}
	}
	p := engine.Publish[*mq.Message]{
		Payload:   m,
		Key:       key,
		Size:      messageSize(m),
		Partition: -1,
		Delay:     o.Delay,
		DeliverAt: o.DeliverAt,
		TTL:       o.TTL,
		Mode:      mode,
	}
	if s.wildcards {
		// On a wildcard topic the publication needs a subject, and the SPI
		// gives a sink no field for one. Message.Topic is what a bound
		// publisher ignores for routing, so it is what carries the
		// subject here.
		p.Subject = m.Topic
	}
	if o.PartitionSet {
		p.Partition = int(o.Partition)
	}
	if o.PrioritySet {
		p.Priority = o.Priority
	}
	if o.DedupKey != "" {
		// The engine's dedup key is bytes and the API's is a string. A
		// conversion allocates, so it happens only for a publication that
		// named one — which is the publication that asked for the window.
		p.DedupKey = []byte(o.DedupKey)
	}
	return p, nil
}

// envelope wraps a message in the portable binary envelope, which is what
// MetadataPortableEnvelope asks for.
//
// The enveloped message carries the envelope as its whole body and nothing
// else, exactly as it would on a broker with no header support: a source
// opened in the same mode decodes it, and one opened in native mode sees the
// envelope bytes, which is the body-interoperability change the specification requires the
// mode to be explicit about.
func envelope(layout wire.Layout, m *mq.Message) (*mq.Message, error) {
	body, err := layout.Encode(nil, m)
	if err != nil {
		return nil, err
	}
	return &mq.Message{Topic: m.Topic, Body: body}, nil
}

// publishBatch holds one pooled publication slice, so that the pool stores a
// pointer and Put allocates no interface box.
type publishBatch struct {
	ps []engine.Publish[*mq.Message]
}

// The publication failures.
var (
	errNilMessage  = errcode.New(errcode.InvalidArgument, "iqmq/memory: message is nil")
	errNilOptions  = errcode.New(errcode.InvalidArgument, "iqmq/memory: publish options are nil; the core resolves them before calling a sink")
	errNilCallback = errcode.New(errcode.InvalidArgument, "iqmq/memory: PublishAsync was given no completion callback")
)

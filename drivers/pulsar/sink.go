package pulsar

import (
	"bytes"
	"context"
	"errors"
	"sync"
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
	mu       sync.Mutex
	closed   bool
	inflight sync.WaitGroup
}

// isClosed reports whether this sink has been closed.
func (s *sink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// admit reserves a place for one asynchronous publication, refusing once the
// sink is closed.
//
// The reservation and the closed check are one atomic step: see [sink.mu].
func (s *sink) admit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.inflight.Add(1)
	return true
}

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
		if !s.admit() {
			s.b.give(p)
			return mq.ErrClosed
		}
		// This call returns before the client has read the payload.
		detach(p, m)
		h.prod.SendAsync(ctx, &p.pm, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, serr error) {
			defer s.inflight.Done()
			if serr != nil {
				h.lost(serr)
				s.b.degrade(s.fail(serr))
			}
			s.b.give(p)
		})
		return nil
	}
	if abandonable(ctx) {
		// A Send that gives up on its context returns while the
		// request may still be queued, payload unread.
		detach(p, m)
	}
	id, err := h.prod.Send(ctx, &p.pm)
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
	if !s.admit() {
		s.b.give(p)
		return mq.ErrClosed
	}
	h.prod.SendAsync(ctx, &p.pm, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, serr error) {
		defer s.inflight.Done()
		if serr != nil {
			h.lost(serr)
			done(s.fail(serr))
			s.b.give(p)
			return
		}
		s.fillReceipt(m, id)
		s.b.give(p)
		done(nil)
	})
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
	if !s.admit() {
		s.b.give(p)
		om.Release(m)
		return mq.ErrClosed
	}
	h.prod.SendAsync(ctx, &p.pm, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, serr error) {
		defer s.inflight.Done()
		h.lost(serr)
		if serr == nil {
			s.fillReceipt(m, id)
		}
		s.b.give(p)
		// Never under an internal lock, and exactly once: this is the
		// only place the callback can reach.
		om.Release(m)
	})
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
		if !s.admit() {
			s.b.give(p)
			errs[i] = mq.ErrClosed
			continue
		}
		wg.Add(1)
		h.prod.SendAsync(ctx, &p.pm, func(id pulsar.MessageID, _ *pulsar.ProducerMessage, serr error) {
			defer s.inflight.Done()
			defer wg.Done()
			if serr != nil {
				h.lost(serr)
				errs[i] = s.fail(serr)
			} else {
				s.fillReceipt(m, id)
			}
			s.b.give(p)
		})
	}
	wg.Wait()
	return batchResult(errs)
}

// Flush completes the publications the producer's batching is still holding
func (s *sink) Flush(ctx context.Context) error {
	if s.isClosed() {
		return mq.ErrClosed
	}
	h, err := s.b.live(ctx, s.entry)
	if err != nil {
		return err
	}
	if err := h.prod.FlushWithCtx(ctx); err != nil {
		h.lost(err)
		return s.fail(err)
	}
	return nil
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
func (s *sink) Close() error { return s.close(true) }

// close is Close with a say in whether the producer reference is released,
// which the backend's own teardown has already done.
//
// It is idempotent through the closed flag rather than through a sync.Once,
// because the flag is what a publication has to read anyway: the first caller
// takes it under the mutex and every later one returns at once.
func (s *sink) close(release bool) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	// Outside the mutex, and after the flag: every Add fell before this
	// Wait, and no publication can be admitted from here on.
	s.inflight.Wait()
	if release {
		s.b.releaseProducer(s.name)
		s.b.forget(nil, s)
	}
	return nil
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

package kafka

import (
	"context"
	"errors"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// The sink implements the mandatory publication surface and all four optional
// ones.
//
// All four are real here, which is unusual and is why each is worth a line:
//
//   - [bitbucket.org/iqhive/iqmq.BatchSink] because a Kafka produce request
//     carries many records and franz-go batches them itself, so a batch of a
//     hundred is one round trip rather than a hundred promises;
//   - [bitbucket.org/iqhive/iqmq.AsyncSink] because franz-go's own produce IS
//     asynchronous — the promise is the callback — so the asynchronous surface
//     costs no goroutine per message, which the specification forbids;
//   - [bitbucket.org/iqhive/iqmq.OwnedSink] because the record's storage is
//     released the moment the promise runs, which is earlier than a
//     synchronous Publish can release it;
//   - [bitbucket.org/iqhive/iqmq.FlushSink] because a publication under
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

	once   sync.Once
	closed chan struct{}
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
// the record in its log. Under ConfirmNone it returns as soon as the record is
// in the producer's buffer, and the message's storage is copied first, because
// The specification lets the caller reuse its buffers the instant Publish returns and
// franz-go is still reading them.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	if s.confirm == mq.ConfirmNone {
		return s.publishBuffered(ctx, m, o)
	}
	buf := getRecbuf()
	defer putRecbuf(buf)
	if err := s.build(ctx, buf, m, o, false); err != nil {
		return err
	}
	err := s.produceOne(ctx, &buf.rec)
	if err != nil {
		return err
	}
	s.fillReceipt(m, &buf.rec)
	return nil
}

// publishBuffered is the ConfirmNone path: the record's storage is the
// driver's own copy, the record is handed to the producer, and the call
// returns without waiting.
//
// The copy is not avoidable and is the price of the mode. The specification says a
// synchronous Publish may not retain the caller's key, body or header storage
// after it returns, and franz-go holds the record until it has been written —
// so either the call waits (which is what the other modes do) or the bytes are
// copied. The copy is a pooled buffer, so the steady state allocates nothing.
func (s *sink) publishBuffered(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	buf := getRecbuf()
	if err := s.build(ctx, buf, m, o, true); err != nil {
		putRecbuf(buf)
		return err
	}
	// A receipt cannot be filled: nothing is known about the record yet.
	// The specification makes Receipts a publisher configuration, and a publisher
	// configured with both Receipts and ConfirmNone is asking for two
	// things that cannot both be true; the receipt slot is left as the
	// core zeroed it.
	s.b.cl.Produce(ctx, &buf.rec, func(_ *kgo.Record, err error) {
		if err != nil {
			// Nobody is waiting, so the specification makes this a background
			// failure: it goes to health, which is the only place a
			// caller can see it.
			s.b.health.Degraded(s.produceFailed(err))
		}
		putRecbuf(buf)
	})
	return nil
}

// PublishBatch admits a batch.
//
// Every record is handed to the producer and the call waits for all of their
// promises, so a batch of a hundred costs the round trips franz-go's own
// batching decides on rather than a hundred. The result is index-aligned with
// ms, which is the whole contract: a result aligned with anything else — the
// completion order, which is what franz-go's own ProduceSync returns — reports
// one message's outcome for another, and that is how a batch publish loses a
// message silently. FuzzKafkaBatchOutcome exists for it.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	for _, m := range ms {
		if err := s.check(ctx, m, o); err != nil {
			return err
		}
	}
	bufs := make([]*recbuf, 0, len(ms))
	defer func() {
		for _, b := range bufs {
			putRecbuf(b)
		}
	}()
	errs := make([]error, len(ms))
	var wg sync.WaitGroup
	failed := 0
	for i, m := range ms {
		buf := getRecbuf()
		bufs = append(bufs, buf)
		if err := s.build(ctx, buf, m, o, false); err != nil {
			errs[i] = err
			failed++
			continue
		}
		wg.Add(1)
		// The index is captured, not inferred from the completion order:
		// franz-go's promises fire in whatever order the partitions
		// resolve.
		s.b.cl.Produce(ctx, &buf.rec, func(_ *kgo.Record, err error) {
			errs[i] = err
			wg.Done()
		})
	}
	wg.Wait()
	for i := range errs {
		if errs[i] == nil {
			s.fillReceipt(ms[i], &bufs[i].rec)
			continue
		}
		if !isDriverRefusal(errs[i]) {
			errs[i] = s.produceFailed(errs[i])
			failed++
		}
	}
	_ = failed
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
	buf := getRecbuf()
	if err := s.build(ctx, buf, m, o, false); err != nil {
		putRecbuf(buf)
		return err
	}
	s.b.cl.Produce(ctx, &buf.rec, func(rec *kgo.Record, err error) {
		if err == nil {
			s.fillReceipt(m, rec)
			done(nil)
		} else {
			done(s.produceFailed(err))
		}
		putRecbuf(buf)
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
// holding a lock.
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
	buf := getRecbuf()
	if err := s.build(ctx, buf, om.Message, o, false); err != nil {
		putRecbuf(buf)
		release()
		return err
	}
	s.b.cl.Produce(ctx, &buf.rec, func(rec *kgo.Record, err error) {
		if err == nil {
			s.fillReceipt(om.Message, rec)
		} else {
			s.b.health.Degraded(s.produceFailed(err))
		}
		putRecbuf(buf)
		release()
	})
	return nil
}

// Flush completes the publications already admitted, bounded by ctx.
func (s *sink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	if err := s.b.cl.Flush(ctx); err != nil {
		return s.produceFailed(err)
	}
	return nil
}

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It does not flush: the specification gives Close no context, and a flush is a round trip
// a caller who wants it asks for with Shutdown. What it does not do is touch
// the client — many sinks share one.
func (s *sink) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.b.forget(nil, s)
	})
	return nil
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
	if s.isClosed() {
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
		// The envelope is freshly allocated storage nobody else holds, so
		// it never needs a second copy.
		own = false
	}
	buf.sbuf = Encode(&buf.rec, wrapped, s.name, buf.sbuf)
	if wrapped != m {
		// The record's value is the envelope, and the marker is where that
		// is recorded: a consumer must never have to read the value to find
		// out.
		MarkEnveloped(&buf.rec)
	}
	if o != nil && o.PartitionSet {
		RouteToPartition(ctx, &buf.rec, o.Partition)
	}
	if own {
		buf.retain()
	}
	return nil
}

// produceOne hands a record to the producer and waits for its promise.
//
// This is where the ambiguity lives. `Produce` returning without an
// immediate error means the record is IN THE BUFFER, and from that moment
// every failure — the context being cancelled, the deadline passing, franz-go
// giving up, the broker answering NOT_ENOUGH_REPLICAS_AFTER_APPEND — may be a
// failure to hear the answer rather than a failure to write the record. So the
// promise's error is always classified as post-send.
func (s *sink) produceOne(ctx context.Context, rec *kgo.Record) error {
	var (
		wg  sync.WaitGroup
		err error
	)
	wg.Add(1)
	s.b.cl.Produce(ctx, rec, func(_ *kgo.Record, perr error) {
		err = perr
		wg.Done()
	})
	wg.Wait()
	if err != nil {
		return s.produceFailed(err)
	}
	return nil
}

// produceFailed maps a produce failure onto the model, and is the one
// place this driver decides that an outcome is unknown rather than bad.
//
// Every failure this reaches is a POST-BUFFER one, which is why there is no
// flag to pass: once franz-go has accepted the record it may write it, and a
// context cancellation or a deadline after that point is not a failed
// publication — it is a publication whose outcome nobody knows. A failure
// BEFORE the buffer never gets here, because `check` and `build` refuse those
// and return their own error.
//
// The code is rewritten to CodeSettlementUnknown for every ambiguous outcome,
// so that `errors.Is(err, mq.ErrSettlementUnknown)` answers yes as well as
// [bitbucket.org/iqhive/iqmq.IsAmbiguous]. The specification names the sentinel for exactly
// this condition, and a caller that has to call IsAmbiguous to find out is a
// caller who will not. The native error is preserved
// underneath, so `errors.Is(err, context.Canceled)` still answers yes too.
func (s *sink) produceFailed(err error) error {
	mapped := s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, true)
	var oe *mq.OpError
	if !errors.As(mapped, &oe) || !oe.Ambiguous {
		return mapped
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

// isDriverRefusal reports whether err is one this driver produced itself,
// rather than one the client library or the broker produced, so that a batch
// does not map an already-mapped refusal a second time.
func isDriverRefusal(err error) bool {
	var oe *mq.OpError
	return errors.As(err, &oe) || mq.CodeOf(err) != mq.CodeUnknown
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
}

// emptyBytes is the non-nil empty slice, which is what keeps the specification's
// distinction between a nil body and an empty one through a copy.
var emptyBytes = []byte{}

// retain copies everything the record points at into the buffer's own storage,
// so the record may outlive the call that built it.
func (b *recbuf) retain() {
	b.rec.Key = copyInto(&b.key, b.rec.Key)
	b.rec.Value = copyInto(&b.val, b.rec.Value)
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

// copyInto copies src into dst's storage, preserving the difference between a
// nil slice and an empty one.
func copyInto(dst *[]byte, src []byte) []byte {
	switch {
	case src == nil:
		return nil
	case len(src) == 0:
		return emptyBytes
	default:
		*dst = append((*dst)[:0], src...)
		return *dst
	}
}

// recbufPool holds the record buffers. The specification's ownership rules are what make
// pooling safe here: a buffer goes back only after the promise has run, which
// is the moment franz-go stops reading the record.
var recbufPool = sync.Pool{New: func() any { return &recbuf{} }}

func getRecbuf() *recbuf {
	b, _ := recbufPool.Get().(*recbuf)
	return b
}

func putRecbuf(b *recbuf) {
	if b == nil {
		return
	}
	b.rec = kgo.Record{Headers: b.rec.Headers[:0]}
	recbufPool.Put(b)
}

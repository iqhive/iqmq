package kafkago

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The sink implements the mandatory publication surface and all four optional
// ones.
//
// All four are real here, and each of them costs something particular on this
// client, which is why each is worth a line:
//
//   - [github.com/iqhive/iqmq.BatchSink] because a Kafka produce request
//     carries many records and kafka-go's writer batches per partition, so a
//     batch of a hundred is one round trip per partition rather than a hundred
//     — but only when [Config.BatchMaxRecords] is raised, because the writer's
//     batch size is what decides how many records one request carries and the
//     default is one;
//   - [github.com/iqhive/iqmq.AsyncSink] because kafka-go's writer will
//     produce without waiting when Async is set, so the asynchronous surface
//     costs no goroutine per message, which the specification forbids — at the price of a
//     SECOND writer, since Async is a field of the writer ;
//   - [github.com/iqhive/iqmq.OwnedSink] because the message's storage is
//     released the moment the completion runs, which is earlier than a
//     synchronous Publish can release it;
//   - [github.com/iqhive/iqmq.FlushSink] because a publication under
//     ConfirmNone is in a batch, and a caller has to be able to wait for it.
//     kafka-go has no Flush — Close is its only flush, and Close is terminal —
//     so the driver counts what it has admitted.
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

// completion is the writer-level callback both producers share, and it is the
// only route from a produce response back to the caller: kafka-go's
// WriteMessages returns an error and nothing else, so a partition, an offset
// and an asynchronous outcome can only arrive here.
//
// The messages it is handed are the writer's own copies of the values the
// driver submitted, with Topic, Partition, Offset and Time filled in from the
// produce response — and with WriterData intact, which is what makes them
// attributable. All of them belong to one partition and one produce, so err is
// the whole batch's.
//
// It runs on a writer goroutine, and on the synchronous path it runs BEFORE
// WriteMessages returns (kafka-go calls Completion and then closes the batch's
// done channel), which is what makes a receipt filled here visible to a caller
// whose Publish has returned.
func (b *Backend) completion(msgs []kgo.Message, err error) {
	for i := range msgs {
		r, ok := msgs[i].WriterData.(*route)
		if !ok {
			continue
		}
		if err == nil && r.msg != nil && r.msg.Receipt != nil {
			// A Kafka partition is an int32 on the wire.
			r.msg.Receipt.Partition = int32(msgs[i].Partition)
			r.msg.Receipt.Offset = msgs[i].Offset
			r.msg.Receipt.Confirmation = r.confirm
			r.msg.Receipt.ConfirmedAt = msgs[i].Time
			// Kafka assigns no message identifier: the coordinates ARE the
			// identifier, and the specification makes an empty MessageID "the broker
			// assigns none" rather than a claim.
		}
		// The order matters: settle may hand the buffer's contents to a
		// caller's callback, and release may put the buffer back in the pool
		// for another publication to overwrite.
		buf := r.buf
		_ = r.settle(err)
		if buf != nil {
			buf.release()
		}
	}
}

// Publish admits one message.
//
// The order is fixed and it is the specification: the per-call controls Kafka cannot
// honour are refused before anything is written, then the size limit is
// checked, then the metadata is mapped, and only then does the message go to
// the writer.
//
// Under ConfirmAccepted and ConfirmDurable — the same boundary here — the call
// waits for the produce response, which arrives when every in-sync replica has
// the record in its log. Under ConfirmNone it returns as soon as the record is
// in the writer's batch, and the message's storage is copied first, because
// The specification lets the caller reuse its buffers the instant Publish returns and
// kafka-go is still reading them.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	if s.confirm == mq.ConfirmNone {
		return s.publishBuffered(ctx, m, o)
	}
	buf := getMsgbuf()
	if err := s.build(buf, m, o, false); err != nil {
		buf.discard()
		return err
	}
	buf.route.msg, buf.route.confirm = s.receiptTarget(m), s.confirm
	// The buffer is REFERENCE-COUNTED from here, and this goroutine drops
	// only its own reference. A synchronous WriteMessages that returns the
	// caller's ctx.Err() has NOT cancelled the produce — kafka-go's own
	// documentation says the messages "might also still be written after this
	// method has already returned" — so the writer may still be reading this
	// record when Publish returns. Returning the buffer to the pool here, as
	// the first draft's `defer putMsgbuf` did, hands a live record's storage
	// to the next publication.
	defer buf.release()
	return s.write(ctx, s.b.w, &buf.msg)
}

// publishBuffered is the ConfirmNone path: the message's storage is the
// driver's own copy, the record goes to the ASYNCHRONOUS writer, and the call
// returns without waiting.
//
// The copy is not avoidable and is the price of the mode. The specification says a
// synchronous Publish may not retain the caller's key, body or header storage
// after it returns, and kafka-go holds the value until it has been written —
// so either the call waits (which is what the other modes do) or the bytes are
// copied. The buffers are pooled, so the steady state allocates nothing.
//
// A receipt cannot be filled: nothing is known about the record yet. The specification
// makes Receipts a publisher configuration, and a publisher configured with
// both Receipts and ConfirmNone is asking for two things that cannot both be
// true; the receipt slot is left as the core zeroed it.
func (s *sink) publishBuffered(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	buf := getMsgbuf()
	if err := s.build(buf, m, o, true); err != nil {
		buf.discard()
		return err
	}
	defer buf.release()
	s.b.pending.add()
	buf.route.report = func(err error) {
		if err != nil {
			// Nobody is waiting, so the specification makes this a background failure: it
			// goes to health, which is the only place a caller can see it.
			s.b.health.Degraded(s.produceFailed(err))
		}
		s.b.pending.done()
	}
	if err := s.b.wa.WriteMessages(ctx, buf.msg); err != nil {
		// The writer answered before the completion could. `settle` is what
		// makes the accounting happen once whichever of the two got here
		// first, because an error from WriteMessages does not say whether the
		// record was submitted.
		_ = buf.route.settle(err)
		return s.produceFailed(err)
	}
	return nil
}

// PublishBatch admits a batch.
//
// Every message is handed to the writer in one call, so kafka-go groups them
// per partition and produces one request per partition per batch-size worth of
// records. The result is index-aligned with ms, which is the whole contract: a
// result aligned with anything else reports one message's outcome for another,
// and that is how a batch publish loses a message silently.
//
// kafka-go's own [kgo.WriteErrors] is already index-aligned with the slice it
// was given — every message of a failed batch carries that batch's error — so
// the alignment is the client's here rather than the driver's. What the driver
// must not do is collapse it: FuzzKafkaGoBatchOutcome scrambles the per-message
// outcomes and checks that [BatchOutcome] and [BatchSummary] keep them where
// they belong.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	for _, m := range ms {
		if err := s.check(ctx, m, o); err != nil {
			return err
		}
	}
	bufs := make([]*msgbuf, 0, len(ms))
	defer func() {
		for _, b := range bufs {
			b.release()
		}
	}()
	errs := make([]error, len(ms))
	msgs := make([]kgo.Message, 0, len(ms))
	// A message the driver itself refuses is reported at its own index and
	// left out of the write, and the mapping back is what `at` records: the
	// writer's own errors are aligned with what it was GIVEN, not with what
	// the caller passed.
	at := make([]int, 0, len(ms))
	for i, m := range ms {
		buf := getMsgbuf()
		bufs = append(bufs, buf)
		if err := s.build(buf, m, o, false); err != nil {
			errs[i] = err
			continue
		}
		buf.route.msg, buf.route.confirm = s.receiptTarget(m), s.confirm
		msgs = append(msgs, buf.msg)
		at = append(at, i)
	}
	if len(msgs) > 0 {
		if err := s.b.w.WriteMessages(ctx, msgs...); err != nil {
			var werr kgo.WriteErrors
			if errors.As(err, &werr) && len(werr) == len(msgs) {
				for j, e := range werr {
					if e != nil {
						errs[at[j]] = s.produceFailed(e)
					}
				}
			} else {
				// Not a per-message answer: the whole call failed, and every
				// message it carried shares the outcome. An oversized record
				// is the live case — kafka-go refuses the CALL rather than
				// the record.
				mapped := s.produceFailed(err)
				for _, i := range at {
					errs[i] = mapped
				}
			}
		}
	}
	return BatchSummary(errs)
}

// BatchOutcome turns a produce call's per-message answers into the
// index-aligned error slice the specification requires, mapping each one through.
//
// It is exported and pure because it is the one piece of batch arithmetic that
// is easy to get wrong in a way no ordinary test notices. kafka-go hands back
// a [kgo.WriteErrors] whose entries line up with the slice it was given, and
// the temptation is to collapse it to "the batch failed": a caller reading an
// index-aligned result would then retry every message and duplicate the ones
// that succeeded — and on THIS client a duplicate is not de-duplicated by the
// broker, because there is no producer id.
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
		out[i] = mapSent(m, mq.OpPublish, "", "", err)
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
// m and o are borrowed until done runs, which is exactly kafka-go's own
// contract for a message and its completion, so this path copies nothing and
// starts no goroutine: the completion IS the callback. The rules hold as
// written — a non-nil immediate error means done will not run, a nil return
// means done runs exactly once.
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return errNilCallback
	}
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	buf := getMsgbuf()
	if err := s.build(buf, m, o, false); err != nil {
		buf.discard()
		return err
	}
	buf.route.msg, buf.route.confirm = s.receiptTarget(m), s.confirm
	s.b.pending.add()
	buf.route.report = func(err error) {
		if err != nil {
			err = s.produceFailed(err)
		}
		s.b.pending.done()
		done(err)
	}
	defer buf.release()
	if err := s.b.wa.WriteMessages(ctx, buf.msg); err != nil {
		// Either this goroutine settles the publication or the completion
		// already did, and in BOTH cases the callback has been given an
		// outcome — so the specification requires a nil return, because a non-nil one
		// means the callback will not run. `settle` says which of the two it
		// was and needs no reasoning about which of kafka-go's error returns
		// mean "the record was never submitted".
		buf.route.settle(err)
		return nil
	}
	return nil
}

// PublishOwned takes ownership of om's message at invocation and releases it
// exactly once.
//
// The release happens in the completion, which is the earliest moment it can:
// the record points at the message's own key, body and header storage, so
// nothing is copied and the storage goes back to whoever owned it as soon as
// kafka-go has written it. Every path releases — a refusal before the write, a
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
	buf := getMsgbuf()
	if err := s.build(buf, om.Message, o, false); err != nil {
		buf.discard()
		release()
		return err
	}
	buf.route.msg, buf.route.confirm = s.receiptTarget(om.Message), s.confirm
	s.b.pending.add()
	buf.route.report = func(err error) {
		if err != nil {
			s.b.health.Degraded(s.produceFailed(err))
		}
		s.b.pending.done()
		release()
	}
	defer buf.release()
	if err := s.b.wa.WriteMessages(ctx, buf.msg); err != nil {
		// settle releases the message exactly once, whichever of the two
		// goroutines reaches it first.
		_ = buf.route.settle(err)
		return s.produceFailed(err)
	}
	return nil
}

// Flush completes the publications already admitted, bounded by ctx.
//
// It waits on the driver's own count of admitted-but-uncompleted records
// rather than on the client, because [kgo.Writer] has no Flush: its only
// flush is Close, which is terminal, and a Flush that closed the writer would
// end every other sink on the backend. The count is exact
// — the writer's completion decrements it — so this returns when the last
// admitted record has its reply and not on a timer.
func (s *sink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	if err := s.b.pending.wait(ctx); err != nil {
		return err
	}
	return nil
}

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It does not flush: the specification gives Close no context, and a flush is a round trip
// a caller who wants it asks for with Shutdown. What it does not do is touch
// the writers — many sinks share them.
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

// receiptTarget is the message whose receipt slot the completion fills, or nil
// when this publisher was not configured for receipts.
func (s *sink) receiptTarget(m *mq.Message) *mq.Message {
	if !s.receipts || m == nil || m.Receipt == nil {
		return nil
	}
	return m
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

// build fills buf's message with what this sink publishes, enveloping the
// message when the metadata mode says to and copying its storage when own says
// the record has to outlive the call.
func (s *sink) build(buf *msgbuf, m *mq.Message, o *mq.PublishOptions, own bool) error {
	wrapped := m
	if s.meta == metaEnvelope {
		var err error
		if wrapped, err = s.b.env.Wrap(m); err != nil {
			return err
		}
		// The envelope is freshly allocated storage nobody else holds, so it
		// never needs a second copy.
		own = false
	}
	buf.scratch = Encode(&buf.msg, wrapped, s.name, buf.scratch)
	if wrapped != m {
		// The record's value is the envelope, and the marker is where that
		// is recorded: a consumer must never have to read the value to find
		// out.
		MarkEnveloped(&buf.msg)
	}
	// The route is both halves of the per-message channel: the balancer reads
	// the partition out of it on the way out, and the writer's completion
	// reads the receipt slot and drops its reference to the buffer out of it
	// on the way in. Both are set HERE, in the one function every publication
	// path calls, and not at each call site — the first draft set the buffer
	// reference at four of the five sites and left `Publish`, which does not
	// corrupt anything (the missing reference means the buffer is collected
	// rather than pooled) and quietly costs an allocation per synchronous
	// publication.
	buf.msg.WriterData = &buf.route
	buf.route.buf = buf
	if o != nil && o.PartitionSet {
		Route(&buf.msg, o.Partition)
	}
	if own {
		buf.retain()
	}
	return nil
}

// write hands one message to a writer and reports the outcome.
//
// This is where the ambiguity lives. Kafka-go's WriteMessages returns
// ctx.Err() as soon as the caller's context is done and lets the produce
// carry on — its own documentation says "there are no guarantees made on
// whether messages were written to kafka, they might also still be written
// after this method has already returned" — so every failure this reaches is
// classified as post-send.
func (s *sink) write(ctx context.Context, w *kgo.Writer, msg *kgo.Message) error {
	if err := w.WriteMessages(ctx, *msg); err != nil {
		var werr kgo.WriteErrors
		if errors.As(err, &werr) && len(werr) == 1 && werr[0] != nil {
			err = werr[0]
		}
		return s.produceFailed(err)
	}
	return nil
}

// produceFailed maps a produce failure onto the model, and is the one
// place this driver decides that an outcome is unknown rather than bad.
//
// Every failure this reaches is a POST-SUBMISSION one, which is why there is
// no flag to pass: once the writer has accepted the message it may write it,
// and a context cancellation or a deadline after that point is not a failed
// publication — it is a publication whose outcome nobody knows. A failure
// BEFORE the writer never gets here, because `check` and `build` refuse those
// and return their own error.
//
// The code is rewritten to CodeSettlementUnknown for every ambiguous outcome,
// so that `errors.Is(err, mq.ErrSettlementUnknown)` answers yes as well as
// [github.com/iqhive/iqmq.IsAmbiguous]. The specification names the sentinel for exactly
// this condition, and a caller that has to call IsAmbiguous to find out is a
// caller who will not (the franz-go driver's). The native
// error is preserved underneath, so `errors.Is(err, context.Canceled)` still
// answers yes too.
func (s *sink) produceFailed(err error) error {
	mapped := mapSent(s.b.errs, mq.OpPublish, s.topic, "", err)
	var oe *mq.OpError
	if !errors.As(mapped, &oe) || !oe.Ambiguous {
		return mapped
	}
	s.b.ambiguous.Add(1)
	// An ambiguous produce on this client may ALSO have been duplicated by
	// the writer's own retries, because there is no producer id to
	// de-duplicate them. The counter is what a deployment
	// reads to know that.
	s.b.duplicable.Add(1)
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

// firstNonNil is gone. It existed only to answer a wholly failed batch with
// its first error, which is the rule driverkit.CollapseBatch replaced on the
// publish side and on the settlement side. Its absence is deliberate: a call site that reverted to the old
// rule would no longer compile.

// msgbuf is a pooled message, its route and the storage a buffered
// publication copies into.
//
// It exists and for the allocation budget at the same time. A
// publication that returns before its record has been written has to own the
// bytes, and a driver that allocated them per message would allocate three
// times per publication under ConfirmNone. The buffers are reused, so the
// steady state allocates nothing.
type msgbuf struct {
	// refs is how many goroutines still need the buffer's storage: the one
	// that submitted the publication and the writer's completion. The buffer
	// goes back to the pool when the LAST of them drops its reference, which
	// is the only rule that is safe on this client — an error from
	// WriteMessages does not say whether the record was submitted, so neither
	// goroutine can know whether the other will run.
	refs  atomic.Int32
	msg   kgo.Message
	route route
	// scratch holds the reserved header values Encode writes, so that the
	// identifier and the typed properties cost no conversion per field.
	scratch []byte
	key     []byte
	val     []byte
	hbuf    []byte
}

// emptyBytes is the non-nil empty slice, which is what keeps the specification's
// distinction between a nil body and an empty one through a copy.
var emptyBytes = []byte{}

// retain copies everything the message points at into the buffer's own
// storage, so the record may outlive the call that built it.
func (b *msgbuf) retain() {
	b.msg.Key = copyInto(&b.key, b.msg.Key)
	b.msg.Value = copyInto(&b.val, b.msg.Value)
	b.hbuf = b.hbuf[:0]
	for i := range b.msg.Headers {
		v := b.msg.Headers[i].Value
		if v == nil {
			continue
		}
		start := len(b.hbuf)
		b.hbuf = append(b.hbuf, v...)
		b.msg.Headers[i].Value = b.hbuf[start:len(b.hbuf):len(b.hbuf)]
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

// msgbufPool holds the message buffers. The specification's ownership rules are what make
// pooling safe here: a buffer goes back only after every holder has dropped
// its reference, and the writer's completion is the moment kafka-go stops
// reading the message.
var msgbufPool = sync.Pool{New: func() any { return &msgbuf{} }}

// getMsgbuf takes a buffer with TWO references: the submitting goroutine's and
// the writer completion's.
func getMsgbuf() *msgbuf {
	b, _ := msgbufPool.Get().(*msgbuf)
	b.refs.Store(2)
	return b
}

// release drops one reference and pools the buffer when the last one goes.
//
// A buffer whose completion never runs — a publication the writer refused
// before it took the record — keeps one reference for ever and is collected
// rather than pooled. That is a missed reuse on an error path and never a
// reuse of live storage, which is the way round this has to fail.
func (b *msgbuf) release() {
	if b == nil || b.refs.Add(-1) != 0 {
		return
	}
	b.reset()
	msgbufPool.Put(b)
}

// discard pools a buffer the writer has NOT been given, which is every
// refusal `check` or `build` makes: nothing else can hold it, so both
// references go at once.
func (b *msgbuf) discard() {
	if b == nil {
		return
	}
	b.refs.Store(0)
	b.reset()
	msgbufPool.Put(b)
}

// reset clears everything of one publication, keeping only the grown storage.
func (b *msgbuf) reset() {
	headers := b.msg.Headers[:0]
	b.msg = kgo.Message{Headers: headers}
	b.route.reset()
}

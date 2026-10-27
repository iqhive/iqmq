package jetstream

import (
	"context"
	"strconv"
	"sync"
	"time"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The sink implements the mandatory publication surface, the optional native
// batch, the optional asynchronous surface and the optional flush.
//
// It does NOT implement [github.com/iqhive/iqmq.OwnedSink], and that is a
// decision rather than an omission. Every asynchronous path on this broker hands
// the message to the client library, which holds it — payload included — until
// its PubAck arrives, so there is nothing to release earlier than the PubAck
// itself; a sink that released at invocation would hand the caller's buffer back
// while the library was still writing it.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
	_ mq.AsyncSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
)

// sink is one open publisher, bound to one subject for its lifetime (ADR-021).
type sink struct {
	b       *Backend
	topic   string
	subject string
	meta    metadataMode
	confirm mq.ConfirmationMode
	receipt bool

	once   sync.Once
	closed chan struct{}
}

// Publish admits one message.
//
// The order is fixed and it is the specification: the per-call controls this stream cannot
// honour are refused before anything is written, then the metadata is mapped,
// then the message goes out — and under every confirmation but ConfirmNone the
// call waits for the stream's PubAck, which is the durable boundary.
//
// Nothing of m is retained. The mapping copies every header key and value into
// the nats.Header as a string; the payload is aliased for the synchronous path,
// which is finished with it before it returns, and COPIED for every asynchronous
// path, because the client library holds the message until its PubAck.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	var msg natsgo.Msg
	if err := s.build(m, &msg, o); err != nil {
		return err
	}
	if s.confirm == mq.ConfirmNone {
		// Fire and forget, with core NATS: the subject is bound to the
		// stream, so the server stores the message, and asking for no reply
		// is what makes the publication cost no round trip. Nothing is
		// retained — PublishMsg serialises into the write buffer — and
		// nothing is reported, which is what ConfirmNone means.
		if err := s.b.nc.PublishMsg(&msg); err != nil {
			return s.fail(err, false)
		}
		return nil
	}
	pctx, cancel := s.b.bound(ctx)
	defer cancel()
	ack, err := s.b.js.PublishMsg(pctx, &msg)
	if err != nil {
		// The request had left the socket, so the stream may hold the
		// message: the specification makes the outcome ambiguous and a retry may
		// duplicate — which is what DedupKey exists for.
		return s.fail(err, true)
	}
	s.fillReceipt(m, ack)
	return nil
}

// PublishAsync admits one message and reports its outcome to done.
//
// It costs no goroutine per message, which the specification forbids outright: the JetStream
// client reports every asynchronous publication through two connection-wide
// callbacks that carry the *nats.Msg the publication was made with, so the
// backend keeps a map from that pointer to the publication and resolves each one
// from the client's own reply goroutine.
//
// The bound is [Config.PublishAsyncMaxPending]: once that many publications are
// outstanding the client stalls and then refuses with CodeBackpressure, which is the answer and not a buffer that grows.
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return errNilCallback
	}
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	msg := &natsgo.Msg{}
	if err := s.build(m, msg, o); err != nil {
		return err
	}
	// The client library holds this message until its PubAck, so the payload
	// cannot stay the caller's.
	msg.Data = s.copyBody(m.Body)
	pub := &asyncPub{sink: s, msg: m, done: done}
	s.b.trackAsync(msg, pub)
	if _, err := s.b.js.PublishMsgAsync(msg); err != nil {
		s.b.untrackAsync(msg)
		// A non-nil immediate error means done does not run.
		return s.fail(err, false)
	}
	return nil
}

// PublishBatch admits a batch.
//
// The saving is real: every message is published asynchronously, so the batch
// costs one round trip's latency rather than one per message, and every PubAck is
// awaited, so each message is as durable as a single publication. It is NOT
// atomic — JetStream's atomic batch is a stream flag this driver does not set —
// which is what the capability note says and why the claim is an emulation.
//
// The result is index-aligned with ms, which is the whole contract: a result
// aligned with anything else reports one message's outcome for another.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	for _, m := range ms {
		if err := s.check(ctx, m, o); err != nil {
			return err
		}
	}
	pctx, cancel := s.b.bound(ctx)
	defer cancel()
	errs := make([]error, len(ms))
	futures := make([]js.PubAckFuture, len(ms))
	failed := 0
	for i, m := range ms {
		msg := &natsgo.Msg{}
		if err := s.build(m, msg, o); err != nil {
			errs[i], failed = err, failed+1
			continue
		}
		msg.Data = s.copyBody(m.Body)
		fut, err := s.b.js.PublishMsgAsync(msg)
		if err != nil {
			errs[i], failed = s.fail(err, false), failed+1
			continue
		}
		futures[i] = fut
	}
	for i, fut := range futures {
		if fut == nil {
			continue
		}
		ack, err := awaitAck(pctx, fut)
		if err != nil {
			errs[i], failed = s.fail(err, true), failed+1
			continue
		}
		s.fillReceipt(ms[i], ack)
	}
	if failed == 0 {
		return nil
	}
	return collapse(errs)
}

// collapse is the batch answer: the single error only when every message
// failed with the same outcome, and the index-aligned *BatchError
// otherwise. It is driverkit.CollapseBatch; the first error used to stand for
// a batch every message of which failed, so a definite refusal next to an
// ambiguous failure read as a refusal.
func collapse(errs []error) error { return driverkit.CollapseBatch(errs) }

// awaitAck waits for one asynchronous publication's outcome.
//
// A context that is done while the publication is outstanding is NOT a failed
// publication: the message had already left, so the outcome is unknown and the
// caller is told so rather than being told the publication failed.
func awaitAck(ctx context.Context, fut js.PubAckFuture) (*js.PubAck, error) {
	select {
	case ack := <-fut.Ok():
		return ack, nil
	case err := <-fut.Err():
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Flush completes the publications already admitted, bounded by ctx.
//
// It waits for every outstanding asynchronous PubAck of the whole client, which
// is the only boundary the client library offers: the pending window is per
// JetStream context and not per sink.
func (s *sink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	fctx, cancel := s.b.bound(ctx)
	defer cancel()
	select {
	case <-s.b.js.PublishAsyncComplete():
		return nil
	case <-fctx.Done():
		return fctx.Err()
	}
}

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It resolves every asynchronous publication this sink still has outstanding
// with CodeSettlementUnknown, because that is the truth: the message had left
// and its PubAck had not arrived, so nobody knows whether the stream holds it.
// The specification requires the callback to run exactly once, close included, and a sink
// that simply dropped them would leave a caller's pooled message unreleased for
// ever.
func (s *sink) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.b.resolveSinkAsync(s)
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

// check is "enforced before I/O": the controls this stream has no way to
// honour are refused here, with ErrUnsupported, before a byte is written.
//
// The two version-gated controls are the point. A per-message TTL needs a stream
// created with AllowMsgTTL and a schedule needs one created with
// AllowMsgSchedules, and this driver knows which the bound stream has because it
// read the stream back at Open. A driver that ignored them instead would publish
// a message the caller asked to expire, and the caller would have no way to find
// out.
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
		scheduled := o.Delay > 0 || !o.DeliverAt.IsZero()
		switch {
		case o.Delay > 0 && !o.DeliverAt.IsZero():
			return errNoDelayAndDeliverAt
		case scheduled && !s.b.facts.schedulesUsable():
			return errNoSchedule
		case o.TTL > 0 && !s.b.facts.ttlUsable():
			return errNoTTL
		case o.PrioritySet:
			return errNoPriority
		case o.PartitionSet:
			return errNoPartition
		}
		if scheduled {
			if _, err := s.scheduleAt(o); err != nil {
				return err
			}
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

// scheduleAt is the absolute time a scheduled publication fires.
//
// Delay and DeliverAt are the two spellings of one control, and this is where
// they become one value. A time already past is refused rather than fired
// immediately: a caller that asked for a delay and got none has been told
// something untrue about when its message becomes visible.
func (s *sink) scheduleAt(o *mq.PublishOptions) (time.Time, error) {
	at := o.DeliverAt
	if o.Delay > 0 {
		at = s.b.now().Add(o.Delay)
	}
	if !at.After(s.b.now()) {
		return time.Time{}, errScheduleInPast
	}
	return at, nil
}

// build fills msg with what this sink publishes: the mapped message, the
// per-call controls that travel as JetStream headers, and — for a scheduled
// publication — the schedule subject and target.
//
// msg is the caller storage on the synchronous path, so a publication allocates
// nothing but the header map, and only when the message has metadata to put in
// one.
func (s *sink) build(m *mq.Message, msg *natsgo.Msg, o *mq.PublishOptions) error {
	wrapped := m
	if s.meta == metaEnvelope || (s.meta == metaAdaptive && Representable(m) != nil) {
		var err error
		if wrapped, err = s.b.env.Wrap(m); err != nil {
			return err
		}
	}
	msg.Subject = s.subject
	var drop func(string)
	if s.meta == metaDrop {
		// The specification lenient mode: what cannot be carried is left out. The hook
		// and the metric it asks for are the caller's, and only the metric
		// exists — the SPI gives a sink no hooks (see the maintenance notes).
		drop = s.b.dropped
	}
	Encode(msg, wrapped, drop)
	if wrapped != m {
		// The payload is the envelope, and the marker is where that is
		// recorded: a consumer must never have to read the payload to find
		// out.
		MarkEnveloped(msg)
	}
	return s.controls(msg, o)
}

// controls writes the per-call publication controls as the JetStream headers the
// server reads them from.
//
// They are written here rather than through the client library's PublishOpt
// functions so that all three publication paths carry them identically: a
// ConfirmNone publication is a core publish and never sees a PublishOpt, and a
// caller who asked for de-duplication has no way to tell which path its message
// took.
func (s *sink) controls(msg *natsgo.Msg, o *mq.PublishOptions) error {
	if o == nil {
		return nil
	}
	if o.DedupKey == "" && o.TTL <= 0 && o.Delay <= 0 && o.DeliverAt.IsZero() {
		return nil
	}
	if msg.Header == nil {
		msg.Header = make(natsgo.Header, 4)
	}
	if o.DedupKey != "" {
		if !HeaderValueOK([]byte(o.DedupKey)) {
			return errDedupKey
		}
		msg.Header.Set(js.MsgIDHeader, o.DedupKey)
	}
	if o.TTL > 0 {
		msg.Header.Set(js.MsgTTLHeader, o.TTL.String())
	}
	if o.Delay > 0 || !o.DeliverAt.IsZero() {
		at, err := s.scheduleAt(o)
		if err != nil {
			return err
		}
		// A schedule is a CONTROL message on a subject of its own, and the
		// message the application published is what the server generates on
		// the target subject when the schedule fires. Writing it to the
		// topic's own subject would deliver it immediately and store a
		// schedule nobody asked for.
		msg.Subject = ScheduleSubject(s.b.schedRoot, s.b.schedSeq.Add(1))
		// The fraction of a second is written, which RFC 3339 allows and
		// the server parses: whole seconds truncated the time, so the
		// message fired up to a second early and a sub-second Delay could
		// name a time already past.
		msg.Header.Set(js.ScheduleHeader, "@at "+at.UTC().Format(time.RFC3339Nano))
		msg.Header.Set(js.ScheduleTargetHeader, s.subject)
	}
	return nil
}

// fillReceipt records what the broker said about an accepted publication.
//
// The storage is the caller's and the core has already zeroed it, so filling it
// in place is what keeps "no result struct allocation on success" true. The
// confirmation is reported as ConfirmDurable whatever the publisher asked for,
// because that is what a PubAck IS on this broker: a driver that echoed
// ConfirmAccepted back would be under-reporting its own guarantee.
func (s *sink) fillReceipt(m *mq.Message, ack *js.PubAck) {
	if !s.receipt || m == nil || m.Receipt == nil || ack == nil {
		return
	}
	m.Receipt.Sequence = ack.Sequence
	m.Receipt.Confirmation = mq.ConfirmDurable
	m.Receipt.ConfirmedAt = s.b.now()
	m.Receipt.MessageID = ack.Stream + ":" + strconv.FormatUint(ack.Sequence, 10)
}

// fail maps a native failure onto the model, with sentAlready deciding
// whether the outcome is ambiguous.
func (s *sink) fail(err error, sentAlready bool) error {
	return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, sentAlready)
}

// copyBody copies a payload the client library will hold past the call.
//
// It is the obligation of every asynchronous path, and the reason is
// narrower than "the library buffers": nats.go's PublishMsg appends the payload
// to its own write buffer inside the call, so the first attempt never aliases —
// but the JetStream client RETAINS the *nats.Msg it was given and republishes it
// when the server answers "no responders", and again after a reconnection. A
// retry that read the caller's slice would put whatever the caller wrote next on
// the wire, which is a corrupted message rather than a failed one.
//
// It preserves nil, because the specification makes a nil body and an empty one different
// things and this driver carries the distinction in a header.
func (s *sink) copyBody(b []byte) []byte {
	s.b.bodyCopies.Add(1)
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

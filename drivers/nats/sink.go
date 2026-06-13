package nats

import (
	"context"
	"sync"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// The sink implements the mandatory publication surface, the optional native
// batch and the optional flush.
//
// It does NOT implement [bitbucket.org/iqhive/iqmq.AsyncSink] or
// [bitbucket.org/iqhive/iqmq.OwnedSink], and that is a decision rather than an
// omission. nats.go's Publish already returns as soon as the message is in the
// write buffer, so "asynchronous publication" on this broker is ConfirmNone,
// which the core reaches through the synchronous surface; an AsyncSink here
// would be a goroutine per message wrapped around a call that does not block,
// which the specification forbids outright. OwnedSink would have nothing to release earlier
// than Publish already does: the payload is serialised into the write buffer
// before Publish returns.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
)

// sink is one open publisher, bound to one subject for its lifetime (ADR-021).
type sink struct {
	b       *Backend
	topic   string
	subject string
	meta    metadataMode
	confirm mq.ConfirmationMode

	once   sync.Once
	closed chan struct{}
}

// Publish admits one message.
//
// The order is fixed and it is the specification: the per-call controls this broker cannot
// honour are refused before anything is written, then the metadata is mapped,
// then the bytes go to the socket, and only then — under ConfirmAccepted — does
// the flush wait for the server to say it has parsed them.
//
// Nothing of m is retained. The mapping copies every header key and value into
// the nats.Header as a string and the payload is serialised into the
// connection's write buffer by the call, so a caller may reuse its buffers the
// instant Publish returns.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m, o); err != nil {
		return err
	}
	var msg natsgo.Msg
	if err := s.build(m, &msg); err != nil {
		return err
	}
	if err := s.b.nc.PublishMsg(&msg); err != nil {
		// The write never left the client, so the failure is definite.
		return s.fail(err, false)
	}
	return s.confirmOne(ctx)
}

// PublishBatch admits a batch.
//
// The saving is real and it is the only one NATS Core offers: every message is
// written and ONE flush confirms all of them, so a batch of a hundred costs one
// round trip instead of a hundred. It is not atomic — there is no batch verb to
// be atomic with — which is what the capability note says and why the claim is
// an emulation rather than a native feature.
//
// The result is index-aligned with ms, which is the whole contract: a result
// aligned with anything else reports one message outcome for another.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	for _, m := range ms {
		if err := s.check(ctx, m, o); err != nil {
			return err
		}
	}
	var errs []error
	failed := 0
	for i, m := range ms {
		var msg natsgo.Msg
		err := s.build(m, &msg)
		if err == nil {
			err = s.b.nc.PublishMsg(&msg)
		}
		if err != nil {
			if errs == nil {
				errs = make([]error, len(ms))
			}
			errs[i] = s.fail(err, false)
			failed++
		}
	}
	if cerr := s.confirmOne(ctx); cerr != nil {
		// The flush covers every message that was written, so a flush
		// failure is every one of their outcomes, and each is ambiguous:
		// the server may have taken them all before the link went away.
		if errs == nil {
			errs = make([]error, len(ms))
		}
		for i := range errs {
			if errs[i] == nil {
				errs[i] = cerr
				failed++
			}
		}
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

// Flush completes the publications already admitted, bounded by ctx.
func (s *sink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	return s.flush(ctx)
}

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It does not flush: the specification gives Close no context, and a flush is a round trip
// a caller who wants it asks for with Shutdown. What it does not do is touch
// the connection — many sinks share one.
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

// check is "enforced before I/O": the controls this broker has no way to
// honour are refused here, with ErrUnsupported, before a byte is written.
//
// A driver that ignored them instead would publish a message the caller asked
// to be delayed, to expire, or to be de-duplicated, and the caller would have
// no way to find out. The specification strict mode makes the metadata refusal the default,
// and the three other modes do not soften these five: a delay is not metadata
// that can be dropped or enveloped, it is behaviour the broker does not have.
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
		case o.PartitionSet:
			return errNoPartition
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

// build fills msg with what this sink publishes, enveloping the message when
// the metadata mode says to.
//
// msg is the caller storage, so a publication allocates nothing but the header
// map, and only when the message has metadata to put in one.
func (s *sink) build(m *mq.Message, msg *natsgo.Msg) error {
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
	return nil
}

// confirmOne applies the sink's confirmation boundary.
//
// ConfirmAccepted costs one PING/PONG round trip per publication, because this
// broker has no per-message acknowledgement and a flush is the strongest
// boundary it offers. A caller who wants throughput asks for ConfirmNone, or
// publishes a batch, which is written in full and confirmed once.
func (s *sink) confirmOne(ctx context.Context) error {
	if s.confirm == mq.ConfirmNone {
		return nil
	}
	return s.flush(ctx)
}

// flush is the PING/PONG round trip that proves the server has parsed
// everything written before it.
//
// The caller context bounds it when the caller gave a deadline; when it did
// not, [Config.FlushTimeout] does, because a flush with no bound at all waits
// for ever on a link that is up but silent. That is a driver-level bound on a
// driver-level round trip, not a second deadline on a caller who has one.
func (s *sink) flush(ctx context.Context) error {
	if err := s.b.flush(ctx); err != nil {
		// Everything this flush covers had already been written, so its
		// outcome is ambiguous: the server may have taken it.
		return s.fail(err, true)
	}
	return nil
}

// fail maps a native failure onto the model, with sentAlready deciding
// whether the outcome is ambiguous.
func (s *sink) fail(err error, sentAlready bool) error {
	return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, sentAlready)
}

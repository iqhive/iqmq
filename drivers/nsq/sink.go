package nsq

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of what this sink is.
//
// There is no FlushSink: every publication here is a round trip that has
// already completed when Publish returns, so there is nothing admitted to
// flush. And no OwnedSink: the message is encoded synchronously and a body
// published without an envelope is copied, so ownership transfer would buy
// nothing and add a release the driver could get wrong.
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.BatchSink = (*sink)(nil)
)

// sink publishes to one NSQ topic.
type sink struct {
	b       *Backend
	topic   string
	iqTopic string
	meta    mq.MetadataMode
	env     *driverkit.Envelope
	forced  *driverkit.Envelope

	once   sync.Once
	closed atomic.Bool
}

// Publish admits one message.
//
// The order is the whole contract: every refusal this driver can make
// without I/O is made before the frame is built, and the envelope's own
// admission check is one of them. On a broker with no headers the envelope is
// the ONLY metadata path, so a message the envelope cannot carry is a message
// that cannot be published — and the refusal names the field and the bound,
// because "cannot be enveloped" on its own leaves a caller nothing to act on.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	body, delay, err := s.prepare(ctx, m, o)
	if err != nil {
		return err
	}
	if delay > 0 {
		if err := s.b.prod.DeferredPublish(ctx, s.topic, delay, body); err != nil {
			return s.fail(err)
		}
		return nil
	}
	if err := s.b.prod.Publish(ctx, s.topic, body); err != nil {
		return s.fail(err)
	}
	return nil
}

// PublishBatch admits a batch through MPUB.
//
// MPUB is one command carrying many bodies and one answer, so a partial
// outcome is not something nsqd reports: either the whole command was taken or
// it was refused. The *BatchError this returns is therefore index-aligned with
// ms and carries the SAME error at every index, which is the honest shape —
// the alternative, one error on a guessed index, would report one message's
// outcome for another, which is how a batch publish loses a message silently.
//
// A batch that would exceed nsqd's --max-body-size is SPLIT into as many MPUBs
// as it needs, and each piece is its own command with its own outcome. So a
// batch can half succeed here even though one MPUB cannot, and the alignment
// is what tells the caller which half.
//
// A batch carrying a delay cannot use MPUB at all: DPUB takes one body. Such a
// batch is published one message at a time, which is what the core's own loop
// would have done.
func (s *sink) PublishBatch(ctx context.Context, ms []*mq.Message, o *mq.PublishOptions) error {
	if len(ms) == 0 {
		return nil
	}
	errs := make([]error, len(ms))
	bodies := make([][]byte, 0, len(ms))
	idx := make([]int, 0, len(ms))
	deferred := o != nil && (o.Delay > 0 || !o.DeliverAt.IsZero())
	for i, m := range ms {
		body, delay, err := s.prepare(ctx, m, o)
		if err != nil {
			errs[i] = err
			continue
		}
		if deferred {
			// DPUB carries one body, so a scheduled batch is a loop.
			if perr := s.b.prod.DeferredPublish(ctx, s.topic, delay, body); perr != nil {
				errs[i] = s.fail(perr)
			}
			continue
		}
		bodies = append(bodies, body)
		idx = append(idx, i)
	}
	if len(bodies) > 0 {
		s.multiPublish(ctx, bodies, idx, errs)
	}
	return batchResult(errs)
}

// multiPublish sends the accepted bodies as one or more MPUBs, splitting on
// nsqd's --max-body-size, and records each piece's outcome against every index
// that piece carried.
func (s *sink) multiPublish(ctx context.Context, bodies [][]byte, idx []int, errs []error) {
	limit := s.b.cfg.MaxBodyBytes
	start := 0
	// MPUB's framing is a 4-byte count followed by a 4-byte length per body,
	// and nsqd bounds the whole command body. Counting the framing is what
	// keeps a batch that is exactly at the limit from being refused after
	// the round trip.
	const mpubHeader = 4
	const perBody = 4
	size := int64(mpubHeader)
	for i := range bodies {
		n := int64(perBody + len(bodies[i]))
		if i > start && limit > 0 && size+n > limit {
			s.sendMulti(ctx, bodies[start:i], idx[start:i], errs)
			start = i
			size = int64(mpubHeader)
		}
		size += n
	}
	s.sendMulti(ctx, bodies[start:], idx[start:], errs)
}

// sendMulti sends one MPUB and writes its outcome to every index it carried.
func (s *sink) sendMulti(ctx context.Context, bodies [][]byte, idx []int, errs []error) {
	if len(bodies) == 0 {
		return
	}
	err := s.b.prod.MultiPublish(ctx, s.topic, bodies)
	if err == nil {
		return
	}
	mapped := s.fail(err)
	for _, i := range idx {
		errs[i] = mapped
	}
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

// prepare is every refusal this sink can make without I/O, and the body to
// publish.
func (s *sink) prepare(ctx context.Context, m *mq.Message, o *mq.PublishOptions) ([]byte, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if s.closed.Load() {
		return nil, 0, mq.ErrClosed
	}
	if m == nil {
		return nil, 0, errNilMessage
	}
	var delay time.Duration
	if o != nil {
		switch {
		case o.TTL > 0:
			return nil, 0, errNoTTL
		case o.PrioritySet:
			return nil, 0, errNoPriority
		case o.DedupKey != "":
			return nil, 0, errNoDedup
		case o.PartitionSet:
			return nil, 0, errNoPartition
		}
		delay = o.Delay
		if !o.DeliverAt.IsZero() {
			delay = o.DeliverAt.Sub(s.b.now())
			if delay < 0 {
				// A time already past is "deliver now", not an error: the
				// caller asked for a message to be deliverable at an
				// instant, and that instant has come.
				delay = 0
			}
		}
		if delay > s.b.cfg.MaxRequeueDelay {
			return nil, 0, errDelayTooLong
		}
	}
	out, err := wrapForPublish(s.env, s.forced, s.b.limit, s.meta, m)
	if err != nil {
		return nil, 0, err
	}
	if s.meta == mq.MetadataLenientDrop && out != m {
		s.b.dropped.Add(uint64(droppedMetadata(m)))
	}
	// The size check is on what will actually be written, which for an
	// enveloped message is the envelope: a 1 MiB body plus its headers is
	// over nsqd's default limit even though the body alone is not, and
	// finding that out from nsqd costs a round trip and an E_BAD_MESSAGE.
	if lim := s.b.cfg.MaxMessageBytes; lim > 0 && int64(len(out.Body)) > lim {
		return nil, 0, mq.ErrMessageTooLarge
	}
	if len(out.Body) == 0 {
		// nsqd refuses an empty PUB body with E_BAD_MESSAGE, so an empty
		// and a nil body are both unpublishable here. Refusing before the
		// round trip is saying which is.
		return nil, 0, errEmptyBody
	}
	if sharesStorage(out.Body, m.Body) {
		// go-nsq holds the body until its writer has sent the frame, and a
		// publish whose context ends first returns before that: the caller
		// would then be free to reuse a buffer the frame is still being
		// written from. An envelope is a buffer of the driver's own.
		return slices.Clone(out.Body), delay, nil
	}
	return out.Body, delay, nil
}

// sharesStorage reports whether a and b begin at the same byte.
func sharesStorage(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

// fail maps a publication's native error and records a background failure when
// the link is what went wrong.
func (s *sink) fail(err error) error {
	mapped := s.b.errs.Map(mq.OpPublish, s.iqTopic, "", err)
	if mq.CodeOf(mapped) == mq.CodeUnavailable {
		s.b.degrade(mapped)
	}
	return mapped
}

// Close releases the sink. It is idempotent, and a Publish afterwards returns
// ErrClosed.
//
// It does not stop the producer: producers are the BACKEND's, shared by every
// sink it opened, and a sink that stopped one would close another sink's
// connection.
func (s *sink) Close() error {
	s.once.Do(func() {
		s.closed.Store(true)
		s.b.forget(nil, s)
	})
	return nil
}

// The sink's refusals.
var (
	errNoDurableConfirm = errcode.New(errcode.Unsupported, "iqmq/nsq: nsqd acknowledges a publication once it is in the topic's queue, which is in memory up to --mem-queue-size, so ConfirmDurable cannot be honoured (the message contract)")
	errNoTTL            = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ cannot give a message a time to live")
	errNoPriority       = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no message priority")
	errNoDedup          = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no publication de-duplication")
	errNoPartition      = errcode.New(errcode.Unsupported, "iqmq/nsq: NSQ has no partitions to publish to")
	errDelayTooLong     = errcode.New(errcode.InvalidArgument, "iqmq/nsq: the delay is longer than MaxRequeueDelay, which mirrors nsqd's --max-req-timeout")
	errEmptyBody        = errcode.New(errcode.InvalidArgument, "iqmq/nsq: nsqd refuses a message with an empty body; publish at least one byte, or use MetadataPortableEnvelope, whose envelope is never empty")
)

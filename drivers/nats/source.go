package nats

import (
	"context"
	"sync"

	natsgo "github.com/nats-io/nats.go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
)

// The source implements the mandatory consumption surface and nothing else.
//
// [source.Settler] returns nil, which is AckNone model declared: NATS Core
// has no settlement of any kind — no ack, no nack, no lease, no redelivery —
// so the core answers every Ack, Nack and Reject with ErrNotSettleable and
// never calls this driver. A driver that returned a settler whose Ack silently
// succeeded would be the worse lie: the caller would believe a message was
// safely processed at a broker that had already forgotten it.
var _ mq.Source = (*source)(nil)

// source is one open subscription: the NATS queue subscription, the bounded
// ring the native callback pushes into, and the fetch that drains it.
type source struct {
	b            *Backend
	sub          *natsgo.Subscription
	push         *driverkit.PushAdapter
	topic        string
	subscription string
	prefix       string
	meta         metadataMode

	once   sync.Once
	closed chan struct{}
	gen    uint32
}

// newSource subscribes and wires the callback into the ring.
//
// The ring is the whole reason driverkit.PushAdapter exists: NATS delivers by
// calling the driver and the SPI asks the driver to be called, so something has
// to sit between them, and that something has to be bounded. A push adapter
// that buffered without bound would turn a slow handler into unbounded memory
// growth, which the specification forbids.
//
// Blocking in the callback is the backpressure, and on this broker it has a
// consequence worth stating: nats.go keeps delivering into the subscription
// pending queue while the callback is parked, and once that queue passes
// SetPendingLimits the client starts DROPPING messages and reports a slow
// consumer. On an at-most-once broker that is the contract — there is nowhere
// to push back to — and it is why the health transition in health.go matters
// more here than it would on a broker with flow control.
func (b *Backend) newSource(cfg mq.SourceConfig, subject string, meta metadataMode) (*source, error) {
	push, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: cfg.Prefetch})
	if err != nil {
		return nil, err
	}
	src := &source{
		b:            b,
		push:         push,
		topic:        cfg.Topic,
		subscription: cfg.Subscription,
		prefix:       b.cfg.SubjectPrefix,
		meta:         meta,
		closed:       make(chan struct{}),
		gen:          1,
	}
	sub, err := b.nc.QueueSubscribe(subject, cfg.Subscription, src.onMessage)
	if err != nil {
		_ = push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	if err := sub.SetPendingLimits(b.cfg.PendingMessages, b.cfg.PendingBytes); err != nil {
		_ = sub.Unsubscribe()
		_ = push.Close()
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	src.sub = sub
	return src, nil
}

// onMessage is the native callback. It blocks while the ring is full, which is
// what makes the ring a bound rather than a buffer.
func (s *source) onMessage(msg *natsgo.Msg) {
	d, err := s.delivery(msg)
	if err != nil {
		// A message this driver cannot decode is not a message the caller
		// can do anything with, and there is no settlement to reject it
		// with. The specification makes it a background failure, so it goes to health.
		s.b.health.Degraded(s.b.errs.Map(mq.OpFetch, s.topic, s.subscription, err))
		return
	}
	// context.Background is deliberate: the callback has no context of its
	// own, and Close wakes a parked Push with ErrClosed, which is what
	// The specification requires and what keeps this goroutine from outliving the
	// source. The only error Push can report is that close, so the message
	// is dropped — which is what a closed at-most-once subscription means.
	_ = s.push.Push(context.Background(), d)
}

// delivery maps one native message onto a DriverDelivery.
func (s *source) delivery(msg *natsgo.Msg) (mq.DriverDelivery, error) {
	m := &mq.Message{}
	switch s.meta {
	case metaEnvelope:
		var raw mq.Message
		Decode(&raw, msg, s.prefix)
		if err := s.b.env.Unwrap(m, &raw); err != nil {
			return mq.DriverDelivery{}, err
		}
		m.Topic = raw.Topic
	case metaAdaptive:
		var raw mq.Message
		Decode(&raw, msg, s.prefix)
		// The marker and never the payload: a native body that happens to
		// be a valid envelope is a payload, and decoding it as metadata
		// would hand the application bytes nobody published.
		if Enveloped(msg.Header) {
			if err := s.b.env.Unwrap(m, &raw); err != nil {
				return mq.DriverDelivery{}, err
			}
			m.Topic = raw.Topic
		} else {
			*m = raw
		}
	case metaStrict, metaDrop:
		Decode(m, msg, s.prefix)
	}
	return mq.DriverDelivery{
		Message: m,
		Info: mq.DeliveryInfo{
			Topic:        m.Topic,
			Subscription: s.subscription,
			ReceivedAt:   s.b.now(),
			// Attempt is zero: the specification makes that "the broker does not count
			// attempts", which is the truth here rather than a default.
			// LeaseExpires is the zero time: the specification makes that "the broker
			// grants no lease", which is also the truth.
		},
		Native: msg,
		Gen:    s.gen,
	}, nil
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// It is the ring drain and nothing more: one blocking wait then a non-blocking
// take of everything there, which is what makes a batch receive a batch. It
// allocates nothing per fetch.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	return s.push.Fetch(ctx, dst)
}

// Settler returns nil: this source has no settlement (AckNone).
func (s *source) Settler() mq.Settler { return nil }

// Close unsubscribes, wakes a parked Fetch and drops whatever the ring still
// held. It is idempotent.
//
// There is nothing to requeue: a delivery this source handed out or still holds
// is a message the server has already forgotten, so "requeue or abandon"
// can only be abandon, and saying so is what the at-most-once claim means in
// practice.
func (s *source) Close() error {
	var err error
	s.once.Do(func() {
		close(s.closed)
		if s.sub != nil {
			if uerr := s.sub.Unsubscribe(); uerr != nil &&
				!isGone(uerr) {
				err = s.b.errs.Map(mq.OpClose, s.topic, s.subscription, uerr)
			}
		}
		_ = s.push.Close()
		s.push.Drain(nil)
		s.b.forget(s, nil)
	})
	return err
}

// isGone reports whether an unsubscribe failed because there was nothing left
// to unsubscribe: the connection was closed underneath us, which is not a
// failure of Close.
func isGone(err error) bool {
	if err == nil {
		return true
	}
	return errorsIsAny(err,
		natsgo.ErrConnectionClosed,
		natsgo.ErrBadSubscription,
		natsgo.ErrInvalidConnection,
		natsgo.ErrConnectionDraining,
	)
}

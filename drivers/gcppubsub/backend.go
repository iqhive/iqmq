package gcppubsub

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/gcppubsub/internal/psapi"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the Google Cloud Pub/Sub [github.com/iqhive/iqmq.Backend].
//
// It owns the client, the capability table, the error mapping, the health
// state machine and the fixed completion-worker pool, and it hands out sinks
// and sources over one client: the library multiplexes its own gRPC
// connections, so a client per publisher would be spending connections to no
// purpose.
type Backend struct {
	*driverkit.BaseBackend
	cfg   Config
	api   psapi.Client
	errs  *driverkit.ErrorMapper
	caps  mq.Capabilities
	clock mq.Clock
	pool  *completionPool

	// health is the reporter, held separately from BaseBackend's copy so
	// that a construction that fails can report Degraded before the base
	// exists.
	health *driverkit.Health

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than
	// left holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool

	// sick is true once a background failure has been reported and until the
	// next success. It exists so that the recovery half of the state
	// machine costs one atomic load per delivery rather than a mutex: see
	// [Backend.recovered].
	sick atomic.Bool
}

// init finishes construction once a client exists.
func (b *Backend) init(api psapi.Client, own driverkit.Ownership) error {
	b.api = api
	b.clock = b.cfg.Clock
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	caps, err := capabilities(b.cfg)
	if err != nil {
		return err
	}
	b.caps = caps
	pool, err := newCompletionPool(b.cfg.CompletionWorkers, b.cfg.CompletionQueue)
	if err != nil {
		return err
	}
	b.pool = pool
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       api.Native(),
		Ownership:    own,
		CloseNative:  api.Close,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		b.pool.close()
		return err
	}
	b.BaseBackend = base
	b.health.Ready()
	return nil
}

// info is the description of the driver.
//
// The profile is the CLIENT GENERATION — "v2" or "v1compat" — and it is never
// empty and never hidden. The specification is a bake-off between two client
// libraries behind one capability table, so a caller who read "gcppubsub" and
// did not know which library answered would not know which set of upstream
// bugs it has.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       b.api.Generation(),
		DriverVersion: Version,
		ClientName:    b.api.ClientModule(),
		ClientVersion: b.api.ClientVersion(),
	}
}

// Project is the Google Cloud project this backend is bound to.
func (b *Backend) Project() string { return b.api.Project() }

// now reads the injected clock, which is what every lease view and every
// timestamp in this driver reads.
func (b *Backend) now() time.Time {
	if b.clock != nil {
		return b.clock.Now()
	}
	return time.Now()
}

// call bounds one API call whose caller gave no deadline of its own.
//
// It is never a SECOND deadline on a caller that has one: a context with a
// deadline is returned unchanged, which is what keeps a caller's own budget
// authoritative.
func (b *Backend) call(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.cfg.APITimeout)
}

// degraded records a background failure, which the specification makes Health's job
// because there is no call to return it from.
func (b *Backend) degraded(err error) {
	if err != nil {
		b.sick.Store(true)
		b.health.Degraded(err)
	}
}

// recovered records that the background path is working again, and is the one
// half of the health state machine that has to be cheap.
//
// A failure is rare and a delivery is not, so "the driver is working" cannot
// be reported per delivery through [driverkit.Health] — its state is behind a
// mutex. The atomic flag is what makes the common case one relaxed load: only
// a source that has actually seen a failure calls Ready, and it calls it once.
func (b *Backend) recovered() {
	if b.sick.CompareAndSwap(true, false) {
		b.health.Ready()
	}
}

// OpenSink opens a publication channel to one topic.
//
// The topic identifier is derived here rather than per publication, so a topic
// name the service's grammar refuses is a construction failure rather than a
// surprise at the first message. Whether the topic EXISTS is not checked: a
// publisher handle performs no I/O, and a GetTopic per sink would cost a round
// trip to learn something the first publication reports anyway.
// [Config.AutoCreate] is what creates it.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	topic, err := b.cfg.TopicID(cfg.Topic)
	if err != nil {
		return nil, err
	}
	enc, err := newEncoder(b.cfg, b.caps, cfg.Metadata)
	if err != nil {
		return nil, err
	}
	if b.cfg.AutoCreate {
		if err := b.ensureTopic(ctx, topic); err != nil {
			return nil, err
		}
	}
	pub, err := b.api.Publisher(psapi.PublisherConfig{
		Topic:                      topic,
		Ordering:                   b.cfg.Ordering,
		CountThreshold:             b.cfg.PublishCountThreshold,
		ByteThreshold:              b.cfg.PublishByteThreshold,
		DelayThreshold:             b.cfg.PublishDelayThreshold,
		Timeout:                    b.cfg.PublishTimeout,
		NumGoroutines:              b.cfg.PublishGoroutines,
		FlowMaxOutstandingMessages: b.cfg.PublishFlowMaxOutstandingMessages,
		FlowMaxOutstandingBytes:    b.cfg.PublishFlowMaxOutstandingBytes,
		Compression:                b.cfg.Compression,
		CompressionThreshold:       b.cfg.CompressionThreshold,
	})
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", err)
	}
	s := &sink{b: b, cfg: cfg, enc: enc, pub: pub, topic: topic}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		pub.Stop()
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	return s, nil
}

// OpenSource opens a subscription.
//
// SourceConfig.Subscription is the subscription IDENTIFIER, and it is
// required: a Pub/Sub subscription is a named resource with its own
// acknowledgement state, so two consumers of one topic with no name would be
// one subscription or two with nothing to say which.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	topic, err := b.cfg.TopicID(cfg.Topic)
	if err != nil {
		return nil, err
	}
	subID, err := b.cfg.SubscriptionID(cfg.Topic, cfg.Subscription)
	if err != nil {
		return nil, err
	}
	if err := b.checkStart(cfg.Start); err != nil {
		return nil, err
	}
	enc, err := newEncoder(b.cfg, b.caps, cfg.Metadata)
	if err != nil {
		return nil, err
	}
	if b.cfg.AutoCreate {
		if err := b.ensureSubscription(ctx, topic, subID, cfg.AckDeadline); err != nil {
			return nil, err
		}
	}
	info, err := b.subscriptionInfo(ctx, subID)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	if err := b.applyStart(ctx, subID, cfg.Start); err != nil {
		return nil, err
	}
	// The receiver does not take the opening context, and contextcheck is
	// right that it does not: a streaming pull outlives the call that opened
	// it — a source lives until Close — so a receiver bound to the open's
	// context would stop the moment a caller who used a bounded context for
	// construction let it expire, which is a subscription that has silently
	// stopped consuming.
	src, err := b.newSource(cfg, enc, topic, subID, info)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = src.Close()
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	src.start() //nolint:contextcheck // as above: a receiver outlives the call that opened it
	return src, nil
}

// checkStart refuses a start position this broker cannot honour, before any
// I/O, which is what the specification requires of an option a driver cannot keep.
//
// A Pub/Sub subscription has no start position of its own: its backlog begins
// at its own creation and a consumer reads whatever it has not acknowledged.
// StartLatest and StartEarliest are therefore the same position and both are
// accepted — which is the rule, reached through the start
// position rather than through the Need. StartAt is a Seek and needs
// [Config.RetainAcked]; a sequence is refused outright, because there is no
// per-message sequence number to seek to.
func (b *Backend) checkStart(s mq.StartPosition) error {
	switch s.Resolve().Kind() {
	case mq.StartKindLatest, mq.StartKindEarliest:
		return nil
	case mq.StartKindTime:
		if !b.cfg.RetainAcked {
			return errNoSeek
		}
		return nil
	case mq.StartKindSequence:
		return errNoSeekBySequence
	case mq.StartKindUnset:
		return nil
	default:
		return errNoSeekBySequence
	}
}

// applyStart performs the Seek a StartAt asks for.
func (b *Backend) applyStart(ctx context.Context, subID string, s mq.StartPosition) error {
	if s.Resolve().Kind() != mq.StartKindTime {
		return nil
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	if err := b.api.Admin().Seek(callCtx, subID, s.Time()); err != nil {
		return b.errs.Map(mq.OpOpen, "", subID, err)
	}
	return nil
}

// subscriptionInfo reads a subscription's configuration, which is what decides
// whether settlement reports an outcome and whether a delivery carries an
// attempt count. Neither is something a driver may assume.
func (b *Backend) subscriptionInfo(ctx context.Context, subID string) (psapi.SubscriptionInfo, error) {
	callCtx, cancel := b.call(ctx)
	defer cancel()
	return b.api.Admin().GetSubscription(callCtx, subID)
}

// Ping is a cheap liveness check.
//
// It lists at most one topic, which consumes no message and creates no
// topology: the two things the specification forbids a Ping to do. A GetTopic for a name
// nobody uses would also work and would report NotFound for a healthy service,
// which a caller cannot tell from a real failure.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	n := 0
	err := b.api.Admin().ListTopics(callCtx, func(string) bool {
		n++
		return false
	})
	if err != nil {
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	return nil
}

// teardown closes everything this backend opened.
//
// The children are taken under the lock and closed outside it: a source's
// Close waits for its receiver, and holding the backend's mutex across that
// would block every other operation for as long as a streaming pull takes to
// notice.
func (b *Backend) teardown() error {
	b.mu.Lock()
	b.closed = true
	srcs := make([]*source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	b.mu.Unlock()
	var first error
	for _, s := range srcs {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, s := range sinks {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	// The pool is closed LAST, after every sink has stopped its publisher and
	// therefore after every completion it submitted has been drained: a pool
	// closed first would leave an asynchronous publication's callback
	// uncalled, which is the specification's "exactly once" broken in the direction
	// nothing reports.
	b.pool.close()
	return first
}

// forget drops a child from the backend's bookkeeping.
func (b *Backend) forget(src *source, snk *sink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if src != nil {
		delete(b.srcs, src)
	}
	if snk != nil {
		delete(b.sinks, snk)
	}
}

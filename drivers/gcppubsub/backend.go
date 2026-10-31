package gcppubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"

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

	// retainer is the arm's topic retention, nil for an arm without it:
	// drivers/gcppubsub/v1compat up to v1.1.1. See [psapi.TopicRetainer].
	retainer psapi.TopicRetainer

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
	b.retainer, _ = api.Admin().(psapi.TopicRetainer)
	if b.cfg.TopicRetention > 0 && b.retainer == nil {
		return errArmWithoutRetention
	}
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
	replay, err := b.checkEarliest(ctx, cfg, topic, subID)
	if err != nil {
		return nil, err
	}
	if err := b.checkStartAt(ctx, cfg, topic); err != nil {
		return nil, err
	}
	enc, err := newEncoder(b.cfg, b.caps, cfg.Metadata)
	if err != nil {
		return nil, err
	}
	sought := false
	if b.cfg.AutoCreate {
		created, sent, err := b.ensureSubscription(ctx, topic, subID, cfg.AckDeadline)
		if err != nil {
			if replay && sent && mayHaveLanded(err) {
				return nil, b.abandonCreate(ctx, cfg, subID, err)
			}
			return nil, err
		}
		if replay {
			// The seek comes straight after the create, before anything
			// else in this open can fail. A subscription created for
			// StartEarliest and not sought reads from its creation, and a
			// retry would accept it as an existing one: so from the
			// create to a successful seek every failure ends in a
			// deletion or a conflict, and once the seek has succeeded a
			// failure leaves a subscription whose backlog is the history.
			if err := b.seekToHistory(ctx, cfg, subID, created); err != nil {
				return nil, err
			}
			sought = true
		}
	}
	info, err := b.subscriptionInfo(ctx, subID)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	if !info.Exists {
		// The read reports a missing subscription as a configuration with
		// Exists false, not as an error. Opening on it anyway gave a
		// consumer that received nothing for ever, behind a streaming pull
		// failing every receiveBackoff, and settled as if the subscription
		// had no exactly-once delivery whatever it would turn out to have.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, errSubscriptionMissing)
	}
	if replay && !sought {
		// AutoCreate is off and the subscription appeared after
		// checkEarliest found it missing: another process made it a moment
		// ago, and it is sought as one whose create answered AlreadyExists.
		if err := b.seekToHistory(ctx, cfg, subID, false); err != nil {
			return nil, err
		}
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
// StartLatest is that position for a new subscription. StartEarliest asks for
// more, the topic's history, which a new subscription reaches only through a
// seek into the topic's message retention; whether the subscription is new and
// whether the topic retains anything are known only from the service, so that
// decision is [Backend.checkEarliest]'s. StartAt is a Seek and needs something
// retained to seek back over, the subscription's acknowledged messages
// ([Config.RetainAcked]) or the topic's own retention, which only the service
// knows, so that decision is [Backend.checkStartAt]'s. A sequence is refused
// outright, because there is no per-message sequence number to seek to.
func (b *Backend) checkStart(s mq.StartPosition) error {
	switch s.Resolve().Kind() {
	case mq.StartKindLatest, mq.StartKindEarliest:
		return nil
	case mq.StartKindTime:
		return nil
	case mq.StartKindSequence:
		return errNoSeekBySequence
	case mq.StartKindUnset:
		return nil
	default:
		return errNoSeekBySequence
	}
}

// checkEarliest decides what StartEarliest means for this open, before
// Config.AutoCreate creates anything, and reports whether the subscription has
// to be sought into its topic's history.
//
// A start position is where a subscription with no position of its own
// begins, and a subscription that exists has one, its backlog, which
// StartEarliest leaves where it is. A new one begins at its own creation, so
// it reads nothing the topic was sent before unless the topic retains
// messages and the subscription seeks back into them, which Google documents
// as what topic retention is for. With no retention there is nothing to read
// and StartEarliest is refused with ErrUnsupported. This used to accept
// StartEarliest, create the subscription and read from its creation, which is
// StartLatest under another name: a consumer that asked for the topic's
// history received none of it and was told nothing.
func (b *Backend) checkEarliest(ctx context.Context, cfg mq.SourceConfig, topic, subID string) (bool, error) {
	if cfg.Start.Resolve().Kind() != mq.StartKindEarliest {
		return false, nil
	}
	info, err := b.subscriptionInfo(ctx, subID)
	if err != nil {
		return false, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	if info.Exists {
		return false, nil
	}
	retains, err := b.topicRetains(ctx, cfg, topic)
	if err != nil {
		return false, err
	}
	if !retains {
		return false, errNoHistory
	}
	return true, nil
}

// checkStartAt refuses a StartAt that has nothing retained to seek back over.
//
// With Config.RetainAcked the subscription keeps its own acknowledged
// messages, and that is enough, as it always was. Without it the seek can
// reach only what the topic retains, so the topic's own retention is read
// rather than Config.TopicRetention, which applies only to topics this driver
// creates: a topic that already exists without retention would take the seek
// and reach nothing earlier than the backlog.
func (b *Backend) checkStartAt(ctx context.Context, cfg mq.SourceConfig, topic string) error {
	if cfg.Start.Resolve().Kind() != mq.StartKindTime || b.cfg.RetainAcked {
		return nil
	}
	retains, err := b.topicRetains(ctx, cfg, topic)
	if err != nil {
		return err
	}
	if !retains {
		return errNoSeek
	}
	return nil
}

// topicRetains reports whether the topic keeps messages for a seek to reach
// back to, from its own messageRetentionDuration as the service reports it, so
// a topic provisioned elsewhere counts. A topic that does not exist yet has
// the retention AutoCreate will give it, Config.TopicRetention.
func (b *Backend) topicRetains(ctx context.Context, cfg mq.SourceConfig, topic string) (bool, error) {
	if b.retainer == nil {
		return false, errArmWithoutRetention
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	retention, err := b.retainer.TopicRetention(callCtx, topic)
	switch {
	case errors.Is(err, psapi.ErrNotFound):
		return b.cfg.AutoCreate && b.cfg.TopicRetention > 0, nil
	case err != nil:
		return false, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	return retention > 0, nil
}

// seekToHistory seeks a subscription this open found missing back past the
// start of its topic's retention window, so that it reads what the topic
// retains rather than beginning at its own creation.
//
// A seek that fails leaves a subscription that reads from its creation, and
// the next open at StartEarliest would find it and accept it as an existing
// subscription, which is the silent StartLatest this replaces. So a
// subscription this open created is deleted again and the seek's failure is
// returned: a retry starts from nothing. A subscription whose create answered
// AlreadyExists is sought too, because the open saw it missing a moment
// before and the client library retries a create whose first attempt may
// have landed; it may also be another consumer's, so it is not deleted. When
// the subscription is left behind either way the error is a conflict and not
// retryable, and says what to do about it.
func (b *Backend) seekToHistory(ctx context.Context, cfg mq.SourceConfig, subID string, created bool) error {
	callCtx, cancel := b.call(ctx)
	defer cancel()
	err := b.api.Admin().Seek(callCtx, subID, historyStart())
	if err == nil {
		return nil
	}
	if !created {
		return leftBehind(cfg, err, nil)
	}
	if derr := b.deleteDetached(ctx, subID); derr != nil {
		return leftBehind(cfg, err, derr)
	}
	return b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
}

// abandonCreate answers a create that failed when StartEarliest needed it and
// may have landed all the same.
//
// A create can fail after the service has made the subscription, a deadline
// that expired with the answer on its way, and a subscription left that way
// reads from its creation exactly as one whose seek failed. So what the create
// may have left is deleted, and a deletion that answers NotFound means there
// was nothing to undo. It is only called for such a failure ([mayHaveLanded]):
// a refusal such as InvalidArgument made nothing, and a deletion after it
// could only fail into a misleading conflict, or remove a subscription another
// consumer's open created a moment before.
func (b *Backend) abandonCreate(ctx context.Context, cfg mq.SourceConfig, subID string, createErr error) error {
	if derr := b.deleteDetached(ctx, subID); derr != nil {
		return leftBehind(cfg, createErr, derr)
	}
	return createErr
}

// mayHaveLanded reports whether a failed request may have been carried out
// all the same: its answer was lost, timed out or never came, rather than being
// the service's refusal. These are the transient codes the error mapping
// already treats as retryable, the cancelled ones, and a context's own error.
func mayHaveLanded(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return matchCodes(codes.Unavailable, codes.DeadlineExceeded, codes.Unknown,
		codes.Aborted, codes.Internal, codes.Canceled)(err)
}

// deleteDetached deletes a subscription this open made and could not finish,
// on a context that outlives the caller's, because a cancelled or expired
// caller is one way the open fails. NotFound is success: there is nothing
// left.
func (b *Backend) deleteDetached(ctx context.Context, subID string) error {
	cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), b.cfg.APITimeout)
	defer cancel()
	err := b.api.Admin().DeleteSubscription(cleanCtx, subID)
	if err == nil || errors.Is(err, psapi.ErrNotFound) {
		return nil
	}
	return err
}

// historyStart is where a new subscription is sought to for StartEarliest:
// 32 days before the wall clock, a day before the start of the longest
// retention window the service allows.
//
// It is deliberately not "now less the topic's retention". Google documents a
// seek to a time before the retention window, or before the subscription's
// creation, as marking only what is still retained, and a seek to 40 days ago
// was measured giving the same result as one to ten minutes ago on the
// managed service and the emulator. A target that close to the window's start
// had to be read from a clock, and Config.Clock running ahead of the service
// by more than the window made the open succeed and read nothing. A month of
// margin takes every clock out of the answer.
func historyStart() time.Time {
	return time.Now().Add(-MaxRetention - 24*time.Hour)
}

// leftBehind is the error for a subscription that may exist without its
// seek. It is built here rather than mapped from the service's answer,
// because that answer is often retryable and this is not: a retry would open
// the subscription as an existing one and read none of the history.
func leftBehind(cfg mq.SourceConfig, failure, deleteErr error) error {
	return &mq.OpError{
		Driver:       DriverName,
		Operation:    mq.OpOpen,
		Topic:        cfg.Topic,
		Subscription: cfg.Subscription,
		Code:         mq.CodeConflict,
		Err:          errors.Join(errSeekLeftBehind, failure, deleteErr),
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

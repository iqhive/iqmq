package snssqs

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/snssqs/internal/snsapi"
	"bitbucket.org/iqhive/iqmq/drivers/sqs"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the SNS-to-SQS [bitbucket.org/iqhive/iqmq.Backend].
//
// It is two halves that meet at a queue. The publish half is this package: an
// SNS topic ARN, the mapping and PublishBatch. The consume half is a
// whole `drivers/sqs` backend, opened over the same credentials, whose sources
// this one wraps to report the SNS topic rather than the queue name.
//
// The composition is the design and not a shortcut. Everything the consume
// side has to get right — the long poll, the bounded ring, the local lease
// check, the supersession bookkeeping that makes a stale receipt detectable,
// the partial delete batch, the dead-letter ordering — is behaviour the specification got
// right, tested against a fake and against two emulators. A second copy of it
// here would be a second thing to get right, and the half that drifted would
// be the half nobody ran.
//
// What the composition costs is one place where the two halves have to be
// kept apart rather than shared: SETTLEMENT. One publication becomes N
// messages in N queues, so a delivery belongs to a subscription and nothing
// else. There is therefore no per-TOPIC bookkeeping in this type at all — the
// caches below are topology, not deliveries — and a source is one inner
// source over one queue with its own settler.
type Backend struct {
	*driverkit.BaseBackend
	cfg    Config
	sns    snsapi.SNS
	queues snsapi.Queues
	inner  *sqs.Backend
	errs   *driverkit.ErrorMapper
	caps   mq.Capabilities

	// health is the reporter, held separately from BaseBackend's copy so
	// that a construction that fails can report Degraded before the base
	// exists.
	health *driverkit.Health

	// mu guards the topology caches and the open children.
	//
	// The caches hold what a resolution cost — a topic ARN, and the queue
	// and subscription a (topic, subscription) pair names — and only
	// successful resolutions: a topic that does not exist yet must be
	// re-resolved, or a publisher opened before its topic was created would
	// never find it.
	mu     sync.Mutex
	arns   map[string]string
	subs   map[string]resolvedSub
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
}

// resolvedSub is one (topic, subscription) pair's topology, as the driver
// found or created it.
type resolvedSub struct {
	// QueueName is the SQS queue this subscription's copies arrive in.
	QueueName string
	// QueueURL is that queue's URL.
	QueueURL string
	// QueueARN is that queue's ARN, which is the Subscribe endpoint.
	QueueARN string
	// TopicARN is the SNS topic the subscription is on.
	TopicARN string
	// ARN is the subscription's own ARN.
	ARN string
}

// init finishes construction once the clients exist.
func (b *Backend) init(native any, own driverkit.Ownership, closeNative func() error) error {
	b.arns, b.subs = map[string]string{}, map[string]resolvedSub{}
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	caps, err := capabilities(b.cfg)
	if err != nil {
		return err
	}
	b.caps = caps
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       native,
		Ownership:    own,
		CloseNative:  closeNative,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return err
	}
	b.BaseBackend = base
	b.health.Ready()
	return nil
}

// info is the description of the driver.
//
// The profile is "standard", and it is stated rather than left empty because
// SNS has a FIFO topic profile this driver does not implement: a caller who
// read "snssqs" and planned on ordering would be planning against a topic type
// [CheckTopicName] refuses.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       "standard",
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
	}
}

// now reads the injected clock.
func (b *Backend) now() time.Time {
	if b.cfg.Clock != nil {
		return b.cfg.Clock.Now()
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

// OpenSink opens a publication channel to one SNS topic.
//
// The topic is resolved here rather than on the first publication, so that a
// topic that does not map to an SNS topic is a construction failure rather
// than a surprise at the first message.
//
// Nothing about the subscribers is checked, and that is not an omission: a
// topic with no subscriptions accepts a publication and delivers it nowhere,
// which is the service's behaviour and is also what a deployment that
// subscribes later wants. docs/drivers/snssqs.md's "known gotchas" says so.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	enc, err := sqs.NewEncoder(b.cfg.sqsConfig(), b.caps, cfg.Metadata)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	arn, err := b.topicARN(callCtx, cfg.Topic, b.cfg.AutoCreate)
	if err != nil {
		return nil, b.mapOpen(cfg.Topic, "", err)
	}
	s := &sink{b: b, cfg: cfg, enc: enc, arn: arn}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	return s, nil
}

// OpenSource opens a subscription.
//
// SourceConfig.Subscription is the QUEUE, which is the whole difference from
// `drivers/sqs`: there the field is ignored because a queue has exactly one
// set of messages, and here it selects which of the topic's queues to consume.
//
// Two refusals happen before any delivery can arrive, and both are open-time
// on purpose:
//
//   - a subscription that does not exist and may not be created (the specification's
//     opt-in), naming [Config.AutoCreate];
//   - a subscription whose RawMessageDelivery is not on. That one is the
//     reason this driver has a subscription check at all: without raw
//     delivery the body a consumer receives is SNS's JSON envelope AROUND the
//     publisher's body, so every message would arrive changed and every
//     payload would need unwrapping by hand. A driver that accepted it would
//     silently corrupt every message on the subscription.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	sub, err := b.subscription(callCtx, cfg.Topic, cfg.Subscription, b.cfg.AutoCreate)
	if err != nil {
		return nil, b.mapOpen(cfg.Topic, cfg.Subscription, err)
	}
	// The inner driver is handed the QUEUE NAME as its topic, because on
	// that driver a topic IS a queue. Its own resolver is the identity
	// (Config.sqsConfig), so the name is validated against the queue-name
	// grammar and looked up once, and every refusal it makes about a queue
	// is made in one place.
	inner, err := b.inner.OpenSource(ctx, mq.SourceConfig{
		Topic:        sub.QueueName,
		Subscription: cfg.Subscription,
		Prefetch:     cfg.Prefetch,
		Start:        cfg.Start,
		AckDeadline:  cfg.AckDeadline,
		Metadata:     cfg.Metadata,
	})
	if err != nil {
		return nil, err
	}
	src := &source{b: b, inner: inner, topic: cfg.Topic, sub: cfg.Subscription, queue: sub}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = src.Close()
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	return src, nil
}

// Ping is a cheap liveness check.
//
// It pings BOTH services, because a client that can publish and not consume is
// not a healthy fan-out client: SNS's ListTopics answers whether the topic
// half is reachable, and the inner driver's own Ping answers for the queue
// half. Neither consumes a message nor creates topology, which is what the specification
// forbids a Ping to do.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	if _, err := b.sns.ListTopics(callCtx, &sns.ListTopicsInput{}); err != nil {
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	return b.inner.Ping(ctx)
}

// Health is the backend's current state.
//
// It is the WORSE of the two halves, which is the only reading that is useful:
// the publish half has no background work and nothing to fail in — SNS is
// request/response over HTTP — and the consume half runs the pollers, so
// everything a health report can say about this driver comes from the inner
// one. Reporting only this backend's own state would answer Ready for a client
// whose pollers have been failing for an hour.
func (b *Backend) Health() mq.HealthSnapshot {
	own := b.BaseBackend.Health()
	if b.inner == nil {
		return own
	}
	in := b.inner.Health()
	switch {
	case own.State == mq.HealthClosed || in.State == mq.HealthClosed:
		// Closed is terminal, so it wins over a degradation
		// recorded before the close.
		return mq.HealthSnapshot{State: mq.HealthClosed}
	case in.State == mq.HealthDegraded:
		return in
	case own.State == mq.HealthDegraded:
		return own
	default:
		return own
	}
}

// There is deliberately NO method here that records a background failure.
// Everything in this driver that runs in the background is a poller, and the
// pollers are the inner driver's — so the only writer of a degraded state is
// the inner Health that [Backend.Health] reads, plus this backend's own
// construction failure in open.go. A second reporter would be a second answer
// to the same question.

// SQS is the inner queue-side backend.
//
// It is exported because this driver is honestly two drivers, and everything
// SQS's own typed administration offers — the queue attributes, the redrive
// policy, the message-move task that drains a dead-letter queue — belongs to
// the queue and is reachable only here. A caller that wants a subscription's
// queue by name asks [TypedAdmin.Queue] first.
func (b *Backend) SQS() *sqs.Backend { return b.inner }

// teardown closes everything this backend opened.
//
// The children are taken under the lock and closed outside it: a source's
// Close waits for its pollers, and holding this mutex across that would block
// every other operation for as long as a long poll takes to notice. It is the
// same rule as the recorded BaseBackend.Close defect, one level down.
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
	// The inner backend closes what IT opened, which is every source this
	// one wrapped plus the SQS client when it owns it. It is closed last so
	// that a source closing above still has a backend to talk to.
	if err := b.inner.Close(); err != nil && first == nil {
		first = err
	}
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

// mapOpen maps a topology failure raised while opening.
//
// A refusal this driver built itself — a name the mapping cannot express, a
// subscription with raw delivery off — already carries its code, and
// wrapping it in an *OpError whose code says something else is how a caller
// comes to retry an open that can never succeed.
func (b *Backend) mapOpen(topic, sub string, err error) error {
	if err == nil {
		return nil
	}
	if mq.CodeOf(err) != mq.CodeUnknown {
		return err
	}
	return b.errs.Map(mq.OpOpen, topic, sub, err)
}

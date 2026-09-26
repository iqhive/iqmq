package sqs

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/sqs/internal/sqsapi"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the SQS [github.com/iqhive/iqmq.Backend].
//
// It owns the client, the queue resolver, the capability table, the error
// mapping and the health state machine, and it hands out sinks and sources
// over one client: the SDK pools connections itself, so a client per publisher
// would be spending file descriptors to no purpose.
type Backend struct {
	*driverkit.BaseBackend
	cfg   Config
	api   sqsapi.API
	errs  *driverkit.ErrorMapper
	res   *resolver
	caps  mq.Capabilities
	clock mq.Clock

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
}

// init finishes construction once a client exists.
func (b *Backend) init(api sqsapi.API, native any, own driverkit.Ownership, closeNative func() error) error {
	b.api = api
	b.clock = b.cfg.Clock
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	b.res = newResolver(api, b.cfg)
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
// The profile is "standard" or "fifo" and is never empty, because the two
// profiles differ in seven capability claims: a caller who read "sqs" and got
// one profile's table for the other would be planning against a different
// queue.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       b.cfg.Profile(),
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
	}
}

// now reads the injected clock, which is what the local lease check and every
// timestamp in this driver read.
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

// OpenSink opens a publication channel to one queue.
//
// The queue is resolved here rather than on the first publication, so that a
// topic that does not map to a queue is a construction failure rather than a
// surprise at the first message.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Confirmation.Resolve() == mq.ConfirmNone {
		// There is no weaker boundary than the response: the SDK has no
		// fire-and-forget publication, so ConfirmNone would be
		// ConfirmAccepted under another name. Saying so through the
		// receipt is the specification's "a driver that had to fall back says so here",
		// and it is honest either way — the publication really is
		// confirmed.
		_ = cfg.Confirmation
	}
	enc, err := newEncoder(b.cfg, b.caps, cfg.Metadata)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	url, err := b.res.URL(callCtx, cfg.Topic)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", err)
	}
	s := &sink{b: b, cfg: cfg, enc: enc, url: url, nonce: nextNonce()}
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
// SourceConfig.Subscription is deliberately unused: a queue has exactly one
// set of messages, so every consumer of one topic competes for them whatever
// the subscription says. The capability table states it as part of
// FeatureConsumerGroups, and drivers/snssqs is where a real fan-out lives.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if err := checkStart(cfg.Start); err != nil {
		return nil, err
	}
	enc, err := newEncoder(b.cfg, b.caps, cfg.Metadata)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	url, err := b.res.URL(callCtx, cfg.Topic)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	// The source's pollers and its Close do not take this context, and
	// contextcheck is right that they do not: a poller outlives the call
	// that opened it — a source lives until Close — so a poller bound to the
	// open's context would stop the moment a caller who used a bounded
	// context for construction let it expire, which is a subscription that
	// has silently stopped consuming. Each round derives its own context
	// from Background instead. What bounds Close is Config.APITimeout: the
	// RELEASES a closing source makes take at most two -- one for the answer a
	// poller was admitting, undecodable messages included, and one for the
	// ring's drain, each batched under one deadline -- and the settlements a
	// coalescing Settle policy still holds are sent after them, each run
	// under an APITimeout of its own.
	src, err := b.newSource(cfg, enc, url)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		// Source.Close takes no context, which contextcheck reads as a
		// lost one. It is deliberate: the SPI's Close has no context, and
		// what bounds the release calls a closing source makes is
		// Config.APITimeout, as OpenSource says above. This is the backend
		// having closed underneath a source that had just been built, so
		// the source is torn down before it is ever registered.
		_ = src.Close() //nolint:contextcheck // Source.Close takes no context; Config.APITimeout is the bound
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	src.start() //nolint:contextcheck // as above: a poller outlives the call that opened it
	return src, nil
}

// checkStart refuses a start position this broker cannot honour.
//
// A queue has no history: a settled message is deleted and an unsettled one is
// at the head. StartLatest and StartEarliest are therefore the same position
// and both are accepted; a time or a sequence is refused before any I/O, which
// The specification requires of an option the driver cannot honour.
func checkStart(s mq.StartPosition) error {
	switch s.Resolve().Kind() {
	case mq.StartKindLatest, mq.StartKindEarliest:
		return nil
	case mq.StartKindTime, mq.StartKindSequence, mq.StartKindUnset:
		return errNoSeek
	default:
		return errNoSeek
	}
}

// Ping is a cheap liveness check.
//
// It lists at most one queue, which consumes no message and creates no
// topology: the two things the specification forbids a Ping to do. A GetQueueUrl for a
// name nobody uses would also work and would report NotFound for a healthy
// service, which a caller cannot tell from a real failure.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	callCtx, cancel := b.call(ctx)
	defer cancel()
	_, err := b.api.ListQueues(callCtx, &sqs.ListQueuesInput{MaxResults: aws.Int32(1)})
	if err != nil {
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	return nil
}

// QueueName is the queue name a topic maps to, without any I/O.
//
// It is exported because the mapping is a deployment's business: an operator
// creating queues by hand, or a Terraform module naming them, needs to know
// what a topic becomes before anything runs.
func (b *Backend) QueueName(topic string) (string, error) { return b.res.Name(topic) }

// QueueURL is the queue URL a topic maps to, resolving it if necessary.
func (b *Backend) QueueURL(ctx context.Context, topic string) (string, error) {
	callCtx, cancel := b.call(ctx)
	defer cancel()
	url, err := b.res.URL(callCtx, topic)
	if err != nil {
		return "", b.errs.Map(mq.OpAdmin, topic, "", err)
	}
	return url, nil
}

// teardown closes everything this backend opened.
//
// The children are taken under the lock and closed outside it: a source's
// Close waits for its pollers, and holding the backend's mutex across that
// would block every other operation for as long as a long poll takes to
// notice. It is the same rule as the recorded BaseBackend.Close defect, one
// level down.
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
		// Close takes no context, which contextcheck reads as a lost one:
		// teardown runs from BaseBackend.Close, which has none either, and
		// what bounds a source's own calls is Config.APITimeout, as
		// OpenSource says.
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, s := range sinks {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
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

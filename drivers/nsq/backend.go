package nsq

import (
	"context"
	"net/http"
	"sync"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/nsq/internal/nsqapi"
	"github.com/iqhive/iqmq/wire"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
//
// Two absences are as deliberate as the presences. There is no
// [github.com/iqhive/iqmq.BatchSettler]: nsqd has no batched settlement verb,
// so a driver that offered one would be coalescing FIN frames and calling it a
// batch. And no [github.com/iqhive/iqmq.AsyncSink]: PUB's round trip is what
// makes ConfirmAccepted mean anything here, and an asynchronous sink
// that returned before it would be ConfirmNone under another name.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
	_ mq.ClosingBackend = (*Backend)(nil)
)

// Backend is the NSQ backend.
type Backend struct {
	*driverkit.BaseBackend
	cfg   Config
	errs  *driverkit.ErrorMapper
	caps  mq.Capabilities
	limit *wire.Limits
	clock mq.Clock
	prod  nsqapi.Producer
	http  *http.Client
	counters

	// newConsumer is how a source gets its client. It is a field so that a
	// test can hand the driver an in-process fake: the four things the specification has to make happen on purpose — an expired per-message timeout, a
	// redelivery to a second consumer, a delayed requeue and a lookupd that
	// changes its mind — are all the broker's decisions.
	newConsumer func(topic, channel string, maxInFlight int, deadline time.Duration) (nsqapi.Consumer, error)

	// health is the reporter, held separately from BaseBackend's copy so
	// that the connection callbacks can drive it before the base exists.
	health *driverkit.Health

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than
	// left holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
	// closeBy is when every source's close stops waiting for its consumer's
	// stop once the client has begun to close ([Backend.BeginClose]), and
	// zero before. It is guarded by mu.
	closeBy time.Time
}

// newBackend builds the backend around an already-constructed producer.
func newBackend(cfg Config, prod nsqapi.Producer, own driverkit.Ownership, native any) (*Backend, error) {
	r := cfg.resolve()
	caps, err := capabilities(r)
	if err != nil {
		return nil, err
	}
	b := &Backend{
		cfg:   r,
		errs:  newErrorMapper(),
		caps:  caps,
		limit: envelopeLimits(r),
		clock: r.Clock,
		prod:  prod,
		http:  newAdminClient(r.APITimeout, r.TLS),
		srcs:  map[*source]struct{}{},
		sinks: map[*sink]struct{}{},
	}
	b.health = driverkit.NewHealth(nil)
	if f, ok := prod.(nsqapi.ConsumerFactory); ok {
		// The caller handed over a client that speaks both halves, so the
		// consuming side comes from there rather than from a real go-nsq
		// consumer dialling the configured addresses.
		b.newConsumer = func(topic, channel string, maxInFlight int, deadline time.Duration) (nsqapi.Consumer, error) {
			return f.Consumer(topic, channel, maxInFlight, deadline), nil
		}
	} else {
		b.newConsumer = func(topic, channel string, maxInFlight int, deadline time.Duration) (nsqapi.Consumer, error) {
			return newConsumer(b.cfg, topic, channel, maxInFlight, deadline)
		}
	}
	if native == nil {
		native = prod
	}
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       native,
		Ownership:    own,
		CloseNative:  func() error { prod.Close(); return nil },
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return nil, err
	}
	b.BaseBackend = base
	b.health.Ready()
	return b, nil
}

// envelopeLimits is the limit set handed to BOTH halves of the envelope.
//
// It is the decoder's own default set, narrowed only by what nsqd will carry:
// a body larger than nsqd's --max-msg-size cannot be published at all, so an
// envelope larger than it is a record that could never exist. Handing the same
// value to the encoder's check and the decoder is what keeps a publication
// this driver accepts decodable by the consumer that reads it — the defect
// class the maintenance notes records three drivers falling into, each through a
// second set of numbers that had drifted.
func envelopeLimits(r Config) *wire.Limits {
	lim := wire.DefaultLimits()
	if r.MaxMessageBytes > 0 && r.MaxMessageBytes < int64(lim.MaxBody) {
		lim.MaxBody = int(r.MaxMessageBytes)
	}
	return &lim
}

// metadataMode resolves a handle mode against [Config.Metadata].
//
// A handle at the zero value takes the configuration's; anything else keeps
// what it asked for, so an explicit MetadataPortableEnvelope is never
// downgraded and an explicit MetadataLenientDrop is never upgraded. The zero
// value is the one mode the override can replace, and Config.Metadata's own
// documentation says what that costs.
func (b *Backend) metadataMode(m mq.MetadataMode) mq.MetadataMode {
	if m == mq.MetadataNativeStrict && b.cfg.Metadata != mq.MetadataNativeStrict {
		return b.cfg.Metadata
	}
	return m
}

// info describes the driver and the client library behind it.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
		// BrokerVersion is left empty rather than guessed. nsqd reports its
		// version on the HTTP /info endpoint, which is a request rather
		// than a handshake, and the specification makes an empty string "the driver did
		// not say" — which is the truth until somebody asks.
	}
}

// now reads the injected clock, or the wall clock when there is none.
func (b *Backend) now() time.Time {
	if b.clock != nil {
		return b.clock.Now()
	}
	return time.Now()
}

// Ping is the cheap liveness check: a NOP to one nsqd, the next in the
// producer's round-robin order, so N calls reach each of N nsqds once and an
// nsqd that is down elsewhere in Addrs costs this call nothing. It never
// publishes and never creates a topic or a channel.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	degraded := b.health.State() == mq.HealthDegraded
	if err := b.prod.Ping(ctx); err != nil {
		b.degrade(err)
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	if degraded {
		// A successful probe after a failure is the only reconnection this
		// driver can observe: go-nsq reconnects inside its producer and its
		// consumer and offers no callback for it, so OnReconnect has
		// no other source here. It is coarse — a caller that never calls
		// Ping never sees one — and the driver page says so.
		b.recovered(ctx)
		return nil
	}
	b.health.Ready()
	return nil
}

// OpenSink opens a publication channel to one topic.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Confirmation.Resolve() == mq.ConfirmDurable {
		// nsqd's OK means the message is in the topic's queue, which is in
		// memory up to --mem-queue-size. The specification requires a publisher asking
		// for a durable boundary to fail at construction rather than be
		// given a weaker one silently.
		return nil, errNoDurableConfirm
	}
	topic, err := TopicOf(b.cfg.TopicPrefix, cfg.Topic)
	if err != nil {
		return nil, err
	}
	if !cfg.Metadata.Valid() {
		return nil, errMetadataMode
	}
	mode := b.metadataMode(cfg.Metadata)
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mode,
		Limits:       b.limit,
		Capabilities: b.caps,
	})
	if err != nil {
		return nil, err
	}
	forced, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mq.MetadataPortableEnvelope,
		Limits:       b.limit,
		Capabilities: b.caps,
	})
	if err != nil {
		return nil, err
	}
	if b.cfg.EnsureTopology {
		if err := b.ensureTopic(ctx, topic); err != nil {
			return nil, err
		}
	}
	s := &sink{b: b, topic: topic, iqTopic: cfg.Topic, meta: mode, env: env, forced: forced}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	return s, nil
}

// OpenSource opens a subscription: an nsqd channel on a topic.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	topic, err := TopicOf(b.cfg.TopicPrefix, cfg.Topic)
	if err != nil {
		return nil, err
	}
	channel, err := ChannelOf(cfg.Subscription)
	if err != nil {
		return nil, err
	}
	if start := cfg.Start.Resolve(); start.Kind() != mq.StartKindLatest {
		// A channel has no cursor: nsqd delivers what arrives after the
		// channel exists and keeps nothing it has finished. The specification forbids
		// emulating a start position, and refusing StartEarliest with
		// ErrUnsupported is also what makes the conformance kit's replay
		// denial answerable.
		return nil, errNoStartPosition
	}
	if !cfg.Metadata.Valid() {
		return nil, errMetadataMode
	}
	deadline := cfg.AckDeadline
	if deadline <= 0 {
		deadline = b.cfg.AckDeadline
	}
	if deadline > b.cfg.MaxAckDeadline {
		return nil, errAckDeadlinePastMax
	}
	if deadline < MinAckDeadline {
		return nil, errAckDeadlineBelowMin
	}
	if b.cfg.LeaseGuard >= deadline {
		// Config.Validate holds the backend's deadline to this and cannot
		// see a source's own. A shorter one put every delivery inside its
		// guard the moment it arrived: Fetch dropped each as dead, the
		// sweeper gave it back, nsqd sent it again, and nothing was ever
		// delivered. MinAckDeadline is below DefaultLeaseGuard, so the
		// shortest deadline nsqd accepts was one of them.
		return nil, errSourceGuardPastDeadline
	}
	mode := b.metadataMode(cfg.Metadata)
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mode,
		Limits:       b.limit,
		Capabilities: b.caps,
	})
	if err != nil {
		return nil, err
	}
	if b.cfg.EnsureTopology {
		if err := b.ensureChannel(ctx, topic, channel); err != nil {
			return nil, err
		}
	}
	cfg.Metadata = mode
	src, err := b.newSourceFor(cfg, topic, channel, deadline, env)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = src.Close() //nolint:contextcheck // Close takes no context by contract; its stop wait is bounded by APITimeout
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	// A backend Close that reaches the source while it connects leaves the
	// teardown to this call, which then reports ErrClosed (see
	// source.connMu): the Close below is that teardown.
	if err := src.connect(ctx); err != nil {
		_ = src.Close() //nolint:contextcheck // Close takes no context by contract; its stop wait is bounded by APITimeout
		return nil, err
	}
	return src, nil
}

// Admin returns the topology surface, which for NSQ is nsqd's HTTP API.
func (b *Backend) Admin() (mq.Admin, error) {
	if len(b.adminAddrs()) == 0 {
		return nil, errNoAdminAddress
	}
	return &admin{b: b}, nil
}

// forget drops a closed child.
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

// BeginClose is the client telling the backend that it is closing
// (mq.ClosingBackend). Client.Close calls it first, before it closes any
// publisher or consumer, and their closes and Backend.Close follow it.
// Client.Shutdown calls it once every handle's own shutdown has returned and
// the publishers have shut down, and only Backend.Close follows it there.
// Behind a consumer still draining under a longer bound another caller gave
// its shutdown, Client.Shutdown keeps the publishers open for that drain,
// shuts them down once the drain has ended, is stuck or has been cut short by
// a Close, and calls BeginClose only then. A handle whose close another caller
// began, a prompt Close or a publisher's Shutdown under a longer bound, may
// still reach its source or sink after it.
//
// It sets when every consumer's close stops waiting for its stop: one
// [Config.APITimeout] from now ([Backend.stopBy]). Client.Close closes its
// consumers one after another before the backend, and each consumer's close
// waited for its own stop, which on an nsqd that has stopped answering, with
// the connections still up, lasts APITimeout. So a process closing its client
// through a partition, or beside a frozen nsqd, waited one APITimeout for each
// of its consumers. Now the closes after this one wait for all of them for
// one APITimeout at most. A consumer whose close comes after that time, one
// behind the closes before it on an nsqd that has stopped answering or one
// opened while the client closes, does not wait for its stop at all: it has
// given back what it held by then, and go-nsq finishes the stop on its own.
//
// It also sets every open consumer's in-flight limit to zero (RDY 0), so
// nsqd sends nothing more, and changes nothing else. The consumers are not
// stopped: a stop gains nothing here, because go-nsq keeps a stopping
// connection until every message it carried is answered, and those answers
// come from each consumer's own close. What a consumer's handlers hold can
// still be acknowledged, requeued and extended until that close. A consumer
// OpenSource is still connecting
// as this runs receives nothing once it has connected. The publishers are
// left as they are: a publication waits for nsqd's answer up to its own
// bounds, as it did before, and go-nsq's producer redials on the next
// publication as it always does. A producer the caller lent stays open, as
// Close leaves it.
func (b *Backend) BeginClose() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	if b.closeBy.IsZero() {
		b.closeBy = time.Now().Add(b.cfg.APITimeout)
	}
	srcs := make([]*source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	b.mu.Unlock()
	for _, s := range srcs {
		s.cons.ChangeMaxInFlight(0)
	}
}

// stopBy is when a source's close stops waiting for its consumer's stop: one
// [Config.APITimeout] after the client began to close ([Backend.BeginClose]),
// or after now when it has not.
func (b *Backend) stopBy() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closeBy.IsZero() {
		return b.closeBy
	}
	return time.Now().Add(b.cfg.APITimeout)
}

// teardown is what NewBase calls on Close: close every child, then the client.
//
// The children are collected under the mutex and closed outside it, because a
// source's Close requeues what its ring still holds and that is a write to
// nsqd: holding the backend's mutex across it would make a slow broker block
// every other open and close on the backend.
//
// A source's close waits for its consumer's stop, which on an nsqd that has
// stopped answering lasts [Config.APITimeout]. Every stop starts before any
// is waited for, so they run together and Close waits one APITimeout for
// them all, or until the time [Backend.BeginClose] set. It closed the
// sources one after another, and waited one APITimeout for each. A source
// OpenSource is still connecting is left to OpenSource, which closes it once
// go-nsq has returned.
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

	started := srcs[:0]
	for _, s := range srcs {
		if s.startClose() {
			started = append(started, s)
		}
	}
	stopped := b.stopBy()
	for _, s := range started {
		s.finishClose(stopped)
	}
	var first error
	for _, s := range sinks {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	// The administrative transport is this backend's own (newAdminClient),
	// so the connections it pooled are this backend's to release. A request
	// still in flight releases its own on the way out (do).
	b.http.CloseIdleConnections()
	b.health.Close()
	return first
}

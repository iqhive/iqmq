package pulsar

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"bitbucket.org/iqhive/iqmq/wire"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the Pulsar backend.
type Backend struct {
	*driverkit.BaseBackend
	cfg    Config
	cl     pulsarapi.Client
	errs   *driverkit.ErrorMapper
	caps   mq.Capabilities
	limit  *wire.Limits
	http   *http.Client
	health *driverkit.Health
	counters

	// mu guards the producer cache and the open children. A backend closes
	// what it opened, which is the obligation and the reason a parked
	// Fetch is woken rather than left holding a goroutine after Close.
	mu        sync.Mutex
	producers map[string]*producerEntry
	srcs      map[*source]struct{}
	sinks     map[*sink]struct{}
	pingTopic string
	closed    bool

	// broker is what the administrative API said about the broker at open,
	// which is the only place this driver can learn any of it.
	broker brokerFacts
}

// producerEntry is one cached producer and how many sinks are using it.
//
// The specification asks for "producer per topic cached", and the reason is Pulsar's own
// cost model: creating a producer is a lookup plus a connect plus a broker-side
// registration, and two publishers on one topic have no reason to pay for it
// twice. The refcount is what lets the LAST sink on a topic close the producer
// and an earlier one not close it underneath its sibling.
type producerEntry struct {
	prod pulsarapi.Producer
	refs int
}

// newBackend builds the backend around an already-constructed client.
func newBackend(ctx context.Context, cfg Config, cl pulsarapi.Client, own driverkit.Ownership, native any) (_ *Backend, err error) {
	r := cfg.resolve()
	b := &Backend{
		cfg:       r,
		cl:        cl,
		errs:      newErrorMapper(),
		http:      newAdminClient(r.OperationTimeout),
		producers: map[string]*producerEntry{},
		srcs:      map[*source]struct{}{},
		sinks:     map[*sink]struct{}{},
	}
	// An Open refused below has already asked the broker for its facts, and
	// the connection that answered is pooled in a transport no caller can
	// reach: teardown only runs for a backend that was returned. Every
	// refused Open would leave one for the idle timeout.
	defer func() {
		if err != nil {
			b.http.CloseIdleConnections()
		}
	}()
	b.health = driverkit.NewHealth(nil)
	// What the broker itself says, when there is an administrative endpoint
	// to ask. It is a real lookup rather than a set of constants, because
	// the limits are claims and a claim nobody read from the broker is a
	// number this driver made up.
	facts, ok := b.lookupBrokerFacts(ctx)
	if ok {
		b.broker = facts
		if facts.maxMessageSize > 0 {
			b.cfg.MaxMessageBytes = facts.maxMessageSize
		}
	}
	if b.cfg.EnableBatchIndexAck && !b.broker.batchIndexAck {
		// Refusing at open rather than at every settlement, because the
		// failure this prevents is silent: a broker that ignores an
		// acknowledgement set settles the whole batched entry and tells
		// nobody.
		return nil, errBrokerRefusesBatchIndexAck
	}
	caps, err := capabilities(b.cfg)
	if err != nil {
		return nil, err
	}
	b.caps = caps
	b.limit = envelopeLimits(b.cfg)
	if native == nil {
		native = cl
	}
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       native,
		Ownership:    own,
		CloseNative:  func() error { cl.Close(); return nil },
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
// It is the decoder's own default set, narrowed only by what Pulsar will carry:
// a body larger than the broker's maxMessageSize cannot be published at all, so
// an envelope larger than it is a record that could never exist. Handing the
// same value to the encoder's check and the decoder is what keeps a publication
// this driver accepts decodable by the consumer that reads it — the defect
// class the maintenance notes records three drivers falling into, each through a
// second set of numbers that had drifted.
func envelopeLimits(r Config) *wire.Limits {
	lim := wire.DefaultLimits
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
		Profile:       b.cfg.SubscriptionType.String(),
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
		// BrokerVersion is left empty rather than guessed.
		// pulsar-client-go learns the broker's version in the connection
		// handshake and surfaces it nowhere, and the specification makes an empty
		// string "the driver did not say" — which is the truth until the
		// administrative API is asked.
		BrokerVersion: b.brokerVersion(),
	}
}

// Capabilities is the table, constant for the life of the backend.
func (b *Backend) Capabilities() mq.Capabilities { return b.caps }

// now reads the injected clock, or the wall clock when there is none.
func (b *Backend) now() time.Time {
	if b.cfg.Clock != nil {
		return b.cfg.Clock.Now()
	}
	return time.Now()
}

// newTimer is a timer on the injected clock, or on the wall clock when there is
// none.
func (b *Backend) newTimer(d time.Duration) mq.Timer {
	if b.cfg.Clock != nil {
		return b.cfg.Clock.NewTimer(d)
	}
	return wallTimer{t: time.NewTimer(d)}
}

// wallTimer adapts a *time.Timer to mq.Timer, which is what a backend with no
// injected clock uses.
type wallTimer struct{ t *time.Timer }

// C is the timer's channel.
func (w wallTimer) C() <-chan time.Time { return w.t.C }

// Stop stops the timer.
func (w wallTimer) Stop() bool { return w.t.Stop() }

// Reset restarts the timer.
func (w wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// Ping is the cheap liveness check. It never consumes a message and never
// creates topology.
//
// Which probe it uses depends on what the configuration gave it, and the order
// is about topology rather than cost:
//
//   - with an administrative endpoint, the broker's own health endpoint, which
//     touches no topic at all.
//   - without one, a partitioned-topic METADATA lookup for a topic this backend
//     already has a handle on — so the topic certainly exists and the lookup
//     creates nothing. [Config.PingTopic] names one for a backend that has
//     opened no handle yet.
//
// A backend with neither reports ErrUnsupported rather than looking up a topic
// nobody created: on a namespace with automatic creation enabled, a metadata
// lookup for a topic that does not exist is exactly the hidden topology
// creation the specification forbids.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	degraded := b.health.State() == mq.HealthDegraded
	err := b.probe(ctx)
	if err != nil {
		b.degrade(err)
		return err
	}
	if degraded {
		// A successful probe after a failure is the only reconnection
		// this driver can observe: pulsar-client-go reconnects inside its
		// own connection pool and offers no callback for it, so the specification's
		// OnReconnect has no other source here.
		b.recovered(ctx)
		return nil
	}
	b.health.Ready()
	return nil
}

// probe is Ping's one round trip.
func (b *Backend) probe(ctx context.Context) error {
	if b.cfg.AdminURL != "" {
		return b.brokerHealth(ctx)
	}
	topic := b.pingTarget()
	if topic == "" {
		return errNoPingTarget
	}
	if _, err := b.cl.TopicPartitions(topic); err != nil {
		return b.errs.Map(mq.OpOpen, topic, "", err)
	}
	return nil
}

// pingTarget is the topic Ping looks up: the configured one, or the first this
// backend opened a handle on.
func (b *Backend) pingTarget() string {
	if b.cfg.PingTopic != "" {
		if t, err := TopicOf(b.cfg, b.cfg.PingTopic); err == nil {
			return t.String()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pingTopic
}

// producer returns the cached producer for a topic, creating it if this is the
// first sink on it, and takes a reference.
//
// The client call happens OUTSIDE the mutex, and the loser of a race closes its
// own producer rather than replacing the winner's. Holding the lock across the
// creation would make a slow lookup block every other open and close on the
// backend — including the Close that is trying to tear the backend down, which
// The specification requires to be prompt.
func (b *Backend) producer(ctx context.Context, topic TopicName) (pulsarapi.Producer, error) {
	name := topic.String()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, mq.ErrClosed
	}
	if e := b.producers[name]; e != nil {
		e.refs++
		b.mu.Unlock()
		return e.prod, nil
	}
	b.mu.Unlock()

	prod, err := b.cl.CreateProducer(b.producerOptions(name))
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, topic.Local, "", err)
	}
	_ = ctx // the client's own OperationTimeout bounds the creation; pulsar-client-go takes no context here

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		prod.Close()
		return nil, mq.ErrClosed
	}
	if e := b.producers[name]; e != nil {
		// Another sink on the same topic won the race. Its producer is
		// the cached one; ours is closed rather than leaked, and the
		// reference is taken on the winner.
		prod.Close()
		e.refs++
		return e.prod, nil
	}
	b.producers[name] = &producerEntry{prod: prod, refs: 1}
	return prod, nil
}

// releaseProducer drops one sink's reference and closes the producer when the
// last one goes.
func (b *Backend) releaseProducer(name string) {
	b.mu.Lock()
	e := b.producers[name]
	if e == nil {
		b.mu.Unlock()
		return
	}
	e.refs--
	if e.refs > 0 {
		b.mu.Unlock()
		return
	}
	delete(b.producers, name)
	b.mu.Unlock()
	// Outside the mutex: Producer.Close flushes what is pending, which is a
	// round trip.
	e.prod.Close()
}

// producerOptions is the pulsar-client-go producer configuration for one topic.
func (b *Backend) producerOptions(topic string) pulsar.ProducerOptions {
	o := pulsar.ProducerOptions{
		Topic:                           topic,
		Name:                            b.cfg.ProducerName,
		Properties:                      b.cfg.ProducerProperties,
		SendTimeout:                     b.cfg.SendTimeout,
		DisableBlockIfQueueFull:         !b.cfg.BlockIfQueueFull,
		MaxPendingMessages:              b.cfg.MaxPendingMessages,
		DisableBatching:                 b.cfg.DisableBatching,
		BatchingMaxPublishDelay:         b.cfg.BatchingMaxPublishDelay,
		BatchingMaxMessages:             uint(b.cfg.BatchingMaxMessages),
		BatchingMaxSize:                 uint(b.cfg.BatchingMaxSize),
		PartitionsAutoDiscoveryInterval: b.cfg.AutoDiscoveryPeriod,
		CompressionType:                 b.cfg.Compression.Native(),
		HashingScheme:                   b.cfg.Hashing.Native(),
	}
	if b.cfg.SubscriptionType == SubscriptionKeyShared {
		// The KEY-BASED batcher, and this is a correctness requirement
		// rather than a tuning choice.
		//
		// Pulsar dispatches an ENTRY to one consumer, and with the
		// default batcher one entry holds whatever messages arrived
		// within BatchingMaxPublishDelay of each other — which is
		// several keys. A Key_Shared subscription would then have to send
		// an entry holding keys belonging to two different consumers to
		// one of them, and the per-key ordering this driver claims for
		// Key_Shared would be false for every key that shared an entry
		// with another. The key-based batcher puts one key in one entry,
		// which is what makes the claim true.
		o.BatcherBuilderType = pulsar.KeyBasedBatchBuilder
	}
	return o
}

// OpenSink opens a publication channel to one topic.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	topic, err := TopicOf(b.cfg, cfg.Topic)
	if err != nil {
		return nil, err
	}
	if cfg.Confirmation.Resolve() == mq.ConfirmDurable && !topic.Persistent() {
		// A non-persistent topic's send receipt is given by a broker
		// that has the message in memory and will never write it down.
		// The specification requires a publisher asking for a durable boundary to
		// fail at construction rather than be given a weaker one
		// silently.
		return nil, errNoDurableConfirm
	}
	if !cfg.Metadata.Valid() {
		return nil, errMetadataMode
	}
	mode := b.metadataMode(cfg.Metadata)
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{Mode: mode, Limits: b.limit, Capabilities: b.caps})
	if err != nil {
		return nil, err
	}
	forced, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode: mq.MetadataPortableEnvelope, Limits: b.limit, Capabilities: b.caps,
	})
	if err != nil {
		return nil, err
	}
	if b.cfg.EnsureTopology {
		if err := b.ensureTopic(ctx, topic); err != nil {
			return nil, err
		}
	}
	prod, err := b.producer(ctx, topic)
	if err != nil {
		return nil, err
	}
	s := &sink{
		b: b, prod: prod, topic: topic, name: topic.String(), iqTopic: cfg.Topic,
		meta: mode, env: env, forced: forced, receipts: cfg.Receipts,
		confirm: cfg.Confirmation.Resolve(),
		props:   make(map[string]string, 8),
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.releaseProducer(s.name)
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	if b.pingTopic == "" {
		b.pingTopic = s.name
	}
	b.mu.Unlock()
	return s, nil
}

// Admin returns the topology surface, which for Pulsar is its HTTP
// administrative API.
func (b *Backend) Admin() (mq.Admin, error) {
	if b.cfg.AdminURL == "" {
		return nil, errNoAdminURL
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

// teardown is what NewBase calls on Close: close every child, then every cached
// producer, and then the client.
//
// The children are collected under the mutex and closed outside it, because a
// source's Close gives back what it still holds and that is a round trip to the
// broker — holding the backend's mutex across it would make a slow broker block
// every other open and close on the backend.
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
	prods := make([]pulsarapi.Producer, 0, len(b.producers))
	for _, e := range b.producers {
		prods = append(prods, e.prod)
	}
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	b.producers = map[string]*producerEntry{}
	b.mu.Unlock()

	var first error
	for _, s := range srcs {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, s := range sinks {
		// The sink's own Close would release a producer this teardown has
		// already taken out of the cache, so it is told not to.
		if err := s.close(false); err != nil && first == nil {
			first = err
		}
	}
	for _, p := range prods {
		p.Close()
	}
	// The administrative transport is this backend's own (newAdminClient),
	// so the connections it pooled are this backend's to release. A request
	// still in flight releases its own on the way out (admin.do,
	// ).
	b.http.CloseIdleConnections()
	b.health.Close()
	return first
}

package pulsar

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/wire"
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
	// pendings holds the publications' scratch messages; see [pending].
	pendings sync.Pool
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
	// renewals counts the producer replacements under way ([Backend.replace]).
	// One is added under mu while the backend is open ([Backend.current]), so
	// none is added once teardown has set closed, and teardown waits for them.
	renewals sync.WaitGroup
	// retiring is the producers a replacement has taken out of use and is
	// waiting to close ([liveProducer.retire]). Teardown closes them with the
	// cached ones, which fails the sends still in flight on them and so ends
	// the wait.
	retiring map[*liveProducer]struct{}

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
//
// The entry outlives the producer in it. pulsar-client-go reconnects a
// producer on its own, except when the broker refuses the reconnection with
// TopicNotFound, TopicTerminated or ProducerFenced: then it closes the
// producer for good and every later Send fails with ProducerClosed. The entry
// used to keep that producer, so every sink on the topic, and every sink opened
// on it while one stayed open, failed every publication. Now a publication
// that sees the close marks the producer gone, and the next one replaces it
// for every sink at once (see [Backend.current]).
type producerEntry struct {
	// cur is the producer the sinks publish through. It is read on every
	// publication without the backend's mutex and replaced under it.
	cur atomic.Pointer[liveProducer]
	// name is the topic's full name, which a replacement is created for.
	name string
	// local is the topic's name as the caller gave it, for the errors.
	local string

	// The rest is guarded by Backend.mu.
	refs int
	// renewing is closed when the replacement under way ends, and nil when
	// there is none, so the publications waiting for one share it.
	renewing chan struct{}
	// refused is why the last replacement failed, and nil after one that
	// succeeded.
	refused error
}

// liveProducer is one producer and whether the client has closed it for good.
type liveProducer struct {
	prod pulsarapi.Producer
	gone atomic.Bool

	// mu guards retired and the admission of a send, for the reason sink.mu
	// gives: sends counts the sends admitted on prod and not yet finished,
	// and an Add must not race the Wait in retire.
	mu      sync.Mutex
	retired bool
	sends   sync.WaitGroup
}

// use admits one send on h, which its caller ends with [liveProducer.done]
// once the send has finished. It refuses once a replacement has retired h.
func (h *liveProducer) use() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.retired {
		return false
	}
	h.sends.Add(1)
	return true
}

// done ends a send [liveProducer.use] admitted.
func (h *liveProducer) done() { h.sends.Done() }

// retire refuses every later send on h and waits for the ones it admitted,
// so that the producer it replaces is closed only once nothing is sending on
// it.
//
// A producer the client closed for good on one partition of a partitioned
// topic still has its other partition producers, and other sinks can have
// sends in flight on them. Closing it fails every message it still holds
// (pulsar-client-go v0.21, doClose), so a sink whose partition was healthy
// had its publication fail ambiguously, and a caller that retried it
// published it twice. The wait ends with each send's receipt, its
// SendTimeout, or the backend's Close, which closes a retiring producer with
// the cached ones and so fails what it still holds.
func (h *liveProducer) retire() {
	h.mu.Lock()
	h.retired = true
	h.mu.Unlock()
	h.sends.Wait()
}

// lost marks the producer gone when err is one of the answers pulsar-client-go
// v0.21 gives only once it has closed the producer for good: the reason it
// closed (reconnectToBroker, doClose), which the message waiting for its
// receipt carries, and ProducerClosed, which every publication after it gets.
//
// The backend's own Close of a producer gives ProducerClosed too, but only to
// a publication racing the last sink's Close or the backend's, and both have
// taken the entry out of use by then. A nil err, a publication that succeeded,
// changes nothing.
func (h *liveProducer) lost(err error) {
	if closedForGood(err) {
		h.gone.Store(true)
	}
}

// closedForGood matches the answers [liveProducer.lost] reads as the client
// having closed a producer for good.
var closedForGood = matchResult(pulsar.ProducerClosed, pulsar.TopicNotFound, pulsar.TopicTerminated, pulsar.ProducerFenced)

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
		retiring:  map[*liveProducer]struct{}{},
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
		Profile:       b.cfg.SubscriptionType.String(),
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
		// BrokerVersion is left empty rather than guessed.
		// pulsar-client-go learns the broker's version in the connection
		// handshake and surfaces it nowhere, and the specification makes an empty
		// string "the driver did not say" — which is the truth until the
		// administrative API is asked, at /admin/v2/brokers/version.
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
// creation the specification forbids. That answer, and a probe its caller
// cancelled, leave health as it was.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	err := b.probe(ctx)
	if err != nil {
		if !beforeTheBroker(err) {
			b.lost.Store(true)
			b.degrade(err)
		}
		return err
	}
	if b.lost.Swap(false) {
		// A successful probe after the broker or the link to it failed is
		// the only reconnection this driver can observe: pulsar-client-go
		// reconnects inside its own connection pool and offers no callback
		// for it, so the specification's OnReconnect has no other source
		// here. A probe that succeeds after some other degradation, such
		// as an undecodable delivery, restores health and is no
		// reconnection.
		b.recovered(ctx)
		return nil
	}
	b.health.Ready()
	return nil
}

// beforeTheBroker reports whether a Ping failed for a reason that says nothing
// about the broker: there was nothing to probe (ErrUnsupported, which is also
// a web service without the health endpoint), or the caller gave up on the
// probe. Such a failure is a fact about the configuration or the caller, and
// moving health for it made the next successful Ping read as a reconnection
// that never happened.
func beforeTheBroker(err error) bool {
	return errors.Is(err, mq.ErrUnsupported) || errors.Is(err, context.Canceled)
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
//
// A cached producer the client has closed for good is replaced before the
// sink is handed it, so the open is refused with what a fresh creation
// answers rather than handed a producer that fails every publication.
func (b *Backend) producer(ctx context.Context, topic TopicName) (*producerEntry, error) {
	name := topic.String()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, mq.ErrClosed
	}
	if e := b.producers[name]; e != nil {
		e.refs++
		b.mu.Unlock()
		if _, err := b.current(ctx, e); err != nil {
			b.releaseProducer(ctx, name)
			return nil, err
		}
		return e, nil
	}
	b.mu.Unlock()

	prod, err := b.cl.CreateProducer(b.producerOptions(name))
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, topic.Local, "", err)
	}
	_ = ctx // the client's own OperationTimeout bounds the creation; pulsar-client-go takes no context here

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		prod.Close()
		return nil, mq.ErrClosed
	}
	if e := b.producers[name]; e != nil {
		// Another sink on the same topic won the race. Its producer is
		// the cached one and ours is closed rather than leaked. One the
		// client has closed for good is replaced as on the cached path
		// above, by [Backend.current]. Making ours the replacement here
		// instead meant retiring the winner's on the caller's goroutine,
		// which waited for other sinks' sends past this open's ctx, and
		// out of the sight of a Close.
		e.refs++
		b.mu.Unlock()
		prod.Close()
		if _, err := b.current(ctx, e); err != nil {
			b.releaseProducer(ctx, name)
			return nil, err
		}
		return e, nil
	}
	e := &producerEntry{name: name, local: topic.Local, refs: 1}
	e.cur.Store(&liveProducer{prod: prod})
	b.producers[name] = e
	b.mu.Unlock()
	return e, nil
}

// live is the producer a publication on e sends through, as [Backend.current]
// finds it, admitted for one send ([liveProducer.use]). The caller ends the
// send with [liveProducer.done] once it has finished, whatever its outcome.
//
// A producer is retired only after its replacement is in the entry, so one
// found retired here has been replaced, and the next look finds what replaced
// it.
func (b *Backend) live(ctx context.Context, e *producerEntry) (*liveProducer, error) {
	for {
		h, err := b.current(ctx, e)
		if err != nil {
			return nil, err
		}
		if h.use() {
			return h, nil
		}
	}
}

// current is the producer a publication on e goes through: the cached one, or
// its replacement when the client has closed that one for good.
//
// The replacement is created once for every sink on the topic, on a goroutine
// of its own, and each publication waits for it no longer than its own ctx:
// pulsar-client-go's CreateProducer takes no context, so a publication that
// called it would outlive its deadline by up to OperationTimeout. Nothing has
// been sent when it gives up, so that answer is definite. A refused creation
// is the answer too, mapped as the open it is (a terminated topic is a
// definite Conflict), and the next publication asks again.
func (b *Backend) current(ctx context.Context, e *producerEntry) (*liveProducer, error) {
	if h := e.cur.Load(); !h.gone.Load() {
		return h, nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, mq.ErrClosed
	}
	if h := e.cur.Load(); !h.gone.Load() {
		b.mu.Unlock()
		return h, nil
	}
	done := e.renewing
	if done == nil {
		done = make(chan struct{})
		e.renewing = done
		b.renewals.Add(1)
		go b.replace(e, done)
	}
	b.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	h := e.cur.Load()
	if h.gone.Load() && e.refused != nil {
		return nil, e.refused
	}
	// A replacement the client has closed again fails this publication on
	// its own, and that failure marks it gone in turn.
	return h, nil
}

// replace creates the producer that takes the place of one the client closed
// for good, and then closes the closed one once the sends in flight on it have
// finished ([liveProducer.retire]).
//
// A backend that has closed meanwhile, or an entry whose last sink has gone,
// has nobody to hand the new producer to, so it is closed at once: teardown or
// releaseProducer has already closed the one the entry held.
//
// Its owner is the backend: [Backend.current] counts it in renewals, and
// teardown waits for it before the client is closed. A Close that returned
// first left it calling CreateProducer on a client the backend had closed, or
// creating a producer after Close had returned. The producer it retires is
// recorded in retiring while it waits, so that teardown can close it and end
// the wait.
//
// A creation that fails once the backend is closing does not degrade its
// health: nothing will use the producer, and a closed backend's health is
// Closed.
func (b *Backend) replace(e *producerEntry, done chan struct{}) {
	defer b.renewals.Done()
	prod, err := b.cl.CreateProducer(b.producerOptions(e.name))
	b.mu.Lock()
	var (
		stale   pulsarapi.Producer
		retired *liveProducer
	)
	switch {
	case err != nil:
		e.refused = b.errs.Map(mq.OpOpen, e.local, "", err)
	case b.closed || e.refs == 0:
		stale, e.refused = prod, mq.ErrClosed
	default:
		retired = e.cur.Load()
		e.cur.Store(&liveProducer{prod: prod})
		e.refused = nil
		b.retiring[retired] = struct{}{}
	}
	refused, closing := e.refused, b.closed
	e.renewing = nil
	close(done)
	b.mu.Unlock()
	if !closing && mq.CodeOf(refused) == mq.CodeUnavailable {
		b.degrade(refused)
	}
	if retired != nil {
		retired.retire()
		b.mu.Lock()
		delete(b.retiring, retired)
		b.mu.Unlock()
		stale = retired.prod
	}
	if stale != nil {
		// Outside the mutex, because closing a producer is a round trip:
		// each partition producer the client left open sends
		// CLOSE_PRODUCER and fails what it still holds.
		stale.Close()
	}
}

// releaseProducer drops one sink's reference and, when the last one goes,
// flushes the producer under ctx and at most OperationTimeout, and closes it.
func (b *Backend) releaseProducer(ctx context.Context, name string) {
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
	prod := e.cur.Load().prod
	b.mu.Unlock()
	// Outside the mutex: the flush and the close are round trips.
	b.flush(ctx, prod)
	prod.Close()
}

// flush hands the broker what the client still holds for prods, in its send
// queue and in each producer's batch, and waits for the answers, until ctx
// ends and for at most OperationTimeout across all of them. It is what a close
// does first.
//
// pulsar-client-go v0.21's Producer.Close fails only the messages waiting for
// their receipt (doClose, failPendingMessages). A message still queued, or in
// a batch the batching delay has not flushed yet, keeps its callback where the
// close never looks, so it never gets one: a PublishAsync's done never ran and
// a synchronous Send waited for its context alone. After the flush everything
// is either answered or waiting for its receipt, which the close fails as
// ambiguous. A flush that fails or runs out of time leaves the rest to that
// close.
func (b *Backend) flush(ctx context.Context, prods ...pulsarapi.Producer) {
	ctx, cancel := context.WithTimeout(ctx, b.cfg.OperationTimeout)
	defer cancel()
	for _, p := range prods {
		_ = p.FlushWithCtx(ctx)
	}
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
	entry, err := b.producer(ctx, topic)
	if err != nil {
		return nil, err
	}
	s := &sink{
		b: b, entry: entry, topic: topic, name: topic.String(), iqTopic: cfg.Topic,
		meta: mode, env: env, forced: forced, receipts: cfg.Receipts,
		confirm: cfg.Confirmation.Resolve(),
		props:   make(map[string]string, 8),
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.releaseProducer(ctx, s.name)
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

// teardown is what NewBase calls on Close: close every source, stop every sink,
// flush and close every cached producer and every producer a replacement is
// retiring, wait for the sinks' callbacks and for any producer replacement under
// way, and then the client.
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
	prods := make([]pulsarapi.Producer, 0, len(b.producers)+len(b.retiring))
	for _, e := range b.producers {
		prods = append(prods, e.cur.Load().prod)
	}
	// A producer a replacement is retiring waits for the sends in flight on
	// it, and with SendTimeout disabled a synchronous one waits for its
	// caller's context alone. Closing it here fails them, ambiguously, as
	// closing a cached producer fails its own, which ends the replacement's
	// wait. The replacement's own Close of it afterwards does nothing.
	for h := range b.retiring {
		prods = append(prods, h.prod)
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
	// The sinks stop admitting first, and every publication they admitted is
	// waited for until it has returned from SendAsync; the producers are
	// flushed and closed next, and only then does anything wait for the
	// asynchronous publications' callbacks. Waiting for the callbacks before
	// the close waited for every receipt, for ever with SendTimeout
	// disabled, because only the close fails what never gets one; closing
	// before the flush lost the callbacks of what the client was still
	// batching ([Backend.flush]); and a flush and a close with a SendAsync
	// still on its way in either dropped that publication's callback or
	// panicked it on the producer's closed queue ([sink.handing]).
	for _, s := range sinks {
		s.stop()
	}
	for _, s := range sinks {
		s.handing.Wait()
	}
	b.flush(context.Background(), prods...)
	for _, p := range prods {
		p.Close()
	}
	// Every sink, including one whose own Close got to it first, so that no
	// callback runs after Close has returned.
	for _, s := range sinks {
		s.inflight.Wait()
	}
	// A producer replacement under way is either inside CreateProducer, which
	// takes no context and is bounded by OperationTimeout, or waiting for the
	// sends on the producer it retired, which the loop above has just failed.
	// It has to end before the client is closed under it, and it closes what
	// it created, because the backend is closed. No new one can start, and
	// none can retire a producer after the snapshot above: both happen only
	// under mu while closed is false.
	b.renewals.Wait()
	// The administrative transport is this backend's own (newAdminClient),
	// so the connections it pooled are this backend's to release. A request
	// still in flight releases its own on the way out (admin.do,
	// ).
	b.http.CloseIdleConnections()
	b.health.Close()
	return first
}

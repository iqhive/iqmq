package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the Kafka [github.com/iqhive/iqmq.Backend].
//
// It owns the PRODUCER client and every source it opens owns a client of its
// own. That split is not a style choice: a franz-go client consumes at most
// one group, so a backend that shared one client between the producer and a
// consumer group would tie the producer's lifetime and its metadata refreshes
// to one subscription, and a second subscription would be impossible. What is
// shared is the seed brokers, the TLS and SASL configuration and the
// administrative client.
type Backend struct {
	*driverkit.BaseBackend
	cfg   Config
	errs  *driverkit.ErrorMapper
	cl    *kgo.Client
	adm   *kadm.Client
	caps  mq.Capabilities
	env   *driverkit.Envelope
	part  kgo.Partitioner
	clock mq.Clock
	counters
	// recbufs holds the record buffers. The specification's ownership rules
	// are what make pooling safe here: a buffer goes back only after the
	// promise has run, which is the moment franz-go stops reading the record.
	recbufs sync.Pool

	// health is the reporter, held separately from BaseBackend's copy so
	// that the client's own hooks can drive it before the base exists: a
	// dial that fails has to report Degraded and there is no base yet.
	health *driverkit.Health

	// mu guards the open children. A backend closes what it opened, which
	// is the obligation and the reason a parked Fetch is woken rather than
	// left holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
	// moves is the producer that dead-letter and retry-topic copies go
	// through when cl is transactional, built on first use under mu. See
	// [Backend.mover].
	moves *kgo.Client
}

// counters is the backend's background bookkeeping, split out so that the
// struct literal stays about the client.
type counters struct {
	reconnects atomic.Uint64
	degraded   atomic.Uint64
	rebalances atomic.Uint64
	lost       atomic.Uint64
	commits    atomic.Uint64
	drops      atomic.Uint64
	ambiguous  atomic.Uint64
}

// init finishes construction once a client exists.
func (b *Backend) init(ctx context.Context, cl *kgo.Client, own driverkit.Ownership) error {
	b.cl = cl
	b.adm = kadm.NewClient(cl)
	b.clock = b.cfg.Clock
	b.part = Partitioner()
	// The one limit the cluster really knows. A failure to read it is not a
	// failure to open: the specification makes an unadvertised limit unknown rather than
	// zero, and a cluster whose ACLs forbid DescribeConfigs is a cluster
	// this driver still works against.
	maxBytes := b.messageMaxBytes(ctx)
	caps, err := capabilities(b.cfg, maxBytes)
	if err != nil {
		return err
	}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         mq.MetadataPortableEnvelope,
		Capabilities: caps,
	})
	if err != nil {
		return err
	}
	b.caps, b.env = caps, env
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       cl,
		Ownership:    own,
		CloseNative:  func() error { cl.Close(); return nil },
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return err
	}
	b.BaseBackend = base
	b.health.Ready()
	return nil
}

// messageMaxBytes asks the cluster for its own record-size bound, and returns
// zero when it will not say.
func (b *Backend) messageMaxBytes(ctx context.Context) int64 {
	rctx, cancel := b.bound(ctx)
	defer cancel()
	// The broker id has to be named. With no id kadm
	// asks for the empty resource name, which is the CLUSTER-level view and
	// carries only the configuration someone set DYNAMICALLY: on
	// apache/kafka:4.3.1 that answer is one entry, min.insync.replicas, and
	// message.max.bytes -- whose source is DEFAULT_CONFIG -- is not in it at
	// all. Naming a broker asks for that broker's own EFFECTIVE
	// configuration, defaults included, and that answer has it.
	md, err := b.adm.BrokerMetadata(rctx)
	if err != nil || len(md.Brokers) == 0 {
		return 0
	}
	resources, err := b.adm.DescribeBrokerConfigs(rctx, md.Brokers[0].NodeID)
	if err != nil {
		return 0
	}
	for _, r := range resources {
		if r.Err != nil {
			continue
		}
		for _, c := range r.Configs {
			if c.Key != "message.max.bytes" || c.Value == nil {
				continue
			}
			if n := parseInt64(*c.Value); n > 0 {
				return n
			}
		}
	}
	return 0
}

// info describes the driver and the library behind it. Every field the
// driver does not know is empty, which the specification makes "the driver did not say"
// rather than a claim.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       "franz-go",
		DriverVersion: Version,
		ClientName:    clientModule,
		ClientVersion: clientVersion,
		// BrokerVersion stays empty: Kafka reports supported API versions
		// rather than a version string, so the honest answer is that the
		// driver does not know.
	}
}

// clientVersion is the client library's version, held here so that the
// capability table does not import the client for one constant.
const clientVersion = "v1.22.2"

// clientModule is that library's module path. It is held beside the version
// so the pair has ONE source in this package: it used to be a literal in both
// info() and ConformanceHarness, which is two places for a bump to miss.
const clientModule = "github.com/twmb/franz-go"

// Ping is the liveness check a metadata round trip to one broker,
// which consumes no message and creates no topology.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	rctx, cancel := b.bound(ctx)
	defer cancel()
	if err := b.cl.Ping(rctx); err != nil {
		return b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	return nil
}

// Client returns the underlying producer client, which is this driver's escape
// hatch under a name a caller can find: client.Native().(*kgo.Client) is the
// SPI's route and this is the typed one. It is also what
// github.com/iqhive/iqmq/x/kafkatx drives a transaction through.
func (b *Backend) Client() *kgo.Client { return b.cl }

// Admin returns the topology surface.
func (b *Backend) Admin() (mq.Admin, error) {
	if b.Closed() {
		return nil, mq.ErrClosed
	}
	return &admin{b: b}, nil
}

// Commit commits the watermark of every open source synchronously, which
// narrows the at-least-once window to zero at a point of the caller's own
// choosing.
//
// It is the answer to the question the ticker leaves open: the driver commits
// every Config.CommitInterval, so a process that dies in between redelivers
// what it had completed. A caller that cannot tolerate that calls this — after
// a batch, at a checkpoint — and pays one round trip per source for it.
func (b *Backend) Commit(ctx context.Context) error {
	b.mu.Lock()
	srcs := make([]*source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	b.mu.Unlock()
	var first error
	for _, s := range srcs {
		if err := s.commitNow(ctx); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// OpenSink opens a publication channel to one topic.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	meta, err := metaOf(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	topic, err := Topic(b.cfg.TopicPrefix, cfg.Topic)
	if err != nil {
		return nil, err
	}
	s := &sink{
		b:        b,
		topic:    cfg.Topic,
		name:     topic,
		meta:     meta,
		confirm:  cfg.Confirmation.Resolve(),
		receipts: cfg.Receipts,
		closed:   make(chan struct{}),
	}
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
// The subscription name becomes the consumer group, which is what makes two
// consumers of one subscription share the partitions and two consumers of
// different subscriptions each receive everything. It is required:
// Kafka has no anonymous group, and a driver that invented one would give two
// processes the same durable position by accident.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	meta, err := metaOf(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	topic, err := Topic(b.cfg.TopicPrefix, cfg.Topic)
	if err != nil {
		return nil, err
	}
	if err := CheckGroup(cfg.Subscription); err != nil {
		return nil, err
	}
	src, err := b.newSource(ctx, cfg, topic, meta)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		// The specification gives Close no context, so there is none to pass on: a
		// prompt close is not an operation a caller cancels, and
		// Shutdown is the bounded form. It is the same finding testkit
		// records.
		_ = src.Close() //nolint:contextcheck // the specification gives Close no context
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	return src, nil
}

// CheckGroup reports whether name is a consumer-group name Kafka accepts.
func CheckGroup(name string) error {
	if name == "" {
		return errNoSubscription
	}
	if len(name) > 255 {
		return errBadGroup
	}
	for i := range len(name) {
		if c := name[i]; c <= ' ' || c >= 0x7f {
			return errBadGroup
		}
	}
	return nil
}

// forget drops a closed child.
func (b *Backend) forget(src *source, s *sink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if src != nil {
		delete(b.srcs, src)
	}
	if s != nil {
		delete(b.sinks, s)
	}
}

// teardown closes everything this backend opened, before the client goes.
//
// It is what wakes a Fetch that is parked in a source nobody closed: the specification's
// "Close unblocks blocked operations" has to reach a caller who closed the
// client rather than the consumer.
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
	b.mu.Unlock()
	var first error
	// Close takes no context, which is the specification's own rule — a prompt close is
	// not an operation a caller cancels, and Shutdown is the bounded form —
	// so there is no context to pass on to a child's Close. It is the same
	// finding testkit records.
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
	// After the sources, which are the only callers of the mover.
	b.mu.Lock()
	moves := b.moves
	b.moves = nil
	b.mu.Unlock()
	if moves != nil {
		moves.Close()
	}
	return first
}

// mover returns the client a record moved to a retry or dead-letter topic is
// produced through.
//
// That is the producer itself unless the producer is transactional. A
// transactional franz-go client fails every record produced outside a
// transaction, so a move would never succeed while no transaction was open;
// and a move produced while the application's own x/kafkatx transaction was
// open would join it, so an abort would discard the copy after the original's
// offset had been completed, which loses the record. A move is the source's
// settlement and belongs to no transaction, so it gets a non-transactional
// producer of its own, with the same brokers and security as the sources.
func (b *Backend) mover() (*kgo.Client, error) {
	if !transactional(b.cl) {
		return b.cl, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, mq.ErrClosed
	}
	if b.moves != nil {
		return b.moves, nil
	}
	cfg := b.cfg
	cfg.TransactionalID = ""
	opts, err := b.producerOptions(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	b.moves = cl
	return cl, nil
}

// transactional reports whether cl was built with a transactional id, which
// is the only way to know for a client the caller passed in.
func transactional(cl *kgo.Client) bool {
	v := cl.OptValues(kgo.TransactionalID)
	return len(v) == 2 && v[1] == true
}

// bound gives a driver-level round trip a driver-level deadline when the
// caller gave none, and leaves a caller's own deadline alone.
//
// It is never a second deadline on a caller who has one: a commit or a
// metadata request with no bound at all waits for ever on a link that is up
// and silent, and that is the only case this covers.
func (b *Backend) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || b.cfg.RequestTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, b.cfg.RequestTimeout)
}

// now reads the driver's clock, which is the injected one when the caller gave
// us one and the wall clock otherwise.
func (b *Backend) now() time.Time {
	if b.clock != nil {
		return b.clock.Now()
	}
	return time.Now()
}

// metadataMode is what this driver does with a message's metadata.
type metadataMode uint8

const (
	// metaStrict maps every field to a native Kafka field or a reserved
	// header and refuses anything that would not come back byte for byte.
	metaStrict metadataMode = iota
	// metaDrop leaves out what it cannot carry.
	metaDrop
	// metaEnvelope puts the whole message in the portable binary envelope.
	metaEnvelope
	// metaAdaptive uses the native mapping where it suffices and the
	// envelope where it does not.
	metaAdaptive
)

// metaOf maps the four modes onto this driver's.
//
// Three of the four behave identically here, and that is worth stating rather
// than hiding: Kafka's headers are ordered, binary and duplicate-preserving
// and its key and value are nullable byte strings, so there is no metadata
// this broker cannot represent. Strict refuses nothing, adaptive envelopes
// nothing and lenient drops nothing. Only MetadataPortableEnvelope changes the
// bytes on the wire, and a caller asks for it when a non-iqmq consumer must
// not have to understand the header mapping.
func metaOf(m mq.MetadataMode) (metadataMode, error) {
	switch m {
	case mq.MetadataNativeStrict:
		return metaStrict, nil
	case mq.MetadataLenientDrop:
		return metaDrop, nil
	case mq.MetadataPortableEnvelope:
		return metaEnvelope, nil
	case mq.MetadataAdaptive:
		return metaAdaptive, nil
	default:
		return metaStrict, errMetadataMode
	}
}

// parseInt64 reads a decimal broker configuration value, and returns zero for
// anything it cannot read.
func parseInt64(s string) int64 {
	var n int64
	if s == "" {
		return 0
	}
	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int64(c-'0')
		if n > 1<<40 {
			return 0
		}
	}
	return n
}

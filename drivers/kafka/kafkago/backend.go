package kafkago

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	kgo "github.com/segmentio/kafka-go"

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

// Backend is the kafka-go [github.com/iqhive/iqmq.Backend].
//
// It owns FOUR client objects, and the shape is the client's rather than this
// driver's:
//
//   - w, a [kgo.Writer] with Async false, for the publications that wait for
//     their reply. Async is a field of the WRITER and not of a call, so a
//     backend that had one writer could not offer both a synchronous Publish
//     and an asynchronous one.
//   - wa, a [kgo.Writer] with Async true, for PublishAsync, PublishOwned and
//     a sink configured with ConfirmNone. Both writers share one transport, so
//     the second costs no connections.
//   - cl, a [kgo.Client], for metadata, the administrative surface and the
//     offset commits a source makes outside a live generation.
//   - tr, the [kgo.Transport] all three share, which this backend closes.
//
// Every source additionally owns a [kgo.ConsumerGroup] of its own, because a
// consumer group is one group and one topic list. What is shared is the seed
// brokers, the TLS and SASL configuration, the dialler and the transport.
type Backend struct {
	*driverkit.BaseBackend
	cfg    Config
	errs   *driverkit.ErrorMapper
	w      *kgo.Writer
	wa     *kgo.Writer
	cl     *kgo.Client
	tr     *kgo.Transport
	dialer *kgo.Dialer
	caps   mq.Capabilities
	env    *driverkit.Envelope
	clock  mq.Clock
	counters
	// msgbufs holds the message buffers. The specification's ownership rules
	// are what make pooling safe here: a buffer goes back only after every
	// holder has dropped its reference, and the writer's completion is the
	// moment kafka-go stops reading the message.
	msgbufs sync.Pool

	// health is the reporter, held separately from BaseBackend's copy so that
	// it can report Degraded before the base exists: a dial that fails has to
	// report it and there is no base yet.
	health *driverkit.Health

	// pending counts the publications admitted to the asynchronous writer and
	// not yet completed, which is what Flush waits for: kafka-go's Writer has
	// no Flush of its own — only Close, which is terminal — so the driver
	// counts.
	pending pending

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than left
	// holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
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
	duplicable atomic.Uint64
}

// pending is a bounded-wait counter of admitted-but-unfinished work.
//
// It exists because [kgo.Writer] offers no Flush: Close flushes and is
// terminal, and the specification requires a FlushSink whose Flush a caller can call and then
// carry on publishing. A counter the completion decrements is exact — it
// reaches zero when every admitted record has had its reply — where a sleep
// would be a guess.
type pending struct {
	mu   sync.Mutex
	cond *sync.Cond
	n    int
}

// add admits one unit of work.
func (p *pending) add() {
	p.mu.Lock()
	p.n++
	p.mu.Unlock()
}

// done retires one, and wakes a Wait that was waiting for the last.
func (p *pending) done() {
	p.mu.Lock()
	if p.n > 0 {
		p.n--
	}
	if p.n == 0 && p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
}

// wait blocks until nothing is outstanding or ctx is done, and reports which.
//
// sync.Cond has no deadline, so the context is converted into a broadcast by
// [context.AfterFunc] — one registration per Wait, not one per record, which
// is the rule.
func (p *pending) wait(ctx context.Context) error {
	p.mu.Lock()
	if p.cond == nil {
		p.cond = sync.NewCond(&p.mu)
	}
	p.mu.Unlock()
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	defer stop()
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.n > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.cond.Wait()
	}
	return ctx.Err()
}

// init finishes construction once the clients exist.
func (b *Backend) init(ctx context.Context, own driverkit.Ownership) error {
	b.clock = b.cfg.Clock
	// The one limit the cluster really knows. A failure to read it is not a
	// failure to open: the specification makes an unadvertised limit unknown rather than
	// zero, and a cluster whose ACLs forbid DescribeConfigs is a cluster this
	// driver still works against.
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
		Native:       b.w,
		Ownership:    own,
		CloseNative:  b.closeClients,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return err
	}
	b.BaseBackend = base
	b.health.Ready()
	return nil
}

// closeClients shuts the producer, the asynchronous producer and the transport
// down, in that order.
//
// [kgo.Writer.Close] is also kafka-go's only flush: it produces the batches it
// is holding and blocks on their completions. So the order matters — a
// transport closed first would fail the flush — and a Close that skipped the
// writers would lose a ConfirmNone publication the caller had already been
// told was admitted.
func (b *Backend) closeClients() error {
	first := b.wa.Close()
	if err := b.w.Close(); err != nil && first == nil {
		first = err
	}
	if b.tr != nil {
		b.tr.CloseIdleConnections()
	}
	return first
}

// messageMaxBytes asks the cluster for its own record-size bound, and returns
// zero when it will not say.
//
// It is two round trips where franz-go's kadm needs one: kafka-go's
// DescribeConfigs takes a resource NAME, so a broker's configuration can only
// be asked for by broker id, and the id comes from the metadata. Naming a
// broker that does not exist answers nothing at all, which is why the id is
// read rather than assumed to be zero.
func (b *Backend) messageMaxBytes(ctx context.Context) int64 {
	rctx, cancel := b.bound(ctx)
	defer cancel()
	md, err := b.cl.Metadata(rctx, &kgo.MetadataRequest{})
	if err != nil || len(md.Brokers) == 0 {
		return 0
	}
	resp, err := b.cl.DescribeConfigs(rctx, &kgo.DescribeConfigsRequest{
		Resources: []kgo.DescribeConfigRequestResource{{
			ResourceType: kgo.ResourceTypeBroker,
			ResourceName: strconv.Itoa(md.Brokers[0].ID),
			ConfigNames:  []string{"message.max.bytes"},
		}},
	})
	if err != nil {
		return 0
	}
	for _, r := range resp.Resources {
		if r.Error != nil {
			continue
		}
		for _, e := range r.ConfigEntries {
			if e.ConfigName != "message.max.bytes" {
				continue
			}
			if n := parseInt64(e.ConfigValue); n > 0 {
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
		Profile:       ProfileName,
		DriverVersion: Version,
		ClientName:    clientModule,
		ClientVersion: clientVersion,
		// BrokerVersion stays empty: Kafka reports supported API versions
		// rather than a version string, so the honest answer is that the
		// driver does not know.
	}
}

// Ping is the liveness check a metadata round trip to one broker,
// which consumes no message and creates no topology.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	rctx, cancel := b.bound(ctx)
	defer cancel()
	if _, err := b.cl.Metadata(rctx, &kgo.MetadataRequest{}); err != nil {
		return b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	return nil
}

// Writer returns the synchronous producer, which is this driver's escape hatch
// under a name a caller can find: client.Native().(*kafka.Writer) is the SPI's
// route and this is the typed one.
func (b *Backend) Writer() *kgo.Writer { return b.w }

// AsyncWriter returns the producer the asynchronous publication paths use.
// It is a second writer because Async is a field of a [kgo.Writer] and not of
// a call.
func (b *Backend) AsyncWriter() *kgo.Writer { return b.wa }

// Client returns the protocol client the administrative surface is built on.
func (b *Backend) Client() *kgo.Client { return b.cl }

// Transport returns the transport every client of this backend shares, which
// is what a caller instrumenting the dial reaches for.
func (b *Backend) Transport() *kgo.Transport { return b.tr }

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
// Kafka has no anonymous group, kafka-go refuses a ConsumerGroupConfig with
// an empty ID, and a driver that invented one would give two processes the
// same durable position by accident.
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
		// The specification gives Close no context, so there is none to pass on: a prompt
		// close is not an operation a caller cancels, and Shutdown is the
		// bounded form. It is the same finding testkit records.
		// The specification gives Close no context: a prompt close is not an operation a
		// caller cancels, and Shutdown is the bounded form.
		_ = src.Close()
		return nil, mq.ErrClosed
	}
	b.srcs[src] = struct{}{}
	b.mu.Unlock()
	return src, nil
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

// teardown closes everything this backend opened, before the clients go.
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
	return first
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
	// metaAdaptive uses the native mapping where it suffices and the envelope
	// where it does not.
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

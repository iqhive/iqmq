package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	natsgo "github.com/nats-io/nats.go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
//
// Two absences are as deliberate as the presences. There is no
// [github.com/iqhive/iqmq.AdminBackend]: NATS Core has no topology and no
// administrative API, so Client.Admin answers ErrUnsupported, which is the specification's
// answer for a backend that cannot provide one. And no source here returns a
// [github.com/iqhive/iqmq.Settler]: see source.go.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
)

// Backend is the NATS Core [github.com/iqhive/iqmq.Backend].
//
// It owns the connection, the capability table, the error mapping and the
// health state machine, and it hands out sinks and sources over one *nats.Conn:
// NATS multiplexes everything on a single TCP connection, so a client that
// dialled per publisher would be spending file descriptors to no purpose.
type Backend struct {
	*driverkit.BaseBackend
	cfg   Config
	errs  *driverkit.ErrorMapper
	nc    *natsgo.Conn
	own   driverkit.Ownership
	caps  mq.Capabilities
	env   *driverkit.Envelope
	clock mq.Clock
	counters

	// chain is the driver's handlers on a handed-over connection, which
	// this backend is behind ([Backend.chainHandlers]); nil on a dialled one.
	chain *chain

	// health is the reporter, held separately from BaseBackend's copy so
	// that the asynchronous handlers can drive it before the base exists:
	// a dial that fails has to report Degraded and there is no base yet.
	health *driverkit.Health
	// seeded is the reason a backend handed a connection that was not
	// connected started Degraded ([Backend.Health]); nil otherwise.
	seeded error

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than
	// left holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
	// waits are the round trips waiting for the server, a Request for its
	// reply and, on a borrowed connection, a flush for its PONG, each by its
	// cancel function, with the subject a Request went to and an empty one
	// for a flush. Closing a borrowed connection is the caller's business, so
	// cancelling them is the only way Close can wake them, and a refusal of a
	// Request's subject ends it too ([Backend.refuseWaits]).
	waits map[*context.CancelCauseFunc]string
}

// init finishes construction once a connection exists.
func (b *Backend) init(nc *natsgo.Conn, own driverkit.Ownership) error {
	caps, err := capabilities(nc.MaxPayload())
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
	b.nc, b.caps, b.env, b.own = nc, caps, env, own
	b.clock = b.cfg.Clock
	// Under the lock: on a handed-over connection the chain's handlers are
	// already in place and read srcs ([Backend.othersSlowConsumer]) and
	// waits ([Backend.refuseWaits]) on nats.go's callback goroutine.
	b.mu.Lock()
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	b.waits = map[*context.CancelCauseFunc]string{}
	b.mu.Unlock()
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(nc),
		Capabilities: caps,
		Native:       nc,
		Ownership:    own,
		CloseNative:  func() error { nc.Close(); return nil },
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return err
	}
	b.BaseBackend = base
	b.seedHealth(nc)
	return nil
}

// info describes the driver and the library behind it. Every field the
// driver does not know is empty, which the specification makes "the driver did not say"
// rather than a claim.
func (b *Backend) info(nc *natsgo.Conn) mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       "core",
		DriverVersion: Version,
		ClientName:    clientModule,
		ClientVersion: natsgo.Version,
		BrokerVersion: nc.ConnectedServerVersion(),
	}
}

// Ping is the liveness check a PING/PONG round trip, which consumes no
// message and creates no topology.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	if err := b.flush(ctx); err != nil {
		return b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	return nil
}

// flush is [Backend.pingPong] that [Backend.Close] ends.
//
// Closing an owned connection fails every flush still waiting, and nothing
// did on a borrowed one: a publication waiting for its PONG from a server
// that had stopped answering outlived Close by up to [Config.FlushTimeout].
// A flush Close ends returns errClosedMidWait wrapping the context's error.
// On an owned connection the close does it, and a flush costs no more.
func (b *Backend) flush(ctx context.Context) error {
	if b.own.Owned() {
		return b.pingPong(ctx)
	}
	fctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if !b.track(&cancel, "") {
		// The backend closed after the write this flush confirms, which
		// is a flush Close ended.
		return fmt.Errorf("%w: %w", errClosedMidWait, context.Canceled)
	}
	defer b.untrack(&cancel)
	err := b.pingPong(fctx)
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(fctx), errClosedMidWait) {
		return fmt.Errorf("%w: %w", errClosedMidWait, err)
	}
	return err
}

// pingPong is the PING/PONG round trip every confirmation and every liveness
// check goes through.
//
// nats.go REQUIRES the context to carry a deadline — FlushWithContext refuses
// one that does not, with ErrNoDeadlineContext — so the driver supplies
// [Config.FlushTimeout] when the caller did not. That is a driver-level bound
// on a driver-level round trip, never a second deadline on a caller who has
// one, and it is not optional: without it every Publish under the default
// confirmation would fail on a background context.
func (b *Backend) pingPong(ctx context.Context) error {
	if _, ok := ctx.Deadline(); ok || b.cfg.FlushTimeout <= 0 {
		return b.nc.FlushWithContext(ctx)
	}
	fctx, cancel := context.WithTimeout(ctx, b.cfg.FlushTimeout)
	defer cancel()
	return b.nc.FlushWithContext(fctx)
}

// OpenSink opens a publication channel to one subject.
//
// ConfirmDurable is refused here and not later, which is the rule: this
// profile has no durable boundary to define, and a publisher that discovered
// that at its first message would already have told the application it was
// safe to forget the data.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Confirmation.Resolve() == mq.ConfirmDurable {
		return nil, errNoDurableConfirm
	}
	if cfg.Receipts {
		// FeatureReceipts is not advertised, so the core should never ask;
		// refusing rather than ignoring is what keeps a future core change
		// from silently handing callers an unfilled receipt.
		return nil, mq.ErrUnsupported
	}
	meta, err := metaOf(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	subject, err := b.publishSubject(cfg.Topic)
	if err != nil {
		return nil, err
	}
	s := &sink{
		b:       b,
		topic:   cfg.Topic,
		subject: subject,
		meta:    meta,
		confirm: cfg.Confirmation.Resolve(),
		closed:  make(chan struct{}),
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
// The subscription name becomes the NATS queue group, which is what makes two
// consumers of one subscription compete and two consumers of different
// subscriptions each receive everything. The topic may be a pattern.
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
	if cfg.Start.Resolve().Kind() != mq.StartKindLatest {
		// NATS Core retains nothing, so every subscription starts at the
		// moment it is created and no other start position exists. Saying
		// so is FeatureReplay, FeatureSeekByTime and FeatureSeekBySequence
		// being unsupported; refusing here is what makes the claim true
		// rather than a silent Latest.
		return nil, mq.ErrUnsupported
	}
	subject, err := b.subscribeSubject(cfg.Topic)
	if err != nil {
		return nil, err
	}
	if err := CheckQueue(cfg.Subscription); err != nil {
		return nil, err
	}
	before := b.nc.LastError()
	src, err := b.newSource(cfg, subject, meta)
	if err != nil {
		return nil, err
	}
	// The source is the backend's before the confirmation's round trip, so
	// a slow consumer on it in the meantime is counted as its own.
	if !b.adopt(src) {
		_ = src.Close()
		return nil, mq.ErrClosed
	}
	if err := b.confirmSubscription(ctx, before, cfg.Topic, subject, cfg.Subscription); err != nil {
		_ = src.Close()
		return nil, err
	}
	if !b.adopt(src) {
		// Close ran during the confirmation, and closes the source.
		_ = src.Close()
		return nil, mq.ErrClosed
	}
	return src, nil
}

// adopt records src as one of the backend's sources, and reports false once
// the backend is closing.
func (b *Backend) adopt(src *source) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.srcs[src] = struct{}{}
	return true
}

// confirmSubscription waits for the server to read a new subscription, and
// refuses the source with a definite CodeForbidden when the server refused
// the subscription for permissions. before is the connection's last error
// from before the subscription was made.
//
// The server refuses a subscription with an asynchronous -ERR, which nats.go
// keeps only as the connection's last error and on the subscription, where
// nothing reaches it, so a source used to open on a refused subscription and
// receive nothing while every publication to its topic went elsewhere. The
// -ERR comes before the PONG of the flush that follows the subscription, as
// it does for a publication ([Backend.refusal]). It costs a round trip while
// the connection is up and none while it is not: a subscription made then is
// sent when the connection comes back, and a refusal of it reaches health
// alone. A flush that fails leaves the subscription as it was, which nats.go
// sends again on every reconnection.
func (b *Backend) confirmSubscription(ctx context.Context, before error, topic, subject, queue string) error {
	if !b.nc.IsConnected() || b.flush(ctx) != nil {
		return nil
	}
	now := b.nc.LastError()
	if !errors.Is(now, natsgo.ErrPermissionViolation) || now == before { //nolint:errorlint // identity is what says the refusal is new
		return nil
	}
	if !strings.HasSuffix(now.Error(), "Subscription to "+strconv.Quote(subject)+" using queue "+strconv.Quote(queue)) {
		return nil
	}
	return b.errs.MapNet(mq.OpOpen, topic, queue, now, false)
}

// Conn returns the underlying connection, which is this driver's escape hatch
// under a name a caller can find: client.Native().(*nats.Conn) is the SPI's
// route and this is the typed one.
func (b *Backend) Conn() *natsgo.Conn { return b.nc }

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

// teardown closes everything this backend opened, before the connection goes.
//
// It is what wakes a Fetch that is parked in a source nobody closed: the specification's
// "Close unblocks blocked operations" has to reach a caller who closed the
// client rather than the consumer.
func (b *Backend) teardown() error {
	b.unlink()
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
	for cancel := range b.waits {
		(*cancel)(errClosedMidWait)
	}
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
	return first
}

// metadataMode is what this driver does with a message's metadata.
type metadataMode uint8

const (
	// metaStrict maps every field to a native NATS field or a reserved
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
// All four are distinct here, which is not true of every driver: NATS headers
// are text lines, so there really is metadata this broker cannot represent, and
// strict, lenient, adaptive and always-envelope all behave differently on it.
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

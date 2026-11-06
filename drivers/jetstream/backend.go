package jetstream

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
//
// One absence is as deliberate as the presences: there is no
// [github.com/iqhive/iqmq.AdminBackend]. JetStream has a real topology API and
// this increment does not wrap it, so Client.Admin answers ErrUnsupported, which
// is the answer for a backend that cannot provide one, and
// [github.com/iqhive/iqmq.FeatureAdmin] says so.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
)

// Backend is the JetStream [github.com/iqhive/iqmq.Backend].
//
// It owns the connection, the JetStream context, the bound stream's facts, the
// capability table, the error mapping and the health state machine, and it hands
// out sinks and sources over one *nats.Conn: NATS multiplexes everything on a
// single TCP connection, so a client that dialled per publisher would be
// spending file descriptors to no purpose.
type Backend struct {
	*driverkit.BaseBackend
	cfg    Config
	errs   *driverkit.ErrorMapper
	nc     *natsgo.Conn
	own    driverkit.Ownership
	js     js.JetStream
	caps   mq.Capabilities
	facts  streamFacts
	stream js.Stream
	// streamSubjects is what the bound stream captured when this client
	// opened. A publication to a subject none of them matches is refused with
	// CodeNotFound before any I/O, because the alternative is invisible: a
	// ConfirmNone publication to an unbound subject is a core publish with no
	// reply, which the server drops without telling anybody.
	streamSubjects []string
	env            *driverkit.Envelope
	clock          mq.Clock
	counters

	// chain is the driver's handlers on a handed-over connection, which
	// this backend is behind ([Backend.chainHandlers]); nil on a dialled one.
	chain *chain

	// health is the reporter, held separately from BaseBackend's copy so that
	// the asynchronous handlers can drive it before the base exists: a dial
	// that fails has to report Degraded and there is no base yet.
	health *driverkit.Health

	// amu guards the asynchronous publications in flight. The JetStream
	// client reports every one of them through two connection-wide callbacks
	// that carry the *nats.Msg the publication was made with, so the pointer
	// is the correlation key and this map is the whole of the bookkeeping —
	// which is how PublishAsync honours the specification's "no goroutine per message".
	amu   sync.Mutex
	async map[*natsgo.Msg]*asyncPub

	// lmu guards the asynchronous publications whose PubAck timed out, or
	// that the server refused, and whose callbacks have not run yet. The
	// client library reports a timeout while it holds its publisher lock, and
	// a refusal is heard on nats.go's callback goroutine, so those callbacks
	// run on one goroutine of this backend's instead (see resolveLate).
	// resolving says that goroutine is alive, lateWG is what teardown waits
	// on, and lateClosed stops a new one starting once teardown is waiting.
	lmu        sync.Mutex
	late       []lateAsync
	resolving  bool
	lateClosed bool
	lateWG     sync.WaitGroup

	// schedSeq numbers the schedule control messages, so that two scheduled
	// publications to one topic do not overwrite each other's schedule.
	schedSeq atomic.Uint64
	// schedRoot is the schedule root plus a token random to this backend.
	// schedSeq alone is unique only within one backend and starts at zero
	// in each, so two producers on one stream, or one producer after a
	// restart, wrote the same schedule subject and the later schedule
	// replaced the pending one.
	schedRoot string

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than left
	// holding a goroutine after Close.
	mu     sync.Mutex
	srcs   map[*source]struct{}
	sinks  map[*sink]struct{}
	closed bool
	// waits are the round trips waiting for the server, a publication or a
	// dead-letter copy for its PubAck, a JetStream API request for its reply,
	// a Flush for the asynchronous ones and a Ping for its PONG, each by its
	// cancel function, with the subject a publication went to, the subject
	// pattern of an API request's subjects ([Backend.apiRequest]) and an
	// empty one otherwise. Closing a borrowed connection is the caller's
	// business, and closing an owned one wakes neither a batch nor a Flush,
	// so cancelling them is how Close wakes them, and a refusal of a subject
	// the request went to ends it too ([Backend.refuseWaits]).
	waits map[*context.CancelCauseFunc]string
}

// init finishes construction once a connection exists: the JetStream context,
// the stream, the facts the capability table is read off, and the base.
func (b *Backend) init(ctx context.Context, nc *natsgo.Conn, own driverkit.Ownership) error {
	b.nc = nc
	b.own = own
	b.clock = b.cfg.Clock
	b.schedRoot = b.cfg.scheduleRoot() + "." + rand.Text()
	// Under the locks: on a handed-over connection the chain's handlers are
	// already in place and read async ([Backend.refuseAsync]) and waits
	// ([Backend.refuseWaits]) on nats.go's callback goroutine.
	b.amu.Lock()
	b.async = map[*natsgo.Msg]*asyncPub{}
	b.amu.Unlock()
	b.mu.Lock()
	b.srcs, b.sinks = map[*source]struct{}{}, map[*sink]struct{}{}
	b.waits = map[*context.CancelCauseFunc]string{}
	b.mu.Unlock()

	jsc, err := b.newJetStream(nc)
	if err != nil {
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	b.js = jsc

	stream, info, err := b.ensureStream(ctx)
	if err != nil {
		return err
	}
	b.stream = stream
	b.streamSubjects = append([]string(nil), info.Config.Subjects...)
	b.facts = b.factsOf(nc, info)

	caps, err := capabilities(b.facts)
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
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(nc),
		Capabilities: caps,
		Native:       jsc,
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

// newJetStream builds the JetStream context, with the asynchronous publication
// bound and the two callbacks PublishAsync resolves through.
//
// The asynchronous PubAck gets PublishTimeout too. Without a timeout the client
// library waits for ever for a PubAck the link did not lose but the server
// never sent, as across a cluster leader change: the callback waited for the
// sink's Close and the publication kept its place in the pending window. The
// timeout reaches onAsyncErr as ErrAsyncPublishTimeout, which maps to an
// ambiguous failure, because the stream may hold the message.
func (b *Backend) newJetStream(nc *natsgo.Conn) (js.JetStream, error) {
	opts := []js.JetStreamOpt{
		js.WithPublishAsyncMaxPending(b.cfg.PublishAsyncMaxPending),
		js.WithPublishAsyncAckHandler(b.onAsyncAck),
		js.WithPublishAsyncErrHandler(b.onAsyncErr),
		js.WithDefaultTimeout(b.cfg.PublishTimeout),
		js.WithPublishAsyncTimeout(b.cfg.PublishTimeout),
	}
	switch {
	case b.cfg.Domain != "":
		return js.NewWithDomain(nc, b.cfg.Domain, opts...)
	case b.cfg.APIPrefix != "":
		return js.NewWithAPIPrefix(nc, b.cfg.APIPrefix, opts...)
	default:
		return js.New(nc, opts...)
	}
}

// factsOf reads the capability table's inputs off the connected server and the
// bound stream.
func (b *Backend) factsOf(nc *natsgo.Conn, info *js.StreamInfo) streamFacts {
	f := streamFacts{
		maxPayload: nc.MaxPayload(),
		maxMsgSize: int64(info.Config.MaxMsgSize),
		memory:     info.Config.Storage == js.MemoryStorage,
		// The two flags are read back from the stream the server really
		// created, never from what this client asked for: a 2.10 server
		// ignores both fields, so asking is not the same as having.
		allowMsgTTL:       info.Config.AllowMsgTTL,
		allowMsgSchedules: info.Config.AllowMsgSchedules,
		duplicates:        info.Config.Duplicates,
		doubleAck:         b.cfg.DoubleAck,
	}
	if info.Config.MaxMsgSize < 0 {
		f.maxMsgSize = 0
	}
	if f.duplicates <= 0 {
		f.duplicates = DefaultDuplicateWindow
	}
	if b.cfg.DeadLetterTopic != "" {
		if sub, err := Subject(b.cfg.SubjectPrefix, b.cfg.DeadLetterTopic); err == nil {
			f.deadLetter = sub
		}
	}
	// A schedule is only usable when the stream captures the subject the
	// control message goes to, which is a fact about the stream and not about
	// the configuration: a stream whose Subjects do not reach the schedule
	// root would store nothing and the publication would vanish.
	f.schedulesConfigured = !b.cfg.DisableSchedules &&
		streamCaptures(info.Config.Subjects, ScheduleSubject(b.cfg.scheduleRoot(), 0))
	return f
}

// info describes the driver and the library behind it. Every field the
// driver does not know is empty, which the specification makes "the driver did not say"
// rather than a claim.
func (b *Backend) info(nc *natsgo.Conn) mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       "jetstream",
		DriverVersion: Version,
		ClientName:    clientModule,
		ClientVersion: natsgo.Version,
		BrokerVersion: nc.ConnectedServerVersion(),
	}
}

// Facts is what the connected server and the bound stream said, which is what
// every conditional capability claim is read off.
//
// It is exported because a claim that cannot be checked against its own evidence
// is a claim on trust: TestCapabilityClaimsFollowTheStream reads this and the
// capability table together, and a driver page that says "TTL needs 2.11" has
// somewhere to point.
func (b *Backend) Facts() string { return describeFacts(b.facts) }

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

// flush is the PING/PONG round trip every liveness check goes through.
//
// nats.go REQUIRES the context to carry a deadline — FlushWithContext refuses
// one that does not, with ErrNoDeadlineContext — so the driver supplies
// [Config.PublishTimeout] when the caller did not. That is a driver-level bound
// on a driver-level round trip, never a second deadline on a caller who has one.
// [Backend.Close] ends it with errClosedMidWait.
func (b *Backend) flush(ctx context.Context) error {
	ctx, cancel := b.bound(ctx)
	defer cancel()
	wctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	if !b.track(&stop, "") {
		return errClosedMidWait
	}
	defer b.untrack(&stop)
	if err := b.nc.FlushWithContext(wctx); err != nil {
		if closedMidWait(ctx, wctx) {
			return errClosedMidWait
		}
		return err
	}
	return nil
}

// track records a round trip in flight, a publication to subject or another
// wait with no subject, so that [Backend.teardown] can end it. It reports
// false once the backend is closing, when the round trip must not start.
func (b *Backend) track(cancel *context.CancelCauseFunc, subject string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	b.waits[cancel] = subject
	return true
}

// untrack drops a round trip that has returned.
func (b *Backend) untrack(cancel *context.CancelCauseFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.waits, cancel)
}

// closedMidWait reports whether [Backend.Close] ended the wait on wctx, which
// was derived from the caller's ctx. A caller whose own context ended is
// told so instead.
func closedMidWait(ctx, wctx context.Context) bool {
	return ctx.Err() == nil && errors.Is(context.Cause(wctx), errClosedMidWait)
}

// endedBy is why a wait for the answer to a request to a subject one of
// patterns matches ended without one: the server's refusal of that subject,
// which ends the wait when the driver hears it ([Backend.refuseWaits]) and is
// read off the connection when it does not ([Backend.refusal]), or
// errClosedMidWait when Close ended it; nil when neither did.
func (b *Backend) endedBy(ctx, wctx context.Context, before error, patterns ...string) error {
	cause := context.Cause(wctx)
	for _, pattern := range patterns {
		if errors.Is(cause, natsgo.ErrPermissionViolation) && refuses(cause, pattern) {
			return cause
		}
	}
	for _, pattern := range patterns {
		if refused := b.refusal(before, pattern); refused != nil {
			return refused
		}
	}
	if closedMidWait(ctx, wctx) {
		return errClosedMidWait
	}
	return nil
}

// apiRequest makes the JetStream API requests of call, to subjects one of
// patterns matches ([Backend.apiPatterns]), on a context that a refusal of
// such a subject and Close end ([Backend.waits]). The server discards a
// request it refuses for permissions and sends no reply, so the request used
// to wait out [Config.PublishTimeout] and was reported a retryable
// DeadlineExceeded, which a caller retried for ever at that cost each time.
// The answer is now the refusal, a definite CodeForbidden once mapped, or
// errClosedMidWait when Close ended the wait.
func (b *Backend) apiRequest(ctx context.Context, patterns []string, call func(context.Context) error) error {
	wctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	// One wait for each pattern, each under a key of its own.
	stops := make([]context.CancelCauseFunc, len(patterns))
	defer func() {
		for i := range stops {
			b.untrack(&stops[i])
		}
	}()
	for i, pattern := range patterns {
		stops[i] = stop
		if !b.track(&stops[i], pattern) {
			return mq.ErrClosed
		}
	}
	before := b.nc.LastError()
	err := call(wctx)
	if err == nil {
		return nil
	}
	if ended := b.endedBy(ctx, wctx, before, patterns...); ended != nil {
		return ended
	}
	return err
}

// apiPatterns are the subject patterns of the JetStream API requests whose
// subjects end in rest, under the prefix nats.go puts them under for this
// configuration, [Config.Domain]'s, [Config.APIPrefix] or "$JS.API.", and
// as the server may name them when it refuses one.
//
// A server outside the domain, such as a leaf node connected to the domain's
// hub, refuses the subject the client wrote, "$JS.<domain>.API.<rest>". The
// server whose domain it is maps that subject to "$JS.API.<rest>" before it
// checks permissions, so its refusal names the mapped one, and a request it
// refused used to wait out its bound. So the mapped pattern is added only
// when the connected server owns the domain: on any other server
// "$JS.API.<rest>" is that server's own API, and a refusal of another request
// to it, for a stream of the same name, was read as this request's.
func (b *Backend) apiPatterns(rest string) []string {
	switch {
	case b.cfg.Domain != "":
		if b.nc.ConnectedDomain() == b.cfg.Domain {
			return []string{"$JS." + b.cfg.Domain + ".API." + rest, js.DefaultAPIPrefix + rest}
		}
		return []string{"$JS." + b.cfg.Domain + ".API." + rest}
	case b.cfg.APIPrefix != "":
		return []string{strings.TrimSuffix(b.cfg.APIPrefix, ".") + "." + rest}
	default:
		return []string{js.DefaultAPIPrefix + rest}
	}
}

// publishCopy publishes a dead-letter copy and waits on ctx for its PubAck, a
// wait that a refusal of the copy's subject and Close end ([Backend.waits]).
// The server discards a copy it refuses for permissions and sends no PubAck,
// so the wait used to run to its bound and the copy was reported as one the
// stream may hold. sent reports, for a copy that failed, whether the stream
// may hold it: not when the server refused it, nor when the backend or the
// connection had closed before it was sent. One whose wait Close ended is
// errDeadLetterUnknownAtClose.
//
// nats.go fails a request still waiting for its reply with ErrDisconnected
// when the client is told to reconnect and with ErrConnectionClosed when the
// connection closes, and the error map reads those as a definite failure. The
// copy had left, so they are answered as [sink.failRequested] answers a
// publication: SettlementUnknown for the first, and an ambiguous Closed for
// the second (errDeadLetterUnknownAtCut alongside the native error).
func (b *Backend) publishCopy(ctx context.Context, msg *natsgo.Msg) (sent bool, err error) {
	wctx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	if !b.track(&stop, msg.Subject) {
		return false, mq.ErrClosed
	}
	defer b.untrack(&stop)
	// nats.go refuses a request on a connection that has already closed
	// before it writes anything, so that refusal stays definite.
	closedBefore := b.nc.IsClosed()
	before := b.nc.LastError()
	if _, err = b.js.PublishMsg(wctx, msg); err == nil {
		return true, nil
	}
	// A connection closed before the copy wrote nothing, whatever ended the
	// wait since.
	if closedBefore {
		return false, err
	}
	switch ended := b.endedBy(ctx, wctx, before, msg.Subject); {
	case ended != nil && errors.Is(ended, natsgo.ErrPermissionViolation):
		return false, ended
	case ended != nil:
		return true, errDeadLetterUnknownAtClose
	case errors.Is(err, natsgo.ErrDisconnected):
		return true, errPublishUnknownAtReconnect
	case errors.Is(err, natsgo.ErrConnectionClosed):
		return true, fmt.Errorf("%w: %w", err, errDeadLetterUnknownAtCut)
	default:
		return true, err
	}
}

// refuseWaits ends every round trip waiting for the server's answer to a
// request to a subject the server has now refused to publish to, a
// publication's PubAck or a JetStream API request's reply, with the refusal
// as the cause. The server discards a refused request and sends nothing
// back, so the request used to wait out PublishTimeout, and a publication
// was then reported as one the stream may hold.
func (b *Backend) refuseWaits(refusal error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for cancel, subject := range b.waits {
		if subject != "" && refuses(refusal, subject) {
			(*cancel)(refusal)
		}
	}
}

// bound gives a context a deadline when the caller did not, which is what a
// round trip needs in order not to wait for ever on a link that is up and
// silent. A caller who has a deadline keeps it: a driver that added a second one
// would shorten a deadline the caller chose.
func (b *Backend) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || b.cfg.PublishTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.cfg.PublishTimeout)
}

// OpenSink opens a publication channel to one subject.
//
// ConfirmDurable is NOT refused here, and that is the profile's whole point: a
// PubAck is sent after the message is committed to the stream's storage, so the
// durable boundary this broker offers is the same round trip as the accepted one.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	meta, err := metaOf(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	subject, err := Subject(b.cfg.SubjectPrefix, cfg.Topic)
	if err != nil {
		return nil, err
	}
	if !streamCaptures(b.streamSubjects, subject) {
		// The specification: no hidden creation. A publication to a subject no stream
		// captures is not a topic this client can use, and saying so here is
		// the only way a ConfirmNone publication can ever learn it.
		return nil, b.noStream(subject, cfg.Topic)
	}
	s := &sink{
		b:       b,
		topic:   cfg.Topic,
		subject: subject,
		meta:    meta,
		confirm: cfg.Confirmation.Resolve(),
		receipt: cfg.Receipts,
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

// OpenSource opens a durable pull consumer.
//
// The subscription name becomes the durable's name, which is what makes two
// consumers of one subscription compete and two consumers of different
// subscriptions each receive everything. The topic may be a pattern,
// which becomes the durable's FilterSubjects.
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
	filter, err := Pattern(b.cfg.SubjectPrefix, cfg.Topic)
	if err != nil {
		return nil, err
	}
	if b.facts.schedulesUsable() && PatternReachesSchedules(filter, b.cfg.scheduleRoot()) {
		// A JetStream consumer has no negative filter, so a pattern that
		// reaches the schedule tree would hand the caller this driver's own
		// control messages.
		return nil, errPatternReachesSchedules
	}
	src, err := b.newSource(ctx, cfg, filter, meta)
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
	return src, nil
}

// Conn returns the underlying connection, and JetStream the context built over
// it: this driver's escape hatches under names a caller can find.
// client.Native().(jetstream.JetStream) is the SPI's route and these are the
// typed ones.
func (b *Backend) Conn() *natsgo.Conn { return b.nc }

// JetStream returns the JetStream context, which is the stream and consumer API
// a caller reaches for when it needs topology this driver does not wrap.
func (b *Backend) JetStream() js.JetStream { return b.js }

// Stream is the stream this client is bound to.
func (b *Backend) Stream() js.Stream { return b.stream }

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
// client rather than the consumer. It runs the sinks last, because a sink's
// close resolves the asynchronous publications it still holds and a source's
// close naks what it was holding — and a nak is a publication.
//
// A source's close waits for its consumer's drain, which on a server that has
// stopped answering lasts [Config.DrainTimeout]. Every drain starts before
// any is waited for, so they run together and Close waits one DrainTimeout
// for them all. It closed the sources one after another, and a process
// shutting down through a partition waited one DrainTimeout, or nats.go's
// 10s flush when that is shorter, for each of its consumers.
func (b *Backend) teardown() error {
	b.unlink()
	b.mu.Lock()
	b.closed = true
	for cancel := range b.waits {
		(*cancel)(errClosedMidWait)
	}
	srcs := make([]*source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	b.mu.Unlock()
	for _, s := range srcs {
		s.startClose()
	}
	drained := time.Now().Add(b.cfg.DrainTimeout)
	for _, s := range srcs {
		s.finishClose(drained)
	}
	var first error
	for _, s := range sinks {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	// A publication whose PubAck timed out has left its sink's bookkeeping,
	// so closing the sinks did not resolve it: its callback may still be
	// queued, and Close returns after it has run.
	b.stopLate()
	// The asynchronous publisher's reply subscription and its reconnect watcher
	// belong to the connection rather than to this backend: closing a connection
	// this client owns takes them with it, and one the caller owns would keep
	// them after this backend is gone. Only the borrowed case has anything left
	// to take off, and nats.go cannot clean a publisher whose first asynchronous
	// publication has not yet parked its watcher, so the owned case is left to
	// the close that already reaps it.
	if !b.own.Owned() {
		b.js.CleanupPublisher()
	}
	return first
}

// noStream is the refusal for a subject the bound stream does not capture. It is
// CodeNotFound and not CodeInvalidConfig: the subject is well formed and the
// topology is missing, which is the distinction a caller acts on.
func (b *Backend) noStream(subject, topic string) error {
	return b.errs.Map(mq.OpOpen, topic, "", errNoStreamForSubject{stream: b.cfg.Stream, subject: subject})
}

// errNoStreamForSubject names the stream and the subject, and wraps
// ErrTopicNotFound so that errors.Is reaches the sentinel.
type errNoStreamForSubject struct{ stream, subject string }

func (e errNoStreamForSubject) Error() string {
	return "iqmq/jetstream: stream " + e.stream + " does not capture the subject " + e.subject +
		"; the API forbids creating it behind the caller's back (set Config.ManageStream or Config.StreamSubjects)"
}

func (e errNoStreamForSubject) Unwrap() error { return mq.ErrTopicNotFound }

// streamCaptures reports whether any of the stream's subjects matches the
// concrete subject.
func streamCaptures(subjects []string, subject string) bool {
	for _, pattern := range subjects {
		if subjectMatches(pattern, subject) {
			return true
		}
	}
	return false
}

// metadataMode is what this driver does with a message's metadata.
type metadataMode uint8

const (
	// metaStrict maps every field to a native JetStream field or a reserved
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

package rabbitmq

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
)

// The compile-time proof of what this backend is. Asserting it here makes a
// change to any of these a build failure rather than a surface that quietly
// stops being offered.
//
// The absences are as deliberate as the presences. There is no
// [mq.BatchSink]: AMQP 0-9-1 has no batch frame, so a batch is N publishes on
// one channel and the core's own loop runs them (ADR-021). Each is a
// synchronous Publish that waits for its own confirm, so a batch costs one
// round trip per message under every confirm strategy (see the
// FeatureBatchPublish note in capabilities.go), and no batch on this protocol
// can be atomic. There is no [mq.BatchSettler]:
// `basic.ack{multiple=true}` settles every delivery up to a tag, which is safe
// only when deliveries are settled in the order they arrived, and the specification asks
// for "individual acks unless cumulative proven safe". And there is no
// [mq.DeadlineExtender]: a delivery's lease on this broker is the channel's
// lifetime, so there is nothing to extend and nothing to expire.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
	_ mq.AdminBackend   = (*Backend)(nil)
)

// Backend is the RabbitMQ [mq.Backend].
//
// It owns one connection, the topology ledger, the capability table, the error
// mapping, the health state machine and the dial loop, and it hands out one
// AMQP CHANNEL per sink and per source. The channel-per-handle shape is the
// one decision here that everything else follows from:
//
//   - A channel in confirm mode has ONE delivery-tag sequence, so two
//     publishers sharing a channel would share a confirm window and a
//     `basic.return` would be attributable to either. One channel per sink
//     makes each publisher's confirms its own.
//   - `basic.qos` is per channel, so two consumers sharing one would share a
//     prefetch, and a delivery tag from one would be settleable by the other.
//   - A CHANNEL EXCEPTION closes the channel. Anything sharing it fails too,
//     and on a confirm channel every publication whose confirm had not
//     arrived becomes ambiguous. Isolating the blast radius to one handle is
//     what keeps one malformed publication from failing another publisher's
//     in-flight messages.
//
// The cost is a channel per handle, which is a small integer to the broker and
// no socket at all: AMQP multiplexes every channel over one connection.
type Backend struct {
	*driverkit.BaseBackend
	cfg   Config
	errs  *driverkit.ErrorMapper
	proto protocol.Protocol
	caps  mq.Capabilities
	// fx is what the broker said plus what the configuration decided, and it
	// is written ONLY by [Backend.readFacts] and [Backend.probe], both of
	// which run on the opening goroutine before any handle exists. It is
	// immutable from then on, which is what makes it safe to read from every
	// handle with no lock -- `source.admit` reads `fx.quorum` per delivery.
	// A write to it after open is a data race and, for `fx.delayKind`, a
	// silent change of what this client does.
	fx    facts
	clock mq.Clock
	topo  *topology
	dial  client.Dialer

	// health is held separately from the base's copy so that the dial loop
	// can drive it before the base exists: a first dial that fails has to
	// report Degraded and there is no base yet.
	health *driverkit.Health
	// rc is the dial loop. See reconnect.go for what its Dial
	// closure does and why the driver rather than the client library owns
	// it.
	rc *driverkit.Reconnector

	// connMu guards the connection pointer alone, and is a read-write lock
	// because every publication and every settlement reads it and only a
	// reconnection writes it.
	connMu sync.RWMutex
	cn     client.Conn

	// blocked is `connection.blocked`: RabbitMQ telling a publisher that the
	// broker is under resource pressure and has stopped reading from this
	// connection (NotifyBlocked). It is an atomic rather than a
	// channel because every publication reads it and nothing waits on it:
	// the TCP back-pressure is what actually stops the publisher, and this
	// is what lets the driver report WHY through health and hooks instead of
	// leaving a caller with an unexplained stall.
	blocked atomic.Bool

	// staleCloses counts the channel-close notifications a sink's loop read
	// for a channel that had already been replaced. See [sink.gen] for what
	// makes them ordinary rather than exceptional: every reconnection
	// produces one per publisher, because the supervision loop detaches the
	// sinks itself before it redials.
	//
	// Nothing in the driver reads it; it is what makes the reordering
	// observable to a test, which otherwise has to wait for the absence of a
	// symptom and cannot tell a fix from a race it happened to win.
	staleCloses atomic.Uint64
	// staleConfirms counts the publisher confirms a sink's loop read for a
	// channel that had already been replaced, and dropped: such a confirm
	// names a tag in the OLD channel's sequence, and applying it to the new
	// tracker would resolve a different publication. It exists for
	// staleCloses' reason -- a test cannot otherwise tell a dropped confirm
	// from one that has not arrived yet.
	staleConfirms atomic.Uint64

	// channelRepairs counts the channel exceptions a handle answered ITSELF:
	// its loop read the channel's close notification, found the loss
	// unclaimed, and attached a replacement without the dial loop. It is the
	// repair route, and nothing in the driver reads it.
	//
	// declinedCloses is the other side: a close notification whose loss
	// somebody else had already claimed -- the supervision loop before a
	// redial, or Close. Both exist for `staleCloses`' reason. "The repair
	// happened once" and "the repair did NOT happen" are otherwise both
	// invisible: a test can see the channel that is there afterwards, and not
	// how many attaches made it, nor whether a loop that should have declined
	// had even run.
	channelRepairs atomic.Uint64
	declinedCloses atomic.Uint64

	// mu guards the open children and the closed flag. A backend closes what
	// it opened, which is the obligation and the reason a parked Fetch is
	// woken rather than left holding a goroutine after Close.
	mu     sync.Mutex
	sinks  map[*sink]struct{}
	srcs   map[*source]struct{}
	closed bool

	// ctx bounds every background goroutine this backend starts, and cancel
	// is what Close uses to stop them. A goroutine that outlived its backend
	// would be a leak goleak reports and a caller cannot reclaim.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// seq numbers the consumer tags and the confirm correlation tokens, so
	// that two sinks of one client cannot collide.
	seq atomic.Uint64

	// rq is the direct reply-to endpoint, built on the first request. Most
	// clients never make one, and a consumer on `amq.rabbitmq.reply-to` that
	// nobody uses is a consumer in every broker's management view for no
	// reason.
	rqOnce sync.Once
	rq     *requester
}

// newBackend builds the backend's constant parts. The connection is not made
// here: [Backend.start] dials, because a dial is I/O and everything above it
// has to be constructible without one.
func newBackend(cfg Config) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	proto, err := protocol.New(protocol.AMQP091)
	if err != nil {
		return nil, err
	}
	b := &Backend{
		cfg:   cfg.resolve(),
		errs:  newErrorMapper(),
		proto: proto,
		clock: cfg.Clock,
		topo:  newTopology(),
		dial:  client.Real{},
		sinks: map[*sink]struct{}{},
		srcs:  map[*source]struct{}{},
	}
	if b.clock == nil {
		b.clock = wallClock{}
	}
	b.health = driverkit.NewHealth(b.onHealth)
	b.ctx, b.cancel = context.WithCancel(context.Background())
	return b, nil
}

// onHealth forwards a health transition to the caller's hooks.
//
// The specification asks for a driver's background failures to surface through hooks and
// health rather than only in logs, and the maintenance notes records that
// `mq.Hooks` has no health-transition point at all — so the transition is
// reported through OnReconnect's event, which is the nearest thing the hook
// set has, and the state itself is readable through [Backend.Health]. When the
// hook set grows a health point this is the one place that has to change.
func (b *Backend) onHealth(snap mq.HealthSnapshot) {
	if b.cfg.Hooks.OnReconnect == nil {
		return
	}
	b.cfg.Hooks.OnReconnect(context.Background(), mq.ReconnectEvent{
		Driver: DriverName,
		// The attempt count is the dial loop's, and a health transition that
		// is not a dial (a `connection.blocked`, a Close) reports zero,
		// which is what the specification makes "the driver did not say".
		Attempt: b.attempts(),
		Err:     snap.Err,
	})
}

// attempts is how many dials the loop has made, or zero before it exists.
func (b *Backend) attempts() int {
	if b.rc == nil {
		return 0
	}
	return b.rc.Attempts()
}

// conn is the current connection. It is nil while a reconnection is in
// flight, and every caller has to cope with that rather than assume.
func (b *Backend) conn() client.Conn {
	b.connMu.RLock()
	defer b.connMu.RUnlock()
	return b.cn
}

// reconnectPending reports that the dial loop is on its way with a connection
// and will re-attach every handle when it arrives.
//
// It is the predicate that tells "a channel is coming for this handle" from
// "nothing is coming", which is the difference between a refusal a caller
// should retry and one it should answer by opening a new handle. It is LIVE
// state on purpose, and it is the union of two windows because
// `FuzzRabbitTagSettlement` found each of them in turn:
//
//   - NO USABLE CONNECTION. Either there is none, or the one there is has
//     closed and the supervision loop has not yet reacted. The reporter is
//     still Ready in that second window -- `closeconn` then `publish`, which
//     the fuzzer minimised to in three steps.
//   - A RECONNECTION IN FLIGHT. `redial` installs the new connection, replays
//     the topology and only then re-attaches the handles, so between the
//     install and the re-attach the connection is open and the handle is
//     still detached. The reporter is what knows: `Reconnector.Connect`
//     reports Ready only after the re-attach, which is what
//     `Health.Ready meaning what it says` in reconnect.go's step 4 is about.
//     The fuzzer reached it with two `closeconn`s in a row.
//
// What it replaced was a STICKY record of "the repair failed", which two
// separate races made say the opposite of the truth. Nothing here is stored,
// so nothing can be stale.
func (b *Backend) reconnectPending() bool {
	cn := b.conn()
	if cn == nil || cn.IsClosed() {
		return true
	}
	return b.health.State() != mq.HealthReady
}

// channel opens a channel on the current connection.
//
// It is the one place a channel is created, so that "there is no connection
// right now" has one answer: CodeUnavailable with Retryable set, which is what
// a reconnection in flight is. A caller that waited here instead would hold a
// goroutine for the whole outage.
func (b *Backend) channel() (client.Channel, error) {
	return b.channelOn(b.conn())
}

// channelOn opens a channel on cn, which the caller read with [Backend.conn].
//
// It is [Backend.channel] for a caller that needs to know WHICH connection its
// channel is on: a handle's attach records it, so that a later attach can tell
// a live channel on the current connection -- which it must leave alone --
// from one left behind on a connection that has since been replaced.
func (b *Backend) channelOn(cn client.Conn) (client.Channel, error) {
	if cn == nil {
		return nil, errReconnecting
	}
	ch, err := cn.Channel()
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, "", "", err)
	}
	return ch, nil
}

// channelOnWithin is [Backend.channelOn] with a bound on how long the broker
// may take to answer.
//
// `channel.open` is a round trip and the library's `Channel()` is synchronous
// with no deadline of its own, so a broker that holds the TCP connection open
// and then stops answering parks the caller for ever. **That park is not
// confined to the goroutine doing the waiting**, which is the reason this
// exists: every attach path holds its handle's `attachMu` across this call,
// and that handle's own loop needs the same mutex to report a channel it has
// just lost -- so the loop blocks, `wg.Wait()` in Close blocks on the loop,
// and Close never returns. CI found exactly that cycle, four-way, on
// `TestRedialDoesNotAttachASinkTheCoreIsClosing` in
// `drivers/rabbitmq/b18race_test.go`: that test's gate holds a redial's
// attach inside this very call, so the `attachMu` it holds is the one the
// sink's own loop then needs to report its lost channel. A real broker slow
// to open a channel produces the same stall, so it is a product hazard and
// not a test artefact.
//
// The bound is Config.DialTimeout rather than a new knob: that field already
// means "how long the broker may take over a connection-establishment round
// trip", and `channel.open` is one. The caller's context wins when it expires
// first, which is what makes a closing handle's bounded context the shorter
// bound on this path.
//
// A late channel is CLOSED rather than leaked. `Channel()` cannot be
// cancelled, so the goroutine stays until the library answers; whatever it
// hands back after the bound has expired belongs to nobody, and an orphaned
// channel on a live connection is the exact hazard the specification is about.
// The buffered send means that goroutine never blocks on a receiver that has
// gone.
func (b *Backend) channelOnWithin(ctx context.Context, cn client.Conn) (client.Channel, error) {
	if cn == nil {
		return nil, errReconnecting
	}
	type opened struct {
		ch  client.Channel
		err error
	}
	done := make(chan opened, 1)
	go func() {
		ch, err := cn.Channel()
		done <- opened{ch: ch, err: err}
	}()
	discardLate := func() {
		go func() {
			if o := <-done; o.ch != nil {
				_ = o.ch.Close()
			}
		}()
	}
	t := b.clock.NewTimer(b.cfg.DialTimeout)
	defer t.Stop()
	select {
	case o := <-done:
		if o.err != nil {
			return nil, b.errs.Map(mq.OpOpen, "", "", o.err)
		}
		return o.ch, nil
	case <-ctx.Done():
		discardLate()
		return nil, b.errs.Map(mq.OpOpen, "", "", ctx.Err())
	case <-t.C():
		discardLate()
		return nil, errChannelOpenTimeout
	}
}

// Info describes the driver and the library behind it. Every field the
// driver does not know is empty, which the specification makes "the driver did not say"
// rather than a claim.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		Profile:       "amqp091",
		DriverVersion: Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
		BrokerVersion: b.fx.version.String(),
	}
}

// Facts is what the connected broker and this client's configuration said,
// which is what every conditional capability claim is read off.
//
// It is exported because a claim that cannot be checked against its own
// evidence is a claim on trust: TestCapabilityClaimsFollowTheFacts reads this
// and the capability table together, and a driver page that says "the attempt
// count needs a quorum queue" has somewhere to point.
func (b *Backend) Facts() string {
	var sb []byte
	sb = append(sb, "version="...)
	sb = append(sb, b.fx.version.String()...)
	sb = append(sb, " quorum="...)
	sb = strconv.AppendBool(sb, b.fx.quorum)
	sb = append(sb, " declare="...)
	sb = strconv.AppendBool(sb, b.fx.declare)
	sb = append(sb, " deadletter="...)
	sb = strconv.AppendBool(sb, b.fx.deadLetter)
	sb = append(sb, " delayplugin="...)
	sb = strconv.AppendBool(sb, b.fx.delayPlugin)
	sb = append(sb, " delaykind="...)
	sb = append(sb, b.fx.delayKind.Resolve().String()...)
	sb = append(sb, " maxpriority="...)
	sb = strconv.AppendUint(sb, uint64(b.fx.maxPriority), 10)
	sb = append(sb, " keyasroutingkey="...)
	sb = strconv.AppendBool(sb, b.fx.keyAsRoutingKey)
	sb = append(sb, " mandatory="...)
	sb = strconv.AppendBool(sb, b.fx.mandatory)
	sb = append(sb, " frameMax="...)
	sb = strconv.AppendInt(sb, int64(b.fx.frameMax), 10)
	return string(sb)
}

// Blocked reports whether the broker has told this connection to stop
// publishing (`connection.blocked`).
//
// It is exported because it is the only explanation a stalled publisher has.
// RabbitMQ under a memory or disk alarm stops READING from a publishing
// connection, so a publication blocks in the socket with no error and no
// timeout of its own; without this a caller sees a hung Publish and nothing
// else. The specification asks for that to surface, and the hook fires on the transition.
func (b *Backend) Blocked() bool { return b.blocked.Load() }

// Ping is the liveness check it consumes no message and creates no
// topology.
//
// It opens a channel and closes it, which is one round trip to the broker and
// the cheapest thing AMQP 0-9-1 offers that proves the connection is alive
// end to end. The obvious alternative -- reading `conn.IsClosed()` -- proves
// only that the library has not noticed a failure yet, which is exactly the
// case a liveness check exists to catch.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	ch, err := b.channel()
	if err != nil {
		return err
	}
	if err := ch.Close(); err != nil {
		return b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	return nil
}

// Native returns the library's own connection, for a caller that needs the
// escape hatch.
//
// It also carries this driver's native request/reply: the backend implements
// [reqreply.NativeRequester] through direct reply-to, and `reqreply.New` finds
// it by type-asserting exactly this value.
func (b *Backend) Native() any { return b }

// Conn returns the current AMQP connection, or nil while a reconnection is in
// flight. It is this driver's typed escape hatch.
func (b *Backend) Conn() client.Conn { return b.conn() }

// OpenSink opens a publication channel to one topic.
func (b *Backend) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	var (
		names rabbit.Names
		err   error
	)
	if IsReplyAddress(cfg.Topic) {
		// A direct reply-to address is not a topic: it is a routing key the
		// broker minted, on the default exchange, under the reserved "amq."
		// prefix that `internal/rabbit` will not derive a name from. A
		// responder reaches it by publishing to
		// `Delivery.Properties.ReplyTo`, so the sink has to accept it --
		// this is the one place a topic bypasses the naming layer, and
		// `IsReplyAddress` is where it is documented.
		names = rabbit.Names{RoutingKey: cfg.Topic}
	} else if names, err = b.names(cfg.Topic, ""); err != nil {
		return nil, err
	}
	mode := cfg.Confirmation.Resolve()
	if mode == mq.ConfirmDurable && !b.fx.quorum {
		// The specification: a publisher asking for a durable boundary fails at
		// construction on a profile that cannot define one. A classic
		// non-durable queue returns a confirm once the broker holds the
		// message in memory, which is not a durable boundary, and this
		// driver will not pretend otherwise.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoDurableBoundary)
	}
	s, err := b.newSink(ctx, cfg, names, mode)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = s.Close()
		return nil, mq.ErrClosed
	}
	b.sinks[s] = struct{}{}
	b.mu.Unlock()
	return s, nil
}

// OpenSource opens a consumer on a subscription's queue.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Prefetch <= 0 {
		return nil, errUnresolvedPrefetch
	}
	switch cfg.Start.Resolve().Kind() {
	case mq.StartKindTime, mq.StartKindSequence:
		// A queue has no history and no position: a message that has not
		// been settled is at the head and one that has is gone. So there is
		// nothing to seek to, and refusing at open is what makes the
		// capability table's Unsupported provable -- a driver that accepted
		// the position and ignored it would leave a caller planning on a
		// replay that silently never happens.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, errNoSeek)
	case mq.StartKindUnset, mq.StartKindLatest, mq.StartKindEarliest:
		// Both resolve to the same place, because a queue has only one: the
		// head. A consumer that opens after a publication receives it, and
		// one that opens before receives it too.
	}
	names, err := b.names(cfg.Topic, cfg.Subscription)
	if err != nil {
		return nil, err
	}
	src, err := b.newSource(ctx, cfg, names)
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

// names derives the topology one topic and one subscription map to.
//
// A wildcard topic is accepted for a SUBSCRIPTION and refused for a
// publication, which is `internal/rabbit`'s asymmetry: a topic is its own
// routing key, so a wildcard in one would bind a queue to other topics'
// traffic, while a wildcard subscription is a legitimate binding key.
func (b *Backend) names(topic, sub string) (rabbit.Names, error) {
	if rabbit.HasWildcard(topic) {
		// A pattern is refused as UNSUPPORTED and not as an invalid
		// argument, because it is a thing this driver cannot do rather than
		// a thing the caller got wrong: a topic maps to its own exchange, so
		// a pattern over topic names would have to span exchanges. The
		// capability table says FeatureWildcardSubscribe is unsupported and
		// this is the refusal that proves it.
		return rabbit.Names{}, b.errs.Map(mq.OpOpen, topic, sub, errNoWildcards)
	}
	n, err := rabbit.Derive(rabbit.Config{
		Prefix:          b.cfg.Prefix,
		Topic:           topic,
		Subscription:    sub,
		KeyAsRoutingKey: b.cfg.KeyAsRoutingKey,
		DeadLetterTopic: b.cfg.DeadLetterTopic,
	})
	if err != nil {
		return rabbit.Names{}, b.errs.Map(mq.OpOpen, topic, sub, err)
	}
	return n, nil
}

// forget drops a closed child.
func (b *Backend) forget(s *sink, src *source) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s != nil {
		delete(b.sinks, s)
	}
	if src != nil {
		delete(b.srcs, src)
	}
}

// children is a snapshot of the open handles, taken under the lock and used
// outside it.
//
// Every caller of this function then calls back into a sink or a source, and
// doing that under b.mu would be a lock inversion: a sink's own close takes
// b.mu through forget. The specification lost the Kafka driver's first test to the same
// shape through `BaseBackend.Close` , and this is the
// driver-side half of that repair.
func (b *Backend) children() (sinks []*sink, srcs []*source) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sinks = make([]*sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	srcs = make([]*source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	return sinks, srcs
}

// teardown closes everything this backend opened, before the connection goes.
//
// The order is sources first and sinks second, and it matters: a source's
// close requeues what it was holding, and a requeue is a frame on the
// connection. A backend that closed the connection first would leave those
// deliveries to the broker's own redelivery instead, which is correct but
// slower by a lease.
func (b *Backend) teardown() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cancel()

	sinks, srcs := b.children()
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
	b.wg.Wait()
	b.topo.forget()
	return first
}

// closeConn closes the current connection, if there is one.
func (b *Backend) closeConn() error {
	b.connMu.Lock()
	cn := b.cn
	b.cn = nil
	b.connMu.Unlock()
	if cn == nil {
		return nil
	}
	return cn.Close()
}

// confirmStrategy builds the publisher-confirm strategy this client uses
func (b *Backend) confirmStrategy() (confirm.Strategy, error) {
	return confirm.New(b.cfg.ConfirmKind, confirm.Config{Window: b.cfg.ConfirmWindow})
}

// delayStrategy builds the delayed-redelivery strategy this client uses
// after the detection at open has settled which arm is
// available.
func (b *Backend) delayStrategy() (delay.Strategy, error) {
	return delay.New(b.fx.delayKind, delay.Config{
		Ladder:   b.cfg.DelayLadder,
		QueueTTL: b.cfg.DelayQueueTTL,
	})
}

// options is the protocol mapping's option set for one handle.
func (b *Backend) options(meta mq.MetadataMode) protocol.Options {
	o := protocol.Options{
		Metadata:   meta,
		Persistent: true,
		Limits:     b.caps.Limits(),
	}
	if !b.cfg.KeyAsRoutingKey {
		o.KeyHeader = KeyHeader
	}
	return o
}

// nextSeq is the next number for a consumer tag or a correlation token.
func (b *Backend) nextSeq() uint64 { return b.seq.Add(1) }

// blockingTable is the client-properties table this connection advertises.
//
// `connection_name` is what `rabbitmqctl list_connections` shows, and
// `product`/`version` are what a broker's own management view attributes a
// connection to. A deployment with a hundred connections can find one
// application's.
func (b *Backend) clientProperties() amqp.Table {
	t := amqp.Table{
		"product":  "iqmq/" + DriverName,
		"version":  Version,
		"platform": "Go",
		// `capabilities` is what the broker reads to decide whether it may
		// send this client a `basic.cancel` when a queue is deleted, and
		// whether `connection.blocked` may be sent at all. Both are things
		// this driver handles, and a client that did not advertise them
		// would silently not be told.
		"capabilities": amqp.Table{
			"connection.blocked":         true,
			"consumer_cancel_notify":     true,
			"basic.nack":                 true,
			"publisher_confirms":         true,
			"exchange_exchange_bindings": true,
		},
	}
	if b.cfg.ConnectionName != "" {
		t["connection_name"] = b.cfg.ConnectionName
	}
	return t
}

// wallClock is the default time source.
//
// It is the driver's own rather than driverkit's, which keeps that package's
// single reviewed `time.Now` exception where it is: a driver module is outside
// forbidigo's perf-critical set, so reading the clock here needs no exception
// at all. Everything on a path that matters takes the injected
// [mq.Clock] instead, which is what makes a delay's accuracy and a
// reconnection's backoff testable without waiting.
type wallClock struct{}

// Now is the current time.
func (wallClock) Now() time.Time { return time.Now() }

// NewTimer is a timer on the wall clock.
func (wallClock) NewTimer(d time.Duration) mq.Timer { return &wallTimer{t: time.NewTimer(d)} }

// wallTimer adapts *time.Timer to mq.Timer.
type wallTimer struct{ t *time.Timer }

// C is the timer's channel.
func (w *wallTimer) C() <-chan time.Time { return w.t.C }

// Stop stops the timer.
func (w *wallTimer) Stop() bool { return w.t.Stop() }

// Reset restarts the timer.
func (w *wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

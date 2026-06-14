package rabbitmq

import (
	"context"
	"errors"
	"io"
	"net"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit"
	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"bitbucket.org/iqhive/iqmq/internal/rabbit"
	"bitbucket.org/iqhive/iqmq/internal/rabbit/delay"
)

// start makes the first connection and begins the supervision loop.
//
// It is separate from [newBackend] because it performs I/O and everything
// above it has to be constructible without any: the configuration's own
// validation, the capability table's inputs and the error mapping are all
// decided before a socket exists, so a typo is refused in microseconds rather
// than after a dial timeout.
func (b *Backend) start(ctx context.Context, own driverkit.Ownership) error {
	rc, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
		Dial: b.redial,
		// The handles' invalidation is the helper's now, and the helper runs
		// it between the generation bump and the first dial attempt -- which
		// is the order `repair` used to arrange by hand and the order the specification
		// depends on.
		Invalidate:  b.detachChildren,
		Base:        b.cfg.ReconnectBase,
		Max:         b.cfg.ReconnectMax,
		Growth:      b.cfg.ReconnectGrowth,
		Policy:      b.cfg.BackoffPolicy,
		Clock:       b.clock,
		MaxAttempts: b.cfg.MaxReconnects,
		OnReconnect: b.cfg.Hooks.OnReconnect,
		Health:      b.health,
		Driver:      DriverName,
	})
	if err != nil {
		return err
	}
	b.rc = rc

	// The FIRST dial is bounded by the caller's context and by MaxAttempts,
	// and it is deliberately the same loop as a reconnection: a broker that
	// is starting up when a process starts up is the common case, and a
	// driver whose first dial did not retry would make every deployment's
	// start order load-bearing.
	if err := rc.Connect(ctx); err != nil {
		b.health.Degraded(err)
		return b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	// The broker's own facts are read ONCE, here, and never again. It used to
	// happen inside `redial`, and that was wrong twice over.
	//
	// It was a DATA RACE: `b.fx` is a plain struct with no lock, `redial`
	// rewrote the whole of it on every reconnection, and by then handles
	// exist and read it -- `source.admit` reads `b.fx.quorum` on the pump
	// goroutine for every delivery it maps. Nothing ordered the two.
	//
	// It was also a REGRESSION against [Backend.probe]'s own rule. Everything
	// `readFacts` sets except the version comes from `b.cfg`, which cannot
	// change; the version cannot change what this client CLAIMS, because
	// The specification makes the capability table constant per client and `b.caps` is
	// derived from `b.fx` a few lines below and never again. What a
	// reconnection did change is `b.fx.delayKind`, which `probe` UPGRADES to
	// `delay.Plugin` when it detects the plugin -- and a re-read put it back
	// to `b.cfg.DelayKind`, so a client that had detected the plugin silently
	// fell back to the TTL+DLX arm after its first outage, which is the very
	// thing `probe`'s comment says must not happen.
	cn := b.conn()
	if cn == nil {
		// The dial succeeded, so this cannot happen; answering it is
		// cheaper than assuming it.
		return b.errs.MapNet(mq.OpOpen, "", "", errReconnecting, true)
	}
	b.readFacts(cn)
	if err := b.probe(ctx); err != nil {
		_ = b.closeConn()
		return err
	}

	caps, err := capabilities(b.fx)
	if err != nil {
		_ = b.closeConn()
		return err
	}
	b.caps = caps
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       b,
		Ownership:    own,
		CloseNative:  b.closeConn,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		_ = b.closeConn()
		return err
	}
	b.BaseBackend = base

	b.wg.Add(1)
	go b.supervise()
	b.health.Ready()
	return nil
}

// redial is the [driverkit.Reconnector]'s Dial step, and it is the whole of
// what this driver's reconnection means.
//
// It is one function because the four things it does have to happen in this
// order and a failure in any of them has to be answered by another attempt
// rather than by a half-restored client:
//
//  1. **Dial.** The endpoints are tried in order; the first that answers wins.
//  2. **Watch.** The connection's own close and its `connection.blocked`
//     notifications are registered before anything is published, because a
//     connection that failed between the dial and the registration would never
//     be noticed.
//  3. **Replay the topology.** Everything this client declared is declared
//     again, in the order it was declared. The specification asks for exactly this, and it
//     is not merely tidiness: the durable exchanges and quorum queues survive,
//     and the DELAY queues may not — `x-expires` deletes an unused one — so a
//     client that did not replay would come back unable to delay a
//     redelivery.
//  4. **Re-attach the handles.** Every sink gets a new confirm-mode channel
//     and every source a new consumer with its prefetch. Until this has
//     happened the client is connected and useless, so it happens inside the
//     dial step rather than after it: `Reconnector.Connect` does not return
//     until the client can work again, and `Health.Ready` therefore means
//     what it says.
//
// The client library can do (1) to (4) itself since v1.14, and this driver
// does not use that. The reasons are in `internal/client`'s package comment;
// the one that decides it is that a reconnection invalidates every delivery
// tag this client has handed out, and the driver has to bump its own
// generation at exactly that moment or a settlement for an old delivery lands
// on whatever took its tag.
func (b *Backend) redial(ctx context.Context) error {
	cn, err := b.dial.Dial(ctx, b.cfg.urls(), client.DialConfig{
		Vhost:      b.cfg.Vhost,
		Heartbeat:  b.cfg.Heartbeat,
		Timeout:    b.cfg.DialTimeout,
		ChannelMax: b.cfg.ChannelMax,
		FrameSize:  b.cfg.FrameSize,
		TLS:        b.cfg.TLS,
		Properties: b.clientProperties(),
	})
	if err != nil {
		return err
	}

	b.connMu.Lock()
	b.cn = cn
	b.connMu.Unlock()
	b.blocked.Store(false)

	// The topology replay and the re-attachment share one throwaway channel
	// for the declarations, because a declaration that fails takes its
	// channel with it and the handles' own channels must not be the ones
	// that die.
	if len(b.topo.all()) > 0 {
		ch, cerr := cn.Channel()
		if cerr != nil {
			_ = cn.Close()
			return cerr
		}
		if rerr := b.replay(ctx, ch); rerr != nil {
			_ = ch.Close()
			_ = cn.Close()
			return rerr
		}
		if cerr := ch.Close(); cerr != nil {
			_ = cn.Close()
			return cerr
		}
	}

	// `children` is a SNAPSHOT, and a handle in it can be closed by its owner
	// at any moment -- the core closes a publisher from a `t.Cleanup`, a
	// defer or a shutdown, and it does not and should not wait for a dial
	// loop it cannot see. So `mq.ErrClosed` from a re-attach is the normal
	// answer to "that handle is gone" and not a failure of this dial: the
	// handle has already detached its publications, resolved its waiters and
	// dropped itself from `b.sinks`, and there is nothing left to restore.
	// Treating it as a dial failure threw away a perfectly good connection
	// and spent another attempt to reach the same state.
	sinks, srcs := b.children()
	for _, s := range sinks {
		if aerr := s.attach(ctx); aerr != nil {
			if errors.Is(aerr, mq.ErrClosed) {
				continue
			}
			_ = cn.Close()
			return aerr
		}
	}
	for _, s := range srcs {
		// `reattach` and not `attach`: a source whose first Fetch has not
		// happened has no consumer to restore, and starting one here would
		// make an outage the thing that began the delivery flow.
		if aerr := s.reattach(ctx); aerr != nil {
			if errors.Is(aerr, mq.ErrClosed) {
				continue
			}
			_ = cn.Close()
			return aerr
		}
	}
	return nil
}

// readFacts reads what the connected broker said, which is the input to every
// version-gated capability claim.
//
// It runs ONCE, from [Backend.start], on the goroutine that is opening the
// client and before any handle exists. `b.fx` is immutable from then on, which
// is what makes it safe to read without a lock from every handle: see the
// comment at its one call site for what re-reading it on a reconnection cost.
func (b *Backend) readFacts(cn client.Conn) {
	props := cn.ServerProperties()
	b.fx.version, _ = rabbit.ParseVersion(stringProp(props, "version"))
	b.fx.quorum = b.cfg.QueueType == QueueQuorum && b.cfg.MaxPriority == 0
	b.fx.declare = b.cfg.Declare
	b.fx.deadLetter = b.cfg.DeadLetter
	b.fx.deadLetterTopic = b.cfg.DeadLetterTopic
	b.fx.delayKind = b.cfg.DelayKind
	b.fx.maxPriority = b.cfg.MaxPriority
	b.fx.keyAsRoutingKey = b.cfg.KeyAsRoutingKey
	b.fx.mandatory = b.cfg.Mandatory
	b.fx.frameMax = b.cfg.FrameSize
}

// stringProp reads one entry of the broker's `connection.start` table.
//
// The table's values are AMQP field-table values, so a version can arrive as a
// string or as a byte array depending on the broker, and a driver that
// asserted the one it happened to see in a test would read "" from the other
// and satisfy no version gate at all. That is the shape of the lesson in
// miniature.
func stringProp(t amqp.Table, key string) string {
	switch v := t[key].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

// probe is the one-off detection at open: whether the broker has the
// delayed-message exchange, and therefore which arm this
// client uses.
//
// It runs once, at open, because the specification makes a capability table constant per
// client: a client that discovered the plugin later would have to change a
// table a caller has already read. A reconnection does NOT re-probe, and that
// is deliberate — a broker that lost the plugin across a restart would
// otherwise silently change what this client claims.
func (b *Backend) probe(ctx context.Context) error {
	if !b.cfg.DetectDelayPlugin {
		if b.cfg.DelayKind.Resolve() == delay.Plugin {
			// The configuration asked for the plugin outright. Believing it
			// without a probe is what [Config.DetectDelayPlugin] exists to
			// avoid, and refusing it would refuse a deployment that knows
			// its own broker, so the claim is made and the first delayed
			// nack is what finds out.
			b.fx.delayPlugin = true
		}
		return nil
	}
	// The probe needs a name to declare, and the delay exchange's name is
	// derived from a TOPIC -- which at open there is not one of. The probe
	// therefore uses a reserved probe topic under the configured prefix,
	// which is a real exchange this client then owns; naming it after the
	// driver is what makes it recognisable in a broker's own exchange list.
	n, err := b.names(delayProbeTopic, "")
	if err != nil {
		return err
	}
	ok, perr := b.detectDelayPlugin(ctx, n)
	if perr != nil {
		return perr
	}
	b.fx.delayPlugin = ok
	if ok && b.cfg.DelayKind == delay.Auto {
		// Auto resolves to the arm that needs no plugin, because Auto is
		// what a caller gets by not choosing and a default that assumed a
		// plugin would fail at open on a stock broker. With the plugin
		// really present, the detection upgrades -- which is
		// "plugin when detected, TTL+DLX fallback" exactly.
		b.fx.delayKind = delay.Plugin
	}
	return nil
}

// delayProbeTopic is the topic the delayed-message probe declares its exchange
// under.
//
// It is a topic and not a bare exchange name so that the probe goes through
// the same naming as everything else: a deployment with a prefix gets a
// prefixed probe exchange, and one without gets `iqmq.delay-probe.delayed`.
const delayProbeTopic = "iqmq.delay-probe"

// supervise is the goroutine that notices a lost connection and repairs it.
//
// It is the half of the reconnection `driverkit.Reconnector` does NOT provide,
// and the division is the right one: the helper owns the backoff, the attempt
// counting, the health transitions and the hook, and the driver owns knowing
// that something has broken. Only the driver can know that, because "broken"
// is a notification from a client library and every library spells it
// differently.
//
// The order a loss goes through is the part that has to be right, and since
// The specification it is the helper that arranges it: [driverkit.Reconnector.Lost] bumps
// the generation, calls [Backend.detachChildren] -- which is where the sinks'
// and sources' own order is written down -- and reports Degraded. `repair`
// then runs the dial loop, which re-dials, replays the topology and re-attaches
// every handle.
func (b *Backend) supervise() {
	defer b.wg.Done()
	for {
		cn := b.conn()
		if cn == nil {
			// A reconnection is in flight, or the client is closing. Either
			// way this loop has nothing to watch.
			if b.ctx.Err() != nil {
				return
			}
		}
		lost := make(chan *amqp.Error, 1)
		blocked := make(chan amqp.Blocking, 4)
		if cn != nil {
			cn.NotifyClose(lost)
			cn.NotifyBlocked(blocked)
		}
		for watching := true; watching; {
			select {
			case <-b.ctx.Done():
				return
			case blk, ok := <-blocked:
				if !ok {
					blocked = nil
					continue
				}
				b.onBlocked(blk)
			case aerr := <-lost:
				if b.ctx.Err() != nil {
					return
				}
				b.repair(reasonOf(aerr))
				watching = false
			}
		}
	}
}

// onBlocked records `connection.blocked` and `connection.unblocked`.
//
// RabbitMQ under a memory or disk alarm stops READING from a publishing
// connection, so a publication blocks in the socket with no error of its own.
// Recording it is what turns an unexplained stall into a reportable condition
// (NotifyBlocked): the health state goes Degraded with the
// broker's own reason, and Ready again when the alarm clears.
func (b *Backend) onBlocked(blk amqp.Blocking) {
	b.blocked.Store(blk.Active)
	if blk.Active {
		b.health.Degraded(&blockedError{reason: blk.Reason})
		return
	}
	b.health.Ready()
}

// blockedError is `connection.blocked` as an error, so that the health
// snapshot carries the broker's own reason.
//
// The specification forbids a payload, a credential or an address in an error, and the
// broker's reason is none of those: it is a word such as "low on disk space".
type blockedError struct{ reason string }

// Error implements error.
func (e *blockedError) Error() string {
	return "iqmq/rabbitmq: the broker has blocked this connection: " + e.reason
}

// Unwrap makes it Backpressure, which is what it is to a publisher: the broker
// is refusing to read, so the right response is to slow down rather than to
// retry faster.
func (e *blockedError) Unwrap() error { return mq.ErrBackpressure }

// detachChildren is the [driverkit.ReconnectConfig.Invalidate] seam: everything
// this client was holding when the connection died.
//
// The order inside it is load-bearing and is the driver's own, which is why the
// helper takes a function rather than doing this itself:
//
//  1. Every sink detaches, which resolves the publications it was holding as
//     AMBIGUOUS. It is first because a caller blocked in Publish is the one
//     waiting on an answer, and the answer has to be "the broker may have taken
//     it" rather than a timeout.
//  2. Every source invalidates, which bumps its token generation and discards
//     the deliveries it had buffered. It is second because a settlement
//     carrying a token from before the bump must be refused, and a delivery
//     that was never handed to a caller must not be handed out after the broker
//     has already taken it back.
//
// [driverkit.Reconnector.Lost] calls it after bumping the generation a caller
// reads through Health and the hook and before the Degraded report, so nothing
// observes a client whose generation still names a connection whose handles are
// already dead.
func (b *Backend) detachChildren(cause error) {
	sinks, srcs := b.children()
	for _, s := range sinks {
		s.detach(cause)
	}
	for _, s := range srcs {
		s.invalidate()
	}
}

// repair answers a lost connection.
//
// It is called from the supervision loop alone, so nothing in it has to be
// re-entrant, and it returns once the client works again or the client is
// closing.
func (b *Backend) repair(cause error) {
	b.connMu.Lock()
	b.cn = nil
	b.connMu.Unlock()

	// One call: the helper bumps the generation, runs `detachChildren` and
	// reports Degraded, in that order. This used to be fifteen lines here and
	// the same fifteen in two other drivers.
	b.rc.Lost(cause)

	if err := b.rc.Connect(b.ctx); err != nil {
		// The loop gave up: the context is done (the client is closing) or
		// MaxReconnects was reached. Either way this client cannot work
		// again, and saying so through health is the only honest answer --
		// a driver that kept trying for ever with MaxReconnects set would
		// be ignoring the bound its caller asked for.
		b.health.Degraded(err)
	}
}

// reasonOf turns the library's close notification into an error.
//
// A nil notification means the connection was closed by this client, which is
// not a failure; the supervision loop still has to repair nothing and return,
// and the distinction is what keeps a deliberate Close from being reported as
// an outage.
func reasonOf(aerr *amqp.Error) error {
	if aerr == nil {
		return errClosedChannel
	}
	return aerr
}

// waitDeadline gives a context a deadline when the caller did not, which is
// what a round trip needs in order not to wait for ever on a link that is up
// and silent.
//
// A caller who has a deadline keeps it: a driver that added a second one would
// shorten a deadline the caller chose. It is the same rule every driver in
// this repository follows.
func (b *Backend) waitDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || b.cfg.PublishTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.cfg.PublishTimeout)
}

// isTransportFailure reports whether an error means the channel or the
// connection has gone, rather than that the broker refused the operation.
//
// The distinction decides whether a publication's outcome is AMBIGUOUS. A
// broker that refused a publication definitely did not store it; a socket that
// failed mid-frame may have delivered every byte. The specification makes the second
// ambiguous and the specification names reporting it as a definite failure a settlement
// disqualifier.
func isTransportFailure(err error) bool {
	if err == nil {
		return false
	}
	if code, ok := ReplyCode(err); ok {
		// A reply code means the broker answered, so the outcome is
		// definite -- except for the three that mean the conversation
		// itself broke down.
		switch code {
		case amqp.ChannelError, amqp.ConnectionForced, amqp.FrameError,
			amqp.UnexpectedFrame:
			return true
		default:
			return false
		}
	}
	// Everything else is a TRANSPORT failure only if it really is one. The
	// tempting fallback -- "anything that is not a cancellation" -- is wrong
	// in the direction that costs most: this driver's own definite refusals
	// would then be reported as unknown outcomes, and the specification makes an unknown
	// settlement STICKY, so a delivery whose delayed redelivery the broker
	// refused would become permanently unsettleable instead of being
	// retried. TestCriticalDelayedNackDoesNotLoseTheMessageWhenTheParkFails
	// is what caught it.
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return true
	case errors.Is(err, net.ErrClosed):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		return true
	}
	var nerr net.Error
	return errors.As(err, &nerr)
}

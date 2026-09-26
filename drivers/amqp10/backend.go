package amqp10

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/amqp10/internal/client"
)

// The compile-time proof of what this backend is. Asserting it here is what
// makes a change to any of these a build failure rather than a surface that
// quietly stops being offered.
//
// Two absences are as deliberate as the presences, and both are about telling
// the core the truth at open:
//
//   - There is no unconditional [github.com/iqhive/iqmq.AdminBackend]. AMQP 1.0
//     can attach to an address and cannot create one, so a backend whose
//     profile supplies no topology surface must NOT satisfy the interface at
//     all — a backend that satisfied it and answered ErrUnsupported would make
//     FeatureAdmin look conditionally available to a core that asserts once.
//     admin.go builds the variant that does when [Profile.Admin] is set.
//   - There is no [github.com/iqhive/iqmq.DeadlineExtender] anywhere in this
//     module. AMQP 1.0 grants no lease: an unsettled delivery is the receiver's
//     for as long as the link lives, so there is no deadline and nothing to
//     extend. A driver that offered one would be answering ErrUnsupported to
//     every call for a broker whose management console really can extend a
//     message's delivery timeout — the protocol just has no way to ask.
var (
	_ mq.Backend        = (*Backend)(nil)
	_ mq.HealthReporter = (*Backend)(nil)
)

// Backend is the AMQP 1.0 [github.com/iqhive/iqmq.Backend].
//
// It owns the connection, the dial loop, the capability table, the error
// mapping, the health state machine and the links its sinks and sources have
// attached. One connection serves every sink and source: AMQP 1.0 multiplexes
// sessions and links over a single stream, so a client that dialled per
// publisher would spend file descriptors and connection slots to no purpose —
// and a broker's per-connection limits are what it would run into first.
type Backend struct {
	*driverkit.BaseBackend
	cfg     Config
	profile Profile
	errs    *driverkit.ErrorMapper
	caps    mq.Capabilities
	facts   Facts
	clock   mq.Clock
	dialer  client.Dialer
	addrs   []string

	// health is held separately from the base's copy so that the dial loop
	// can drive it before the base exists: a dial that fails has to report
	// Degraded and there is no base yet.
	health *driverkit.Health

	// reconn supplies the backoff, the generation and the OnReconnect hook.
	// The generation is the kit's and is the ONLY one: see links.go for why
	// two counters would be one too many.
	reconn *driverkit.Reconnector

	// cmu guards the current connection. It is taken for a pointer read and
	// never held across any call into the client library.
	cmu      sync.Mutex
	conn     client.Conn
	connErr  error
	addrNext int

	// supervisor
	superCtx    context.Context
	superCancel context.CancelFunc
	superWG     sync.WaitGroup

	// mu guards the open children. A backend closes what it opened, which is the obligation and the reason a parked Fetch is woken rather than left
	// holding a goroutine after Close.
	mu     sync.Mutex
	sinks  map[*Sink]struct{}
	srcs   map[*Source]struct{}
	closed bool

	counters
}

// counters are the backend's own diagnostics: the things the specification asks a driver to
// surface and the portable API cannot see.
type counters struct {
	// reconnects is how many times the connection was re-established.
	reconnects atomic.Uint64
	// losses is how many times the connection died.
	losses atomic.Uint64
	// stale is how many settlements the staleness guard refused.
	stale atomic.Uint64
	// undecodable is how many deliveries were released because this driver
	// could not represent them.
	undecodable atomic.Uint64
	// lossyFields is how many message fields a decode could not represent.
	lossyFields atomic.Uint64
	// ambiguous is how many publications ended with an unknown outcome.
	ambiguous atomic.Uint64
}

// Stats is the backend's own diagnostics.
//
// It is exported for the reason [CreditStats] is: every number here describes
// something the portable API cannot show, and a deployment whose stale count is
// rising has a handler that outlives its link.
type Stats struct {
	// Reconnects is how many times the dial loop re-established the
	// connection.
	Reconnects uint64
	// Losses is how many times the connection died.
	Losses uint64
	// StaleSettlements is how many settlements the staleness guard refused
	// with ErrLeaseLost, which is a handler that outlived its delivery's
	// link.
	StaleSettlements uint64
	// Undecodable is how many deliveries were released back to the broker
	// because this driver could not represent them as an iqmq message.
	Undecodable uint64
	// LossyFields is how many individual message fields a decode could not
	// represent. Unlike Undecodable the delivery was still handed over: what
	// was lost is one property or one header.
	LossyFields uint64
	// AmbiguousPublications is how many publications ended with an outcome
	// nobody knows.
	AmbiguousPublications uint64
}

// Stats returns the backend's diagnostics.
func (b *Backend) Stats() Stats {
	return Stats{
		Reconnects:            b.reconnects.Load(),
		Losses:                b.losses.Load(),
		StaleSettlements:      b.stale.Load(),
		Undecodable:           b.undecodable.Load(),
		LossyFields:           b.lossyFields.Load(),
		AmbiguousPublications: b.ambiguous.Load(),
	}
}

// Profile is the broker profile this backend was opened with, so that a caller
// or a test can read the addressing and the rules back.
func (b *Backend) Profile() Profile { return b.profile }

// Facts is what the connected peer said, which is what every conditional
// capability claim and every version gate was read off.
func (b *Backend) Facts() Facts { return b.facts }

// init finishes construction once a connection exists: the peer's facts, the
// capability table, the error mapping, the base and the supervisor.
func (b *Backend) init(ctx context.Context, conn client.Conn, own driverkit.Ownership) error {
	b.conn = conn
	b.clock = b.cfg.Clock
	if b.clock == nil {
		b.clock = wallClock{}
	}
	b.sinks, b.srcs = map[*Sink]struct{}{}, map[*Source]struct{}{}

	b.facts = factsOf(conn)
	if b.profile.CheckBroker != nil {
		if err := b.profile.CheckBroker(b.facts); err != nil {
			return err
		}
	}
	caps, err := capabilities(b.profile, b.facts)
	if err != nil {
		return err
	}
	b.caps = caps
	b.errs = newErrorMapper(b.profile, b.cfg.ReceiverSettle)

	reconn, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
		Dial: b.redial,
		// Steps 1 and 2 of reconnect.go's sequence are one call now: the
		// helper runs this between the generation bump and the first dial
		// attempt, which is the order this driver used to arrange by hand and
		// the order the specification depends on.
		Invalidate:  b.invalidate,
		Base:        b.cfg.ReconnectBase,
		Max:         b.cfg.ReconnectMax,
		Growth:      b.cfg.ReconnectGrowth,
		Policy:      b.cfg.Backoff.kind(),
		Clock:       b.clock,
		MaxAttempts: b.cfg.ReconnectAttempts,
		OnReconnect: b.onReconnect,
		Health:      b.health,
		Driver:      b.profile.Driver,
	})
	if err != nil {
		return err
	}
	b.reconn = reconn

	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         b.info(),
		Capabilities: caps,
		Native:       client.Native(conn),
		Ownership:    own,
		CloseNative:  b.closeNative,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return err
	}
	b.BaseBackend = base
	b.health.Ready()

	// The supervisor's context is the BACKEND's and not the caller's. The specification
	// forbids retaining a caller's context beyond its operation, and a dial
	// loop that lived on the context Open was given would stop reconnecting
	// the moment the caller cancelled the context it opened with — which for
	// a request-scoped context is immediately.
	b.superCtx, b.superCancel = context.WithCancel(context.WithoutCancel(ctx))
	b.superWG.Add(1)
	go b.supervise()
	return nil
}

// factsOf reads the capability table's inputs off the connected peer.
//
// Every AMQP 1.0 broker announces itself in the OPEN frame's connection
// properties, and "product" and "version" are the two conventional keys. A peer
// that sends neither leaves both empty, which the specification makes "the peer did not say"
// rather than a claim — and a profile whose version gate reads an empty string
// has to decide what to do about it, which is the profile's decision and not
// this core's.
func factsOf(conn client.Conn) Facts {
	props := conn.Properties()
	return Facts{
		Product:    propString(props, "product"),
		Version:    propString(props, "version"),
		Platform:   propString(props, "platform"),
		Properties: props,
	}
}

// propString reads one connection property as a string, accepting the two
// shapes a peer may encode it in.
func propString(props map[string]any, key string) string {
	switch v := props[key].(type) {
	case string:
		return v
	case amqp.Symbol:
		return string(v)
	case []byte:
		return string(v)
	default:
		return ""
	}
}

// info describes the driver and the library behind it.
//
// Every field the driver does not know is empty, which the specification makes "the driver
// did not say" rather than a claim — and BrokerVersion is exactly that: it is
// the peer's own "version" property when it sent one and empty when it did not,
// never a guess from a condition symbol or a feature probe.
func (b *Backend) info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          b.profile.Driver,
		Profile:       b.profile.profileName(),
		DriverVersion: b.profile.Version,
		ClientName:    ClientModule,
		ClientVersion: ClientVersion,
		BrokerVersion: b.facts.Version,
	}
}

// connection returns the current connection and its generation.
//
// The two are read together under one lock, which is what makes the generation
// the connection's own: reading them separately would let a caller attach a link
// on one connection and tag it with another's generation, and a delivery tagged
// with a generation it did not come from is a staleness guard that answers the
// wrong question.
func (b *Backend) connection() (client.Conn, uint32, error) {
	b.cmu.Lock()
	conn, cerr := b.conn, b.connErr
	b.cmu.Unlock()
	if conn == nil {
		if cerr != nil {
			return nil, 0, cerr
		}
		return nil, 0, mq.ErrClosed
	}
	return conn, b.reconn.Generation(), nil
}

// Generation is the connection generation. It changes on every loss, and a
// delivery handed out under an older one is stale.
func (b *Backend) Generation() uint32 { return b.reconn.Generation() }

// Ping is the liveness check it consumes no message and creates no
// topology.
//
// AMQP 1.0 has no application-level ping. What it has is an idle timeout, which
// the LIBRARY answers with an empty frame on its own, so there is nothing a
// driver can send that a broker is obliged to answer. This therefore reports
// whether the connection this client holds is alive, which is what the question
// means for a protocol whose connection is long-lived — and it is honest about
// costing no round trip, which the specification's "cheap" asks for.
//
// A backend whose connection has died and whose dial loop has not re-established
// it yet reports the connection's own error, mapped: CodeUnavailable and
// retryable, which is exactly what a caller polling liveness wants to see.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	conn, _, err := b.connection()
	if err != nil {
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	if cerr := conn.Err(); cerr != nil {
		return b.errs.Map(mq.OpOpen, "", "", cerr)
	}
	select {
	case <-conn.Done():
		return b.errs.Map(mq.OpOpen, "", "", conn.Err())
	default:
	}
	return nil
}

// Native returns the underlying *amqp.Conn, or nil when the backend was built
// over something that is not one — which in practice means a test's scripted
// peer.
//
// It is the escape hatch the specification asks for, and it is deliberately the CONNECTION
// rather than a session or a link: a caller who needs something this driver does
// not wrap needs to open its own session, and handing out one of the driver's
// would let it be closed underneath a sink.
func (b *Backend) Native() any {
	conn, _, err := b.connection()
	if err != nil {
		return nil
	}
	if c := client.Native(conn); c != nil {
		return c
	}
	return nil
}

// Conn returns the current connection under the driver's own narrow interface,
// for a caller that has to reach past the SPI without knowing whether the peer
// is real.
func (b *Backend) Conn() client.Conn {
	conn, _, err := b.connection()
	if err != nil {
		return nil
	}
	return conn
}

// closeNative closes the connection iqmq owns.
func (b *Backend) closeNative() error {
	b.cmu.Lock()
	conn := b.conn
	b.cmu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// teardown closes everything this backend opened, before the connection goes.
//
// It is what wakes a Fetch that is parked in a source nobody closed: the specification's
// "Close unblocks blocked operations" has to reach a caller who closed the
// client rather than the consumer.
//
// The order is deliberate. The supervisor is stopped FIRST, so that nothing
// re-dials underneath a teardown and the generation stops moving; then the
// sources, because a source's close RELEASES what it was holding and that is a
// disposition on a live link; then the sinks, because a sink's close resolves
// the asynchronous publications it still holds and those need the same
// connection. Reversing the last two would leave a source's releases to the
// detach, which works and is slower: the node waits for the link to go rather
// than being told.
func (b *Backend) teardown() error {
	if b.superCancel != nil {
		b.superCancel()
	}
	b.superWG.Wait()

	b.mu.Lock()
	b.closed = true
	srcs := make([]*Source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*Sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
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

// forget drops a closed child.
func (b *Backend) forget(src *Source, snk *Sink) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if src != nil {
		delete(b.srcs, src)
	}
	if snk != nil {
		delete(b.sinks, snk)
	}
}

// now reads the injected clock.
func (b *Backend) now() time.Time { return b.clock.Now() }

// bound gives a context a deadline when the caller did not, which is what an
// attach, a detach or a disposition round trip needs in order not to wait for
// ever on a link that is up and silent.
//
// A caller who has a deadline keeps it: a driver that added a second one would
// shorten a deadline the caller chose.
func (b *Backend) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok || b.cfg.OperationTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.cfg.OperationTimeout)
}

// wallClock is the default time source: package time, which follows the bubble
// clock inside testing/synctest.
type wallClock struct{}

// Now returns the current time.
func (wallClock) Now() time.Time { return time.Now() }

// NewTimer returns a timer that fires once after d.
func (wallClock) NewTimer(d time.Duration) mq.Timer { return &wallTimer{t: time.NewTimer(d)} }

// wallTimer adapts *time.Timer to mq.Timer.
type wallTimer struct{ t *time.Timer }

// C returns the channel the expiry is delivered on.
func (w *wallTimer) C() <-chan time.Time { return w.t.C }

// Stop prevents the timer from firing.
func (w *wallTimer) Stop() bool { return w.t.Stop() }

// Reset re-arms the timer.
func (w *wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

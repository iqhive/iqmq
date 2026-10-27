package stomp

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/backoff"
)

// wallClock is the default time source.
//
// It is the driver's own rather than driverkit's, which keeps that package's
// single reviewed `time.Now` exception where it is: a driver module is outside
// forbidigo's perf-critical set, so reading the clock here needs no exception
// at all. Everything on a path that matters takes the injected [mq.Clock]
// instead, which is what makes a delay's accuracy and a reconnection's backoff
// testable without waiting -- and it matters more here than in most drivers,
// because STOMP's `expires` header is an absolute instant and a TTL is
// resolved against this clock on every publication.
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

// Reset re-arms the timer.
func (w *wallTimer) Reset(d time.Duration) bool { return w.t.Reset(d) }

// Backend is the SPI backend.
//
// # It owns its dial loop
//
// go-stomp does not reconnect: a connection whose socket died is a
// `*gostomp.Conn` whose every method reports it, and nothing re-establishes
// one. So this driver owns the loop, and it owns it with
// [driverkit.Reconnector] for the four obligations that helper really does
// discharge — the jittered backoff, the attempt counter, the generation the
// staleness guard reads, and the health transitions.
//
// The backoff policy is named EXPLICITLY and not left to the variant family's
// `Auto`, which is the maintenance notes's third recorded gap in that helper:
// `ReconnectConfig.Policy`'s zero value resolves to `None`, which is no delay
// at all, and the specification forbids exactly that ("reconnect with backoff (never
// busy-spin)"). This is the third driver to work around the same default.
//
// # A reconnection invalidates every source
//
// The order is the contract and it is the one `drivers/amqp10` records: the
// generation is bumped FIRST, so that from the instant the connection died
// every delivery a source handed out is stale and a settlement carrying one is
// refused rather than applied to whatever takes its place. Only then
// does the loop re-dial, and the SUBSCRIBE frames are re-sent by each source
// when it next fetches.
type Backend struct {
	*driverkit.BaseBackend

	cfg    Config
	errs   *driverkit.ErrorMapper
	health *driverkit.Health
	reconn *driverkit.Reconnector
	clock  mq.Clock

	// mu guards the connection and the live handle sets.
	mu    sync.Mutex
	conn  *gostomp.Conn
	own   driverkit.Ownership
	srcs  map[*Source]struct{}
	sinks map[*Sink]struct{}
	addr  int

	// receipts numbers every publication receipt this backend asks for. It
	// is the backend's and not a sink's because every sink shares the one
	// connection and go-stomp keys a pending receipt by its id: two sinks
	// counting from one each would overwrite each other's wait. It is never
	// reset, so ids stay unique across a reconnect as well.
	receipts atomic.Uint64

	// unanswered counts the frames written on conn that the broker has not
	// answered, and is nil while conn is a connection the caller gave rather
	// than one this backend dialled: the caller may write frames of its own
	// on it, which nothing here can count. [Backend.dial] assigns it with
	// conn, under mu, so a new connection starts at zero and a late
	// publication on a replaced one counts against that one's.
	//
	// It exists for the one ERROR frame a publication may claim without a
	// receipt-id ([answers]): a count of one, the publication's own,
	// says every other frame on the connection was answered -- which a
	// receipt-less SEND never is, and a receipt that timed out never was.
	unanswered *atomic.Int64

	// superCtx is the supervisor's own context: the caller's, with its
	// cancellation removed and one of this backend's own put in its place.
	// The specification forbids retaining a caller's context beyond its operation, and a
	// dial loop that lived on the context Open was given would stop
	// reconnecting the moment the caller cancelled it -- which for a
	// request-scoped context is immediately.
	superCtx    context.Context
	superCancel context.CancelFunc

	// closing is closed by Close, so that the supervisor stops and a parked
	// fetch wakes.
	closing chan struct{}
	once    sync.Once
	// lost WAKES the supervisor and carries nothing; the notice itself lives
	// in noticeMu/notice beside it. A death notice reaches the supervisor
	// from whichever operation or watcher met it, and the supervisor is what
	// invalidates on the watcher's behalf: a notice from watch.go arrives on
	// go-stomp's reader goroutine, which must not be blocked (watch.go says
	// why) and must not act on a connection that has already been replaced --
	// so it signals and does nothing else, and the supervisor does the work
	// in one place where the ordering is its own.
	//
	// EVERY notice names the connection GENERATION it is about, the
	// operation path's as much as the watcher's. The supervisor used to act
	// on every notice by tearing down whatever connection was current, so a
	// watcher that reported the OLD socket's death late -- after the dial
	// loop had already put a new connection in place -- disconnected the new
	// one, and the next publication failed with "the connection is not
	// established". It took a slow two-CPU runner under coverage to show it:
	// TestCriticalSinkRecoversAfterTheConnectionDies on the first GitHub run.
	// The operation path was left saying generation
	// zero, "the one current when this was sent", which is unorderable
	// against the dial loop and reproduced the same symptom at about one run
	// in two hundred; it now names the generation it tore down, and
	// [Backend.stale] has no zero case left.
	//
	// # Why the notice is a FIELD and the channel only a bell
	//
	// This used to be a `chan lostNotice` of capacity one, written with a
	// `default` that dropped the notice when the buffer was full. Collapsing
	// two handles' reports of ONE death into one re-dial is what the buffer
	// of one is for, but a full buffer does not mean "the same death": a
	// stale notice about an already-replaced generation could sit in the slot
	// while the CURRENT connection died, and the genuine notice was then the
	// one dropped. The supervisor read the stale one, discarded it as stale
	// and went back to sleep with nothing left to wake it -- a client
	// Degraded with no connection and no re-dial. Keeping the notice in a
	// field lets a NEWER death displace an older one instead of losing to it,
	// which is what collapsing was always meant to do.
	lost     chan struct{}
	noticeMu sync.Mutex
	notice   lostNotice
	pending  bool

	// connGen is the generation of conn, and dialGen hands them out. Both
	// exist so a death notice can be matched to the connection it is about,
	// and TWO INVARIANTS carry that. They are written here, beside the
	// fields they constrain, because nothing in the type system states
	// either one and a later change would break them silently.
	//
	//  1. Generations are UNIQUE, and `conn` and `connGen` are assigned
	//     TOGETHER under `b.mu` -- [Backend.dial] does both in one critical
	//     section. This is what [Backend.stale] needs and ALL it needs:
	//     `connGen` is then exactly the generation of `conn`, so
	//     `n.gen == b.connGen` iff the notice is about the connection
	//     installed right now. It compares for equality and never for
	//     order, so it does not care which dial won; a change that dialled
	//     concurrently would not break it. A change that split those two
	//     assignments across two critical sections WOULD, and that is the
	//     reader this paragraph exists for.
	//
	//  2. `connGen` is MONOTONIC across installations. This one is not for
	//     `stale`; it is for [Backend.reportLostFor], which keeps the newer
	//     of two waiting notices by comparing `gen >= b.notice.gen`.
	//     That comparison means "not older" only while
	//     a later-installed connection carries a higher generation.
	//     `dialGen` is an atomic counter, so the generations themselves are
	//     unique and ascending unconditionally -- but two CONCURRENT dials
	//     could still install them out of order, and a genuine notice about
	//     the live connection would then be dropped as older than a stale
	//     one still waiting, which is the exact loss exists to
	//     prevent. What buys it today is that there is one dialler at a
	//     time: every dial goes through `reconn.Connect`, from
	//     [Backend.supervise] or once from [OpenBackend] before the
	//     supervisor starts. A second concurrent dialler would need
	//     `reportLostFor` to order notices by something other than the
	//     generation.
	connGen uint64
	dialGen atomic.Uint64
	// staleNotices counts death notices that arrived about a connection
	// already replaced, and were therefore ignored rather than acted on.
	staleNotices atomic.Uint64

	// openRefused ends OpenBackend's dial loop on a refusal no retry changes
	// ([Backend.refusedForGood]). It is set only for the length of that one
	// `Connect`, which runs Dial on OpenBackend's own goroutine, and cleared
	// before the supervisor starts -- so the supervisor's re-dials, which
	// SHOULD keep trying a broker an operator is fixing, never see it.
	openRefused context.CancelCauseFunc

	// noAddrs is a client opened over a connection the caller gave, with no
	// Config.Addrs of its own. The resolved default would name localhost, so
	// without this a source on such a client -- which dials a connection of
	// its own -- and the dial loop of an owned one dialled localhost:61613,
	// whatever broker the caller's connection reached.
	noAddrs bool

	// reconnects counts completed reconnections, for a test that has to wait
	// for one rather than sleep.
	reconnects atomic.Uint64
	// ambiguous counts publications whose outcome nobody knows, which is the window as a number a deployment can read.
	ambiguous atomic.Uint64
}

var _ mq.Backend = (*Backend)(nil)

// Open dials the broker and returns a client.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return mq.NewClient(b, clientOptions(cfg)...), nil
}

// OpenBackend dials the broker and returns the SPI backend.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	r := cfg.resolve()
	b, err := newBackend(r, nil, driverkit.ClientOwned)
	if err != nil {
		return nil, err
	}
	// The specification: the dial happens before Open returns, so a client that came back
	// ready cannot fail on its first publish for a reason the caller could
	// have been told at construction.
	//
	// The loop retries what a retry can fix and NOTHING else. A broker of the
	// other product, one too old for the mapping, and a refused credential
	// are each the same answer on every attempt, and the reconnector retries
	// any failure until its context ends -- so with a context that never ends
	// and no attempt limit, Open never returned and the caller was told
	// nothing. A dial that meets one of them cancels the loop's context with
	// the refusal as its cause, and that cause is what Open reports.
	dctx, refuse := context.WithCancelCause(ctx)
	b.openRefused = refuse
	err = b.reconn.Connect(dctx)
	b.openRefused = nil
	refused := context.Cause(dctx)
	refuse(nil)
	if err != nil {
		_ = b.Close()
		if refused != nil && ctx.Err() == nil {
			err = refused
		}
		return nil, b.errs.Map(mq.OpOpen, "", "", err)
	}
	b.startSupervisor(ctx)
	return b, nil
}

// OpenWithClient opens a client over a connection the caller already has
// (the specification's `OpenWithClient`).
//
// own says whether iqmq closes it. A BORROWED connection is never redialled:
// dialling would replace a connection the caller still holds, so a borrowed
// client goes Degraded when the socket dies and stays there.
//
// The connection is held to the same product and version gate a dial is, and
// refused when the broker it reached is not the one cfg names. It carries the
// client's PUBLICATIONS; a source dials a connection of its own from
// cfg.Addrs, so a source is refused when cfg.Addrs is empty -- and an owned
// connection with no cfg.Addrs is not redialled either, because there is no
// address to dial.
func OpenWithClient(ctx context.Context, conn *gostomp.Conn, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, conn, own, cfg)
	if err != nil {
		return nil, err
	}
	return mq.NewClient(b, clientOptions(cfg)...), nil
}

// BackendWithClient is [OpenWithClient] at the SPI.
func BackendWithClient(ctx context.Context, conn *gostomp.Conn, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if conn == nil {
		return nil, errNilConn
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b, err := newBackend(cfg.resolve(), conn, own)
	if err != nil {
		return nil, err
	}
	// The gate a dial applies, applied to the connection the caller dialled.
	// It is the same hazard whoever opened the socket: a client configured
	// for one product and given a connection to the other derives every
	// destination the wrong way and publishes to names nothing consumes. The
	// caller's connection is left as it is -- refusing it is not taking it
	// over.
	if err := b.checkBroker(conn.Server()); err != nil {
		return nil, b.errs.Map(mq.OpOpen, "", "", err)
	}
	b.noAddrs = len(cfg.Addrs) == 0
	b.health.Ready()
	if own == driverkit.ClientOwned {
		b.startSupervisor(ctx)
	}
	return b, nil
}

// clientOptions is what the client is built with: the driver's clock, so that
// a test's fake clock reaches the core as well as the driver.
func clientOptions(cfg Config) []mq.ClientOption {
	if cfg.Clock == nil {
		return nil
	}
	return []mq.ClientOption{mq.WithClock(cfg.Clock)}
}

// newBackend builds the backend without dialling.
func newBackend(cfg Config, conn *gostomp.Conn, own driverkit.Ownership) (*Backend, error) {
	caps, err := cfg.capabilities()
	if err != nil {
		return nil, err
	}
	b := &Backend{
		cfg:     cfg,
		errs:    newErrorMapper(),
		clock:   cfg.Clock,
		conn:    conn,
		own:     own,
		srcs:    make(map[*Source]struct{}, 4),
		sinks:   make(map[*Sink]struct{}, 4),
		closing: make(chan struct{}),
		lost:    make(chan struct{}, 1),
	}
	if b.clock == nil {
		b.clock = wallClock{}
	}
	b.health = driverkit.NewHealth(nil)
	base, err := driverkit.NewBase(driverkit.BaseConfig{
		Info:         cfg.info(),
		Capabilities: caps,
		Native:       b,
		Ownership:    own,
		CloseNative:  b.closeNative,
		Health:       b.health,
	}, b.teardown)
	if err != nil {
		return nil, err
	}
	b.BaseBackend = base
	rc, err := driverkit.NewReconnector(driverkit.ReconnectConfig{
		Dial: b.dial,
		// "Restore the client" and not "open a socket", which is
		// the maintenance notes's first recorded gap in this helper: Connect
		// returns when Dial returns and Health.Ready therefore means
		// whatever Dial decided it means. Here Dial sends CONNECT and waits
		// for CONNECTED, so a ready client really can publish -- and the
		// SUBSCRIBE frames are each source's own, re-sent on its next
		// fetch.
		// Named explicitly. The maintenance notes records this as the third
		// gap in the helper: the zero value is `backoff.Auto`, which
		// resolves to `AutoKind`, which is `None` -- no delay at all -- and the specification forbids exactly that. This is the third driver to name a
		// jittered policy to work around one default.
		Policy:      backoff.Full,
		Base:        cfg.ReconnectBase,
		Max:         cfg.ReconnectMax,
		Growth:      cfg.ReconnectGrowth,
		MaxAttempts: cfg.ReconnectAttempts,
		Health:      b.health,
		Clock:       b.clock,
		Driver:      DriverName,
		OnReconnect: b.onReconnect,
	})
	if err != nil {
		return nil, err
	}
	b.reconn = rc
	return b, nil
}

// dial opens one connection and puts it in place.
//
// It is the whole of "the client can work again": the CONNECT frame is sent
// and the CONNECTED frame read before this returns, so a caller whose
// [mq.Client] is Ready has a connection the broker has accepted. What it does
// NOT do is re-subscribe: each source re-sends its own SUBSCRIBE on its next
// fetch, because a subscription belongs to a source and the backend does not
// know what any of them asked for.
func (b *Backend) dial(ctx context.Context) error {
	gen := b.dialGen.Add(1)
	conn, err := b.dialOne(ctx, func(cause error) {
		if errors.Is(cause, errSocketClosed) {
			b.reportClosed(gen, cause)
			return
		}
		b.reportLostFor(gen, cause)
	})
	if err != nil {
		if b.openRefused != nil && b.refusedForGood(err) {
			b.openRefused(err)
		}
		return err
	}
	b.mu.Lock()
	old := b.conn
	b.conn = conn
	b.connGen = gen
	b.unanswered = new(atomic.Int64)
	b.mu.Unlock()
	if old != nil {
		// The old connection is dead; disconnecting it politely would
		// block on a receipt that is not coming, so it is dropped.
		_ = old.MustDisconnect()
	}
	return nil
}

// dialOne opens ONE connection and returns it without installing it.
//
// It is separate from [Backend.dial] because a SOURCE dials its own
// connection, and the reason is a deadlock rather than tidiness: a STOMP
// connection multiplexes MESSAGE frames and RECEIPT frames on one socket and
// go-stomp reads them with one loop, so a consumer that stops reading -- which
// a source did whenever its bounded buffer was full, and still does past the
// broker's flow-control window (overflow.go) -- blocks that loop, and every RECEIPT
// behind the delivery it is holding stops arriving. A publisher on the same
// connection then waits its whole receipt timeout for an answer the broker
// already sent.
//
// Measured: the specification's `publish/concurrent-eight-goroutines` publishes 400
// messages from eight goroutines with one consumer open and hung for seven
// minutes, which is eight publishers each waiting 30 s several times over.
//
// It is the same coupling `drivers/amqp10` refuses one layer up, for the same
// reason stated the same way: a session shared by a sink and a source makes a
// source that stops fetching throttle a sink that is publishing perfectly
// well. AMQP 1.0 can separate them with a session because its flow control is
// per session; STOMP has no such scope, so the separation has to be the
// connection.
func (b *Backend) dialOne(ctx context.Context, dead func(error)) (*gostomp.Conn, error) {
	addr := b.nextAddr()
	d := b.cfg.DialTimeout
	if _, ok := ctx.Deadline(); !ok && d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	sock, err := b.dialNet(ctx, addr)
	if err != nil {
		return nil, err
	}
	netConn := watch(sock, dead)
	opts := []func(*gostomp.Conn) error{
		gostomp.ConnOpt.AcceptVersion(gostomp.V12),
		gostomp.ConnOpt.HeartBeat(b.cfg.HeartbeatSend, b.cfg.HeartbeatReceive),
		gostomp.ConnOpt.MsgSendTimeout(b.cfg.ReceiptTimeout),
		gostomp.ConnOpt.RcvReceiptTimeout(b.cfg.ReceiptTimeout),
		// Both of these default to THIRTY SECONDS in go-stomp v3.1.5 and
		// neither is derived from the two above, so a configuration that
		// asked for a 100 ms receipt timeout still waited half a minute for
		// an UNSUBSCRIBE's answer -- and this driver's requeue IS an
		// unsubscribe (source.go), so the default made a Nack cost 30 s
		// and report `SettlementUnknown`. Measured:
		// `TestCriticalRequeueSendsNoNackFrame (30.13s) ... unsubscribe
		// receipt timeout`.
		gostomp.ConnOpt.UnsubscribeReceiptTimeout(b.cfg.ReceiptTimeout),
		gostomp.ConnOpt.DisconnectReceiptTimeout(b.cfg.ReceiptTimeout),
	}
	if b.cfg.Login != "" || b.cfg.Passcode != "" {
		opts = append(opts, gostomp.ConnOpt.Login(b.cfg.Login, b.cfg.Passcode.Reveal()))
	}
	host := b.cfg.Host
	if host == "" {
		host, _ = splitHostPort(stripScheme(addr))
	}
	if host != "" {
		opts = append(opts, gostomp.ConnOpt.Host(host))
	}
	// The CONNECT/CONNECTED exchange is part of the dial and is bounded like
	// it: a peer that takes the socket and never answers would otherwise
	// hold OpenBackend, a source's redial and the supervisor for ever.
	// go-stomp honours only the context's DEADLINE, as a read deadline, so a
	// cancellation closes the socket. The close is reported as a death like
	// any other (watch.go), and nothing acts on it, which is right, because
	// this connection was never anyone's: the backend's notice wants the
	// connection installed and a source's ignores closes.
	stop := context.AfterFunc(ctx, func() { _ = netConn.Close() })
	conn, err := gostomp.ConnectWithContext(ctx, netConn, opts...)
	if !stop() && err == nil {
		_ = conn.MustDisconnect()
		err = ctx.Err()
	}
	if err != nil {
		_ = netConn.Close()
		return nil, err
	}
	// the product gate, and the one `internal/activemq.CheckProduct`
	// exists for: a client configured for Classic and pointed at Artemis
	// maps its destinations the other broker's way and silently publishes to
	// a name nothing consumes. The CONNECTED frame's `server` header is what
	// settles it and it has just arrived.
	if err := b.checkBroker(conn.Server()); err != nil {
		_ = conn.MustDisconnect()
		return nil, err
	}
	return conn, nil
}

// checkBroker is the gate, read off the CONNECTED frame's `server` header.
//
// Two questions, and both of them are answered by one string. The PRODUCT
// decides the whole destination mapping -- a client configured for Classic and
// pointed at Artemis publishes to `VirtualTopic.orders` on a broker that has no
// such convention, and nothing consumes it -- and the VERSION decides whether
// the mapping this driver emits exists at all: Artemis's FQQN, which is how a
// shared durable subscription is named here, arrived in 2.7, and STOMP 1.2
// with `client-individual` acknowledgement arrived in Classic 5.6. A broker
// that sends no header at all is accepted, because absence is not disagreement
// (`activemq.CheckAtLeast`).
func (b *Backend) checkBroker(server string) error {
	if err := activemq.CheckProduct(b.cfg.Broker, server); err != nil {
		return err
	}
	got, ok := activemq.ParseVersion(server)
	if !ok {
		return nil
	}
	if b.cfg.Broker == activemq.Classic {
		return activemq.CheckAtLeast(got, activemq.MinClassicSTOMP, "ActiveMQ Classic's STOMP transport")
	}
	return activemq.CheckAtLeast(got, activemq.MinArtemisFQQN,
		"Artemis's STOMP acceptor with a fully qualified queue name")
}

// dialNet opens the socket, with TLS when the configuration or the address
// asks for it.
func (b *Backend) dialNet(ctx context.Context, addr string) (net.Conn, error) {
	target := withPort(stripScheme(addr), b.cfg.TLS != nil || isTLSAddr(addr))
	var d net.Dialer
	if b.cfg.TLS == nil && !isTLSAddr(addr) {
		return d.DialContext(ctx, "tcp", target)
	}
	cfg := b.cfg.TLS
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if b.cfg.InsecureSkipVerify {
		cfg = cfg.Clone()
		cfg.InsecureSkipVerify = true
	}
	if cfg.ServerName == "" && !cfg.InsecureSkipVerify {
		host, _ := splitHostPort(target)
		cfg = cfg.Clone()
		cfg.ServerName = host
	}
	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, err
	}
	tc := tls.Client(raw, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return tc, nil
}

// nextAddr is the endpoint the next attempt uses.
//
// The list is tried round-robin ACROSS attempts rather than all of them per
// attempt, so that the backoff is what paces the loop: an attempt that tried
// every address would make the delay between attempts a function of the list's
// length, and a five-address failover set would hammer a down broker five
// times per interval. It is `drivers/amqp10`'s rule and the reasoning is the
// same.
func (b *Backend) nextAddr() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.cfg.Addrs[b.addr%len(b.cfg.Addrs)]
	b.addr++
	return a
}

// startSupervisor derives the supervisor's context from the caller's and
// starts the loop.
func (b *Backend) startSupervisor(ctx context.Context) {
	superCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	b.superCtx, b.superCancel = superCtx, cancel
	go b.supervise(superCtx)
}

// supervise drives the reconnect loop.
//
// It does NOT poll the connection. go-stomp exposes no predicate for a
// connection's death -- `Session`, `Server`, `Version` and `Stats` are all
// answered from memory, and the only frames that would prove liveness either
// publish a message or make the broker answer with an ERROR frame that closes
// the connection -- so the honest design is the opposite of a health poll: the
// OPERATION that meets the failure reports it and this goroutine is what acts
// on the report. A driver that polled would have to send something, and there
// is nothing safe to send.
//
// The two reporting entry points are [Backend.connectionLostFor], which a sink
// calls with the connection it met the failure on, and [Backend.reportLostFor],
// which is all a socket watcher may do from go-stomp's reader goroutine. A
// SOURCE reaches neither: it owns its own connection and repairs it through
// [Source.socketDied].
func (b *Backend) supervise(superCtx context.Context) {
	for {
		select {
		case <-b.closing:
			return
		case <-superCtx.Done():
			return
		case <-b.lost:
		}
		if b.Closed() {
			return
		}
		notice, ok := b.takeNotice()
		if !ok {
			// The bell rang for a notice a previous round already took,
			// which is what two reports of one death sound like.
			continue
		}
		if b.stale(notice) {
			// About a connection that has already been replaced by a working
			// one: acting on it would disconnect the replacement.
			b.staleNotices.Add(1)
			continue
		}
		cause := notice.cause
		// The invalidation happens HERE and not in the watcher, and the
		// order is the specification: the generation is bumped and every handle's
		// tokens are invalidated before anything is re-dialled. A notice
		// that did this itself would race the operation path, which also
		// reports a death it met -- and the loser would have found b.conn
		// already replaced and disconnected a connection that was working.
		// This is a no-op when the operation that met the death got here
		// first, because there is nothing left to invalidate.
		//
		// A NIL connection is "whatever is current", and this is the one
		// place that meaning is right: the supervisor is acting on a notice
		// it has just found not stale, so the connection it is about is the
		// connection in place. Everywhere else the caller knows which socket
		// it met the failure on and says so.
		b.connectionLostFor(nil, cause)
		// Anything reported behind this notice is the SAME death: two
		// handles and a watcher can all report one socket, and the teardown
		// above has just reported the one it did.
		// Discarding collapses them into the single re-dial they deserve,
		// and nothing newer can exist yet because there is no connection to
		// die until the dial below makes one.
		b.takeNotice()
		select {
		case <-b.lost:
		default:
		}
		b.mu.Lock()
		own := b.own
		b.mu.Unlock()
		if own != driverkit.ClientOwned || b.cfg.NoReconnect || b.noAddrs {
			// A borrowed connection is the caller's, and a client that was
			// told not to reconnect asked for this: Degraded is terminal
			// and the honest answer. So is a client handed its connection
			// with no address to dial a replacement at: the resolved
			// default is localhost, which is not where that connection went.
			b.health.Degraded(errClosed)
			return
		}
		ctx, cancel := context.WithCancel(superCtx)
		stop := make(chan struct{})
		go func() {
			select {
			case <-b.closing:
				cancel()
			case <-stop:
			}
		}()
		err := b.reconn.Connect(ctx)
		close(stop)
		cancel()
		if err != nil {
			b.health.Degraded(err)
			return
		}
		b.reconnects.Add(1)
	}
}

// connectionLostFor tears down the backend's connection and invalidates every
// handle on it, for a caller that knows which connection it met the failure
// on.
//
// go-stomp reports a dead connection as an error on the next frame and has no
// event of its own, so the place that knows is the one that met it: a SINK, on
// the backend's connection (`Sink.Publish`), or [Backend.supervise] acting on
// a notice a watcher left. A SOURCE never reaches here -- it owns its own
// socket and repairs it through [Source.socketDied] -- and the sentence this
// replaced said a source calls it, which is the belief exists to remove. It said so because a source once did.
//
// The generation is bumped FIRST, so that from this instant every delivery
// every source handed out is stale and a settlement carrying one is refused
// rather than applied to whatever takes its place -- and only then is
// the reconnect loop woken.
//
// When `failed` is no longer the current connection the report is about one
// the supervisor has already replaced, and acting on it would tear down the
// replacement -- so it does nothing. A NIL connection means "whatever is
// current", which only the supervisor may say, because only the supervisor has
// just established that the notice it holds is not stale.
//
// The notice it leaves BEHIND it names the generation it tore down, which is
// what keeps the same rule true one hop later: the supervisor may read that
// notice after a re-dial has already succeeded.
func (b *Backend) connectionLostFor(failed *gostomp.Conn, cause error) {
	b.mu.Lock()
	if failed != nil && b.conn != failed {
		b.mu.Unlock()
		b.staleNotices.Add(1)
		return
	}
	conn := b.conn
	// The generation of the connection this call is tearing down, read
	// under the same lock that clears it, so that the notice posted at the
	// end names a connection rather than "whatever is current when the
	// supervisor reads it".
	gen := b.connGen
	b.conn = nil
	srcs := make([]*Source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*Sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	b.mu.Unlock()
	if conn == nil {
		// Another goroutine got here first; the generation has already been
		// bumped and the handles already invalidated.
		return
	}
	b.reconn.Lost(cause)
	for _, s := range srcs {
		s.connectionLost(cause)
	}
	for _, s := range sinks {
		s.connectionLost()
	}
	_ = conn.MustDisconnect()
	b.reportLostFor(gen, cause)
}

// reportLostFor wakes the supervisor about ONE connection generation, and is
// the WHOLE of what a death notice from watch.go does.
//
// It never blocks and it never touches the connection, so it is safe to call
// from go-stomp's reader goroutine and safe to call about a connection that
// has already been replaced -- the supervisor decides what, if anything, is
// still there to repair. `noticeMu` is held for two field
// writes and across no I/O and no other lock, which is what keeps that true.
//
// A report about a generation at least as new as the one already waiting
// REPLACES it; an older one is dropped. That is the collapsing the single
// buffered slot used to do by refusing the second writer, done the right way
// round: a stale report can no longer hide a live one.
func (b *Backend) reportLostFor(gen uint64, cause error) {
	b.noticeMu.Lock()
	if !b.pending || gen >= b.notice.gen {
		b.notice, b.pending = lostNotice{gen: gen, cause: cause}, true
	}
	b.noticeMu.Unlock()
	select {
	case b.lost <- struct{}{}:
	default:
	}
}

// reportClosed is the death notice for a socket that was CLOSED, about
// connection generation gen.
//
// It reports only while that connection is still the installed one, because a
// close is either the library's or this driver's and the installation is what
// tells them apart: every close the driver makes of its own accord uninstalls
// the connection first -- [Backend.dial] replaces it, [Backend.connectionLostFor]
// and [Backend.closeNative] clear it -- and a dial that failed never installed
// it. What is left is go-stomp closing a connection this backend is still
// using: a heart-beat timeout, or an ERROR frame nobody was waiting on. That
// is a death the supervisor has to hear about, and before this it heard
// nothing: health stayed Ready and nothing re-dialled.
//
// It may run on go-stomp's process loop while that holds the connection's
// `closeMutex`, so it takes `b.mu` for two field reads and nothing else -- the
// lock is never held across a call into the library -- and the report itself
// is [Backend.reportLostFor], which never blocks.
func (b *Backend) reportClosed(gen uint64, cause error) {
	b.mu.Lock()
	installed := b.conn != nil && b.connGen == gen
	b.mu.Unlock()
	if installed {
		b.reportLostFor(gen, cause)
	}
}

// refusedForGood reports whether a dial failed for a reason no retry changes:
// the product gate, the version gate, and the broker refusing the credentials.
func (b *Backend) refusedForGood(err error) bool {
	code := mq.CodeOf(b.errs.Map(mq.OpOpen, "", "", err))
	return code == mq.CodeUnsupported || code == mq.CodeUnauthorized || code == mq.CodeForbidden
}

// takeNotice removes the waiting death notice, if there is one.
func (b *Backend) takeNotice() (lostNotice, bool) {
	b.noticeMu.Lock()
	defer b.noticeMu.Unlock()
	n, ok := b.notice, b.pending
	b.notice, b.pending = lostNotice{}, false
	return n, ok
}

// stale reports whether a death notice is about a connection that has since
// been replaced by one that is still in place. A notice arriving while there
// is no connection at all is never stale: then there is nothing a re-dial
// could damage.
//
// EVERY notice names a generation, and that is the whole of the rule.
// There used to be an escape: generation zero meant "the
// one current when this was sent", which the operation path used because it
// had just met the death on the current connection -- and it is exactly the
// notice that cannot be ordered against the dial loop. [Backend.supervise]
// drains the channel after invalidating and before re-dialling, so an
// operation whose report landed in the gap between the drain and the new
// connection left a never-stale notice behind; the supervisor read it after
// the re-dial had succeeded, tore down the connection it had just made, and
// the next publication failed with "the connection is not established". That
// is `TestCriticalSinkRecoversAfterTheConnectionDies` failing once per two
// hundred runs on a two-CPU runner -- the same symptom the specification fixed for the WATCHER's report, from the one reporter it left uncovered.
//
// Generation zero survives as an honest value: it is the generation of a
// connection handed to [BackendWithClient], which no dial produced.
func (b *Backend) stale(n lostNotice) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.conn != nil && n.gen != b.connGen
}

// lostNotice is one death notice: the connection generation it is about, and
// why. See [Backend.lost].
type lostNotice struct {
	gen   uint64
	cause error
}

// onReconnect fires the hook.
//
// It is wired from [Config.Hooks] and not from the client's, because a driver
// cannot reach its client's hooks: a Backend is built before the Client that
// holds them. That is the gap the maintenance notes records as the specification, unchanged
// through six increments, and taking Hooks in the driver's own Config is what
// every driver in this repository does about it.
func (b *Backend) onReconnect(ctx context.Context, ev mq.ReconnectEvent) {
	if b.cfg.Hooks.OnReconnect != nil {
		b.cfg.Hooks.OnReconnect(ctx, ev)
	}
}

// connection returns the live connection and the generation it belongs to.
func (b *Backend) connection(ctx context.Context) (*gostomp.Conn, uint32, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, 0, err
	}
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()
	if conn == nil {
		return nil, 0, errNotConnected
	}
	return conn, b.reconn.Generation(), nil
}

// unansweredOn is conn's count of unanswered frames, or nil when conn is not
// the connection this backend dialled and still has in place -- the caller's
// own, or one already replaced. nil proves nothing, which is the safe answer
// for both.
func (b *Backend) unansweredOn(conn *gostomp.Conn) *atomic.Int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if conn != b.conn {
		return nil
	}
	return b.unanswered
}

// Backoff is the delay for one dial attempt, which a source uses
// to pace its OWN re-dial: the connection a source dials is not the backend's,
// so the Reconnector cannot pace it, and a source that dialled on every fetch
// would busy-spin against a broker that is down.
func (b *Backend) Backoff(attempt int, prev time.Duration) time.Duration {
	return b.reconn.Backoff(attempt, prev)
}

// Generation is the connection generation, which rises on every loss.
func (b *Backend) Generation() uint32 { return b.reconn.Generation() }

// Reconnects is how many reconnections have completed.
func (b *Backend) Reconnects() uint64 { return b.reconnects.Load() }

// Ambiguous is how many publications this backend could not resolve, which is the window as a number a deployment can read.
func (b *Backend) Ambiguous() uint64 { return b.ambiguous.Load() }

// Ping reports whether the connection is usable.
//
// STOMP has no ping frame a client may send, so what this does is the
// cheapest thing that reaches the broker and changes nothing: a receipted
// SEND would publish, and a receipted UNSUBSCRIBE of an id nobody holds is an
// ERROR frame that closes the connection. So it reports what the driver
// knows -- whether there is a live connection -- and says so rather than
// pretending to a round trip.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	_, _, err := b.connection(ctx)
	if err != nil {
		return b.errs.Map(mq.OpOpen, "", "", err)
	}
	return nil
}

// Admin is absent deliberately: STOMP has no topology frame, so this backend
// does not satisfy [mq.AdminBackend] and `Client.Admin()` reports
// ErrUnsupported rather than returning a surface that refuses every call. See
// admin.go.

// teardown closes every handle this backend made.
func (b *Backend) teardown() error {
	b.once.Do(func() { close(b.closing) })
	if b.superCancel != nil {
		b.superCancel()
	}
	b.mu.Lock()
	srcs := make([]*Source, 0, len(b.srcs))
	for s := range b.srcs {
		srcs = append(srcs, s)
	}
	sinks := make([]*Sink, 0, len(b.sinks))
	for s := range b.sinks {
		sinks = append(sinks, s)
	}
	b.srcs = map[*Source]struct{}{}
	b.sinks = map[*Sink]struct{}{}
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
	b.health.Close()
	return first
}

// closeNative disconnects the connection iqmq owns.
//
// The polite DISCONNECT is tried first, because it is what tells the broker to
// release the subscriptions' delivering sets rather than waiting for the
// socket to time out; a connection that will not answer its receipt is then
// dropped.
func (b *Backend) closeNative() error {
	b.mu.Lock()
	conn := b.conn
	b.conn = nil
	b.mu.Unlock()
	if conn == nil {
		return nil
	}
	if err := conn.Disconnect(); err != nil {
		_ = conn.MustDisconnect()
	}
	return nil
}

// forget drops a handle from the backend's sets.
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

// stripScheme removes a `stomp://` or `stomp+ssl://` prefix from an endpoint.
func stripScheme(a string) string {
	if i := strings.Index(a, "://"); i >= 0 {
		return a[i+3:]
	}
	return a
}

// isTLSAddr reports whether an endpoint names a protected scheme.
func isTLSAddr(a string) bool {
	return strings.HasPrefix(a, "stomp+ssl://") || strings.HasPrefix(a, "ssl://") ||
		strings.HasPrefix(a, "stomps://") || strings.HasPrefix(a, "tls://")
}

// withPort gives an endpoint the default port when it names none.
func withPort(a string, useTLS bool) string {
	if _, port := splitHostPort(a); port != "" {
		return a
	}
	if useTLS {
		return a + ":" + DefaultTLSPort
	}
	return a + ":" + DefaultPort
}

// splitHostPort separates an endpoint's host from its port, IPv6 literals
// included.
func splitHostPort(s string) (host, port string) {
	if i := strings.LastIndex(s, "]"); i >= 0 {
		if j := strings.Index(s[i:], ":"); j >= 0 {
			return s[:i+1], s[i+j+1:]
		}
		return s, ""
	}
	i := strings.LastIndex(s, ":")
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i+1:]
}

// brokerIsArtemis is a shorthand the sink and the source read.
func (b *Backend) brokerIsArtemis() bool { return b.cfg.Broker == activemq.Artemis }

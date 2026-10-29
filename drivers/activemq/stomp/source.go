package stomp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	gostomp "github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/activemq/protocol"
)

// Source is one subscription's consuming handle.
//
// # STOMP pushes, so the broker's window is the backpressure
//
// A receiving link in AMQP 1.0 has credit and this does not: a SUBSCRIBE frame
// tells the broker to start sending and the broker sends. What stops it is the
// broker's own flow-control window, which counts what the subscription has not
// acknowledged, and the two brokers count in different units: Artemis's
// `consumer-window-size` is BYTES (Config.ConsumerWindow) and Classic's
// `activemq.prefetchSize` is a MESSAGE COUNT (the resolved prefetch). So
// `SourceConfig.Prefetch` -- a count -- can be passed to one of them and not
// the other, and the count is enforced here as well, where it means what the
// specification says it means: what a fetch reads is a
// [driverkit.PushAdapter] whose count and byte budget come from the prefetch.
//
// # The subscription's channel is always read
//
// The pump that fills that buffer waits while it is full, and it used to be
// the reader of go-stomp's subscription channel too. Its wait stopped
// go-stomp's process loop, which is the only writer of the client's
// heart-beats, so a caller that left a full buffer unfetched for longer than
// the window Config.HeartbeatSend negotiated had its connection dropped and
// every unacknowledged delivery redelivered. The channel now has a reader of
// its own, [Source.drain], which queues what it reads in an [overflow] bounded
// by the broker's window, and the pump takes from that queue. A full buffer
// parks the pump and nothing else, so heart-beats, receipts and
// acknowledgements keep flowing however long the caller pauses. overflow.go
// has the measurement and the bound.
//
// # The subscription is the unit of recovery
//
// There is no per-delivery negative settlement on either broker (doc.go says
// what was measured), so a requeue is a subscription RESET: unsubscribe,
// re-subscribe, and the broker redelivers every unacknowledged delivery with
// `redelivered: true`. That makes the subscription the unit of invalidation as
// well: every token issued before a reset is stale afterwards, which
// [driverkit.TokenTable.InvalidateAll] is exactly for.
type Source struct {
	b     *Backend
	topic string
	sub   string
	dest  string
	cfg   mq.SourceConfig

	proto    protocol.Protocol
	envelope *driverkit.Envelope

	table *driverkit.TokenTable
	set   *settler

	// prefetch is the bound on both the buffer and the token table, kept so
	// that a replacement buffer is the same size as the one it replaces.
	prefetch int

	// subMu serialises everything that sends a SUBSCRIBE or retires a
	// subscription: [Source.subscribe], [Source.reset] and
	// [Source.ensureSubscribed]. Each of them reads "is there a live
	// subscription?" and then does network I/O, so without it a requeue's
	// reset and a fetch's re-subscribe both saw none and both subscribed,
	// leaving a second subscription on one connection whose pump drops every
	// delivery unacknowledged. It is held across that I/O, so mu, which the
	// pump and the settler take, never is.
	subMu sync.Mutex
	// mu guards the live subscription, its connection, the buffer and the
	// pump's identity.
	mu sync.Mutex
	// conn is THIS SOURCE's own connection, not the backend's, and the
	// reason is a deadlock: a STOMP connection multiplexes MESSAGE and
	// RECEIPT frames on one socket and go-stomp reads them with one loop,
	// so a consumer that stops reading -- which this source did whenever
	// its buffer was full, and still does past the broker's window
	// (overflow.go) -- blocks every receipt behind the delivery it is
	// holding. A publisher on the
	// same connection then waits its whole receipt timeout for an answer
	// the broker already sent. [Backend.dialOne] has the measurement.
	conn *gostomp.Conn
	// dialAttempt and nextDial pace the source's own re-dial. A source that
	// dialled on every fetch would busy-spin against a broker that is down,
	// which the specification forbids -- and the backend's Reconnector cannot pace it,
	// because the connection is not the backend's. So the delay is the policy the backend was configured with, applied here
	// without blocking: a fetch before `nextDial` reports the remembered
	// failure rather than waiting inside the caller's call.
	dialAttempt int
	nextDial    time.Time
	dialErr     error
	lastDelay   time.Duration
	dialled     bool
	sub2        *gostomp.Subscription
	pending     *driverkit.PushAdapter
	subGen      uint64
	gen         uint32
	closed      bool
	pumpsWG     sync.WaitGroup
	// lost is a connection that DIED -- the socket, the broker, or the
	// backend's own connection -- as against one this source let go itself:
	// a requeue whose UNSUBSCRIBE went unanswered drops the connection on
	// purpose (Source.retire) and dials the next one as part of the requeue,
	// which is not a reconnection and which NoReconnect must not refuse. A
	// dial that succeeds clears it. Guarded by mu.
	lost bool

	// resets counts subscription resets, so that a deployment can see what
	// its nack rate is costing it and a test can prove one happened.
	resets atomic.Uint64
	// stale counts settlements refused because their subscription had gone.
	// It is counted where the REFUSAL happens (settler.go) and nowhere else:
	// the invalidations that make a delivery stale used to add every live
	// token as well, so one refused settlement after a reset counted once for
	// the reset and again for the refusal, and deliveries still in the buffer
	// -- never handed to anyone, never settled -- counted too.
	stale atomic.Uint64
	// redials counts the connections this source has dialled beyond its
	// first, which is what a reconnection is for a handle that owns its own
	// socket. The BACKEND's Reconnects counter cannot report it: the
	// connection is not the backend's.
	redials atomic.Uint64

	// lastErr is the terminal error the pump read, which a fetch reports
	// once and then forgets: the broker's own sentence is the only
	// diagnosis STOMP offers and a fetch that dropped it would leave a
	// caller with "Unavailable" and nothing else.
	lastErr error
	// tokenWaits counts deliveries that had to WAIT for a token because
	// every slot was in use. It is not an error: it is this driver's
	// backpressure reaching the one place that used to answer it by
	// throwing the delivery away.
	tokenWaits atomic.Uint64
	// undecodable counts deliveries this driver could not map, which are
	// left UNACKNOWLEDGED rather than discarded.
	undecodable atomic.Uint64

	// dl is the dead-letter sink, opened on first use.
	dlmu sync.Mutex
	dl   mq.Sink

	closing chan struct{}
	once    sync.Once
	// stopPumps is closed by Close once the connection is gone, and ends every
	// drain still waiting on its channel and every pump waiting on its
	// overflow. go-stomp does not always close a subscription's channel when
	// its connection goes: its process loop hands every registered channel an
	// error with a blocking send, and the unbuffered receipt channel of a
	// DISCONNECT that timed out has no reader, so in some map orders the
	// subscription's channel is never reached.
	stopPumps chan struct{}
}

var _ mq.Source = (*Source)(nil)

// OpenSource subscribes and returns a consuming handle.
func (b *Backend) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := b.CheckOpen(ctx); err != nil {
		return nil, err
	}
	if cfg.Subscription == "" {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, "", errNoSubscription)
	}
	if b.noAddrs {
		// Refused rather than dialled: the address a source would dial is
		// the resolved default, localhost, and not the broker the caller's
		// connection reaches -- a consumer of some other broker, or of
		// nothing, behind a client that otherwise works.
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, errSourceNeedsAddrs)
	}
	n, err := b.cfg.Names(cfg.Topic, cfg.Subscription)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	p, err := protocol.New(protocol.STOMP)
	if err != nil {
		return nil, err
	}
	dest, err := p.Destination(n, protocol.Consume)
	if err != nil {
		return nil, b.errs.Map(mq.OpOpen, cfg.Topic, cfg.Subscription, err)
	}
	prefetch := cfg.Prefetch
	if prefetch <= 0 {
		prefetch = b.cfg.Prefetch
	}
	pending, err := driverkit.NewPushAdapter(driverkit.PushConfig{
		Capacity: prefetch,
		MaxBytes: b.cfg.PrefetchBytes,
	})
	if err != nil {
		return nil, err
	}
	// TWICE the prefetch: a delivery claims its token BEFORE it is pushed
	// into the bounded buffer, and a delivery the caller is holding has
	// left the buffer and still holds its token -- so the live token count
	// reaches the buffer's capacity PLUS whatever the caller has not
	// settled. With a table the same size as the buffer, `Claim` failed
	// once both were full, and the specification measured that as
	// `publish/concurrent-eight-goroutines` receiving 256 of 400.
	//
	// The factor is a MARGIN and never a guarantee, which is the half
	// The specification did not say: what the caller holds unsettled is bounded by
	// nothing, so no finite number here is safe. Doubling lowered the odds
	// far enough that the remaining case took a second broker and a second
	// consumer to find -- `groups/competing-consumers` on Artemis, six of
	// forty messages lost with the table at 16 of 16. What makes the size
	// merely a tuning choice rather than a correctness one is
	// [Source.claimSlot], which waits instead of dropping.
	table, err := driverkit.NewTokenTable(2 * prefetch)
	if err != nil {
		return nil, err
	}
	env, err := newEnvelope(b.cfg, b.Capabilities())
	if err != nil {
		return nil, err
	}
	s := &Source{
		b:         b,
		topic:     cfg.Topic,
		sub:       cfg.Subscription,
		dest:      dest,
		cfg:       cfg,
		proto:     p,
		envelope:  env,
		pending:   pending,
		prefetch:  prefetch,
		table:     table,
		closing:   make(chan struct{}),
		stopPumps: make(chan struct{}),
	}
	s.set = &settler{src: s}
	// The SUBSCRIBE frame is sent HERE, eagerly, and the choice is worth
	// stating because `drivers/amqp10` makes the opposite one for a link.
	//
	// On ActiveMQ Classic a SUBSCRIBE is the only topology operation this
	// protocol has: a consumer queue comes into existence when a
	// subscription attaches to it and nothing else creates one over STOMP.
	// A lazy subscribe would therefore make `OpenSource` create nothing,
	// and a deployment that opened its consumers and then published would
	// lose everything published before the first fetch -- the virtual-topic
	// hazard this driver's documentation opens with.
	//
	// What it costs is measured and is in the capability table: a STOMP
	// subscription is a PUSH, so from the moment it exists the broker sends
	// and this driver buffers, and two things the broker does AT DISPATCH
	// stop being observable through it -- the priority ordering of a
	// destination's messages and the expiry of a message whose TTL has
	// passed. The priority probe reads "low" first and its TTL probe is
	// delivered a message three seconds past its expiry, and both are the
	// same fact: the dispatch happened while the messages were being
	// published. The lazy alternative was written and reverted, because it
	// buys those two claims by making the consumer queue's existence depend
	// on somebody calling Fetch.
	s.subMu.Lock()
	err = s.subscribe(ctx)
	s.subMu.Unlock()
	if err != nil {
		_ = pending.Close()
		return nil, err
	}
	b.mu.Lock()
	b.srcs[s] = struct{}{}
	b.mu.Unlock()
	return s, nil
}

// Destination is the destination this source consumes from, in the broker's
// own form: an Artemis fully qualified queue name, or a Classic virtual-topic
// consumer queue.
func (s *Source) Destination() string { return s.dest }

// Resets is how many subscription resets this source has performed, which is
// what a requeue costs on this protocol.
func (s *Source) Resets() uint64 { return s.resets.Load() }

// StaleSettlements is how many settlements were refused because their
// subscription had gone.
func (s *Source) StaleSettlements() uint64 { return s.stale.Load() }

// Redials is how many connections this source has dialled beyond its first,
// which is what a reconnection is for a handle that owns its own socket.
func (s *Source) Redials() uint64 { return s.redials.Load() }

// subscribe sends the SUBSCRIBE frame and starts the pump.
//
// # There is no receipt on this frame, and that is deliberate
//
// A `receipt` header on a SUBSCRIBE is a DEFECT in go-stomp v3.1.5: the
// library registers the subscription's own frame channel under the receipt id,
// the RECEIPT frame closes that channel, and the library then panics with
// "send on closed channel" from inside its own `processLoop`. Observed
// against Artemis. So a subscription cannot be confirmed synchronously
// through this library, and what happens instead is that a destination which
// does not exist surfaces on the FIRST FETCH: the broker sends an ERROR frame,
// the pump reads it, and the fetch reports [mq.CodeNotFound].
//
// The caller holds subMu.
func (s *Source) subscribe(ctx context.Context) error {
	if err := s.b.CheckOpen(ctx); err != nil {
		return s.b.errs.MapNet(mq.OpOpen, s.topic, s.sub, err, false)
	}
	// A source that will never dial again says so before the backend is
	// asked anything. The backend's connection dying takes this source's with
	// it, and the backend's answer was then the reconnect window's retryable
	// one, so this source's own refusal was never reached.
	s.mu.Lock()
	refused := s.redialRefused()
	s.mu.Unlock()
	if refused != nil {
		return s.b.errs.MapNet(mq.OpOpen, s.topic, s.sub, refused, false)
	}
	// The backend's connection is asked for next, and only for its
	// GENERATION and its liveness check: a source that dialled while the
	// client was closing, or while the backend had no connection at all,
	// would be a handle outliving the client that made it.
	_, gen, err := s.b.connection(ctx)
	if err != nil {
		return s.b.errs.MapNet(mq.OpOpen, s.topic, s.sub, err, false)
	}
	conn, err := s.connection(ctx)
	if err != nil {
		return s.b.errs.MapNet(mq.OpOpen, s.topic, s.sub, err, false)
	}
	opts := s.subscribeOptions()
	sub, err := conn.Subscribe(s.dest, gostomp.AckClientIndividual, opts...)
	if err != nil {
		if isConnGone(err) {
			// The socket that died is the one five lines above, which is
			// THIS SOURCE's  -- so the repair is this
			// source's too, with the identity check `socketDied` carries.
			// Telling the backend instead tore down its own healthy
			// connection, bumped the generation every other handle compares
			// against and invalidated every other source's deliveries and
			// every sink, for a socket none of them was using. That is the
			// rule [Source.fail] states and this call broke; the death the
			// BACKEND is told about is the one the liveness check above
			// reports, which it does by returning rather than by reporting.
			s.socketDied(conn)
		}
		return s.b.errs.MapNet(mq.OpOpen, s.topic, s.sub, err, false)
	}
	s.mu.Lock()
	if s.closed {
		// Close ran while the frame was going out. Its pump would be added
		// after Close's wait had begun, so there is none: the subscription
		// dies with the connection Close disconnects, and the broker takes
		// back whatever it sent to it.
		s.mu.Unlock()
		return errClosed
	}
	s.sub2 = sub
	s.subGen++
	s.gen = gen
	which := s.subGen
	// Two goroutines per subscription, the drain and the pump (overflow.go
	// says why one is not enough), counted together before either starts so
	// that Close's wait covers both.
	s.pumpsWG.Add(2)
	s.mu.Unlock()
	q := newOverflow(s.overflowBounds())
	go s.drain(sub, q)
	// The pump outlives the fetch that started it, so it gets the caller's
	// context with its cancellation removed rather than a fresh
	// Background: the specification forbids retaining a caller's context beyond its
	// operation, and a pump that died with the first fetch's context would
	// stop reading the subscription it just made.
	go s.pump(context.WithoutCancel(ctx), conn, q, which, gen)
	return nil
}

// overflowBounds is the broker's flow-control window in the broker's own unit,
// as the bound on a subscription's overflow: the prefetch COUNT on Classic and
// the ConsumerWindow BYTES on Artemis, which are the two values
// [Source.subscribeOptions] asks each broker for.
func (s *Source) overflowBounds() (int, int64) {
	if s.b.brokerIsArtemis() {
		return 0, int64(s.b.cfg.ConsumerWindow)
	}
	return s.prefetch, 0
}

// connection is this source's own connection, dialled on first use.
func (s *Source) connection(ctx context.Context) (*gostomp.Conn, error) {
	s.mu.Lock()
	refused := s.redialRefused()
	switch {
	case s.closed:
		s.mu.Unlock()
		return nil, errClosed
	case s.conn != nil:
		conn := s.conn
		s.mu.Unlock()
		return conn, nil
	case refused != nil:
		s.mu.Unlock()
		return nil, refused
	case !s.nextDial.IsZero() && s.b.now().Before(s.nextDial):
		// Too soon. The remembered failure is what the caller gets, so a
		// fetch loop against a broker that is down costs one error per
		// fetch and no dial at all.
		err := s.dialErr
		s.mu.Unlock()
		if err == nil {
			err = errClosed
		}
		return nil, err
	}
	s.mu.Unlock()
	// The socket is watched, so that a broker that drops this source's
	// connection while nothing is fetching is noticed at once rather than on
	// the next operation that fails for a reason the rule table has to guess
	// at (watch.go).
	//
	// The notice names the connection it is ABOUT. Without that identity a
	// notice delivered late -- the pump saw the death first, the next fetch
	// re-dialled, and only then did the notice run -- would drop the fresh
	// connection and cost the fetch after it an error for a socket that was
	// working. The pointer is stored after the dial returns and read by the
	// reader goroutine, so it is atomic; the gap between a successful dial
	// and the store is covered by the pump, which is what has always
	// reported this source's deaths.
	var owner atomic.Pointer[gostomp.Conn]
	conn, _, err := s.b.dialOne(ctx, func(cause error) {
		if errors.Is(cause, errSocketClosed) {
			// A CLOSE, which for a source is never this notice's to act on
			// (watchedConn.Close says why): the driver's own closes have
			// already let the connection go, and the library's run while it
			// holds the mutex the repair would take -- after handing the pump
			// the terminal message that makes the same repair.
			return
		}
		// The CAUSE is deliberately dropped here; [Source.socketDied] says
		// why. The pump owns the fetch's terminal error.
		if c := owner.Load(); c != nil {
			s.socketDied(c)
		}
	})
	if err != nil {
		s.mu.Lock()
		prev := time.Duration(0)
		if !s.nextDial.IsZero() {
			prev = s.lastDelay
		}
		s.dialAttempt++
		s.lastDelay = s.b.Backoff(s.dialAttempt, prev)
		s.nextDial = s.b.now().Add(s.lastDelay)
		s.dialErr = err
		s.mu.Unlock()
		return nil, err
	}
	s.mu.Lock()
	s.dialAttempt, s.nextDial, s.dialErr, s.lastDelay = 0, time.Time{}, nil, 0
	if s.closed {
		s.mu.Unlock()
		_ = conn.MustDisconnect()
		return nil, errClosed
	}
	if s.conn != nil {
		// Another fetch dialled first. One connection per source, so this
		// one goes rather than being kept beside it.
		existing := s.conn
		s.mu.Unlock()
		_ = conn.MustDisconnect()
		return existing, nil
	}
	first := s.dialled
	s.dialled, s.lost = true, false
	s.conn = conn
	s.mu.Unlock()
	owner.Store(conn)
	if first {
		s.redials.Add(1)
	}
	return conn, nil
}

// redialRefused is why this source will not dial another connection, or nil
// while it may. The caller holds s.mu.
func (s *Source) redialRefused() error {
	switch {
	case s.lost && s.b.cfg.NoReconnect:
		// A RE-dial, and the configuration made a lost connection terminal.
		// The backend's own loop honours it (Backend.supervise) and the
		// source's has to as well: it is the same promise about the same
		// client, and a source that re-dialled anyway was a reconnection the
		// caller had switched off.
		return errSourceNoReconnect
	case s.dialled && redialsExhausted(s.dialAttempt, s.b.cfg.ReconnectAttempts):
		// The attempt budget is the reconnector's own reading of
		// ReconnectAttempts -- consecutive failed dials in ONE outage -- and
		// it ends this source's re-dialling the way it ends the backend's:
		// for good, with the last failure attached.
		return redialsSpent(s.dialErr)
	}
	return nil
}

// redialsExhausted reports whether a source has spent its re-dial budget:
// `failed` consecutive failed dials against a ReconnectAttempts of `budget`,
// where zero is no limit.
func redialsExhausted(failed, budget int) bool { return budget > 0 && failed >= budget }

// redialsSpent is the terminal answer of a source that has used its whole
// re-dial budget, carrying the last dial's failure.
func redialsSpent(last error) error {
	if last == nil {
		return errSourceRedialsSpent
	}
	return fmt.Errorf("%w: %w", errSourceRedialsSpent, last)
}

// socketDied is the death notice for this source's OWN socket (watch.go).
//
// It takes the CONNECTION and not the cause, and both halves of that are
// deliberate. The connection is an identity check: a notice can be delivered
// after the pump has already seen the same death and the next fetch has
// re-dialled, and one that acted then would drop the connection that works.
// The cause is dropped because the PUMP owns the fetch's terminal error --
// see the bottom of this method.
func (s *Source) socketDied(conn *gostomp.Conn) {
	s.mu.Lock()
	live := s.conn == conn
	s.mu.Unlock()
	if !live {
		// The notice is about a connection this source has already replaced
		// or given up, so there is nothing to invalidate and nothing to
		// drop: acting on it would take away the one that works. With the
		// notice delivered INLINE on go-stomp's reader goroutine this
		// should be unreachable -- the reader sees the error before
		// `processLoop`, so before the pump and before any fetch can
		// re-dial -- and it is here because that ordering is the library's
		// and not this driver's.
		return
	}
	// The tokens go first and they go IMMEDIATELY: this is the earliest
	// moment anything in the process knows, so it is the moment from which a
	// settlement carrying an older delivery has to be refused.
	s.table.InvalidateAll()
	s.loseConnection()
	// What this deliberately does NOT do is remember the cause or wake a
	// parked fetch. The PUMP owns the fetch's terminal error, because the
	// pump is what saw whatever the broker said before it hung up -- and a
	// broker that refuses a SUBSCRIBE answers with an ERROR frame and THEN
	// closes the socket, so a notice that remembered `EOF` would displace
	// `AMQ339001 Destination does not exist` and report a retryable
	// transport failure instead of a cause. Measured, as the regression this
	// comment exists to prevent:
	//
	//	the fetch reports activemq: fetch topic=orders subscription=workers:
	//	Unavailable: EOF (code Unavailable), want NotFound
	//
	// The pump always terminates when the socket dies, and remembering is
	// its job on the way out.
}

// loseConnection is [Source.dropConnection] for a connection that died, which
// the next dial replaces as a RECONNECTION and the configuration may forbid.
func (s *Source) loseConnection() {
	s.mu.Lock()
	s.lost = true
	s.mu.Unlock()
	s.dropConnection()
}

// dropConnection forgets this source's connection, so that the next
// subscribe dials a new one.
func (s *Source) dropConnection() {
	s.mu.Lock()
	conn := s.conn
	s.conn, s.sub2 = nil, nil
	s.mu.Unlock()
	if conn != nil {
		_ = conn.MustDisconnect()
	}
}

// subscribeOptions are the SUBSCRIBE frame's headers.
//
// Three of them are the broker's own vocabulary and each was measured:
//
//	subscription-type: MULTICAST   Artemis only. Without it a SUBSCRIBE to a
//	                               fully qualified queue name is read as an
//	                               ANYCAST consumer, which is a different
//	                               routing type on the same address. It is
//	                               ANYCAST under AddressQueue, which is what
//	                               a JMS queue's consumer is.
//	consumer-window-size           Artemis's flow control, in BYTES.
//	activemq.prefetchSize          Classic's flow control, in MESSAGES.
//	selector                       a JMS message selector. Measured: it
//	                               filters on Classic and is IGNORED on
//	                               Artemis when the subscription is an FQQN,
//	                               which is why Config.Validate refuses one
//	                               there rather than accepting it.
func (s *Source) subscribeOptions() []func(*frame.Frame) error {
	prefetch := s.cfg.Prefetch
	if prefetch <= 0 {
		prefetch = s.b.cfg.Prefetch
	}
	out := make([]func(*frame.Frame) error, 0, 4)
	if s.b.brokerIsArtemis() {
		out = append(out,
			gostomp.SubscribeOpt.Header("subscription-type", subscriptionType(s.b.cfg.Addressing)),
			gostomp.SubscribeOpt.Header("consumer-window-size", strconv.Itoa(s.b.cfg.ConsumerWindow)),
		)
	} else {
		out = append(out, gostomp.SubscribeOpt.Header("activemq.prefetchSize", strconv.Itoa(prefetch)))
	}
	if s.b.cfg.Selector != "" {
		out = append(out, gostomp.SubscribeOpt.Header("selector", s.b.cfg.Selector))
	}
	return out
}

// subscriptionType is the routing type an Artemis SUBSCRIBE asks for.
func subscriptionType(a Addressing) string {
	if a == AddressQueue {
		return "ANYCAST"
	}
	return "MULTICAST"
}

// pump moves one subscription's deliveries from its overflow into the bounded
// buffer.
//
// It is the only reader of that queue and the only writer of the buffer, and
// the one drain fills the queue in the channel's own order, so the ordering a
// queue gives is the ordering a caller sees. It exits when the drain has ended
// and the queue is empty, which is what an unsubscribe and a connection death
// come to; when it has repaired the source after its subscription's terminal
// error; or when Close lets it go once the connection is gone. Its exit ends
// the drain as well ([overflow.done]).
//
// `which` is the subscription generation it belongs to: a pump whose
// generation is no longer the source's has been superseded by a reset and must
// not write, or a delivery from a subscription the broker has already taken
// back would be handed to a caller who cannot settle it. `conn` is the
// connection the subscription is on, which every delivery it reads is settled
// on.
func (s *Source) pump(ctx context.Context, conn *gostomp.Conn, q *overflow, which uint64, gen uint32) {
	defer s.pumpsWG.Done()
	defer q.finish()
	for {
		m, ok := q.take(s.stopPumps)
		if !ok {
			return
		}
		if m.Err != nil {
			// A terminal error is answered BEFORE the supersession check and
			// never after it, and that order is the whole an error is not a delivery. It carries no token, it
			// belongs to no subscription a caller can settle against, and it
			// is the only diagnosis this protocol offers -- so "am I still
			// the current subscription?", which is the right question to ask
			// about a MESSAGE, discards the broker's sentence when it is
			// asked about an ERROR. [Source.reportTerminal] is what a
			// superseded pump may still do with one, and whether this pump is
			// finished is ITS answer rather than this loop's: only the pump
			// that repaired the source may stop. A superseded one keeps
			// taking, because its exit ends its drain and go-stomp does not
			// always close `Subscription.C` after a terminal message --
			// `Unsubscribe`'s receipt timeout pushes one and leaves the
			// channel open -- and a drain that stopped reading a channel the
			// library still writes to would fill its sixteen-deep buffer and
			// then block `Subscription.readLoop`, which blocks `processLoop`
			// on its UNBUFFERED handoff and stalls every subscription on that
			// connection.
			if s.reportTerminal(which, m.Err) {
				return
			}
			continue
		}
		// The buffer is read under the SAME lock as the check, and the
		// delivery goes into that buffer and no other. Decoding and claiming a
		// slot happen between the check and the push, and a requeue landing
		// there bumps the generation, invalidates the table and swaps in a
		// fresh buffer: a push into whatever `buffer()` said by then put the
		// superseded subscription's delivery in front of the caller after its
		// Nack had returned, already unsettleable, and the broker redelivered
		// the same message on the new subscription -- one message processed
		// twice. Pushed here, it lands in the buffer the reset closed, the
		// push fails, and the slot is released below.
		s.mu.Lock()
		current := s.subGen == which && !s.closed
		buf := s.pending
		s.mu.Unlock()
		if !current {
			// A superseded pump drops what it reads. The broker will
			// redeliver it to the live subscription, which is the whole
			// mechanism a reset rests on.
			continue
		}
		d, ok := s.deliveryFor(ctx, conn, m, which, gen)
		if !ok {
			continue
		}
		if err := buf.Push(ctx, d); err != nil {
			// The buffer was closed under a parked push: the source is
			// closing, or a reset swapped it. Releasing the token keeps the
			// accounting straight, and the pump keeps TAKING, because its
			// return would end the drain and the unsubscribe that follows
			// waits for a RECEIPT that go-stomp's single process loop delivers
			// only while the channel drains -- a pump that returned here left
			// Close and reset waiting out the receipt timeout and then blocked
			// for ever on the library's own send of the timeout into the
			// channel.
			_ = s.table.Release(d.Token)
			continue
		}
	}
}

// drain reads one subscription's channel into its overflow and does nothing
// else, so that go-stomp's process loop -- and with it this connection's
// heart-beats, receipts and acknowledgements -- never waits on a caller who is
// not fetching (overflow.go).
//
// It exits when the channel closes, when the pump has stopped, or when Close
// lets the pumps go, and it ends the queue on the way out so that the pump
// takes what is left and then stops too. Its only wait is a queue holding the
// broker's whole window, which a broker that honours the window it was asked
// for never fills.
func (s *Source) drain(sub *gostomp.Subscription, q *overflow) {
	defer s.pumpsWG.Done()
	defer q.end()
	for {
		m, ok := s.next(sub, q)
		if !ok {
			return
		}
		if m == nil {
			continue
		}
		if !q.put(m, s.stopPumps) {
			return
		}
	}
}

// next is the subscription's next message, or false once its channel closes,
// its pump has stopped, or Close has let the pumps go.
func (s *Source) next(sub *gostomp.Subscription, q *overflow) (*gostomp.Message, bool) {
	select {
	case m, ok := <-sub.C:
		return m, ok
	case <-q.done():
		return nil, false
	case <-s.stopPumps:
		return nil, false
	}
}

// reportTerminal records subscription `which`'s terminal error, repairs this
// source only when that subscription is still the live one, and reports whether
// the pump that read the error is finished.
//
// # Why the record is unconditional and the repair is not
//
// The pump is this driver's only carrier of the broker's sentence: an ERROR
// frame's `message` header is the whole of what STOMP says about a refusal, and
// [Source.socketDied] drops the CAUSE deliberately so that a synthetic `EOF`
// cannot displace `AMQ339001 Destination does not exist`. That made the pump's
// authority to speak revocable by the other observer of the same event, and the
// revocation happens on a different goroutine with no ordering between them:
// both brokers write the ERROR frame and then close the socket, `watchedConn`
// fires [Source.socketDied] INLINE on go-stomp's reader goroutine one read
// later, and `socketDied` clears `sub2` -- so the next fetch re-subscribes,
// [Source.subscribe] bumps `subGen`, and the pump that is still carrying the
// sentence five goroutine hops behind (`readLoop` -> `processLoop` ->
// `Subscription.readLoop` -> `sub.C` -> [Source.drain]) was superseded before
// it could speak.
// The sentence was then discarded, `lastErr` stayed nil, and the fetch reported
// its own deadline. Measured, and with the sentence named in the counter that
// dropped it.
//
// What a superseded pump may NOT do is repair: the connection and the
// subscription belong to a later generation, so dropping either takes away the
// one that works -- which is the damage and the specification are
// about, one hop along -- and swapping the buffer would strand whatever the
// LIVE subscription has already buffered, because STOMP has no per-delivery
// redelivery and an unacknowledged delivery comes back only on a reset.
// So it remembers and keeps reading, and the fetch that
// superseded it reads `lastErr` when its own call returns -- at its own deadline
// in the worst case, which is bounded by the caller's context and is the one
// thing this shape gives up. Waking it would mean closing the buffer, and that
// buffer is the LIVE subscription's.
//
// `false` therefore means "not yours to end": only the pump that repaired the
// source stops reading.
func (s *Source) reportTerminal(which uint64, err error) bool {
	s.mu.Lock()
	current, closed := s.subGen == which, s.closed
	s.mu.Unlock()
	switch {
	case closed:
		// The specification has already decided what every later call answers, so there
		// is nothing to report and nothing to repair.
		return false
	case current:
		s.fail(err)
		return true
	default:
		s.remember(err)
		return false
	}
}

// fail records a subscription's terminal error and wakes a parked fetch.
//
// An ERROR frame is fatal to the CONNECTION on both brokers -- measured: the
// broker closes the socket after it -- so the connection is dropped here and
// the next fetch dials a new one. The backend is NOT told, and the paragraph
// below is the whole reason; this comment used to say the opposite of its own
// body, and the two call sites that did tell the backend were a defect.
func (s *Source) fail(err error) {
	// The connection is THIS SOURCE's, so a death here is this source's to
	// repair and not the backend's: telling the backend would invalidate
	// every other handle's deliveries for a socket none of them was using.
	// What the backend is told is a death of ITS connection, which a source
	// meets only through the liveness check in `subscribe` -- and which that
	// check reports by RETURNING the error rather than by tearing anything
	// down, because `Backend.connection` is read-only.
	s.loseConnection()
	s.table.InvalidateAll()
	s.remember(err)
	s.swapBuffer()
}

// remember records the terminal error a fetch should report once.
func (s *Source) remember(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.lastErr == nil {
		s.lastErr = err
	}
	s.mu.Unlock()
}

// claimSlot takes a token for one delivery, WAITING for a slot rather than
// throwing the delivery away when every slot is in use.
//
// The wait is this driver's backpressure reaching the last place that did not
// apply it. The pump already blocks on a full buffer -- that is what `Push`
// blocking means, and the broker's own window is what stops the broker
// (overflow.go) -- and the token table was the one bound that answered
// exhaustion by dropping.
// A dropped delivery is not "left with the broker" in any useful sense: STOMP
// has no per-delivery requeue on either broker, so it comes back only when the
// subscription is RESET, and a long-lived consumer never resets. It is a lost
// message.
//
// The specification found this and made the table twice the buffer, which
// lowers the odds and does not remove the path: the caller's unsettled
// holdings are bounded by nothing, so no finite table is safe. Measured
// against Artemis with `prefetch=8` and two competing consumers, the table
// still reached 16 of 16 and `groups/competing-consumers` lost six of forty
// messages -- the LAST six, with both consumers then idle for the whole
// deadline because the broker considered them dispatched.
//
// Blocking here cannot deadlock against the settlement that would free a
// slot: go-stomp's `Conn.Ack` does not wait for a receipt, and the process
// loop that writes its frame is kept reading by the drain, not by this
// goroutine. It once was this goroutine, and then an acknowledgement waited in
// go-stomp's twenty-slot write queue for a loop this wait had stopped.
//
// # It does not poll, and the exits are not the RabbitMQ driver's
//
// This was a 500-microsecond poll, because `driverkit.TokenTable` had no
// release signal to wait on. It has one now
// ([driverkit.TokenTable.ClaimOrWait]): the
// table waits on a broadcast that every release closes, and it decides the
// claim and starts that wait inside ONE critical section, so there is no
// ordering here for a caller to get wrong. An earlier design
// exposed the channel through a separate accessor the caller had to read
// BEFORE retrying its claim; the gap between the two was a lost wakeup, and withdrew it. A read-then-claim sequence must not come
// back.
//
// `drivers/rabbitmq`'s equivalent waits on four things, and only two of them
// have a counterpart here, because a stomp source owns its OWN socket
//
//	rather than sharing the backend's channel:
//
//	 - The release broadcast, which is the one that makes the wait end for the
//	   reason the wait exists.
//	 - The pump's context, which is kept for the same reason `Push` is given
//	   one and is NOT what ends this wait: `subscribe` starts the pump with
//	   `context.WithoutCancel`, whose `Done` channel is nil, so this case can
//	   never fire while the only caller is the pump. It is here so that a
//	   future caller with a cancellable context is honoured rather than as an
//	   exit this driver relies on.
//
// The two RabbitMQ cases with no counterpart are the second `NotifyClose`
// listener and `s.closed`, and in both cases the release broadcast IS the
// edge:
//
//   - A dead SOCKET. RabbitMQ needs a second close listener because the pump
//     is the only reader of the first one and the pump is the goroutine parked
//     here. Here the socket is watched independently (watch.go), the notice
//     runs on go-stomp's own reader goroutine, and [Source.socketDied]
//     invalidates the whole table before it drops the connection -- so the
//     death is reported by a goroutine that is not this one, and
//     `InvalidateAll` closes the broadcast.
//   - A SUPERSEDED subscription and a CLOSING source. Every path that bumps
//     `subGen` or sets `closed` invalidates the table first or immediately
//     after: [Source.reset] (bump, then `InvalidateAll`), [Source.Close]
//     (`closed`, then a Drain that releases every buffered token, then
//     `InvalidateAll`), [Source.fail], [Source.socketDied] and
//     [Source.connectionLost]. `Backend.connectionLostFor` bumps the BACKEND's
//     generation before it calls `connectionLost` on each source, so the one
//     `subGen` bump that does not invalidate -- `subscribe`'s, reached from
//     `ensureSubscribed` when the backend generation moved -- always follows
//     an invalidation by microseconds. So the wake always arrives and the
//     `current` re-check below is what turns it into a refusal.
//
// A second exit for each of those would be one no mutation could
// kill, which the contributor notes calls an instrument nobody has watched fail.
// What IS watched is the claim above:
// `TestASlotWaitEndsWhenTheSubscriptionIsReset` and
// `TestASlotWaitEndsWhenTheSourceCloses` both park the pump here and both go
// red when the broadcast is not waited on.
func (s *Source) claimSlot(ctx context.Context, h *held, which uint64) (uint64, bool) {
	if token, ok := s.table.Claim(h); ok {
		return token, true
	}
	s.tokenWaits.Add(1)
	for {
		s.mu.Lock()
		current := s.subGen == which && !s.closed
		s.mu.Unlock()
		if !current {
			return 0, false
		}
		// The claim and the start of the wait happen together, under the
		// table's own lock, so a release landing between them is impossible
		// rather than merely unlikely. A wake is not a claim, so the loop
		// re-checks the subscription before it claims again.
		token, ok, err := s.table.ClaimOrWait(ctx, h)
		if ok {
			return token, true
		}
		if err != nil {
			return 0, false
		}
	}
}

// deliveryFor builds one DriverDelivery, or reports that the message is not
// being handed over.
func (s *Source) deliveryFor(ctx context.Context, conn *gostomp.Conn, m *gostomp.Message, which uint64, gen uint32) (mq.DriverDelivery, bool) {
	var f protocol.Fields
	readFields(&f, m)
	var raw mq.Message
	po := s.b.cfg.protocolOptions()
	po.Limits = s.b.Capabilities().Limits()
	if err := s.proto.Restore(&raw, &f, po); err != nil {
		s.giveBack(ctx, conn, m, err)
		return mq.DriverDelivery{}, false
	}
	raw.Body = m.Body
	// The two arguments are deliberately DIFFERENT messages: the specification recorded
	// that `Envelope.Unwrap` must not be given the same pointer twice, and
	// the shape of the defect is that the decode reads the body it is
	// overwriting.
	msg := mq.Message{}
	if err := s.envelope.Unwrap(&msg, &raw); err != nil {
		s.giveBack(ctx, conn, m, err)
		return mq.DriverDelivery{}, false
	}
	h := &held{msg: m, conn: conn, subGen: which, gen: gen}
	token, ok := s.claimSlot(ctx, h, which)
	if !ok {
		// The source is closing or this pump has been superseded. The
		// delivery stays unacknowledged, and a reset is what brings it
		// back -- which is the one case where "the broker has it" is true.
		return mq.DriverDelivery{}, false
	}
	// Attempt is ZERO here, which is its own documented answer: "the
	// broker does not count attempts". Measured on both brokers, a STOMP
	// delivery carries no `delivery-count`, no `JMSXDeliveryCount` and no
	// `_AMQ_DELIVERY_COUNT` -- and the `redelivered` header is not even a
	// reliable boolean: measured on Artemis, the redelivery a REQUEUE
	// produces arrives with `redelivered: false`, and only one after a
	// reconnection arrives with true.
	//
	// Reporting 1 and 2 from that boolean was this driver's first answer and
	// it was worse than zero: the specification's `nack/immediate-redelivery` compares
	// the redelivery's attempt against the first delivery's and read "the
	// redelivery reports attempt 1 after a first delivery at 1", because
	// Artemis had said the redelivery was not one. A caller cannot tell that
	// 1 from a first delivery's. Zero says truthfully that the number is not
	// available, which is what makes a `MaxAttempts` policy above this
	// driver refuse to act rather than act on a number the wire never
	// carried; the boolean itself travels on `DeliveryInfo.Redelivered`, for
	// what it is worth.
	const attempt = 0
	return mq.DriverDelivery{
		Message: &msg,
		Info: mq.DeliveryInfo{
			Topic:        s.topic,
			Source:       s.dest,
			Subscription: s.sub,
			ReceivedAt:   s.b.now(),
			// Attempt: zero, meaning unavailable. See above.
			Attempt:     uint32(attempt),
			Redelivered: f.Redelivered,
		},
		Token: token,
		Gen:   gen,
		Ref:   m,
	}, true
}

// giveBack answers a delivery this driver cannot decode, so that it stops
// holding the consumer's credit.
//
// It is not handed over and it is not acknowledged: an ACK would destroy it,
// and the specification makes an undecodable body the caller's to see rather
// than the driver's to discard. Leaving it unacknowledged was this driver's
// first answer, and it stalled the consumer: every unacknowledged delivery
// holds one message of the broker's flow-control window (Classic's
// `activemq.prefetchSize`, Artemis's consumer window), so `prefetch` of them
// stopped the subscription for good, and a reconnect redelivered them and
// stopped it again. So the delivery goes where the settler's own
// `Nack(NackDeadLetter())` would send it:
//
//   - with Config.DeadLetterTopic set, a copy is published there and the
//     original is acknowledged only once the copy's RECEIPT arrives, on the
//     backend's connection and not this source's, whose process loop this
//     pump is part of;
//   - on Classic with no topic, a NACK frame, which Classic treats as a
//     POISON_ACK and hands to its own dead-letter strategy (ActiveMQ.DLQ by
//     default).
//
// Artemis with no topic has no frame that gives a delivery back without
// destroying it -- a NACK there is an acknowledgement -- so the delivery
// stays unacknowledged, as does one whose dead-letter copy failed. Both are
// counted in Undecodable, which is how a deployment sees the window shrink.
func (s *Source) giveBack(ctx context.Context, conn *gostomp.Conn, m *gostomp.Message, cause error) {
	s.undecodable.Add(1)
	switch {
	case s.b.cfg.DeadLetterTopic != "":
		snk, err := s.deadLetterSink(ctx)
		if err != nil {
			return
		}
		// The raw frame body, unlike the settler's copy (deadLetterCopy),
		// which publishes the decoded message: there is no decoded message
		// here, which is why this path exists. The sink wraps these bytes
		// once more under MetadataPortableEnvelope, and that is the point:
		// the bytes that failed to decode reach the dead-letter topic intact,
		// inside an envelope a consumer there CAN decode, rather than as a
		// second copy of the same undecodable frame.
		out := &mq.Message{Body: m.Body}
		out.Headers.Add(DeadLetterReasonHeader, []byte("undecodable: "+cause.Error()))
		out.Headers.Add(DeadLetterAttemptHeader, []byte("0"))
		if err := snk.Publish(ctx, out, nil); err != nil {
			return
		}
		_ = conn.Ack(m)
	case s.b.cfg.Broker == activemq.Classic:
		_ = conn.Nack(m)
	}
}

// readFields reads a delivery's frame headers into the shared mapping's form.
func readFields(f *protocol.Fields, m *gostomp.Message) {
	f.Reset()
	if m.Header == nil {
		return
	}
	for i := range m.Header.Len() {
		k, v := m.Header.GetAt(i)
		switch k {
		case "content-type":
			f.ContentType = v
		case "message-id":
			f.MessageID = v
		case "correlation-id":
			f.CorrelationID = v
		case "reply-to":
			f.ReplyTo = v
		case "redelivered":
			f.Redelivered = v == "true"
		case "persistent":
			f.Durable = v == "true"
		case "priority":
			if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 255 {
				f.Priority, f.HasPriority = uint8(n), true
			}
		case "expires":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				f.Expiry = millis(n)
			}
		case "timestamp":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				f.Timestamp = millis(n)
			}
		default:
			if protocol.ReservedFrameHeader(k) {
				// A header the protocol or the broker owns, which the
				// mapping reads through its own named fields. Carrying it
				// into the caller's headers would put `subscription: 1` and
				// `ack: 1082,1088` in a message.
				continue
			}
			f.Props = append(f.Props, protocol.Entry{Key: k, Text: v, Kind: protocol.Text})
		}
	}
}

// Fetch hands over buffered deliveries.
func (s *Source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	if err := s.b.CheckOpen(ctx); err != nil {
		return 0, err
	}
	// A subscription whose connection died is re-established here rather
	// than by the supervisor, for the reason `drivers/amqp10` gives: the
	// generation bump and the invalidation are EAGER because they are the
	// correctness half, and the re-subscribe is lazy because the next fetch
	// is the first thing that needs it.
	if err := s.ensureSubscribed(ctx); err != nil {
		// What the pump recorded about the connection that died is reported
		// first. A source that may not re-dial (NoReconnect, or a spent
		// ReconnectAttempts budget) fails HERE on every fetch after the
		// death, so a terminal error left for the branch below would never be
		// read, and the broker's own sentence -- the only diagnosis STOMP has
		// -- would be replaced by "the source does not reconnect".
		if terminal := s.takeErr(); terminal != nil {
			return 0, s.terminalError(terminal)
		}
		return 0, err
	}
	buf := s.buffer()
	n, err := buf.Fetch(ctx, dst)
	if n > 0 {
		return n, nil
	}
	if terminal := s.takeErr(); terminal != nil {
		return 0, s.terminalError(terminal)
	}
	if err == nil {
		return 0, nil
	}
	// A buffer that was CLOSED while this fetch was parked is a buffer that
	// was replaced, and the reason was already taken above: the specification makes
	// ErrClosed the answer only when the SOURCE is closed, so a swap reports
	// a retryable failure and the next fetch reads the new one.
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if !closed && errors.Is(err, mq.ErrClosed) {
		return 0, s.b.errs.MapNet(mq.OpFetch, s.topic, s.sub, gostomp.ErrClosedUnexpectedly, false)
	}
	return 0, s.b.errs.MapNet(mq.OpFetch, s.topic, s.sub, err, false)
}

// terminalError maps a terminal error the pump recorded for a fetch.
func (s *Source) terminalError(terminal error) error {
	if isConnGone(terminal) {
		return s.b.errs.MapNet(mq.OpFetch, s.topic, s.sub, terminal, false)
	}
	return s.b.errs.Map(mq.OpFetch, s.topic, s.sub, terminal)
}

// takeErr returns and clears the terminal error the pump recorded.
func (s *Source) takeErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.lastErr
	s.lastErr = nil
	return err
}

// ensureSubscribed re-sends the SUBSCRIBE frame when there is no live
// subscription and the source is not closing.
func (s *Source) ensureSubscribed(ctx context.Context) error {
	if s.subscribed() {
		return nil
	}
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.mu.Lock()
	sub, closed, gen := s.sub2, s.closed, s.gen
	s.mu.Unlock()
	if closed {
		return errClosed
	}
	if sub != nil && gen == s.b.Generation() {
		// A reset or another fetch subscribed while this one waited.
		return nil
	}
	if sub != nil {
		// The backend's connection moved under it. This source's own
		// connection may still be up, so the subscription is retired rather
		// than forgotten: one left on the socket is a second consumer whose
		// deliveries nothing reads.
		s.mu.Lock()
		s.sub2 = nil
		s.subGen++
		s.mu.Unlock()
		s.retire(sub)
	}
	return s.subscribe(ctx)
}

// subscribed reports whether the live subscription is current, without
// waiting for a subscribe in progress: it is every fetch's first question.
func (s *Source) subscribed() bool {
	s.mu.Lock()
	live, gen := s.sub2 != nil && !s.closed, s.gen
	s.mu.Unlock()
	return live && gen == s.b.Generation()
}

// retire unsubscribes a subscription this source no longer uses, and drops the
// connection when the broker cannot be shown to have let it go.
//
// An UNSUBSCRIBE that failed for any reason but the socket's death leaves the
// subscription in doubt -- a receipt timeout is the common one -- and a
// subscription in doubt is one the broker may still be dispatching to. Dropping
// the connection is the one answer that ends it for certain, and it is what a
// requeue wants anyway: the broker takes back every unacknowledged delivery on
// a socket that goes.
func (s *Source) retire(sub *gostomp.Subscription) {
	if err := sub.Unsubscribe(); err != nil && !isConnGone(err) {
		s.dropConnection()
	}
}

// Settler settles this source's deliveries.
func (s *Source) Settler() mq.Settler { return s.set }

// Close releases the source, unsubscribes, and unblocks a parked fetch.
//
// The order is the contract. The buffer is closed FIRST, so that a parked
// fetch wakes with ErrClosed rather than with whatever the unsubscribe did to
// the channel; the token table is invalidated SECOND, so that a pump parked
// waiting for a slot wakes and goes back to taking from its queue, where the
// wait at the end of Close can let it go; and only then is the subscription
// dropped, which is what makes the broker redeliver everything this consumer
// never acknowledged. A settlement racing the close is
// refused either way: `closed` is set before any of it, and [settler.begin]
// reads it first.
func (s *Source) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	sub := s.sub2
	s.sub2 = nil
	s.mu.Unlock()
	s.once.Do(func() { close(s.closing) })
	buf := s.buffer()
	conn := s.takeConnection()
	// Whatever the buffer still holds was never handed to a caller. Its
	// tokens are released so the accounting is clean; the messages
	// themselves stay unacknowledged, which is what makes the broker
	// redeliver them. Closing it is the one thing that wakes a pump parked
	// on a full buffer, and that pump has to be gone before the wait at the
	// end of Close can return. It once had to go before the unsubscribe as
	// well, when the pump was also the channel's reader and the
	// unsubscribe's RECEIPT sat behind it in go-stomp's process loop; the
	// drain reads the channel now.
	buf.Drain(func(d *mq.DriverDelivery) { _ = s.table.Release(d.Token) })
	_ = buf.Close()
	// The table goes next, for the same reason. A caller holding every token
	// leaves the pump parked in [Source.claimSlot], settling the held
	// deliveries cannot free it because a closed source refuses settlements,
	// and that wait does not watch stopPumps -- so invalidating is what wakes
	// the parked claim, the pump sees `closed`, and it goes back to taking
	// from its queue until Close lets it go. Before the drain existed that
	// parked pump also stopped go-stomp's loop, the UNSUBSCRIBE's RECEIPT was
	// never read, and Close hung for ever, Backend.Close with it.
	s.table.InvalidateAll()
	var err error
	if sub != nil {
		// Unsubscribing is what returns the unacknowledged deliveries: the
		// broker redelivers everything this subscription held, marked
		// `redelivered`. A close that only dropped the socket would leave
		// them in the broker's delivering set until it timed out.
		if uerr := sub.Unsubscribe(); uerr != nil && !isConnGone(uerr) {
			err = uerr
		}
	}
	if conn != nil {
		// The polite DISCONNECT is what tells the broker to release this
		// subscription's delivering set rather than waiting for the socket
		// to time out, which is what makes everything this consumer never
		// acknowledged available to the next one.
		if derr := conn.Disconnect(); derr != nil {
			_ = conn.MustDisconnect()
		}
	}
	// The pumps are let go and waited for only once the connection is gone.
	// Until then their drains have to keep reading, because the RECEIPTs the
	// unsubscribe and the disconnect wait for are read by the same go-stomp
	// loop that feeds their channels. Waiting for them to end on their own
	// hung Close for ever on a broker that never answered: go-stomp closes a
	// channel whose UNSUBSCRIBE timed out only when the connection goes, and
	// not always then (see stopPumps).
	close(s.stopPumps)
	s.pumpsWG.Wait()
	s.dlmu.Lock()
	dl := s.dl
	s.dl = nil
	s.dlmu.Unlock()
	if dl != nil {
		if derr := dl.Close(); derr != nil && err == nil {
			err = derr
		}
	}
	s.b.forget(s, nil)
	if err != nil {
		return s.b.errs.Map(mq.OpClose, s.topic, s.sub, err)
	}
	return nil
}

// takeConnection removes and returns this source's connection.
func (s *Source) takeConnection() *gostomp.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn := s.conn
	s.conn = nil
	return conn
}

// connectionLost invalidates everything this source was holding.
//
// It is the step that has to happen before anything else: from the instant the
// connection died every delivery this source handed out is stale, and a
// settlement carrying one must be refused rather than applied to whatever
// takes its place.
func (s *Source) connectionLost(cause error) {
	s.table.InvalidateAll()
	// The BACKEND's connection died, which for a source means the client is
	// being repaired around it: its own connection goes too, so that the
	// next fetch re-establishes both and a settlement in flight is refused
	// rather than written to a subscription nothing will answer for.
	s.loseConnection()
	s.remember(cause)
	s.swapBuffer()
}

// reset is the requeue mechanism: unsubscribe, re-subscribe, and let the
// broker redeliver.
//
// It is the whole of this driver's answer to Nack, and the reason is
// measured rather than argued (doc.go): a STOMP NACK frame returns nothing on
// either broker. What DOES return a message is the subscription going away,
// so that is what a requeue does.
//
// Its cost is the one the capability note states: EVERY unacknowledged
// delivery of this subscription comes back, so every token issued before the
// reset is invalidated and a later settlement of one reports a lost lease.
func (s *Source) reset(ctx context.Context) error {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errClosed
	}
	sub := s.sub2
	s.sub2 = nil
	s.subGen++
	s.mu.Unlock()

	// Invalidate BEFORE the unsubscribe: from the moment the broker is told
	// to take the subscription back, every delivery it held is its own
	// again, and a settlement in flight must be refused rather than written
	// to a subscription that is going.
	s.table.InvalidateAll()
	s.swapBuffer()
	if sub != nil {
		// A failed unsubscribe used to end the reset here, with the old
		// subscription possibly still on the socket and no new one: the
		// consumer stopped receiving and the next fetch subscribed beside the
		// orphan. Retiring drops the connection in that case, which is a
		// requeue by other means, so the reset carries on.
		s.retire(sub)
	}
	s.resets.Add(1)
	if err := s.subscribe(ctx); err != nil {
		return err
	}
	return nil
}

// held is what a token resolves to: the library's own message, the connection
// its subscription is on, and the two generations that say whether it is still
// settleable.
type held struct {
	msg    *gostomp.Message
	conn   *gostomp.Conn
	subGen uint64
	gen    uint32
}

// buffer is the live bounded buffer.
func (s *Source) buffer() *driverkit.PushAdapter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending
}

// swapBuffer closes the live buffer, which wakes every parked fetch, and
// installs a fresh one.
//
// It is how a terminal error reaches a PARKED fetch, and it is the only way:
// `driverkit.PushAdapter` wakes a parked Fetch when it is CLOSED and has no
// other signal, so a driver that merely recorded the error would leave a
// consumer parked on a buffer nothing will ever push to. Closing it drops
// whatever it held, which is correct -- those deliveries were never handed to
// a caller and are still the broker's, unacknowledged -- and the fresh buffer
// is what the re-subscription fills.
//
// The replacement is skipped once the source is closing, so that a close
// cannot be undone by a racing failure.
func (s *Source) swapBuffer() {
	s.mu.Lock()
	old, closed := s.pending, s.closed
	if !closed {
		fresh, err := driverkit.NewPushAdapter(driverkit.PushConfig{
			Capacity: s.prefetch,
			MaxBytes: s.b.cfg.PrefetchBytes,
		})
		if err == nil {
			s.pending = fresh
		}
	}
	s.mu.Unlock()
	if old != nil {
		old.Drain(func(d *mq.DriverDelivery) { _ = s.table.Release(d.Token) })
		_ = old.Close()
	}
}

// deadLetterSink is the sink a dead-lettered delivery is published to.
//
// It is opened lazily and kept, because a source that never rejects anything
// should not pay for a handle it does not use -- and because opening it at the
// source's own open would make a configuration error in the dead-letter topic
// a failure of the CONSUMER, which is not where a caller would look for it.
func (s *Source) deadLetterSink(ctx context.Context) (mq.Sink, error) {
	s.dlmu.Lock()
	defer s.dlmu.Unlock()
	if s.dl != nil {
		return s.dl, nil
	}
	snk, err := s.b.OpenSink(ctx, mq.SinkConfig{
		Topic: s.b.cfg.DeadLetterTopic,
		// The specification: the original may not be acknowledged until the dead-letter
		// publication is durable, so the boundary is the strongest one this
		// protocol has rather than the caller's.
		Confirmation: mq.ConfirmDurable,
	})
	if err != nil {
		return nil, err
	}
	s.dl = snk
	return snk, nil
}

// Undecodable is how many deliveries this source could not map. None of them
// is discarded: each is dead-lettered where this configuration has a route,
// and is otherwise left unacknowledged, so it is redelivered when the
// subscription goes away (see [Source.giveBack]).
func (s *Source) Undecodable() uint64 { return s.undecodable.Load() }

// TokenWaits is how many deliveries waited for a settlement to free a token
// slot. A non-zero count is the prefetch being smaller than what the caller
// holds unsettled, which is a tuning fact rather than a fault; before
// The specification each one of these was a message thrown away instead.
func (s *Source) TokenWaits() uint64 { return s.tokenWaits.Load() }

// millis is an epoch-millisecond instant.
func millis(n int64) time.Time { return time.UnixMilli(n) }

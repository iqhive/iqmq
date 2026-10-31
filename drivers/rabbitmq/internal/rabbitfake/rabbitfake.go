// Package rabbitfake is an in-process, deterministic, scriptable RabbitMQ: a
// [client.Dialer] that needs no broker, no container and no network, because
// it IS the broker.
//
// It is the RabbitMQ equivalent of `internal/streams/fakestreams` and it sits
// one level higher than `internal/streams/respsrv`. respsrv is a real TCP
// server speaking real RESP, so a client LIBRARY can be driven with no
// container; this fake stops at the library seam
// (`drivers/rabbitmq/internal/client`) instead, and the reason is that on this
// driver the library is not a transparent pipe. Read that package's comment:
// the four behaviours it lists are all things amqp091-go interposes between a
// broker's frames and the driver's Go channels, and three of them cannot be
// provoked from a broker at all. A socket-level fake would reproduce the
// PROTOCOL faithfully and then hand the frames to the real library, which
// would be the honest thing to do if the library were the thing under test. It
// is not; the driver is.
//
// So this package reproduces two things at once, and the split is worth
// knowing when you read it. The BROKER half -- topology, routing, FIFO queues,
// prefetch, settlement, dead-lettering, TTL -- is a plain model of RabbitMQ
// under one mutex. The LIBRARY half is `confirms.go`, which is a deliberate
// re-implementation of `amqp091.confirms` field for field, because the
// expansion of a cumulative ack, the resequencing of an out-of-order one and
// what `deferredConfirmations.Close` does to what is outstanding are the
// driver's hardest cases and the library is the only thing that produces them.
//
// # What it can do that a container cannot
//
// A container gives you a real broker and a real clock. This gives you a
// broker whose clock you set, whose queue depth and unacknowledged count you
// read between two operations, whose confirms arrive cumulatively or backwards
// because you said so, and whose channel closes at the instant you choose --
// with a publication outstanding, which is the ambiguity and the one case
// a driver cannot be trusted on without a test. `TestCriticalChannelCloseDuringConfirm`
// is a dozen lines here and is not writable at all against a container.
//
// # What it is NOT
//
//   - AMQP 0-9-1. Nothing here frames a byte. The seam carries the library's
//     own value types -- [amqp091.Publishing], [amqp091.Delivery],
//     [amqp091.Return], [amqp091.Confirmation] -- so the wire format is
//     exactly the part that is absent, and `conformance/` against a real
//     broker is what covers it.
//   - complete. It implements the topology, the routing and the settlement
//     this driver uses, and refuses the rest by name rather than ignoring it:
//     an exchange type it does not know is a 503, a queue type it does not
//     know is a 406. A driver that started using something new fails loudly
//     here instead of passing a suite that never ran.
//   - a performance model. Everything is a map and a slice under one mutex,
//     and every benchmark in this repository that measures the RabbitMQ driver
//     measures it against a container.
//   - a scheduler. Time moves when [Broker.Advance] says so and at no other
//     moment; see [Broker.expireLocked].
//
// # Determinism
//
// Two runs of one script produce identical observations, and three rules buy
// that. Nothing reads a wall clock except the opt-in notification timeout
// ([notifyAll] says why). Every collection whose order is observable -- the
// queues, the exchanges, an exchange's bindings, a queue's consumers, the
// tags a cumulative confirm resolves -- is an ordered slice or is sorted
// before it is read, never a ranged map. And [Broker.Sync] is the barrier that
// makes "has the dispatcher got there yet" a question with an answer: one
// goroutine per connection dispatches frames in wire order, so a test that
// wants to observe after a publication calls Sync and then asserts.
package rabbitfake

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
)

// DefaultProduct and DefaultVersion are what the broker reports in its
// `server_properties` when the configuration names nothing.
//
// The version is 4.1 and not the newest release, because it is the interesting
// one: it is above the 4.0 the AMQP 1.0 profile requires and below the 4.3 one
// of the claims is gated on, so a test that sets nothing gets a broker
// where some gates are open and some are shut rather than one where every
// answer is yes.
const (
	DefaultProduct = "RabbitMQ"
	DefaultVersion = "4.1.0"
)

// DefaultConsumeBuffer is the buffer on a consumer's delivery channel.
//
// The library buffers a consumer without bound, in a goroutine per consumer
// (`amqp091.consumers.buffer`), so a driver that is slow to read its
// deliveries never stalls the connection's reader. This fake uses a buffer
// instead, and the trade is deliberate. A pump goroutine per consumer would
// make "has this delivery arrived yet" a scheduling question, which is exactly
// the determinism this package is for; a buffer keeps the dispatcher's single
// goroutine and the prefetch keeps the buffer from filling, because a consumer
// with a prefetch of N never holds more than N unsettled deliveries. What is
// given up is the one behaviour a driver is not tested for here -- a consumer
// slow enough to stall its connection -- and [Config.ConsumeBuffer] is the
// knob for a test that wants to provoke it.
const DefaultConsumeBuffer = 256

// DeliveryCountMode decides which queues expose `x-delivery-count`.
//
// It is a knob because the asymmetry is a `(verify)` a quorum queue
// exposes the header and a classic queue does not have it at all, and the
// driver's capability table says so. A claim that rests on an asymmetry needs
// a test on both sides of it, and the mode is how a test asks for the side the
// topology it declared would not give it.
type DeliveryCountMode uint8

// The delivery-count modes.
const (
	// DeliveryCountQuorum is RabbitMQ's own behaviour and the default: the
	// header appears on a delivery out of a queue declared
	// `x-queue-type: quorum`, from the SECOND delivery onwards, carrying the
	// count of attempts that did not end in an ack. It is absent on the first
	// delivery and absent from a classic queue entirely.
	DeliveryCountQuorum DeliveryCountMode = iota
	// DeliveryCountAlways puts the header on every delivery from every queue,
	// zero included. It is for a test that wants the driver's Attempt mapping
	// exercised without declaring a quorum queue.
	DeliveryCountAlways
	// DeliveryCountNever removes the header everywhere, which is the broker a
	// driver meets when somebody's topology was declared classic by mistake.
	DeliveryCountNever
)

// Config configures a [Broker]. The zero value is a usable broker.
type Config struct {
	// Clock is the time source. Nil means a frozen clock starting at a fixed
	// instant, which is what makes a TTL in a test a number the test chose
	// rather than a number the scheduler chose. [Broker.Advance] moves time
	// whichever clock is in use, by an offset on top of it.
	Clock mq.Clock

	// Product and Version are what `server_properties` reports, and they are
	// configurable because four of the claims are gated on the broker's
	// version: AMQP 1.0 needs 4.0, `x-delivery-count` needs a quorum queue,
	// and two more need 3.13 and 4.3. A gate nobody has watched refuse is
	// assumed not to work.
	Product string
	Version string
	// Capabilities is merged into `server_properties` under "capabilities",
	// which is where a broker advertises `publisher_confirms`,
	// `exchange_exchange_bindings` and the rest.
	Capabilities amqp.Table

	// DelayedMessagePlugin reports whether
	// `rabbitmq_delayed_message_exchange` is installed. When it is not, an
	// `exchange.declare` of type `x-delayed-message` is refused with the
	// broker's own 503 COMMAND_INVALID, which is what the detection
	// reads.
	DelayedMessagePlugin bool

	// NotifyTimeout is how long a notification send may block the connection's
	// dispatcher before it is abandoned. Zero blocks for ever, which is the
	// deterministic default; negative abandons immediately. See [notifyAll],
	// which is where the whole of this decision is written down.
	NotifyTimeout time.Duration

	// DeliveryCount decides which queues expose `x-delivery-count`.
	DeliveryCount DeliveryCountMode
	// DeliveryCountName is the header the count travels in. Empty means
	// [DeliveryCountHeader], which is what RabbitMQ 3.13 and 4.1 send;
	// [AcquiredCountHeader] is what 4.3 sends.
	//
	// It exists because the rename is the single most consequential
	// difference the version matrix found, and a driver that read only one
	// name would report attempt 1 on every delivery from one of the two.
	// Making it a broker option is what lets the driver's reading of both be
	// held by a test with no container at all.
	DeliveryCountName string

	// Confirms is the confirm framing new channels get. The zero value is
	// [ConfirmPerTag].
	Confirms ConfirmScript

	// ConsumeBuffer is the buffer on a consumer's delivery channel; zero means
	// [DefaultConsumeBuffer]. A buffer of one makes the dispatcher block on a
	// consumer that is not reading, which is how a test provokes a stalled
	// connection.
	ConsumeBuffer int

	// MaxChannels bounds the channels one connection may open; zero means no
	// bound. Reaching it is [amqp091.ErrChannelMax], which is the library's
	// own refusal and not a broker frame.
	MaxChannels int

	// OnConnection and OnChannel are called once per connection and once per
	// channel, as it opens, BEFORE the driver's own call returns. They are the
	// answer to "each fault addressable at a chosen moment": the driver opens
	// its own channels, so a test that wants to arm a fault on the channel the
	// driver is about to publish on has to be given it at the moment it
	// exists. Neither is called with any lock held, so a hook may use the
	// whole of the handle's surface.
	OnConnection func(*ConnHandle)
	OnChannel    func(*ChannelHandle)
}

// Broker is the fake. One Broker is one RabbitMQ server.
//
// Everything the broker holds is under one mutex, and every operation takes it
// for the whole operation. That is not a performance model and is not meant to
// be one: what it buys is that a publication, a routing decision, an
// enqueueing and the confirm frame that answers it are one atomic step, so
// there is no interleaving for a test to be surprised by and no lock order for
// a reader of this package to hold in their head. The one thing that happens
// OUTSIDE the mutex is a notification send, which is the connection
// dispatcher's job; see [fakeConn.dispatch].
type Broker struct {
	mu  sync.Mutex
	cfg Config

	clock  mq.Clock
	offset time.Duration

	exchanges map[string]*exchange
	exOrder   []string
	queues    map[string]*queue
	qOrder    []string

	conns []*fakeConn

	// seq, delivery, generated, consumerSeq and replySeq are the broker's
	// counters. They are broker-wide and monotone so that an identity is never
	// reused, which is what lets an observation name a delivery rather than a
	// tag.
	seq         uint64
	delivery    uint64
	generated   uint64
	consumerSeq uint64
	replySeq    uint64

	// received is the arrival log, keyed by queue NAME rather than held on
	// the queue itself, so that it survives a `queue.delete` and a
	// re-declaration. It is an observation log and not queue state: a test
	// asking "did this binding ever reach that queue" is asking about the
	// past, and a log that vanished with the queue would answer a different
	// question.
	received map[string][]Publication

	settlements []Settlement
	deadLetters []DeadLetter
	drops       []Drop
	dials       []client.DialConfig
	dialErrs    []error
	// down models a broker that is not listening: every dial is refused
	// until it is cleared. See [Broker.SetDown].
	down bool
	// declGate parks declarations until a test steps them. See
	// [Broker.HoldDeclarations].
	declGate *DeclareGate

	closed bool
}

// New returns a broker with the broker's own exchanges already declared.
func New(cfg Config) *Broker {
	b := &Broker{
		cfg:       cfg,
		clock:     cfg.Clock,
		exchanges: map[string]*exchange{},
		queues:    map[string]*queue{},
		received:  map[string][]Publication{},
	}
	if b.clock == nil {
		b.clock = frozenClock{at: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	}
	if b.cfg.Product == "" {
		b.cfg.Product = DefaultProduct
	}
	if b.cfg.Version == "" {
		b.cfg.Version = DefaultVersion
	}
	if b.cfg.ConsumeBuffer <= 0 {
		b.cfg.ConsumeBuffer = DefaultConsumeBuffer
	}
	// The default exchange and the five the broker declares for itself. They
	// are here rather than absent because a driver may publish through
	// `amq.topic` or bind to it, and because a DECLARE of any of them is a 403
	// -- which is a refusal `internal/rabbit.CheckName` exists to keep a
	// caller's prefix out of, and therefore a refusal worth being able to
	// provoke.
	for _, pre := range []struct{ name, kind string }{
		{"", amqp.ExchangeDirect},
		{"amq.direct", amqp.ExchangeDirect},
		{"amq.topic", amqp.ExchangeTopic},
		{"amq.fanout", amqp.ExchangeFanout},
		{"amq.headers", amqp.ExchangeHeaders},
		{"amq.match", amqp.ExchangeHeaders},
	} {
		b.exchanges[pre.name] = &exchange{
			name: pre.name, kind: pre.kind, durable: true, predeclared: true,
		}
		b.exOrder = append(b.exOrder, pre.name)
	}
	return b
}

// Dialer returns the [client.Dialer] to hand the driver. Every connection it
// opens is this broker's.
func (b *Broker) Dialer() client.Dialer { return fakeDialer{b: b} }

// Close closes every connection, stops every dispatcher and makes further
// dials fail. It is idempotent.
//
// It abandons notifications rather than waiting for them. A listener the
// driver stopped reading would otherwise park a dispatcher for ever and no
// tear-down could complete, which is the deadlock the library's five-second
// timeout exists to avoid; here the tear-down itself is the signal, so a
// notification that has nowhere to go is dropped and the goroutine exits. That
// is why [Broker.Close] is not a way to observe a close notification and
// [ChannelHandle.CloseWith] is.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	conns := append([]*fakeConn(nil), b.conns...)
	gate := b.declGate
	b.mu.Unlock()
	if gate != nil {
		// A declaration parked at the gate would otherwise hold the goroutine
		// that made it for ever, which turns a test that forgot to release
		// into a test that hangs.
		gate.Release()
	}
	for _, c := range conns {
		c.close(nil, true)
	}
	return nil
}

// Now is the broker's current time: the injected clock plus whatever
// [Broker.Advance] has added.
func (b *Broker) Now() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.nowLocked()
}

// Advance moves the broker's clock forward and runs every deadline that is now
// in the past: a message whose `x-message-ttl` has elapsed is dead-lettered, a
// delayed-message publication whose delay has elapsed is routed, and a queue
// whose `x-expires` has elapsed is deleted.
//
// It is how the specification's `ttldlx` mechanism is tested with no broker: park a
// message in a delay queue, Advance past the TTL, and assert that it came back
// through the dead-letter route. Against a container the same test is a real
// sleep, and a sleep in a test about a thirty-second delay is not a test
// anybody runs.
func (b *Broker) Advance(d time.Duration) {
	b.mu.Lock()
	if d > 0 {
		b.offset += d
	}
	b.expireLocked()
	b.pumpLocked()
	b.mu.Unlock()
	b.Sync()
}

// Tick runs the deadline sweep without moving the broker's own offset, for a
// test that drives the injected clock itself -- inside testing/synctest, say,
// where time moves when the bubble says so.
func (b *Broker) Tick() { b.Advance(0) }

// Sync waits until every connection's dispatcher has drained the frames
// already enqueued for it.
//
// It is the barrier that makes an observation deterministic. A publication
// enqueues a `basic.return` and a `basic.ack` and returns; whether the
// driver's listeners have seen them yet is a scheduling question until Sync
// answers it. A test that reads a notification channel does not need Sync --
// the receive is its own barrier -- but one that asserts on
// [Broker.Settlements] or a queue's depth right after an operation does.
func (b *Broker) Sync() {
	b.mu.Lock()
	conns := append([]*fakeConn(nil), b.conns...)
	b.mu.Unlock()
	for _, c := range conns {
		c.sync()
	}
}

// SetServerVersion changes what `server_properties` reports. It affects
// connections opened after it, which is the only shape that makes sense: a
// broker does not change version under a live connection.
func (b *Broker) SetServerVersion(product, version string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.Product, b.cfg.Version = product, version
}

// SetDelayedMessagePlugin installs or removes
// `rabbitmq_delayed_message_exchange`, which decides whether an
// `exchange.declare` of type [ExchangeDelayed] is accepted or refused with
// 503 COMMAND_INVALID.
func (b *Broker) SetDelayedMessagePlugin(installed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.DelayedMessagePlugin = installed
}

// SetNotifyTimeout changes the abandonment policy for notification sends. See
// [notifyAll] for what the three regimes are.
func (b *Broker) SetNotifyTimeout(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.NotifyTimeout = d
}

// SetDeliveryCountName changes the header the delivery count travels in, so
// that one test can imitate both broker generations.
func (b *Broker) SetDeliveryCountName(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.DeliveryCountName = name
}

// SetDeliveryCountMode changes which queues expose `x-delivery-count`.
func (b *Broker) SetDeliveryCountMode(m DeliveryCountMode) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.DeliveryCount = m
}

// SetConfirmScript changes the confirm framing new channels are opened with.
// [ChannelHandle.SetConfirmScript] changes one channel's.
func (b *Broker) SetConfirmScript(s ConfirmScript) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.Confirms = s
}

// FailNextDial makes the next dial return err instead of a connection. Calls
// queue, so two of them fail two dials, which is what a bounded-reconnection
// test needs.
func (b *Broker) FailNextDial(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dialErrs = append(b.dialErrs, err)
}

// SetDown makes every dial fail, or stops doing so.
//
// It models a broker that is not LISTENING rather than one that refuses a
// connection: the port is closed, so the dial itself fails and the driver's
// dial loop retries with its backoff. [Broker.FailNextDial] queues a fixed
// number of failures and this one lasts until it is cleared, which is the
// difference between "the first attempt failed" and "the broker is down".
//
// It exists for [Broker.Restart]: a restart has to close the connections AND
// forget the topology before any dial can succeed, or the driver's replay
// races the forget and has its own declarations deleted.
func (b *Broker) SetDown(down bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.down = down
}

// Dials is every dial the driver attempted, with the configuration it dialled
// with, in order. It is how a test asserts that the heartbeat, the frame size
// and the client properties the caller configured reached the wire.
func (b *Broker) Dials() []client.DialConfig {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]client.DialConfig(nil), b.dials...)
}

// Publish publishes one message as the BROKER rather than as a client, and
// reports how many queues it reached.
//
// It exists so that a test can put a message where it wants one without
// opening a channel to do it. Seeding a queue through the driver's own
// publisher makes the arrangement of a settlement test depend on the publish
// path being correct, which is the other half of the same driver.
func (b *Broker) Publish(exchange, key string, msg amqp.Publishing) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, amqp.ErrClosed
	}
	b.expireLocked()
	b.seq++
	m := &message{id: b.seq, exchange: exchange, routingKey: key, pub: msg}
	m.pub.Headers = cloneTable(msg.Headers)
	if msg.Body != nil {
		m.pub.Body = append([]byte(nil), msg.Body...)
	}
	routed := b.routeAndEnqueueLocked(exchange, key, m)
	b.pumpLocked()
	return routed, nil
}

// frozenClock is the clock a broker gets when the caller injects none.
//
// It is frozen rather than running, and that is the point: an expiry computed
// from a frozen clock is exactly the expiry the test asked for, and a test
// that wants time to pass says so with [Broker.Advance]. `fakestreams` made
// the same choice for the same reason.
type frozenClock struct{ at time.Time }

// Now returns the fixed instant.
func (c frozenClock) Now() time.Time { return c.at }

// NewTimer returns a timer that never fires.
//
// Nothing in this package asks for one -- the deadline sweep is what replaces
// every timer here -- and a timer that fired off a frozen clock would fire at
// a moment no test chose. Returning one that never fires keeps [mq.Clock]
// satisfied and makes a future caller that reaches for a timer hang visibly
// rather than pass quietly.
func (c frozenClock) NewTimer(time.Duration) mq.Timer { return frozenTimer{} }

// frozenTimer is a timer that never fires.
type frozenTimer struct{}

// C returns a channel nothing is ever sent on.
func (frozenTimer) C() <-chan time.Time { return nil }

// Stop reports that the timer was still pending.
func (frozenTimer) Stop() bool { return true }

// Reset reports that the timer was still pending.
func (frozenTimer) Reset(time.Duration) bool { return true }

// fakeDialer is the [client.Dialer] a [Broker] hands out.
type fakeDialer struct{ b *Broker }

var _ client.Dialer = fakeDialer{}

// Dial opens a connection to the broker, recording the configuration it was
// asked for.
//
// The URL list is recorded and otherwise ignored: there is one broker, so
// "try them in order" has nothing to choose between. What a test wants from
// the endpoint loop is that a dial that FAILS moves on, and
// [Broker.FailNextDial] is how that is provoked -- one queued failure per URL
// the driver should skip.
func (d fakeDialer) Dial(ctx context.Context, urls []string, cfg client.DialConfig) (client.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(urls) == 0 {
		return nil, errNoEndpoint
	}
	b := d.b
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, amqp.ErrClosed
	}
	b.dials = append(b.dials, cfg)
	if b.down {
		b.mu.Unlock()
		return nil, errBrokerDown
	}
	if len(b.dialErrs) > 0 {
		err := b.dialErrs[0]
		b.dialErrs = b.dialErrs[1:]
		b.mu.Unlock()
		return nil, err
	}
	c := b.newConnLocked(cfg)
	b.mu.Unlock()
	c.start()
	if hook := b.cfg.OnConnection; hook != nil {
		hook(&ConnHandle{c: c})
	}
	return c, nil
}

// errNoEndpoint is a dial with no address, which the driver's own Validate
// should already have refused. It is here so that the fake refuses it too
// rather than opening a connection to nothing.
var errNoEndpoint = &amqp.Error{Code: amqp.NotAllowed, Reason: "rabbitfake: no endpoint to dial"}

// errBrokerDown is what a dial gets while [Broker.SetDown] is on.
//
// It is a TRANSPORT failure and not a protocol one, because that is what a
// closed port is: `net.ErrClosed` is the error a real dial to a broker that is
// not listening produces, and the driver's own classification of a transport
// failure (`isTransportFailure`) is what decides whether the dial loop retries.
// An `*amqp.Error` here would be a broker that answered, which a broker that is
// down cannot do.
var errBrokerDown = fmt.Errorf("rabbitfake: the broker is not listening: %w", net.ErrClosed)

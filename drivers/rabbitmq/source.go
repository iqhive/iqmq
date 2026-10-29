package rabbitmq

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
)

var _ mq.Source = (*source)(nil)

// source is one consumer: one AMQP channel, one `basic.qos` prefetch, one
// delivery-tag space and one token table.
//
// RabbitMQ PUSHES: `basic.consume` gives the client a stream of deliveries and
// the client's only flow control is the prefetch and the pace at which it
// settles. The SPI asks for a pull `Fetch`. [driverkit.PushAdapter] is what
// makes the two meet, and its bound is what makes the meeting safe — a
// consumer that buffered without bound would turn a slow handler into
// unbounded memory growth, which the specification forbids.
//
// The prefetch is the ring's capacity, the token table's size and the
// per-delivery storage's length, all three, and that is not a coincidence: the
// broker will not have more than `prefetch` deliveries unsettled at this
// consumer, so anything sized larger would be storage nothing can fill and
// anything smaller would be a ring the broker can overrun. The broker's count
// and the table's are not the same count at every instant, though: a
// settlement frees its slot only after its frame is written, and the broker
// can send the next delivery in between, which is why a full table is WAITED
// on rather than read as a broker that overran its prefetch ([source.claimSlot]).
type source struct {
	b        *Backend
	topic    string
	sub      string
	names    rabbit.Names
	prefetch int
	env      *driverkit.Envelope
	opts     protocol.Options
	delay    delay.Strategy
	adapter  *driverkit.PushAdapter
	// tokens resolves a settlement back to its delivery tag, and its
	// generation is what makes a stale settlement detectable: a reconnection
	// invalidates every token, so a settlement carrying one from before is
	// refused with ErrLeaseLost rather than applied to whatever delivery
	// took the slot.
	tokens *driverkit.TokenTable
	// tag is this consumer's own `basic.consume` tag, which is what
	// `rabbitmqctl list_consumers` shows and what a broker-side cancel
	// names.
	tag string

	// attachMu serialises attachment. It is a second lock and not a flag,
	// because an attach performs I/O -- a declaration, a `basic.qos`, a
	// `basic.consume` -- and holding `mu` across that would block every
	// settlement for the length of a round trip.
	//
	// It exists because TWO routes repair a broken source and they can run at
	// once: the pump's own `NotifyClose` handler, and the backend's
	// supervision loop after a lost connection. Without it both attach, the
	// source ends up with two channels and two `basic.consume`s for one
	// subscription, and the broker then hands the same message to each --
	// which is a delivery the caller sees twice with only one of them
	// settleable. `FuzzRabbitTagSettlement` found it.
	attachMu sync.Mutex

	mu sync.Mutex
	// ch is the current channel, or nil while a reconnection is in flight.
	ch client.Channel
	// chConn is the connection ch was opened on. It is what lets an attach
	// tell a live channel on the CURRENT connection -- which it must leave
	// alone -- from one a failed dial attempt left behind on a connection
	// that has since been replaced.
	chConn client.Conn
	// tr is the confirm bookkeeping for the RE-PUBLICATIONS a delayed nack
	// makes. The consume channel is in confirm mode for that one reason:
	// parking a message in a delay queue and then acknowledging the original
	// is only safe if the parked copy is known to be stored, and a confirm
	// is the only thing that knows.
	tr *confirm.Tracker
	// gen is the channel generation. It is carried on every delivery and
	// checked at settlement, which is the second of the two staleness
	// guards: the token table catches a token from before an invalidation,
	// and this catches a delivery whose channel has been replaced without
	// one.
	gen uint32
	// pumpCancel stops the CURRENT channel's pump, and it is what keeps a
	// delivery from the old channel out of the ring.
	//
	// The window it closes is the engine's own recurring defect class in a
	// driver's clothing: a record placed somewhere the source holds it while
	// the source is being torn down. `admit` checks the generation before it
	// claims a token, and then BLOCKS in `PushAdapter.Push` while the ring is
	// full -- which is the normal case under backpressure. If the channel
	// goes during that block, the drain in `invalidate` runs and the blocked
	// push then inserts a delivery the broker has already requeued, so the
	// caller is handed a delivery nobody can settle AND the same message
	// arrives again on the new channel. `FuzzRabbitTagSettlement` found it in
	// forty seconds.
	//
	// Cancelling the pump's context makes the blocked push return rather than
	// insert, and `invalidate` cancels BEFORE it drains, so a push that had
	// already completed is drained and one that had not is refused.
	pumpCancel context.CancelFunc
	// parks are the delayed-nack re-publications waiting for their confirm,
	// keyed by delivery tag. There is at most one per in-flight settlement,
	// so the map is bounded by the prefetch.
	parks map[uint64]*parkWait
	// started records that the caller has asked for a delivery at least
	// once, which is when the `basic.consume` happens. See [source.attach].
	started bool

	// cancelled records that the BROKER cancelled this consumer, which
	// happens when the queue it was consuming is deleted. It is a different
	// condition from a close and is reported differently: the caller did not
	// close anything, and the queue is gone. A reconnection whose
	// `basic.consume` meets 404 sets it too (see [source.markGone]), because
	// that is the same deletion seen by a source that had no channel to hear
	// the cancel on.
	cancelled atomic.Bool
	// refused records that the broker refused a reconnection's
	// `basic.consume` for this started source with something other than 404
	// (see [source.consumeRefused]), and that nothing has attached it since.
	// While it is set a Fetch asks the broker again, as the first Fetch of a
	// source that has never started does, instead of only waiting for the
	// next reconnection: an active/standby pair whose standby took the
	// exclusive consumer during an outage would otherwise never fail back
	// to this client. It is set under `attachMu` and cleared where a channel
	// is installed.
	refused atomic.Bool
	// stale counts the deliveries this source dropped because their channel
	// had been replaced before they reached the ring. It is exported for a
	// test because the property is invisible from the portable API: a source
	// that handed out a superseded delivery would fail a settlement the
	// caller can do nothing about, and the only way to tell that apart from
	// a source that never reached the case is to count.
	stale atomic.Uint64
	// slotWaits counts the deliveries that found the token table full and
	// waited for a slot, which is [source.claimSlot]'s path. It is exported
	// for a test for [source.stale]'s reason: the wait is invisible from the
	// portable API, and a test that did not force it would pass without ever
	// reaching it.
	slotWaits atomic.Uint64
	// stranded mirrors "this consumer has started and has no channel" OUTSIDE
	// `mu`, and it is the only thing [Backend.Health] reads from a source.
	//
	// It is an atomic for [sink.stranded]'s reason: `settler.settle` holds
	// `mu` across the `basic.publish` a delayed nack parks with, so a Health
	// that took `mu` could wait on the socket. It is written only where `ch`
	// is, under `mu`, and it carries `started` with it because a source that
	// has never fetched has no channel ON PURPOSE (see [source.attach]).
	stranded atomic.Bool
	// repairing and repairCause are [sink.repairing] and [sink.repairCause] on
	// the consuming side, and carry the same trade-off: the state a caller is
	// told about is DERIVED from a flag the repair goroutine owns for exactly
	// its own lifetime, and the cause is best effort.
	repairing   atomic.Bool
	repairCause atomic.Pointer[repairFailure]

	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// store is ONE ATTACHMENT's delivery storage: the decoded message and the
// protocol fields for each of the token table's slots, indexed by the slot.
//
// It belongs to the attachment and not to the source, and that is the whole
// point of the type. The slot INDEX comes from the token table, whose
// `InvalidateAll` frees every slot at once on a reconnection or a channel
// exception -- so the replacement channel's pump can claim a slot whose
// previous delivery the old pump is still decoding into, or whose *Message a
// CALLER is still holding. Source-wide storage made both of those the same
// memory: two pumps writing one `mq.Message` (which the race detector found
// through `FuzzRabbitTagSettlement`) and, worse and silently, a body the specification
// says stays borrowed until the delivery is settled being overwritten under a
// caller who is reading it.
//
// One allocation pair per attachment is the whole cost, and an attachment
// happens on the first Fetch and once per outage.
type store struct {
	msgs []mq.Message
	flds []protocol.Fields
}

// newStore is one attachment's storage, sized to the prefetch because that is
// the token table's capacity and therefore the highest slot index it can hand
// out.
func newStore(prefetch int) *store {
	return &store{
		msgs: make([]mq.Message, prefetch),
		flds: make([]protocol.Fields, prefetch),
	}
}

// deliveryRef is what a settlement needs in order to act: the delivery tag,
// the channel it was issued on, and the generation that channel had.
//
// The CHANNEL is in the reference and not read from the source, and that is
// the guard: a settlement resolved against the source's current channel would
// send an ack for an old tag on a new channel, which is the disqualifier.
// Holding the channel means a stale settlement can be recognised by comparing
// generations and refused before any frame is written.
type deliveryRef struct {
	ch  client.Channel
	tag uint64
	gen uint32
}

// newSource builds a consumer and attaches its first channel.
func (b *Backend) newSource(ctx context.Context, cfg mq.SourceConfig, n rabbit.Names) (*source, error) {
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         cfg.Metadata,
		Capabilities: b.caps,
	})
	if err != nil {
		return nil, err
	}
	tokens, err := driverkit.NewTokenTable(cfg.Prefetch)
	if err != nil {
		return nil, err
	}
	adapter, err := driverkit.NewPushAdapter(driverkit.PushConfig{Capacity: cfg.Prefetch})
	if err != nil {
		return nil, err
	}
	strategy, err := b.delayStrategy()
	if err != nil {
		return nil, err
	}
	s := &source{
		b:        b,
		topic:    cfg.Topic,
		sub:      cfg.Subscription,
		names:    n,
		prefetch: cfg.Prefetch,
		env:      env,
		opts:     b.options(cfg.Metadata),
		delay:    strategy,
		adapter:  adapter,
		tokens:   tokens,
		tag:      b.cfg.ConsumerTagPrefix + "-" + strconv.FormatUint(b.nextSeq(), 36),
		parks:    make(map[uint64]*parkWait, 4),
		closed:   make(chan struct{}),
	}
	if err := s.declare(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// declare creates or asserts this subscription's topology.
//
// It is separate from [source.attach] and runs at OPEN, which is the half of
// the lazy attachment that is not lazy and must not be: a publication made
// between opening a consumer and its first Fetch has to reach the queue, and a
// queue that did not exist yet would have discarded it. Every conformance
// scenario opens its consumer first and publishes second, and so does every
// sensible caller.
//
// The declaration is on a channel of its own and the channel is closed again,
// for the reason [Backend.ensure] gives: a declaration the broker refuses is a
// channel exception, and a channel exception on the consume channel would take
// the consumer with it.
func (s *source) declare(ctx context.Context) error {
	decl, err := s.b.channel()
	if err != nil {
		return err
	}
	if err := s.b.ensure(ctx, decl, s.names, true); err != nil {
		_ = decl.Close()
		return err
	}
	if err := decl.Close(); err != nil {
		return s.b.errs.Map(mq.OpOpen, s.topic, s.sub, err)
	}
	return nil
}

// attach opens a channel, sets the prefetch, starts the consumer and starts
// the goroutine that moves deliveries into the ring.
//
// The order is the one that matters: `basic.qos` BEFORE `basic.consume`. The
// broker applies a prefetch to the consumers started after it, so a consume
// that ran first would be an unbounded one for its whole life — the ring would
// then be the only bound, and a broker pushing into a full ring is a broker
// whose own flow control never sees the slow consumer.
//
// It is called from the FIRST Fetch and not from the open, and the lateness is
// deliberate. RabbitMQ pushes: a `basic.consume` makes the broker start
// sending, and every message it sends counts against the prefetch and starts
// its acknowledgement deadline whether or not anybody has asked for a
// delivery. A source that consumed at open would therefore hold `prefetch`
// messages unacknowledged from the moment it existed — a lease ticking against
// a caller who has not called Fetch — and the SPI's Source is a PULL shape
// whose deliveries are asked for.
//
// Two properties fall out of it that no amount of care inside an
// eagerly-attached source recovers, because both are properties of the
// BROKER's queue and not of the client's buffer: a message expires by its own
// TTL while it is queued and not after it has been pushed, and a priority
// queue serves the most urgent message it HOLDS. Both were failing their
// conformance probes against a real broker before this, with the message
// already sitting in this driver's ring.
func (s *source) attach(ctx context.Context) error {
	s.attachMu.Lock()
	defer s.attachMu.Unlock()
	return s.attachLocked(ctx)
}

// reattach starts the consumer again after a reconnection, and does nothing at
// all for a source whose first Fetch has not happened yet or whose consumer
// the broker cancelled.
//
// The first exception is what lazy attachment costs the dial loop: a source
// that had never consumed must not start consuming because the connection was
// replaced, or a caller who opened a consumer and never read from it would
// find the broker pushing into it after an outage and not before one.
//
// The second is a source whose queue went away (see [source.onCancel]). Its
// ring is closed and every Fetch answers CodeNotFound, so a consumer started
// for it would take up to `prefetch` messages that nothing ever reads. And
// when the queue has stayed gone, the `basic.consume` meets a 404 that fails
// the whole dial attempt, every attempt, and one deleted subscription keeps
// the client from reconnecting at all. A cancel that lands while this attach
// is under way is the same source, so its refusal is not a dial failure
// either.
//
// A `basic.consume` the broker REFUSES is this source's failure too, and not
// the dial's. The refusal is a channel exception, so the connection it came
// on is sound, and the next attempt would meet the same refusal. With
// [Config.Declare] off, a queue deleted while the client was disconnected
// answers 404 on every attempt, because nothing declares it again. With
// Declare on, the topology replay that runs before this has declared it
// again, so the consume succeeds and the source reads the new, empty queue.
// A standby that took the exclusive consumer during the outage earns a 403 on
// every attempt either way. Returning the refusal used to keep the whole
// client from reconnecting, with every publisher refused for good, because of
// one consumer. See [source.consumeRefused] for what the source does instead.
// Only a failure that is not a channel exception, which is the connection's,
// goes back to the dial loop.
func (s *source) reattach(ctx context.Context) error {
	s.attachMu.Lock()
	defer s.attachMu.Unlock()
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started || s.cancelled.Load() {
		return nil
	}
	err := s.attachLocked(ctx)
	if err == nil || s.cancelled.Load() {
		return nil
	}
	if !IsChannelException(err) {
		return err
	}
	s.consumeRefused(err)
	return nil
}

// consumeRefused answers the broker refusing a `basic.consume` for this
// started source, on a reconnection or on the Fetch that asks again after
// one. attachMu is held, so no other route can install a channel while it
// runs and the source it marks is the one that was refused.
//
// A 404 is the queue deleted while this source had no channel, which is the
// news a broker cancel brings when it has one, and the answer is the same:
// the source is gone and every Fetch reports CodeNotFound. RabbitMQ also
// answers 404 for a classic queue whose node is down, during a rolling
// restart for example, and the source cannot tell that from a deletion. The
// broker cancels a connected consumer of such a queue as well, so this is
// the answer a connected consumer already gets, and the caller opens a new
// consumer once the node is back.
//
// Any other refusal leaves the source with no channel. Health reports it with
// the broker's reason, and [source.refused] makes each new Fetch ask the
// broker again and report the refusal, as a standby that has never started
// does, until a consume succeeds. The next reconnection asks again too. A
// Fetch that was already waiting goes on waiting, because nothing short of
// closing the ring can wake it.
func (s *source) consumeRefused(err error) {
	if code, _ := ReplyCode(err); code == amqp.NotFound {
		s.markGone()
		return
	}
	s.repairCause.Store(&repairFailure{advice: errConsumeRefused, cause: err})
	s.refused.Store(true)
}

// markGone is [source.onCancel]'s answer for a source whose queue was deleted
// while it had no channel. The ring is closed, which wakes a parked Fetch,
// and the tokens are invalidated. `cancelled` is set under `mu` for
// [source.attachLocked]'s install check.
func (s *source) markGone() {
	s.mu.Lock()
	s.cancelled.Store(true)
	s.mu.Unlock()
	s.invalidate()
	_ = s.adapter.Close()
}

// attachLocked is [source.attach]'s body. attachMu is held.
func (s *source) attachLocked(ctx context.Context) error {
	// The caller's context bounds the DECISION to attach and not the
	// consumer's life: a Fetch whose context has already expired must not
	// open a channel and start the broker sending, because nothing would then
	// read what arrives. `basic.consume` itself is given the BACKEND's
	// context, for the reason below.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.gone(); err != nil {
		return err
	}
	// A source with a channel on the CURRENT connection is attached, and
	// attaching it again would orphan that channel rather than replace it.
	// `attachMu` serialises the three routes that attach -- the first Fetch,
	// a channel exception's repair and the dial loop's re-attach -- but it
	// does not stop them running one after another, and the second of two
	// back-to-back attaches used to install a new channel over a live one
	// without closing it or cancelling its consumer. The broker then went on
	// filling the orphan's prefetch with messages no pump reads and nothing
	// redelivers until the connection dies, and it bumped the generation, so
	// the delivery the caller held from the live channel was refused as stale.
	//
	// The sequence that does it is a consumer whose FIRST Fetch -- the one
	// that starts it; a started source's Fetch attaches only while
	// `refused` is set and no reconnection is under way -- lands while
	// the dial loop is between installing the new connection and reaching
	// this source: the Fetch attaches on the new connection and sets
	// `started`, and the dial loop's `reattach` then attaches again. Every
	// double attach `FuzzRabbitTagSettlement` produced was that one, and its
	// seed 2fb2f184e2f948b5 saw it as a "stale" drop that succeeded: the
	// channel the target's fault closed was the orphan, so the delivery's own
	// channel had never been replaced.
	//
	// "Has a channel" is not the test, because a channel can be installed on
	// a connection that is gone. A dial attempt that fails part-way closes the
	// connection it has just made, and the next attempt runs without the
	// invalidation that clears `s.ch` before the first -- so a source it had
	// already attached arrives still holding a channel on the closed
	// connection. Skipping it left the repair to that channel's own close
	// notification and nothing else
	// (`TestCriticalARedialThatFailsPartwayReattachesEveryHandle`). Such a
	// channel is retired here exactly as a lost one is, and replaced. The
	// connection is compared rather than the channel's own closed flag
	// because every caller that can find a channel installed is the dial loop
	// on a NEW connection, so "on the current connection" is the whole
	// question; a channel that dies on the current connection is its pump's
	// to answer.
	cn := s.b.conn()
	s.mu.Lock()
	cur, curConn := s.ch, s.chConn
	s.mu.Unlock()
	if cur != nil {
		if curConn == cn {
			return nil
		}
		s.invalidate()
	}
	ch, err := s.b.channelOnWithin(ctx, cn)
	if err != nil {
		return err
	}
	if err := ch.Qos(s.prefetch, 0, false); err != nil {
		_ = ch.Close()
		return s.b.errs.Map(mq.OpOpen, s.topic, s.sub, err)
	}
	// Confirm mode on a CONSUME channel, for the one reason given on
	// [source.tr]: a delayed nack parks the message and then acknowledges
	// the original, and doing that without a confirm would lose the message
	// whenever the park did not land.
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return s.b.errs.Map(mq.OpOpen, s.topic, s.sub, err)
	}
	confirms := make(chan amqp.Confirmation, s.prefetch+1)
	ch.NotifyPublish(confirms)
	// A park is `mandatory`, so a delay queue that has gone (an operator
	// deleted it, or it expired between its declaration and the park)
	// answers with a `basic.return` and then a `basic.ack`.
	// Without this listener the library drops the return and the ack alone
	// reads as a park that landed. UNBUFFERED, as on [sink.attach]: that is
	// what keeps the return ahead of the confirm behind it.
	returns := make(chan amqp.Return)
	ch.NotifyReturn(returns)
	cancels := make(chan string, 1)
	ch.NotifyCancel(cancels)
	lost := make(chan *amqp.Error, 1)
	ch.NotifyClose(lost)
	// A SECOND close listener, for [source.claimSlot] alone. `NotifyClose`
	// registers listeners rather than replacing one -- amqp091 appends to
	// `Channel.closes` and broadcasts to all of them, and the fake does the
	// same -- so a slot wait can select on this one without consuming the
	// notification the pump repairs from. The alternative was to peek at
	// `lost` and put the value back, which cannot be done safely: the library
	// CLOSES that channel after broadcasting, and a send on it panics.
	dead := make(chan *amqp.Error, 1)
	ch.NotifyClose(dead)

	// `autoAck` is false: the explicit settlement is the whole point, and
	// an auto-acknowledged consumer would acknowledge a message before the
	// handler ran. `exclusive` is [Config.ExclusiveConsumer], off by
	// default because the competing consumers are what a subscription
	// means. `noLocal` is not implemented by RabbitMQ at all and is false.
	// The BACKEND's lifetime context and not the caller's, and it is
	// deliberate: `basic.consume`'s context bounds the consumer's whole life
	// rather than the call that started it, so passing the caller's would
	// cancel the consumer when whoever opened it returned. It is what
	// `Close` and the backend's own teardown cancel. contextcheck cannot
	// know that, hence the exemption.
	//nolint:contextcheck // the consumer outlives the call that opens it; the backend's context is what bounds it
	deliveries, err := ch.ConsumeWithContext(s.b.ctx, s.names.Queue, s.tag, false, s.b.cfg.ExclusiveConsumer, false, false, amqp.Table(s.b.cfg.ConsumeArgs))
	if err != nil {
		_ = ch.Close()
		return s.b.errs.Map(mq.OpFetch, s.topic, s.sub, err)
	}

	pctx, pcancel := context.WithCancel(s.b.ctx)
	st := newStore(s.prefetch)
	// The closed check, the install and the pump's REGISTRATION are one
	// critical section, for [sink.attach]'s reason and with the same
	// consequence if they are not: the check at the top of this function is
	// an early-out, the whole of [source.Close] can run between it and here,
	// and `s.wg.Add(1)` would then land while Close is inside `s.wg.Wait()`.
	// `sync` calls that a data race and may answer it with "WaitGroup misuse:
	// Add called concurrently with Wait", and the source would be left with a
	// consumer and a pump goroutine that nothing closes, because `b.forget`
	// has already dropped it.
	//
	// A broker cancel is checked here too, and for the same reason:
	// [source.onCancel] sets the flag under `mu`, so an attach that passed the
	// early check either installs first, and the cancel is then about a
	// channel this source has already replaced, or sees the flag and installs
	// nothing. Installing after the flag was set was the dial loop's zombie
	// consumer by another route.
	s.mu.Lock()
	if err := s.gone(); err != nil {
		s.mu.Unlock()
		pcancel()
		_ = ch.Close()
		return err
	}
	if s.pumpCancel != nil {
		s.pumpCancel()
	}
	s.ch, s.chConn = ch, cn
	s.tr = confirm.NewTracker(s.prefetch)
	s.pumpCancel = pcancel
	s.started = true
	s.stranded.Store(false)
	// A channel installed answers every earlier failure to get one, so
	// neither a refusal nor its reason can outlive it. A reason left behind
	// would be reported for this source after a later loss, in the window
	// before the repair or the dial loop takes over.
	s.refused.Store(false)
	s.repairCause.Store(nil)
	s.gen++
	gen := s.gen
	s.wg.Add(1)
	s.mu.Unlock()

	// The pump's context is derived from the BACKEND's and not from the
	// caller's, which is the whole point of [source.pumpCancel]: the pump
	// outlives the attach that started it and is stopped by the generation
	// changing, not by whoever asked for the re-attach returning.
	//nolint:contextcheck // the pump outlives the call that starts it; its own cancel is what bounds it
	go s.pump(pctx, gen, st, deliveries, confirms, returns, cancels, lost, dead)
	return nil
}

// pump moves deliveries from the library's channel into the ring, and reads
// this channel's other notifications.
//
// It is one goroutine per attached channel, and it exits when the channel goes
// or the source closes. The generation it was started with is what it stamps
// on every delivery, so a delivery that was in flight when the channel was
// replaced is recognisable afterwards.
//
// It is also the ONLY reader of the channel's close notification, and so the
// only thing that repairs a channel exception. Every exit is therefore one of
// two kinds: the pump has answered the close itself (`lost`, or a broker-side
// cancel), or somebody else has already claimed the channel -- its context is
// cancelled by `claimLost` and by a replacing attach -- or the source is
// closing. An exit of any other kind leaves the source holding a dead channel
// that nothing will ever replace.
func (s *source) pump(ctx context.Context, gen uint32, st *store, deliveries <-chan amqp.Delivery, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, cancels <-chan string, lost, dead <-chan *amqp.Error) {
	defer s.wg.Done()
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				// The library closes the delivery channel when the consumer
				// ends, which is either a cancel or the channel closing.
				// Both are reported through their own notifications, so this
				// branch only stops reading.
				//
				// It must not RETURN, even when the confirm and cancel
				// channels have closed as well. They all close together when
				// the channel shuts down, and the close error is still
				// waiting on `lost` -- which is the one notification that
				// repairs the source. It used to return here, and whenever
				// `select` happened to take the three closed channels before
				// `lost` the channel exception was never answered: the source
				// kept the dead channel with its generation unchanged, no
				// consumer on the broker, and a Fetch that waited for ever.
				// About one channel exception in a hundred and fifty went
				// that way against the fake. `lost` is
				// closed on every shutdown, so the loop always reaches it.
				deliveries = nil
				continue
			}
			if err := s.admit(ctx, gen, st, dead, &d); err != nil {
				// A failed admission is the end of this pump only when the
				// pump is finished anyway. `admit` also fails when its own
				// `basic.nack` of an unmappable delivery meets a channel that
				// has just closed, and returning then was the second way the
				// pump left without reading `lost`: a poison message arriving
				// as its channel took an exception stranded the source.
				if ctx.Err() != nil || s.isClosed() {
					return
				}
			}
		case c, ok := <-confirms:
			if !ok {
				confirms = nil
				continue
			}
			s.confirmTag(c.DeliveryTag, c.Ack)
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			s.onReturn(&r)
		case tag, ok := <-cancels:
			if !ok {
				cancels = nil
				continue
			}
			cancelled, ours := s.onCancel(gen, tag)
			if !ours {
				// A `basic.cancel` for a consumer that is not this source's
				// is not this pump's news, and returning on it was a fourth
				// exit that skipped `lost` -- the class, with
				// the source left holding a dead channel nothing repairs.
				// One channel carries one consumer here, so it takes a broker
				// that cancels by a tag it was not given; the pump does not
				// need to be right about that to be safe.
				continue
			}
			if cancelled != nil {
				retire(cancelled, confirms, returns)
			}
			return
		case aerr := <-lost:
			// NOT the pump's context. `onChannelLost` calls `claimLost`
			// first, and that cancels this very context -- so a repair given
			// it would start with it already done and fail every time. It
			// derives its own bound from the backend's.
			//nolint:contextcheck // claimLost cancels the pump's context, so the repair cannot be bounded by it
			s.onChannelLost(gen, reasonOf(aerr))
			return
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		}
	}
}

// admit turns one AMQP delivery into a [mq.DriverDelivery] and pushes it into
// the ring, blocking while the ring is full.
//
// The block is the backpressure: the library's delivery goroutine waits here,
// so the broker's own prefetch accounting sees a consumer that is not
// settling. It returns an error when the ring refuses the delivery -- the pump's
// context is done or the source is closing -- and when rejecting an unmappable
// delivery fails, which on this protocol means the channel has closed. The
// pump reads only the first as the end of its work (see [source.pump]).
func (s *source) admit(ctx context.Context, gen uint32, st *store, dead <-chan *amqp.Error, d *amqp.Delivery) error {
	s.mu.Lock()
	if s.ch == nil || s.gen != gen {
		// The channel was replaced while this delivery was in flight. The
		// broker has already taken the delivery back -- an unacknowledged
		// delivery on a closed channel is requeued -- so handing it to a
		// caller would hand out a delivery nobody can settle.
		s.mu.Unlock()
		s.stale.Add(1)
		return nil
	}
	ch := s.ch
	s.mu.Unlock()

	if s.expiredOnReturn(d) {
		// The broker would have expired it had the dead-letter hop left its
		// `expiration` alone, so it goes where an expired message goes: onto
		// the queue's dead-letter route, or nowhere when there is none.
		return ch.Nack(d.DeliveryTag, false, false)
	}
	token, ok := s.claimSlot(ctx, gen, dead, &deliveryRef{ch: ch, tag: d.DeliveryTag, gen: gen})
	if !ok {
		// The wait for a slot ended because the channel died or was
		// replaced, or the source is closing, so the broker has taken the
		// delivery back (or will, when the channel closes) and nothing is
		// handed out. Only the first two are a STALE delivery; a closing
		// source is not replacing anything.
		if !s.isClosed() {
			s.stale.Add(1)
		}
		return nil
	}
	slot := uint32(token)
	msg, fields := &st.msgs[slot], &st.flds[slot]
	info, err := decode(s.b.proto, msg, fields, d, s.opts, s.sub, s.b.fx.quorum, s.b.clock.Now())
	if err != nil {
		// A delivery this driver cannot map is a poison message: it is in
		// the queue and nothing can read it. It is rejected onto the
		// dead-letter route when there is one and moved to the back of the
		// queue when there is not (see rejectUnmappable), and the slot is
		// released either way.
		_ = s.tokens.Release(token)
		return s.rejectUnmappable(ch, d, err)
	}
	if s.env.Mode() != mq.MetadataNativeStrict {
		if uerr := s.env.Unwrap(msg, msg); uerr != nil {
			_ = s.tokens.Release(token)
			return s.rejectUnmappable(ch, d, uerr)
		}
	}
	dd := mq.DriverDelivery{
		Message: msg,
		Info:    info,
		Token:   token,
		Ref:     nil,
		Native:  d,
		Gen:     gen,
	}
	if err := s.adapter.Push(ctx, dd); err != nil {
		// The ring is closed, which means the source is closing. The
		// delivery is left unsettled so that the broker redelivers it,
		// which is what the specification asks a closing source to do with what it never
		// handed out.
		_ = s.tokens.Release(token)
		return err
	}
	return nil
}

// expiredOnReturn reports whether d is a copy back from this subscription's
// delay queue that has waited in the queue for longer than its own TTL since
// it came back.
//
// The broker cannot tell: it removed the copy's `expiration` on the
// dead-letter hop out of the delay queue, so the park carried the TTL and the
// moment it starts to run instead (see [parkExpiration]). Only a copy whose
// latest hop was out of one of this queue's delay queues counts
// ([carriedTTL]). Dead-lettered from there, whether it stays a dead letter or
// is moved back to this queue later, it is a dead letter, and RabbitMQ does not
// expire those.
//
// The two clocks are the parking client's and this one's, so a skew between
// them moves the expiry by as much.
func (s *source) expiredOnReturn(d *amqp.Delivery) bool {
	ttl, due, ok := carriedTTL(d, s.names.Queue)
	if !ok || due > math.MaxInt64-ttl {
		return false
	}
	return s.b.clock.Now().UnixMilli() > due+ttl
}

// claimSlot takes a token for one delivery, WAITING for a slot rather than
// leaving the delivery unsettled when every slot is in use.
//
// The table is prefetch-sized and the broker never has more than the prefetch
// unacknowledged at this consumer, so a full table used to be read as "the
// broker sent more than it said it would" and the delivery was left unsettled
// and counted. It is not that. A settlement WRITES its frame and only then
// frees its slot (`settler.settle`: the act, then `Complete`), and the broker
// answers the frame by sending the next delivery at once -- so the pump can
// meet that delivery in the window between the two, with the table still
// full. Left unsettled on a LIVE channel, the delivery is not redelivered:
// RabbitMQ gives an unacknowledged delivery back only when its channel closes,
// so it sat in the consumer's prefetch, never handed to anybody, for as long
// as the channel lived, and the prefetch was one smaller for all of that time.
// `drivers/activemq/stomp` met the same drop and answered
// it the same way.
//
// The wait is short on a live channel: the slot it waits for belongs to a
// settlement that has already written its frame, and that settlement frees the
// slot without needing this goroutine -- `basic.ack` and `basic.nack` are
// written and not answered. It ends early, and reports false, in three cases,
// because in each the delivery is the broker's again:
//
//   - The channel DIES. This is the one that nothing else would notice: the
//     pump is the only reader of the channel's close notification and it is
//     waiting here, so a wait that asked only whether the channel had been
//     REPLACED would wait for the replacement that its own caller is the one
//     to start -- until a caller happened to settle something, and for ever
//     when the caller was waiting for this very delivery before settling.
//     `TestCriticalAChannelExceptionDuringASlotWaitIsAnswered`.
//   - The channel is replaced, or the source closes. Both also invalidate every
//     token, which frees a slot, and a close also closes the channel, which
//     the first exit sees; so these exits are one of several ways the wait
//     ends, and `TestCriticalAConnectionLossDuringASlotWaitRecovers` and
//     `TestCriticalACloseDuringASlotWaitReturns` fail only when every one of
//     them is gone.
//
// It does not poll. It used to, at two hundred microseconds, because
// `driverkit.TokenTable` had no release signal; it has one now
// ([driverkit.TokenTable.ClaimOrWait]), and the
// three exits above
// are three more things that end the wait:
//
//   - `dead` is a SECOND `NotifyClose` listener on this attachment's channel,
//     registered beside the pump's own (see [source.attachLocked]). Reading it
//     here does not consume the pump's, so the close is still answered by the
//     pump afterwards -- and it is the edge the poll's `ref.ch.IsClosed()`
//     check stood in for. Without it, nothing wakes this wait when the channel
//     dies: the pump is the only reader of that notification and it is the
//     goroutine parked here.
//   - `ctx` is the PUMP's context, which `source.claimLost` cancels before it
//     drains -- so a replaced channel and a closing source both end the wait
//     through it.
//   - `s.closed` is the same answer from the other direction, for a Close that
//     has not reached `claimLost` yet.
//
// There is no ordering here to get wrong, which is deliberate. The first
// version read the release broadcast and then retried the claim, and the gap
// between the two was a lost wakeup waiting to happen -- a release landing in
// it closed a channel this goroutine did not hold yet and handed it a fresh
// one nothing would close, so the delivery waited for ever rather than for
// 200 microseconds. Review round 1 found that nothing pinned that ordering
// (rigour-2). `ClaimOrWait` takes the claim and starts the wait under the
// table's one lock, so the window does not exist to be pinned.
//
// The table's wait takes one context, so `dead` and `s.closed` reach it as a
// cancellation: [source.slotWaitContext] spends one goroutine for the length
// of a slot wait -- never per delivery, and only on a full table -- and
// claimSlot waits for it before returning.
func (s *source) claimSlot(ctx context.Context, gen uint32, dead <-chan *amqp.Error, ref *deliveryRef) (uint64, bool) {
	if token, ok := s.tokens.Claim(ref); ok {
		return token, true
	}
	s.slotWaits.Add(1)
	wctx, stop := s.slotWaitContext(ctx, dead)
	defer stop()
	for {
		s.mu.Lock()
		current := s.ch != nil && s.gen == gen
		s.mu.Unlock()
		if !current || ref.ch.IsClosed() {
			return 0, false
		}
		token, ok, err := s.tokens.ClaimOrWait(wctx, ref)
		if ok {
			return token, true
		}
		if err != nil {
			return 0, false
		}
	}
}

// slotWaitContext is ctx, ended as well by a closing source or a dead
// channel. The returned stop ends the goroutine that watches those two and
// waits for it, so the goroutine never outlives the slot wait it serves.
func (s *source) slotWaitContext(ctx context.Context, dead <-chan *amqp.Error) (context.Context, func()) {
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		select {
		case <-wctx.Done():
		case <-s.closed:
		case <-dead: //nolint:errcheck // a signal only: dead is the slot wait's own close listener, and the pump repairs from lost
		}
	}()
	return wctx, func() {
		cancel()
		<-done
	}
}

// rejectUnmappable answers a delivery this driver cannot turn into a message.
//
// It is one of the two places the driver settles a delivery the caller never
// saw, and the reason is that the alternative is worse: a message the mapping
// refuses is unreadable for ever, so leaving it unsettled redelivers it for
// ever and blocks nothing else only because this broker has no head-of-line
// ordering guarantee to break. The dead-letter route is where an
// unprocessable message belongs, and the driver page lists this among the
// settlements the driver makes on its own. The other is a delayed redelivery
// whose own TTL ran out after it came back ([source.expiredOnReturn]), which
// the broker would have expired itself.
//
// Without a dead-letter route the message is not destroyed -- that is what
// `basic.ack` would do, and silently dropping a message is worse than a queue
// that visibly does not drain -- and it is not left unsettled either, which
// was this driver's first answer: every unsettled delivery holds one of the
// channel's `basic.qos` slots, so `prefetch` poison messages stopped the
// consumer for good, and a reconnection redelivered them and stopped it
// again. A `basic.nack` with requeue is no better on a classic queue, which
// puts the message back at the HEAD: once `prefetch` of them are queued the
// consumer receives nothing else, as fast as the broker can send. So the
// message is parked at the queue's TAIL ([source.parkUnmappable]) and the
// messages behind it are delivered.
func (s *source) rejectUnmappable(ch client.Channel, d *amqp.Delivery, cause error) error {
	_ = cause
	if s.b.cfg.DeadLetter {
		return ch.Nack(d.DeliveryTag, false, false)
	}
	// The park waits for a confirm, and confirms reach it through this
	// pump, so it cannot run here. The closed check and the registration are
	// one critical section for [source.attachLocked]'s reason: a `wg.Add`
	// may not race Close's `wg.Wait`.
	s.mu.Lock()
	if s.isClosed() {
		// Close returns what the channel holds to the broker.
		s.mu.Unlock()
		return nil
	}
	s.wg.Add(1)
	s.mu.Unlock()
	native := *d
	go func() {
		defer s.wg.Done()
		s.parkUnmappable(ch, &native)
	}()
	return nil
}

// parkUnmappable moves a delivery this driver cannot map to the tail of its
// own queue: a re-publication with no delay, its confirm, and then the ack of
// the original -- [settler.requeueLater]'s order, for its reason. A park that
// fails leaves the original unsettled, and the broker takes it back with the
// channel; a crash between the confirm and the ack leaves both copies, which
// is the duplicate at-least-once already allows.
//
// The copy is a new queue entry, so a quorum queue's delivery limit does not
// see it come round: it circulates until a dead-letter route or an operator
// takes it, which is what [Config.DeadLetter] is for.
func (s *source) parkUnmappable(ch client.Channel, d *amqp.Delivery) {
	plan, err := s.delay.Plan(delay.Request{Target: delay.RedeliveryTarget(s.names.Queue)})
	if err != nil {
		return
	}
	if err := (*settler)(s).park(s.b.ctx, ch, d, &plan, 0); err != nil {
		return
	}
	_ = ch.Ack(d.DeliveryTag, false)
}

// parkWait is one delayed-nack re-publication waiting for its confirm.
//
// `done` is buffered and sent to rather than closed, so that a settlement
// whose context expired leaves nothing for the pump to block on.
type parkWait struct {
	done   chan struct{}
	result confirm.Result
	// returned records that the park came back through `basic.return`: the
	// delay queue it was routed to is not there.
	returned bool
}

// confirmTag resolves a delayed nack's re-publication and wakes whatever is
// waiting for it.
func (s *source) confirmTag(tag uint64, ack bool) {
	s.mu.Lock()
	if s.tr == nil {
		s.mu.Unlock()
		return
	}
	var out []confirm.Outcome
	if ack {
		out = s.tr.Ack(tag, false)
	} else {
		out = s.tr.Nack(tag, false, false)
	}
	woken := s.wakeParksLocked(out)
	s.mu.Unlock()
	for _, w := range woken {
		select {
		case w.done <- struct{}{}:
		default:
		}
	}
}

// onReturn records a `basic.return` for a park, which the `basic.ack` behind
// it then resolves as Rejected. A park carries a correlation token because
// several settlements may each have one outstanding; a return with no token is
// not a park's and is ignored.
func (s *source) onReturn(r *amqp.Return) {
	token := tokenOfReturn(r)
	if token == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tr != nil {
		s.tr.Returned(token)
	}
}

// wakeParksLocked pairs each resolved outcome with its waiter and removes it
// from the map. The waiters are woken outside the lock by the caller.
func (s *source) wakeParksLocked(out []confirm.Outcome) []*parkWait {
	if len(out) == 0 {
		return nil
	}
	woken := make([]*parkWait, 0, len(out))
	for i := range out {
		w := s.parks[out[i].Tag]
		if w == nil {
			continue
		}
		delete(s.parks, out[i].Tag)
		w.result, w.returned = out[i].Result, out[i].Returned
		woken = append(woken, w)
	}
	return woken
}

// onCancel records that the BROKER cancelled this consumer.
//
// It happens when the queue this consumer was reading is deleted, and it is
// not a close: the caller closed nothing, and the queue is gone. So the ring
// is closed — which wakes a parked Fetch — and the tokens are invalidated,
// because an unacknowledged delivery from a deleted queue can never be
// settled. Fetch then reports CodeNotFound rather than ErrClosed, which is the
// difference between "you closed this" and "the thing you were reading is
// gone".
//
// It reports whether the cancel was THIS source's, which is what the pump
// reads: a cancel that is not ours ends nothing, and a pump that returned on
// one would leave without answering its channel's close notification. It also
// returns the cancelled channel when the source still held it, for the pump
// to [retire]: the source gives it up here, so nothing else would close it.
//
// gen is the generation of the pump that read the cancel. A cancel for a
// channel the dial loop has since REPLACED ends that pump and nothing else:
// the replacement subscribed again, which a queue that is gone would have
// refused, so the consumer the source has now is not the one the broker
// cancelled. A cancel for a channel that was lost and not yet replaced is
// the source's, and is what keeps the dial loop from subscribing it again
// (see [source.reattach]). `cancelled` is set under `mu` for
// [source.attachLocked]'s install check.
func (s *source) onCancel(gen uint32, tag string) (client.Channel, bool) {
	if tag != "" && tag != s.tag {
		return nil, false
	}
	s.mu.Lock()
	if s.ch != nil && s.gen != gen {
		s.mu.Unlock()
		return nil, true
	}
	s.cancelled.Store(true)
	ch := s.ch
	s.mu.Unlock()
	s.invalidate()
	_ = s.adapter.Close()
	return ch, true
}

// retire closes the channel of a consumer the broker cancelled.
//
// The close runs on a goroutine of its own while this one goes on reading the
// channel's confirms and returns, because the library hands both over from
// the connection's reader goroutine with a blocking send, and `returns` is
// unbuffered on purpose (see [source.attachLocked]). A park's `basic.return`
// that the broker sent before its `channel.close-ok` would otherwise hold
// that reader with nobody to take it, and the close, and every other channel
// on the connection, would wait behind it. What arrives is dropped: the
// cancel has already resolved every park the channel was holding.
func retire(ch client.Channel, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return) {
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		_ = ch.Close()
	}()
	for {
		select {
		case <-closed:
			return
		case _, ok := <-confirms:
			if !ok {
				confirms = nil
			}
		case _, ok := <-returns:
			if !ok {
				returns = nil
			}
		}
	}
}

// onChannelLost answers this channel going away without the whole connection
// going with it, which is what a channel exception is.
//
// The backend's supervision loop handles a lost CONNECTION; a channel
// exception is narrower and the source repairs it itself by asking the
// backend's dial loop for nothing at all: the connection is still there, so a
// new channel is one round trip away. Everything the old channel had handed
// out is invalidated first, for the reason on [source.gen].
func (s *source) onChannelLost(gen uint32, cause error) {
	if !s.claimLost(gen) {
		// The backend's supervision loop got there first, and it owns the
		// repair. Two repairs would attach two consumers to one
		// subscription.
		//
		// The two reasons `claimLost` refuses are counted apart, so that the
		// counters mean the same thing on a source as on a sink: a generation
		// that has MOVED is the reordering `staleCloses` is about, and a channel somebody has already given up is a DECLINE.
		// Counting both as declines made `declinedCloses` useless to a test
		// with a publisher and a consumer in it (review round 1,
		// correctness-declinedcloses-conflated).
		if _, cur := s.current(); cur != gen {
			s.b.staleCloses.Add(1)
		} else {
			s.b.declinedCloses.Add(1)
		}
		return
	}
	s.discard()
	if s.isClosed() || s.b.ctx.Err() != nil {
		return
	}
	if s.b.conn() == nil {
		// The connection has gone and the dial loop owns the repair. See
		// [sink.onChannelLost].
		return
	}
	s.b.channelRepairs.Add(1)
	s.repairing.Store(true)
	defer s.repairing.Store(false)
	// One attempt, and no loop: a channel exception that repeats is a
	// topology problem the caller has to fix, and a driver that re-attached
	// for ever would hide it behind an empty Fetch. A failure here leaves the
	// source with no channel and no consumer on the broker, which Health
	// reports -- a Fetch parks instead, because the specification makes an empty ring a wait
	// rather than an error, and the comment here used to claim otherwise.
	//
	// The failure is recorded on the SOURCE and not reported into the
	// backend's health state machine. That machine belongs to the connection
	// and nothing clears it while the connection stays up, so this line left
	// the whole client Degraded for ever after one transient channel failure
	// -- including after the caller closed the broken consumer and opened a
	// working one. It has been here since review round 1
	// found it through the publisher route that copied it
	// (correctness-health-stuck-degraded). `Backend.Health` reads the record
	// instead, and that answer clears itself.
	//
	// ErrClosed is not recorded: the source was closed between the claim and
	// the attach, and a caller who closed one consumer must not be shown a
	// Degraded backend for it.
	ctx, cancel := s.b.waitDeadline(s.b.ctx)
	defer cancel()
	if err := s.attach(ctx); err != nil && !errors.Is(err, mq.ErrClosed) {
		s.repairCause.Store(&repairFailure{advice: errConsumerRepairFailed, cause: err})
	}
	_ = cause
}

// invalidate bumps the token generation, discards the deliveries the ring was
// holding and detaches the channel.
//
// The two halves are both necessary and neither is enough. Bumping the
// generation makes every settlement for a delivery handed out before now
// report ErrLeaseLost, which is the rule; draining the ring stops a
// delivery the broker has already taken back from being handed to a caller in
// the first place, which is the better of the two outcomes because the caller
// never sees a delivery it cannot settle.
func (s *source) invalidate() {
	s.claimLost(0)
	s.discard()
}

// claimLost marks the current channel gone and reports whether THIS caller
// made the transition.
//
// It is the atomic half of the repair, and both halves of "atomic" matter. The
// generation check and the bump happen under one lock, so two callers that
// notice the same broken channel cannot both decide to repair it; and the
// generation is bumped HERE rather than in `attach`, so a delivery handed out
// before now is stale from this moment rather than from whenever the
// replacement channel opens -- a window in which a settlement would otherwise
// be sent to a channel the broker has already closed.
//
// A gen of zero means "whatever is current", which is what the backend's
// supervision loop asks for: it knows the connection has gone and does not
// care which generation was on it.
func (s *source) claimLost(gen uint32) bool {
	s.mu.Lock()
	if gen != 0 && (s.gen != gen || s.ch == nil) {
		s.mu.Unlock()
		return false
	}
	if s.pumpCancel != nil {
		// The pump's context is cancelled BEFORE the drain in `discard`,
		// which is the order that closes the window on [source.pumpCancel]:
		// a push that has already completed is removed by the drain, and one
		// that is still blocked returns rather than inserting.
		s.pumpCancel()
		s.pumpCancel = nil
	}
	var woken []*parkWait
	if s.tr != nil {
		woken = s.wakeParksLocked(s.tr.Detach())
	}
	s.ch, s.chConn, s.tr = nil, nil, nil
	// A source that had never started is not stranded by an invalidation: it
	// had no channel to lose.
	s.stranded.Store(s.started)
	s.gen++
	s.mu.Unlock()
	for _, w := range woken {
		select {
		case w.done <- struct{}{}:
		default:
		}
	}
	return true
}

// discard invalidates the tokens and throws away what the ring was holding.
//
// It is separate from [source.claimLost] because it must run OUTSIDE the
// source's lock: the drain hands each delivery to a callback, and the token
// table takes a lock of its own.
func (s *source) discard() {
	s.tokens.InvalidateAll()
	s.adapter.Drain(nil)
}

// Fetch blocks until at least one delivery is available, ctx is done or the
// source is closed, and fills dst(n].
//
// The FIRST call is what starts the broker sending. See [source.attach] for
// why the `basic.consume` is here rather than at open; the cost is one round
// trip on the first Fetch, and the caller was going to wait for a delivery
// anyway.
func (s *source) Fetch(ctx context.Context, dst []mq.DriverDelivery) (int, error) {
	for {
		if err := s.ensureAttached(ctx); err != nil {
			return 0, err
		}
		n, err := s.adapter.Fetch(ctx, dst)
		if err != nil {
			if s.cancelled.Load() {
				// The broker cancelled the consumer, which means the queue
				// went away. Reporting ErrClosed would tell the caller they
				// closed something they did not.
				return 0, s.b.errs.Map(mq.OpFetch, s.topic, s.sub, errQueueGone)
			}
			return 0, err
		}
		if kept := s.keepCurrent(dst[:n]); kept > 0 {
			return kept, nil
		}
		// Everything the ring held came from a channel that has since been
		// replaced, so there is nothing to return and nothing has gone
		// wrong. Waiting again is right: the broker has taken those
		// deliveries back and will send them on the new channel, and a
		// Fetch that returned (0, nil) would make its caller spin.
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
	}
}

// keepCurrent drops the deliveries whose channel has been replaced, compacts
// the survivors to the front of got and returns how many there are.
//
// It is the LAST guard before a delivery leaves this driver, and it is here
// rather than only in [source.admit] because a check at admission cannot hold.
// `admit` reads the generation, then claims a token, decodes the delivery,
// unwraps its envelope and only then pushes into the ring, and the channel can
// be replaced anywhere in between. `source.claimLost` cannot stop a push that
// is already past the check: it cancels the pump and `source.discard` drains
// the ring, and the insert lands in the drained ring afterwards. That is this
// repository's recurring defect class -- a record placed where a drain has
// already walked -- and the established answer is to re-read the flags after
// the placement. Reading them HERE reads them after every placement at once.
//
// `driverkit.PushAdapter.Push` now refuses an insert once its context is
// cancelled, which is the same guard one layer down and does NOT make this one
// unnecessary: the pump can be descheduled between that check and the insert,
// and `Push` cannot know what a stale generation is anyway. It removes the two
// cases -- a push with room, and a push woken by the drain's own broadcast --
// that this guard would otherwise be the only answer to.
//
// A dropped delivery is not settled and not requeued, which is
// [source.admit]'s reasoning for its own stale branch: the broker has already
// taken back an unacknowledged delivery whose channel is gone, so the only
// thing to do is not hand it to a caller. The token is released so that its
// slot and its decode buffer come back, and a settlement carrying it reports
// ErrLeaseLost rather than landing on whatever takes the slot next (
// The specification). Handing one out was observable: `FuzzRabbitTagSettlement`'s seed
// `52404cfc9f059630` fetched a delivery from a dead channel, then fetched the
// broker's redelivery of the same message with nothing in between that could
// have invalidated the first.
func (s *source) keepCurrent(got []mq.DriverDelivery) int {
	s.mu.Lock()
	gen, attached := s.gen, s.ch != nil
	s.mu.Unlock()

	kept := 0
	for i := range got {
		if attached && got[i].Gen == gen {
			if kept != i {
				got[kept] = got[i]
			}
			kept++
			continue
		}
		s.stale.Add(1)
		_ = s.tokens.Release(got[i].Token)
	}
	return kept
}

// ensureAttached starts the consumer if nothing has yet, and asks the broker
// again for one whose consume it refused on a reconnection (see
// [source.refused]).
//
// It is cheap on the steady path: one mutex and two booleans. The attach
// itself takes `attachMu`, which already serialises the two repair routes, so
// two concurrent first Fetches cannot start two consumers.
func (s *source) ensureAttached(ctx context.Context) error {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started && !s.askAgain() {
		return nil
	}
	// The second check is under `attachMu`, which is what makes two
	// concurrent first Fetches start ONE consumer: the fast path above is an
	// optimisation and this is the decision.
	s.attachMu.Lock()
	defer s.attachMu.Unlock()
	s.mu.Lock()
	started = s.started
	s.mu.Unlock()
	if started && !s.askAgain() {
		return nil
	}
	if s.cancelled.Load() {
		return s.b.errs.Map(mq.OpFetch, s.topic, s.sub, errQueueGone)
	}
	err := s.attachLocked(ctx)
	if err != nil && started && IsChannelException(err) {
		// The same refusal again, or a different one, and it gets the
		// reconnection's answer: a 404 means the queue has gone since.
		s.consumeRefused(err)
		if s.cancelled.Load() {
			return s.b.errs.Map(mq.OpFetch, s.topic, s.sub, errQueueGone)
		}
	}
	return err
}

// askAgain reports that a Fetch on this started source should try the
// `basic.consume` the broker refused on a reconnection. During an outage it
// does not: the dial loop re-attaches the source when the connection is back,
// and until then a Fetch waits, as it does on any started source with no
// channel, rather than failing because there is no connection.
func (s *source) askAgain() bool {
	return s.refused.Load() && !s.b.reconnectPending()
}

// Settler settles this source's deliveries. There is one per source
func (s *source) Settler() mq.Settler { return (*settler)(s) }

// Close releases the source, requeues what it handed out unsettled and
// unblocks a parked Fetch.
//
// The requeue is `basic.nack{requeue=true}` for everything still in the ring,
// which is the specification's "requeues or abandons everything it has handed out that was
// never settled". It is done BEFORE the channel closes, because a channel
// close would requeue them anyway but only after the broker notices — an
// explicit nack is the same outcome a round trip sooner.
//
// `s.closed` is closed BEFORE the first `s.mu` section and `s.wg` is waited on
// after the last one, which is the interlock with [source.attachLocked]: an
// attach either completes before this close reads `s.ch` -- so the channel it
// installed is the one cancelled, drained and closed here -- or it sees the
// source closed and installs nothing. Do not rearrange
// those three points.
func (s *source) Close() error {
	var first error
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		ch := s.ch
		s.mu.Unlock()

		if ch != nil {
			// Cancelling the consumer first stops the broker sending more,
			// so the requeue below is a bounded amount of work rather than
			// a race with the broker's own delivery.
			if err := ch.Cancel(s.tag, false); err != nil && !isBenignClose(err) {
				first = s.b.errs.Map(mq.OpClose, s.topic, s.sub, err)
			}
		}
		s.adapter.Drain(func(d *mq.DriverDelivery) {
			if ch == nil {
				return
			}
			if ref, err := s.tokens.Resolve(d.Token); err == nil {
				if r, ok := ref.(*deliveryRef); ok {
					_ = r.ch.Nack(r.tag, false, true)
				}
			}
			_ = s.tokens.Release(d.Token)
		})
		_ = s.adapter.Close()
		s.tokens.InvalidateAll()

		if ch != nil {
			if err := ch.Close(); err != nil && !isBenignClose(err) && first == nil {
				first = s.b.errs.Map(mq.OpClose, s.topic, s.sub, err)
			}
		}
		s.mu.Lock()
		if s.pumpCancel != nil {
			s.pumpCancel()
			s.pumpCancel = nil
		}
		s.ch, s.chConn, s.tr = nil, nil, nil
		s.mu.Unlock()
		s.wg.Wait()
		s.b.forget(nil, s)
	})
	return first
}

// detached reports whether this consumer has started and has no channel, which
// is a consumer whose Fetch will never return a delivery.
//
// A source that has never fetched has no channel ON PURPOSE (see
// [source.attach]), and one the BROKER cancelled has none either and is not
// coming back: Fetch reports that as CodeNotFound, which is a better answer
// than a Degraded backend. See [Backend.detachedHandle].
func (s *source) detached() bool {
	return !s.isClosed() && !s.cancelled.Load() && s.stranded.Load()
}

// detachedReason is what [Backend.Health] reports for this consumer, or nil
// when the consumer is fine, has never fetched, was cancelled by the broker or
// is on its way out. See [sink.detachedReason].
func (s *source) detachedReason() error {
	if !s.detached() {
		return nil
	}
	if s.repairing.Load() || s.b.reconnectPending() {
		return errHandleDetached
	}
	if f := s.repairCause.Load(); f != nil {
		return f
	}
	return errHandleDetached
}

// gone is why this source can no longer attach, or nil when it can: the caller
// closed it, or the broker cancelled its consumer.
func (s *source) gone() error {
	if s.isClosed() {
		return mq.ErrClosed
	}
	if s.cancelled.Load() {
		return s.b.errs.Map(mq.OpFetch, s.topic, s.sub, errQueueGone)
	}
	return nil
}

// isClosed reports whether Close has run.
func (s *source) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// Stale is how many deliveries this source dropped rather than hand out,
// because their channel had been replaced. See [source.stale].
func (s *source) Stale() uint64 { return s.stale.Load() }

// current is the channel and generation a settlement has to match.
func (s *source) current() (client.Channel, uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ch, s.gen
}

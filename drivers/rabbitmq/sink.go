package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/protocol"
)

// The surfaces a sink offers. The absences are listed on [Backend].
var (
	_ mq.Sink      = (*sink)(nil)
	_ mq.AsyncSink = (*sink)(nil)
	_ mq.FlushSink = (*sink)(nil)
	_ mq.OwnedSink = (*sink)(nil)
)

// sink is one publisher: one AMQP channel, one confirm-tag sequence, one
// confirm strategy.
//
// The locks are the whole design. `pubMu` is held across THREE things that
// have to happen together — taking the next delivery tag, encoding the
// publication, and writing the frame — because the broker numbers
// publications in the order their frames reach it. A sink that took a tag,
// released the lock and then wrote would publish tag 8 before tag 7 and
// mis-attribute both confirms, which is a silent cross-message swap rather
// than a crash. It is NOT held across the wait for the confirm: that would
// make every strategy per-message.
//
// `mu` guards the bookkeeping, and it is released BEFORE the write. Under a
// `connection.blocked` alarm the broker stops reading, so the write can block
// for as long as the alarm lasts, and the loop that applies confirms and
// returns needs `mu`. amqp091 abandons a notification it cannot hand over
// within five seconds, so a loop parked behind the write lost a `basic.return`
// and reported the returned publication Confirmed when its ack arrived. The
// loop never takes `pubMu`, which is what keeps it reading.
type sink struct {
	b     *Backend
	topic string
	names rabbit.Names
	mode  mq.ConfirmationMode
	// strategy is the choice for this publisher.
	strategy confirm.Strategy
	// confirming reports whether the channel is in confirm mode at all. It
	// is false only for ConfirmNone, whose boundary is the socket write.
	confirming bool
	mandatory  bool
	// replyTo reports that this sink publishes to a direct reply-to address
	// rather than to a topic's exchange. See [IsReplyAddress].
	replyTo  bool
	receipts bool
	env      *driverkit.Envelope
	opts     protocol.Options

	// attachMu serialises [sink.attach], which has two callers that can run
	// back to back on one sink: the open that creates it and the dial loop's
	// re-attach. It is a second lock and not `mu` because an attach performs
	// I/O -- declarations, a channel open, `confirm.select` -- and holding
	// `mu` across that would block every publication for the length of a
	// round trip.
	attachMu sync.Mutex
	// pubMu keeps publish order: [sink.admit] holds it from taking the tag to
	// the end of the write, and it is what `buf` is guarded by. Nothing that
	// reads the channel's notifications takes it. Taken before `mu`, never
	// while holding it.
	pubMu sync.Mutex

	mu sync.Mutex
	// ch is the current channel, or nil while a reconnection is in flight.
	ch client.Channel
	// chConn is the connection ch was opened on, which is how an attach tells
	// a live channel on the current connection from one a failed dial attempt
	// left on a closed one. See [sink.attach].
	chConn client.Conn
	// tr is the current channel's confirm bookkeeping. It is replaced with
	// the channel, never reused across one: delivery tags restart at 1.
	tr *confirm.Tracker
	// confirms is the current channel's confirm notifications, which its loop
	// reads. [sink.detachLocked] applies what is already waiting in it before
	// it resolves the rest as ambiguous: see [sink.takeBufferedLocked]. It is
	// replaced and cleared with tr.
	confirms <-chan amqp.Confirmation
	// gen names the installed channel. Every attach bumps it, and the loop
	// that reads a channel's notifications carries the generation it was
	// started for, so a close notification that arrives after the dial loop
	// has already re-attached this sink is recognised as stale.
	//
	// It exists because a lost connection detaches every sink TWICE and
	// nothing orders the two: once from the supervision loop, which is
	// `Backend.repair` -> `Reconnector.Lost` -> `Backend.detachChildren`
	// before it redials, and once from this sink's own loop reading its
	// channel's close notification. Without the guard, a loop whose
	// notification lands after the reconnection has re-attached sets
	// `s.ch, s.tr = nil, nil` on the NEW channel -- and the publisher is
	// then dead for ever: every Publish afterwards is refused with
	// CodeUnavailable and nothing ever re-attaches it, because the
	// reconnection that would have has already happened.
	//
	// It is not a rare window. It failed as
	// `TestCriticalReconnectRestoresTheTopology` on GitHub's runner and
	// reproduces about once in forty locally under `GOMAXPROCS=2` with
	// coverage instrumentation, which is enough to make `make ci` red on a
	// two-core machine. [source.claimLost] is the same guard on the
	// consuming side, where `FuzzRabbitTagSettlement` found the same shape
	// first.
	gen uint64
	// lostGen is the newest generation whose channel somebody has already
	// taken responsibility for repairing, and it is what keeps the repair
	// route of [sink.onChannelLost] from racing the dial loop.
	//
	// ONE route owns a repair and says so here: `Backend.detachChildren`,
	// through [sink.detach], before the dial loop redials. It does not move
	// the generation, so the loop's own close notification -- which arrives
	// afterwards, and ordinarily does on every lost connection -- still names
	// the installed generation and would otherwise read it as an unanswered
	// channel exception.
	//
	// [sink.Close] deliberately does NOT claim, although it did until review
	// round 1 asked what fails without it: nothing does, because `s.closed` is
	// closed before Close takes `mu` and [sink.onChannelLost] reads it after
	// the claim, so a loop that claims during a Close finds the sink closed
	// and repairs nothing either way. A second guard that cannot fail is a
	// guard nobody can test, and the comment here used to present it as
	// load-bearing (round 1, rigour-5).
	//
	// A detach that does NOT claim is [sink.publishFailedLocked]'s: a
	// publication that met a channel the broker had already closed resolves
	// what that channel held, and the repair is still the loop's, because the
	// close notification for that very channel is on its way. Claiming there
	// was this guard's first shape and it left the publisher dead exactly as
	// before -- the detach happened, the notification arrived, and nothing
	// attached a channel.
	lostGen uint64
	// waiting maps a delivery tag to the publication waiting for it.
	waiting map[uint64]*pending
	// free is the pending free list, so that a publication allocates nothing
	// on the steady path.
	free []*pending
	// buf is the reusable encoding buffer. See [publishing]. It is guarded by
	// `pubMu`, not `mu`: the write reads it after `mu` is released.
	buf publishing
	// progress is closed and replaced whenever a publication resolves, which
	// is the broadcast a window wait and a flush park on. It is the idiom
	// `driverkit.PushAdapter` uses for the same reason: a `sync.Cond` cannot
	// be selected against a context, and a channel per waiter would allocate
	// on a path the specification budgets.
	progress chan struct{}
	// cause is why the channel went, so that an ambiguous outcome carries
	// the broker's own reason rather than a generic one.
	cause error

	// stranded mirrors "this publisher has no channel" OUTSIDE `mu`, and it is
	// the only thing [Backend.Health] reads from a sink.
	//
	// It is an atomic and not a read of `ch` under the lock because `admit`
	// used to hold `mu` across the socket write, and a broker under
	// `connection.blocked` stops READING, so that write can block for as long
	// as the alarm lasts. Health taking `mu` then hung exactly when it was the
	// only thing that could explain the stall, which is the failure the
	// specification's "answered without I/O" exists to prevent
	// (`TestCriticalHealthDoesNotWaitForAPublicationInFlight`). The write now
	// holds only `pubMu`, and Health still takes neither. Written only where
	// `ch` is, under `mu`.
	stranded atomic.Bool
	// repairing is set while this publisher's one repair attempt is in flight.
	// It is set and cleared by the repair goroutine itself, through a defer,
	// so it is never stale and no other path has to clear it.
	repairing atomic.Bool
	// repairCause is the last repair failure's reason, kept for the MESSAGE
	// and never for the decision. See [sink.detachedError] for why that
	// distinction is the whole design.
	repairCause atomic.Pointer[repairFailure]

	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// repairFailure is a handle's one-shot channel repair that failed, or a
// consumer's `basic.consume` the broker refused after a reconnection, and the
// broker's own reason for it.
//
// It unwraps to BOTH its advice, the sentence that says what the caller should
// do ([errRepairFailed] for a publisher, [errConsumerRepairFailed] or
// [errConsumeRefused] for a consumer), and the cause, so `errors.Is` finds
// either.
type repairFailure struct{ advice, cause error }

// Error is the advice followed by the reason.
func (f *repairFailure) Error() string { return f.advice.Error() + ": " + f.cause.Error() }

// Unwrap exposes both halves to errors.Is.
func (f *repairFailure) Unwrap() []error { return []error{f.advice, f.cause} }

// pending is one publication waiting for its confirm.
//
// `done` is BUFFERED and sent to rather than closed, which is what makes a
// pending value reusable: a closed channel stays closed, so a free list of
// closed channels would be a free list of nothing. The buffer is one, so the
// confirm loop never blocks on a waiter that has already given up.
type pending struct {
	done   chan struct{}
	result confirm.Result
	// returned records that the publication came back through
	// `basic.return`: the broker took it and could not route it, which the specification
	// makes Rejected and which a caller acts on differently from a refusal.
	returned bool
	// callback is the asynchronous publication's `done`, or nil for a
	// synchronous one.
	callback func(error)
	// owned is the ownership release for [mq.OwnedSink], or nil.
	owned func()
	// msg and tag are what an ASYNCHRONOUS publication's receipt is filled
	// from. A synchronous one is filled by [sink.wait], which still holds
	// both; an asynchronous one resolves in the confirm loop, which is the
	// only place that knows the outcome, so the two have to travel with the
	// pending value. The specification makes the message borrowed until the callback runs,
	// which is what makes writing to it there legal.
	msg *mq.Message
	tag uint64
}

// newSink builds a publisher and attaches its first channel.
func (b *Backend) newSink(ctx context.Context, cfg mq.SinkConfig, n rabbit.Names, mode mq.ConfirmationMode) (*sink, error) {
	strategy, err := b.confirmStrategy()
	if err != nil {
		return nil, err
	}
	env, err := driverkit.NewEnvelope(driverkit.EnvelopeConfig{
		Mode:         cfg.Metadata,
		Capabilities: b.caps,
	})
	if err != nil {
		return nil, err
	}
	s := &sink{
		b:          b,
		topic:      cfg.Topic,
		names:      n,
		mode:       mode,
		strategy:   strategy,
		confirming: mode != mq.ConfirmNone,
		mandatory:  b.cfg.Mandatory,
		replyTo:    IsReplyAddress(cfg.Topic),
		receipts:   cfg.Receipts,
		env:        env,
		opts:       b.options(cfg.Metadata),
		waiting:    make(map[uint64]*pending, strategy.Window()),
		progress:   make(chan struct{}),
		closed:     make(chan struct{}),
	}
	if err := s.attach(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// attach opens a channel, puts it in confirm mode and starts the loop that
// reads its notifications.
//
// It is called at open and again after every reconnection, which is why it
// takes no arguments beyond the context: everything it needs is the sink's
// own configuration, and the topology is declared on a SEPARATE channel
// because a declaration that fails takes its channel with it.
func (s *sink) attach(ctx context.Context) error {
	s.attachMu.Lock()
	defer s.attachMu.Unlock()
	// The caller's context bounds the DECISION to attach, which is
	// [source.attachLocked]'s rule and was missing here: an attach whose
	// context has already expired must not open a channel and leave it to be
	// discarded (review round 1, concurrency-close-waits-on-unbounded-repair-io).
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	// A sink with a channel on the current connection is attached, and
	// attaching it again orphans that channel: it stays open, its loop keeps
	// running, and -- because its confirms are applied to whatever tracker is
	// installed -- the broker confirming ITS tag 1 resolved the publication
	// holding tag 1 on the new channel. The dial loop installs the new
	// connection, replays the topology (round trips) and only then re-attaches
	// the publishers it knows about, so a publisher OPENED during the replay
	// attached itself on the new connection and was then attached again
	// (`TestCriticalAPublisherOpenedDuringAReplayIsAttachedOnce`).
	//
	// A channel on a connection the dial loop has since replaced is NOT
	// attached, even though it is installed: a dial attempt that fails
	// part-way closes its connection and the next attempt does not detach
	// first, so it is replaced below, and what it held is resolved
	// (`TestCriticalARedialThatFailsPartwayReattachesEveryHandle`,
	// `TestCriticalAPublisherReplacedByARetriedDialResolvesWhatItHeld`).
	cn := s.b.conn()
	s.mu.Lock()
	cur, curConn := s.ch, s.chConn
	s.mu.Unlock()
	if cur != nil && curConn == cn {
		return nil
	}
	if !s.replyTo {
		// A reply address needs no topology at all: the default exchange
		// always exists, and the routing key is one the broker minted for
		// this request. Declaring or asserting anything here would refuse
		// the publication, because the name is under the reserved "amq."
		// prefix.
		decl, err := s.b.channelOnWithin(ctx, cn)
		if err != nil {
			return err
		}
		if err := s.b.ensure(ctx, decl, s.names, false); err != nil {
			_ = decl.Close()
			return err
		}
		if err := decl.Close(); err != nil {
			return s.b.errs.Map(mq.OpOpen, s.topic, "", err)
		}
	}

	ch, err := s.b.channelOnWithin(ctx, cn)
	if err != nil {
		return err
	}
	var confirms chan amqp.Confirmation
	if s.confirming {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return s.b.errs.Map(mq.OpOpen, s.topic, "", err)
		}
		// The confirm channel is buffered to the window, so the library's
		// reader never waits for this sink's loop to catch up on a confirm.
		confirms = make(chan amqp.Confirmation, s.strategy.Window()+1)
		ch.NotifyPublish(confirms)
	}
	// The return channel is UNBUFFERED, and that is the one channel here
	// whose buffering is load-bearing. RabbitMQ sends `basic.return` before
	// the `basic.ack` for the same publication, the library's reader
	// dispatches frames in wire order and BLOCKS on each send, so an
	// unbuffered return channel guarantees the loop sees the return before
	// the ack that would otherwise resolve the publication as confirmed.
	// With both channels buffered a `select` may take the ack first and a
	// message the broker threw away is reported as delivered.
	returns := make(chan amqp.Return)
	ch.NotifyReturn(returns)
	lost := make(chan *amqp.Error, 1)
	ch.NotifyClose(lost)

	tr := confirm.NewTracker(s.strategy.Window())

	// The closed check, the install and the loop's REGISTRATION are one
	// critical section, and that is load-bearing rather than tidy.
	//
	// The check at the top of this function is an early-out and nothing more:
	// between it and here the whole of [sink.Close] can run, and then this
	// attach would install a channel on a closed sink, start a goroutine
	// nothing will ever wait for, and call `s.wg.Add(1)` while Close is
	// inside `s.wg.Wait()` -- which is the WaitGroup's one documented misuse
	// ("calls with a positive delta that occur when the counter is zero must
	// happen before a Wait"), and which `sync` reports as a data race and may
	// answer with "WaitGroup misuse: Add called concurrently with Wait".
	//
	// Deciding under `s.mu` is what removes the window, because Close marks
	// itself closed BEFORE it takes the same lock and waits AFTER it has
	// released it. So exactly one of two things happens, and neither is a
	// race: this section runs first, in which case the Add strictly precedes
	// Close's Wait and Close closes the channel this attach just installed --
	// or Close's section runs first, in which case `s.closed` is already
	// closed here and nothing is installed, added or started at all.
	s.mu.Lock()
	if s.isClosed() {
		s.mu.Unlock()
		_ = ch.Close()
		return mq.ErrClosed
	}
	// A channel still installed here is one the guard above found on a
	// replaced connection. What it was holding is resolved as ambiguous --
	// the broker may have taken any of it -- before its tag space is reused,
	// which is [sink.detach]'s rule, apart from what the broker had already
	// confirmed; and it is closed, so that nothing of it outlives the
	// replacement.
	old := s.ch
	var (
		resolved []*pending
		out      []confirm.Outcome
	)
	if old != nil || s.tr != nil {
		resolved, out = s.detachLocked(errClosedChannel)
	}
	s.gen++
	gen := s.gen
	s.ch, s.chConn, s.tr, s.confirms = ch, cn, tr, confirms
	s.stranded.Store(false)
	s.wg.Add(1)
	s.mu.Unlock()
	s.finish(resolved, out)
	if old != nil {
		_ = old.Close()
	}

	// The loop repairs a channel exception from the BACKEND's context and not
	// from this attach's, which is what `contextcheck` sees and objects to.
	// Passing this one would bound a repair that happens minutes later by the
	// deadline of the call that installed the channel -- and on the dial
	// loop's path, by the dial's own context, which is cancelled as soon as
	// the reconnection finishes. [sink.onChannelLost] derives its own bound.
	//nolint:contextcheck // the repair outlives the attach that starts the loop; onChannelLost bounds it from the backend's context
	go s.loop(gen, confirms, returns, lost)
	return nil
}

// loop reads this channel's notifications and resolves the publications they
// name.
//
// It does BOOKKEEPING ONLY, and that is a requirement rather than a style: the
// library's `notifyAll` abandons any send that blocks for more than five
// seconds, so a loop that did anything slow would lose a `basic.return` and
// report an unroutable publication as delivered. Every branch here is a map
// lookup and a non-blocking send.
func (s *sink) loop(gen uint64, confirms <-chan amqp.Confirmation, returns <-chan amqp.Return, lost <-chan *amqp.Error) {
	defer s.wg.Done()
	for {
		select {
		case c, ok := <-confirms:
			if !ok {
				// The library closes the confirm channel when the channel
				// closes, discarding any out-of-sequence confirmation. The
				// close notification is what carries the reason, so this
				// branch only stops reading.
				confirms = nil
				continue
			}
			s.confirmTag(gen, c.DeliveryTag, c.Ack)
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			s.onReturn(gen, &r)
		case aerr := <-lost:
			// The library hands over every confirm that came before the
			// close and only then reports the close, and a select with both
			// ready picks either. Taking the close first resolved acked
			// publications as ambiguous, so the confirms go first.
			s.drainConfirms(gen, confirms)
			s.onChannelLost(gen, reasonOf(aerr))
			return
		case <-s.closed:
			return
		}
	}
}

// onChannelLost answers this publisher's channel going away without the whole
// connection going with it, which is what a channel exception is.
//
// It is [source.onChannelLost] for the publishing side, and it exists for the
// reason that side already had: the supervision loop repairs a lost
// CONNECTION, and a channel exception leaves the connection up, so the dial
// loop never runs and nothing else attaches a sink. Without this route the
// detach below was the end of the publisher: `admit` found `s.ch == nil` and
// refused every publication from then on as Unavailable and Retryable, for
// ever, while Health went on saying Ready. Probed against the fake, a
// `PRECONDITION_FAILED` on the publisher's channel was followed by 296
// refusals in three seconds and no recovery at all. A publication to an
// exchange somebody deleted is enough to reach it on a real broker.
//
// Whether this loop may repair at all is [sink.detachGen]'s answer, and it is
// the close-vs-redial race in one place: a channel that has already been
// detached -- by `Backend.detachChildren` before a redial, or by an attach
// that replaced it -- belongs to whoever detached it, and a second repair
// would be the double attach the specification is about.
func (s *sink) onChannelLost(gen uint64, cause error) {
	if !s.detachGen(gen, cause) {
		return
	}
	if s.isClosed() || s.b.ctx.Err() != nil {
		return
	}
	if s.b.conn() == nil {
		// The whole CONNECTION has gone, not just this channel. The dial loop
		// owns that: it detaches every handle, redials and re-attaches them,
		// and a repair started here would race it for this publisher. It is
		// the ordinary case rather than a rare one -- a lost connection closes
		// every channel on it, so every sink's loop arrives here at once.
		//
		// This comment does not overclaim it: removing this return fails no
		// test. What it buys is that an outage does not look like one channel
		// repair per publisher.
		return
	}
	s.b.channelRepairs.Add(1)
	// In flight from here until the attach returns, which is what
	// [sink.detachedError] reads to tell "a channel is coming" from "no
	// channel is coming". The defer is the whole of its lifetime: nothing
	// else sets it, nothing else clears it, and no later route can leave it
	// saying something that is no longer true.
	s.repairing.Store(true)
	defer s.repairing.Store(false)
	// One attempt, and no loop, which is [source.onChannelLost]'s rule: a
	// channel exception that repeats is a topology problem the caller has to
	// fix, and a driver that re-attached for ever would hide it behind a
	// publisher that never publishes. A failure leaves the sink with no
	// channel, which Publish refuses and which Health now reports.
	//
	// The failure is recorded ON THE SINK and NOT reported into the backend's
	// health state machine. That machine belongs to the CONNECTION and only a
	// redial or `connection.unblocked` ever calls Ready again, so a
	// handle-level Degraded stuck there for ever -- through the caller closing
	// the broken publisher, opening a working one and publishing successfully
	// (review round 1, correctness-health-stuck-degraded). It is the failure
	// mode this branch's own argument for a DERIVED answer predicts, applied
	// to the half that still reported. `Backend.Health` reads the recorded
	// failure instead, so the answer clears itself when the handle is
	// re-attached or closed.
	//
	// ErrClosed is not recorded: the sink was closed between the claim above
	// and the attach, so there is nothing wrong with the CLIENT.
	//
	// The repair's context is bounded, because `sink.Close` waits for this
	// goroutine: `b.ensure`'s declarations are round trips, and an unbounded
	// one would hold a Close for as long as the broker stayed silent. What is
	// still unbounded is `Connection.Channel` itself, which amqp091 offers no
	// deadline for; the maintenance notes records it with an owner.
	ctx, cancel := s.b.waitDeadline(s.b.ctx)
	defer cancel()
	if err := s.attach(ctx); err != nil && !errors.Is(err, mq.ErrClosed) {
		s.repairCause.Store(&repairFailure{advice: errRepairFailed, cause: err})
	}
}

// confirmTag applies one confirm from the channel of generation gen.
//
// A confirm from a channel that is no longer installed names a tag in THAT
// channel's sequence, and the installed tracker's sequence restarted at one:
// applying it would resolve a different publication. Whatever the old channel
// held was resolved as ambiguous when it was replaced, so the confirm is
// dropped.
func (s *sink) confirmTag(gen, tag uint64, ack bool) {
	s.mu.Lock()
	if s.gen != gen {
		s.mu.Unlock()
		s.b.staleConfirms.Add(1)
		return
	}
	if s.tr == nil {
		s.mu.Unlock()
		return
	}
	out := s.applyConfirmLocked(tag, ack)
	resolved := s.takeWaitersLocked(out)
	s.progressLocked()
	s.mu.Unlock()
	s.finish(resolved, out)
}

// applyConfirmLocked applies one `basic.ack` or `basic.nack` to the installed
// tracker, which the caller has checked is there.
func (s *sink) applyConfirmLocked(tag uint64, ack bool) []confirm.Outcome {
	if ack {
		return s.tr.Ack(tag, false)
	}
	return s.tr.Nack(tag, false, false)
}

// drainConfirms applies, on a channel's own loop, the confirms already waiting
// in that channel's notifications.
//
// It is safe on a `mandatory` publisher too, which [sink.takeBufferedLocked]
// is not, so it is the one route that applies a `mandatory` publisher's. The return channel is unbuffered and the library sends a
// publication's `basic.return` before its ack, so every return that came
// before these acks has already been read and recorded by this same loop.
func (s *sink) drainConfirms(gen uint64, confirms <-chan amqp.Confirmation) {
	for {
		select {
		case c, ok := <-confirms:
			if !ok {
				return
			}
			s.confirmTag(gen, c.DeliveryTag, c.Ack)
		default:
			return
		}
	}
}

// progressLocked wakes everything parked on a resolution: the publishers
// waiting for window room and the flushes waiting for the window to empty.
//
// It closes the current channel and installs a new one, which is a broadcast
// to every waiter at once and costs one channel per RESOLUTION BATCH rather
// than one per waiter.
func (s *sink) progressLocked() {
	if s.progress != nil {
		close(s.progress)
	}
	s.progress = make(chan struct{})
}

// listen returns the channel a waiter parks on, taken under the lock so that
// a resolution between the check and the park cannot be missed.
func (s *sink) listen() <-chan struct{} { return s.progress }

// onReturn records a `basic.return`: the broker took the publication and could
// not route it.
//
// Every `mandatory` publication this sink makes carries a correlation token,
// so the attribution is exact. A return without one is not this sink's to
// attribute by token, and the tracker's own "exactly one outstanding
// publication" rule is the attribution then, which refuses rather than
// guessing when that does not hold.
func (s *sink) onReturn(gen uint64, r *amqp.Return) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tr == nil || s.gen != gen {
		// A return from a channel that has been replaced, for
		// [sink.confirmTag]'s reason.
		return
	}
	if token := tokenOfReturn(r); token != "" {
		s.tr.Returned(token)
		return
	}
	s.tr.ReturnedOnly()
}

// takeWaitersLocked pairs each resolved outcome with the publication that was
// waiting for it, removing both from the map.
func (s *sink) takeWaitersLocked(out []confirm.Outcome) []*pending {
	if len(out) == 0 {
		return nil
	}
	resolved := make([]*pending, len(out))
	for i := range out {
		p := s.waiting[out[i].Tag]
		if p == nil {
			continue
		}
		delete(s.waiting, out[i].Tag)
		p.result, p.returned = out[i].Result, out[i].Returned
		resolved[i] = p
	}
	return resolved
}

// finish wakes the synchronous waiters and calls the asynchronous callbacks.
//
// It runs OUTSIDE the sink's lock, which is the rule the specification states for an
// asynchronous callback ("a callback must not block the driver's progress")
// and which is also what keeps a callback that publishes again from
// self-deadlocking.
func (s *sink) finish(resolved []*pending, out []confirm.Outcome) {
	for i, p := range resolved {
		if p == nil {
			continue
		}
		err := s.outcomeError(out[i])
		if p.owned != nil {
			p.owned()
			p.owned = nil
		}
		if p.callback != nil {
			cb := p.callback
			p.callback = nil
			if err == nil {
				s.fillReceipt(p.msg, p.tag)
			}
			p.msg = nil
			cb(err)
			s.recycle(p)
			continue
		}
		select {
		case p.done <- struct{}{}:
		default:
		}
	}
}

// outcomeError turns one publication's outcome into the error a caller sees.
//
// The three answers are the specification, and the distinction between the last two is
// the one that matters most: Rejected is definite (the broker took the message
// and threw it away, so a retry throws it away again) and Ambiguous is not
// (the broker may have it, so a retry may duplicate).
func (s *sink) outcomeError(o confirm.Outcome) error {
	switch o.Result {
	case confirm.Confirmed:
		return nil
	case confirm.Rejected:
		if o.Returned {
			return s.b.errs.Map(mq.OpPublish, s.topic, "", &amqp.Error{
				Code:   amqp.NoRoute,
				Reason: "no queue is bound for this routing key, so the broker returned the message",
				Server: true,
			})
		}
		return s.b.errs.Map(mq.OpPublish, s.topic, "", &amqp.Error{
			Code:   amqp.InternalError,
			Reason: "the broker could not take responsibility for the publication (basic.nack)",
			Server: true,
		})
	case confirm.Ambiguous:
		return &mq.OpError{
			Driver: DriverName, Operation: mq.OpPublish, Topic: s.topic,
			Code: mq.CodeAmbiguous, Retryable: false, Ambiguous: true,
			Err: s.ambiguousCause(),
		}
	default:
		return nil
	}
}

// ambiguousCause is why the channel went, or the generic reason when nobody
// recorded one.
func (s *sink) ambiguousCause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cause != nil {
		return s.cause
	}
	return errClosedChannel
}

// Publish admits one message and waits for whatever boundary this publisher's
// confirmation mode names.
func (s *sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	return s.publish(ctx, m, o, nil, nil)
}

// PublishAsync admits one message and reports its outcome to done.
//
// The contract is the specification: a non-nil immediate error means done will not run, a
// nil return means done runs exactly once, and the message is borrowed until
// done runs. The window is what bounds the pending set — the specification forbids an
// unbounded one — so a publication made while the window is full waits for
// room, which is backpressure and not a confirmation boundary.
func (s *sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return s.publish(ctx, m, o, nil, nil)
	}
	return s.publish(ctx, m, o, done, nil)
}

// PublishOwned takes ownership of om's message and releases it exactly once
//
// Ownership transfers at invocation, so the release is this sink's obligation
// from the moment it is called — including when it refuses the publication,
// when it is closed and when the failure is permanent. The release runs when
// the publication RESOLVES rather than when the frame is written, because the
// encoded frame borrows the message's body: the library copies nothing, so
// releasing before the confirm could hand the body back to a pool while it is
// still in the socket.
func (s *sink) PublishOwned(ctx context.Context, om mq.OwnedMessage, o *mq.PublishOptions) error {
	if om.Message == nil {
		if om.Release != nil {
			om.Release(nil)
		}
		return nil
	}
	release := func() { om.Release(om.Message) }
	err := s.publish(ctx, om.Message, o, nil, release)
	return err
}

// publish is the one publication path every surface goes through.
func (s *sink) publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error), owned func()) error {
	release := owned
	defer func() {
		// The ownership release the sink still holds when it leaves this
		// function: the paths that hand it to a pending value clear it
		// first. The specification makes the release the callee's obligation on every
		// path, including a refusal.
		if release != nil {
			release()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return mq.ErrClosed
	}
	if m == nil {
		return nil
	}
	// The envelope's admission check comes first and costs no I/O: a message
	// this driver would accept and then be unable to decode again is a
	// record that can be neither delivered nor settled, which is the specification's
	// disqualifier reached through the driver's own doing.
	if err := s.env.Check(m); err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	msg, err := s.env.Wrap(m)
	if err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	if err := s.checkOptions(o); err != nil {
		return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}

	if done != nil && s.confirming {
		// An ASYNCHRONOUS publication waits for ROOM and it waits BEFORE it
		// is admitted, and both halves are the contract rather than a
		// preference.
		//
		// It waits because the specification forbids an unbounded pending set: without a
		// bound, a caller that publishes faster than the broker confirms
		// holds every unconfirmed message alive, and the messages are
		// BORROWED until their callbacks run, so the bound is on the
		// caller's memory and not just on this sink's map.
		//
		// It waits BEFORE admission because the specification also says a non-nil return
		// means the callback does not run at all. A publication that was
		// admitted and then failed its wait would have its callback
		// registered and its error returned, which is both answers at once —
		// the one shape of this function that cannot be made correct
		// afterwards.
		//
		// The limit is MaxAmbiguous and not Window: it is the number of
		// publications this strategy is willing to have outstanding, which
		// for the per-message strategy is one. Using the window would let the
		// per-message strategy hold sixty-four outstanding publications
		// through this path alone, and its MaxAmbiguous — which the specification reads as the strategy's whole cost — would be a lie.
		if err := s.waitRoom(ctx, s.strategy.MaxAmbiguous()); err != nil {
			return err
		}
	}
	tag, p, action, err := s.admit(ctx, m, msg, o, done, &release)
	if err != nil {
		return err
	}
	if !s.confirming {
		// ConfirmNone: the boundary is the socket write, which has already
		// happened. The specification says so and the driver page says what it costs.
		// An asynchronous publication's callback is therefore owed now: no
		// confirm will ever arrive to run it, and a nil return promises it
		// runs exactly once.
		s.fillReceipt(m, 0)
		if done != nil {
			done(nil)
		}
		return nil
	}
	if done != nil {
		// Admitted, so the callback is registered and this call owes its
		// caller nothing more. Every action is the same answer here: the
		// outcome goes to the callback, and the wait for room has already
		// happened above.
		return nil
	}
	switch action {
	case confirm.WaitTag:
		return s.wait(ctx, m, tag, p)
	case confirm.WaitWindow:
		// Backpressure rather than a confirmation boundary: the publication
		// that filled the window is still outstanding when the wait ends.
		//
		// The wait for room does not answer for this publication, which is
		// written already. Whatever ends that wait early (a Close, a channel
		// that went, the deadline) leaves this publication Ambiguous or still
		// outstanding, and its own wait is what says which. Returning the
		// room's error answered a Close with ErrClosed and the other two with
		// retryable refusals, each a definite "not sent" for a frame the
		// broker may hold. One deadline bounds both waits, so a confirm that
		// never comes costs one PublishTimeout and not two.
		wctx, cancel := s.b.waitDeadline(ctx)
		defer cancel()
		_ = s.waitRoom(wctx, s.strategy.Window())
		return s.wait(wctx, m, tag, p)
	case confirm.Continue, confirm.Defer:
		// A synchronous publish owes its caller an outcome whatever the
		// strategy says, because `Publish` returns an error and there is
		// nowhere else to put it. The windowed strategy's benefit is
		// therefore realised by `PublishAsync` alone: the core's
		// `PublishBatch` is a loop of `Publish` calls, so each message of a
		// batch waits here for its own confirm.
		//
		// `Defer` is the asynchronous strategy's answer below its window, and
		// it is the same case: the strategy defers to a callback, and a
		// synchronous publish has none. It used to return nil here, before
		// the confirm, so a `basic.nack` or the `basic.return` of an
		// unroutable mandatory publication reached nobody and the caller had
		// been told the broker held a message it had refused. The core's
		// batch loop publishes through this path too.
		return s.wait(ctx, m, tag, p)
	default:
		return nil
	}
}

// admit takes the tag, encodes the publication and writes the frame, all under
// the sink's publish-order lock. See [sink] for why the three are one critical
// section, and for why `mu` is released before the write.
//
// m is the caller's message and wire is what goes on the wire, which is a
// different message when the envelope wrapped it. The wrapped copy carries
// only the topic and the body, so the routing key and the receipt slot are
// read from m: routing a wrapped publication by its own Key routed every one
// of them by the topic.
func (s *sink) admit(ctx context.Context, m, wire *mq.Message, o *mq.PublishOptions, done func(error), release *func()) (uint64, *pending, confirm.Action, error) {
	// What a write into a closed channel resolved. The deferred finish is
	// registered first so that it runs AFTER the deferred unlock: see
	// [sink.finish] for why it runs outside the lock -- and a callback that
	// publishes again needs `pubMu` as much as `mu`.
	var (
		resolved []*pending
		out      []confirm.Outcome
	)
	defer func() { s.finish(resolved, out) }()
	s.pubMu.Lock()
	defer s.pubMu.Unlock()
	st, err := s.stage(m, wire, o, done, release)
	if err != nil {
		return 0, nil, 0, err
	}

	exchange, key := s.route(m)
	// The caller's context bounds the WRITE. It deliberately does not bound
	// the wait for the confirm, which [sink.wait] does with its own
	// deadline: a context that expired between the two would leave the
	// publication outstanding and the caller told it had failed, which is
	// the ambiguity the specification asks a driver not to invent.
	if err := st.ch.PublishWithContext(ctx, exchange, key, s.mandatory, false, s.buf.pub); err != nil {
		s.mu.Lock()
		var perr error
		resolved, out, perr = s.publishFailedLocked(st.tr, st.tag, st.p, release, err)
		s.mu.Unlock()
		if perr != nil {
			return 0, nil, 0, perr
		}
		// The channel was detached or replaced while the frame was being
		// written, and whoever did it already resolved this publication as
		// ambiguous and handed the outcome to its waiter. That waiter is the
		// answer, and an error here as well would be two answers: a callback
		// has already run, and a synchronous caller is sent to read the
		// outcome with WaitTag whatever the strategy said, because `Defer`
		// would report the failed write as a success.
		return st.tag, st.p, confirm.WaitTag, nil
	}
	if !s.confirming {
		if *release != nil {
			(*release)()
			*release = nil
		}
		return 0, nil, confirm.Defer, nil
	}
	return st.tag, st.p, st.action, nil
}

// staged is one publication [sink.stage] has numbered and registered, and the
// channel it has to be written to.
type staged struct {
	ch     client.Channel
	tr     *confirm.Tracker
	tag    uint64
	p      *pending
	action confirm.Action
}

// stage is the half of [sink.admit] that needs `mu`: it encodes the
// publication into the sink's buffer, takes its delivery tag and registers its
// waiter. The caller holds `pubMu`, which is what keeps the buffer and the tag
// its own until the frame has been written.
//
// The strategy's action is decided here rather than after the write because
// it reads the tracker, and the tracker may be gone by then. Nothing is lost
// by deciding early: only a confirm can lower the outstanding count, and the
// write in between was the one thing this publication was waiting for.
func (s *sink) stage(m, wire *mq.Message, o *mq.PublishOptions, done func(error), release *func()) (staged, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		return staged{}, s.detachedError()
	}
	// The token goes on every `mandatory` publication, whatever the strategy.
	// The per-message strategy keeps one publication outstanding only per
	// CALLER: `Publish` is safe for concurrent use and `pubMu` does not span
	// the wait for the confirm, so two goroutines leave two publications
	// outstanding on one channel. A return between them had no candidate the
	// tracker would name, it was dropped, and the unroutable publication was
	// then reported Confirmed when its ack arrived.
	token := ""
	if s.confirming && s.mandatory {
		token = confirmToken(s.b.nextSeq())
	}
	opts := s.opts
	if o != nil {
		opts.Publish = *o
	}
	// The key is the caller's message's, as the route is ([sink.route]): an
	// enveloped copy carries no key of its own. It goes in [KeyHeader] too
	// under KeyAsRoutingKey and on a reply, whose routing keys are not always
	// the key.
	carryKey := s.b.cfg.KeyAsRoutingKey || s.replyTo
	if err := s.buf.encode(s.b.proto, wire, opts, token, carryKey, m.Key); err != nil {
		return staged{}, s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	s.buf.identify(s.b.cfg.AppID, s.b.cfg.PublishType)
	// The header table is validated before the tag is taken, because the
	// library validates it too and returns before it touches its own
	// counter: a driver that took a tag first would be one ahead of the
	// library for the rest of the channel's life.
	if err := s.buf.pub.Headers.Validate(); err != nil {
		return staged{}, s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}

	st := staged{ch: s.ch, tr: s.tr}
	if !s.confirming {
		return st, nil
	}
	tag, err := s.tr.Submit(token)
	if err != nil {
		return staged{}, s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	p := s.acquire()
	p.callback = done
	p.tag = tag
	if done != nil && s.receipts {
		// Only for an asynchronous publication whose publisher asked for
		// receipts: a synchronous one is filled by [sink.wait], which
		// has the caller's own message, and holding a pointer to a
		// message nobody will read is a reference a pool cannot reclaim.
		p.msg = m
	}
	if *release != nil {
		p.owned = *release
		*release = nil
	}
	s.waiting[tag] = p
	st.tag, st.p = tag, p
	st.action = s.strategy.OnSubmit(s.tr.Outstanding())
	return st, nil
}

// publishFailedLocked answers a write that did not happen, or may not have.
//
// The distinction is the one turns on. When the CHANNEL IS STILL OPEN the
// library refused the publication before it wrote anything — it validates the
// header table and checks the context first — so the tag it never used has to
// be taken back, or this sink is one ahead of the library for ever. When the
// channel has gone the frame may have reached the broker, so every publication
// this channel was holding is AMBIGUOUS and the detach is what says so.
//
// The publications a detach resolved are returned rather than finished here,
// because [sink.finish] runs outside the lock.
//
// tr is the tracker the tag was taken from. The write runs without `mu`, so by
// the time it fails the channel may have been detached or replaced; whoever
// did that resolved this publication with the rest, and the nil error says
// there is nothing left to answer.
func (s *sink) publishFailedLocked(tr *confirm.Tracker, tag uint64, p *pending, release *func(), err error) ([]*pending, []confirm.Outcome, error) {
	if !s.confirming {
		return nil, nil, s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, true)
	}
	if s.tr != tr {
		return nil, nil, nil
	}
	if s.ch != nil && !s.ch.IsClosed() {
		s.unwaitLocked(tag, p, release)
		_ = s.tr.Unsubmit(tag)
		return nil, nil, s.b.errs.Map(mq.OpPublish, s.topic, "", err)
	}
	// The channel is gone. Everything it was holding, including this
	// publication, is ambiguous, apart from what a confirm already waiting
	// for the channel answers ([sink.detachLocked]). This publication's
	// answer is the error returned here, so it leaves the map before the
	// detach: resolving it as well would run its callback after a non-nil
	// return. The others are returned for the caller to finish outside the
	// lock.
	s.unwaitLocked(tag, p, release)
	resolved, out := s.detachLocked(err)
	return resolved, out, &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish, Topic: s.topic,
		Code: mq.CodeAmbiguous, Retryable: false, Ambiguous: true, Err: err,
	}
}

// unwaitLocked takes a publication whose write failed back out of the map,
// handing its ownership release back to the caller's.
func (s *sink) unwaitLocked(tag uint64, p *pending, release *func()) {
	delete(s.waiting, tag)
	if p == nil {
		return
	}
	if p.owned != nil {
		*release = p.owned
		p.owned = nil
	}
	p.callback = nil
	// The Locked form: `admit` holds the lock, and Go's mutex is not
	// reentrant.
	s.recycleLocked(p)
}

// route is the exchange and routing key one publication goes to.
//
// With the key as the routing key a message with no key falls back to the
// topic, which is what keeps a keyless publication routable: a routing key of
// "" on a topic exchange matches only a binding of "" and would reach nothing.
func (s *sink) route(m *mq.Message) (exchange, key string) {
	if s.replyTo {
		// The default exchange routes by name, and the name is the reply
		// address itself. The message key is ignored: a reply goes where the
		// requester said and nowhere else.
		return "", s.topic
	}
	if !s.b.cfg.KeyAsRoutingKey || len(m.Key) == 0 {
		return s.names.Exchange, s.topic
	}
	return s.names.Exchange, string(m.Key)
}

// wait blocks until the publication resolves or the context is done.
//
// A context that expires leaves the publication OUTSTANDING and reports
// CodeDeadlineExceeded with Ambiguous set, which is the honest answer: the
// frame is at the broker and the confirm has not arrived, so nobody knows
// whether it was stored. Reporting a plain timeout would invite a retry that
// duplicates it, which the specification names a settlement disqualifier.
//
// A Close is not a case of its own, for the same reason. The frame is written
// by the time this runs, and [sink.Close] resolves every publication it finds
// outstanding as Ambiguous, which wakes this wait through `p.done`. It closes
// `s.closed` BEFORE it does that, so a wait with a case on `s.closed` woke
// there first and answered ErrClosed: a definite "not sent" for a publication
// the broker may hold, which invites the duplicating retry.
func (s *sink) wait(ctx context.Context, m *mq.Message, tag uint64, p *pending) error {
	if p == nil {
		return nil
	}
	ctx, cancel := s.b.waitDeadline(ctx)
	defer cancel()
	select {
	case <-p.done:
		result, returned := s.readResult(p)
		s.recycle(p)
		err := s.outcomeError(confirm.Outcome{Tag: tag, Result: result, Returned: returned})
		if err == nil {
			s.fillReceipt(m, tag)
		}
		return err
	case <-ctx.Done():
		// The pending value is NOT recycled: the confirm loop still holds a
		// reference to it through the waiting map and will write to it. A
		// sink that returned it to the free list here would hand it to the
		// next publication and then have the old confirm resolve that one.
		return &mq.OpError{
			Driver: DriverName, Operation: mq.OpPublish, Topic: s.topic,
			Code: mq.CodeDeadlineExceeded, Retryable: false, Ambiguous: true,
			Err: ctx.Err(),
		}
	}
}

// waitRoom waits until fewer than limit publications are outstanding.
//
// It parks on the sink's resolution broadcast rather than polling: the window
// has room exactly when a publication resolves, and every resolution closes
// the broadcast channel. So a publisher held at the window costs one select
// and no timer.
//
// The limit is a parameter rather than the strategy's window because the two
// callers bound different things: a synchronous publication held at
// `WaitWindow` is waiting for the strategy's own window, and an asynchronous
// one is waiting for the number of outstanding publications the strategy is
// willing to make ambiguous. They are equal for the windowed and asynchronous
// kinds and differ by sixty-three for the per-message one.
func (s *sink) waitRoom(ctx context.Context, limit int) error {
	if limit < 1 {
		limit = 1
	}
	ctx, cancel := s.b.waitDeadline(ctx)
	defer cancel()
	for {
		s.mu.Lock()
		if s.tr == nil {
			s.mu.Unlock()
			return s.detachedError()
		}
		if s.tr.Outstanding() < limit {
			s.mu.Unlock()
			return nil
		}
		wake := s.listen()
		s.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return &mq.OpError{
				Driver: DriverName, Operation: mq.OpPublish, Topic: s.topic,
				Code: mq.CodeBackpressure, Retryable: true,
				Err: ctx.Err(),
			}
		case <-s.closed:
			return mq.ErrClosed
		}
	}
}

// readResult reads a pending value's outcome under the lock, because the
// confirm loop wrote it under the lock.
func (s *sink) readResult(p *pending) (confirm.Result, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return p.result, p.returned
}

// Flush completes the publications already admitted (FlushSink).
//
// It is how a caller of `PublishAsync` reaches the boundary every synchronous
// `Publish` reaches on its own, and the core calls it from
// `PublisherHandle.Flush` and from a graceful shutdown. The core's batch loop
// does not need it, because every `Publish` in that loop has already waited.
//
// It waits for the WHOLE window to drain rather than for one publication,
// because that is what "the publications already admitted" means; a caller
// that wants one publication's outcome waits on that publication.
func (s *sink) Flush(ctx context.Context) error {
	if s.isClosed() {
		return mq.ErrClosed
	}
	ctx, cancel := s.b.waitDeadline(ctx)
	defer cancel()
	for {
		s.mu.Lock()
		outstanding := len(s.waiting)
		wake := s.listen()
		s.mu.Unlock()
		if outstanding == 0 {
			return nil
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closed:
			return mq.ErrClosed
		}
	}
}

// fillReceipt fills the caller's receipt slot in place.
//
// The storage is the caller's and the core has already zeroed it, which is
// what keeps a successful publication free of a result-struct allocation. The
// sequence is the DELIVERY TAG, which is this channel's own counter and not a
// broker-wide position: a caller must not read it as one, and the driver page
// says so.
func (s *sink) fillReceipt(m *mq.Message, tag uint64) {
	if !s.receipts || m == nil || m.Receipt == nil {
		return
	}
	m.Receipt.Confirmation = s.mode
	m.Receipt.ConfirmedAt = s.b.clock.Now()
	m.Receipt.Sequence = tag
	m.Receipt.MessageID = m.ID
}

// checkOptions refuses a per-call control this publisher cannot honour, before
// any I/O.
//
// Every refusal here is a case where the broker would ACCEPT the publication
// and not do what was asked, which is the worst kind of unsupported: a
// priority on a queue with no `x-max-priority` is ignored, and a delayed
// publication is delivered at once. Refusing is what makes the capability
// table's Unsupported mean something.
//
// A delay is refused with the delayed-message plugin too. The refusal used to
// depend on it, and with the plugin detected a Delay or a DeliverAt went to
// the topic's exchange with no `x-delay`: see the FeatureScheduledPublish row
// in [narrow] for why no topology this driver declares can schedule one.
func (s *sink) checkOptions(o *mq.PublishOptions) error {
	if o == nil {
		return nil
	}
	if o.PrioritySet && s.b.cfg.MaxPriority == 0 {
		return errPriorityWithoutQueue
	}
	if o.Delay > 0 || !o.DeliverAt.IsZero() {
		return errScheduledPublish
	}
	if o.DedupKey != "" {
		// RabbitMQ de-duplicates nothing. A driver that accepted the key and
		// ignored it would let a caller believe a publication was
		// idempotent.
		return errNoDeduplication
	}
	return nil
}

// detach resolves everything this channel was holding as AMBIGUOUS, apart
// from what the broker has already answered, and drops the channel.
//
// It is the single most important function in this file. A channel that closes
// with confirms outstanding leaves those publications unresolved: the broker
// may have taken every one of them, and it may have taken none. The specification forbids
// reporting that as a definite failure and the specification names doing so a settlement
// disqualifier, because a caller that retries a publication the broker took
// has duplicated it.
//
// amqp091-go's own answer to the same event is to NACK every pending
// confirmation — `deferredConfirmations.Close` sets `ack=false` — which is a
// definite negative and the wrong one. This driver does not use that path at
// all, which is why it reads its confirms from `NotifyPublish` rather than
// from a `DeferredConfirmation`.
// The caller is the supervision loop, which owns the repair that follows, so
// it CLAIMS the installed generation: the sink's own loop will read the same
// channel's close notification a moment later, and a repair started from there
// would run against a connection the dial loop is replacing -- reporting a
// failure nobody should act on, or attaching a channel mid-replay. See
// [sink.lostGen].
//
// A confirm the library has already handed over is the broker's answer and
// not an unknown, so it is applied first wherever that is safe
// ([sink.takeBufferedLocked]).
func (s *sink) detach(cause error) {
	s.mu.Lock()
	s.lostGen = s.gen
	resolved, out := s.detachLocked(cause)
	s.mu.Unlock()
	s.finish(resolved, out)
}

// takeBufferedLocked applies the confirms waiting in the installed channel's
// notifications, which its loop has not read yet, and returns their outcomes.
// The tracker is installed.
//
// It is for [sink.detachLocked], so that every route that drops a channel
// applies them. A broker that restarts gracefully acks what it has stored and
// then closes the connection, and the library delivers the acks before it
// reports the close. The supervision loop then detaches every publisher while
// those acks still wait in its confirm channel, and resolving them as
// ambiguous told the caller the broker might not have a message it had
// confirmed. A publication that meets the closed channel before the loop has
// read the close, an attach that replaces a channel left on a dead connection
// and a Close drop the channel the same way.
//
// A `mandatory` publisher's are left alone. Its loop may have taken a
// `basic.return` off the unbuffered return channel and not yet recorded it,
// while the library sends that publication's ack straight after: applying the
// ack here would report a message the broker threw away as confirmed. A
// confirm the loop has taken and not yet applied is resolved as ambiguous too.
// Both are the safe answer, and neither is ever a wrong definite one.
func (s *sink) takeBufferedLocked() []confirm.Outcome {
	if s.mandatory {
		return nil
	}
	var out []confirm.Outcome
	for {
		select {
		case c, ok := <-s.confirms:
			if !ok {
				return out
			}
			out = append(out, s.applyConfirmLocked(c.DeliveryTag, c.Ack)...)
		default:
			return out
		}
	}
}

// detachGen is [sink.detach] for one generation's loop: it detaches only if
// that generation is still the installed one, and reports whether THIS caller
// made the transition.
//
// A close notification names the channel it came from, and by the time the
// loop reads it that channel may already have been replaced -- see [sink.gen]
// for the sequence, which is the ordinary one on a lost connection rather than
// a rare interleaving. Detaching then would clear the channel the dial loop
// had just installed, so the notification is dropped instead: everything the
// old channel was holding was already resolved by whoever replaced it, and
// there is nothing left for this call to do.
//
// [sink.lostGen] is the second half of the question, and it is what makes the
// answer a one-shot CLAIM rather than merely a generation check. A repair keyed on the generation alone would race the dial loop:
// `Backend.detachChildren` detaches every sink before a redial without moving
// the generation, so a loop whose notification arrives afterwards still
// matches on `gen` and would attach a second channel beside the one the dial
// loop is about to install -- which is the orphan, reached
// from the other direction.
//
// Only the generation mismatch is counted: `staleCloses` is the REORDERING
// The specification is about, and a notification for a channel somebody has
// already taken responsibility for is not reordered, it is answered.
func (s *sink) detachGen(gen uint64, cause error) bool {
	s.mu.Lock()
	if s.gen != gen {
		s.mu.Unlock()
		s.b.staleCloses.Add(1)
		return false
	}
	if s.lostGen >= gen {
		s.mu.Unlock()
		s.b.declinedCloses.Add(1)
		return false
	}
	s.lostGen = gen
	resolved, out := s.detachLocked(cause)
	s.mu.Unlock()
	s.finish(resolved, out)
	return true
}

// detachLocked is [sink.detach] for a caller that already holds the lock. It
// returns the waiters to wake, which the caller wakes outside the lock.
//
// The confirms already waiting for the channel are applied here and not in
// [sink.detach] alone, because every route that drops a channel comes through
// here. Applying them only on the supervision loop's route still reported an
// acked publication as ambiguous when a publication met the closed channel
// first ([sink.publishFailedLocked]), which is what a channel exception with
// publishers in flight does.
func (s *sink) detachLocked(cause error) ([]*pending, []confirm.Outcome) {
	s.cause = cause
	s.stranded.Store(true)
	if s.tr == nil {
		s.ch, s.chConn, s.confirms = nil, nil, nil
		return nil, nil
	}
	out := s.takeBufferedLocked()
	out = append(out, s.tr.Detach()...)
	resolved := s.takeWaitersLocked(out)
	s.progressLocked()
	// Anything still in the map has no outcome and no waiter pairing, which
	// cannot happen — Detach resolves every outstanding tag — but a map left
	// with entries would leak pending values, so it is emptied explicitly.
	for tag := range s.waiting {
		delete(s.waiting, tag)
	}
	s.ch, s.chConn, s.tr, s.confirms = nil, nil, nil, nil
	return resolved, out
}

// Close releases the sink. It is idempotent, and a Publish afterwards
// returns ErrClosed.
//
// The publications it was holding are resolved as ambiguous, for
// [sink.detach]'s reason: a close does not tell the broker to forget what it
// has already taken. Those whose confirm is already waiting are resolved by
// it, as on every detach.
//
// THE ORDER IS THE INTERLOCK WITH [sink.attach] and must not be rearranged:
// `s.closed` is closed BEFORE `s.mu` is taken, and `s.wg` is waited on AFTER
// it has been released. That is what makes attach's decision -- refuse, or
// register a loop -- happen entirely on one side of this close or the other.
// `redial` re-attaches every sink on the supervise
// goroutine while the core is free to close a publisher at any moment, so this
// is not a rare window: it is the window `Backend.repair` exists for.
func (s *sink) Close() error {
	var first error
	s.closeOnce.Do(func() {
		close(s.closed)
		s.mu.Lock()
		ch := s.ch
		resolved, out := s.detachLocked(mq.ErrClosed)
		s.mu.Unlock()
		s.finish(resolved, out)
		if ch != nil {
			if err := ch.Close(); err != nil && !isBenignClose(err) {
				first = s.b.errs.Map(mq.OpClose, s.topic, "", err)
			}
		}
		s.wg.Wait()
		s.b.forget(s, nil)
	})
	return first
}

// detached reports whether this publisher has no channel, which is a publisher
// that refuses every publication. See [Backend.detachedHandle].
func (s *sink) detached() bool { return !s.isClosed() && s.stranded.Load() }

// detachedError is what a publication meets while this publisher has no
// channel, and the two answers are different in the one way a caller acts on.
//
// A repair in flight or a redial is Retryable: something is attaching a
// channel. A repair that FAILED is not: the repair is one-shot, `Publish`
// never attaches, and nothing else will until the connection is replaced --
// so a retry loop here is the spin the specification set out to remove. Nor
// is a dial loop that has given up, which says so in place of the repair's
// advice: a new publisher on a client that will never reconnect fails too.
func (s *sink) detachedError() error {
	if s.repairing.Load() || s.b.reconnectPending() {
		return errDetached
	}
	why := s.repairReason()
	if gone := s.b.lostForGood(); gone != nil {
		why = gone
	}
	return &mq.OpError{
		Driver: DriverName, Operation: mq.OpPublish, Topic: s.topic,
		Code: mq.CodeUnavailable, Retryable: false, Err: why,
	}
}

// repairReason is the last repair failure's cause wrapped in the advice, or
// the advice alone when nobody recorded one.
//
// It is BEST EFFORT and deliberately not synchronised with the decision above:
// it may name an earlier episode's failure. That is the trade-off takes. The first design made a sticky record the DECISION, and two
// separate races then made it say the opposite of the truth -- a publisher the
// dial loop had just re-attached, refused as one that would never work again.
// A cause that is out of date can mislead about WHY; a decision that is out of
// date refuses a live channel.
func (s *sink) repairReason() error {
	if f := s.repairCause.Load(); f != nil {
		return f
	}
	return errRepairFailed
}

// detachedReason is what [Backend.Health] reports for this publisher, or nil
// when the publisher is fine or on its way out.
//
// A failed repair reports its own cause rather than the generic sentence,
// because the two mean different things to whoever is reading a health
// endpoint: one mends itself and the other does not.
func (s *sink) detachedReason() error {
	if !s.detached() {
		return nil
	}
	if s.repairing.Load() || s.b.reconnectPending() {
		return errHandleDetached
	}
	return s.repairReason()
}

// isClosed reports whether Close has run.
func (s *sink) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// acquire takes a pending value from the free list, or makes one.
func (s *sink) acquire() *pending {
	if n := len(s.free); n > 0 {
		p := s.free[n-1]
		s.free = s.free[:n-1]
		p.result, p.returned, p.callback, p.owned = 0, false, nil, nil
		// The channel may hold a token nobody took -- a publication whose
		// caller gave up on a deadline -- and reusing it without draining
		// would resolve the next publication instantly.
		select {
		case <-p.done:
		default:
		}
		return p
	}
	return &pending{done: make(chan struct{}, 1)}
}

// recycle returns a pending value to the free list.
//
// The bound is the window, because that is the most publications that can be
// outstanding at once: a free list longer than the window would be holding
// storage nothing will ask for.
func (s *sink) recycle(p *pending) {
	if p == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recycleLocked(p)
}

// recycleLocked is [sink.recycle] for a caller that already holds the lock.
func (s *sink) recycleLocked(p *pending) {
	if p == nil {
		return
	}
	// The message reference is dropped before the value goes back on the free
	// list: a pending value holds a caller's message only for as long as its
	// receipt has to be filled, and a free list of stale message pointers is
	// a free list nothing can reclaim.
	p.msg = nil
	if len(s.free) < s.strategy.Window()+1 {
		s.free = append(s.free, p)
	}
}

// isBenignClose reports whether a close error is the handle having already
// gone, which is not a failure to report.
func isBenignClose(err error) bool {
	code, ok := ReplyCode(err)
	return ok && code == amqp.ChannelError
}

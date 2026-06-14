package rabbitfake

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
)

// DelayHeader is the header the delayed-message plugin reads the delay out of.
//
// It is declared here rather than imported from `internal/rabbit/delay`, which
// declares the same constant, because this package's import list is
// deliberately short: the library's value types, `internal/rabbit` for the
// topic matcher, and the standard library. A fake that pulled in a variant
// family to read one string would tie the broker to the strategy that drives
// it.
const DelayHeader = "x-delay"

// pendingConfirm is one publication a confirm script is holding back.
type pendingConfirm struct {
	tag uint64
	ack bool
}

// fakeChan is one AMQP channel.
//
// The state divides in two and the division is the only thing a reader has to
// keep straight. Everything to do with the BROKER -- the unacknowledged
// deliveries, the consumers, the prefetch, the confirm script -- is guarded by
// the broker's own mutex, because a settlement and the redelivery it releases
// have to be one step. Everything to do with NOTIFICATION -- the listener
// slices, the delivery channels -- is guarded by notifyMu, because the
// connection's dispatcher reads it while blocking on a send and a close has to
// be able to wait for that send rather than race it. That is the library's own
// arrangement (`Channel.m` and `Channel.notifyM`) and it is its arrangement for
// the same reason.
type fakeChan struct {
	conn *fakeConn
	id   int

	// closed is read by the dispatcher without any lock, which is why it is
	// atomic rather than a field under the broker's mutex: a delivery frame
	// that was already in the queue when the channel closed must be dropped,
	// and the dispatcher cannot take the broker's mutex to find that out
	// without inverting the lock order.
	closed atomic.Bool
	// gone is closed when the channel's resources are torn down. It releases
	// anything parked waiting for this channel.
	gone chan struct{}
	once sync.Once

	notifyMu sync.RWMutex
	closes   []chan *amqp.Error
	returns  []chan amqp.Return
	cancels  []chan string
	flows    []chan bool

	confirms *confirms

	// The rest is guarded by the broker's mutex.
	cause      *amqp.Error
	confirming bool
	// conPrefetch and chPrefetch are the two prefetch readings, held
	// SEPARATELY because RabbitMQ enforces them separately: conPrefetch is
	// the last `basic.qos` with global=false and is snapshotted by each
	// `basic.consume` that follows, chPrefetch is the last one with
	// global=true and bounds this channel's total. Zero is unbounded. See
	// [fakeChan.Qos] and [Broker.capacityLocked].
	conPrefetch   int
	chPrefetch    int
	unacked       map[uint64]*inflight
	unackedTags   []uint64
	outstanding   int
	nextTag       uint64
	consumers     map[string]*consumer
	consumerOrder []string
	script        ConfirmScript
	heldConfirms  []pendingConfirm
	nackNext      int
	failPublish   []error
	failDeclare   *amqp.Error
	hold          chan struct{}
}

var (
	_ client.Channel    = (*fakeChan)(nil)
	_ amqp.Acknowledger = (*fakeChan)(nil)
)

// newChannel opens a channel on a connection. The connection's own mutex is
// held.
func newChannel(c *fakeConn, id int) *fakeChan {
	c.b.mu.Lock()
	script := c.b.cfg.Confirms
	c.b.mu.Unlock()
	return &fakeChan{
		conn:      c,
		id:        id,
		gone:      make(chan struct{}),
		confirms:  newConfirms(),
		unacked:   map[uint64]*inflight{},
		consumers: map[string]*consumer{},
		script:    script,
	}
}

// broker is the channel's broker.
func (ch *fakeChan) broker() *Broker { return ch.conn.b }

// IsClosed reports whether the channel has closed.
func (ch *fakeChan) IsClosed() bool { return ch.closed.Load() }

// Close closes the channel the way a client's own Close does: no error reaches
// a `NotifyClose` listener, every notification channel is closed, and every
// delivery the channel was holding goes back to its queue.
func (ch *fakeChan) Close() error {
	ch.closeWith(nil)
	return nil
}

// closeWith closes the channel once, with a cause or without one, and does not
// return until the tear-down has happened.
//
// The tear-down runs on the connection's dispatcher and the caller waits for
// it, which is what makes the ordering the library's: every frame the broker
// had already produced for this channel is dispatched first, and only then do
// the listeners close. A close that ran on the caller's goroutine would let a
// `basic.return` that the broker produced BEFORE the close arrive after the
// channel had already been reported shut, which is a sequence no broker can
// produce and one a driver would be entitled to treat as impossible.
func (ch *fakeChan) closeWith(cause *amqp.Error) {
	b := ch.broker()
	b.mu.Lock()
	first := !ch.closed.Load()
	ch.markClosedLocked(cause)
	b.pumpLocked()
	b.mu.Unlock()
	if !first {
		return
	}
	done := make(chan struct{})
	ch.conn.post(frame{kind: frChanClose, ch: ch, err: cause, done: done}, true)
	<-done
}

// markClosedLocked takes the channel out of the broker. The broker's mutex is
// held.
//
// Requeueing the unacknowledged deliveries is the part that matters, and it is
// RabbitMQ's own behaviour: a channel that closes gives every delivery it was
// holding back to its queue, redelivered, for somebody else to take. A fake
// that dropped them would make a channel close look like a message loss, and
// one that kept them unacknowledged for ever would make it look like a leak.
// They go back in TAG order at the HEAD, so that the first delivery the
// channel was holding is the first one out again.
func (ch *fakeChan) markClosedLocked(cause *amqp.Error) {
	if ch.closed.Load() {
		return
	}
	ch.closed.Store(true)
	ch.cause = cause
	b := ch.broker()
	for i := len(ch.unackedTags) - 1; i >= 0; i-- {
		fl := ch.unacked[ch.unackedTags[i]]
		if fl == nil {
			continue
		}
		b.requeueLocked(ch, fl, SettleChannelClosed, true)
	}
	ch.unacked = map[uint64]*inflight{}
	ch.unackedTags = nil
	ch.outstanding = 0
	ch.heldConfirms = nil
	for _, tag := range ch.consumerOrder {
		con := ch.consumers[tag]
		if con == nil {
			continue
		}
		con.cancelled = true
		if con.q != nil {
			con.q.consumers = withoutConsumer(con.q.consumers, con)
		}
	}
}

// teardown closes the channel's notification channels and resolves what its
// confirm bookkeeping was holding. It runs on the connection's dispatcher.
//
// The order is the library's `Channel.shutdown` followed by
// `Channel.closeResources`: the close error is BROADCAST first, on a blocking
// send, and only then are the listener channels closed. A driver that is
// selecting on `NotifyClose` therefore receives the error rather than a closed
// channel, which is the difference between "the broker closed this with a 406"
// and "something closed".
func (ch *fakeChan) teardown(cause *amqp.Error, timeout time.Duration) {
	ch.once.Do(func() {
		if cause != nil {
			ch.notifyMu.RLock()
			listeners := ch.closes
			ch.notifyMu.RUnlock()
			notifyAll(listeners, cause, timeout, ch.conn.hard)
		}
		close(ch.gone)
		ch.notifyMu.Lock()
		for _, l := range ch.closes {
			close(l)
		}
		for _, l := range ch.returns {
			close(l)
		}
		for _, l := range ch.cancels {
			close(l)
		}
		for _, l := range ch.flows {
			close(l)
		}
		ch.closes, ch.returns, ch.cancels, ch.flows = nil, nil, nil, nil
		ch.notifyMu.Unlock()
		b := ch.broker()
		b.mu.Lock()
		outs := make([]chan amqp.Delivery, 0, len(ch.consumerOrder))
		for _, tag := range ch.consumerOrder {
			if con := ch.consumers[tag]; con != nil && !con.outClosed {
				con.outClosed = true
				outs = append(outs, con.out)
			}
		}
		b.mu.Unlock()
		for _, out := range outs {
			close(out)
		}
		// `confirms.Close` last, because it is the one that nacks what was
		// outstanding and closes `NotifyPublish`. A driver that reads the
		// close error first and the confirm channel second sees them in that
		// order.
		ch.confirms.close()
	})
}

// closeConsumer closes one consumer's delivery channel. It runs on the
// dispatcher, which is also the only goroutine that sends on it, so nothing
// here can race a send.
func (ch *fakeChan) closeConsumer(tag string) {
	b := ch.broker()
	b.mu.Lock()
	con := ch.consumers[tag]
	if con == nil || con.outClosed {
		b.mu.Unlock()
		return
	}
	con.outClosed = true
	out := con.out
	b.mu.Unlock()
	close(out)
}

// begin is the preamble every synchronous operation shares: refuse a closed
// channel the way the library's sendClosed does, then bring the broker's
// deadlines up to date.
//
// The deadline sweep is here rather than on a timer so that every operation
// sees a broker whose TTLs have already fired. See [Broker.expireLocked].
func (ch *fakeChan) begin() *amqp.Error {
	b := ch.broker()
	if b.closed || ch.closed.Load() {
		return amqp.ErrClosed
	}
	b.expireLocked()
	return nil
}

// Confirm puts the channel into publisher-confirm mode.
//
// From here the publications get consecutive delivery tags from 1 and
// `GetNextPublishSeqNo` is one more than the last one issued. RabbitMQ's own
// `confirm.select` is idempotent and so is this.
func (ch *fakeChan) Confirm(_ bool) error {
	b := ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ch.begin(); err != nil {
		return err
	}
	ch.confirming = true
	return nil
}

// Qos sets the prefetch.
//
// global=false is RabbitMQ's per-consumer prefetch and is snapshotted by each
// `basic.consume` that follows, which is the broker's behaviour: a qos changes
// what NEW consumers are given and leaves the running ones alone. global=true
// bounds the whole channel, which is the other of the two readings of
// `Qos(prefetch)` and the one a driver with several consumers on one channel
// has to get right.
//
// The two readings are held separately and BOTH stay in force: a global qos
// does not replace the per-consumer one, nor the other way round.
// rabbitmq.com/docs/consumer-prefetch, "Multiple Consumers Sharing the
// Limit": "RabbitMQ interprets this as meaning that the two prefetch limits
// should be enforced independently of each other; consumers will only receive
// new messages when neither limit on unacknowledged messages has been
// reached." One storage slot for both readings is how this fake let a
// consumer with a per-consumer prefetch of one hold two after a later
// qos(3, global=true) -- crasher #75, which [FuzzRabbitFake] reported against
// the harness because the fake, not the oracle, was the thing that deviated.
//
// A qos does NOT recall deliveries that are already out. Lowering the prefetch
// below the number a consumer is holding leaves it holding them and stops the
// broker sending more until it drops back under the new limit, which is the
// only thing a broker can do -- a delivery that has left cannot be unsent.
// [FuzzRabbitFake] found this by lowering the prefetch under two outstanding
// deliveries and calling it a breach; the invariant is about what the broker
// HANDS OUT, not about what a later qos makes of what it already handed.
func (ch *fakeChan) Qos(prefetchCount, _ int, global bool) error {
	b := ch.broker()
	b.mu.Lock()
	if err := ch.begin(); err != nil {
		b.mu.Unlock()
		return err
	}
	if prefetchCount < 0 {
		b.mu.Unlock()
		e := newError(amqp.PreconditionFailed, "PRECONDITION_FAILED - prefetch count must not be negative")
		ch.closeWith(e)
		return e
	}
	if global {
		ch.chPrefetch = prefetchCount
	} else {
		ch.conPrefetch = prefetchCount
	}
	b.pumpLocked()
	b.mu.Unlock()
	return nil
}

// declare runs one topology operation and turns its refusal into a channel
// exception, which is what a refused declaration IS on AMQP 0-9-1.
//
// That is the whole reason `internal/rabbit` validates a name before the first
// frame: the refusal closes the channel, and on a confirm channel a channel
// close takes every publication whose confirm has not arrived with it. A fake
// that returned the error and left the channel open would make that
// consequence unreachable.
func (ch *fakeChan) declare(fn func() *amqp.Error) error {
	b := ch.broker()
	if g := b.declareGate(); g != nil {
		// Before the lock, so that a test can read the broker's own state
		// while this declaration is parked.
		g.enter()
	}
	b.mu.Lock()
	if err := ch.begin(); err != nil {
		b.mu.Unlock()
		return err
	}
	if e := ch.failDeclare; e != nil {
		ch.failDeclare = nil
		b.mu.Unlock()
		ch.closeWith(e)
		return e
	}
	e := fn()
	b.pumpLocked()
	b.mu.Unlock()
	if e != nil {
		ch.closeWith(e)
		return e
	}
	return nil
}

// ExchangeDeclare declares an exchange.
func (ch *fakeChan) ExchangeDeclare(
	name, kind string,
	durable, autoDelete, internal, _ bool,
	args amqp.Table,
) error {
	return ch.declare(func() *amqp.Error {
		return ch.broker().declareExchange(name, kind, durable, autoDelete, internal, args)
	})
}

// ExchangeDeclarePassive asserts that an exchange exists without creating it.
// An exchange that is not there is a 404 that closes the channel.
func (ch *fakeChan) ExchangeDeclarePassive(
	name, _ string,
	_, _, _, _ bool,
	_ amqp.Table,
) error {
	return ch.declare(func() *amqp.Error {
		return ch.broker().declareExchangePassive(name)
	})
}

// ExchangeDelete deletes an exchange.
func (ch *fakeChan) ExchangeDelete(name string, ifUnused, _ bool) error {
	return ch.declare(func() *amqp.Error {
		return ch.broker().deleteExchange(name, ifUnused)
	})
}

// QueueDeclare declares a queue and reports its message and consumer counts.
func (ch *fakeChan) QueueDeclare(
	name string,
	durable, autoDelete, exclusive, _ bool,
	args amqp.Table,
) (amqp.Queue, error) {
	return ch.queueDeclare(name, durable, autoDelete, exclusive, false, args)
}

// QueueDeclarePassive asserts that a queue exists without creating it, which is the specification's "no hidden creation" made checkable. A queue that is not there is a
// 404 that closes the channel.
func (ch *fakeChan) QueueDeclarePassive(
	name string,
	durable, autoDelete, exclusive, _ bool,
	args amqp.Table,
) (amqp.Queue, error) {
	return ch.queueDeclare(name, durable, autoDelete, exclusive, true, args)
}

// queueDeclare is the shared half of the two declares.
func (ch *fakeChan) queueDeclare(
	name string,
	durable, autoDelete, exclusive, passive bool,
	args amqp.Table,
) (amqp.Queue, error) {
	var q amqp.Queue
	err := ch.declare(func() *amqp.Error {
		var e *amqp.Error
		q, e = ch.broker().declareQueue(ch.conn.id, name, durable, autoDelete, exclusive, passive, args)
		return e
	})
	if err != nil {
		return amqp.Queue{}, err
	}
	return q, nil
}

// QueueBind binds a queue to an exchange.
func (ch *fakeChan) QueueBind(name, key, exchange string, _ bool, args amqp.Table) error {
	return ch.declare(func() *amqp.Error {
		return ch.broker().bindQueue(ch.conn.id, name, key, exchange, args)
	})
}

// QueueDelete deletes a queue and reports how many messages went with it.
func (ch *fakeChan) QueueDelete(name string, ifUnused, ifEmpty, _ bool) (int, error) {
	var n int
	err := ch.declare(func() *amqp.Error {
		var e *amqp.Error
		n, e = ch.broker().deleteQueue(ch.conn.id, name, ifUnused, ifEmpty)
		return e
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// QueuePurge empties a queue and reports how many messages it discarded.
func (ch *fakeChan) QueuePurge(name string, _ bool) (int, error) {
	var n int
	err := ch.declare(func() *amqp.Error {
		var e *amqp.Error
		n, e = ch.broker().purgeQueue(ch.conn.id, name)
		return e
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetNextPublishSeqNo is the delivery tag the next publication on this channel
// will be given, which is the library's own counter and not the broker's.
func (ch *fakeChan) GetNextPublishSeqNo() uint64 { return ch.confirms.nextSeqNo() }

// PublishWithContext publishes one message.
//
// Nothing here returns a broker's refusal, because `basic.publish` has no
// reply: a publication the broker will not take produces a channel exception
// (an exchange that is not there), a `basic.return` (mandatory and unroutable)
// or a `basic.nack` (a bounded queue that is full), and all three of them
// arrive later and elsewhere. The only errors this can return are the
// library's own -- a closed channel, a cancelled context, a write that failed
// -- which is exactly the distinction a driver's publish path has to make.
func (ch *fakeChan) PublishWithContext(
	ctx context.Context,
	exchange, key string,
	mandatory, immediate bool,
	msg amqp.Publishing,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ch.waitForHold(ctx); err != nil {
		return err
	}
	b := ch.broker()
	b.mu.Lock()
	if err := ch.begin(); err != nil {
		b.mu.Unlock()
		return err
	}
	if len(ch.failPublish) > 0 {
		err := ch.failPublish[0]
		ch.failPublish = ch.failPublish[1:]
		if ch.confirming {
			// The library counts the publication BEFORE it writes and hands
			// the tag back when the write fails, which is the one place its
			// counter moves backwards. Reproducing it is what makes the
			// `GetNextPublishSeqNo` assertion on the seam able to fail.
			ch.confirms.publish()
			ch.confirms.unpublish()
		}
		b.mu.Unlock()
		return err
	}
	b.seq++
	m := &message{id: b.seq, exchange: exchange, routingKey: key, pub: msg}
	m.pub.Headers = cloneTable(msg.Headers)
	if msg.Body != nil {
		m.pub.Body = append([]byte(nil), msg.Body...)
	}
	var tag uint64
	if ch.confirming {
		tag = ch.confirms.publish()
	}
	if immediate {
		// RabbitMQ has never implemented `immediate`. It is a refusal rather
		// than an ignored flag because a driver that set it would otherwise
		// pass here and fail against every real broker. The tag has already
		// been issued -- the library counts before it writes -- so the
		// channel close is what resolves it.
		b.drops = append(b.drops, Drop{Message: publicationOf(m), Reason: DropRefused})
		b.mu.Unlock()
		ch.closeWith(newError(amqp.NotImplemented, "NOT_IMPLEMENTED - immediate=true"))
		return nil
	}
	routed, accepted, e := b.publishLocked(exchange, key, m)
	if e != nil {
		// The message is discarded and the discard is recorded, for the
		// reason [DropRefused] gives: the call returns nil, so nothing else
		// in the system knows this publication is gone.
		b.drops = append(b.drops, Drop{Message: publicationOf(m), Reason: DropRefused})
		b.mu.Unlock()
		// The publication is NOT unpublished: the broker took the frame and
		// then raised an exception, so the tag exists and is outstanding, and
		// the channel close is what resolves it. That is the ambiguity
		// arriving exactly as the library produces it.
		ch.closeWith(e)
		return nil
	}
	if routed == 0 && mandatory {
		// The return goes in FIRST and the confirm behind it, which is the
		// order on the wire and the order the library's reader dispatches
		// them in. A fake that enqueued them the other way round would let a
		// driver that reads whichever is ready first pass.
		ch.enqueueReturn(m, exchange, key)
	}
	if ch.confirming {
		ch.confirmPublicationLocked(tag, accepted)
	}
	b.pumpLocked()
	b.mu.Unlock()
	return nil
}

// publishLocked routes one publication, holding it back when the exchange is a
// delayed-message exchange and the publication asked for a delay.
func (b *Broker) publishLocked(exName, key string, m *message) (routed int, accepted bool, err *amqp.Error) {
	if ex := b.exchangeLocked(exName); ex != nil && ex.kind == ExchangeDelayed {
		if ms, ok := intArg(m.pub.Headers, DelayHeader); ok && ms > 0 {
			m.expiry = b.nowLocked().Add(time.Duration(ms) * time.Millisecond)
			at := len(ex.held)
			for i, other := range ex.held {
				if other.expiry.After(m.expiry) {
					at = i
					break
				}
			}
			ex.held = append(ex.held, nil)
			copy(ex.held[at+1:], ex.held[at:])
			ex.held[at] = m
			// A held publication counts as routed. The plugin accepts the
			// message into the exchange and routes it when the delay elapses,
			// so `mandatory` cannot be answered at publish time and the
			// broker does not answer it.
			return 1, true, nil
		}
	}
	targets, e := b.routeLocked(exName, key, m)
	if e != nil {
		return 0, false, e
	}
	if len(targets) == 0 {
		// An unroutable publication is discarded, and the discard is
		// RECORDED. A broker is allowed to throw it away -- `mandatory` is
		// the only thing that makes it say so -- and a fake that threw it
		// away silently would make [FuzzRabbitFake]'s "no message is lost"
		// invariant uncheckable, because a lost message and a legitimately
		// dropped one would look the same.
		b.drops = append(b.drops, Drop{Message: publicationOf(m), Reason: DropUnroutable})
		return 0, true, nil
	}
	accepted = true
	for _, q := range targets {
		if !b.enqueueLocked(q, m) {
			accepted = false
		}
	}
	return len(targets), accepted, nil
}

// enqueueReturn posts the `basic.return` for an unroutable mandatory
// publication. The broker's mutex is held.
//
// The Headers are the publication's own, because that is what the driver
// correlates on: a return carries no delivery tag, so the only way to know
// WHICH publication came back is a header the publisher put there.
func (ch *fakeChan) enqueueReturn(m *message, exchange, key string) {
	ret := amqp.Return{
		ReplyCode:  amqp.NoRoute,
		ReplyText:  "NO_ROUTE",
		Exchange:   exchange,
		RoutingKey: key,

		Headers:         cloneTable(m.pub.Headers),
		ContentType:     m.pub.ContentType,
		ContentEncoding: m.pub.ContentEncoding,
		DeliveryMode:    m.pub.DeliveryMode,
		Priority:        m.pub.Priority,
		CorrelationId:   m.pub.CorrelationId,
		ReplyTo:         m.pub.ReplyTo,
		Expiration:      m.pub.Expiration,
		MessageId:       m.pub.MessageId,
		Timestamp:       m.pub.Timestamp,
		Type:            m.pub.Type,
		UserId:          m.pub.UserId,
		AppId:           m.pub.AppId,
		Body:            append([]byte(nil), m.pub.Body...),
	}
	ch.conn.enqueue(frame{kind: frReturn, ch: ch, ret: ret})
}

// confirmPublicationLocked frames the confirm for one publication according to
// the channel's script. The broker's mutex is held.
func (ch *fakeChan) confirmPublicationLocked(tag uint64, accepted bool) {
	ack := accepted
	if ch.nackNext > 0 {
		ch.nackNext--
		ack = false
	}
	switch ch.script.Style {
	case ConfirmManual:
		return
	case ConfirmCumulative, ConfirmReversed:
		ch.heldConfirms = append(ch.heldConfirms, pendingConfirm{tag: tag, ack: ack})
		if len(ch.heldConfirms) >= ch.script.batch() {
			ch.flushConfirmsLocked()
		}
	default:
		ch.postConfirm(tag, ack, false)
	}
}

// flushConfirmsLocked sends the confirms a script has been holding.
//
// A cumulative flush is ONE `basic.ack{multiple=true}` naming the highest tag,
// which is what the library expands per tag -- unless the group is mixed, in
// which case it cannot be cumulative at all: `multiple` carries one ack flag
// for the whole range, so a broker with a nack in the middle of a batch sends
// separate frames and so does this.
func (ch *fakeChan) flushConfirmsLocked() {
	held := ch.heldConfirms
	ch.heldConfirms = nil
	if len(held) == 0 {
		return
	}
	if ch.script.Style == ConfirmReversed {
		for i := len(held) - 1; i >= 0; i-- {
			ch.postConfirm(held[i].tag, held[i].ack, false)
		}
		return
	}
	uniform, top := true, held[0].tag
	for _, p := range held {
		if p.ack != held[0].ack {
			uniform = false
		}
		if p.tag > top {
			top = p.tag
		}
	}
	if uniform {
		ch.postConfirm(top, held[0].ack, true)
		return
	}
	for _, p := range held {
		ch.postConfirm(p.tag, p.ack, false)
	}
}

// postConfirm enqueues one `basic.ack` or `basic.nack`.
func (ch *fakeChan) postConfirm(tag uint64, ack, multiple bool) {
	ch.conn.enqueue(frame{
		kind:     frConfirm,
		ch:       ch,
		conf:     amqp.Confirmation{DeliveryTag: tag, Ack: ack},
		multiple: multiple,
	})
}

// waitForHold parks a publication that [ChannelHandle.HoldNextPublish] has
// caught, until the hold is released, the caller's context is done or the
// channel goes.
func (ch *fakeChan) waitForHold(ctx context.Context) error {
	b := ch.broker()
	b.mu.Lock()
	hold := ch.hold
	ch.hold = nil
	b.mu.Unlock()
	if hold == nil {
		return nil
	}
	select {
	case <-hold:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-ch.gone:
		return amqp.ErrClosed
	}
}

// ConsumeWithContext starts a consumer and returns its delivery stream.
func (ch *fakeChan) ConsumeWithContext(
	ctx context.Context,
	queueName, consumerTag string,
	autoAck, _, _, _ bool,
	_ amqp.Table,
) (<-chan amqp.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := ch.broker()
	b.mu.Lock()
	if err := ch.begin(); err != nil {
		b.mu.Unlock()
		return nil, err
	}
	q := b.queues[queueName]
	if q == nil && isReplyPseudoQueue(queueName) {
		// Direct reply-to. There is no queue on the real broker either: the
		// name is a pseudo-queue a client CONSUMES from and can never
		// declare, and a reply published to it is routed straight back over
		// this consumer. Conjuring a queue here is how that is modelled
		// without a second delivery path -- the default exchange already
		// binds every queue by its own name, so the reply routes with no
		// further help.
		//
		// `no-ack` is mandatory and the refusal is the broker's: there is no
		// queue to hold an unacknowledged reply in, so a consumer that asks
		// for explicit settlement is rejected rather than quietly given
		// at-least-once semantics it cannot have.
		if !autoAck {
			b.mu.Unlock()
			e := newError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - reply consumer cannot acknowledge")
			ch.closeWith(e)
			return nil, e
		}
		var e *amqp.Error
		if q, e = b.newQueue(ch.conn.id, queueName, false, true, true, nil); e != nil {
			b.mu.Unlock()
			ch.closeWith(e)
			return nil, e
		}
		b.queues[queueName] = q
		b.qOrder = append(b.qOrder, queueName)
	}
	if q == nil {
		b.mu.Unlock()
		e := newError(amqp.NotFound, "NOT_FOUND - no queue '"+queueName+"' in vhost '/'")
		ch.closeWith(e)
		return nil, e
	}
	if e := b.checkExclusive(q, ch.conn.id); e != nil {
		b.mu.Unlock()
		ch.closeWith(e)
		return nil, e
	}
	if consumerTag == "" {
		consumerTag = b.consumerTag()
	}
	if _, dup := ch.consumers[consumerTag]; dup {
		b.mu.Unlock()
		e := newError(amqp.NotAllowed, "NOT_ALLOWED - attempt to reuse consumer tag '"+consumerTag+"'")
		ch.closeWith(e)
		return nil, e
	}
	// The per-consumer reading only, and it is NOT cleared by a global qos:
	// the channel-wide reading is a second, independent bound that
	// [Broker.capacityLocked] applies on top of this snapshot.
	prefetch := ch.conPrefetch
	con := &consumer{
		tag:      consumerTag,
		ch:       ch,
		q:        q,
		autoAck:  autoAck,
		out:      make(chan amqp.Delivery, b.cfg.ConsumeBuffer),
		prefetch: prefetch,
	}
	ch.consumers[consumerTag] = con
	ch.consumerOrder = append(ch.consumerOrder, consumerTag)
	q.consumers = append(q.consumers, con)
	q.lastUse = b.nowLocked()
	b.pumpLocked()
	b.mu.Unlock()
	if ctx.Done() != nil {
		// The library spawns exactly this goroutine, and it cancels the
		// consumer when the caller's context ends -- the specification's "a consume the
		// caller can cancel". It is skipped when the context can never be
		// done, because a goroutine selecting on a nil channel and a closed
		// channel is a goroutine goleak would find and the library only gets
		// away with it because its own close channel releases it.
		//
		// The registration goes through [fakeConn.goIfLive] and not through
		// `c.wg.Go`, because `b.mu` was released one line above and the whole
		// of a connection close -- `drain`'s `wg.Wait()` included -- can run
		// between there and here. That is the WaitGroup misuse goIfLive
		// documents, and it is what `FuzzRabbitTagSettlement`'s seed
		// 6dad9459895ffff8 reports as a data race between
		// `source.attachLocked` and `fakeConn.drain` (issue 76).
		//
		// A refusal costs nothing: the connection has drained, so its
		// dispatcher has already run `teardown` for every channel on it --
		// `ch.gone` is closed and `markClosedLocked` has cancelled this
		// consumer -- and the goroutine would have taken the `ch.gone` branch
		// and returned immediately.
		ch.conn.goIfLive(func() {
			select {
			case <-ctx.Done():
				_ = ch.Cancel(consumerTag, false)
			case <-ch.gone:
			}
		})
	}
	return con.out, nil
}

// Cancel stops a consumer by tag.
//
// A client-initiated cancel closes the delivery channel and sends NOTHING to a
// `NotifyCancel` listener: that listener is for a cancel the BROKER decided,
// which is what [ChannelHandle.CancelConsumer] and a queue deletion produce. A
// fake that notified both would let a driver treat its own cancel as the
// broker's and tear a subscription down twice.
func (ch *fakeChan) Cancel(consumerTag string, _ bool) error {
	b := ch.broker()
	b.mu.Lock()
	if err := ch.begin(); err != nil {
		b.mu.Unlock()
		return err
	}
	con := ch.consumers[consumerTag]
	if con == nil || con.cancelled {
		b.mu.Unlock()
		return nil
	}
	con.cancelled = true
	if con.q != nil {
		con.q.consumers = withoutConsumer(con.q.consumers, con)
	}
	b.pumpLocked()
	b.mu.Unlock()
	ch.conn.enqueue(frame{kind: frCancel, ch: ch, tag: consumerTag, notify: false})
	return nil
}

// enqueueCancel posts a broker-side `basic.cancel`, which is what reaches a
// `NotifyCancel` listener. The broker's mutex is held.
func (ch *fakeChan) enqueueCancel(tag string) {
	ch.conn.enqueue(frame{kind: frCancel, ch: ch, tag: tag, notify: true})
}

// Ack acknowledges a delivery, and every earlier one when multiple is set.
func (ch *fakeChan) Ack(tag uint64, multiple bool) error {
	return ch.settle(tag, multiple, SettleAck, false)
}

// Nack refuses a delivery, requeueing it or dead-lettering it.
func (ch *fakeChan) Nack(tag uint64, multiple, requeue bool) error {
	return ch.settle(tag, multiple, SettleNack, requeue)
}

// Reject refuses one delivery. It is `basic.nack` without the multiple flag,
// which is the older frame and the one a driver reaches for when it has
// nothing to batch.
func (ch *fakeChan) Reject(tag uint64, requeue bool) error {
	return ch.settle(tag, false, SettleReject, requeue)
}

// settle is the one settlement path.
//
// The two refusals it can produce are different things and a driver has to
// tell them apart. A settlement on a channel that has CLOSED returns
// [amqp091.ErrClosed] from the call itself, because the library's sendClosed
// refuses to write anything but `channel.close-ok`. A settlement naming a tag
// the channel never issued -- or one it issued and has already settled --
// returns NIL and then closes the channel with 406 PRECONDITION_FAILED,
// because `basic.ack` has no reply and the broker's only way to object is a
// channel exception. So "settling twice is an error" is true, and the error
// does not come back from the call that made it.
func (ch *fakeChan) settle(tag uint64, multiple bool, kind SettleKind, requeue bool) error {
	b := ch.broker()
	b.mu.Lock()
	if err := ch.begin(); err != nil {
		b.mu.Unlock()
		return err
	}
	tags, ok := ch.tagsForLocked(tag, multiple)
	if !ok {
		b.mu.Unlock()
		ch.closeWith(newError(amqp.PreconditionFailed,
			"PRECONDITION_FAILED - unknown delivery tag "+strconv.FormatUint(tag, 10)))
		return nil
	}
	for _, t := range tags {
		fl := ch.unacked[t]
		if fl == nil {
			continue
		}
		switch {
		case kind == SettleAck:
			b.completeLocked(ch, fl, kind, multiple)
		case requeue:
			b.requeueLocked(ch, fl, kind, multiple)
		default:
			b.rejectLocked(ch, fl, kind, multiple)
		}
	}
	b.pumpLocked()
	b.mu.Unlock()
	return nil
}

// tagsForLocked works out which deliveries one settlement frame names.
//
// `multiple` with tag 0 is AMQP's "everything outstanding" and is never a
// refusal, even when nothing is outstanding. Any other tag has to be one the
// channel is currently holding: a tag it never issued and a tag it has already
// settled are the same thing to a broker, which is why settling twice is a
// 406.
func (ch *fakeChan) tagsForLocked(tag uint64, multiple bool) ([]uint64, bool) {
	if multiple && tag == 0 {
		return append([]uint64(nil), ch.unackedTags...), true
	}
	if _, held := ch.unacked[tag]; !held {
		return nil, false
	}
	if !multiple {
		return []uint64{tag}, true
	}
	out := make([]uint64, 0, len(ch.unackedTags))
	for _, t := range ch.unackedTags {
		if t <= tag {
			out = append(out, t)
		}
	}
	return out, true
}

// forgetLocked takes a delivery out of the channel's in-flight set.
func (ch *fakeChan) forgetLocked(fl *inflight) {
	delete(ch.unacked, fl.tag)
	ch.unackedTags = withoutTag(ch.unackedTags, fl.tag)
	ch.outstanding--
	fl.q.unacked--
	if fl.con != nil {
		fl.con.outstanding--
	}
	fl.q.lastUse = ch.broker().nowLocked()
}

// completeLocked acknowledges a delivery: the message is gone.
func (b *Broker) completeLocked(ch *fakeChan, fl *inflight, kind SettleKind, multiple bool) {
	ch.forgetLocked(fl)
	b.settlements = append(b.settlements, Settlement{
		Delivery: fl.id, Channel: ch.id, Tag: fl.tag, Queue: fl.q.name,
		Message: publicationOf(fl.msg), Kind: kind, Multiple: multiple,
	})
}

// requeueLocked puts a delivery back at the head of its queue, redelivered.
//
// The head is the point. The immediate nack asks for the message BACK, and
// a broker that put it at the tail would hand the caller every other message
// in the queue first -- which for a queue with a backlog is not a retry, it is
// a deferral of unbounded length. RabbitMQ puts it at the head and so does
// this.
func (b *Broker) requeueLocked(ch *fakeChan, fl *inflight, kind SettleKind, multiple bool) {
	ch.forgetLocked(fl)
	fl.msg.redelivered = true
	fl.msg.attempts++
	b.settlements = append(b.settlements, Settlement{
		Delivery: fl.id, Channel: ch.id, Tag: fl.tag, Queue: fl.q.name,
		Message: publicationOf(fl.msg), Kind: kind, Requeue: true, Multiple: multiple,
	})
	if fl.q.deleted {
		// The queue went away while the delivery was out. There is nowhere to
		// put the message back, so it is a drop with a record rather than a
		// silent loss.
		b.drops = append(b.drops, Drop{
			Message: publicationOf(fl.msg), Queue: fl.q.name, Reason: DropQueueDeleted,
		})
		return
	}
	b.insertReady(fl.q, fl.msg, true)
}

// rejectLocked refuses a delivery without requeueing it, which dead-letters it
// when the queue names an `x-dead-letter-exchange` and drops it otherwise.
func (b *Broker) rejectLocked(ch *fakeChan, fl *inflight, kind SettleKind, multiple bool) {
	ch.forgetLocked(fl)
	fl.msg.attempts++
	b.settlements = append(b.settlements, Settlement{
		Delivery: fl.id, Channel: ch.id, Tag: fl.tag, Queue: fl.q.name,
		Message: publicationOf(fl.msg), Kind: kind, Multiple: multiple,
	})
	b.deadLetterLocked(fl.q, fl.msg, DeathRejected)
}

// pumpLocked hands out every message it can, to every consumer with capacity.
//
// It runs at the end of every operation that could have created capacity or
// work: a publication, a settlement, a consume, a qos, a clock advance. That
// is deliberately more often than necessary -- the alternative is a wake-up
// rule per operation, and a missed wake-up is a message that sits in a queue
// with an idle consumer next to it, which is the hardest class of bug to see
// in a queue and the easiest to write.
func (b *Broker) pumpLocked() {
	for _, name := range b.qOrder {
		q := b.queues[name]
		if q == nil {
			continue
		}
		for len(q.ready) > 0 {
			con := b.pickConsumerLocked(q)
			if con == nil {
				break
			}
			m := q.ready[0]
			q.ready = q.ready[1:]
			b.handOutLocked(q, con, m)
		}
	}
}

// pickConsumerLocked is whose turn it is: the next consumer round the ring
// with capacity under its prefetch, or nil when nobody has any.
//
// Round-robin rather than first-with-capacity, because RabbitMQ's is
// round-robin and the difference is visible: with a prefetch of one and two
// consumers, first-with-capacity delivers everything to the first consumer
// that acknowledges quickly, which makes a two-consumer test pass while
// proving nothing about competition.
func (b *Broker) pickConsumerLocked(q *queue) *consumer {
	n := len(q.consumers)
	if n == 0 {
		return nil
	}
	for i := range n {
		idx := (q.nextConsumer + i) % n
		con := q.consumers[idx]
		if con.cancelled || con.ch.closed.Load() {
			continue
		}
		if !b.capacityLocked(con) {
			continue
		}
		q.nextConsumer = (idx + 1) % n
		return con
	}
	return nil
}

// capacityLocked reports whether one consumer may be given another delivery.
//
// BOTH readings have to have room, which is RabbitMQ's rule when a channel has
// set each of them: "consumers will only receive new messages when neither
// limit on unacknowledged messages has been reached"
// (rabbitmq.com/docs/consumer-prefetch). Picking one reading and ignoring the
// other -- which is what this did while the two shared one field -- lets a
// global qos silently widen a per-consumer bound that is still in force.
func (b *Broker) capacityLocked(con *consumer) bool {
	if con.autoAck {
		// An auto-acknowledging consumer holds nothing unsettled, so the
		// prefetch has nothing to bound. RabbitMQ ignores the prefetch for
		// `no-ack` consumers for exactly that reason.
		return true
	}
	if con.prefetch > 0 && con.outstanding >= con.prefetch {
		return false
	}
	if con.ch.chPrefetch > 0 && con.ch.outstanding >= con.ch.chPrefetch {
		return false
	}
	return true
}

// handOutLocked builds one `basic.deliver` and enqueues it.
func (b *Broker) handOutLocked(q *queue, con *consumer, m *message) {
	ch := con.ch
	b.delivery++
	ch.nextTag++
	tag := ch.nextTag
	d := amqp.Delivery{
		Acknowledger: ch,
		Headers:      b.deliveryHeaders(q, m),

		ContentType:     m.pub.ContentType,
		ContentEncoding: m.pub.ContentEncoding,
		DeliveryMode:    m.pub.DeliveryMode,
		Priority:        m.pub.Priority,
		CorrelationId:   m.pub.CorrelationId,
		ReplyTo:         m.pub.ReplyTo,
		Expiration:      m.pub.Expiration,
		MessageId:       m.pub.MessageId,
		Timestamp:       m.pub.Timestamp,
		Type:            m.pub.Type,
		UserId:          m.pub.UserId,
		AppId:           m.pub.AppId,

		ConsumerTag: con.tag,
		DeliveryTag: tag,
		Redelivered: m.redelivered,
		Exchange:    m.exchange,
		RoutingKey:  m.routingKey,
		Body:        append([]byte(nil), m.pub.Body...),
	}
	id := b.delivery
	if con.autoAck {
		// `no-ack`: the message leaves the broker at the moment it is handed
		// out, which is the at-most-once and the reason a delivery frame
		// dropped by a closing channel is a loss rather than a requeue. The
		// settlement is recorded here so that the message is still accounted
		// for -- [FuzzRabbitFake]'s "no message is lost" invariant reads it.
		b.settlements = append(b.settlements, Settlement{
			Delivery: id, Channel: ch.id, Tag: tag, Queue: q.name,
			Message: publicationOf(m), Kind: SettleAutoAck,
		})
	} else {
		q.unacked++
		con.outstanding++
		ch.outstanding++
		ch.unacked[tag] = &inflight{id: id, tag: tag, msg: m, q: q, con: con}
		ch.unackedTags = append(ch.unackedTags, tag)
	}
	ch.conn.enqueue(frame{kind: frDeliver, ch: ch, delivery: d, out: con.out})
}

// deliveryHeaders is the delivery's header table, `x-delivery-count` included
// or excluded.
//
// The asymmetry is the specification's `(verify)` and the driver's capability table depends
// on it: a quorum queue exposes the count and a classic queue has no such
// thing. It appears from the SECOND delivery onwards, because the first
// delivery has no failed attempts behind it and a real broker omits the header
// then -- so a driver reading a missing header as "attempt 1" is reading it
// correctly, and a test for that needs the header to be genuinely absent.
func (b *Broker) deliveryHeaders(q *queue, m *message) amqp.Table {
	h := cloneTable(m.pub.Headers)
	var want bool
	switch b.cfg.DeliveryCount {
	case DeliveryCountNever:
		want = false
	case DeliveryCountAlways:
		want = true
	default:
		want = q.quorum && m.attempts > 0
	}
	name := b.cfg.DeliveryCountName
	if name == "" {
		name = DeliveryCountHeader
	}
	if !want {
		delete(h, DeliveryCountHeader)
		delete(h, AcquiredCountHeader)
		return h
	}
	if h == nil {
		h = amqp.Table{}
	}
	h[name] = int64(m.attempts)
	return h
}

// NotifyClose registers a listener for this channel's close, which is where a
// channel exception arrives.
func (ch *fakeChan) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	if ch.register(func() { ch.closes = append(ch.closes, c) }) {
		return c
	}
	close(c)
	return c
}

// NotifyReturn registers a listener for `basic.return`. The channel should be
// UNBUFFERED; the seam's own comment says why.
func (ch *fakeChan) NotifyReturn(c chan amqp.Return) chan amqp.Return {
	if ch.register(func() { ch.returns = append(ch.returns, c) }) {
		return c
	}
	close(c)
	return c
}

// NotifyCancel registers a listener for a consumer the BROKER cancelled.
func (ch *fakeChan) NotifyCancel(c chan string) chan string {
	if ch.register(func() { ch.cancels = append(ch.cancels, c) }) {
		return c
	}
	close(c)
	return c
}

// NotifyFlow registers a listener for `channel.flow`.
func (ch *fakeChan) NotifyFlow(c chan bool) chan bool {
	if ch.register(func() { ch.flows = append(ch.flows, c) }) {
		return c
	}
	close(c)
	return c
}

// NotifyPublish registers a listener for publisher confirms, expanded per tag
// and resequenced by the confirm bookkeeping.
func (ch *fakeChan) NotifyPublish(c chan amqp.Confirmation) chan amqp.Confirmation {
	if ch.closed.Load() {
		close(c)
		return c
	}
	ch.confirms.listen(c)
	return c
}

// register adds a listener unless the channel has already closed, which is the
// library's `noNotify`: a listener registered on a dead channel is handed back
// CLOSED rather than never written to, so a driver's range over it terminates.
func (ch *fakeChan) register(add func()) bool {
	if ch.closed.Load() {
		return false
	}
	ch.notifyMu.Lock()
	defer ch.notifyMu.Unlock()
	if ch.closed.Load() {
		return false
	}
	add()
	return true
}

// withoutConsumer removes one consumer from a queue's list, keeping the order
// of the rest so that the round-robin cursor still means something.
func withoutConsumer(list []*consumer, con *consumer) []*consumer {
	out := list[:0]
	for _, c := range list {
		if c != con {
			out = append(out, c)
		}
	}
	return out
}

// withoutTag removes one tag from the sorted in-flight list.
func withoutTag(tags []uint64, tag uint64) []uint64 {
	i := sort.Search(len(tags), func(i int) bool { return tags[i] >= tag })
	if i < len(tags) && tags[i] == tag {
		return append(tags[:i], tags[i+1:]...)
	}
	return tags
}

// ReplyPseudoQueue is RabbitMQ's direct reply-to address. A client consumes
// from this name and never declares it; the driver publishes a request whose
// `reply_to` is this name, and the reply comes back over the consumer.
const ReplyPseudoQueue = "amq.rabbitmq.reply-to"

// isReplyPseudoQueue reports whether a name is the direct reply-to address or
// one of the per-request addresses the broker mints beneath it.
func isReplyPseudoQueue(name string) bool {
	if name == ReplyPseudoQueue {
		return true
	}
	const prefix = ReplyPseudoQueue + "."
	return strings.HasPrefix(name, prefix)
}

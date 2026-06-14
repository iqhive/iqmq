package rabbitfake

import (
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"bitbucket.org/iqhive/iqmq/drivers/rabbitmq/internal/client"
)

// frameKind is which frame a dispatcher is delivering.
type frameKind uint8

// The frames the dispatcher carries. There is one queue per connection and
// they all travel in it, because the ORDER between them is the behaviour: a
// `basic.return` and the `basic.ack` that follows it are two frames on two Go
// channels, and the only thing that makes the driver see them in the right
// order is that one goroutine sent them in the order the broker produced them.
const (
	frDeliver frameKind = iota
	frReturn
	frConfirm
	frCancel
	frFlow
	frChanClose
	frConnClose
	frBlocking
	frBarrier
)

// frame is one thing the broker has to tell a client, in wire order.
type frame struct {
	kind frameKind
	ch   *fakeChan

	delivery amqp.Delivery
	ret      amqp.Return
	conf     amqp.Confirmation
	multiple bool
	tag      string
	active   bool
	notify   bool
	err      *amqp.Error
	blocking amqp.Blocking

	// out is the consumer's delivery channel for a `basic.deliver`. It is on
	// the frame rather than looked up when the frame is dispatched, because
	// by then the consumer may be gone and the delivery still has to reach
	// the channel that was handed to the driver.
	out chan amqp.Delivery

	// done is closed once the frame has been dispatched. It is what makes
	// [Broker.Sync] a barrier and what makes a close synchronous, so that a
	// test never has to poll for a tear-down that has already been decided.
	done chan struct{}
}

// fakeConn is one AMQP connection.
//
// The dispatcher is the whole design of this type. amqp091-go has one reader
// goroutine per connection; it decodes a frame, hands it to the channel it
// belongs to, and BLOCKS on the send -- which is why a slow listener can wedge
// a connection and why `notifyAll` has a timeout at all. So this has one
// goroutine per connection too, draining one FIFO, blocking on each send.
// Anything else -- a goroutine per channel, a goroutine per notification --
// would let two frames overtake each other and the driver's `basic.return`
// handling rests on them not doing that.
type fakeConn struct {
	b   *Broker
	id  int
	cfg client.DialConfig

	// stateMu guards the connection's own state. It is never held across a
	// send and never taken under the broker's mutex the other way round: the
	// broker takes b.mu and then reaches in here, so nothing in here may take
	// b.mu while holding stateMu.
	stateMu sync.Mutex
	closed  bool
	chans   []*fakeChan
	closes  []chan *amqp.Error
	blocks  []chan amqp.Blocking
	nextCh  int

	// dmu, dcond and frames are the dispatcher's queue. It is unbounded
	// because a bound would deadlock the broker against its own client: the
	// operation that enqueues a frame holds b.mu, and a bounded queue would
	// park it there until the dispatcher drained -- which may be waiting on a
	// listener that is waiting on a broker operation. The socket buffer a real
	// connection has is the same unbounded-enough thing.
	dmu      sync.Mutex
	dcond    *sync.Cond
	frames   []frame
	draining bool
	dead     bool

	// hard is closed when the broker is being torn down, and it releases
	// every blocking send. Without it a listener the driver stopped reading
	// would park the dispatcher for ever and no tear-down could finish.
	hard     chan struct{}
	hardOnce sync.Once
	wg       sync.WaitGroup
}

var _ client.Conn = (*fakeConn)(nil)

// newConnLocked registers a new connection. The broker's mutex is held.
func (b *Broker) newConnLocked(cfg client.DialConfig) *fakeConn {
	c := &fakeConn{b: b, id: len(b.conns), cfg: cfg, hard: make(chan struct{})}
	c.dcond = sync.NewCond(&c.dmu)
	b.conns = append(b.conns, c)
	return c
}

// start launches the dispatcher.
func (c *fakeConn) start() { c.wg.Go(c.loop) }

// goIfLive registers fn on the connection's goroutine group unless the
// connection has already drained, and reports whether it did.
//
// It exists because `c.wg` is the connection's accounting and [fakeConn.drain]
// is the only thing allowed to find it empty. A caller that has released the
// broker's mutex and then calls `c.wg.Go` is racing the whole of a connection
// close: `drain` sets `draining`, releases `dmu` and only then waits, so a
// `Go` that lands after that wait returned is `sync.WaitGroup`'s one
// documented misuse ("calls with a positive delta that occur when the counter
// is zero must happen before a Wait"). The race detector reports it as a
// write/read pair on the WaitGroup's semaphore with `drain` on one side, and
// `sync` may answer it with a panic instead.
//
// Deciding under `dmu` removes the window, for the driver's own `sink.attach`
// reason and with the same shape: either this registration completes before
// `drain` takes `dmu`, and the `Add` strictly precedes the `Wait` -- or `drain`
// got there first, `draining` is already set, and nothing is registered at all.
func (c *fakeConn) goIfLive(fn func()) bool {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	if c.draining || c.dead {
		return false
	}
	c.wg.Go(fn)
	return true
}

// loop is the dispatcher: one frame at a time, in the order they were
// produced, until the connection drains.
func (c *fakeConn) loop() {
	for {
		f, ok := c.next()
		if !ok {
			return
		}
		c.dispatch(f)
		if f.done != nil {
			close(f.done)
		}
	}
}

// next waits for the head of the queue.
func (c *fakeConn) next() (frame, bool) {
	c.dmu.Lock()
	defer c.dmu.Unlock()
	for len(c.frames) == 0 {
		if c.draining {
			return frame{}, false
		}
		c.dcond.Wait()
	}
	f := c.frames[0]
	c.frames[0] = frame{}
	c.frames = c.frames[1:]
	return f, true
}

// post puts a frame at the back of the queue.
//
// force is for the close frame itself, which has to be accepted after the
// connection has stopped taking anything else -- otherwise a close would have
// to race its own queue. Anything refused has its done channel closed rather
// than dropped, so nothing waiting on a frame can be left waiting for ever.
func (c *fakeConn) post(f frame, force bool) {
	c.dmu.Lock()
	if c.dead || (c.draining && !force) {
		c.dmu.Unlock()
		if f.done != nil {
			close(f.done)
		}
		return
	}
	c.frames = append(c.frames, f)
	c.dcond.Signal()
	c.dmu.Unlock()
}

// enqueue posts a frame from a broker operation.
func (c *fakeConn) enqueue(f frame) { c.post(f, false) }

// sync waits until every frame already enqueued has been dispatched.
func (c *fakeConn) sync() {
	done := make(chan struct{})
	c.post(frame{kind: frBarrier, done: done}, false)
	<-done
}

// abort releases every blocking send, once.
func (c *fakeConn) abort() { c.hardOnce.Do(func() { close(c.hard) }) }

// drain stops the dispatcher, waits for it, and releases anything that was
// waiting on a frame it will now never dispatch.
//
// The dispatcher finishes what is already queued before it exits -- next()
// only gives up when the queue is EMPTY -- so a close frame posted just before
// a drain is still delivered. The cleanup afterwards is for the narrow window
// between the loop exiting and this flag being set, in which a force-posted
// frame can still arrive: closing its done channel is what keeps whoever
// posted it from waiting for ever.
func (c *fakeConn) drain() {
	c.dmu.Lock()
	if c.dead {
		c.dmu.Unlock()
		return
	}
	c.draining = true
	c.dcond.Broadcast()
	c.dmu.Unlock()
	c.wg.Wait()
	c.dmu.Lock()
	c.dead = true
	for i := range c.frames {
		if done := c.frames[i].done; done != nil {
			close(done)
		}
	}
	c.frames = nil
	c.dmu.Unlock()
}

// notifyTimeout reads the broker's abandonment policy.
func (c *fakeConn) notifyTimeout() time.Duration {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	return c.b.cfg.NotifyTimeout
}

// dispatch delivers one frame, blocking on the send exactly as the library's
// reader does.
func (c *fakeConn) dispatch(f frame) {
	timeout := c.notifyTimeout()
	switch f.kind {
	case frBarrier:
		return
	case frDeliver:
		if f.ch.closed.Load() {
			// The channel closed while this delivery was in the queue, so
			// the broker has already put the message back (or, for a
			// `no-ack` consumer, already recorded it as gone) and the
			// consumer's channel is about to be closed. Sending now would
			// deliver a message twice and then panic on a closed channel.
			return
		}
		// A delivery is the one send that does not go through notifyAll: the
		// library never abandons a delivery, it buffers it. See
		// [DefaultConsumeBuffer] for what this fake does instead and why.
		select {
		case f.out <- f.delivery:
		case <-c.hard:
		}
	case frReturn:
		f.ch.notifyMu.RLock()
		listeners := f.ch.returns
		f.ch.notifyMu.RUnlock()
		notifyAll(listeners, f.ret, timeout, c.hard)
	case frConfirm:
		// The library's own dispatch: `basic.ack{multiple}` becomes
		// `confirms.Multiple`, which EXPANDS it per tag, and a single ack
		// becomes `confirms.One`, which resequences it. Both of them notify
		// from this goroutine, which is what keeps a confirm behind the
		// return that preceded it.
		if f.multiple {
			f.ch.confirms.multiple(f.conf, timeout, c.hard)
		} else {
			f.ch.confirms.one(f.conf, timeout, c.hard)
		}
	case frCancel:
		f.ch.notifyMu.RLock()
		listeners := f.ch.cancels
		f.ch.notifyMu.RUnlock()
		if f.notify {
			// Only a cancel the BROKER decided reaches `NotifyCancel`. A
			// client's own `basic.cancel` closes the delivery channel and
			// says nothing, because a driver that heard its own cancel back
			// would tear the subscription down a second time.
			notifyAll(listeners, f.tag, timeout, c.hard)
		}
		f.ch.closeConsumer(f.tag)
	case frFlow:
		f.ch.notifyMu.RLock()
		listeners := f.ch.flows
		f.ch.notifyMu.RUnlock()
		notifyAll(listeners, f.active, timeout, c.hard)
	case frChanClose:
		f.ch.teardown(f.err, timeout)
	case frConnClose:
		c.stateMu.Lock()
		chans := append([]*fakeChan(nil), c.chans...)
		closes, blocks := c.closes, c.blocks
		c.closes, c.blocks = nil, nil
		c.stateMu.Unlock()
		if f.err != nil {
			notifyAll(closes, f.err, timeout, c.hard)
		}
		for _, ch := range chans {
			ch.teardown(f.err, timeout)
		}
		for _, l := range closes {
			close(l)
		}
		for _, l := range blocks {
			close(l)
		}
	case frBlocking:
		c.stateMu.Lock()
		listeners := append([]chan amqp.Blocking(nil), c.blocks...)
		c.stateMu.Unlock()
		notifyAll(listeners, f.blocking, timeout, c.hard)
	}
}

// Channel opens a channel on this connection.
func (c *fakeConn) Channel() (client.Channel, error) {
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		return nil, amqp.ErrClosed
	}
	if maxCh := c.b.channelMax(); maxCh > 0 && len(c.chans) >= maxCh {
		c.stateMu.Unlock()
		// The library's own refusal, not a broker frame: it runs out of
		// channel ids before it writes anything.
		return nil, amqp.ErrChannelMax
	}
	c.nextCh++
	ch := newChannel(c, c.nextCh)
	c.chans = append(c.chans, ch)
	c.stateMu.Unlock()
	if hook := c.b.channelHook(); hook != nil {
		hook(&ChannelHandle{ch: ch})
	}
	return ch, nil
}

// NotifyClose registers a connection-close listener.
func (c *fakeConn) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed {
		close(ch)
		return ch
	}
	c.closes = append(c.closes, ch)
	return ch
}

// NotifyBlocked registers a `connection.blocked` listener.
func (c *fakeConn) NotifyBlocked(ch chan amqp.Blocking) chan amqp.Blocking {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.closed {
		close(ch)
		return ch
	}
	c.blocks = append(c.blocks, ch)
	return ch
}

// IsClosed reports whether the connection has closed.
func (c *fakeConn) IsClosed() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.closed
}

// Close closes the connection and every channel on it, the way a client's own
// Close does: no error reaches a `NotifyClose` listener, but every
// notification channel is closed.
func (c *fakeConn) Close() error {
	c.close(nil, false)
	return nil
}

// ServerProperties is the `connection.start` table, which is where the
// broker's version comes from.
func (c *fakeConn) ServerProperties() amqp.Table { return c.b.serverProperties() }

// Native is the escape hatch the real connection has. There is no
// *amqp091.Connection behind a fake, so it returns nil -- and it is here so
// that a driver that reaches for the hatch gets a nil it can test for rather
// than failing to compile against the fake.
func (c *fakeConn) Native() *amqp.Connection { return nil }

// close closes the connection once, with a cause or without one.
//
// abort is for the broker's own tear-down: it releases every blocking send
// first, so a notification with nowhere to go is dropped instead of parking
// the dispatcher. A FAULT does not abort -- the point of
// [ConnHandle.CloseWith] is that the driver's listener really receives the
// error -- so it blocks on the send like the library does.
func (c *fakeConn) close(cause *amqp.Error, abort bool) {
	c.stateMu.Lock()
	first := !c.closed
	c.closed = true
	chans := append([]*fakeChan(nil), c.chans...)
	c.stateMu.Unlock()
	if abort {
		c.abort()
	}
	if first {
		// Every channel goes with the connection, and every delivery those
		// channels were holding goes back to its queue. Doing it here rather
		// than in the dispatcher means the broker's state is already correct
		// by the time the driver is told, which is the order a broker does it
		// in: the requeue happens at the broker, the notification is what
		// travels.
		c.b.mu.Lock()
		for _, ch := range chans {
			ch.markClosedLocked(cause)
		}
		c.b.pumpLocked()
		c.b.mu.Unlock()
		done := make(chan struct{})
		c.post(frame{kind: frConnClose, err: cause, done: done}, true)
		<-done
	}
	c.drain()
}

// channelMax reads the per-connection channel bound.
func (b *Broker) channelMax() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cfg.MaxChannels
}

// channelHook reads the per-channel hook.
func (b *Broker) channelHook() func(*ChannelHandle) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cfg.OnChannel
}

// serverProperties is the `connection.start` table.
//
// "product" and "version" are the two fields the version gates read, and
// "capabilities" is where a broker says what it can do. They are built fresh
// on every call so that a caller who mutates the table it was given cannot
// change what the next connection reports.
func (b *Broker) serverProperties() amqp.Table {
	b.mu.Lock()
	defer b.mu.Unlock()
	caps := amqp.Table{
		"publisher_confirms":           true,
		"consumer_cancel_notify":       true,
		"exchange_exchange_bindings":   true,
		"basic.nack":                   true,
		"connection.blocked":           true,
		"per_consumer_qos":             true,
		"authentication_failure_close": true,
	}
	for k, v := range b.cfg.Capabilities {
		caps[k] = v
	}
	return amqp.Table{
		"product":      b.cfg.Product,
		"version":      b.cfg.Version,
		"platform":     "Erlang/OTP",
		"copyright":    "rabbitfake",
		"cluster_name": "rabbitfake@localhost",
		"capabilities": caps,
	}
}

// ConnHandle is a test's handle on one connection the driver opened.
//
// It exists because the driver dials and opens its own channels: a test that
// wants to close the connection the driver is publishing on cannot be handed
// the connection at construction time, because it does not exist yet.
// [Config.OnConnection] hands it over at the moment it opens, and
// [Broker.Connections] finds it afterwards.
type ConnHandle struct{ c *fakeConn }

// ID is the connection's index in the order the broker accepted them, counting
// from zero.
func (h *ConnHandle) ID() int { return h.c.id }

// DialConfig is the configuration this connection was dialled with.
func (h *ConnHandle) DialConfig() client.DialConfig { return h.c.cfg }

// Channels are the channels open on this connection, in the order they were
// opened. A channel that has closed is not in the list.
func (h *ConnHandle) Channels() []*ChannelHandle {
	h.c.stateMu.Lock()
	chans := append([]*fakeChan(nil), h.c.chans...)
	h.c.stateMu.Unlock()
	out := make([]*ChannelHandle, 0, len(chans))
	for _, ch := range chans {
		if !ch.IsClosed() {
			out = append(out, &ChannelHandle{ch: ch})
		}
	}
	return out
}

// LastChannel is the most recently opened channel that is still open, or nil.
func (h *ConnHandle) LastChannel() *ChannelHandle {
	chans := h.Channels()
	if len(chans) == 0 {
		return nil
	}
	return chans[len(chans)-1]
}

// CloseWith closes the connection as the BROKER would, delivering err to every
// `NotifyClose` listener on the connection and on each of its channels.
//
// It is the fault the specification's "driver owns bounded reconnection" is tested with,
// and the important half is what it does to state rather than what it
// notifies: every delivery every channel was holding goes back to its queue,
// and every publication whose confirm had not arrived is resolved the way
// `deferredConfirmations.Close` resolves it. A driver that settles a delivery
// tag from before this point is settling a tag the new channel has reissued,
// which is the disqualifier.
func (h *ConnHandle) CloseWith(err *amqp.Error) { h.c.close(err, false) }

// Block emits `connection.blocked` with a reason, which is how RabbitMQ tells
// a publisher it is under resource pressure.
func (h *ConnHandle) Block(reason string) {
	h.c.enqueue(frame{kind: frBlocking, blocking: amqp.Blocking{Active: true, Reason: reason}})
	h.c.sync()
}

// Unblock emits `connection.unblocked`.
func (h *ConnHandle) Unblock() {
	h.c.enqueue(frame{kind: frBlocking, blocking: amqp.Blocking{Active: false}})
	h.c.sync()
}

// Sync waits until this connection's dispatcher has drained.
func (h *ConnHandle) Sync() { h.c.sync() }

// IsClosed reports whether the connection has closed.
func (h *ConnHandle) IsClosed() bool { return h.c.IsClosed() }

// Connections are every connection the driver has opened, in order, closed
// ones included -- a test about reconnection needs to see that the first one
// is gone as well as that the second one exists.
func (b *Broker) Connections() []*ConnHandle {
	b.mu.Lock()
	conns := append([]*fakeConn(nil), b.conns...)
	b.mu.Unlock()
	out := make([]*ConnHandle, 0, len(conns))
	for _, c := range conns {
		out = append(out, &ConnHandle{c: c})
	}
	return out
}

// Connection is the i'th connection, or nil when there is no such connection.
func (b *Broker) Connection(i int) *ConnHandle {
	conns := b.Connections()
	if i < 0 || i >= len(conns) {
		return nil
	}
	return conns[i]
}

// LastConnection is the most recently opened connection, or nil.
func (b *Broker) LastConnection() *ConnHandle {
	conns := b.Connections()
	if len(conns) == 0 {
		return nil
	}
	return conns[len(conns)-1]
}

// Channels are every open channel across every connection, in the order they
// were opened.
func (b *Broker) Channels() []*ChannelHandle {
	var out []*ChannelHandle
	for _, c := range b.Connections() {
		out = append(out, c.Channels()...)
	}
	return out
}

// Channel is the i'th open channel, or nil.
func (b *Broker) Channel(i int) *ChannelHandle {
	chans := b.Channels()
	if i < 0 || i >= len(chans) {
		return nil
	}
	return chans[i]
}

// LastChannel is the most recently opened channel that is still open, or nil.
// It is what a test reaches for after letting the driver open its publisher.
func (b *Broker) LastChannel() *ChannelHandle {
	chans := b.Channels()
	if len(chans) == 0 {
		return nil
	}
	return chans[len(chans)-1]
}

// OpenChannels counts the channels open across every connection.
func (b *Broker) OpenChannels() int { return len(b.Channels()) }

// consumerTag generates a tag for a `basic.consume` that named none.
//
// The library generates one from the process name and a counter and never lets
// the SERVER choose, so the fake's generated tags look like a broker's and are
// numbered from a counter of the broker's own: a test that reads a tag out of
// an observation gets the same string on every run.
func (b *Broker) consumerTag() string {
	b.consumerSeq++
	return "ctag-rabbitfake-" + strconv.FormatUint(b.consumerSeq, 10)
}

package amqp10

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/Azure/go-amqp"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The compile-time proof of the sink's surfaces.
//
// Two absences are decisions:
//
//   - There is no [github.com/iqhive/iqmq.BatchSink]. An AMQP 1.0 transfer
//     carries one message, and go-amqp has no batched transfer, so a
//     PublishBatch here would be a loop wearing a name that promises a saving.
//     The core loops instead (ADR-021) and FeatureBatchPublish says
//     unsupported, which is the honest answer rather than a surface that costs
//     the same and reads as though it did not.
//   - There is no [github.com/iqhive/iqmq.OwnedSink]. Every asynchronous path
//     here hands the message to the library, which holds the encoded transfer
//     until its disposition arrives; there is nothing to release earlier than
//     that, so a sink that released at invocation would hand the caller's
//     buffer back while the library was still using it. It is the same
//     reasoning drivers/jetstream recorded.
var (
	_ mq.Sink      = (*Sink)(nil)
	_ mq.AsyncSink = (*Sink)(nil)
	_ mq.FlushSink = (*Sink)(nil)
)

// Sink is one open publisher, bound to one address for its lifetime (ADR-021).
//
// It is exported because a broker-specific module built on this core needs to be
// able to name it — a profile that wraps a sink in order to add its own control
// has to have a type to wrap — and because the critical tests live in another
// package.
type Sink struct {
	linkHolder
	b       *Backend
	topic   string
	address string
	confirm mq.ConfirmationMode
	receipt bool
	mapper  *Mapper
	bufs    sync.Pool

	link *senderLink

	// The asynchronous publication queue and the ONE goroutine that resolves
	// it. The specification forbids a goroutine per message, and a receipt's Wait blocks,
	// so the queue is what turns N blocking waits into one goroutine.
	//
	// The queue is a ring under amu that the worker waits on through
	// queued; slots counts the publications admitted and not yet resolved,
	// and maxPending is its bound. The ring never holds more than slots, so
	// it is allocated once at its bound.
	amu        sync.Mutex
	queued     sync.Cond
	queue      []*pendingPub
	qhead      int
	qlen       int
	slots      atomic.Int64
	maxPending int64
	aclosed    bool
	worker     sync.Once
	workerCtx  context.Context
	workerOff  context.CancelFunc
	workerEnd  chan struct{}

	// The hot-path errors the specification asks to be preallocated per handle: their
	// content is the handle's own, so they are built once at open and the
	// failure path formats nothing.
	errClosed *mq.OpError
}

// pendingPub is one asynchronous publication waiting for its disposition.
type pendingPub struct {
	msg  *mq.Message
	rec  receiptWaiter
	done func(error)
	buf  *Buf
	// link is the link the transfer went out on, so that a failure the
	// worker hears about late is charged to that link and not to the one
	// that may have replaced it since.
	link *senderLink
}

// receiptWaiter is the part of the client's receipt a pending publication uses.
// Naming it here rather than importing the seam's whole interface keeps the
// asynchronous queue readable: what it waits on is one method.
type receiptWaiter interface {
	Wait(ctx context.Context) (amqp.DeliveryState, error)
}

// newSink builds a sink for one destination.
func (b *Backend) newSink(cfg mq.SinkConfig) (*Sink, error) {
	address, err := b.profile.SinkAddress(cfg.Topic)
	if err != nil {
		return nil, err
	}
	confirm := cfg.Confirmation.Resolve()
	if confirm == mq.ConfirmDurable && !b.profile.Durable.Supported {
		// The specification: "ConfirmDurable fails at construction when the profile cannot
		// define a meaningful durable boundary". Failing here rather than per
		// publication is what makes it a construction error the caller sees
		// once.
		return nil, errNoDurableBoundary
	}
	if confirm == mq.ConfirmNone && b.cfg.SenderSettle.Resolve() == SenderSettleUnsettled {
		// A link attached as unsettled cannot send a pre-settled transfer, so
		// ConfirmNone has no spelling on it. Saying so at open is the only
		// place a caller can act on it.
		return nil, errNoUnconfirmedOnUnsettledLink
	}
	if confirm != mq.ConfirmNone && b.cfg.SenderSettle.Resolve() == SenderSettleSettled {
		// The mirror image: a link that pre-settles everything has no
		// disposition to wait for, so there is nothing to confirm.
		return nil, errNoConfirmOnSettledLink
	}
	mapper, err := NewMapper(b.profile, cfg.Metadata, b.caps, !b.cfg.NonDurable)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Sink{
		b:          b,
		topic:      cfg.Topic,
		address:    address,
		confirm:    confirm,
		receipt:    cfg.Receipts,
		mapper:     mapper,
		queue:      make([]*pendingPub, b.cfg.MaxPending),
		maxPending: int64(b.cfg.MaxPending),
		workerCtx:  ctx,
		workerOff:  cancel,
		workerEnd:  make(chan struct{}),
		errClosed: &mq.OpError{
			Driver: b.profile.Driver, Operation: mq.OpPublish,
			Topic: cfg.Topic, Code: mq.CodeClosed, Err: mq.ErrClosed,
		},
	}
	s.bufs.New = func() any { return NewBuf() }
	s.queued.L = &s.amu
	return s, nil
}

// Address is the AMQP address this sink's link attaches its target to, which is
// what the profile's naming produced.
func (s *Sink) Address() string { return s.address }

// Publish admits one message.
//
// The order is fixed and it is the specification: everything this broker cannot honour is
// refused before anything is written, then the message is mapped, then the
// transfer goes out — and under every confirmation but ConfirmNone the call
// waits for the peer's disposition.
//
// Nothing of m is retained. The mapping aliases m.Body into the transfer's data
// section and the transfer is finished with it before this returns; the
// asynchronous path copies it, because the library holds the message until its
// disposition.
//
// # The Ambiguous outcome
//
// A transfer that has left the socket and whose disposition never arrived may
// have been accepted. The specification makes that Ambiguous and makes reporting it as
// a definite failure a disqualifying defect: the caller retries and the broker
// now holds two copies of a message it believes it published once. Every error
// on this path therefore goes through
// [github.com/iqhive/iqmq/driverkit.ErrorMapper.MapNet] with a sentAlready the
// call site knows, and the two halves are:
//
//   - a failure from the SEND, which is ambiguous only when the error says the
//     link or the connection died while writing ([sentDuringSend]); a peer's
//     own refusal — an oversized message, a bad field — arrived as an answer,
//     so the transfer definitely was not taken;
//   - a failure from the WAIT, which is ALWAYS ambiguous: the transfer left,
//     and what failed was hearing the answer.
func (s *Sink) Publish(ctx context.Context, m *mq.Message, o *mq.PublishOptions) error {
	if err := s.check(ctx, m); err != nil {
		return err
	}
	buf, _ := s.bufs.Get().(*Buf)
	defer s.bufs.Put(buf)
	msg, err := s.mapper.Encode(buf, m, o)
	if err != nil {
		return s.refuse(err)
	}
	link, err := s.sender(ctx)
	if err != nil {
		return s.fail(err, false, nil)
	}
	if s.confirm == mq.ConfirmNone {
		// Fire and forget: the transfer is pre-settled, so no disposition is
		// coming and the call costs one frame and no round trip. A failure
		// here is a failure to WRITE, which is local admission failing — what
		// The specification makes ConfirmNone's boundary — and is therefore definite.
		if err := link.snd.Send(ctx, msg, &amqp.SendOptions{Settled: true}); err != nil {
			return s.fail(err, false, link)
		}
		s.fillReceipt(m, nil)
		return nil
	}
	rec, err := link.snd.SendWithReceipt(ctx, msg, nil)
	if err != nil {
		return s.fail(err, sentDuringSend(err), link)
	}
	state, werr := rec.Wait(ctx)
	if werr != nil {
		s.b.ambiguous.Add(1)
		return s.fail(werr, true, link)
	}
	if oerr := outcomeError(state); oerr != nil {
		// The disposition ARRIVED and said no. That is a definite answer, and
		// reporting it as ambiguous would make every rejection something a
		// caller has to reconcile.
		return s.fail(oerr, false, link)
	}
	s.fillReceipt(m, state)
	return nil
}

// PublishAsync admits one message and reports its outcome to done.
//
// It costs no goroutine per message, which the specification forbids outright: the transfer
// is written on the caller's goroutine and its receipt joins a bounded queue
// that ONE goroutine per sink drains in the order the transfers were written.
// Out-of-order dispositions cost nothing — a receipt whose disposition arrived
// while an earlier one was outstanding resolves immediately when its turn comes
// — and the ordering guarantee the caller gets is the one the specification asks for: every
// callback runs exactly once.
//
// The bound is [Config.MaxPending]. Once that many publications are outstanding
// the next is refused with CodeBackpressure, which is the answer and not a
// buffer that grows. The slot is taken BEFORE the transfer is written, so a
// refusal is a publication that never left rather than one that left and was
// then disowned.
func (s *Sink) PublishAsync(ctx context.Context, m *mq.Message, o *mq.PublishOptions, done func(error)) error {
	if done == nil {
		return errNilCallback
	}
	if err := s.check(ctx, m); err != nil {
		return err
	}
	if s.confirm == mq.ConfirmNone {
		// There is no disposition to wait for, so the asynchronous path is
		// the synchronous one plus a callback. Doing it here rather than
		// queueing a receipt that will never resolve is what keeps the queue
		// bounded by outstanding DISPOSITIONS.
		err := s.Publish(ctx, m, o)
		done(err)
		return nil
	}
	if !s.takeSlot() {
		return errAsyncFull
	}
	buf, _ := s.bufs.Get().(*Buf)
	msg, err := s.mapper.Encode(buf, m, o)
	if err != nil {
		s.release(buf)
		return s.refuse(err)
	}
	// The library holds the transfer until its disposition, so the payload
	// cannot stay the caller's.
	msg.Data = [][]byte{append([]byte(nil), m.Body...)}
	if m.Body == nil {
		msg.Data = nil
	}
	link, err := s.sender(ctx)
	if err != nil {
		s.release(buf)
		return s.fail(err, false, nil)
	}
	rec, err := link.snd.SendWithReceipt(ctx, msg, nil)
	if err != nil {
		s.release(buf)
		// A non-nil immediate error means done does not run.
		return s.fail(err, sentDuringSend(err), link)
	}
	if !s.enqueue(&pendingPub{msg: m, rec: rec, done: done, buf: buf, link: link}) {
		s.release(buf)
		// The sink closed between the send and the queueing. The transfer has
		// left, so the outcome is unknown rather than failed — and the
		// callback is the place that has to say so, because the specification promises it
		// runs exactly once for a publication that was admitted.
		done(s.fail(mq.ErrSettlementUnknown, true, link))
		return nil
	}
	return nil
}

// Flush completes the publications already admitted, bounded by ctx.
//
// "Already admitted" is exactly the asynchronous queue: a synchronous Publish
// has finished by the time it returns, so there is nothing of it to flush. It
// polls rather than waiting on a condition because the queue's drain is the
// worker's and a second waiter on it would have to be woken by the worker,
// which would make the worker's fast path pay for a Flush nobody called.
func (s *Sink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return s.errClosed
	}
	for s.outstanding() > 0 {
		t := s.b.clock.NewTimer(flushPoll)
		select {
		case <-t.C():
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		t.Stop()
		if s.isClosed() {
			return s.errClosed
		}
	}
	return nil
}

// flushPoll is how often Flush looks at the queue. It is short enough not to
// add latency a caller would notice and long enough not to spin.
const flushPoll = time.Millisecond

// Close releases the sink. It is idempotent, and a Publish afterwards reports
// ErrClosed.
//
// It resolves every asynchronous publication this sink still has outstanding
// with CodeSettlementUnknown, because that is the truth: the transfer had left
// and its disposition had not arrived, so nobody knows whether the broker holds
// it. The specification requires the callback to run exactly once, close included, and a sink
// that simply dropped them would leave a caller's pooled message unreleased for
// ever.
func (s *Sink) Close() error {
	if !s.closeHolder() {
		return nil
	}
	s.amu.Lock()
	if !s.aclosed {
		s.aclosed = true
		s.queued.Broadcast()
	}
	s.amu.Unlock()
	// Cancelling the worker's context is what makes a Close prompt: a worker
	// parked on a receipt whose disposition is never coming would otherwise
	// hold the close for as long as the broker stays silent (the specification's "never
	// waits indefinitely").
	s.workerOff()
	// The worker is started by the FIRST asynchronous publication, so a sink
	// that only ever published synchronously has none — and a close that
	// waited for one anyway would hang for ever on a channel nothing is going
	// to close. Running the same sync.Once here is what settles the question
	// exactly once: whichever of the two gets there first decides whether
	// there is a worker at all, and the other becomes a no-op. The first
	// version of this waited unconditionally, and every test whose sink
	// published synchronously timed out in its own cleanup.
	s.worker.Do(func() { close(s.workerEnd) })
	<-s.workerEnd

	s.mu.Lock()
	link := s.link
	s.link = nil
	s.mu.Unlock()
	ctx, cancel := s.b.bound(context.Background())
	defer cancel()
	err := detachSender(ctx, link)
	s.b.forget(nil, s)
	if err != nil && !isTransportGone(err) {
		// A detach that failed because the link was already gone is not a
		// close that failed: the link is gone, which is what Close wanted.
		return s.b.errs.Map(mq.OpClose, s.topic, "", err)
	}
	return nil
}

// connectionLost fails every outstanding publication AMBIGUOUS.
//
// It is step 3 of the sequence in reconnect.go. The transfers had left and their
// dispositions had not arrived, so the answer is that nobody knows — and the specification makes the alternative a disqualifier. The link is dropped so that the
// next publication attaches a new one under the new generation.
func (s *Sink) connectionLost() {
	s.mu.Lock()
	s.link = nil
	s.mu.Unlock()
	// The worker's own Wait is already failing with the dead link's error,
	// which the publish rules map as ambiguous; nothing has to be drained
	// here. What dropping the link does is stop the NEXT publication using it,
	// which would otherwise fail every publication until the caller happened
	// to close and reopen the publisher.
}

// linkLost is [Sink.connectionLost] for a link that died on a connection that
// did not.
//
// It drops the link so that the next publication attaches a new one. Without
// it the generation check in [Sink.sender] hands back the dead link for ever,
// because a link detach does not bump the connection generation -- so a sink
// whose destination was deleted and recreated, or whose link a same-named
// client stole, fails every publication until the caller happens to close and
// reopen the publisher.
//
// It acts only while link is still the current one. The resolve loop hears
// about a dead link's receipts after a publication may already have attached
// its replacement, and dropping the replacement then would orphan a healthy
// link and its session and make the next publication attach a third. And
// unlike a dead connection, a detached link leaves its session begun on a
// connection this client keeps, so the session is ended here.
func (s *Sink) linkLost(link *senderLink) {
	s.mu.Lock()
	if link == nil || s.link != link {
		s.mu.Unlock()
		return
	}
	s.link = nil
	s.mu.Unlock()
	ctx, cancel := s.b.bound(context.Background())
	defer cancel()
	_ = detachSender(ctx, link)
}

// sender returns this sink's link, attaching one when there is none or when the
// one it has belongs to an older connection.
//
// The generation check and the attach happen under ONE lock, which is what keeps
// two concurrent publications on a reconnected backend from each attaching a
// link and orphaning one of them (links.go says why that matters).
func (s *Sink) sender(ctx context.Context) (*senderLink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, s.errClosed
	}
	_, gen, err := s.b.connection()
	if err != nil {
		return nil, err
	}
	if s.link != nil && s.link.gen == gen {
		return s.link, nil
	}
	old := s.link
	s.link = nil
	actx, cancel := s.b.bound(ctx)
	defer cancel()
	if old != nil {
		// Best effort: the old link is on a connection that has died, so this
		// is about releasing the library's own goroutines rather than about
		// telling the peer anything.
		_ = detachSender(actx, old)
	}
	link, err := s.b.attachSender(actx, s.address, s.senderOptions())
	if err != nil {
		return nil, err
	}
	s.link = link
	return link, nil
}

// senderOptions are the attach's own.
//
// The settlement mode is the configuration's, and it is a POINTER in go-amqp
// where nil means "take the peer's choice" — which this driver never wants: a
// sink whose mode the peer chose could not know whether ConfirmNone has a
// spelling on it, and the answer would change between brokers.
func (s *Sink) senderOptions() *amqp.SenderOptions {
	opts := &amqp.SenderOptions{
		SettlementMode:              s.b.cfg.SenderSettle.amqpMode(),
		RequestedReceiverSettleMode: s.b.cfg.ReceiverSettle.amqpMode(),
	}
	s.b.cfg.Publisher.apply(opts)
	return opts
}

// check is the guard every publication starts with.
func (s *Sink) check(ctx context.Context, m *mq.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.isClosed() {
		return s.errClosed
	}
	if m == nil {
		return errNilMessage
	}
	return nil
}

// refuse reports a mapping failure, which is a refusal made before any I/O and
// is therefore never ambiguous and never retryable.
func (s *Sink) refuse(err error) error {
	return s.b.errs.Map(mq.OpPublish, s.topic, "", err)
}

// fail maps a transport or peer failure, with the sentAlready distinction the specification
// asks a driver to make. link is the link the failure came from, or nil when
// none was attached.
func (s *Sink) fail(err error, sentAlready bool, link *senderLink) error {
	// A link the peer detached is dropped here rather than at each call
	// site, because every publish failure -- synchronous, asynchronous and
	// the resolve loop's -- comes through this one funnel.
	if isLinkGone(err) {
		s.linkLost(link)
	}
	return s.b.errs.MapNet(mq.OpPublish, s.topic, "", err, sentAlready)
}

// sentDuringSend reports whether a failure from the send itself means the
// transfer may already have reached the peer.
//
// go-amqp validates before it writes — the delivery tag's length, the message
// against the link's declared maximum — and those refusals are definite: nothing
// left. What is not definite is a link, session or connection that died, or a
// context that ended, because those can happen on either side of the write and
// the library does not say which. The specification makes misclassifying the second as the
// first a disqualifier, so the ambiguous answer is the one this function gives
// whenever it cannot tell.
func sentDuringSend(err error) bool {
	if err == nil {
		return false
	}
	if isTransportGone(err) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// fillReceipt records what the peer said about an accepted publication.
//
// The receipt carries the boundary and the time and NOT an identifier: an AMQP
// 1.0 disposition says `accepted` and nothing else, so there is no
// broker-assigned identifier to report. Filling MessageID with the identifier
// the PUBLISHER sent would be reporting the caller's own value back as the
// broker's, which is worse than leaving it empty — and the capability note says
// so.
func (s *Sink) fillReceipt(m *mq.Message, state amqp.DeliveryState) {
	if !s.receipt || m.Receipt == nil {
		return
	}
	m.Receipt.Confirmation = s.confirm
	m.Receipt.ConfirmedAt = s.b.now()
	if state != nil {
		// The delivery state is already an interface value, so surfacing it
		// costs no boxing of its own.
		m.Receipt.Native = state
	}
}

// enqueue puts a pending publication on the worker's queue, starting the worker
// on the first one.
func (s *Sink) enqueue(p *pendingPub) bool {
	s.amu.Lock()
	defer s.amu.Unlock()
	if s.aclosed {
		return false
	}
	// The slot taken before the transfer bounds the queue, so the ring has
	// room for this one.
	tail := s.qhead + s.qlen
	if tail >= len(s.queue) {
		tail -= len(s.queue)
	}
	s.queue[tail] = p
	s.qlen++
	s.queued.Signal()
	s.worker.Do(func() { go s.resolveLoop() })
	return true
}

// nextPending takes the oldest queued publication, waiting for one. It
// reports false once the sink has closed and the queue is empty, which is
// the worker's signal to end: what was queued before the close is still
// handed out, so that every callback runs.
func (s *Sink) nextPending() (*pendingPub, bool) {
	s.amu.Lock()
	defer s.amu.Unlock()
	for s.qlen == 0 && !s.aclosed {
		s.queued.Wait()
	}
	if s.qlen == 0 {
		return nil, false
	}
	p := s.queue[s.qhead]
	s.queue[s.qhead] = nil
	s.qhead++
	if s.qhead == len(s.queue) {
		s.qhead = 0
	}
	s.qlen--
	return p, true
}

// takeSlot admits one more asynchronous publication, or reports that
// MaxPending are already outstanding.
func (s *Sink) takeSlot() bool {
	for {
		n := s.slots.Load()
		if n >= s.maxPending {
			return false
		}
		if s.slots.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// giveSlot gives one admission back. It never takes the count below zero,
// so a release that has nothing to give back is harmless.
func (s *Sink) giveSlot() {
	for {
		n := s.slots.Load()
		if n <= 0 {
			return
		}
		if s.slots.CompareAndSwap(n, n-1) {
			return
		}
	}
}

// release gives back a slot and a buffer for a publication that never joined
// the queue.
func (s *Sink) release(buf *Buf) {
	s.bufs.Put(buf)
	s.giveSlot()
}

// outstanding is how many asynchronous publications are waiting for a
// disposition.
func (s *Sink) outstanding() int { return int(s.slots.Load()) }

// Outstanding is how many asynchronous publications this sink is waiting for a
// disposition on, which is what [Sink.Flush] waits to reach zero.
func (s *Sink) Outstanding() int { return s.outstanding() }

// resolveLoop is the one goroutine per sink that waits on the receipts.
//
// It drains the queue in the order the transfers were written, which is not an
// ordering promise about the DISPOSITIONS — those may arrive in any order, and a
// receipt whose disposition arrived early resolves the instant its turn comes.
// What the order buys is one goroutine instead of N and a queue whose length is
// the outstanding count.
//
// A closed sink's remaining publications are resolved with CodeSettlementUnknown
// rather than dropped, because the specification promises every callback runs exactly once.
func (s *Sink) resolveLoop() {
	defer close(s.workerEnd)
	for {
		p, ok := s.nextPending()
		if !ok {
			return
		}
		state, err := p.rec.Wait(s.workerCtx)
		switch {
		case err != nil:
			s.b.ambiguous.Add(1)
			err = s.fail(err, true, p.link)
		default:
			if oerr := outcomeError(state); oerr != nil {
				err = s.fail(oerr, false, p.link)
			} else {
				s.fillReceipt(p.msg, state)
			}
		}
		s.bufs.Put(p.buf)
		s.giveSlot()
		// The callback runs LAST and outside every lock this sink holds: the specification
		// says a callback must not block the driver's progress, and a
		// callback that ran under a lock would make that impossible to
		// honour.
		p.done(err)
	}
}

// The sink's refusals.
var (
	errNilCallback = errcode.New(errcode.InvalidArgument,
		"iqmq/amqp10: PublishAsync was given no callback")
	errAsyncFull = errcode.New(errcode.Backpressure,
		"iqmq/amqp10: this sink already has Config.MaxPending publications waiting for a disposition")
	errNoDurableBoundary = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: this profile defines no durable confirmation boundary, so ConfirmDurable cannot be honoured (the message contract); set Profile.Durable or publish with ConfirmAccepted")
	errNoUnconfirmedOnUnsettledLink = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: ConfirmNone needs a pre-settled transfer and Config.SenderSettle is unsettled, which forbids one; use SenderSettleMixed")
	errNoConfirmOnSettledLink = errcode.New(errcode.Unsupported,
		"iqmq/amqp10: Config.SenderSettle is settled, so no disposition ever arrives and there is nothing for ConfirmAccepted to wait for; use SenderSettleMixed")
)

package jetstream

import (
	"errors"

	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// asyncPub is one asynchronous publication waiting for its PubAck.
//
// It holds the sink so that a closing sink can resolve its own publications and
// nobody else's, the caller's message so that the callback can be checked
// against it, and the callback itself — which the specification requires to run exactly once.
//
// It also holds the client library's future for the publication, once
// PublishMsgAsync has returned one, because the future is the only place the
// library records that it dropped a publication at a disconnection (see
// resolveDropped). sweep is the backend's sweep count when the publication was
// tracked, which is how a publication whose future arrived after a sweep is
// still checked.
type asyncPub struct {
	sink  *sink
	msg   *mq.Message
	done  func(error)
	fut   js.PubAckFuture
	sweep uint64
}

// trackAsync records a publication before it is written.
//
// The order matters and it is the engine's recurring defect class in miniature:
// the entry goes in BEFORE the publish, because the client library may resolve
// the publication from its own goroutine before PublishMsgAsync has returned, and
// a callback that arrived before the entry existed would be dropped and the
// caller's done would never run.
func (b *Backend) trackAsync(native *natsgo.Msg, pub *asyncPub) {
	b.amu.Lock()
	pub.sweep = b.sweeps
	b.async[native] = pub
	b.amu.Unlock()
}

// attachFuture records the future the client library returned for a
// publication, and reports whether a reconnection's sweep ran while the
// publication was still inside PublishMsgAsync.
//
// That is the one publication a sweep cannot see. PublishMsgAsync registers a
// publication with the library before it stalls on a full window, so a
// disconnection during the stall drops it with the rest; the library then lets
// it out when another PubAck frees the window and never reports it again. The
// sweep had no future to read, so the publication is checked here instead.
func (b *Backend) attachFuture(native *natsgo.Msg, fut js.PubAckFuture) (swept bool) {
	b.amu.Lock()
	defer b.amu.Unlock()
	pub := b.async[native]
	if pub == nil {
		return false
	}
	pub.fut = fut
	return pub.sweep != b.sweeps
}

// untrackAsync takes a publication back, for the one case where the write never
// happened.
func (b *Backend) untrackAsync(native *natsgo.Msg) {
	b.amu.Lock()
	delete(b.async, native)
	b.amu.Unlock()
}

// takeAsync removes and returns the publication a native message names, or nil
// when nothing is waiting for it.
//
// A nil answer is normal rather than exceptional: PublishBatch and Flush use the
// client library's own futures, so their publications reach these callbacks too
// and belong to nobody here.
func (b *Backend) takeAsync(native *natsgo.Msg) *asyncPub {
	b.amu.Lock()
	defer b.amu.Unlock()
	pub := b.async[native]
	if pub != nil {
		delete(b.async, native)
	}
	return pub
}

// onAsyncAck resolves one asynchronous publication the stream accepted.
func (b *Backend) onAsyncAck(_ js.JetStream, native *natsgo.Msg, ack *js.PubAck) {
	pub := b.takeAsync(native)
	if pub == nil {
		return
	}
	pub.sink.fillReceipt(pub.msg, ack)
	pub.done(nil)
}

// onAsyncErr resolves one asynchronous publication that failed.
//
// Every failure the client library reports here reaches the caller as an
// AMBIGUOUS publication, because every one of them happened after the message
// left the socket: a PubAck that timed out, a link that dropped with the
// publication outstanding, a JetStream context that was cleaned up. The specification
// forbids reporting those as definite failures, and a caller that retried one as
// though it were definite would duplicate the message.
func (b *Backend) onAsyncErr(_ js.JetStream, native *natsgo.Msg, err error) {
	pub := b.takeAsync(native)
	if pub == nil {
		return
	}
	if errors.Is(err, js.ErrAsyncPublishTimeout) {
		b.resolveLate(pub, err)
		return
	}
	pub.done(pub.sink.fail(err, true))
}

// lateAsync is an asynchronous publication whose PubAck timed out, waiting for
// its callback to run.
type lateAsync struct {
	pub *asyncPub
	err error
}

// resolveLate runs a timed-out publication's callback on the backend's late
// goroutine, starting it when none is running.
//
// The client library reports this failure, and only this one, from its PubAck
// timer while it still holds its publisher lock. The core lets a callback
// publish again, a publication takes that lock, and so a callback run here
// would wait for ever on the goroutine that holds it. Every other failure is
// reported after the library has let go, which is why the rest still run
// where they are reported and no goroutine exists until a PubAck is lost.
func (b *Backend) resolveLate(pub *asyncPub, err error) {
	start, closed := b.queueLate(lateAsync{pub: pub, err: err})
	switch {
	case closed:
		// Teardown has closed every sink, so a callback that publishes
		// again is refused before it reaches the library's lock.
		pub.done(pub.sink.fail(err, true))
	case start:
		go b.runLate()
	}
}

// queueLate adds one timed-out publication to the queue and reports whether
// the caller must start the goroutine that drains it, or whether teardown has
// already stopped taking any.
func (b *Backend) queueLate(l lateAsync) (start, closed bool) {
	b.lmu.Lock()
	defer b.lmu.Unlock()
	if b.lateClosed {
		return false, true
	}
	b.late = append(b.late, l)
	if b.resolving {
		return false, false
	}
	b.resolving = true
	b.lateWG.Add(1)
	return true, false
}

// takeLate empties the queue, and marks the goroutine gone when there was
// nothing in it, so that the next timeout starts another.
func (b *Backend) takeLate() []lateAsync {
	b.lmu.Lock()
	defer b.lmu.Unlock()
	batch := b.late
	b.late = nil
	if len(batch) == 0 {
		b.resolving = false
	}
	return batch
}

// runLate is the late goroutine: it runs queued callbacks until none is left.
func (b *Backend) runLate() {
	defer b.lateWG.Done()
	for batch := b.takeLate(); len(batch) > 0; batch = b.takeLate() {
		for _, l := range batch {
			l.pub.done(l.pub.sink.fail(l.err, true))
		}
	}
}

// stopLate waits for the late goroutine, and makes any timeout reported after
// it run its callback where it is reported.
func (b *Backend) stopLate() {
	b.lmu.Lock()
	b.lateClosed = true
	b.lmu.Unlock()
	b.lateWG.Wait()
}

// resolveSinkAsync resolves every asynchronous publication of one sink with
// CodeSettlementUnknown, which is what a Close that outruns a PubAck means.
//
// It is taken under the same lock the callbacks use, so a publication either
// resolves through its callback or through here and never through both: the specification's
// "exactly once" has to hold across a close, and a double release corrupts a
// caller's message pool rather than merely losing a report.
func (b *Backend) resolveSinkAsync(s *sink) {
	b.amu.Lock()
	var held []*asyncPub
	for native, pub := range b.async {
		if pub.sink == s {
			held = append(held, pub)
			delete(b.async, native)
		}
	}
	b.amu.Unlock()
	for _, pub := range held {
		pub.done(errPublishUnknownAtClose)
	}
}

// resolveDropped completes, with CodeSettlementUnknown, every asynchronous
// publication the client library dropped at the disconnection a reconnection
// has ended.
//
// nats.go v1.54.0 drops every publication outstanding when the link goes and
// defers each one's error callback inside the loop that watches the
// connection, so those callbacks run only when the loop returns, which is when
// the connection closes. Until then the caller's callback, the publication's
// place in the core's completion table and every Flush waited, and the PubAck
// timeout could not help, because the library had already forgotten the
// publication. What the library does do at the drop is fail the publication's
// future with ErrDisconnected, and that is what this reads: a publication the
// library kept, such as one made after the drop and buffered across the
// outage, has an untouched future and is left to its PubAck.
//
// The answer is SettlementUnknown because it is the truth: the message may
// have reached the stream and only its PubAck been lost. It is exactly once
// against the library's own callback, which still arrives at the close: both
// take the publication out of the map under amu, and whichever comes second
// finds nothing.
//
// amu is not held while a future is read or a callback runs. A future takes
// the library's publisher lock, which the library holds while it calls
// onAsyncErr for a timed-out PubAck, and onAsyncErr takes amu, so reading a
// future under amu would take the two locks in the opposite order. A callback
// may publish again, which takes the publisher lock too.
//
// The library's watcher is a goroutine of its own and is not ordered with the
// reconnect callback this runs from. It drops the publications long before the
// reconnection is reported, which is a dial and a round trip later, but one it
// dropped after this sweep would wait for the next reconnection or the close.
func (b *Backend) resolveDropped() {
	b.amu.Lock()
	b.sweeps++
	held := make([]heldAsync, 0, len(b.async))
	for native, pub := range b.async {
		if pub.fut != nil {
			held = append(held, heldAsync{native: native, fut: pub.fut})
		}
	}
	b.amu.Unlock()
	for _, h := range held {
		b.resolveIfDropped(h.native, h.fut)
	}
}

// heldAsync is one publication whose future a sweep reads, copied out of the
// map so that the future is read with amu released.
type heldAsync struct {
	native *natsgo.Msg
	fut    js.PubAckFuture
}

// resolveIfDropped completes one publication if the client library dropped it
// at a disconnection and nothing has completed it since.
func (b *Backend) resolveIfDropped(native *natsgo.Msg, fut js.PubAckFuture) {
	if !droppedAtDisconnect(fut) {
		return
	}
	pub := b.takeAsync(native)
	if pub == nil {
		return
	}
	pub.done(errPublishUnknownAtReconnect)
}

// droppedAtDisconnect reports whether the client library failed a future with
// ErrDisconnected, which it does only when a disconnection drops the
// publication.
//
// It does not wait. Taking some other error off the channel loses nothing:
// every other outcome reaches the backend through the connection-wide
// callbacks, and nothing else here reads a PublishAsync future.
func droppedAtDisconnect(fut js.PubAckFuture) bool {
	select {
	case err := <-fut.Err():
		return errors.Is(err, natsgo.ErrDisconnected)
	default:
		return false
	}
}

// AsyncPending is how many asynchronous publications of this backend are
// outstanding, which is the number [Config.PublishAsyncMaxPending] bounds.
func (b *Backend) AsyncPending() int { return b.js.PublishAsyncPending() }

// The asynchronous publication path's own answers.
var (
	errNilCallback = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: PublishAsync was given no callback; the specification makes the callback the only report an asynchronous publication has")
	errPublishUnknownAtClose = errcode.New(errcode.SettlementUnknown,
		"iqmq/jetstream: the publisher closed while this publication was outstanding, so whether the stream holds it is unknown (the API)")
	errPublishUnknownAtReconnect = errcode.New(errcode.SettlementUnknown,
		"iqmq/jetstream: the connection was lost while this publication was outstanding, so whether the stream holds it is unknown")
	errDedupKey = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: the de-duplication key is not a value a Nats-Msg-Id header can carry")
)

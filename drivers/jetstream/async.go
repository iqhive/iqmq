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
type asyncPub struct {
	sink *sink
	msg  *mq.Message
	done func(error)
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
	b.async[native] = pub
	b.amu.Unlock()
}

// untrackAsync takes a publication back, for the one case where the write never
// happened, and reports whether it was still waiting.
//
// It was not when the library registered the publication and then answered it
// before its write failed: a disconnection dropped it, or its PubAck timer
// fired. Its callback has then run, or is running, and the caller must not be
// told a second time.
func (b *Backend) untrackAsync(native *natsgo.Msg) bool {
	return b.takeAsync(native) != nil
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
//
// A dropped publication is answered here by name, because the error the library
// gives it, ErrDisconnected, is the one its synchronous calls return before
// anything was sent, and the error map reads it that way. The library drops
// every publication outstanding when the link goes and reports each one at
// once, outside its publisher lock, so a callback that publishes again does not
// wait on it. The message may have reached the stream and only its PubAck been
// lost, so the answer is SettlementUnknown.
func (b *Backend) onAsyncErr(_ js.JetStream, native *natsgo.Msg, err error) {
	pub := b.takeAsync(native)
	if pub == nil {
		return
	}
	switch {
	case errors.Is(err, js.ErrAsyncPublishTimeout):
		b.resolveLate(pub, err)
	case errors.Is(err, natsgo.ErrDisconnected):
		pub.done(errPublishUnknownAtReconnect)
	default:
		pub.done(pub.sink.fail(err, true))
	}
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

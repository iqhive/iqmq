package jetstream

import (
	natsgo "github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
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
	pub.done(pub.sink.fail(err, true))
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
	errDedupKey = errcode.New(errcode.InvalidArgument,
		"iqmq/jetstream: the de-duplication key is not a value a Nats-Msg-Id header can carry")
)

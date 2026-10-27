package rabbitfake

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// ChannelHandle is a test's handle on one channel the driver opened.
//
// Every fault a RabbitMQ driver has to survive is here, and they are here as
// methods rather than as configuration for one reason: a fault has a MOMENT.
// "Close the channel" is not interesting; "close the channel while a
// publication's confirm is outstanding" is, and the only way to express
// it is to publish, then close. [Config.OnChannel] hands the handle over at the
// instant the driver opens the channel, so a fault can be armed before the
// driver's first frame, and [Broker.LastChannel] finds it afterwards.
type ChannelHandle struct{ ch *fakeChan }

// ID is the channel's number within its connection, counting from one, which is
// the channel id AMQP puts in every frame header.
func (h *ChannelHandle) ID() int { return h.ch.id }

// Connection is the connection this channel is on.
func (h *ChannelHandle) Connection() *ConnHandle { return &ConnHandle{c: h.ch.conn} }

// IsClosed reports whether the channel has closed.
func (h *ChannelHandle) IsClosed() bool { return h.ch.IsClosed() }

// Cause is the channel exception that closed the channel, or nil when it is
// open or was closed by the client.
func (h *ChannelHandle) Cause() *amqp.Error {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	return h.ch.cause
}

// IsConfirming reports whether `confirm.select` has been sent on this channel.
func (h *ChannelHandle) IsConfirming() bool {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	return h.ch.confirming
}

// Published is how many publications this channel has made, which is the last
// delivery tag it issued. [client.Channel.GetNextPublishSeqNo] is one more than
// this, and a test that finds them disagreeing has found something publishing
// outside the driver's own lock.
func (h *ChannelHandle) Published() uint64 { return h.ch.confirms.nextSeqNo() - 1 }

// PendingConfirms is the delivery tags this channel has published and not had
// resolved, in tag order.
func (h *ChannelHandle) PendingConfirms() []uint64 { return h.ch.confirms.pending() }

// ConfirmResolutions is how each publication's confirm was resolved, in the
// order the resolutions happened.
//
// It is the observation the specification needs and the seam cannot give: the library
// resolves a deferred confirmation by closing a channel nobody above the seam
// holds, so "what did the close do to publication 3" is otherwise a question
// only the library can answer. Here it is a list.
func (h *ChannelHandle) ConfirmResolutions() []Resolution { return h.ch.confirms.resolutions() }

// DeliveryTagsIssued is the last delivery tag this channel has handed out, so
// that a test can ask what the broker believes it issued rather than inferring
// it from what it managed to read.
//
// The distinction matters as soon as a channel has two consumers. The tags are
// issued consecutively by the CHANNEL, but they arrive on two different Go
// channels, so a reader that takes the second consumer's delivery first sees
// tag 2 before tag 1 -- and has learned nothing about the broker.
// [FuzzRabbitFake] asserted the read order was the issue order and this is what
// it should have asserted against.
func (h *ChannelHandle) DeliveryTagsIssued() uint64 {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	return h.ch.nextTag
}

// Prefetch is both prefetch counts in force, zero for unbounded: the
// per-consumer one that the next `basic.consume` will snapshot, and the
// channel-wide one that bounds this channel's total.
//
// Both, rather than one and a flag saying which, because RabbitMQ enforces
// them independently of each other and a channel that has set both is bounded
// by both -- see [fakeChan.Qos].
func (h *ChannelHandle) Prefetch() (perConsumer, channelWide int) {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	return h.ch.conPrefetch, h.ch.chPrefetch
}

// Unacked is how many deliveries this channel holds unsettled.
func (h *ChannelHandle) Unacked() int {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	return h.ch.outstanding
}

// UnackedTags is the delivery tags this channel holds unsettled, in order.
func (h *ChannelHandle) UnackedTags() []uint64 {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]uint64(nil), h.ch.unackedTags...)
}

// Consumers is the consumer tags registered on this channel, in registration
// order, cancelled ones excluded.
func (h *ChannelHandle) Consumers() []string {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(h.ch.consumerOrder))
	for _, tag := range h.ch.consumerOrder {
		if con := h.ch.consumers[tag]; con != nil && !con.cancelled {
			out = append(out, tag)
		}
	}
	return out
}

// ConsumerStates is the prefetch accounting of every consumer on this
// channel, in registration order.
//
// It is per CONSUMER because that is what `basic.qos` bounds when global is
// false, which is RabbitMQ's own reading and the driver's: a channel with two
// consumers and a prefetch of one holds two unacknowledged deliveries, one
// each, and a test that asserted on the channel's total would call that a
// breach. [ChannelHandle.Prefetch] reports both readings; a consumer's own
// Prefetch here is the snapshot its `basic.consume` took, which no later qos
// moves.
func (h *ChannelHandle) ConsumerStates() []ConsumerState {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]ConsumerState, 0, len(h.ch.consumerOrder))
	for _, tag := range h.ch.consumerOrder {
		con := h.ch.consumers[tag]
		if con == nil {
			continue
		}
		state := ConsumerState{
			Tag:         tag,
			Prefetch:    con.prefetch,
			Outstanding: con.outstanding,
			AutoAck:     con.autoAck,
			Cancelled:   con.cancelled,
			Exclusive:   con.exclusive,
			Args:        cloneTable(con.args),
		}
		if con.q != nil {
			state.Queue = con.q.name
		}
		out = append(out, state)
	}
	return out
}

// SetConfirmScript changes how this channel's confirms are framed. It takes
// effect for the publications that follow; anything a previous script was
// holding is flushed first, so that a change of script never loses a confirm.
func (h *ChannelHandle) SetConfirmScript(s ConfirmScript) {
	b := h.ch.broker()
	b.mu.Lock()
	h.ch.flushConfirmsLocked()
	h.ch.script = s
	b.mu.Unlock()
	h.ch.conn.sync()
}

// FlushConfirms sends whatever a cumulative or reversed script is still
// holding, which is how a test ends a batch that has not reached its size.
func (h *ChannelHandle) FlushConfirms() {
	b := h.ch.broker()
	b.mu.Lock()
	h.ch.flushConfirmsLocked()
	b.mu.Unlock()
	h.ch.conn.sync()
}

// AckPublication sends `basic.ack{multiple=false}` for one delivery tag, which
// the library resequences.
//
// It is for [ConfirmManual], where the test writes the interleaving itself: a
// tag confirmed out of order, a tag confirmed twice, a tag that was never
// published. All three are things a broker will not do and a driver has to
// survive, which is why they are reachable.
func (h *ChannelHandle) AckPublication(tag uint64) { h.confirm(tag, true, false) }

// NackPublication sends `basic.nack{multiple=false}` for one delivery tag.
func (h *ChannelHandle) NackPublication(tag uint64) { h.confirm(tag, false, false) }

// AckPublicationsThrough sends `basic.ack{multiple=true}` naming one delivery
// tag, which the library EXPANDS into one confirmation per tag up to and
// including it. It is the frame the driver never sees and the reason this fake
// exists.
func (h *ChannelHandle) AckPublicationsThrough(tag uint64) { h.confirm(tag, true, true) }

// NackPublicationsThrough sends `basic.nack{multiple=true}` naming one
// delivery tag.
func (h *ChannelHandle) NackPublicationsThrough(tag uint64) { h.confirm(tag, false, true) }

// confirm posts one confirm frame and waits for it to be dispatched, so that a
// test's next assertion is about a confirm that has already arrived.
func (h *ChannelHandle) confirm(tag uint64, ack, multiple bool) {
	h.ch.postConfirm(tag, ack, multiple)
	h.ch.conn.sync()
}

// NackNextPublishes makes the broker answer the next n publications with
// `basic.nack` instead of `basic.ack`.
//
// This is what "the broker refused the publication" is on AMQP 0-9-1: there is
// no synchronous refusal, so a publisher learns about it from a negative
// confirm and from nothing else. A driver that treated a nack as a transport
// error would retry a publication the broker has definitively refused, for
// ever.
func (h *ChannelHandle) NackNextPublishes(n int) {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	h.ch.nackNext = n
}

// FailNextPublishes makes the next n calls to `PublishWithContext` return err
// without publishing anything.
//
// It is a different fault from [ChannelHandle.NackNextPublishes] and the
// difference is the one turns on. A nack is the BROKER refusing a
// publication it received; this is the write never leaving, which the library
// answers by handing the delivery tag back (`confirms.unpublish`) -- the one
// place its counter moves backwards. A driver that assigns its own tags under
// its own lock has to move backwards with it, or every confirm after this one
// is attributed to the wrong publication.
func (h *ChannelHandle) FailNextPublishes(n int, err error) {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	for range n {
		h.ch.failPublish = append(h.ch.failPublish, err)
	}
}

// FailNextDeclare makes the next declaration on this channel fail with err,
// which closes the channel exactly as a real refusal does.
//
// The channel close is the point. A declaration is synchronous, so a driver
// sees the error return; what it also has to handle is that the channel it was
// declaring on is now dead, and every publication in flight on it with it.
func (h *ChannelHandle) FailNextDeclare(err *amqp.Error) {
	b := h.ch.broker()
	b.mu.Lock()
	defer b.mu.Unlock()
	h.ch.failDeclare = err
}

// HoldNextPublish makes the next `PublishWithContext` on this channel block
// until the returned function is called.
//
// It is how a test puts something in the middle of a publish: close the
// channel while a publication is parked, cancel the caller's context while it
// is parked, or let a second publisher overtake it. The returned release is
// idempotent, and a held publish is also released by the caller's context
// ending or by the channel closing, so a test that forgets to release cannot
// hang.
func (h *ChannelHandle) HoldNextPublish() (release func()) {
	hold := make(chan struct{})
	b := h.ch.broker()
	b.mu.Lock()
	h.ch.hold = hold
	b.mu.Unlock()
	var once bool
	return func() {
		if once {
			return
		}
		once = true
		close(hold)
	}
}

// CloseWith closes the channel as the BROKER would: err reaches every
// `NotifyClose` listener, every delivery the channel was holding goes back to
// its queue, and the confirm bookkeeping resolves what was outstanding the way
// `deferredConfirmations.Close` resolves it -- with `ack=false`, discarding
// whatever the sequencer was holding, and closing `NotifyPublish`.
//
// That last part is the behaviour the driver exists to absorb. The library
// reports a DEFINITE negative for a publication the broker may well have
// taken; the specification calls it ambiguous and makes reporting it as a failure a
// settlement disqualifier. A driver has to be held to that, and this is the
// only way to arrange it.
func (h *ChannelHandle) CloseWith(err *amqp.Error) { h.ch.closeWith(err) }

// CancelConsumer cancels one consumer from the BROKER side -- `basic.cancel`,
// which is what reaches `NotifyCancel` -- and reports whether there was such a
// consumer.
//
// A real broker sends it when the queue a consumer was on is deleted, which is
// why deleting a queue through the seam produces it too. This is the direct
// form, for a test that wants the notification without the topology change.
func (h *ChannelHandle) CancelConsumer(tag string) bool {
	b := h.ch.broker()
	b.mu.Lock()
	con := h.ch.consumers[tag]
	if con == nil || con.cancelled {
		b.mu.Unlock()
		return false
	}
	con.cancelled = true
	if con.q != nil {
		con.q.consumers = withoutConsumer(con.q.consumers, con)
	}
	b.pumpLocked()
	b.mu.Unlock()
	h.ch.conn.enqueue(frame{kind: frCancel, ch: h.ch, tag: tag, notify: true})
	h.ch.conn.sync()
	return true
}

// Flow emits `channel.flow`, RabbitMQ's per-channel back-pressure signal.
//
// The fake does not itself stop taking publications when flow is off, and that
// is deliberate: what a driver has to do with `channel.flow` is stop
// publishing, and a fake that enforced it would make a driver that ignored the
// signal look correct because the broker refused its publications anyway.
func (h *ChannelHandle) Flow(active bool) {
	h.ch.conn.enqueue(frame{kind: frFlow, ch: h.ch, active: active})
	h.ch.conn.sync()
}

// Sync waits until this channel's connection has dispatched everything already
// enqueued.
func (h *ChannelHandle) Sync() { h.ch.conn.sync() }

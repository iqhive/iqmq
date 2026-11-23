package rabbitmq

import (
	"context"
	"iter"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

var _ mq.Admin = (*admin)(nil)

// Admin returns the topology surface (AdminBackend).
//
// It is always available and it is not always allowed to CREATE: a client
// whose [Config.Declare] is off answers ErrUnsupported to every creating call
// and still answers the inspecting and destroying ones. That split is the specification:
// "Open never creates production topology" is about what a driver does BEHIND
// a caller's back, and an explicit `EnsureTopic` is not behind anybody's back
// — but a deployment that turned declaration off did so for a reason, and a
// driver that honoured it for the publish path and ignored it for the admin
// path would be honouring it nowhere.
func (b *Backend) Admin() (mq.Admin, error) { return (*admin)(b), nil }

// admin is the backend under another name: the topology calls need the
// connection, the naming and the error mapping, and nothing else.
type admin Backend

// backend is the admin's own backend, for the methods that read it.
func (a *admin) backend() *Backend { return (*Backend)(a) }

// EnsureTopic declares a topic's exchange.
func (a *admin) EnsureTopic(ctx context.Context, spec mq.TopicSpec) error {
	b := a.backend()
	if !b.cfg.Declare {
		return b.errs.Map(mq.OpAdmin, spec.Name, "", errAdminNeedsDeclare)
	}
	n, err := b.names(spec.Name, "")
	if err != nil {
		return err
	}
	return b.onChannel(ctx, spec.Name, "", func(ch client.Channel) error {
		return b.ensure(ctx, ch, n, false)
	})
}

// EnsureSubscription declares a subscription's queue, its binding and its
// dead-letter route.
func (a *admin) EnsureSubscription(ctx context.Context, spec mq.SubscriptionSpec) error {
	b := a.backend()
	if !b.cfg.Declare {
		return b.errs.Map(mq.OpAdmin, spec.Topic, spec.Name, errAdminNeedsDeclare)
	}
	n, err := b.names(spec.Topic, spec.Name)
	if err != nil {
		return err
	}
	return b.onChannel(ctx, spec.Topic, spec.Name, func(ch client.Channel) error {
		return b.ensure(ctx, ch, n, true)
	})
}

// DeleteTopic deletes a topic's exchange and, on the plugin arm, its delayed
// exchange once no subscription is bound to it.
//
// It deletes the EXCHANGE and not the queues bound to it, because they are
// other subscriptions' and deleting one would cancel a consumer that has
// nothing to do with this call. A caller who wants the queues gone deletes the
// subscriptions.
//
// The delayed exchange, `<exchange>.delayed.direct`, is the topic's and was
// left behind, at the broker and in the ledger, so a reconnection declared it
// again (TestDeleteTopicDeletesThePluginArmsDelayedExchange). It is deleted
// if-unused: each subscription on the plugin arm binds its queue to it, and
// it holds their parked copies, which deleting it with a subscription still
// bound would lose. Such a subscription keeps it until it is deleted too.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	b := a.backend()
	n, err := b.names(topic, "")
	if err != nil {
		return err
	}
	err = b.onChannel(ctx, topic, "", func(ch client.Channel) error {
		if derr := ch.ExchangeDelete(n.Exchange, false, false); derr != nil {
			return b.errs.Map(mq.OpAdmin, topic, "", derr)
		}
		// The ledger has to forget it too, or a later EnsureTopic is
		// skipped and a reconnection re-declares what the caller deleted.
		b.topo.dropDeleted(decl{kind: declExchange, name: n.Exchange})
		return nil
	})
	if err != nil || !pluginArm(b.fx) {
		return err
	}
	// A channel of its own, because the broker answers an exchange in use
	// with 406 PRECONDITION_FAILED, which closes the channel it is made on.
	return b.onChannel(ctx, topic, "", func(ch client.Channel) error {
		derr := ch.ExchangeDelete(n.DelayExchange, true, false)
		if code, ok := ReplyCode(derr); ok && code == amqp.PreconditionFailed {
			return nil
		}
		if derr != nil {
			return b.errs.Map(mq.OpAdmin, topic, "", derr)
		}
		b.topo.dropDeleted(decl{kind: declExchange, name: n.DelayExchange})
		return nil
	})
}

// DeleteSubscription deletes a subscription's queue and, with
// [Config.DeadLetter] on, its dead-letter queue and exchange, and with them
// the dead letters the queue holds.
//
// The dead-letter route is the subscription's own, named after its queue. It
// was left behind, at the broker and in the ledger, so every reconnection
// declared it again for a subscription that no longer existed
// (TestDeleteSubscriptionDeletesItsDeadLetters). Under
// [Config.DeadLetterTopic] the dead letters go to another topic, whose exchange
// is that topic's, and it stays. The queue goes first, so that nothing is
// dead-lettered into a route that is going.
//
// Off the plugin arm the subscription's delay queues go too, with the parked
// copies in them: those copies would come back to the queue that is going.
// They are the delayed nacks' own, named after the queue, and they stayed at
// the broker, for good under the default DelayQueueTTL, and in the ledger, so
// a reconnection declared them again
// (TestDeleteSubscriptionDeletesItsDelayQueues). See
// [Backend.deleteDelayQueues] for which.
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	b := a.backend()
	n, err := b.names(topic, sub)
	if err != nil {
		return err
	}
	gone := []decl{{kind: declQueue, name: n.Queue}}
	if b.cfg.DeadLetter && n.DeadLetterQueue != "" {
		gone = append(gone,
			decl{kind: declQueue, name: n.DeadLetterQueue},
			decl{kind: declExchange, name: n.DeadLetterExchange},
		)
	}
	err = b.onChannel(ctx, topic, sub, func(ch client.Channel) error {
		for _, d := range gone {
			var derr error
			switch d.kind {
			case declQueue:
				_, derr = ch.QueueDelete(d.name, false, false, false)
			default:
				derr = ch.ExchangeDelete(d.name, false, false)
			}
			if derr != nil {
				return b.errs.Map(mq.OpAdmin, topic, sub, derr)
			}
			// [admin.DeleteTopic]'s reason.
			b.topo.dropDeleted(d)
		}
		return nil
	})
	if err != nil || pluginArm(b.fx) {
		return err
	}
	return b.deleteDelayQueues(ctx, topic, sub, n.Queue)
}

// delayQueues are the ttldlx arm's delay queues for queue, one for each bucket
// of the client's ladder, as a delayed nack's plan declares them.
func (b *Backend) delayQueues(queue string) []decl {
	ladder := b.cfg.DelayLadder
	if len(ladder) == 0 {
		ladder = delay.DefaultLadder()
	}
	ttl, err := delay.New(delay.TTLDLX, delay.Config{Ladder: ladder, QueueTTL: b.cfg.DelayQueueTTL})
	if err != nil {
		// Validate refused such a ladder at Open.
		return nil
	}
	out := make([]decl, 0, len(ladder))
	for _, bucket := range ladder {
		p, err := ttl.Plan(delay.Request{Target: delay.RedeliveryTarget(queue), Delay: bucket})
		if err != nil || p.Declare == nil {
			continue
		}
		out = append(out, decl{kind: declQueue, name: p.Declare.Name, args: delayQueueArgs(p.Declare)})
	}
	return out
}

// deleteDelayQueues deletes queue's delay queues ([Backend.delayQueues]) and
// drops them from the ledger. Every bucket's is deleted and not only those the
// ledger holds, because the client deleting a subscription is often not the
// one that nacked: a CLI, or the process after a restart.
//
// A bucket's name can be another subscription's queue: the topic
// `orders.workers.delay` and its subscription `3200` have the queue
// `orders.workers.delay.3200`, which is also the 3.2 s bucket of
// orders/workers. So each name is declared first with the arguments a delayed
// nack declares it with, and deleted only when the broker takes that: a queue
// of that name with other arguments is answered with 406 PRECONDITION_FAILED,
// and is left alone. An exchange of that topic's name used to be the test,
// which a deleted topic, whose subscriptions keep their queues, passed, and the
// other subscription was deleted with its messages
// (TestDeleteSubscriptionDeletesItsDelayQueues). The declaration and the delete
// are one after the other on one channel, so a queue another client declares
// under the name in between meets the same 406 itself. A bucket that is not
// there is made and deleted at once. A delay queue declared with another
// DelayQueueTTL, or under another ladder, is left too, and so is another
// connection's exclusive queue (405 RESOURCE_LOCKED) and a durable queue whose
// node is down (404 NOT_FOUND), which cannot be asked about. Each refusal
// closes the channel, so the next name is asked on a new one.
//
// The declaration needs more than the delete: configure on the name, read on
// it and write on the default exchange, which it dead-letters to. A user who
// may only delete was refused with 403 ACCESS_REFUSED after the subscription's
// queue had gone, and every retry was refused again (review 11, R2-B1). Such a
// user cannot make a delayed nack on this arm either, so a refusal ends the
// sweep, nothing more is deleted, and the call succeeds: the buckets left are
// another user's. With Config.Declare off nothing is declared at all, and only
// the delay queues the ledger holds are deleted ([Backend.deleteRecordedDelays]).
func (b *Backend) deleteDelayQueues(ctx context.Context, topic, sub, queue string) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	if !b.cfg.Declare {
		return b.deleteRecordedDelays(ctx, topic, sub, queue)
	}
	var ch client.Channel
	for _, q := range b.delayQueues(queue) {
		if err := ctx.Err(); err != nil {
			if ch != nil {
				_ = ch.Close()
			}
			return err
		}
		if ch == nil {
			c, err := b.channelOnWithin(ctx, b.conn())
			if err != nil {
				return err
			}
			ch = c
		}
		if err := apply(ch, q); err != nil {
			code, _ := ReplyCode(err)
			switch code {
			case amqp.AccessRefused:
				return nil
			case amqp.PreconditionFailed, amqp.ResourceLocked, amqp.NotFound:
				ch = nil
				continue
			}
			_ = ch.Close()
			return b.errs.Map(mq.OpAdmin, topic, sub, err)
		}
		if _, err := ch.QueueDelete(q.name, false, false, false); err != nil {
			_ = ch.Close()
			return b.errs.Map(mq.OpAdmin, topic, sub, err)
		}
		b.topo.dropDeleted(q)
	}
	if ch == nil {
		return nil
	}
	if cerr := ch.Close(); cerr != nil && !isBenignClose(cerr) {
		return b.errs.Map(mq.OpAdmin, topic, sub, cerr)
	}
	return nil
}

// deleteRecordedDelays deletes the delay queues of queue the ledger holds, by
// their name and their arguments, which this client declared, and declares
// nothing. It is [Backend.deleteDelayQueues] with Config.Declare off, under
// which a client declares no delay queue, so the ledger holds none and the
// buckets other clients made stay.
func (b *Backend) deleteRecordedDelays(ctx context.Context, topic, sub, queue string) error {
	var gone []decl
	for _, d := range b.topo.all() {
		if d.kind == declQueue && strings.HasPrefix(d.name, queue+rabbit.DelayPrefix) &&
			d.args["x-dead-letter-routing-key"] == queue {
			gone = append(gone, d)
		}
	}
	if len(gone) == 0 {
		return nil
	}
	return b.onChannel(ctx, topic, sub, func(ch client.Channel) error {
		for _, d := range gone {
			if _, err := ch.QueueDelete(d.name, false, false, false); err != nil {
				return b.errs.Map(mq.OpAdmin, topic, sub, err)
			}
			b.topo.dropDeleted(d)
		}
		return nil
	})
}

// Purge empties a subscription's queue.
//
// It purges the subscription's own queue and NOT its dead-letter queue, which
// is the answer a caller wants almost every time: a purge is for clearing a
// backlog, and the dead-letter queue is the record of what could not be
// processed.
func (a *admin) Purge(ctx context.Context, topic, sub string) error {
	b := a.backend()
	n, err := b.names(topic, sub)
	if err != nil {
		return err
	}
	return b.onChannel(ctx, topic, sub, func(ch client.Channel) error {
		if _, perr := ch.QueuePurge(n.Queue, false); perr != nil {
			return b.errs.Map(mq.OpAdmin, topic, sub, perr)
		}
		return nil
	})
}

// Stats reports a subscription's depth and in-flight count.
//
// The LAG is not reported, and that is the honest answer rather than a gap: a
// queue has no position and therefore no lag. `Depth` is how many messages are
// waiting and `InFlight` is how many have been delivered and not settled, both
// of which `queue.declare-ok` reports; "how far behind is this subscription"
// has no meaning when there is nothing to be behind. The rule that an
// unknown is never an overloaded zero is what makes saying so possible.
//
// The declare is PASSIVE, because Stats must not create: a stats call that
// created an empty queue would be the worst possible answer to "how deep is
// this queue".
func (a *admin) Stats(ctx context.Context, topic, sub string) (mq.QueueStats, error) {
	b := a.backend()
	n, err := b.names(topic, sub)
	if err != nil {
		return mq.QueueStats{}, err
	}
	var stats mq.QueueStats
	err = b.onChannel(ctx, topic, sub, func(ch client.Channel) error {
		q, derr := ch.QueueDeclarePassive(n.Queue, true, false, false, false, nil)
		if derr != nil {
			return b.errs.Map(mq.OpAdmin, topic, sub, derr)
		}
		// `queue.declare-ok` reports the messages NOT awaiting
		// acknowledgement, so the depth is exactly that and the in-flight
		// count is not in the frame at all. It is read from this client's own
		// token tables, which know what THIS client is holding -- and that is
		// all a client can honestly say: another consumer's unsettled
		// deliveries are invisible from here, so reporting a total would be
		// reporting a number this client cannot know.
		stats.Depth = mq.Limit[int64]{Value: int64(q.Messages), Known: true}
		stats.InFlight = mq.Limit[int64]{Value: b.inFlight(n.Queue), Known: true}
		return nil
	})
	return stats, err
}

// inFlight is how many deliveries THIS client is holding unsettled on a queue.
//
// It is this client's own count and not the broker's, and Stats says so: a
// queue's total unacknowledged count includes every other consumer's, which
// nothing on this connection can see. Reporting the local number is the only
// figure that is true.
func (b *Backend) inFlight(queue string) int64 {
	_, srcs := b.children()
	var n int64
	for _, s := range srcs {
		if s.names.Queue == queue {
			n += int64(s.tokens.Live())
		}
	}
	return n
}

// ListTopics iterates the topics this client knows about.
//
// AMQP 0-9-1 has no way to list exchanges: the protocol has no such method,
// and RabbitMQ exposes the list through its HTTP management API, which is a
// different service on a different port with its own credentials. So this
// iterates what THIS CLIENT has declared, and the driver page says so —
// a caller who needs the broker's whole exchange list uses the management API,
// and one who wants to know what their own client built gets it here.
//
// A topic is its exchange, which is what [admin.DeleteTopic] deletes, so the
// list is of the topic exchanges this client declared, a dead-letter topic's
// included (the `topic` of a [decl]). It was of the subscription queues, which
// DeleteTopic leaves to their subscriptions: a deleted topic stayed listed for
// as long as one of its subscriptions did, and a topic made with EnsureTopic
// and no subscription was not listed
// (TestListTopicsListsWhatDeleteTopicRemoves). The exchanges the driver
// derives, a subscription's dead-letter exchange and the delayed-message ones,
// are not topics. Which declarations are a topic's is recorded and not read off
// the name, because a topic may be named like a derived name, "orders.dlq" or
// "events.dlx" (TestListTopicsListsTopicsNamedLikeTheDriversDerivedNames).
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	b := a.backend()
	return func(yield func(string, error) bool) {
		if err := b.CheckOpen(ctx); err != nil {
			yield("", err)
			return
		}
		for _, d := range b.topo.all() {
			if d.kind != declExchange || d.topic == "" {
				continue
			}
			if !yield(d.topic, nil) {
				return
			}
		}
	}
}

// onChannel runs one administrative operation on a channel of its own.
//
// A channel of its own is not tidiness: every one of these operations can fail
// with a channel exception — 404 for a queue that does not exist, 406 for a
// mismatched re-declaration, 403 for a reserved name — and a failure closes
// the channel it was made on. Sharing a channel between two administrative
// calls would make the first failure break the second, and sharing it with a
// publisher would make an administrative typo lose that publisher's in-flight
// confirms.
func (b *Backend) onChannel(ctx context.Context, topic, sub string, fn func(client.Channel) error) error {
	if err := b.CheckOpen(ctx); err != nil {
		return err
	}
	ch, err := b.channelOnWithin(ctx, b.conn())
	if err != nil {
		return err
	}
	ferr := fn(ch)
	cerr := ch.Close()
	if ferr != nil {
		return ferr
	}
	if cerr != nil && !isBenignClose(cerr) {
		return b.errs.Map(mq.OpAdmin, topic, sub, cerr)
	}
	return nil
}

// Names is the topology one topic and one subscription map to, for an operator
// or a test that needs to find it in a broker's own management view.
//
// It is exported because the names are derived rather than configured: a
// caller who has to look at `rabbitmqctl list_queues` needs to know that the
// subscription "workers" of the topic "orders" is the queue "orders.workers"
// and that its dead-letter queue is "orders.workers.dlq", and deriving that by
// hand from the driver page is exactly the kind of thing that goes wrong.
func (b *Backend) Names(topic, sub string) (rabbit.Names, error) { return b.names(topic, sub) }

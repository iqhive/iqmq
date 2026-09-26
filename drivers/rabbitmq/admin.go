package rabbitmq

import (
	"context"
	"iter"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
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

// DeleteTopic deletes a topic's exchange.
//
// It deletes the EXCHANGE and not the queues bound to it, because they are
// other subscriptions' and deleting one would cancel a consumer that has
// nothing to do with this call. A caller who wants the queues gone deletes the
// subscriptions.
func (a *admin) DeleteTopic(ctx context.Context, topic string) error {
	b := a.backend()
	n, err := b.names(topic, "")
	if err != nil {
		return err
	}
	return b.onChannel(ctx, topic, "", func(ch client.Channel) error {
		if derr := ch.ExchangeDelete(n.Exchange, false, false); derr != nil {
			return b.errs.Map(mq.OpAdmin, topic, "", derr)
		}
		return nil
	})
}

// DeleteSubscription deletes a subscription's queue and its dead-letter route
func (a *admin) DeleteSubscription(ctx context.Context, topic, sub string) error {
	b := a.backend()
	n, err := b.names(topic, sub)
	if err != nil {
		return err
	}
	return b.onChannel(ctx, topic, sub, func(ch client.Channel) error {
		if _, derr := ch.QueueDelete(n.Queue, false, false, false); derr != nil {
			return b.errs.Map(mq.OpAdmin, topic, sub, derr)
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
func (a *admin) ListTopics(ctx context.Context) iter.Seq2[string, error] {
	b := a.backend()
	return func(yield func(string, error) bool) {
		if err := b.CheckOpen(ctx); err != nil {
			yield("", err)
			return
		}
		seen := make(map[string]struct{})
		for _, d := range b.topo.all() {
			if d.kind != declQueue {
				continue
			}
			topic, _, ok := rabbit.ParseQueue(b.cfg.Prefix, d.name)
			if !ok {
				continue
			}
			if _, dup := seen[topic]; dup {
				continue
			}
			seen[topic] = struct{}{}
			if !yield(topic, nil) {
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
	ch, err := b.channel()
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

package rabbitfake

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Publication is one message as the broker holds it.
//
// It is a VALUE and not a pointer into the broker's own state, which is the
// only shape an observation can have: a test that held a pointer would watch
// the thing it is asserting about change under it, and a failure message built
// from it would describe the broker as it is now rather than as it was when the
// assertion ran.
type Publication struct {
	// ID is the message's broker-wide identity. Two publications of the same
	// bytes have two identities, which is what lets an assertion say WHICH
	// message was dead-lettered.
	ID uint64
	// Exchange and RoutingKey are the ones the message was published with. A
	// dead-letter hop does not change them; the hop's own exchange and key are
	// on the [DeadLetter] record.
	Exchange   string
	RoutingKey string
	// Body and Headers are copies.
	Body    []byte
	Headers amqp.Table
	// AppID and Type are the `app-id` and `type` properties.
	AppID string
	Type  string
	// Priority is the priority the queue holding it read.
	Priority uint8
	// Redelivered reports that the message has been handed out and given back.
	Redelivered bool
	// Attempts is how many deliveries ended without an acknowledgement, which
	// is what a quorum queue reports in `x-delivery-count`.
	Attempts uint64
}

// publicationOf snapshots a message.
func publicationOf(m *message) Publication {
	return Publication{
		ID:          m.id,
		Exchange:    m.exchange,
		RoutingKey:  m.routingKey,
		Body:        append([]byte(nil), m.pub.Body...),
		Headers:     cloneTable(m.pub.Headers),
		AppID:       m.pub.AppId,
		Type:        m.pub.Type,
		Priority:    m.priority,
		Redelivered: m.redelivered,
		Attempts:    m.attempts,
	}
}

// SettleKind is what a delivery was settled with.
type SettleKind string

// The settlements a delivery can receive.
const (
	// SettleAck is `basic.ack`: the message is gone.
	SettleAck SettleKind = "ack"
	// SettleNack is `basic.nack`.
	SettleNack SettleKind = "nack"
	// SettleReject is `basic.reject`.
	SettleReject SettleKind = "reject"
	// SettleAutoAck is a `no-ack` consumer: the message left the broker at the
	// moment it was handed out and nothing will ever settle it. It is recorded
	// as a settlement so that the message is accounted for, which is what
	// makes "no message is lost" checkable on an at-most-once consumer.
	SettleAutoAck SettleKind = "auto-ack"
	// SettleChannelClosed is the settlement a CHANNEL CLOSE performs on the
	// deliveries it was holding: they go back to their queues, redelivered. It
	// is the one settlement the client did not ask for, and a driver that
	// settles a tag from before it is settling a tag the next channel has
	// reissued.
	SettleChannelClosed SettleKind = "channel-closed"
)

// Settlement is one delivery and what became of it.
type Settlement struct {
	// Delivery is the delivery's broker-wide identity, so that a settlement
	// names a delivery rather than a tag a later channel reused.
	Delivery uint64
	// Channel is the channel's id within its connection.
	Channel int
	// Tag is the delivery tag the channel had issued.
	Tag uint64
	// Queue is where the message came from.
	Queue string
	// Message is the message as it was when it was settled.
	Message Publication
	// Kind is what settled it.
	Kind SettleKind
	// Requeue reports that the message went back to its queue.
	Requeue bool
	// Multiple reports that the settling frame carried `multiple=true`, so
	// that a test can tell a cumulative settlement from a run of individual
	// ones.
	Multiple bool
}

// DeathReason is why a message was dead-lettered. The strings are RabbitMQ's
// own, because they are what it puts in the `x-death` table and what an
// operator reads.
type DeathReason string

// The reasons RabbitMQ dead-letters a message.
const (
	// DeathExpired is a message whose TTL elapsed before it was consumed.
	DeathExpired DeathReason = "expired"
	// DeathRejected is `basic.nack` or `basic.reject` with `requeue=false`.
	DeathRejected DeathReason = "rejected"
	// DeathMaxLen is a message pushed out of a queue that was at its
	// `x-max-length`.
	DeathMaxLen DeathReason = "maxlen"
)

// DeadLetter is one dead-letter hop.
type DeadLetter struct {
	// Message is the message as it was re-published, `x-death` headers and
	// all.
	Message Publication
	// From is the queue it was dead-lettered OUT of.
	From string
	// Exchange and RoutingKey are where it was dead-lettered TO: the queue's
	// `x-dead-letter-exchange`, and its `x-dead-letter-routing-key` or the
	// message's own key when the queue named none.
	Exchange   string
	RoutingKey string
	// Reason is why.
	Reason DeathReason
}

// DropReason is why the broker discarded a message rather than holding it.
//
// Every discard has a record, and that is the point of the type. A broker is
// allowed to drop a message -- an unroutable publication, a reject with no
// dead-letter exchange, a purge -- and a fake that dropped them silently would
// make "no message is lost" uncheckable, because a lost message and a
// legitimately dropped one would look the same.
type DropReason string

// The reasons this broker drops a message.
const (
	// DropUnroutable is a publication no queue was bound for.
	DropUnroutable DropReason = "unroutable"
	// DropRejected is a reject or a nack with `requeue=false` on a queue that
	// names no dead-letter exchange.
	DropRejected DropReason = "rejected with no dead-letter exchange"
	// DropExpired is a TTL that elapsed on a queue that names no dead-letter
	// exchange.
	DropExpired DropReason = "expired with no dead-letter exchange"
	// DropOverflow is a message pushed out of a full queue that names no
	// dead-letter exchange, or refused by `x-overflow: reject-publish`.
	DropOverflow DropReason = "over x-max-length"
	// DropUnroutableDeadLetter is a dead-letter hop whose exchange routed
	// nowhere, which is the failure `internal/rabbit`'s constant dead-letter
	// routing key exists to prevent.
	DropUnroutableDeadLetter DropReason = "dead-lettered to nowhere"
	// DropDeathCycle is a message that took more than [maxDeaths] hops.
	DropDeathCycle DropReason = "dead-letter cycle"
	// DropQueueDeleted is a message that was in a queue when it was deleted.
	DropQueueDeleted DropReason = "queue deleted"
	// DropQueueExpired is a message that was in a queue when its `x-expires`
	// elapsed.
	DropQueueExpired DropReason = "queue expired"
	// DropPurged is `queue.purge`.
	DropPurged DropReason = "purged"
	// DropRefused is a publication the broker answered with a CHANNEL
	// EXCEPTION rather than with a route: an exchange that is not there, or
	// the `immediate` flag RabbitMQ has never implemented.
	//
	// It is the discard a publisher is least likely to expect, because
	// `basic.publish` has no reply: the call returns nil, the message is
	// gone, and the only evidence is the channel dying a moment later. A
	// driver that read the nil as success would report a message nobody has
	// as published, which is why the discard is recorded here where a test
	// can see it.
	DropRefused DropReason = "refused with a channel exception"
	// DropExchangeDeleted is a delayed-message publication that was still
	// being held by an exchange when the exchange was deleted.
	DropExchangeDeleted DropReason = "exchange deleted"
)

// dropFor is the discard reason that goes with a dead-letter reason on a queue
// that names no dead-letter exchange.
func dropFor(reason DeathReason) DropReason {
	switch reason {
	case DeathExpired:
		return DropExpired
	case DeathMaxLen:
		return DropOverflow
	case DeathRejected:
		return DropRejected
	default:
		return DropReason(reason)
	}
}

// Drop is one message the broker discarded, and why.
type Drop struct {
	// Message is the message as it was discarded.
	Message Publication
	// Queue is where it was, or empty for a publication that reached no queue
	// at all.
	Queue string
	// Reason is why.
	Reason DropReason
}

// Binding is one queue bound to one exchange with one key.
type Binding struct {
	// Queue is the bound queue.
	Queue string
	// Key is the binding key, which a topic exchange reads as a pattern.
	Key string
	// Args are the binding arguments, which is where a headers exchange's
	// predicate lives.
	Args amqp.Table
}

// QueueInfo is one queue as a test sees it.
type QueueInfo struct {
	// Name is the queue's name.
	Name string
	// Durable, AutoDelete and Exclusive are the declaration's flags.
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	// Args are the declaration's arguments, as declared.
	Args amqp.Table
	// Quorum reports `x-queue-type: quorum`, which is what decides whether a
	// delivery carries `x-delivery-count`.
	Quorum bool
	// Depth is how many messages are ready to be delivered.
	Depth int
	// Unacked is how many deliveries out of this queue are unsettled, across
	// every channel.
	Unacked int
	// Consumers is how many consumers are registered.
	Consumers int
}

// ExchangeInfo is one exchange as a test sees it.
type ExchangeInfo struct {
	// Name and Kind are the declaration's name and type.
	Name string
	Kind string
	// Durable, AutoDelete and Internal are the declaration's flags.
	Durable    bool
	AutoDelete bool
	Internal   bool
	// Args are the declaration's arguments.
	Args amqp.Table
	// DelayedType is the `x-delayed-type` of a delayed-message exchange.
	DelayedType string
	// Predeclared marks the broker's own exchanges -- the default one and the
	// `amq.*` set -- which a client may bind to and publish through but may
	// not declare.
	Predeclared bool
	// Bindings are the queues bound to it, in declaration order.
	Bindings []Binding
	// Held is how many delayed publications the exchange is holding.
	Held int
}

// Queues is every queue, ordered by name.
//
// By name rather than by declaration order, because a test asserting on the
// whole set wants a stable order that does not change when an unrelated
// declaration is added to the arrangement.
func (b *Broker) Queues() []QueueInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	names := append([]string(nil), b.qOrder...)
	sortStrings(names)
	out := make([]QueueInfo, 0, len(names))
	for _, name := range names {
		if q := b.queues[name]; q != nil {
			out = append(out, queueInfo(q))
		}
	}
	return out
}

// Queue is one queue by name.
func (b *Broker) Queue(name string) (QueueInfo, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[name]
	if q == nil {
		return QueueInfo{}, false
	}
	return queueInfo(q), true
}

// queueInfo snapshots a queue.
func queueInfo(q *queue) QueueInfo {
	return QueueInfo{
		Name:       q.name,
		Durable:    q.durable,
		AutoDelete: q.autoDelete,
		Exclusive:  q.exclusive,
		Args:       cloneTable(q.args),
		Quorum:     q.quorum,
		Depth:      len(q.ready),
		Unacked:    q.unacked,
		Consumers:  len(q.consumers),
	}
}

// Exchanges is every exchange, ordered by name, the broker's own included.
func (b *Broker) Exchanges() []ExchangeInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	names := append([]string(nil), b.exOrder...)
	sortStrings(names)
	out := make([]ExchangeInfo, 0, len(names))
	for _, name := range names {
		if ex := b.exchanges[name]; ex != nil {
			out = append(out, exchangeInfo(ex))
		}
	}
	return out
}

// Exchange is one exchange by name. The default exchange is named "".
func (b *Broker) Exchange(name string) (ExchangeInfo, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ex := b.exchanges[name]
	if ex == nil {
		return ExchangeInfo{}, false
	}
	return exchangeInfo(ex), true
}

// exchangeInfo snapshots an exchange.
func exchangeInfo(ex *exchange) ExchangeInfo {
	bindings := make([]Binding, 0, len(ex.bindings))
	for _, bd := range ex.bindings {
		bindings = append(bindings, Binding{Queue: bd.queue, Key: bd.key, Args: cloneTable(bd.args)})
	}
	return ExchangeInfo{
		Name:        ex.name,
		Kind:        ex.kind,
		Durable:     ex.durable,
		AutoDelete:  ex.autoDelete,
		Internal:    ex.internal,
		Args:        cloneTable(ex.args),
		DelayedType: ex.delayedType,
		Predeclared: ex.predeclared,
		Bindings:    bindings,
		Held:        len(ex.held),
	}
}

// Publications is every message one queue accepted, in the order it accepted
// them.
//
// It is the arrival log and not the current contents: a message that has been
// consumed is still here, and so is one that arrived at a queue which has
// since been deleted. That is the observation a routing test wants -- did this
// binding reach this queue -- and [Broker.Queue]'s Depth and [Broker.Ready]
// are the ones a backlog test wants.
func (b *Broker) Publications(queue string) []Publication {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Publication(nil), b.received[queue]...)
}

// ArrivalQueues is every queue name anything was ever published to, ordered by
// name, deleted queues included. It is the key set of [Broker.Publications].
func (b *Broker) ArrivalQueues() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	names := make([]string, 0, len(b.received))
	for name := range b.received {
		names = append(names, name)
	}
	sortStrings(names)
	return names
}

// Ready is the messages one queue is holding, in the order it will hand them
// out. It is how a requeue-goes-to-the-head assertion is written.
func (b *Broker) Ready(queue string) []Publication {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queues[queue]
	if q == nil {
		return nil
	}
	out := make([]Publication, 0, len(q.ready))
	for _, m := range q.ready {
		out = append(out, publicationOf(m))
	}
	return out
}

// InFlight is one delivery a channel is holding unsettled.
type InFlight struct {
	// Channel is the channel's id within its connection.
	Channel int
	// Tag is the delivery tag it was handed out with.
	Tag uint64
	// Queue is where the message came from.
	Queue string
	// Consumer is the consumer tag it went to.
	Consumer string
	// Message is the message.
	Message Publication
}

// InFlight is every delivery every channel is holding unsettled, ordered by
// channel and then by delivery tag.
//
// It is the half of the accounting [Broker.Settlements] does not cover, and
// together with [Broker.Ready], [Broker.DeadLettered] and [Broker.Dropped] it
// is what makes "no message is lost" a checkable statement rather than a hope:
// every message this broker ever accepted is in exactly one of those five
// places.
func (b *Broker) InFlight() []InFlight {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []InFlight
	for _, c := range b.conns {
		c.stateMu.Lock()
		chans := append([]*fakeChan(nil), c.chans...)
		c.stateMu.Unlock()
		for _, ch := range chans {
			for _, tag := range ch.unackedTags {
				fl := ch.unacked[tag]
				if fl == nil {
					continue
				}
				row := InFlight{Channel: ch.id, Tag: tag, Queue: fl.q.name, Message: publicationOf(fl.msg)}
				if fl.con != nil {
					row.Consumer = fl.con.tag
				}
				out = append(out, row)
			}
		}
	}
	return out
}

// ConsumerState is one consumer's prefetch accounting.
type ConsumerState struct {
	// Tag is the consumer tag.
	Tag string
	// Queue is the queue it consumes.
	Queue string
	// Prefetch is the count it was given, or zero for unbounded.
	Prefetch int
	// Outstanding is how many deliveries it holds unsettled, which is the
	// number the prefetch bounds.
	Outstanding int
	// AutoAck reports a `no-ack` consumer, for which the prefetch bounds
	// nothing.
	AutoAck bool
	// Cancelled reports that the consumer has been cancelled.
	Cancelled bool
	// Exclusive reports an exclusive consumer.
	Exclusive bool
	// Args are the `basic.consume` arguments, copied.
	Args amqp.Table
}

// Settlements is every settlement, in order.
func (b *Broker) Settlements() []Settlement {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Settlement(nil), b.settlements...)
}

// DeadLettered is every dead-letter hop, in order: what was dead-lettered and
// to where.
func (b *Broker) DeadLettered() []DeadLetter {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]DeadLetter(nil), b.deadLetters...)
}

// Dropped is every message the broker discarded, in order, with the reason.
func (b *Broker) Dropped() []Drop {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Drop(nil), b.drops...)
}

// Delayed is the publications the delayed-message exchanges are holding, in
// the order they will be released.
func (b *Broker) Delayed() []Publication {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Publication
	names := append([]string(nil), b.exOrder...)
	sortStrings(names)
	for _, name := range names {
		ex := b.exchanges[name]
		if ex == nil {
			continue
		}
		for _, m := range ex.held {
			out = append(out, publicationOf(m))
		}
	}
	return out
}

// sortStrings sorts a name list in place. It is insertion sort rather than
// sort.Strings because these lists are a handful of names long and the import
// is not worth it; nothing here is on a path anything measures.
func sortStrings(names []string) {
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
}

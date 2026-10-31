package rabbitmq

import (
	"context"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/drivers/rabbitmq/internal/client"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// declKind is which AMQP declaration a recorded entry is.
type declKind uint8

const (
	declExchange declKind = iota
	declQueue
	declBinding
)

// decl is one declaration this client made.
//
// It is recorded rather than recomputed because the specification requires the driver to
// restore the topology after a reconnection, and "recompute it from the
// configuration" is not the same thing: a client that declared a delay queue
// for a 51.2-second bucket has to declare that bucket again, and the
// configuration does not say which buckets were used. Replaying what was
// really declared, in the order it was declared, is what makes the restored
// topology the same topology.
type decl struct {
	kind declKind
	// name is the exchange or queue name, or the queue name of a binding.
	name string
	// kindName is an exchange's type.
	kindName string
	// key and source are a binding's routing key and source exchange.
	key, source string
	// args are the declaration's arguments.
	args amqp.Table
}

// id is the key a declaration is remembered under, so that declaring one
// twice costs one round trip rather than two.
func (d decl) id() string {
	switch d.kind {
	case declExchange:
		return "x:" + d.name
	case declQueue:
		return "q:" + d.name
	default:
		return "b:" + d.source + "\x00" + d.key + "\x00" + d.name
	}
}

// topology is the ledger of everything this client declared.
//
// It is a ledger and not a cache: its purpose is the replay after a
// reconnection, and the de-duplication is a side effect. Both halves are
// needed — a driver that re-declared on every publication would spend a round
// trip per message, and one that did not record would come back from a
// reconnection with the exchanges present (the broker keeps durable ones) and
// the delay queues gone (they may have expired under `x-expires`).
type topology struct {
	mu    sync.Mutex
	order []decl
	seen  map[string]struct{}
}

// newTopology returns an empty ledger.
func newTopology() *topology { return &topology{seen: map[string]struct{}{}} }

// record adds a declaration to the ledger, unless it is already there, and
// marks it made. One the broker refused (see [topology.refused]) keeps its
// place in the order.
func (t *topology) record(d decl) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := d.id()
	if _, dup := t.seen[id]; dup {
		return
	}
	t.seen[id] = struct{}{}
	for i := range t.order {
		if t.order[i].id() == id {
			return
		}
	}
	t.order = append(t.order, d)
}

// refused marks a declaration the broker refused in a replay as not made, so
// that [topology.known] stops vouching for it and the next ensure declares it
// again. It stays in the order, so the next replay declares it again too.
func (t *topology) refused(d decl) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.seen, d.id())
}

// known reports whether the declaration has already been made.
func (t *topology) known(d decl) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.seen[d.id()]
	return ok
}

// all is the ledger in declaration order, which is the order a replay has to
// use: a binding cannot be declared before the queue and the exchange it
// names.
func (t *topology) all() []decl {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]decl, len(t.order))
	copy(out, t.order)
	return out
}

// drop removes one declaration from the ledger, for one the broker no longer
// has.
func (t *topology) drop(d decl) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := d.id()
	if _, ok := t.seen[id]; !ok {
		return
	}
	delete(t.seen, id)
	for i := range t.order {
		if t.order[i].id() == id {
			t.order = append(t.order[:i], t.order[i+1:]...)
			break
		}
	}
}

// dropDeleted removes an exchange or queue the broker has deleted, together with
// the bindings the broker deleted with it: those whose source is the exchange,
// or whose destination is the queue. Leaving a binding behind would make the
// next ensure trust the ledger and skip it, and the re-created queue would
// then receive nothing.
func (t *topology) dropDeleted(d decl) {
	t.mu.Lock()
	defer t.mu.Unlock()
	kept := t.order[:0]
	for _, e := range t.order {
		if e.id() == d.id() || boundTo(e, d) {
			delete(t.seen, e.id())
			continue
		}
		kept = append(kept, e)
	}
	t.order = kept
}

// boundTo reports whether e is a binding that names the exchange or queue d.
func boundTo(e, d decl) bool {
	if e.kind != declBinding {
		return false
	}
	switch d.kind {
	case declExchange:
		return e.source == d.name
	case declQueue:
		return e.name == d.name
	default:
		return false
	}
}

// forget drops everything, for a client that is closing.
func (t *topology) forget() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.order, t.seen = nil, map[string]struct{}{}
}

// apply performs one declaration on a channel.
//
// A failure here is a CHANNEL exception: RabbitMQ answers a mismatched
// re-declaration with 406 PRECONDITION_FAILED and a reserved name with 403
// ACCESS_REFUSED, and either closes the channel. The caller therefore has to
// treat an error from this function as "this channel is gone", which is why
// the delay-plugin probe uses a channel of its own.
func apply(ch client.Channel, d decl) error {
	switch d.kind {
	case declExchange:
		return ch.ExchangeDeclare(d.name, d.kindName, true, false, false, false, d.args)
	case declQueue:
		_, err := ch.QueueDeclare(d.name, true, false, false, false, d.args)
		return err
	case declBinding:
		return ch.QueueBind(d.name, d.key, d.source, false, d.args)
	default:
		return errNoTopology
	}
}

// replay re-declares everything in the ledger, in order, on cn.
//
// It is what the specification's "topology restoration" is. The durable exchanges and
// quorum queues survive a broker restart and a re-declaration of one that
// exists with the same arguments is a no-op, so the cost of replaying them is
// one round trip each; what really needs the replay is the bindings after a
// broker's own data loss and the delay queues that may have expired.
//
// A declaration the broker REFUSES fails nothing but itself. The refusal is a
// channel exception, so the connection is sound and every later attempt would
// meet it again: a 406 for a delay queue another client declared again with a
// different Config.DelayQueueTTL after this one's expired, an exchange
// declared again with another type, or the 404 RabbitMQ answers for a durable
// classic queue whose node is down during a rolling restart. Returning it kept
// the whole client from reconnecting. So the replay goes on with the rest on a
// new channel, because the refusal closed the one it was made on.
//
// The refused declaration stays in the ledger and is marked not made
// ([topology.refused]). A refusal may not last, a permission missing for a
// moment for example, and nothing else declares a consumer's exchange, queue
// or binding again: dropping it lost them for the life of the client. So the
// next replay declares it again, and what needs it in the meantime declares it
// again itself and meets the refusal: a publisher's attach declares its
// exchange, and a delayed nack its delay queue. A consumer's `basic.consume`
// finds what the broker has (see [source.consumeRefused]). A declaration a
// later replay makes is marked made again. Only a failure that is not a
// channel exception, which is the connection's, fails the dial attempt.
func (b *Backend) replay(ctx context.Context, cn client.Conn) error {
	var ch client.Channel
	for _, d := range b.topo.all() {
		if err := ctx.Err(); err != nil {
			if ch != nil {
				_ = ch.Close()
			}
			return err
		}
		if ch == nil {
			c, err := cn.Channel()
			if err != nil {
				return err
			}
			ch = c
		}
		if err := apply(ch, d); err != nil {
			if !IsChannelException(err) {
				_ = ch.Close()
				return b.errs.Map(mq.OpOpen, d.name, "", err)
			}
			b.topo.refused(d)
			ch = nil
			continue
		}
		b.topo.record(d)
	}
	if ch == nil {
		return nil
	}
	return ch.Close()
}

// ensure declares what a topic and a subscription need, or checks that it is
// there.
//
// With [Config.Declare] off it declares NOTHING and instead asserts, through a
// passive declare, that the queue a subscription names exists. That is the specification's
// rule made checkable rather than merely obeyed: the alternative is a consumer
// that opens successfully and then blocks for ever on a queue nobody created,
// which is the symptom exists to prevent and is indistinguishable from an
// idle topic.
//
// A passive declare that fails closes the channel, so the caller hands this a
// channel it is prepared to lose.
func (b *Backend) ensure(ctx context.Context, ch client.Channel, n rabbit.Names, wantQueue bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !b.cfg.Declare {
		return b.assert(ch, n, wantQueue)
	}
	decls := make([]decl, 0, 6)
	decls = append(decls, decl{kind: declExchange, name: n.Exchange, kindName: b.cfg.ExchangeType})
	if wantQueue && n.Queue != "" {
		if b.cfg.DeadLetter && n.DeadLetterQueue != "" {
			// The dead-letter exchange and queue come FIRST, because the
			// subscription's own queue names the exchange in its arguments
			// and RabbitMQ does not require it to exist -- it would accept
			// the queue and then drop every dead-lettered message silently,
			// which is exactly the failure the dead-letter action is
			// supposed to prevent.
			decls = append(decls,
				decl{kind: declExchange, name: n.DeadLetterExchange, kindName: "direct"},
				decl{kind: declQueue, name: n.DeadLetterQueue, args: b.deadLetterQueueArgs()},
				decl{kind: declBinding, name: n.DeadLetterQueue, key: n.DeadLetterRoutingKey, source: n.DeadLetterExchange},
			)
		} else if b.cfg.DeadLetter {
			// [Config.DeadLetterTopic] names another TOPIC as the
			// destination, so the exchange is that topic's and is declared
			// with the topic exchange's own type; the queues behind it are
			// that topic's subscriptions' and are none of this
			// subscription's business. The exchange is declared all the same
			// -- RabbitMQ drops a dead-lettered message whose dead-letter
			// exchange does not exist, silently, and a destination that
			// swallows failures is worse than no destination.
			decls = append(decls,
				decl{kind: declExchange, name: n.DeadLetterExchange, kindName: b.cfg.ExchangeType},
			)
		}
		decls = append(decls,
			decl{kind: declQueue, name: n.Queue, args: b.queueArgs(n)},
			decl{kind: declBinding, name: n.Queue, key: n.RoutingKey, source: n.Exchange},
		)
		if b.fx.delayPlugin {
			// the plugin arm publishes a delayed redelivery to
			// `<exchange>.delayed` with the SUBSCRIPTION'S QUEUE as the
			// routing key, so that exchange has to exist and has to be
			// bound to this queue. Without the binding the delayed
			// publication is unroutable: on a `mandatory` publication the
			// broker returns it, and on the source's own channel a
			// publication to an exchange that does not exist is a 404 that
			// closes the channel -- which is how the mechanism failed
			// before this was here.
			//
			// It is declared per SUBSCRIPTION rather than at open because
			// the exchange's name is derived from the topic and there is no
			// topic at open; the probe declares one of its own under a
			// reserved probe topic, which is what detects the plugin.
			decls = append(decls,
				decl{
					kind: declExchange, name: n.DelayExchange, kindName: ExchangeDelayed,
					args: amqp.Table{"x-delayed-type": b.delayedType()},
				},
				decl{kind: declBinding, name: n.Queue, key: n.Queue, source: n.DelayExchange},
			)
		}
	}
	for _, d := range decls {
		if b.topo.known(d) {
			continue
		}
		if err := apply(ch, d); err != nil {
			return b.errs.Map(mq.OpOpen, d.name, "", err)
		}
		b.topo.record(d)
	}
	return nil
}

// delayedType is the `x-delayed-type` a topic's `<exchange>.delayed` is
// declared with, which is how the plugin routes a park once its delay is over.
//
// A park carries the subscription's queue name as its routing key and each
// queue is bound by that name, so the type has to route by the key. A direct or
// topic exchange does, and keeps the topic's own type, which is what every
// earlier client declared. A fanout exchange ignores the key and a headers
// exchange matches on headers, so under either the delayed exchange is direct.
// With the topic's type, one subscription's delayed nack reached every
// subscription of a fanout topic, and on a headers topic the queue's name
// played no part in where it went.
//
// An exchange that already exists keeps the type it was declared with, because
// the broker does not change an exchange's arguments, so the driver page tells
// a fanout or headers deployment to delete the old one. The probe's exchange
// keeps the topic's type for that reason: nothing is routed through it, and
// declaring it with another type over the old one gains nothing and may be
// refused at open, on every client of such a deployment (the fake refuses it
// with 406; a broker that compares fewer arguments keeps the old type).
func (b *Backend) delayedType() string {
	switch b.cfg.ExchangeType {
	case amqp.ExchangeDirect, amqp.ExchangeTopic:
		return b.cfg.ExchangeType
	default:
		return amqp.ExchangeDirect
	}
}

// assert is the no-declaration path: it checks that the topology exists
// without creating any of it.
//
// An exchange cannot be checked without declaring it passively either, and a
// passive exchange declare is `exchange.declare-passive`, which the library
// exposes; both are used, and both close the channel when they fail.
func (b *Backend) assert(ch client.Channel, n rabbit.Names, wantQueue bool) error {
	if !wantQueue || n.Queue == "" {
		// A publisher does not need the queue, and a passive exchange
		// declare is the cheapest way to answer "will my publications go
		// anywhere at all".
		if err := ch.ExchangeDeclarePassive(n.Exchange, b.cfg.ExchangeType, true, false, false, false, nil); err != nil {
			return b.errs.Map(mq.OpOpen, n.Exchange, "", err)
		}
		return nil
	}
	if _, err := ch.QueueDeclarePassive(n.Queue, true, false, false, false, nil); err != nil {
		return b.errs.Map(mq.OpOpen, n.Exchange, n.Queue, err)
	}
	return nil
}

// queueArgs are the arguments a subscription's queue is declared with.
//
// Every entry is a decision and is commented in [Config]. The ORDER does not
// matter to the broker, but the SET does: RabbitMQ compares the whole argument
// table on a re-declaration and answers a difference with 406
// PRECONDITION_FAILED, which closes the channel. So a change to this function
// is a change that makes an existing deployment's queues undeclarable, and
// `TestQueueArgumentsArePinned` is there to make that a deliberate act.
func (b *Backend) queueArgs(n rabbit.Names) amqp.Table {
	args := amqp.Table{"x-queue-type": b.cfg.QueueType.String()}
	if b.cfg.DeadLetter {
		args["x-dead-letter-exchange"] = n.DeadLetterExchange
		args["x-dead-letter-routing-key"] = n.DeadLetterRoutingKey
	}
	if b.cfg.QueueMaxLength > 0 {
		args["x-max-length"] = b.cfg.QueueMaxLength
	}
	if b.cfg.MaxPriority > 0 {
		// `x-max-priority` and a quorum queue are mutually exclusive: the
		// broker refuses the combination. Validate does not refuse it,
		// because the caller may have meant either, and the driver picks
		// the one they asked for EXPLICITLY -- a priority ceiling is only
		// ever set deliberately, and the queue type has a default.
		args["x-max-priority"] = int32(b.cfg.MaxPriority)
		args["x-queue-type"] = QueueClassic.String()
	}
	return args
}

// deadLetterQueueArgs are the dead-letter queue's own arguments.
//
// It gets no dead-letter route of its own, which is the one thing that must
// not be copied from the subscription's queue: a dead-letter queue that
// dead-lettered would send an unprocessable message round a cycle, and
// RabbitMQ's own cycle detection drops it after one hop. A message in the
// dead-letter queue is meant to sit there until somebody looks at it.
func (b *Backend) deadLetterQueueArgs() amqp.Table {
	args := amqp.Table{"x-queue-type": b.cfg.QueueType.String()}
	if b.cfg.MaxPriority > 0 {
		args["x-queue-type"] = QueueClassic.String()
	}
	return args
}

// ensureDelay declares what a delay plan needs, on the channel the settlement
// is about to use.
//
// A permanent delay queue is declared at most once per name: the ladder has at
// most a few dozen buckets, so the ledger's de-duplication turns an unbounded
// number of delayed redeliveries into one declaration per bucket.
//
// A queue with `x-expires` ([Config.DelayQueueTTL]) is declared again before
// every park, and the round trip is what keeps the parked copies. RabbitMQ
// renews the lease on a consumer, a declaration or a `basic.get`, never on a
// publication, and it deletes an expired queue with the messages in it. Once
// per connection meant the queue went DelayQueueTTL after that one
// declaration, however busy it was, and took with it copies whose originals
// were already acknowledged. [delayFits] is the other half: it refuses a bucket
// the renewed lease would not outlast.
//
// The client library's `queue.declare` takes no context, so under a
// `connection.blocked` alarm, when the broker stops reading the connection, a
// park waits for the declaration's reply until the alarm clears, past the
// settlement's deadline.
func (b *Backend) ensureDelay(ch client.Channel, p *delay.Plan) error {
	if p.Declare == nil {
		return nil
	}
	if !b.cfg.Declare {
		return errDelayNeedsDeclare
	}
	d := decl{kind: declQueue, name: p.Declare.Name, args: delayQueueArgs(p.Declare)}
	if b.topo.known(d) && permanent(p.Declare) {
		return nil
	}
	if err := apply(ch, d); err != nil {
		return b.errs.Map(mq.OpNack, p.Declare.Name, "", err)
	}
	b.topo.record(d)
	return nil
}

// forgetDelay drops a plan's queue from the ledger, for a park the broker
// returned unroutable: the queue is not there, and [Backend.ensureDelay] would
// otherwise trust the ledger and never declare it again.
func (b *Backend) forgetDelay(p *delay.Plan) {
	if p.Declare == nil {
		return
	}
	b.topo.drop(decl{kind: declQueue, name: p.Declare.Name})
}

// delayFits reports whether a delay queue's `x-expires` leaves room for the
// copy parked in it.
//
// The park declares the queue first ([Backend.ensureDelay]), so the lease
// starts before the copy arrives, and the copy leaves one bucket later.
// The gap between the declaration and the copy's arrival is a round trip, and
// longer on a loaded broker, so the bucket has to leave the queue a margin: a
// minute of its lease, or half of it when the lease is under 2 minutes. A
// queue with no `x-expires` is permanent and fits every bucket.
//
// It is checked rather than worked around. Raising `x-expires` to fit the
// bucket would change the queue's arguments, and a client that declares the
// queue with the old ones, during a rolling upgrade or a rollback, would then
// meet 406 PRECONDITION_FAILED on every park and on its topology replay.
func delayFits(q *delay.QueueDecl) bool {
	if permanent(q) {
		return true
	}
	margin := min(q.ExpiresMillis/2, time.Minute.Milliseconds())
	return q.TTLMillis <= q.ExpiresMillis-margin
}

// permanent reports whether a delay queue has no `x-expires`, so that the one
// declaration the ledger records is enough to keep it.
func permanent(q *delay.QueueDecl) bool { return q.ExpiresMillis <= 0 }

// delayQueueArgs turns a plan's queue declaration into AMQP arguments.
//
// The queue is CLASSIC and not quorum, which is deliberate: nothing is ever
// consumed from a delay queue, so replication buys nothing, and a quorum queue
// refuses `x-expires` in some versions. The message's durability is the
// message's own (`delivery_mode`), and the dead-letter hop preserves it.
func delayQueueArgs(q *delay.QueueDecl) amqp.Table {
	args := amqp.Table{
		"x-queue-type":              QueueClassic.String(),
		"x-message-ttl":             q.TTLMillis,
		"x-dead-letter-exchange":    q.DeadLetterExchange,
		"x-dead-letter-routing-key": q.DeadLetterRoutingKey,
	}
	if q.ExpiresMillis > 0 {
		args["x-expires"] = q.ExpiresMillis
	}
	return args
}

// detectDelayPlugin reports whether the broker has the delayed-message
// exchange, which is the better arm.
//
// It is DETECTED and not inferred from a version, because the plugin is not
// part of any RabbitMQ release: the official image does not carry it,
// `hack/images/rabbitmq-delayed` builds one that does, and the plugin only
// supports the RabbitMQ minor it was built for. The probe is a declaration of
// an `x-delayed-message` exchange, which a broker without the plugin answers
// with 503 COMMAND_INVALID.
//
// The probe runs on a channel of its OWN and closes it afterwards, because a
// failed declaration closes the channel it was made on. A probe that used the
// control channel would take the client's own topology channel down on every
// broker that does not have the plugin, which is most of them.
func (b *Backend) detectDelayPlugin(ctx context.Context, n rabbit.Names) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	ch, err := b.channelOnWithin(ctx, b.conn())
	if err != nil {
		return false, err
	}
	perr := ch.ExchangeDeclare(n.DelayExchange, ExchangeDelayed, true, false, false, false,
		amqp.Table{"x-delayed-type": b.cfg.ExchangeType})
	if perr == nil {
		// The exchange now exists and the ledger has to know, or a
		// reconnection would not restore it.
		b.topo.record(decl{
			kind: declExchange, name: n.DelayExchange, kindName: ExchangeDelayed,
			args: amqp.Table{"x-delayed-type": b.cfg.ExchangeType},
		})
		_ = ch.Close()
		return true, nil
	}
	_ = ch.Close()
	if isUnknownExchangeType(perr) {
		return false, nil
	}
	// Any other failure is not "no plugin": a 403 on the name, a 406 against
	// an exchange of ANOTHER type, a connection that went away. Reporting one
	// of those as "no plugin" would silently downgrade a client whose real
	// problem is something else.
	return false, b.errs.Map(mq.OpOpen, n.DelayExchange, "", perr)
}

// isUnknownExchangeType reports whether the broker refused the declaration
// because it does not know the exchange type.
//
// The CODE is not enough, and finding that out cost a whole conformance run.
// The AMQP 0-9-1 specification makes an unknown exchange type 503
// COMMAND_INVALID, which is what this function was first written against;
// RabbitMQ 4.3.5 answers **406 PRECONDITION_FAILED - unknown exchange type
// 'x-delayed-message'**, and 406 is also what it answers a re-declaration with
// different arguments with. So the code alone cannot tell "there is no plugin"
// from "an exchange of that name exists and is something else", and the only
// thing that can is the reason text.
//
// Matching on a message is the last resort and `driverkit.MatchContains`'s own
// doc comment says so. It is the right call here for two reasons: the
// distinction is not available any other way, and getting it wrong in the
// other direction is worse — a driver that read every 406 as "no plugin" would
// silently fall back on a broker whose delayed exchange had been declared with
// the wrong routing type, which is a topology error an operator has to fix.
// Both spellings are accepted, so a broker that follows the specification and
// one that follows RabbitMQ both work.
func isUnknownExchangeType(err error) bool {
	code, ok := ReplyCode(err)
	if !ok {
		return false
	}
	switch code {
	case amqp.CommandInvalid:
		// The specification's answer. Accepted whatever the reason says: 503
		// on an `exchange.declare` has no other meaning.
		return true
	case amqp.PreconditionFailed:
		return strings.Contains(err.Error(), "unknown exchange type")
	default:
		return false
	}
}

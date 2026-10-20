package rabbitfake

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/iqhive/iqmq/internal/rabbit"
)

// ExchangeDelayed is the plugin exchange type `rabbitmq_delayed_message_exchange`
// installs.
//
// It is a string and not an [amqp091] constant because the library has no
// constant for it: the type does not exist in AMQP 0-9-1 and does not exist on
// a stock broker either. Declaring it against a broker without the plugin is a
// 503 COMMAND_INVALID, which is the refusal the detection reads, and
// [Config.DelayedMessagePlugin] is what decides which of the two answers this
// fake gives.
const ExchangeDelayed = "x-delayed-message"

// DelayedTypeArg names the routing type a delayed-message exchange routes
// with once a message's delay has elapsed.
const DelayedTypeArg = "x-delayed-type"

// The queue arguments this fake interprets. The rest are carried, compared on
// a re-declare and otherwise ignored, which is what a broker does with an
// argument it does not know.
const (
	// DeadLetterExchangeArg is `x-dead-letter-exchange`.
	DeadLetterExchangeArg = "x-dead-letter-exchange"
	// DeadLetterRoutingKeyArg is `x-dead-letter-routing-key`.
	DeadLetterRoutingKeyArg = "x-dead-letter-routing-key"
	// DeliveryCountHeader is the header a quorum queue exposes its delivery
	// count in, which the specification maps to Attempt. It is what RabbitMQ 3.13
	// and 4.1 send.
	DeliveryCountHeader = "x-delivery-count"
	// AcquiredCountHeader is the same count under the name RabbitMQ 4.3
	// sends it under.
	//
	// The rename is a fact measured against three real brokers rather than a
	// hypothetical: 3.13.7 and 4.1.8 send `x-delivery-count` and 4.3.5 sends
	// `x-acquired-count`. [Config.DeliveryCountName] is how a test says which
	// broker it is imitating, so the driver's reading of both can be held
	// without three containers.
	AcquiredCountHeader = "x-acquired-count"
)

// maxDeaths bounds how many dead-letter hops ONE PUBLICATION may cause, copies
// included -- not how many hops one copy may take in a row.
//
// RabbitMQ detects a dead-letter CYCLE -- a message routed back to a queue it
// has already been dead-lettered out of, with no TTL anywhere in the loop --
// and drops the message rather than moving it for ever. This fake bounds the
// hop count instead, which is coarser and is here for a reason the real broker
// does not have: [FuzzRabbitFake] can build a two-queue loop whose TTLs are
// both zero, and a fake that moved that message on every clock advance would
// not terminate. Thirty-two is far above any topology a driver builds and far
// below a hang.
//
// The budget is shared by every copy because a bound on one copy's hops is not
// a bound on the work. A dead-letter exchange bound to two queues turns one hop
// into two, each of which hops again, so a per-copy bound of thirty-two permits
// 2^32 hops in total -- and a two-queue topology that evicts on `x-max-length`
// runs exactly that, which is what crashers #54, #60 and #64 are. One counter
// per publication makes the total hops linear in the script rather than
// exponential in this constant.
const maxDeaths = 32

// message is one message inside the broker.
type message struct {
	// id is broker-wide and monotone. It is the identity every observation
	// in this package is keyed by, because a body is not an identity: two
	// publications of the same bytes are two messages.
	id uint64
	// exchange and routingKey are the ones the message was PUBLISHED with,
	// which a dead-letter hop deliberately does not change -- the hop
	// re-publishes with the dead-letter exchange's name, and the delivery
	// still carries the original key unless the queue named a replacement.
	exchange   string
	routingKey string
	pub        amqp.Publishing
	// redelivered is `basic.deliver`'s redelivered flag, set once the message
	// has been handed out and given back.
	redelivered bool
	// attempts counts the deliveries that ended without an ack, which is what
	// a quorum queue's `x-delivery-count` reports.
	attempts uint64
	// expiry is when the message dies where it now sits, or the zero time
	// when it does not. It is computed on entry to each queue from the
	// queue's `x-message-ttl` and the publication's own `Expiration`,
	// whichever is shorter, because that is RabbitMQ's rule.
	expiry time.Time
	// priority is `x-max-priority` ordering, clamped to the queue's maximum.
	priority uint8
	// deaths counts the dead-letter hops this message AND EVERY COPY A
	// DEAD-LETTER HOP MADE OF IT have taken between them, which is why it is a
	// pointer: [message.clone] shares the counter rather than copying it, so
	// one publication's whole dead-letter tree spends one budget. It is nil
	// until the first hop. See [maxDeaths].
	deaths *deathCount
}

// deathCount is the dead-letter hop counter a message and its copies share.
type deathCount struct {
	hops int
}

// exchange is one declared exchange.
type exchange struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	args       amqp.Table
	// delayedType is the `x-delayed-type` of a delayed-message exchange: the
	// routing type it uses once a delay has elapsed.
	delayedType string
	// bindings are in declaration order, which is what makes the set of
	// queues one publication reaches the same on every run.
	bindings []binding
	// held are the messages a delayed-message exchange is holding, ordered by
	// due time and then by message id so that two due at once are released in
	// publication order.
	held []*message
	// predeclared marks the broker's own exchanges, which a client may bind
	// to and publish through but may not declare.
	predeclared bool
}

// binding is one queue bound to one exchange with one key.
type binding struct {
	queue string
	key   string
	args  amqp.Table
}

// queue is one declared queue.
type queue struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	args       amqp.Table
	// owner is the connection an exclusive queue belongs to, which is what a
	// second connection's declare is refused with 405 against.
	owner int
	// quorum reports `x-queue-type: quorum`, which is the whole of the specification's
	// `x-delivery-count` asymmetry.
	quorum bool
	// ttl, dlx, dlk, expires, maxPriority, maxLength and overflow are the
	// interpreted arguments.
	ttl         time.Duration
	hasTTL      bool
	dlx         string
	dlk         string
	hasDLK      bool
	expires     time.Duration
	maxPriority uint8
	maxLength   int
	overflow    string

	// ready is the FIFO the queue hands messages out of, highest priority
	// first when the queue has a maximum priority.
	ready []*message
	// unacked counts the deliveries out of this queue that have not been
	// settled, across every channel. The per-delivery state is the channel's,
	// because a delivery tag is a channel's.
	unacked int
	// consumers are in registration order, which is what makes "whose turn is
	// it" the same on every run.
	consumers []*consumer
	// nextConsumer is the round-robin cursor. RabbitMQ hands the next message
	// to the next consumer with capacity, so a fake that always started at
	// the first consumer would make a two-consumer test look like a
	// one-consumer test.
	nextConsumer int
	// lastUse is when the queue was last declared, published to, consumed
	// from or settled on, which is what `x-expires` is measured from.
	lastUse time.Time
	deleted bool
}

// consumer is one `basic.consume` on one channel.
type consumer struct {
	tag     string
	ch      *fakeChan
	q       *queue
	autoAck bool
	out     chan amqp.Delivery
	// prefetch is the count this consumer was given by the `basic.qos` in
	// force when it started, or the channel's when the qos is global.
	prefetch int
	// outstanding is how many deliveries this consumer holds unsettled, which
	// is what the prefetch bounds.
	outstanding int
	cancelled   bool
	// outClosed records that the delivery channel has been closed, so that a
	// cancel followed by a channel close does not close it twice.
	outClosed bool
}

// inflight is one delivery a channel has handed out and not yet been settled.
type inflight struct {
	// id is the delivery's own identity, so that a settlement can be
	// attributed to a delivery rather than to a tag a later channel reused.
	id  uint64
	tag uint64
	msg *message
	q   *queue
	con *consumer
}

// newError builds a channel or connection exception the way the library's own
// newError does, so that a driver matching on [amqp091.Error] fields sees what
// it would see from a broker.
func newError(code int, reason string) *amqp.Error {
	return &amqp.Error{Code: code, Reason: reason, Server: true, Recover: isSoftCode(code)}
}

// isSoftCode reports whether a reply code is a CHANNEL exception rather than a
// connection one, which is what the library's Recover field carries.
func isSoftCode(code int) bool {
	switch code {
	case amqp.ContentTooLarge, amqp.NoRoute, amqp.NoConsumers,
		amqp.AccessRefused, amqp.NotFound, amqp.ResourceLocked, amqp.PreconditionFailed:
		return true
	default:
		return false
	}
}

// errPrecondition is the 406 a declaration that disagrees with an existing one
// is refused with. It closes the channel, which on a confirm channel takes
// every publication in flight with it -- which is why the driver validates
// names before it writes a frame.
func errPrecondition(what, name string) *amqp.Error {
	return newError(amqp.PreconditionFailed,
		"PRECONDITION_FAILED - inequivalent arg for "+what+" '"+name+"'")
}

// nowLocked is the broker's time: the injected clock plus whatever
// [Broker.Advance] has added to it.
//
// The offset exists so that one entry point moves time whichever clock the
// caller injected. A test that supplies no clock gets a frozen one and drives
// it with Advance; a test inside testing/synctest supplies the bubble's clock
// and may drive either. `fakestreams` has the same pair for the same reason.
func (b *Broker) nowLocked() time.Time { return b.clock.Now().Add(b.offset) }

// exchangeLocked returns an exchange by name.
func (b *Broker) exchangeLocked(name string) *exchange { return b.exchanges[name] }

// declareExchange is `exchange.declare`.
func (b *Broker) declareExchange(
	name, kind string,
	durable, autoDelete, internal bool,
	args amqp.Table,
) *amqp.Error {
	if name == "" {
		// The default exchange exists and cannot be declared. RabbitMQ
		// answers a client that tries with ACCESS_REFUSED, which is the same
		// answer it gives for the reserved prefix and for the same reason:
		// the name is the broker's.
		return newError(amqp.AccessRefused, "ACCESS_REFUSED - operation not permitted on the default exchange")
	}
	if strings.HasPrefix(name, rabbit.ReservedPrefix) {
		return newError(amqp.AccessRefused,
			"ACCESS_REFUSED - operation not permitted on the reserved prefix '"+rabbit.ReservedPrefix+"'")
	}
	if err := b.checkExchangeKind(kind); err != nil {
		return err
	}
	delayedType := ""
	if kind == ExchangeDelayed {
		dt, _ := args[DelayedTypeArg].(string)
		if dt == "" {
			// The plugin requires the argument; without it the exchange has
			// no way to route once the delay elapses.
			return newError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - invalid exchange type '"+ExchangeDelayed+
					"' without the '"+DelayedTypeArg+"' argument")
		}
		if err := b.checkExchangeKind(dt); err != nil {
			return err
		}
		delayedType = dt
	}
	if ex := b.exchanges[name]; ex != nil {
		if ex.kind != kind || ex.durable != durable || ex.autoDelete != autoDelete ||
			ex.internal != internal || !argsEqual(ex.args, args) {
			return errPrecondition("exchange", name)
		}
		return nil
	}
	b.exchanges[name] = &exchange{
		name:        name,
		kind:        kind,
		durable:     durable,
		autoDelete:  autoDelete,
		internal:    internal,
		args:        cloneTable(args),
		delayedType: delayedType,
	}
	b.exOrder = append(b.exOrder, name)
	return nil
}

// checkExchangeKind refuses an exchange type this broker does not have.
func (b *Broker) checkExchangeKind(kind string) *amqp.Error {
	switch kind {
	case amqp.ExchangeDirect, amqp.ExchangeTopic, amqp.ExchangeFanout, amqp.ExchangeHeaders:
		return nil
	case ExchangeDelayed:
		if b.cfg.DelayedMessagePlugin {
			return nil
		}
		// This is the broker's own refusal when
		// `rabbitmq_delayed_message_exchange` is not enabled, and the CODE
		// is the finding: the AMQP 0-9-1 specification makes an unknown
		// exchange type 503 COMMAND_INVALID, and RabbitMQ 4.3.5 answers
		// **406 PRECONDITION_FAILED - unknown exchange type
		// 'x-delayed-message'**. The detection was written against
		// the specification and the real broker's conformance run refused
		// every scenario at open because of it, so the fake now sends what
		// the broker sends.
		return newError(amqp.PreconditionFailed,
			"PRECONDITION_FAILED - unknown exchange type '"+ExchangeDelayed+"'")
	default:
		return newError(amqp.PreconditionFailed, "PRECONDITION_FAILED - unknown exchange type '"+kind+"'")
	}
}

// declareExchangePassive is `exchange.declare` with the passive flag.
//
// It asserts existence and checks nothing else, which is the broker's own
// behaviour: RabbitMQ reads only the name from a passive declare and answers
// 404 NOT_FOUND when there is no such exchange. That refusal is what makes
// the specification's "no hidden creation" checkable for a PUBLISHER -- an exchange nobody
// declared is a topic this client cannot use, and a `ConfirmNone` publication
// would otherwise never learn it, because an unroutable publication to a
// missing exchange and a successful one look the same from above.
func (b *Broker) declareExchangePassive(name string) *amqp.Error {
	if b.exchanges[name] == nil {
		return newError(amqp.NotFound, "NOT_FOUND - no exchange '"+name+"' in vhost '/'")
	}
	return nil
}

// deleteExchange is `exchange.delete`.
func (b *Broker) deleteExchange(name string, ifUnused bool) *amqp.Error {
	ex := b.exchanges[name]
	if ex == nil {
		// RabbitMQ answers a delete of an exchange that is not there with
		// success, not with 404: the post-condition the client asked for
		// already holds. Only a PASSIVE declare asserts existence.
		return nil
	}
	if ex.predeclared {
		return newError(amqp.AccessRefused, "ACCESS_REFUSED - operation not permitted on exchange '"+name+"'")
	}
	if ifUnused && len(ex.bindings) > 0 {
		return newError(amqp.PreconditionFailed, "PRECONDITION_FAILED - exchange '"+name+"' in use")
	}
	for _, m := range ex.held {
		// A delayed-message exchange holds messages INSIDE itself, so
		// deleting it destroys them. The record is what keeps that from
		// looking like a loss.
		b.drops = append(b.drops, Drop{Message: publicationOf(m), Reason: DropExchangeDeleted})
	}
	ex.held = nil
	delete(b.exchanges, name)
	b.exOrder = removeName(b.exOrder, name)
	return nil
}

// declareQueue is `queue.declare`, active or passive.
func (b *Broker) declareQueue(
	connID int,
	name string,
	durable, autoDelete, exclusive, passive bool,
	args amqp.Table,
) (amqp.Queue, *amqp.Error) {
	if name == "" {
		if passive {
			return amqp.Queue{}, newError(amqp.NotFound, "NOT_FOUND - no queue '' in vhost '/'")
		}
		b.generated++
		name = "amq.gen-" + strconv.FormatUint(b.generated, 10)
	}
	q := b.queues[name]
	if passive {
		if q == nil {
			// the specification's "no hidden creation", made checkable: a passive declare
			// is the one declaration that asserts rather than creates, and
			// its refusal closes the channel.
			return amqp.Queue{}, newError(amqp.NotFound, "NOT_FOUND - no queue '"+name+"' in vhost '/'")
		}
		if err := b.checkExclusive(q, connID); err != nil {
			return amqp.Queue{}, err
		}
		q.lastUse = b.nowLocked()
		return amqp.Queue{Name: q.name, Messages: len(q.ready), Consumers: len(q.consumers)}, nil
	}
	if strings.HasPrefix(name, rabbit.ReservedPrefix) && q == nil {
		// The generated names are under the reserved prefix too, and this
		// broker generates them itself, so the refusal is only for a name a
		// CLIENT chose. A generated name is already in b.queues by the time a
		// client could name it.
		if !strings.HasPrefix(name, "amq.gen-") {
			return amqp.Queue{}, newError(amqp.AccessRefused,
				"ACCESS_REFUSED - queue name '"+name+"' contains reserved prefix '"+rabbit.ReservedPrefix+"'")
		}
	}
	if q != nil {
		if err := b.checkExclusive(q, connID); err != nil {
			return amqp.Queue{}, err
		}
		if q.durable != durable || q.autoDelete != autoDelete || q.exclusive != exclusive ||
			!argsEqual(q.args, args) {
			return amqp.Queue{}, errPrecondition("queue", name)
		}
		q.lastUse = b.nowLocked()
		return amqp.Queue{Name: q.name, Messages: len(q.ready), Consumers: len(q.consumers)}, nil
	}
	nq, err := b.newQueue(connID, name, durable, autoDelete, exclusive, args)
	if err != nil {
		return amqp.Queue{}, err
	}
	b.queues[name] = nq
	b.qOrder = append(b.qOrder, name)
	return amqp.Queue{Name: name}, nil
}

// newQueue interprets the declaration arguments into a queue.
func (b *Broker) newQueue(
	connID int,
	name string,
	durable, autoDelete, exclusive bool,
	args amqp.Table,
) (*queue, *amqp.Error) {
	q := &queue{
		name:       name,
		durable:    durable,
		autoDelete: autoDelete,
		exclusive:  exclusive,
		args:       cloneTable(args),
		owner:      -1,
		lastUse:    b.nowLocked(),
	}
	if exclusive {
		q.owner = connID
	}
	if kind, ok := args[amqp.QueueTypeArg].(string); ok {
		switch kind {
		case amqp.QueueTypeClassic, amqp.QueueTypeQuorum:
			q.quorum = kind == amqp.QueueTypeQuorum
		default:
			// A stream queue is a different thing with a different API and
			// this fake does not have it; refusing loudly is what keeps a
			// driver that started declaring one from passing here.
			return nil, newError(amqp.PreconditionFailed,
				"PRECONDITION_FAILED - invalid queue type '"+kind+"' (rabbitfake has classic and quorum)")
		}
	}
	if ms, ok := intArg(args, amqp.QueueMessageTTLArg); ok {
		if ms < 0 {
			return nil, errPrecondition("queue", name)
		}
		q.ttl, q.hasTTL = time.Duration(ms)*time.Millisecond, true
	}
	if dlx, ok := args[DeadLetterExchangeArg].(string); ok {
		q.dlx = dlx
	}
	if dlk, ok := args[DeadLetterRoutingKeyArg].(string); ok {
		q.dlk, q.hasDLK = dlk, true
	}
	if ms, ok := intArg(args, amqp.QueueTTLArg); ok {
		if ms < 0 {
			return nil, errPrecondition("queue", name)
		}
		q.expires = time.Duration(ms) * time.Millisecond
	}
	if p, ok := intArg(args, "x-max-priority"); ok {
		if p < 0 || p > 255 {
			return nil, errPrecondition("queue", name)
		}
		q.maxPriority = uint8(p)
	}
	if n, ok := intArg(args, amqp.QueueMaxLenArg); ok {
		if n < 0 {
			return nil, errPrecondition("queue", name)
		}
		q.maxLength = int(n)
	}
	if o, ok := args[amqp.QueueOverflowArg].(string); ok {
		switch o {
		case amqp.QueueOverflowDropHead, amqp.QueueOverflowRejectPublish,
			amqp.QueueOverflowRejectPublishDLX:
			q.overflow = o
		default:
			return nil, errPrecondition("queue", name)
		}
	}
	return q, nil
}

// checkExclusive refuses a second connection's use of an exclusive queue,
// which RabbitMQ answers with 405 RESOURCE_LOCKED.
func (b *Broker) checkExclusive(q *queue, connID int) *amqp.Error {
	if q.exclusive && q.owner != connID {
		return newError(amqp.ResourceLocked,
			"RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '"+q.name+"' in vhost '/'")
	}
	return nil
}

// bindQueue is `queue.bind`.
func (b *Broker) bindQueue(connID int, name, key, exName string, args amqp.Table) *amqp.Error {
	q := b.queues[name]
	if q == nil {
		return newError(amqp.NotFound, "NOT_FOUND - no queue '"+name+"' in vhost '/'")
	}
	if err := b.checkExclusive(q, connID); err != nil {
		return err
	}
	if exName == "" {
		// The default exchange binds every queue by its own name and cannot
		// be bound to. RabbitMQ refuses with ACCESS_REFUSED.
		return newError(amqp.AccessRefused, "ACCESS_REFUSED - operation not permitted on the default exchange")
	}
	ex := b.exchanges[exName]
	if ex == nil {
		return newError(amqp.NotFound, "NOT_FOUND - no exchange '"+exName+"' in vhost '/'")
	}
	for i := range ex.bindings {
		if ex.bindings[i].queue == name && ex.bindings[i].key == key &&
			argsEqual(ex.bindings[i].args, args) {
			// A binding is idempotent: the same triple twice is one binding,
			// and a publication reaches the queue once.
			return nil
		}
	}
	ex.bindings = append(ex.bindings, binding{queue: name, key: key, args: cloneTable(args)})
	q.lastUse = b.nowLocked()
	return nil
}

// deleteQueue is `queue.delete`. It reports how many messages went with the
// queue, which is what the seam's QueueDelete returns.
func (b *Broker) deleteQueue(connID int, name string, ifUnused, ifEmpty bool) (int, *amqp.Error) {
	q := b.queues[name]
	if q == nil {
		return 0, newError(amqp.NotFound, "NOT_FOUND - no queue '"+name+"' in vhost '/'")
	}
	if err := b.checkExclusive(q, connID); err != nil {
		return 0, err
	}
	if ifUnused && len(q.consumers) > 0 {
		return 0, newError(amqp.PreconditionFailed, "PRECONDITION_FAILED - queue '"+name+"' in use")
	}
	if ifEmpty && len(q.ready) > 0 {
		return 0, newError(amqp.PreconditionFailed, "PRECONDITION_FAILED - queue '"+name+"' not empty")
	}
	n := len(q.ready)
	b.dropQueueLocked(q, DropQueueDeleted)
	return n, nil
}

// dropQueueLocked removes a queue and everything pointing at it.
//
// The consumer cancellations are the part that matters to a driver: deleting a
// queue is the one thing that makes the broker send `basic.cancel`, which is
// why `NotifyCancel` is on the seam at all. A driver that treated a cancelled
// consumer as a transient read error would spin against a queue that is gone.
func (b *Broker) dropQueueLocked(q *queue, reason DropReason) {
	q.deleted = true
	for _, m := range q.ready {
		b.drops = append(b.drops, Drop{
			Message: publicationOf(m), Queue: q.name, Reason: reason,
		})
	}
	q.ready = nil
	for _, con := range q.consumers {
		con.cancelled = true
		con.ch.enqueueCancel(con.tag)
	}
	q.consumers = nil
	for _, name := range b.exOrder {
		ex := b.exchanges[name]
		kept := ex.bindings[:0]
		for _, bd := range ex.bindings {
			if bd.queue != q.name {
				kept = append(kept, bd)
			}
		}
		ex.bindings = kept
	}
	delete(b.queues, q.name)
	b.qOrder = removeName(b.qOrder, q.name)
}

// purgeQueue is `queue.purge`.
func (b *Broker) purgeQueue(connID int, name string) (int, *amqp.Error) {
	q := b.queues[name]
	if q == nil {
		return 0, newError(amqp.NotFound, "NOT_FOUND - no queue '"+name+"' in vhost '/'")
	}
	if err := b.checkExclusive(q, connID); err != nil {
		return 0, err
	}
	n := len(q.ready)
	for _, m := range q.ready {
		b.drops = append(b.drops, Drop{Message: publicationOf(m), Queue: q.name, Reason: DropPurged})
	}
	q.ready = nil
	q.lastUse = b.nowLocked()
	return n, nil
}

// routeLocked works out which queues a publication reaches, in a deterministic
// order and with no queue named twice.
//
// The duplicate rule is the broker's: a queue bound to one exchange with two
// keys that both match receives the message ONCE. A fake that delivered twice
// would make a wildcard subscription look like a duplicate-delivery bug in the
// driver.
func (b *Broker) routeLocked(exName, key string, m *message) ([]*queue, *amqp.Error) {
	if exName == "" {
		// The default exchange is a direct exchange that binds every queue by
		// its own name. It is not in b.exchanges' binding table at all,
		// because the binding is implicit.
		if q := b.queues[key]; q != nil {
			return []*queue{q}, nil
		}
		return nil, nil
	}
	ex := b.exchanges[exName]
	if ex == nil {
		// Publishing to an exchange that is not there is a channel exception,
		// and it is the one publish failure a driver meets in practice: a
		// topology that was never declared, or was deleted under it.
		return nil, newError(amqp.NotFound, "NOT_FOUND - no exchange '"+exName+"' in vhost '/'")
	}
	kind := ex.kind
	if kind == ExchangeDelayed {
		kind = ex.delayedType
	}
	var out []*queue
	seen := make(map[string]struct{}, len(ex.bindings))
	for _, bd := range ex.bindings {
		if !bindingMatches(kind, bd, key, m.pub.Headers) {
			continue
		}
		if _, dup := seen[bd.queue]; dup {
			continue
		}
		q := b.queues[bd.queue]
		if q == nil {
			continue
		}
		seen[bd.queue] = struct{}{}
		out = append(out, q)
	}
	return out, nil
}

// bindingMatches is the per-type routing rule.
func bindingMatches(kind string, bd binding, key string, headers amqp.Table) bool {
	switch kind {
	case amqp.ExchangeFanout:
		return true
	case amqp.ExchangeDirect:
		return bd.key == key
	case amqp.ExchangeTopic:
		// `rabbit.Matches` rather than a second matcher: the driver answers
		// the specification's "does this binding really reach that topic" with it, and two
		// implementations of one pattern language would eventually disagree
		// about a corner -- at which point the fake would be certifying the
		// driver against the fake's own bug.
		return rabbit.Matches(bd.key, key)
	case amqp.ExchangeHeaders:
		return headersMatch(bd.args, headers)
	default:
		return false
	}
}

// headersMatch is a headers exchange's rule: `x-match: all` (the default)
// needs every binding argument present and equal, `any` needs one.
func headersMatch(bind, headers amqp.Table) bool {
	all := true
	if m, ok := bind["x-match"].(string); ok {
		all = m != "any"
	}
	matched, considered := 0, 0
	for k, want := range bind {
		if strings.HasPrefix(k, "x-") {
			// The broker's own arguments are not part of the match, which is
			// what lets `x-match` sit in the same table as the predicate.
			continue
		}
		considered++
		if got, ok := headers[k]; ok && argEqual(want, got) {
			matched++
		}
	}
	if considered == 0 {
		// A headers binding with no predicate matches nothing. RabbitMQ
		// treats it as unsatisfiable rather than as a fanout, which is the
		// safer reading of an empty argument table.
		return false
	}
	if all {
		return matched == considered
	}
	return matched > 0
}

// enqueueLocked puts a message into a queue, or says why the queue would not
// take it.
//
// The return is a REFUSAL and not an error because of what a refusal means on
// a confirm channel: `x-overflow: reject-publish` makes the broker answer the
// publication with `basic.nack`, which is the only way a publisher learns that
// a bounded queue was full. A fake that silently dropped the message would
// make the driver's nack path unreachable.
func (b *Broker) enqueueLocked(q *queue, orig *message) (accepted bool) {
	m := orig.clone()
	m.priority = clampPriority(m.pub.Priority, q.maxPriority)
	m.expiry = b.expiryFor(q, m)
	if q.maxLength > 0 && len(q.ready) >= q.maxLength {
		switch q.overflow {
		case amqp.QueueOverflowRejectPublish, amqp.QueueOverflowRejectPublishDLX:
			if q.overflow == amqp.QueueOverflowRejectPublishDLX {
				b.deadLetterLocked(q, m, DeathMaxLen)
			} else {
				b.drops = append(b.drops, Drop{
					Message: publicationOf(m), Queue: q.name, Reason: DropOverflow,
				})
			}
			return false
		default:
			// drop-head, which is RabbitMQ's default and dead-letters the
			// head when the queue names a dead-letter exchange.
			head := q.ready[0]
			q.ready = q.ready[1:]
			b.deadLetterLocked(q, head, DeathMaxLen)
		}
	}
	b.received[q.name] = append(b.received[q.name], publicationOf(m))
	q.lastUse = b.nowLocked()
	b.insertReady(q, m, false)
	return true
}

// insertReady puts a message into the ready list.
//
// head puts it at the FRONT, which is what `basic.nack{requeue=true}` does and
// is the behaviour the immediate nack rests on: a requeued message is the
// next one out, not the last. On a priority queue "the front" is the front of
// its own priority band, because a requeue may not overtake a higher priority.
func (b *Broker) insertReady(q *queue, m *message, head bool) {
	if q.maxPriority == 0 {
		if head {
			q.ready = append([]*message{m}, q.ready...)
		} else {
			q.ready = append(q.ready, m)
		}
		return
	}
	at := len(q.ready)
	for i, other := range q.ready {
		if head {
			if other.priority <= m.priority {
				at = i
				break
			}
			continue
		}
		if other.priority < m.priority {
			at = i
			break
		}
	}
	q.ready = append(q.ready, nil)
	copy(q.ready[at+1:], q.ready[at:])
	q.ready[at] = m
}

// expiryFor is when a message entering a queue dies there.
//
// RabbitMQ takes the SHORTER of the queue's `x-message-ttl` and the
// publication's own `Expiration`, and a message with neither never expires.
// The zero time means never, which is why nothing in here compares against
// time.Time's zero value by accident: [Broker.expireLocked] skips it.
func (b *Broker) expiryFor(q *queue, m *message) time.Time {
	ttl, ok := time.Duration(0), false
	if q.hasTTL {
		ttl, ok = q.ttl, true
	}
	if ms, err := strconv.ParseInt(m.pub.Expiration, 10, 64); err == nil && ms >= 0 {
		d := time.Duration(ms) * time.Millisecond
		if !ok || d < ttl {
			ttl, ok = d, true
		}
	}
	if !ok {
		return time.Time{}
	}
	return b.nowLocked().Add(ttl)
}

// deadLetterLocked re-publishes a message through a queue's dead-letter
// exchange, or drops it when the queue names none.
//
// The routing key is the queue's `x-dead-letter-routing-key` when it named one
// and the message's ORIGINAL key otherwise, which is RabbitMQ's rule and the
// reason `internal/rabbit` gives the dead-letter exchange a constant key: a
// fanout-shaped DLX with one binding would route a message keyed "orders.eu"
// nowhere at all.
func (b *Broker) deadLetterLocked(q *queue, m *message, reason DeathReason) {
	if q.dlx == "" && !q.hasDLK {
		b.drops = append(b.drops, Drop{
			Message: publicationOf(m), Queue: q.name, Reason: dropFor(reason),
		})
		return
	}
	if m.deaths == nil {
		m.deaths = &deathCount{}
	}
	m.deaths.hops++
	if m.deaths.hops > maxDeaths {
		b.drops = append(b.drops, Drop{
			Message: publicationOf(m), Queue: q.name, Reason: DropDeathCycle,
		})
		return
	}
	key := m.routingKey
	if q.hasDLK {
		key = q.dlk
	}
	dead := m.clone()
	dead.pub.Headers = withDeathHeaders(dead.pub.Headers, q, m, reason)
	dead.redelivered = false
	b.deadLetters = append(b.deadLetters, DeadLetter{
		Message:    publicationOf(dead),
		From:       q.name,
		Exchange:   q.dlx,
		RoutingKey: key,
		Reason:     reason,
	})
	targets, err := b.routeLocked(q.dlx, key, dead)
	if err != nil || len(targets) == 0 {
		b.drops = append(b.drops, Drop{
			Message: publicationOf(dead), Queue: q.name, Reason: DropUnroutableDeadLetter,
		})
		return
	}
	dead.exchange, dead.routingKey = q.dlx, key
	for _, t := range targets {
		b.enqueueLocked(t, dead)
	}
}

// withDeathHeaders adds the `x-death` bookkeeping RabbitMQ adds to a
// dead-lettered message.
//
// It is a reduced form -- one entry, not the accumulated array a real broker
// builds -- and it is here because the three `x-first-death-*` headers are
// what an operator reads to find out why a message is in a dead-letter queue,
// and because a driver may surface the reason. The full array was the
// alternative and it buys nothing this fake is used for.
func withDeathHeaders(h amqp.Table, q *queue, m *message, reason DeathReason) amqp.Table {
	out := cloneTable(h)
	if out == nil {
		out = amqp.Table{}
	}
	if _, ok := out["x-first-death-reason"]; !ok {
		out["x-first-death-reason"] = string(reason)
		out["x-first-death-queue"] = q.name
		out["x-first-death-exchange"] = m.exchange
	}
	out["x-death"] = []any{amqp.Table{
		"count":        int64(1),
		"reason":       string(reason),
		"queue":        q.name,
		"exchange":     m.exchange,
		"routing-keys": []any{m.routingKey},
	}}
	return out
}

// expireLocked advances every deadline the broker holds to the current time.
//
// It is called at the start of every operation rather than from a timer, and
// that is the whole of this package's TTL mechanism. A timer per TTL queue was
// the alternative and it is the wrong shape twice over: it needs a goroutine
// and a real clock to be worth anything, and under an injected clock it fires
// when the TEST says rather than when the deadline passes -- so the sweep
// would have to exist anyway. Doing the sweep and nothing else means a TTL
// test is `Advance` followed by an assertion, with no scheduler in it.
func (b *Broker) expireLocked() {
	now := b.nowLocked()
	// Released delayed-message-plugin publications first: they can put a
	// message into a queue whose TTL has also elapsed, and a sweep that ran
	// the other way round would hold it for one more advance.
	for _, name := range b.exOrder {
		ex := b.exchanges[name]
		if len(ex.held) == 0 {
			continue
		}
		due, kept := []*message(nil), ex.held[:0]
		for _, m := range ex.held {
			if !m.expiry.After(now) {
				due = append(due, m)
			} else {
				kept = append(kept, m)
			}
		}
		ex.held = kept
		for _, m := range due {
			m.expiry = time.Time{}
			b.routeAndEnqueueLocked(ex.name, m.routingKey, m)
		}
	}
	for _, name := range b.qOrder {
		q := b.queues[name]
		if q == nil {
			continue
		}
		expired, kept := []*message(nil), q.ready[:0]
		for _, m := range q.ready {
			if !m.expiry.IsZero() && !m.expiry.After(now) {
				expired = append(expired, m)
			} else {
				kept = append(kept, m)
			}
		}
		q.ready = kept
		for _, m := range expired {
			b.deadLetterLocked(q, m, DeathExpired)
		}
	}
	// `x-expires` last, because expiring a message counts as activity no more
	// than a consumer does and the sweep above may have emptied a queue.
	for _, name := range append([]string(nil), b.qOrder...) {
		q := b.queues[name]
		if q == nil || q.expires == 0 || len(q.consumers) > 0 || q.unacked > 0 {
			continue
		}
		if now.Sub(q.lastUse) >= q.expires {
			b.dropQueueLocked(q, DropQueueExpired)
		}
	}
}

// routeAndEnqueueLocked routes one message and puts it in every queue that
// takes it, reporting how many queues it reached.
//
// It does not report whether every queue ACCEPTED, which
// [Broker.publishLocked] does: nothing on this path has a publisher to tell.
// A message released by a delayed-message exchange or seeded by
// [Broker.Publish] was confirmed long ago or was never confirmable, so a
// bounded queue that refuses it has only the drop record to say so.
func (b *Broker) routeAndEnqueueLocked(exName, key string, m *message) (routed int) {
	targets, err := b.routeLocked(exName, key, m)
	if err != nil || len(targets) == 0 {
		b.drops = append(b.drops, Drop{Message: publicationOf(m), Reason: DropUnroutable})
		return 0
	}
	for _, q := range targets {
		b.enqueueLocked(q, m)
	}
	return len(targets)
}

// clone copies a message so that two queues holding one publication hold two
// messages.
//
// They have to be two. A fan-out publication delivered to two queues is
// settled twice, expires on two schedules and carries two delivery counts, and
// a fake that shared one struct would let a settlement on one subscription
// move another's message -- which is the independence, and the defect a
// shared pointer produces is invisible until two subscriptions disagree.
func (m *message) clone() *message {
	out := *m
	out.pub.Headers = cloneTable(m.pub.Headers)
	if m.pub.Body != nil {
		out.pub.Body = append([]byte(nil), m.pub.Body...)
	}
	return &out
}

// clampPriority reads a publication's priority against the queue's maximum,
// which is what RabbitMQ does rather than refusing the publication.
func clampPriority(p, maxP uint8) uint8 {
	if maxP == 0 {
		return 0
	}
	if p > maxP {
		return maxP
	}
	return p
}

// cloneTable copies an argument or header table, because the caller owns the
// one it passed and a broker that kept the caller's map would let a later
// mutation rewrite a declaration that has already been compared.
func cloneTable(t amqp.Table) amqp.Table {
	if t == nil {
		return nil
	}
	out := make(amqp.Table, len(t))
	for k, v := range t {
		switch val := v.(type) {
		case amqp.Table:
			out[k] = cloneTable(val)
		case []byte:
			out[k] = append([]byte(nil), val...)
		default:
			out[k] = v
		}
	}
	return out
}

// argsEqual reports whether two declaration tables are the same declaration.
//
// A nil table and an empty one are the same declaration, because they are the
// same frame on the wire. Integers compare by VALUE across Go's widths: a
// driver that declared `x-message-ttl` as an int and re-declared it as an
// int32 sent the same frame both times, and a fake that refused the second
// with 406 would report a precondition failure the broker never raises.
func argsEqual(a, b amqp.Table) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !argEqual(av, bv) {
			return false
		}
	}
	return true
}

// argEqual compares two argument values.
func argEqual(a, b any) bool {
	if ai, ok := asInt(a); ok {
		bi, ok := asInt(b)
		return ok && ai == bi
	}
	switch av := a.(type) {
	case amqp.Table:
		bv, ok := b.(amqp.Table)
		return ok && argsEqual(av, bv)
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !argEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		// reflect.DeepEqual rather than ==: an AMQP table can carry a type
		// this switch does not name -- a []byte, a nested slice, whatever
		// [FuzzRabbitFake] put there -- and == on two uncomparable values is
		// a panic rather than a false. A fake that crashed comparing two
		// declarations would send whoever met it looking for a driver defect.
		return reflect.DeepEqual(a, b)
	}
}

// asInt reads an AMQP integer field of any width.
func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), true
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), true
	default:
		return 0, false
	}
}

// intArg reads one integer declaration argument.
func intArg(t amqp.Table, key string) (int64, bool) {
	v, ok := t[key]
	if !ok {
		return 0, false
	}
	return asInt(v)
}

// removeName deletes one name from an insertion-ordered list, keeping the
// order of the rest.
func removeName(names []string, name string) []string {
	out := names[:0]
	for _, n := range names {
		if n != name {
			out = append(out, n)
		}
	}
	return out
}

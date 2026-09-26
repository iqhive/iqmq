// Package delay holds the delayed-redelivery strategies.
//
// It is in the root module rather than in `drivers/rabbitmq` for the reason
// families, and the specification record: measures a family by
// running one `go test` in one package, and a root-module `go test` cannot
// reach a nested driver module. A delay plan is arithmetic
// over durations and names, so the whole family is measurable with no broker.
//
// # The problem
//
// AMQP 0-9-1 has no delayed redelivery. `basic.nack{requeue=true}` puts a
// message back at the head of its queue immediately, and the specification's
// `NackDelay(d)` asks for it back *after* d — so "make this message
// deliverable again in thirty seconds" has to be built out of what RabbitMQ
// has, and there are exactly two ways to build it:
//
//   - [TTLDLX] acknowledges the delivery and re-publishes the message into a
//     queue whose `x-message-ttl` is the delay and whose
//     `x-dead-letter-exchange` routes back to where the message came from.
//     The broker expires it and dead-letters it back. It works on every
//     RabbitMQ there has ever been and needs no plugin, and it pays for that
//     in two ways the criterion reads: one QUEUE per distinct delay, and a
//     delay rounded up to whichever queue exists.
//   - [Plugin] publishes to an `x-delayed-message` exchange with an `x-delay`
//     header, and the broker holds the message for exactly that long. One
//     exchange serves every delay and the delay is exact. It needs
//     `rabbitmq_delayed_message_exchange`, which is **not** part of the
//     official image — `hack/images/rabbitmq-delayed/Dockerfile` builds one
//     that has it — so a driver DETECTS it rather than assuming it.
//
// the expected outcome is "plugin when detected, TTL+DLX fallback", and
// that is why [AutoKind] is [TTLDLX] rather than the better arm: Auto is what
// a caller gets by not choosing, and a default that assumed a plugin would
// fail at open against a stock broker. The driver's own detection is what
// upgrades it, and `Strategy.Requires` is what the detection is read against.
//
// # Why rounding UP and never down
//
// The specification makes a delay a MINIMUM: "hold the publication back for this long". So
// a strategy that cannot express a delay exactly rounds up, never down — a
// message delivered early breaks the contract the caller wrote, and one
// delivered late does not. [TTLDLX] therefore grants the first ladder bucket
// at or above the requested delay, and [Plan.Granted] says what it granted so
// that the driver can report it and the benchmark can measure the error.
//
// # Why the delay queue dead-letters to the DEFAULT exchange
//
// The obvious route back is the topic's own exchange, and it is wrong. A
// delayed redelivery belongs to the ONE subscription that nacked the message
// dead-lettering into the topic exchange would fan it out to every
// subscription of that topic, so a nack on one subscription would deliver a
// duplicate to all the others. The default exchange ("") is a direct exchange
// that routes by queue name, so a delay queue whose `x-dead-letter-exchange`
// is "" and whose `x-dead-letter-routing-key` is the subscription's queue name
// delivers to exactly that queue and needs no exchange of its own.
//
// A SCHEDULED PUBLICATION (Delay on a publish) is the other direction
// and does want the fan-out, so [Request.Target] carries the exchange and
// routing key the message is to reappear at, and the same two strategies serve
// both. The queue count that measures is then per TARGET as well as per
// bucket, which is the growth the criterion is about.
package delay

import (
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// Kind identifies a strategy variant. Auto resolves to AutoKind.
type Kind uint8

// The kinds. TTLDLX is the reference implementation and the one
// that works on every broker.
const (
	// Auto selects AutoKind, the bake-off winner recorded in auto.go.
	Auto Kind = iota
	// TTLDLX parks the message in a queue whose TTL is the delay and whose
	// dead-letter route goes back to where it came from.
	TTLDLX
	// Plugin publishes to an x-delayed-message exchange with an x-delay
	// header. It needs the rabbitmq_delayed_message_exchange plugin.
	Plugin

	kindCount
)

var kindSlugs = [kindCount]string{"auto", "ttldlx", "plugin"}

var kindIdents = [kindCount]string{"Auto", "TTLDLX", "Plugin"}

// String returns the kind's slug as used in benchmark names, DSN parameters
// and ADRs.
func (k Kind) String() string {
	if k < kindCount {
		return kindSlugs[k]
	}
	return "kind(?)"
}

// Ident returns the kind's Go identifier.
func (k Kind) Ident() string {
	if k < kindCount {
		return kindIdents[k]
	}
	return ""
}

// Valid reports whether k names a concrete kind or Auto.
func (k Kind) Valid() bool { return k < kindCount }

// Resolve maps Auto to AutoKind and returns every other kind unchanged.
func (k Kind) Resolve() Kind {
	if k == Auto {
		return AutoKind
	}
	return k
}

// ParseKind returns the kind with the given slug.
func ParseKind(slug string) (Kind, bool) {
	for i, s := range kindSlugs {
		if s == slug {
			return Kind(i), true
		}
	}
	return 0, false
}

// Kinds lists every concrete kind in declaration order (Auto excluded).
func Kinds() []Kind {
	out := make([]Kind, 0, kindCount-1)
	for k := TTLDLX; k < kindCount; k++ {
		out = append(out, k)
	}
	return out
}

// Slugs lists every concrete kind's slug.
func Slugs() []string {
	out := make([]string, 0, kindCount-1)
	for _, k := range Kinds() {
		out = append(out, k.String())
	}
	return out
}

// Requirement is what a broker has to provide for a strategy to work.
type Requirement uint8

// The requirements.
const (
	// RequiresNothing is a strategy that works on any RabbitMQ: the TTL and
	// dead-letter arguments are core AMQP 0-9-1 features.
	RequiresNothing Requirement = iota
	// RequiresDelayedExchange is a strategy that needs the
	// rabbitmq_delayed_message_exchange plugin, which the official image
	// does not carry.
	RequiresDelayedExchange

	requirementCount
)

var requirementNames = [requirementCount]string{"nothing", "delayed-message-exchange-plugin"}

// String returns the requirement's name.
func (r Requirement) String() string {
	if r < requirementCount {
		return requirementNames[r]
	}
	return "Requirement(?)"
}

// Target is where a delayed message is to reappear.
//
// It is an exchange and a routing key rather than a queue name because the two
// uses of a delay want different things: a delayed REDELIVERY goes back to the
// one subscription that nacked it, which is the default exchange and the
// queue's name; a SCHEDULED PUBLICATION goes to the topic, which is the
// topic's exchange and the topic's routing key. See the package comment.
type Target struct {
	// Exchange is the exchange the message is to be routed through when its
	// delay is over. The empty string is AMQP's default exchange, which
	// routes by queue name.
	Exchange string
	// RoutingKey is the key it is to be routed with.
	RoutingKey string
	// Name is the target's name for the purpose of building a delay queue's
	// own name, which is the queue name for a redelivery and the exchange
	// name for a scheduled publication. It is separate from Exchange
	// because the default exchange has no name.
	Name string
}

// RedeliveryTarget is the [Target] for a delayed redelivery to one
// subscription's queue.
func RedeliveryTarget(queue string) Target {
	return Target{Exchange: "", RoutingKey: queue, Name: queue}
}

// PublishTarget is the [Target] for a scheduled publication to a topic.
func PublishTarget(exchange, routingKey string) Target {
	return Target{Exchange: exchange, RoutingKey: routingKey, Name: exchange}
}

// Request is one delayed message.
type Request struct {
	// Target is where the message is to reappear.
	Target Target
	// Delay is how long to hold it, as a minimum.
	Delay time.Duration
	// DelayExchange is the name of the x-delayed-message exchange the
	// [Plugin] strategy publishes to. It is [rabbit.Names.DelayExchange],
	// and it is a parameter rather than derived here because the exchange is
	// per TOPIC and the target may be a queue.
	DelayExchange string
}

// Plan is how to realise one delayed message: where to publish it, what to put
// on it, and what topology has to exist first.
//
// It is a value with no methods that act, which is the whole design: the
// strategy decides and the driver performs, so the decision is pure and the family is measurable without a broker.
type Plan struct {
	// Exchange is the exchange to publish to now.
	Exchange string
	// RoutingKey is the routing key to publish with now.
	RoutingKey string
	// Header is the header name to set, or empty. [Plugin] sets "x-delay".
	Header string
	// HeaderMillis is the value of that header, in milliseconds.
	HeaderMillis int64
	// Declare is the queue that has to exist before the publication, or nil
	// when the strategy needs none. It is idempotent: the driver declares it
	// once per name and remembers.
	Declare *QueueDecl
	// Granted is the delay the message will really experience, which is at
	// least Request.Delay and may be more. The specification makes a delay a minimum, so
	// rounding up is allowed and rounding down is not.
	Granted time.Duration
	// Hops is how many times the broker moves the message before it is
	// deliverable again: one for a publication the broker holds itself, two
	// for a park-and-dead-letter. It is the closest thing to the specification's "broker
	// CPU" this package can compute, and the driver page says so.
	Hops int
}

// QueueDecl is a delay queue's declaration.
//
// Every field is an AMQP 0-9-1 queue argument or a `queue.declare` parameter,
// so a driver copies it into the client's own type and performs no arithmetic
// of its own.
type QueueDecl struct {
	// Name is the queue's name.
	Name string
	// TTLMillis is `x-message-ttl`: how long a message sits here before the
	// broker expires it. It is fixed at declaration, which is why there is
	// one queue per bucket.
	TTLMillis int64
	// DeadLetterExchange is `x-dead-letter-exchange`: where an expired
	// message goes. The empty string is the default exchange.
	DeadLetterExchange string
	// DeadLetterRoutingKey is `x-dead-letter-routing-key`. It must be set
	// whenever the dead-letter exchange is the default one, because the
	// default exchange routes by queue name and an expired message would
	// otherwise keep the delay queue's own routing key and come back here
	// for ever.
	DeadLetterRoutingKey string
	// ExpiresMillis is `x-expires`: how long the delay queue itself survives
	// with no consumer and no messages, so that a deployment whose delays
	// change over time does not accumulate empty queues for ever. Zero
	// leaves the queue permanent.
	ExpiresMillis int64
}

// Strategy decides how to realise a delayed message.
//
// Every method is pure and safe to call from any goroutine: a strategy holds
// no state, so one value may serve every source of a client.
type Strategy interface {
	// Plan returns how to realise req, or refuses it.
	Plan(req Request) (Plan, error)
	// Requires is what the broker must provide for this strategy to work.
	Requires() Requirement
	// MaxDelay is the longest delay this strategy can express. A longer one
	// is refused rather than silently truncated.
	MaxDelay() time.Duration
	// Resolution is the smallest delay this strategy can express, so that a
	// caller can tell what a request for a millisecond will really cost.
	Resolution() time.Duration
	// Queues reports how many distinct delay queues a set of requests needs.
	//
	// It is the specification's "queue count growth" as a number, and it is the term the
	// two arms differ most on: one arm needs a queue per (target, bucket)
	// pair and the other needs none at all.
	Queues(reqs []Request) int
	// Kind returns the variant this strategy implements.
	Kind() Kind
}

// Config configures a strategy. The zero value is usable for every kind.
type Config struct {
	// Ladder is the buckets [TTLDLX] rounds a delay up to, ascending and
	// strictly increasing. Empty means [DefaultLadder].
	//
	// A deployment whose delays are known exactly should name them here: the
	// rounding error then becomes zero and the queue count becomes the
	// number of delays the deployment really uses, which is the whole of
	// what the two TTL+DLX criteria measure. That is the escape hatch the
	// default ladder's error exists to make unnecessary.
	Ladder []time.Duration
	// QueueTTL is `x-expires` on a delay queue: how long an unused delay
	// queue survives, so that a ladder a deployment has stopped using does
	// not leave empty queues behind for ever. Zero means the queues are
	// permanent, which is the safe default — a queue that expires while a
	// message is on its way to it would lose the message.
	QueueTTL time.Duration
}

// The ladder's shape.
const (
	// LadderBase is the smallest bucket of [DefaultLadder] and therefore
	// [TTLDLX]'s resolution. A hundred milliseconds rather than one
	// millisecond because each bucket is a QUEUE: a ladder fine enough to
	// express a millisecond exactly would be tens of thousands of queues,
	// and RabbitMQ's own dead-letter hop is itself a few milliseconds, so a
	// finer bucket would be below the mechanism's own noise.
	LadderBase = 100 * time.Millisecond
	// LadderSteps is how many buckets [DefaultLadder] has. Each is twice the
	// one below, so twenty-one buckets reach a little over a day and the
	// worst rounding error is a factor of two.
	LadderSteps = 21
)

// DefaultLadder is the bucket ladder [TTLDLX] uses when a configuration names
// none: [LadderBase] doubling [LadderSteps] times.
//
// The doubling is the trade the criterion is about, stated as arithmetic. A
// ladder of k buckets covering a range of r has a worst-case relative error of
// r^(1/k) - 1, so the queue count and the accuracy move together and nothing
// can improve both: twenty-one buckets over a factor of a million give a
// worst case of one bucket short of double the requested delay, and halving
// the queue count would square that. A deployment that needs better than that
// names its own delays in [Config.Ladder] and gets both.
func DefaultLadder() []time.Duration {
	out := make([]time.Duration, LadderSteps)
	d := LadderBase
	for i := range out {
		out[i] = d
		d *= 2
	}
	return out
}

// MaxPluginDelay is the longest delay the delayed-message plugin accepts.
//
// The plugin carries the delay in a 32-bit signed integer of milliseconds, so
// the ceiling is a little under twenty-five days. A longer delay is refused
// rather than truncated: a message that reappears twenty-four days early is
// worse than a refusal, which the caller can see.
const MaxPluginDelay = time.Duration(1<<31-1) * time.Millisecond

// DelayHeader is the header the delayed-message plugin reads.
const DelayHeader = "x-delay"

// New returns the strategy of kind k configured by cfg.
func New(k Kind, cfg Config) (Strategy, error) {
	if err := cfg.Validate(k); err != nil {
		return nil, err
	}
	base := strategy{cfg: cfg.resolve(), kind: k.Resolve()}
	switch k.Resolve() {
	case TTLDLX:
		return ttldlx{base}, nil
	case Plugin:
		return plugin{base}, nil
	case Auto:
		// Unreachable: Resolve maps Auto to a concrete kind. Returning the
		// reference kind rather than panicking keeps a future AutoKind typo
		// from crashing a caller's Open.
		return ttldlx{base}, nil
	default:
		return nil, ErrInvalidKind
	}
}

// strategy is the shared state every kind carries.
type strategy struct {
	cfg  Config
	kind Kind
}

// Kind returns the variant.
func (s strategy) Kind() Kind { return s.kind }

// resolve fills in the defaults.
func (c Config) resolve() Config {
	if len(c.Ladder) == 0 {
		c.Ladder = DefaultLadder()
	}
	return c
}

// Validate reports whether the configuration can be used with k.
//
// The ladder's own shape is checked here and not at the first plan, because a
// ladder that is not strictly increasing would silently round a delay to the
// wrong bucket and a driver validates its configuration before it starts
func (c Config) Validate(k Kind) error {
	if !k.Valid() {
		return ErrInvalidKind
	}
	if c.QueueTTL < 0 {
		return ErrInvalidQueueTTL
	}
	prev := time.Duration(0)
	for _, d := range c.Ladder {
		if d <= 0 || d <= prev {
			return ErrInvalidLadder
		}
		prev = d
	}
	return nil
}

// The family's refusals.
var (
	// ErrInvalidKind is a kind outside the enumeration.
	ErrInvalidKind = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the delay strategy is not a known kind")
	// ErrInvalidLadder is a bucket ladder that is not strictly increasing or
	// that contains a non-positive bucket.
	ErrInvalidLadder = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the delay bucket ladder must be positive and strictly increasing")
	// ErrInvalidQueueTTL is a negative x-expires.
	ErrInvalidQueueTTL = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the delay queue's x-expires cannot be negative")
	// ErrNegativeDelay is a request for a delay in the past.
	ErrNegativeDelay = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: a delay cannot be negative")
	// ErrDelayTooLong is a delay beyond what the strategy can express. It is
	// a refusal rather than a truncation: the specification makes a delay a minimum, and
	// a message that reappears days early is worse than an error the caller
	// can see.
	ErrDelayTooLong = errcode.New(errcode.Unsupported,
		"iqmq/rabbitmq: the delay is longer than this strategy can express")
	// ErrNoTarget is a request with nowhere for the message to reappear.
	ErrNoTarget = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: a delayed message needs a target exchange and routing key to reappear at")
	// ErrNoDelayExchange is a Plugin request with no x-delayed-message
	// exchange named.
	ErrNoDelayExchange = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the plugin delay strategy needs the name of the x-delayed-message exchange to publish to")
)

// checkRequest is the validation both kinds share.
func checkRequest(req Request, maxDelay time.Duration) error {
	switch {
	case req.Delay < 0:
		return ErrNegativeDelay
	case req.Target.RoutingKey == "" && req.Target.Exchange == "":
		return ErrNoTarget
	case req.Delay > maxDelay:
		return ErrDelayTooLong
	default:
		return nil
	}
}

// millis is a duration in whole milliseconds, rounded UP so that the value a
// plan carries is never shorter than the delay it was derived from.
func millis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// nameOf is the target's name for the purpose of a delay queue's name, with
// the routing key as the fallback for a target that named none: the default
// exchange has no name, so a redelivery target's name is its queue.
func nameOf(t Target) string {
	if t.Name != "" {
		return t.Name
	}
	if t.Exchange != "" {
		return t.Exchange
	}
	return t.RoutingKey
}

// delayQueueName is [rabbit.DelayQueue] over a target, so that both kinds and
// the queue-count metric derive the name one way.
func delayQueueName(t Target, bucketMillis int64) (string, error) {
	return rabbit.DelayQueue(nameOf(t), bucketMillis)
}

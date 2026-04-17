// Package rabbit holds the RabbitMQ-specific code that is pure — naming,
// broker-version arithmetic, delay planning, publisher-confirm bookkeeping and
// the two protocols' property mappings — and is therefore shared by both
// RabbitMQ driver modules and reachable by a root-module `go test`.
//
// Both halves of that sentence are load-bearing.
//
// It is SHARED because there are two RabbitMQ drivers: `drivers/rabbitmq`
// speaks AMQP 0-9-1 through github.com/rabbitmq/amqp091-go and
// `drivers/rabbitmq/amqp10` speaks AMQP 1.0 through `drivers/amqp10` over
// github.com/Azure/go-amqp (the driver documentation). A caller
// who moves between them must find the same exchange, the same quorum queue
// and the same dead-letter route, or the two profiles are two topologies
// wearing one name — so the naming lives once, here, and neither driver owns
// it. That is `internal/streams`' shape for the two Redis drivers.
//
// It is REACHABLE because three of this repository's variant families are
// RabbitMQ's — (delayed redelivery), (publisher confirms) and (the
// protocol) — and measures a family by running one
// `go test` in one package. A root-module `go test` cannot reach a nested
// driver module, so a family whose kinds lived in `drivers/rabbitmq` could not
// be measured at all (and the specification's own failure). The
// kinds are therefore in `internal/rabbit/{delay,confirm,protocol}` and the
// drivers bind them, which is the and the arrangement.
//
// The package imports the standard library and the root package only: the root
// module is standard-library-only (`make stdlib-check`), and nothing in here
// needs a client library. What it cannot do is speak to a broker — every
// function is a pure decision about names, times or bytes — so every claim in
// here is checked by a model test or a fuzz target rather than by a container,
// and the container work is the drivers' own.
package rabbit

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// The limits AMQP 0-9-1 and RabbitMQ put on the names this package builds.
const (
	// MaxNameLen is the longest exchange or queue name RabbitMQ accepts: a
	// shortstr on the wire, so 255 BYTES and not 255 runes. A name that is
	// too long is refused before any I/O, because the broker's own refusal
	// is a channel exception that closes the channel — which on a confirm
	// channel takes every publication in flight with it.
	MaxNameLen = 255
	// MaxRoutingKeyLen is the longest routing key, also a shortstr.
	MaxRoutingKeyLen = 255
	// ReservedPrefix is the prefix RabbitMQ reserves for itself. A client
	// that declares a name under it is refused with ACCESS_REFUSED (403),
	// and `amq.default`, `amq.topic` and friends are the broker's own
	// exchanges.
	ReservedPrefix = "amq."
)

// The suffixes this driver appends. They are constants rather than
// configuration because a caller who changed them would rename the topology of
// a running deployment, and because a dead-letter route whose name is not
// derivable from the queue's cannot be found again by an operator.
const (
	// DLQSuffix names a subscription's dead-letter queue: the specification's
	// "DLX → <queue>.dlq".
	DLQSuffix = ".dlq"
	// DLXSuffix names the exchange that routes to it. It is a separate
	// exchange rather than the default one because a dead-lettered message
	// keeps its original routing key, and the default exchange would then
	// deliver it to a queue of that name rather than to the dead-letter
	// queue.
	DLXSuffix = ".dlx"
	// DelayPrefix is the infix of a TTL-and-dead-letter delay queue
	// (the specification's `ttldlx` kind): "<queue>.delay.<bucket>".
	DelayPrefix = ".delay."
	// DelayExchangeSuffix names the exchange a delayed message is published
	// to under the specification's `plugin` kind.
	DelayExchangeSuffix = ".delayed"
)

// Names is the whole topology one topic and one subscription map to.
//
// It is a value and not a set of functions because the names are derived
// TOGETHER and have to agree: the dead-letter exchange's name is derived from
// the queue's, the delay queues dead-letter back to the exchange the queue is
// bound to, and a driver that recomputed one of them from a slightly different
// input would build a topology whose parts do not point at each other. Deriving
// them once, in one place, is what the conformance suite's dead-letter and
// delayed-redelivery scenarios rest on.
type Names struct {
	// Exchange is the durable exchange a topic maps to. It is the
	// topic name under the configured prefix.
	Exchange string
	// Queue is the durable quorum queue a subscription maps to:
	// "<topic>.<subscription>".
	Queue string
	// RoutingKey is the key the queue is bound to the exchange with.
	RoutingKey string
	// DeadLetterExchange is the exchange a rejected or expired message is
	// routed to.
	DeadLetterExchange string
	// DeadLetterQueue is the queue bound to it.
	DeadLetterQueue string
	// DeadLetterRoutingKey is the key the dead-letter queue is bound with.
	// It is a CONSTANT and not the message's own key, which is the whole
	// reason the dead-letter exchange exists: RabbitMQ dead-letters a
	// message with its original routing key unless the queue names a
	// replacement, so a fan-out-shaped DLX with one binding would route a
	// message keyed "orders.eu" nowhere at all.
	DeadLetterRoutingKey string
	// DelayExchange is the exchange the specification's `plugin` kind publishes a
	// delayed message to.
	DelayExchange string
}

// Config is what a name set is derived from.
type Config struct {
	// Prefix is prepended to every exchange and queue name, so that two
	// deployments can share one virtual host. It is empty by default: a
	// driver that prefixed by default would rename the topology of anybody
	// who set nothing.
	Prefix string
	// Topic is the topic.
	Topic string
	// Subscription is the subscription, empty for a publish-only name set.
	Subscription string
	// KeyAsRoutingKey reports that the message key, not the topic, is the
	// routing key (WithKeyAsRoutingKey). It changes the BINDING: a
	// queue that wants every message of a topic must then bind with "#".
	KeyAsRoutingKey bool
	// DeadLetterTopic routes a dead-lettered message to ANOTHER TOPIC's
	// exchange rather than to the derived `<queue>.dlq`.
	//
	// It changes three of the derived names together, which is the whole
	// reason [Names] is one value: the dead-letter exchange becomes that
	// topic's exchange, the routing key becomes that topic's own key so that
	// its subscriptions' bindings match, and the dead-letter QUEUE becomes
	// empty — the destination belongs to another topic and its subscriptions
	// declare their own queues, so a driver that declared one here would
	// create a queue nothing consumes and dead-letter into it.
	DeadLetterTopic string
}

// Derive builds the name set, or refuses the input.
//
// Every name is validated here and not at the broker. The reason is the shape
// of an AMQP 0-9-1 failure rather than tidiness: a `queue.declare` with a name
// the broker refuses raises a CHANNEL exception, the channel closes, and on a
// publisher-confirm channel every publication whose confirm had not yet
// arrived becomes ambiguous. Refusing before the first frame keeps a
// caller's typo out of that path.
func Derive(cfg Config) (Names, error) {
	if err := CheckTopic(cfg.Topic); err != nil {
		return Names{}, err
	}
	exchange := cfg.Prefix + cfg.Topic
	if err := checkDeclarable(exchange, "exchange"); err != nil {
		return Names{}, err
	}
	n := Names{
		Exchange:      exchange,
		RoutingKey:    cfg.Topic,
		DelayExchange: exchange + DelayExchangeSuffix,
	}
	if cfg.KeyAsRoutingKey {
		// The topic is no longer the routing key, so the queue binds to
		// everything the exchange receives. "#" is a topic exchange's
		// match-every-word pattern.
		n.RoutingKey = MatchAll
	}
	if err := checkDeclarable(n.DelayExchange, "delay exchange"); err != nil {
		return Names{}, err
	}
	if cfg.Subscription == "" {
		return n, nil
	}
	if err := CheckSubscription(cfg.Subscription); err != nil {
		return Names{}, err
	}
	n.Queue = exchange + "." + cfg.Subscription
	n.DeadLetterExchange = n.Queue + DLXSuffix
	n.DeadLetterQueue = n.Queue + DLQSuffix
	n.DeadLetterRoutingKey = DeadLetterKey
	if err := checkDeclarable(n.Queue, "queue"); err != nil {
		return Names{}, err
	}
	if cfg.DeadLetterTopic != "" {
		if err := CheckTopic(cfg.DeadLetterTopic); err != nil {
			return Names{}, err
		}
		n.DeadLetterExchange = cfg.Prefix + cfg.DeadLetterTopic
		n.DeadLetterQueue = ""
		n.DeadLetterRoutingKey = cfg.DeadLetterTopic
		if err := checkDeclarable(n.DeadLetterExchange, "dead-letter exchange"); err != nil {
			return Names{}, err
		}
		return n, nil
	}
	for name, what := range map[string]string{
		n.DeadLetterExchange: "dead-letter exchange",
		n.DeadLetterQueue:    "dead-letter queue",
	} {
		if err := checkDeclarable(name, what); err != nil {
			return Names{}, err
		}
	}
	return n, nil
}

// HasWildcard reports whether s carries a topic exchange's pattern
// characters.
//
// It exists because a wildcard is a legitimate ROUTING KEY PATTERN on a topic
// exchange and is not a legitimate TOPIC for this driver: a topic maps to its
// own exchange, so a pattern over topic names would have to span exchanges and
// a binding cannot. The driver answers a pattern topic with ErrUnsupported
// rather than with an invalid-argument refusal, which is what makes its
// negative FeatureWildcardSubscribe claim provable.
func HasWildcard(s string) bool {
	return strings.ContainsAny(s, "*#")
}

// DeadLetterKey is the routing key a dead-lettered message is re-published
// with. It is a fixed word rather than the message's own key: see
// [Names.DeadLetterRoutingKey].
const DeadLetterKey = "dlq"

// MatchAll is a topic exchange's "every routing key" pattern.
const MatchAll = "#"

// DelayQueue is the name of the TTL queue the specification's `ttldlx` kind parks a
// message in for a bucket, in milliseconds.
//
// The bucket is in the NAME because RabbitMQ fixes a queue's x-message-ttl at
// declaration: a queue holds one TTL, so one queue per bucket is what the
// mechanism costs and the specification's "queue count growth" is the criterion that reads
// it.
func DelayQueue(queue string, bucketMillis int64) (string, error) {
	name := queue + DelayPrefix + strconv.FormatInt(bucketMillis, 10)
	if err := checkDeclarable(name, "delay queue"); err != nil {
		return "", err
	}
	return name, nil
}

// CheckTopic reports whether a topic can be a concrete RabbitMQ exchange name
// and routing key.
//
// It refuses "*" and "#", and that refusal is the point of the function. A
// topic name is used BOTH as the exchange name and as the routing key a
// subscription's queue is bound with, and on a topic exchange those two
// characters are the binding pattern's wildcards: a topic literally called
// "orders.#" would bind a queue to every routing key under "orders", so a
// subscription of one topic would silently receive another topic's messages.
// A wildcard SUBSCRIPTION is a different thing and goes through
// [CheckPattern].
func CheckTopic(topic string) error {
	if err := checkName(topic, "topic"); err != nil {
		return err
	}
	if strings.ContainsAny(topic, "*#") {
		return errName("topic", errTopicWildcard)
	}
	return nil
}

// CheckSubscription reports whether a subscription name can be half of a queue
// name.
//
// It refuses a dot, which is narrower than the broker is: RabbitMQ would take
// "orders.eu.workers" happily. The reason is that the queue name is
// "<topic>.<subscription>" and an operator reading a queue list has to be able
// to tell which part is which — and, more sharply, because
// [ParseQueue] is what the dead-letter and delay names are parsed back out of,
// and a dotted subscription makes that parse ambiguous.
func CheckSubscription(sub string) error {
	if err := checkName(sub, "subscription"); err != nil {
		return err
	}
	switch {
	case strings.ContainsAny(sub, "*#"):
		return errName("subscription", errSubscriptionWildcard)
	case strings.Contains(sub, "."):
		return errName("subscription", errSubscriptionDot)
	}
	for _, reserved := range reservedSubscriptions {
		if sub == reserved {
			return errName("subscription", errSubscriptionReserved)
		}
	}
	return nil
}

// reservedSubscriptions are the subscription names this driver's own derived
// names would collide with.
//
// A subscription called "dlq" would derive the queue "<topic>.dlq", which is
// the name the dead-letter queue of every OTHER subscription of that topic
// has — so a consumer of the "dlq" subscription would be consuming the
// dead-letter queue, and [ParseQueue] could not tell the two apart. Three
// reserved words is a smaller cost than a derived name that is also a real
// one.
var reservedSubscriptions = [...]string{"dlq", "dlx", "delay"}

// CheckPattern reports whether a subscription pattern is a usable topic-exchange
// binding key.
//
// The syntax is the topic exchange's: dot-separated words, "*" for exactly one
// word and "#" for zero or more. A pattern is accepted where a topic is
// refused, which is the asymmetry [CheckTopic] exists for.
func CheckPattern(pattern string) error {
	if err := checkName(pattern, "pattern"); err != nil {
		return err
	}
	for word := range strings.SplitSeq(pattern, ".") {
		switch word {
		case "*", MatchAll:
			continue
		}
		if strings.ContainsAny(word, "*#") {
			// "ord*rs" is not a pattern: the wildcards match whole words,
			// so a "*" inside one is a literal the broker will never match
			// and is far more likely to be a caller's misunderstanding.
			return errName("pattern", errPatternWord)
		}
	}
	return nil
}

// Matches reports whether a topic-exchange binding pattern matches a routing
// key, by AMQP 0-9-1's own rules.
//
// It exists so that a driver can answer the specification's "no hidden creation" question —
// does this subscription's binding really reach the topic the caller named? —
// and so that the wildcard scenarios have an oracle that is not the
// broker.
func Matches(pattern, key string) bool {
	return matchWords(splitWords(pattern), splitWords(key))
}

// splitWords splits a routing key or pattern into its dot-separated words. An
// empty string is no words rather than one empty word, which is what makes
// "#" match the empty routing key.
func splitWords(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ".")
}

// matchWords is the recursive half of [Matches]: "*" consumes one word and "#"
// consumes zero or more.
func matchWords(pat, key []string) bool {
	for len(pat) > 0 {
		switch pat[0] {
		case MatchAll:
			// "#" at the end matches whatever is left, including nothing.
			if len(pat) == 1 {
				return true
			}
			for i := 0; i <= len(key); i++ {
				if matchWords(pat[1:], key[i:]) {
					return true
				}
			}
			return false
		case "*":
			if len(key) == 0 {
				return false
			}
		default:
			if len(key) == 0 || pat[0] != key[0] {
				return false
			}
		}
		pat, key = pat[1:], key[1:]
	}
	return len(key) == 0
}

// ParseQueue splits a queue name back into its topic and subscription under a
// prefix, which is how a driver answers "which subscription is this delivery
// from" for a queue it did not open itself and how the dead-letter and delay
// names are recognised.
//
// It reports false for a name that is not one of this driver's, which includes
// the dead-letter and delay queues: those are derived FROM a queue name and
// are not queue names in this sense.
func ParseQueue(prefix, name string) (topic, sub string, ok bool) {
	if IsDerived(name) {
		return "", "", false
	}
	rest, ok := strings.CutPrefix(name, prefix)
	if !ok || rest == "" {
		return "", "", false
	}
	i := strings.LastIndexByte(rest, '.')
	if i <= 0 || i == len(rest)-1 {
		return "", "", false
	}
	topic, sub = rest[:i], rest[i+1:]
	if CheckTopic(topic) != nil || CheckSubscription(sub) != nil {
		return "", "", false
	}
	return topic, sub, true
}

// IsDerived reports whether a name is one this driver DERIVES from a queue
// name rather than one a subscription maps to: a dead-letter queue, a
// dead-letter exchange or a delay queue.
//
// It is what keeps [ParseQueue] unambiguous. "orders.workers.dlq" splits into
// the topic "orders.workers" and the subscription "dlq" exactly as well as
// "orders.workers" splits into "orders" and "workers", so the split alone
// cannot tell a subscription's queue from another subscription's dead-letter
// queue — and a driver that read a delivery's subscription off the wrong one
// would report a dead-lettered message as a live one. The derived forms are
// recognised first and the reserved subscription names keep a real
// subscription from ever producing one.
func IsDerived(name string) bool {
	switch {
	case strings.HasSuffix(name, DLQSuffix), strings.HasSuffix(name, DLXSuffix):
		return true
	case strings.Contains(name, DelayPrefix):
		return true
	default:
		return false
	}
}

// CheckName is [checkName] for a name this package DERIVED rather than one a
// caller supplied.
//
// It exists for the fuzz targets and for a driver that wants to assert, before
// it writes a frame, that the name it is about to declare is one the broker
// accepts. There is deliberately no exported constructor that skips this: a
// name over 255 bytes or under "amq." is refused by the broker with a channel
// exception, and on a publisher-confirm channel a channel exception takes
// every publication whose confirm has not yet arrived with it.
func CheckName(name string) error { return checkDeclarable(name, "name") }

// checkName is the shared shortstr validation: non-empty, valid UTF-8, at most
// 255 bytes and no control characters.
//
// The control-character rule is not the broker's — RabbitMQ stores the bytes —
// and it is here because a name with a newline in it is unreadable in every
// management view, every log line and every error message this driver
// produces, and because it is almost always a caller having concatenated
// something it read from a file.
func checkName(name, what string) error {
	switch {
	case name == "":
		return errName(what, errEmptyName)
	case len(name) > MaxNameLen:
		return errName(what, errNameTooLong)
	case !utf8.ValidString(name):
		return errName(what, errNameNotUTF8)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return errName(what, errNameControl)
		}
	}
	return nil
}

// checkDeclarable is [checkName] plus the rule that only applies to a name this
// driver DECLARES: RabbitMQ reserves "amq." for itself and answers a
// declaration under it with ACCESS_REFUSED.
//
// It is checked on the derived name rather than on the topic, because the
// prefix is part of what the broker sees: a caller whose Prefix is "amq." and
// whose topic is "orders" would otherwise reach the broker.
func checkDeclarable(name, what string) error {
	if err := checkName(name, what); err != nil {
		return err
	}
	if strings.HasPrefix(name, ReservedPrefix) {
		return errName(what, errNameReserved)
	}
	return nil
}

// errName attaches the thing being named to one of the shared refusals, so
// that a caller is told which of its inputs was refused. The rule itself is a
// shared value, so errors.Is reaches it and a test can name the rule rather
// than matching a string.
func errName(what string, err error) error {
	return &NameError{What: what, Err: err}
}

// NameError is a refusal of one derived name, naming which name it was.
type NameError struct {
	// What is the thing being named: "queue", "exchange", "delay queue".
	What string
	// Err is the rule that refused it.
	Err error
}

// Error implements error.
func (e *NameError) Error() string { return "iqmq/rabbitmq: " + e.What + ": " + e.Err.Error() }

// Unwrap gives errors.Is and mq.CodeOf the underlying rule.
func (e *NameError) Unwrap() error { return e.Err }

// The naming refusals. Every one of them is CodeInvalidArgument: the caller
// named something this broker cannot name, which is neither a configuration
// error nor a missing feature.
var (
	errEmptyName = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the name is empty")
	errNameTooLong = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the name is longer than the 255 bytes an AMQP shortstr can carry")
	errNameNotUTF8 = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the name is not valid UTF-8")
	errNameControl = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the name contains a control character")
	errNameReserved = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the name begins with the reserved prefix \"amq.\", which RabbitMQ answers with ACCESS_REFUSED")
	errTopicWildcard = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: a topic may not contain \"*\" or \"#\": the topic is the routing key a subscription's queue is bound with, so a wildcard in it would bind the queue to other topics (use a wildcard SUBSCRIPTION pattern instead)")
	errSubscriptionWildcard = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: a subscription name may not contain \"*\" or \"#\"")
	errSubscriptionDot = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: a subscription name may not contain \".\": the queue is named \"<topic>.<subscription>\" and a dotted subscription makes that name ambiguous")
	errSubscriptionReserved = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: the subscription names one of this driver's own derived queues (\"dlq\", \"dlx\", \"delay\"), whose name would then be a real subscription's queue too")
	errPatternWord = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: \"*\" and \"#\" match whole dot-separated words, so a pattern may not put one inside a word")
)

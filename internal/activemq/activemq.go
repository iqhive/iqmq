// # Why two brokers are one package
//
// "ActiveMQ" is two products. Apache ActiveMQ Artemis routes a topic through a
// MULTICAST address and gives every subscription a durable queue of its own,
// addressed by the fully qualified queue name `address::queue`; Apache
// ActiveMQ Classic has no such thing and uses VIRTUAL TOPICS, a naming
// convention under which a publication to `VirtualTopic.orders` is copied into
// every queue called `Consumer.<name>.VirtualTopic.orders`. The two mappings
// share no name, and the rule — two consumers of ONE subscription compete,
// two consumers of DIFFERENT subscriptions each receive the message — is the
// only thing they have in common.
//
// Both are reached over two protocols (AMQP 1.0 and STOMP, variant family
// The specification), which is four combinations of which no two agree on every claim. Every
// one of those disagreements is a fact about the broker rather than about the
// client library, so it belongs here rather than in either driver: a name
// derived slightly differently by the AMQP driver and the STOMP driver would
// build two topologies that do not point at each other, which is the defect
// [Derive] exists to make impossible.
package activemq

import (
	"strings"

	"github.com/iqhive/iqmq/internal/errcode"
)

// Broker is which ActiveMQ a driver is speaking to.
//
// There is no Auto: the two products differ in their addressing, their
// scheduled-delivery header and their answer to a negative settlement, and
// nothing on the wire identifies which one a caller MEANT. A driver that
// guessed would address a virtual topic on Artemis, which auto-creates an
// address literally called `Consumer.workers.VirtualTopic.orders` and
// publishes into it for ever. The specification's "never silently substituted" is the rule;
// a required field is how it is kept.
type Broker uint8

// The brokers.
const (
	// BrokerUnset is the zero value and names NO broker.
	//
	// It exists so that the field this type's own documentation calls
	// required can be enforced rather than described. Without it the zero
	// value would be Artemis, and a Classic deployment whose configuration
	// omitted the line would silently get the Artemis mapping -- which is
	// the exact hazard [Broker] is about: the publication goes to a
	// destination nothing consumes, the broker creates it, and there is no
	// error anywhere. Measured as a defect in `drivers/activemq/stomp`,
	// where both doc comments said REQUIRED and only the DSN path refused
	// it.
	BrokerUnset Broker = iota
	// Artemis is Apache ActiveMQ Artemis: multicast addresses and fully
	// qualified queue names.
	Artemis
	// Classic is Apache ActiveMQ Classic (the 5.x/6.x line): virtual topics.
	Classic

	brokerCount
)

var brokerSlugs = [brokerCount]string{"", "artemis", "classic"}

// String returns the broker's lower-case slug.
func (b Broker) String() string {
	if b.Valid() {
		return brokerSlugs[b]
	}
	return "broker(?)"
}

// Valid reports whether b names a broker. The zero value does not.
func (b Broker) Valid() bool { return b > BrokerUnset && b < brokerCount }

// Brokers returns every broker, in declaration order. [BrokerUnset] is not one.
func Brokers() []Broker { return []Broker{Artemis, Classic} }

// ParseBroker reads a broker from its slug.
func ParseBroker(s string) (Broker, bool) {
	for i := Artemis; i < brokerCount; i++ {
		if brokerSlugs[i] == s {
			return i, true
		}
	}
	return BrokerUnset, false
}

// The product names each broker announces itself with.
//
// Artemis sends them in the AMQP 1.0 open performative's properties
// (`product`) and in the STOMP CONNECTED frame's `server` header; Classic
// sends the STOMP one and, over AMQP, sends no product at all. A driver reads
// them to refuse a broker it was not configured for BEFORE it creates
// anything, which is the only defence against the auto-creation hazard
// [Broker] documents.
const (
	// ProductArtemis is Artemis's AMQP `product` property.
	ProductArtemis = "apache-activemq-artemis"
	// ServerArtemis is the prefix of Artemis's STOMP `server` header
	// ("ActiveMQ-Artemis/2.44.0").
	ServerArtemis = "ActiveMQ-Artemis"
	// ServerClassic is the prefix of Classic's STOMP `server` header
	// ("ActiveMQ/6.2.0").
	ServerClassic = "ActiveMQ"
)

// The Classic virtual-topic convention (the driver documentation).
const (
	// VirtualTopicPrefix is the prefix a Classic destination must carry for
	// the broker to copy a publication into the consumer queues. It is
	// Classic's DEFAULT convention and not configurable here: changing it
	// needs a `virtualDestinationInterceptor` in the broker's own
	// configuration, and a driver that renamed it on its own would publish
	// into a topic nothing is subscribed to.
	VirtualTopicPrefix = "VirtualTopic."
	// ConsumerPrefix is the prefix of a virtual topic's consumer queue.
	ConsumerPrefix = "Consumer."
)

// FQQN is Artemis's fully qualified queue name separator: a receiver attaches
// to `address::queue` to consume one named queue of a multicast address rather
// than whatever the broker would give it.
const FQQN = "::"

// Names is the topology one topic and one subscription map to, on one broker.
//
// It is one value because the parts have to agree. A publication goes to
// [Names.Publish] and a consumer attaches to [Names.Consume]; on Artemis the
// second is built from the first and from [Names.Queue], and on Classic both
// are built from the same virtual-topic name. Deriving them separately is how
// a driver comes to publish to `VirtualTopic.orders` and consume from
// `Consumer.workers.orders`, which is silent message loss with every part
// individually plausible.
type Names struct {
	// Publish is the destination a publication is sent to, in the broker's
	// own form: an Artemis address, or a Classic `topic://VirtualTopic.<t>`.
	Publish string
	// Consume is the destination a consumer attaches to: an Artemis FQQN
	// `address::queue`, or a Classic `queue://Consumer.<s>.VirtualTopic.<t>`.
	// It is empty for a publish-only name set.
	Consume string
	// Address is the Artemis multicast address a topic maps to, without any
	// scheme. It is what Admin creates. On Classic it is the virtual topic's
	// bare name (`VirtualTopic.<t>`).
	Address string
	// Queue is the durable queue a subscription maps to, without any scheme
	// and without the address: `<topic>.<subscription>` on Artemis, and
	// `Consumer.<subscription>.VirtualTopic.<topic>` on Classic. It is what
	// Admin creates and what Artemis's FQQN names after the "::".
	//
	// On Artemis the queue name carries the TOPIC as well as the
	// subscription, and that is not decoration: an Artemis queue name is
	// unique across the whole broker, so two topics with a subscription
	// called "workers" collide — the second `createQueue` is refused with
	// AMQ229019 and, worse, a consumer that attached by FQQN would be
	// refused with amqp:not-found while the queue it names exists on another
	// address entirely.
	Queue string
	// DeadLetter is the destination a dead-lettered message is published to,
	// in the broker's own form, or empty when the broker's own dead-letter
	// policy handles it.
	DeadLetter string
	// DeadLetterQueue is the queue bound to it, or empty.
	DeadLetterQueue string
}

// Config is what a name set is derived from.
type Config struct {
	// Broker is which ActiveMQ the names are for. It is required.
	Broker Broker
	// Prefix is prepended to every address and queue so that two
	// deployments can share one broker. It is empty by default: a driver
	// that prefixed by default would rename the topology of anybody who set
	// nothing.
	//
	// On Classic the prefix goes AFTER the virtual-topic prefix
	// (`VirtualTopic.<prefix><topic>`), and the order is measured rather
	// than chosen: Classic's default virtual-destination interceptor
	// matches `VirtualTopic.>`, so a destination called
	// `iqmq.VirtualTopic.orders` is NOT a virtual topic at all. It is an
	// ordinary topic, every publication to it is accepted, and no consumer
	// queue receives anything -- with no error on either side.
	//
	// Measured against apache/activemq-classic:6.2.0, publishing to
	// `topic://<X>` with a consumer on `queue://Consumer.A.<X>`:
	//
	//	iqmq.VirtualTopic.p   NOT DELIVERED
	//	VirtualTopic.iqmq.q   delivered
	//	VirtualTopic.r        delivered
	//
	// The first ordering was this package's first draft and the conformance
	// suite is what found it: every scenario timed out waiting for a
	// delivery.
	Prefix string
	// Topic is the topic.
	Topic string
	// Subscription is the subscription, empty for a publish-only name set.
	Subscription string
	// DeadLetterTopic routes a dead-lettered message to another TOPIC
	// instead of to the broker's own dead-letter address.
	DeadLetterTopic string
	// Scheme reports that Classic's destinations carry their `topic://` and
	// `queue://` schemes. It is true for AMQP 1.0, where the scheme is the
	// only thing that tells the broker which of the two a name is, and false
	// for STOMP, which says so with `/topic/` and `/queue/` instead.
	Scheme bool
	// STOMP reports that the destinations are STOMP's rather than AMQP's:
	// Classic's `/topic/` and `/queue/` path form.
	STOMP bool
}

// Derive builds the name set, or refuses the input.
//
// Every name is checked here rather than at the broker, and the reason is what
// an ActiveMQ refusal costs rather than tidiness. On Artemis an attach to a
// name the broker will not accept detaches the LINK with amqp:not-found, and
// the session goes with it; on Classic a bad destination is accepted and
// created, so the caller's typo becomes a queue nothing consumes and a
// publication nobody receives. Neither failure names the typo.
func Derive(cfg Config) (Names, error) {
	if !cfg.Broker.Valid() {
		return Names{}, ErrNoBroker
	}
	if err := CheckTopic(cfg.Topic); err != nil {
		return Names{}, err
	}
	if cfg.Prefix != "" {
		if err := CheckName(cfg.Prefix, "prefix"); err != nil {
			return Names{}, err
		}
	}
	n := Names{}
	switch cfg.Broker {
	case Artemis:
		n.Address = cfg.Prefix + cfg.Topic
		n.Publish = n.Address
	case Classic:
		// The prefix goes INSIDE the virtual-topic prefix; see
		// [Config.Prefix] for the measurement that settled it.
		n.Address = VirtualTopicPrefix + cfg.Prefix + cfg.Topic
		n.Publish = decorate(n.Address, "topic", cfg)
	case BrokerUnset, brokerCount:
		return Names{}, ErrNoBroker
	}
	if err := CheckName(n.Address, "address"); err != nil {
		return Names{}, err
	}
	if cfg.Subscription != "" {
		if err := CheckSubscription(cfg.Subscription); err != nil {
			return Names{}, err
		}
		switch cfg.Broker {
		case Artemis:
			n.Queue = n.Address + "." + cfg.Subscription
			n.Consume = n.Address + FQQN + n.Queue
		case Classic:
			// `Consumer.<subscription>.VirtualTopic.<prefix><topic>`, which
			// is what Classic's default consumer pattern
			// `Consumer.*.VirtualTopic.>` matches.
			n.Queue = ConsumerPrefix + cfg.Subscription + "." + VirtualTopicPrefix + cfg.Prefix + cfg.Topic
			n.Consume = decorate(n.Queue, "queue", cfg)
		case BrokerUnset, brokerCount:
			return Names{}, ErrNoBroker
		}
		if err := CheckName(n.Queue, "queue"); err != nil {
			return Names{}, err
		}
	}
	if cfg.DeadLetterTopic == "" {
		return n, nil
	}
	if err := CheckTopic(cfg.DeadLetterTopic); err != nil {
		return Names{}, err
	}
	dl := Config{Broker: cfg.Broker, Prefix: cfg.Prefix, Topic: cfg.DeadLetterTopic, Scheme: cfg.Scheme, STOMP: cfg.STOMP}
	dlNames, err := Derive(dl)
	if err != nil {
		return Names{}, err
	}
	n.DeadLetter = dlNames.Publish
	// The dead-letter destination belongs to another TOPIC, so its own
	// subscriptions declare their own queues. Declaring one here would
	// create a queue nothing consumes and dead-letter into it, which is
	// exactly the shape RabbitMQ note records.
	n.DeadLetterQueue = ""
	return n, nil
}

// decorate puts a Classic destination in the form the protocol needs.
//
// Classic has one namespace for queues and topics and decides which a name is
// from the destination's own shape: AMQP 1.0 carries `topic://` and `queue://`,
// STOMP carries `/topic/` and `/queue/`, and a name with neither is whatever
// the broker's default happens to be. A driver that sent an undecorated name
// would publish to a QUEUE called `VirtualTopic.orders`, which every consumer
// queue is then subscribed to and none of them receives anything from.
func decorate(name, kind string, cfg Config) string {
	switch {
	case cfg.STOMP:
		return "/" + kind + "/" + name
	case cfg.Scheme:
		return kind + "://" + name
	default:
		return name
	}
}

// MaxNameLen is the longest address or queue name this package accepts.
//
// Artemis stores a queue name as a SimpleString in its journal and Classic as
// a UTF-8 destination name, and neither documents a ceiling. 255 is the
// smallest ceiling any broker in this repository's harness imposes (RabbitMQ's
// queue name), so it is the one a portable driver can advertise without
// claiming a limit it cannot check.
const MaxNameLen = 255

// CheckTopic refuses a topic name a driver cannot map.
func CheckTopic(topic string) error {
	if topic == "" {
		return ErrNoTopic
	}
	if HasWildcard(topic) {
		return ErrTopicWildcard
	}
	return CheckName(topic, "topic")
}

// CheckSubscription refuses a subscription name a driver cannot map.
func CheckSubscription(sub string) error {
	if sub == "" {
		return ErrNoSubscription
	}
	if HasWildcard(sub) {
		return ErrSubscriptionWildcard
	}
	// A "." in a Classic subscription would be read as another word of the
	// consumer-queue name, so `Consumer.a.b.VirtualTopic.t` and
	// `Consumer.a.VirtualTopic.b.t` are two spellings of one queue and the
	// second is a DIFFERENT topic. Refusing the separator in a subscription
	// is what keeps [ParseQueue] a function.
	if strings.Contains(sub, ".") {
		return ErrSubscriptionDot
	}
	return CheckName(sub, "subscription")
}

// CheckName refuses a name neither broker can carry.
//
// The refused characters are the ones that mean something to a destination
// parser rather than the ones a journal cannot store: "::" is Artemis's fully
// qualified queue name separator, "://" and a leading "/" are Classic's
// scheme and path forms, and a comma separates destinations in a Classic
// composite destination — so a name containing any of them addresses
// something other than itself.
func CheckName(name, what string) error {
	switch {
	case name == "":
		return errName(what, ErrNameEmpty)
	case len(name) > MaxNameLen:
		return errName(what, ErrNameTooLong)
	case strings.Contains(name, FQQN):
		return errName(what, ErrNameFQQN)
	case strings.Contains(name, "/"):
		return errName(what, ErrNameSlash)
	case strings.Contains(name, ","):
		return errName(what, ErrNameComma)
	case strings.ContainsAny(name, "\x00\r\n"):
		return errName(what, ErrNameControl)
	case strings.ContainsAny(name, " \t"):
		return errName(what, ErrNameSpace)
	case strings.ContainsAny(name, ":"):
		// A single colon is a STOMP header separator and is what a
		// destination in a SUBSCRIBE frame cannot carry.
		return errName(what, ErrNameColon)
	}
	return nil
}

// HasWildcard reports whether s carries either broker's wildcard characters.
//
// Both brokers have a wildcard syntax over destination names — Artemis's "#"
// and "*", Classic's ">" and "*" — and neither is a legitimate TOPIC for these
// drivers: a topic maps to its own address, and a pattern over topics would
// have to span addresses. The drivers answer a pattern with ErrUnsupported
// rather than with an invalid-argument refusal, which is what makes their
// negative FeatureWildcardSubscribe claim provable rather than merely stated
// (the shape of).
func HasWildcard(s string) bool { return strings.ContainsAny(s, "#*>") }

// ParseQueue reads the topic and subscription back out of a queue name.
//
// It is the inverse of [Derive]'s queue naming and it exists for Admin: a
// driver that lists a broker's queues has to say which of them are its own,
// and the only way to know is to be able to take one apart. ok is false for a
// name this package would not have produced.
func ParseQueue(broker Broker, prefix, name string) (topic, sub string, ok bool) {
	// Where the prefix sits differs between the two products, so it is
	// stripped inside each branch rather than up front: on Artemis it is the
	// front of the whole name, and on Classic it is INSIDE the virtual-topic
	// prefix, because `iqmq.VirtualTopic.orders` is not a virtual topic at
	// all (see [Config.Prefix]).
	var rest string
	switch broker {
	case Artemis:
		if !strings.HasPrefix(name, prefix) {
			return "", "", false
		}
		rest = name[len(prefix):]
		// "<topic>.<subscription>", and a subscription cannot contain ".".
		i := strings.LastIndex(rest, ".")
		if i <= 0 || i == len(rest)-1 {
			return "", "", false
		}
		return rest[:i], rest[i+1:], true
	case Classic:
		// "Consumer.<subscription>.VirtualTopic.<prefix><topic>". The
		// prefix is inside the virtual-topic prefix, so the name is taken
		// apart from the left and the prefix is stripped from what is left
		// of the topic rather than from the front of the whole name.
		if !strings.HasPrefix(name, ConsumerPrefix) {
			return "", "", false
		}
		rest = name[len(ConsumerPrefix):]
		i := strings.Index(rest, "."+VirtualTopicPrefix)
		if i <= 0 {
			return "", "", false
		}
		sub = rest[:i]
		topic = rest[i+len("."+VirtualTopicPrefix):]
		if !strings.HasPrefix(topic, prefix) {
			return "", "", false
		}
		topic = topic[len(prefix):]
		if topic == "" || strings.Contains(sub, ".") {
			return "", "", false
		}
		return topic, sub, true
	case BrokerUnset, brokerCount:
	}
	return "", "", false
}

// errName wraps a name refusal with what was being named.
func errName(what string, err error) error { return &NameError{What: what, Err: err} }

// NameError says which name a refusal was about.
type NameError struct {
	// What is the thing being named ("topic", "queue", "address").
	What string
	// Err is the refusal.
	Err error
}

// Error implements error.
func (e *NameError) Error() string { return "iqmq/activemq: " + e.What + ": " + e.Err.Error() }

// Unwrap returns the refusal, so that errors.Is reaches its code.
func (e *NameError) Unwrap() error { return e.Err }

// The refusals. The specification names no sentinel for these, and errors.Is matches by
// code, so a caller compares mq.CodeOf(err) against mq.CodeInvalidArgument.
var (
	// ErrNoBroker is a name set asked for without saying which ActiveMQ.
	ErrNoBroker = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: the broker is not Artemis or Classic, and nothing on the wire says which one was meant")
	// ErrNoTopic is an empty topic.
	ErrNoTopic = errcode.New(errcode.InvalidArgument, "iqmq/activemq: the topic is empty")
	// ErrNoSubscription is an empty subscription where one is needed.
	ErrNoSubscription = errcode.New(errcode.InvalidArgument, "iqmq/activemq: the subscription is empty")
	// ErrTopicWildcard is a topic carrying wildcard characters.
	ErrTopicWildcard = errcode.New(errcode.Unsupported,
		"iqmq/activemq: a topic maps to its own address, so a wildcard over topic names is not a topic")
	// ErrSubscriptionWildcard is a subscription carrying wildcard characters.
	ErrSubscriptionWildcard = errcode.New(errcode.Unsupported,
		"iqmq/activemq: a subscription maps to its own queue, so a wildcard over subscription names is not a subscription")
	// ErrSubscriptionDot is a subscription containing ".".
	ErrSubscriptionDot = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: a subscription cannot contain \".\": Classic's consumer queue name would then be ambiguous between the subscription and the topic")
	// ErrNameEmpty is an empty derived name.
	ErrNameEmpty = errcode.New(errcode.InvalidArgument, "the name is empty")
	// ErrNameTooLong is a derived name over MaxNameLen bytes.
	ErrNameTooLong = errcode.New(errcode.InvalidArgument, "the name is longer than 255 bytes")
	// ErrNameFQQN is a name containing Artemis's "::".
	ErrNameFQQN = errcode.New(errcode.InvalidArgument,
		"the name contains \"::\", which is Artemis's fully qualified queue name separator")
	// ErrNameSlash is a name containing "/".
	ErrNameSlash = errcode.New(errcode.InvalidArgument,
		"the name contains \"/\", which is part of Classic's scheme and path destination forms")
	// ErrNameComma is a name containing ",".
	ErrNameComma = errcode.New(errcode.InvalidArgument,
		"the name contains \",\", which separates the parts of a Classic composite destination")
	// ErrNameControl is a name containing a NUL, CR or LF.
	ErrNameControl = errcode.New(errcode.InvalidArgument,
		"the name contains a control character, which terminates a STOMP frame or header")
	// ErrNameSpace is a name containing a space or a tab.
	ErrNameSpace = errcode.New(errcode.InvalidArgument, "the name contains whitespace")
	// ErrNameColon is a name containing ":".
	ErrNameColon = errcode.New(errcode.InvalidArgument,
		"the name contains \":\", which separates a STOMP header's name from its value")
)

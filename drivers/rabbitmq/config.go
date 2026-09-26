package rabbitmq

import (
	"crypto/tls"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/backoff"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/rabbit"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// DriverName is this driver's registered name.
const DriverName = "rabbitmq"

// Version is this driver module's version, reported in [mq.DriverInfo]. It is
// a constant rather than a build-time stamp so that `go test` and a release
// binary report the same thing.
const Version = "0.1.0"

// The client library this driver wraps, reported in [mq.DriverInfo] and in the
// conformance report.
const (
	// ClientModule is the client library's module path.
	ClientModule = "github.com/rabbitmq/amqp091-go"
	// ClientVersion is the version this module pins. It is repeated from
	// go.mod rather than read from it, because a driver that reported a
	// version it could not have been built against would be worse than one
	// that reported nothing; TestClientVersionMatchesGoMod holds the two
	// together.
	ClientVersion = "v1.11.0"
)

// QueueType is the queue implementation a subscription's queue is declared
// with.
type QueueType uint8

// The queue types this driver declares.
const (
	// QueueQuorum is a quorum queue, the default and the choice. It
	// replicates to a majority before a publisher confirm, and it is the
	// only type that carries `x-delivery-count`, which is Attempt.
	QueueQuorum QueueType = iota
	// QueueClassic is a classic queue. It is cheaper and has no
	// `x-delivery-count`, so a driver on one cannot report an attempt count
	// and the capability table says so.
	QueueClassic

	queueTypeCount
)

var queueTypeNames = [queueTypeCount]string{"quorum", "classic"}

// String returns the type's own `x-queue-type` argument value.
func (q QueueType) String() string {
	if q < queueTypeCount {
		return queueTypeNames[q]
	}
	return "QueueType(?)"
}

// Valid reports whether q names a queue type.
func (q QueueType) Valid() bool { return q < queueTypeCount }

// The defaults a zero [Config] resolves to. Each is a decision and is
// commented where it is not obvious.
const (
	// DefaultExchangeType is the exchange kind a topic maps to. `topic`
	// rather than `direct` because a topic name is dot-separated by
	// convention in every broker iqmq supports, and a topic exchange is the
	// only kind on which a wildcard subscription is expressible at all.
	DefaultExchangeType = "topic"
	// DefaultPort is the port an endpoint that names none is dialled on.
	DefaultPort = "5672"
	// DefaultTLSPort is the same for an `amqps` endpoint.
	DefaultTLSPort = "5671"
	// DefaultHeartbeat is the AMQP heartbeat interval. Ten seconds is the
	// broker's own default and is what makes a dead peer detectable in
	// bounded time: without heartbeats a connection whose peer vanished
	// stays open until the operating system's own keepalive notices, which
	// can be hours.
	DefaultHeartbeat = 10 * time.Second
	// DefaultDialTimeout bounds one dial attempt.
	DefaultDialTimeout = 15 * time.Second
	// DefaultPublishTimeout bounds a publication's wait for its confirm and
	// a settlement's round trip when the caller's context has no deadline.
	// A publication that waited for ever on a live and silent link would be
	// a goroutine nothing can reclaim.
	DefaultPublishTimeout = 30 * time.Second
	// DefaultReconnectBase is the first delay after a lost connection.
	DefaultReconnectBase = 100 * time.Millisecond
	// DefaultReconnectMax caps it.
	DefaultReconnectMax = 30 * time.Second
	// DefaultBackoffPolicy is the dial loop's backoff: full jitter, which is
	// the policy that spreads a broker restart across a fleet.
	//
	// A caller who really wants no delay sets `BackoffPolicy: backoff.None`
	// and gets it; what they cannot get by accident is a busy dial loop.
	DefaultBackoffPolicy = backoff.Full
	// DefaultConsumerTagPrefix is prepended to the consumer tag each source
	// registers, so that `rabbitmqctl list_consumers` names the driver.
	DefaultConsumerTagPrefix = "iqmq"
)

// Config is everything this driver needs.
//
// The zero value is not usable — it names no endpoint — and every other field
// has a default that is documented above. Validate is pure and is called
// before any I/O, so a typo is an immediate refusal rather than a dial
// timeout.
//
// Credentials are [github.com/iqhive/iqmq/dsn.Secret], so neither
// [Config.String] nor a slog record can leak one: the redaction is that type's
// own and is proved there.
type Config struct {
	// Addrs are the broker endpoints, as `amqp://host:port` or
	// `amqps://host:port` URLs or as bare `host:port`. They are tried in
	// order and the first that answers is used, which is how a cluster's
	// several nodes are named.
	Addrs []string
	// Username and Password are the user credentials. RabbitMQ's default
	// user is guest/guest and is refused from anywhere but localhost, so a
	// configuration that names neither will fail against a real deployment.
	Username string
	Password dsn.Secret
	// Vhost is the virtual host. Empty means the URL's, or "/" when the URL
	// names none.
	Vhost string

	// TLS is the client TLS configuration. A nil TLS with an `amqps` URL
	// means the platform roots and the server name from the address.
	TLS *tls.Config
	// InsecureSkipVerify records that the operator asked not to verify the
	// server's certificate. It is refused without TLS: a configuration that
	// dialled in the clear after asking to skip verification reads as "less
	// safe" and is "no TLS at all".
	InsecureSkipVerify bool

	// Prefix is prepended to every exchange and queue name, so that two
	// deployments can share one virtual host. It is empty by default: a
	// driver that prefixed by default would rename the topology of anybody
	// who set nothing.
	Prefix string
	// ExchangeType is the kind a topic's exchange is declared with. Empty
	// means DefaultExchangeType.
	ExchangeType string
	// QueueType is the implementation a subscription's queue is declared
	// with. The zero value is QueueQuorum.
	QueueType QueueType
	// Declare lets this driver create the exchanges, queues and bindings it
	// needs. It is OFF by default, which is the rule: see the package
	// comment for why a driver that creates topology behind a caller's back
	// turns a typo into a permanent empty exchange.
	Declare bool
	// DeadLetter declares the `<queue>.dlx` exchange and `<queue>.dlq` queue
	// and points each subscription's queue at them. It needs Declare, and
	// without it a Nack asking for a dead-letter route is refused with
	// ErrUnsupported rather than dropping the message.
	DeadLetter bool
	// DeadLetterTopic sends every dead-lettered message to ANOTHER TOPIC
	// instead of to the derived `<queue>.dlq`, so that a consumer can read it
	// through the portable API.
	//
	// The derived queue is the right default — an operator finds it by name
	// from the queue it belongs to, and it is one queue per subscription so
	// two subscriptions of one topic do not pool their failures — but it is
	// not a TOPIC, so nothing can consume it through [mq.Client]. The specification's
	// dead-letter destination is a place a caller reads from, and this is how
	// a deployment says which place. It needs DeadLetter, and the named topic
	// has to exist: RabbitMQ drops a dead-lettered message whose
	// dead-letter exchange is not there, silently.
	DeadLetterTopic string
	// QueueMaxLength bounds a subscription's queue (`x-max-length`). Zero
	// leaves it unbounded, which is the broker's own default.
	QueueMaxLength int64
	// MaxPriority declares the queue with `x-max-priority`, which is what
	// makes Priority mean anything: a priority on a message to a
	// queue that was not declared with one is ignored by the broker. Zero
	// leaves priority unsupported, and the capability table says so.
	MaxPriority uint8

	// Mandatory sets the `mandatory` flag on every publication, so that a
	// message no queue is bound for comes back as `basic.return` and is
	// reported Rejected rather than silently dropped.
	//
	// It is OFF by default, and the default is the interesting decision. With
	// it on, a publication to a topic nobody has subscribed to FAILS, and
	// that is a stricter contract than the portable one: the specification makes a
	// publication's confirmation a statement about the BROKER taking
	// responsibility, and iqmq's own conformance kit publishes to topics with
	// no subscription and requires the publication to succeed. A driver whose
	// default refused it would be a driver on which a publisher has to know
	// the subscription topology, which no other driver in this repository
	// asks of a caller.
	//
	// It is worth turning on all the same, and the driver page says when: the
	// broker's behaviour without it is to discard an unroutable message and
	// ack it, so a publisher whose exchange has lost its binding succeeds for
	// ever while nothing arrives — the most common way a RabbitMQ deployment
	// loses messages. A deployment that publishes only to topics it has
	// provisioned should set it, and a library that does not know its
	// caller's topology should not.
	//
	// It is also what makes the specification's "Rejected" outcome reachable at all: a
	// `basic.return` is the only frame that says "the broker took this
	// message and could not route it", and without `mandatory` the broker
	// sends none.
	Mandatory bool

	// KeyAsRoutingKey publishes with the message key as the routing key
	// rather than the topic (WithKeyAsRoutingKey), which is what
	// makes ordering by key expressible: the broker reads a routing key and
	// does not read a header. A subscription of the whole topic then binds
	// with "#".
	KeyAsRoutingKey bool

	// ConfirmKind is the publisher-confirm strategy.
	// The zero value is Auto, which resolves to the recorded winner.
	ConfirmKind confirm.Kind
	// ConfirmWindow is how many publications may be outstanding under the
	// windowed and asynchronous strategies. Zero means the family's default.
	ConfirmWindow int

	// DelayKind is the delayed-redelivery strategy. The
	// zero value is Auto, which resolves to the arm that needs no plugin;
	// with [Config.DetectDelayPlugin] the driver upgrades to the plugin when
	// it finds it.
	DelayKind delay.Kind
	// DelayLadder is the bucket ladder the TTL strategy rounds a delay up
	// to. Empty means the family's default. A deployment whose delays are
	// known exactly should name them: the rounding error then becomes zero
	// and the queue count becomes the number of delays really used.
	DelayLadder []time.Duration
	// DelayQueueTTL is `x-expires` on a delay queue, so that a ladder a
	// deployment has stopped using does not leave empty queues behind. Zero
	// leaves them permanent, which is the safe default: a queue that expired
	// while a message was on its way to it would lose the message.
	DelayQueueTTL time.Duration
	// DetectDelayPlugin probes for the delayed-message exchange at open and
	// uses it when it is there. It needs Declare, because the probe is a
	// declaration.
	DetectDelayPlugin bool

	// Heartbeat, DialTimeout, ChannelMax and FrameSize are the connection
	// tuning parameters. Zero means the documented default.
	Heartbeat   time.Duration
	DialTimeout time.Duration
	ChannelMax  uint16
	FrameSize   int
	// ConnectionName is the `connection_name` this client advertises, which
	// is what `rabbitmqctl list_connections` shows. A deployment that sets
	// it can find one application's connections among a hundred.
	ConnectionName string
	// PublishTimeout bounds a publication's wait for its confirm and a
	// settlement's round trip when the caller's context has no deadline.
	PublishTimeout time.Duration

	// ReconnectBase, ReconnectMax, ReconnectGrowth and BackoffPolicy
	// configure the driver's own dial loop.
	//
	// MaxReconnects bounds ONE OUTAGE: the count starts again at zero every
	// time the connection is lost, so a client that has survived twenty
	// outages has the whole budget for the twenty-first. That is the reading
	// a caller who writes `MaxReconnects: 100` means, it is what
	// [driverkit.ReconnectConfig.MaxAttempts] has always done, and it is
	// spelled out here because for a while the helper's own documentation
	// claimed the other one. Zero means "until the client
	// is closed", which is what a long-lived client wants.
	ReconnectBase   time.Duration
	ReconnectMax    time.Duration
	ReconnectGrowth float64
	BackoffPolicy   backoff.Kind
	MaxReconnects   int

	// ConsumerTagPrefix is prepended to each source's consumer tag. Empty
	// means DefaultConsumerTagPrefix.
	ConsumerTagPrefix string
	// AppID is the `app-id` property every publication carries, which
	// RabbitMQ's management view shows and which is how one application's
	// publications are told from another's.
	AppID string
	// PublishType is the `type` property every publication carries. It is
	// the AMQP 0-9-1 field applications most often use for a schema or event
	// name, and iqmq's own message model has no field for it.
	PublishType string
	// ExclusiveConsumer asks the broker for an exclusive consumer: no other
	// consumer may attach to the queue while this one is there, and a second
	// attempt is refused with 403 ACCESS_REFUSED.
	//
	// It is how a caller gets the single-consumer ordering from this
	// broker, and it is off by default because a subscription's whole
	// meaning is competing consumers.
	ExclusiveConsumer bool
	// ConsumeArgs are extra `basic.consume` arguments, for a broker feature
	// this driver does not wrap -- `x-priority` for a consumer priority, for
	// one.
	//
	// They are NOT queue arguments: a queue's own arguments are fixed at
	// declaration, and two consumers of one subscription that declared
	// different ones would raise 406 PRECONDITION_FAILED against each other.
	ConsumeArgs map[string]any

	// These four are per-CLIENT and not per-publisher, and that is a gap
	// rather than a choice. The specification asks for typed `PublisherOptions` and
	// `ConsumerOptions` "carried in PublisherConfig.Driver /
	// ConsumerConfig.Driver", and neither field exists: the SPI's SinkConfig
	// and SourceConfig carry the topic, the confirmation mode, the metadata
	// mode, the prefetch, the start position and the acknowledgement
	// deadline, and nothing else. So there is no channel through which a
	// per-publisher option can reach a driver at all, and the landed
	// drivers -- kafka, sqs, jetstream, redis -- all put these settings in
	// their own Config for the same reason. It matters most for variant
	// the expected answer is "windowed/async for batch,
	// per-message for `Publish`", which is a PER-PUBLISHER choice, and a
	// caller who wants both has to open two clients. Recorded in
	// the maintenance notes.

	// Clock is the time source. Nil means the wall clock. It is injectable
	// because every timing property this driver has — a delay's accuracy, a
	// reconnection's backoff, a publication's timeout — is otherwise only
	// testable by waiting.
	Clock mq.Clock
	// Hooks are the observability callbacks.
	//
	// They are here, in the driver's own configuration, and not read from
	// the client: a driver cannot reach its client's hooks, which
	// the maintenance notes records as the reason `Hooks.OnReconnect` is
	// otherwise unfireable. Every driver in this repository takes them the
	// same way.
	Hooks mq.Hooks
}

// Validate checks the configuration without any I/O.
//
// Everything it can refuse, it refuses here: a caller whose topic prefix is
// reserved, whose ladder is not increasing or whose confirm window is absurd
// finds out at construction rather than three frames into a channel that is
// about to raise an exception.
func (c *Config) Validate() error {
	switch {
	case len(c.Addrs) == 0:
		return errNoAddrs
	case c.Password != "" && c.Username == "":
		return errPasswordWithoutUser
	case c.InsecureSkipVerify && c.TLS == nil:
		return errInsecureWithoutTLS
	case !c.QueueType.Valid():
		return errQueueType
	case c.DeadLetter && !c.Declare:
		return errDeadLetterWithoutDeclare
	case c.DeadLetterTopic != "" && !c.DeadLetter:
		return errDeadLetterTopicWithoutRoute
	case c.DetectDelayPlugin && !c.Declare:
		return errDetectWithoutDeclare
	case c.Heartbeat < 0 || c.DialTimeout < 0 || c.PublishTimeout < 0:
		return errNegativeTimeout
	case c.ReconnectBase < 0 || c.ReconnectMax < 0 || c.MaxReconnects < 0:
		return errReconnectBounds
	case c.QueueMaxLength < 0:
		return errQueueMaxLength
	case c.FrameSize < 0:
		// A negative frame size names no setting: it reaches amqp091-go's
		// tune negotiation as the maximum frame the connection may carry.
		// `framesize=-1` used to parse and then vanish from the rendering,
		// because the renderer writes a count only when it is positive.
		return errFrameSize
	case !c.BackoffPolicy.Valid():
		return errBackoffPolicy
	}
	// A *tls.Config beside an address that explicitly says "amqp://" asks for
	// two different things, and the library grants the address's: it dials in
	// the CLEAR, and this driver's dial URL carries the credential, so the
	// password goes on that wire. Measured against amqp091-go v1.14.0 on a
	// socket: `amqp.DialConfig("amqp://u:s3cret@host/", Config{TLSClientConfig:
	// …})` writes 41 4d 51 50 ("AMQP"), the cleartext protocol header, while
	// the same call on "amqps://" writes a TLS ClientHello. Refusing is the
	// same shape as the InsecureSkipVerify check below.
	for _, a := range c.Addrs {
		if c.TLS != nil && strings.HasPrefix(a, "amqp://") {
			return errTLSWithPlaintextAddr
		}
	}
	// A list whose addresses disagree about TLS has no DSN spelling -- the
	// grammar carries one `tls=` for the whole list -- so a rendering of it
	// necessarily describes one endpoint wrongly, and the direction it got
	// wrong was the unsafe one.
	if len(c.Addrs) > 1 {
		first := addrUsesTLS(c.Addrs[0], c.TLS != nil)
		for _, a := range c.Addrs[1:] {
			if addrUsesTLS(a, c.TLS != nil) != first {
				return errMixedTransport
			}
		}
	}
	for _, addr := range c.Addrs {
		if strings.TrimSpace(addr) == "" {
			return errNoAddrs
		}
		// A credential in an address cannot be rendered: a DSN carries ONE
		// credential, for every endpoint, so `amqp://u:s3cret@h:5672` in
		// Addrs reached Config.DSN, Config.String and Config.LogValue as the
		// bytes of the password (the specification forbids a credential in anything iqmq
		// renders or logs) and had no DSN spelling to come back from. The
		// documented forms are `amqp://host:port`, `amqps://host:port` and a
		// bare `host:port`; the credential goes in Username and Password.
		rest := addr
		if i := strings.Index(rest, "://"); i >= 0 {
			rest = rest[i+3:]
		}
		if strings.ContainsRune(rest, '@') {
			return errAddrCredential
		}
		// The typed twin of the DSN's dial guard. `dialableHost` ran only
		// inside ParseDSN, so a typed Config carried a host straight past it
		// -- the same redirect the DSN refuses, reachable by writing the
		// struct instead of the string. The test is on
		// the HOST, after the scheme, the credential and the port are
		// removed, because those are separate positions with their own rules.
		if host, _ := splitAddr(rest); !dialableHost(host) {
			return errDSNHost
		}
	}
	if c.Prefix != "" {
		// The prefix reaches the broker as part of every name, so a
		// reserved or unencodable prefix is a channel exception waiting to
		// happen on the first declaration. `internal/rabbit` refuses it on
		// a name it would build.
		if _, err := rabbit.Derive(rabbit.Config{Prefix: c.Prefix, Topic: "probe"}); err != nil {
			return err
		}
	}
	if kind := c.ExchangeType; kind != "" && !validExchangeType(kind) {
		return errExchangeType
	}
	if c.DeadLetterTopic != "" {
		// The dead-letter topic reaches the broker as an exchange name on
		// every subscription queue's arguments, so it is checked here for
		// the reason the prefix is: a queue argument the broker refuses is
		// a channel exception on the first declaration.
		if _, err := rabbit.Derive(rabbit.Config{Prefix: c.Prefix, Topic: c.DeadLetterTopic}); err != nil {
			return err
		}
	}
	if err := (confirm.Config{Window: c.ConfirmWindow}).Validate(c.ConfirmKind); err != nil {
		return err
	}
	return (delay.Config{Ladder: c.DelayLadder, QueueTTL: c.DelayQueueTTL}).Validate(c.DelayKind)
}

// validExchangeType reports whether the name is an exchange kind RabbitMQ has.
//
// The list is closed rather than open, and that is a judgement: a plugin can
// add an exchange type, and this driver would refuse a caller who installed
// one. The alternative is worse — a typo such as "topics" becomes a channel
// exception on the first declaration, which closes the channel and, on a
// confirm channel, takes every publication in flight with it. A deployment
// with a plugin exchange type can reach it through [Config.ExchangeType] being
// widened deliberately, which is a one-line change with a test, rather than
// through every typo being accepted.
func validExchangeType(kind string) bool {
	switch kind {
	case "direct", "topic", "fanout", "headers", ExchangeDelayed:
		return true
	default:
		return false
	}
}

// ExchangeDelayed is the exchange type the delayed-message plugin adds.
const ExchangeDelayed = "x-delayed-message"

// resolve fills in the defaults, so that every reader of the configuration
// sees one answer rather than repeating the `if zero` in nine places.
func (c Config) resolve() Config {
	if c.ExchangeType == "" {
		c.ExchangeType = DefaultExchangeType
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = DefaultHeartbeat
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.ReconnectBase == 0 {
		c.ReconnectBase = DefaultReconnectBase
	}
	if c.ReconnectMax == 0 {
		c.ReconnectMax = DefaultReconnectMax
	}
	if c.BackoffPolicy == backoff.Auto {
		c.BackoffPolicy = DefaultBackoffPolicy
	}
	if c.ConsumerTagPrefix == "" {
		c.ConsumerTagPrefix = DefaultConsumerTagPrefix
	}
	return c
}

// usesTLS reports whether this configuration dials with TLS.
//
// It is not `c.TLS != nil`: an address may carry the TLS choice in its own
// scheme ("amqps://"), which [Config.url] keeps and dials on, while
// [renderAddr] strips the scheme from the DSN -- so a TLS configuration
// rendered a PLAINTEXT DSN that parsed back to a cleartext dial. The
// rendering asks this instead, so the two halves cannot disagree.
//
// Every address must be TLS for the answer to be yes: a list with one
// "amqps://" and one plain endpoint has no single answer, and saying "yes"
// would claim protection the plain endpoint does not have.
func (c Config) usesTLS() bool {
	if len(c.Addrs) == 0 {
		return c.TLS != nil
	}
	for _, a := range c.Addrs {
		if !addrUsesTLS(a, c.TLS != nil) {
			return false
		}
	}
	return true
}

// addrUsesTLS reports whether [Config.url] will build an "amqps" URL for this
// one address, which is what amqp091-go dials on.
//
// The SCHEME wins when the address states one, because `Config.url` keeps it
// (config.go's `scheme, rest = addr[:i], addr[i+3:]`) and only falls back to
// `c.TLS != nil` when there is none. Reading `TLS != nil` as "TLS is on"
// whatever the address said made the rendered `tls=` disagree with the dial in
// exactly the direction that matters: a list with one "amqps://" endpoint and
// one plain one rendered a PLAINTEXT DSN, so the round trip turned the
// protected endpoint into a cleartext dial.
func addrUsesTLS(a string, haveTLSConfig bool) bool {
	if strings.HasPrefix(a, "amqps://") {
		return true
	}
	if strings.HasPrefix(a, "amqp://") {
		return false
	}
	return haveTLSConfig
}

// urls renders the endpoints as AMQP URLs with the credentials applied.
//
// It is what the dialler is given, and it is the one place a credential is
// interpolated into a string. The string never leaves this driver: it goes to
// the client library and nowhere else, and [Config.String] and [Config.DSN]
// render the redacted form instead.
func (c Config) urls() []string {
	out := make([]string, 0, len(c.Addrs))
	for _, addr := range c.Addrs {
		out = append(out, c.url(addr, false))
	}
	return out
}

// url builds one endpoint's URL. With redact set, the password is replaced by
// the redaction marker, which is the form every rendered string uses.
func (c Config) url(addr string, redact bool) string {
	scheme, rest := "amqp", addr
	if i := strings.Index(addr, "://"); i >= 0 {
		scheme, rest = addr[:i], addr[i+3:]
	} else if c.TLS != nil {
		scheme = "amqps"
	}
	// The path of an AMQP URL is the virtual host, and it is percent-encoded:
	// the default vhost is "/", which has to travel as "%2f" or the URL
	// parses as "no vhost at all".
	host, vhost := rest, ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		host, vhost = rest[:i], rest[i+1:]
	}
	if c.Vhost != "" {
		vhost = escapeVhost(c.Vhost)
	}
	if !strings.Contains(host, ":") {
		if scheme == "amqps" {
			host += ":" + DefaultTLSPort
		} else {
			host += ":" + DefaultPort
		}
	}
	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	if c.Username != "" {
		// The userinfo is escaped by net/url, which is the exact inverse of
		// how amqp091-go reads it back (url.Parse, then u.User.Username() and
		// u.User.Password(); uri.go:88,118), so the escaping is lossless.
		//
		// Writing it raw was the host refusal's twin one component to the
		// left: net/url ends the authority at the FIRST literal "/", "?" or
		// "#", so a user the shared parser decoded from "%2F" truncated it.
		// Measured against amqp091-go v1.14.0's own ParseURI,
		// "rabbitmq://u%2Fv:p@good-host:5672/prod" built
		// "amqp://u/v:p@good-host:5672/prod", which reads as host "u" with
		// amqp091's DEFAULT guest/guest credentials and the vhost
		// "v:p@good-host:5672/prod" -- the dial went to a host the DSN does
		// not name AND the configured password travelled to it inside the
		// Connection.Open frame, which the specification forbids. "u%3Fv" and "u%23v"
		// dialled "u" too.
		//
		// The password position failed the other way: a "/" in it made
		// ParseURI refuse the whole URL ("invalid port"), so a base64
		// password -- which routinely holds one -- could not dial at all.
		// Escaping fixes that rather than refusing it, because both
		// positions are free text as far as AMQP is concerned; refusing
		// bytes in Username alone would have left the password broken.
		if c.Password != "" {
			pass := c.Password.Reveal()
			if redact {
				pass = dsn.Redacted
			}
			b.WriteString(url.UserPassword(c.Username, pass).String())
		} else {
			b.WriteString(url.User(c.Username).String())
		}
		b.WriteByte('@')
	}
	b.WriteString(host)
	b.WriteByte('/')
	b.WriteString(vhost)
	return b.String()
}

// escapeVhost percent-encodes a virtual host for the path of an AMQP URL, and
// for the path of this driver's own DSN, which the same rules cover.
//
// "/" is the default virtual host's whole name and is also the path separator,
// so it must be encoded; so must "%" or the encoding is ambiguous. "?" ends
// the path and opens a query in both grammars, and "#" opens a fragment in an
// AMQP URL and is refused ANYWHERE in a DSN, so `vhost=a?b` and `vhost=a#b`
// rendered a DSN that did not parse at all. Nothing else
// is touched, because a name the broker accepts should read as itself in a log
// line.
func escapeVhost(v string) string {
	if !strings.ContainsAny(v, "/%?#") && !hasURLUnsafeByte(v) {
		return v
	}
	var b strings.Builder
	b.Grow(len(v) + 4)
	const hex = "0123456789abcdef"
	for i := range len(v) {
		switch ch := v[i]; {
		case ch == '/' || ch == '%' || ch == '?' || ch == '#',
			// A space, a control byte or anything above ASCII makes
			// amqp091-go refuse the whole URI rather than dial it:
			// measured, Vhost "a b" gives "URI must not contain
			// whitespace" and a TAB or a DEL gives "net/url: invalid
			// control character in URL". That is the "could not dial at
			// all" class the specification fixed one component to the
			// left, and it is fixed the same way -- by escaping, which
			// amqp091 decodes back, rather than by refusing a vhost name
			// RabbitMQ itself allows.
			ch <= ' ', ch >= 0x7f:
			b.WriteByte('%')
			b.WriteByte(hex[ch>>4])
			b.WriteByte(hex[ch&0xf])
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// String renders the configuration with every credential redacted (
// ).
func (c Config) String() string {
	var b strings.Builder
	b.WriteString("rabbitmq{addrs=[")
	for i, addr := range c.Addrs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(c.url(addr, true))
	}
	b.WriteString("] vhost=")
	b.WriteString(c.Vhost)
	b.WriteString(" prefix=")
	b.WriteString(c.Prefix)
	b.WriteString(" exchange=")
	b.WriteString(c.ExchangeType)
	b.WriteString(" queue=")
	b.WriteString(c.QueueType.String())
	b.WriteString(" declare=")
	b.WriteString(strconv.FormatBool(c.Declare))
	b.WriteString(" deadletter=")
	b.WriteString(strconv.FormatBool(c.DeadLetter))
	b.WriteString(" mandatory=")
	b.WriteString(strconv.FormatBool(c.Mandatory))
	b.WriteString(" confirm=")
	b.WriteString(c.ConfirmKind.String())
	b.WriteString(" delay=")
	b.WriteString(c.DelayKind.String())
	b.WriteString(" tls=")
	b.WriteString(strconv.FormatBool(c.TLS != nil))
	b.WriteByte('}')
	return b.String()
}

// LogValue is the slog form, with the credentials redacted for the same reason
// [Config.String] redacts them: the specification forbids a credential in anything iqmq
// logs, and a structured logger reaches the fields directly rather than
// through String.
func (c Config) LogValue() slog.Value {
	addrs := make([]string, 0, len(c.Addrs))
	for _, addr := range c.Addrs {
		addrs = append(addrs, c.url(addr, true))
	}
	return slog.GroupValue(
		slog.Any("addrs", addrs),
		slog.String("vhost", c.Vhost),
		slog.String("prefix", c.Prefix),
		slog.String("exchange_type", c.ExchangeType),
		slog.String("queue_type", c.QueueType.String()),
		slog.Bool("declare", c.Declare),
		slog.Bool("dead_letter", c.DeadLetter),
		slog.Bool("mandatory", c.Mandatory),
		slog.String("confirm", c.ConfirmKind.String()),
		slog.String("delay", c.DelayKind.String()),
		slog.Bool("tls", c.TLS != nil),
	)
}

// The configuration's refusals. Every one of them is CodeInvalidConfig: the
// caller's configuration cannot be used, which the specification distinguishes from a
// malformed argument on a call.
var (
	errNoAddrs = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: at least one broker endpoint is required")
	errPasswordWithoutUser = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a password needs a user name")
	errInsecureWithoutTLS = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: InsecureSkipVerify without TLS would dial in the clear, which is not \"less safe\" but \"not safe at all\"")
	errQueueType = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the queue type is not one this driver declares")
	errExchangeType = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the exchange type is not one RabbitMQ has; a typo becomes a channel exception on the first declaration, which takes every in-flight publisher confirm with it")
	errDeadLetterWithoutDeclare = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: DeadLetter needs Declare: the dead-letter exchange and queue have to be created, and the API forbids creating them behind the caller's back")
	errDeadLetterTopicWithoutRoute = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: DeadLetterTopic needs DeadLetter: naming a destination without turning the route on would leave a rejected message destroyed rather than routed")
	errDetectWithoutDeclare = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: DetectDelayPlugin needs Declare: the probe for the delayed-message exchange IS a declaration")
	errNegativeTimeout = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a timeout cannot be negative")
	errReconnectBounds = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the reconnection bounds cannot be negative")
	errQueueMaxLength = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the queue length bound cannot be negative")
	errBackoffPolicy = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the backoff policy is not a known kind")
	errFrameSize = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the frame size cannot be negative")
	errTLSWithPlaintextAddr = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a TLS config is set beside an address whose scheme is \"amqp://\"; amqp091-go picks the transport from the scheme and would dial in the clear, which puts the password in the dial URL on an unprotected wire -- write \"amqps://\" or drop the TLS config")
	errMixedTransport = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the addresses disagree about TLS; a DSN carries one \"tls=\" for the whole list, so such a list has no rendering and one endpoint would be described wrongly")
	errAddrCredential = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: an address carries a credential; Addrs is \"amqp[s]://host:port\" or \"host:port\" and the user name and password go in Username and Password")
)

// hasURLUnsafeByte reports whether s holds a byte net/url refuses inside a
// URL: a space, a control byte, or anything above ASCII.
func hasURLUnsafeByte(s string) bool {
	for i := range len(s) {
		if ch := s[i]; ch <= ' ' || ch >= 0x7f {
			return true
		}
	}
	return false
}

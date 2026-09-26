package amqp10

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/rabbit"
)

// DriverName is the driver name this profile reports.
//
// It is the SAME name the AMQP 0-9-1 driver reports, and that is deliberate:
// one broker, one driver name, two profiles. What tells the two apart in an
// error, in [github.com/iqhive/iqmq.DriverInfo] and in the generated
// capability matrix is [ProfileName], which is the field the specification has for exactly
// this. A second driver name would make the matrix read as though iqmq
// supported two brokers called RabbitMQ.
const DriverName = "rabbitmq"

// ProfileName is the broker variant reported in
// [github.com/iqhive/iqmq.DriverInfo.Profile]. The AMQP 0-9-1 driver reports
// "amqp091"; a report, a matrix row and an error can therefore be read against
// the protocol that produced it.
const ProfileName = "amqp10"

// Version is this module's own version, reported in
// [github.com/iqhive/iqmq.DriverInfo.DriverVersion]. It is a constant rather
// than a build-info lookup so that `go test` and a release binary report the
// same thing.
const Version = "0.1.0"

// The defaults a zero [Config] resolves to. Each is stated here rather than
// left to the core, because a RabbitMQ deployment reads these numbers off this
// page and not off the shared core's.
const (
	// DefaultExchangeType is the exchange kind a topic's exchange is assumed
	// to have been declared with. `topic` rather than `direct` because a
	// topic name is dot-separated by convention in every broker iqmq
	// supports, and it is the only kind on which a wildcard BINDING is
	// expressible — which this profile cannot create, but whoever declared
	// the topology may have.
	DefaultExchangeType = "topic"
	// DefaultCredit is the link credit one source holds when the consumer's
	// own configuration names no prefetch. It is the AMQP 1.0 replacement
	// for `basic.qos`: credit is what bounds the deliveries a consumer holds
	// unsettled, because AMQP 1.0 has no lease of its own.
	DefaultCredit = 100
	// VhostHostnamePrefix is what RabbitMQ 4 reads a virtual host from.
	//
	// AMQP 1.0 has no vhost field. RabbitMQ 4 therefore selects the virtual
	// host from the OPEN frame's `hostname`, as `vhost:<name>`, which is the
	// one place this driver's configuration does not resemble the AMQP 0-9-1
	// driver's: there the vhost is the URL's path. It is a documented
	// RabbitMQ mechanism that nothing in this repository can verify without
	// a broker, so [Config.Vhost] states it and
	// docs/drivers/rabbitmq-amqp10.md records it as unverified.
	VhostHostnamePrefix = "vhost:"
)

// QueueType is the queue implementation a subscription's queue was declared
// with.
//
// This profile DECLARES nothing (see [Config.Declare]), so the field is an
// assertion by whoever provisioned the topology rather than an instruction.
// It is still load-bearing: two capability claims move with it — the delivery
// attempt count and the durable confirmation boundary — and the specification makes a claim
// that depends on a fact have to state the fact.
type QueueType uint8

// The queue types this profile has claims for.
const (
	// QueueQuorum is a quorum queue, which is the choice for both
	// RabbitMQ profiles. It replicates to a majority before the broker
	// answers a transfer with `accepted`, and it is the queue type whose
	// redelivery count reaches an AMQP 1.0 consumer.
	QueueQuorum QueueType = iota
	// QueueClassic is a classic queue. Whether a classic queue's redeliveries
	// reach the AMQP 1.0 header's `delivery-count` is not something this
	// repository can check without a broker, so the attempt claim is
	// withdrawn on one rather than guessed at.
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

// Config is everything this profile needs.
//
// It is RabbitMQ's vocabulary and not the protocol's: a virtual host, a
// topology prefix, an exchange type, a queue type, a dead-letter route. The
// protocol's own knobs — the SASL mechanism, the receiver settle mode, the
// backoff policy, the link options — are the shared core's types rather than
// copies of them, because a translation table between two identical
// enumerations is a table that can drift and the half that drifted would be
// this one.
//
// [Config.CoreConfig] and [Config.Profile] are the conversion, and they are
// this module's real API: everything else here is the surface a caller sets.
//
// Credentials are [github.com/iqhive/iqmq/dsn.Secret], so neither
// [Config.String] nor an slog record nor an error built from one can carry one
type Config struct {
	// Addrs are the broker endpoints, as `amqp://host:port` or
	// `amqps://host:port` URLs or as bare `host:port`. They are tried in
	// order and re-tried from the first, so a list is a FAILOVER set rather
	// than a load-balanced one: AMQP 1.0 is one long-lived connection and a
	// client that spread its links over two nodes would have half its
	// deliveries die with each.
	Addrs []string
	// Username and Password are the SASL credentials. RabbitMQ's default
	// user is guest/guest and is refused from anywhere but localhost, so a
	// configuration that names neither will fail against a real deployment.
	Username string
	Password dsn.Secret
	// Vhost is the virtual host, which reaches RabbitMQ as the OPEN frame's
	// `hostname` under [VhostHostnamePrefix]. Empty means the broker's
	// default.
	//
	// This is the sharpest configuration difference from the AMQP 0-9-1
	// driver, where the vhost is the connection URL's PATH. AMQP 1.0 has no
	// virtual-host field at all, so RabbitMQ 4 overloads `hostname`; a caller
	// who copied a `rabbitmq://host/acme` DSN across would otherwise silently
	// land in the default vhost.
	Vhost string
	// HostName overrides the OPEN frame's `hostname` outright, for a
	// deployment whose endpoint needs a particular one — a TLS SNI name, a
	// load balancer that routes on it. It is refused together with Vhost:
	// the two write the same field, and a configuration that set both would
	// have one of them silently win.
	HostName string
	// ContainerID is the AMQP container-id this client sends. RabbitMQ shows
	// it in its management view and in `rabbitmqctl list_connections`, so it
	// is this profile's equivalent of the AMQP 0-9-1 driver's
	// `connection_name`: a deployment that sets it can find its own
	// connections among a hundred. Empty means go-amqp generates one.
	ContainerID string
	// Properties are extra connection properties sent in the OPEN frame,
	// under this core's own identity.
	Properties map[string]any

	// SASL is the authentication mechanism. The zero value is
	// [github.com/iqhive/iqmq/drivers/amqp10.SASLAuto]: PLAIN when Username
	// is set and ANONYMOUS when it is not.
	//
	// RabbitMQ 4's AMQP 1.0 endpoint offers PLAIN, ANONYMOUS and EXTERNAL,
	// and maps ANONYMOUS onto its configured default user. A SASL layer is
	// what the broker expects; SASLNone exists for a peer that wants none —
	// a proxy, or this module's own scripted peer — and not for RabbitMQ.
	SASL core.SASLMechanism
	// InsecurePlainSASL allows SASL PLAIN without TLS. PLAIN sends the
	// password in the clear, so [Config.Validate] refuses the combination
	// unless the operator has said this.
	InsecurePlainSASL bool
	// TLS is the client TLS configuration. It supplies the PARAMETERS of a
	// TLS connection; it does not select one. go-amqp reads the transport
	// from an address's scheme, so a non-nil value turns TLS on for a bare
	// "host:port" address, is redundant beside `amqps://`, and beside an
	// explicit `amqp://` is refused by the amqp10 Validate this delegates to
	// -- that connection would be in the clear whatever this field holds.
	// This comment said the opposite until the bytes were measured.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. Validate
	// refuses it without TLS: a client that dialled in the clear after the
	// operator asked to skip verification reads as "less safe" and is "no
	// TLS at all".
	InsecureSkipVerify bool

	// Prefix is prepended to every exchange and queue name, so that two
	// deployments can share one virtual host. It is empty by default: a
	// driver that prefixed by default would rename the topology of anybody
	// who set nothing. It is `internal/rabbit`'s prefix and is therefore the
	// same names the AMQP 0-9-1 driver derives.
	Prefix string
	// ExchangeType is the kind the topic's exchange was declared with. Empty
	// means [DefaultExchangeType].
	//
	// This profile declares nothing, so the field is an assertion rather than
	// an instruction, and it is worth being precise about what it buys: it is
	// VALIDATED, so a typo such as "topics" is refused at construction rather
	// than recorded as a topology nobody has, and it is rendered in
	// [Config.String] and [Config.DSN] so that an operator reading a log line
	// knows which exchange kind the deployment was built on. It changes no
	// capability claim, because the two that would depend on it — a wildcard
	// subscription and ordering by key — are withdrawn for reasons that hold
	// whatever the exchange type is.
	ExchangeType string
	// QueueType is the implementation a subscription's queue was declared
	// with. The zero value is [QueueQuorum].
	QueueType QueueType
	// Declare would let this driver create the exchanges, queues and bindings
	// it needs, and it is REFUSED: RabbitMQ 4's AMQP 1.0 endpoint cannot
	// create a queue, an exchange or a binding, and attaching a link to an
	// address nothing provisioned closes the link with `amqp:not-found`
	// rather than creating anything.
	//
	// The field exists so that a caller who moves from `drivers/rabbitmq`
	// finds it and is told why, rather than not finding it and assuming the
	// topology appears. RabbitMQ 4 does have an AMQP 1.0 management
	// extension over a `/management` node; this profile does not implement it
	// and says so instead of claiming FeatureAdmin.
	Declare bool
	// DeadLetter records that the subscriptions' queues were declared with a
	// dead-letter exchange, which is what makes a `rejected` disposition
	// reach `<queue>.dlx` and `<queue>.dlq`.
	//
	// It is an assertion and not a declaration, for the reason Declare is
	// refused. Without it FeatureDeadLetter is not claimed and a Nack asking
	// for a dead-letter route is refused with ErrUnsupported rather than
	// destroying the message.
	DeadLetter bool
	// MaxPriority records that the subscriptions' queues were declared with
	// `x-max-priority`. Zero leaves FeaturePriority unclaimed, which is what
	// makes the core refuse a prioritised publication instead of letting the
	// broker accept the field and ignore it.
	MaxPriority uint8
	// KeyAsRoutingKey would publish with the message key as the routing key
	// rather than the topic, and it is REFUSED on this profile.
	//
	// An AMQP 1.0 sending link carries ONE address and the routing key is
	// part of it (`/exchanges/<exchange>/<routing-key>`), so the routing key
	// is fixed when the link attaches and cannot vary per message. Honouring
	// the option would need the anonymous-terminus extension — a sender
	// attached to a null target with each message's `to` property naming the
	// address — which the shared core does not implement: a Sink is bound to
	// one address for its lifetime. FeatureOrderedByKey is therefore not
	// claimed at all on this profile, where the AMQP 0-9-1 driver claims it
	// conditionally.
	KeyAsRoutingKey bool

	// Credit is the link credit ceiling one source may hold, whatever
	// prefetch the core resolved from the consumer's configuration. Zero
	// means [DefaultCredit].
	//
	// It is the field a caller reaches for where the AMQP 0-9-1 driver has a
	// confirm strategy AND a prefetch. There is no confirm strategy here:
	// every transfer on an unsettled link gets its own disposition, so
	// The specification does not apply and the only window there is is the
	// link's credit.
	Credit int
	// MaxPending bounds the asynchronous publications outstanding on one
	// sink. Zero means the core's default; it is never unlimited.
	MaxPending int
	// NonDurable publishes with the AMQP header's `durable` flag clear, so
	// that the broker is free to lose the message on a restart. It is
	// inverted so that the zero Config publishes DURABLE messages.
	NonDurable bool
	// ReceiverSettle is when a receiving link considers a disposition final.
	// The zero value is `first`, which costs no round trip per settlement;
	// `second` costs one and changes a failed settlement from a lost lease
	// into an unknown outcome.
	ReceiverSettle core.ReceiverSettleMode
	// SenderSettle is how a sending link settles its transfers. The zero
	// value is `mixed`, which is the only mode under which one sink can
	// serve both ConfirmNone and ConfirmAccepted.
	//
	// It is here rather than left to the core because the core's DSN parser
	// reads a `sendersettle` parameter: a field this configuration did not
	// carry would be a parameter accepted by the strict check and then
	// dropped, which is the one thing ADR-010 exists to prevent.
	SenderSettle core.SenderSettleMode

	// IdleTimeout, MaxFrameSize and MaxSessions are the connection's own
	// bounds. Zero means the core's documented default.
	IdleTimeout  time.Duration
	MaxFrameSize uint32
	MaxSessions  uint16

	// DialTimeout bounds one dial attempt, OperationTimeout one attach,
	// detach or disposition round trip whose caller has no deadline, and
	// WriteTimeout one frame write. Zero means the core's default.
	DialTimeout      time.Duration
	OperationTimeout time.Duration
	WriteTimeout     time.Duration

	// Backoff, ReconnectBase, ReconnectMax, ReconnectGrowth and
	// ReconnectAttempts configure the driver's own dial loop, which the specification
	// requires this driver to own. ReconnectAttempts of zero means "until the
	// client is closed", which is what a long-lived client wants.
	Backoff           core.BackoffPolicy
	ReconnectBase     time.Duration
	ReconnectMax      time.Duration
	ReconnectGrowth   float64
	ReconnectAttempts int
	// NoReconnect turns the dial loop off, so a lost connection is terminal.
	// It is for a caller that supervises its own connection through
	// [OpenWithConn].
	NoReconnect bool

	// Publisher and Consumer are the AMQP 1.0 link controls of every sink and
	// every source this client opens: a link name, link properties, offered
	// and desired capabilities, and a consumer's source filters. They are the
	// escape hatch this profile has where the AMQP 0-9-1 driver has
	// `ConsumeArgs`, and they are the core's own types for the reason the
	// enumerations above are.
	Publisher core.PublisherOptions
	Consumer  core.ConsumerOptions

	// Clock is the time source. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability callbacks.
	//
	// They are here, in the driver's own configuration, because a driver
	// cannot reach its client's hooks — which is what makes
	// `Hooks.OnReconnect` otherwise unfireable. Every driver in this
	// repository takes them the same way.
	Hooks mq.Hooks
}

// Validate reports whether the configuration can be used, without any I/O
//
// It is pure on purpose, and the two refusals worth knowing about are
// [Config.Declare] and [Config.KeyAsRoutingKey]: both are fields the AMQP
// 0-9-1 driver has and this protocol cannot honour, and both are refused HERE,
// with the reason, rather than accepted and ignored. The rule against
// silently dropping an unsupported option is what makes that the only honest
// answer.
func (c Config) Validate() error {
	if len(c.Addrs) == 0 {
		return errNoAddrs
	}
	for _, addr := range c.Addrs {
		if strings.TrimSpace(addr) == "" {
			return errNoAddrs
		}
	}
	switch {
	case c.Password != "" && c.Username == "":
		return errPasswordWithoutUser
	case c.InsecureSkipVerify && c.TLS == nil:
		return errInsecureWithoutTLS
	case !c.QueueType.Valid():
		return errQueueType
	case c.Declare:
		return errDeclareUnsupported
	case c.KeyAsRoutingKey:
		return errKeyAsRoutingKey
	case c.Vhost != "" && c.HostName != "":
		return errVhostAndHostName
	case c.Credit < 0 || c.MaxPending < 0 || c.ReconnectAttempts < 0:
		return errNegativeBound
	case c.IdleTimeout < 0 || c.DialTimeout < 0 || c.OperationTimeout < 0 ||
		c.ReconnectBase < 0 || c.ReconnectMax < 0:
		return errNegativeDuration
	}
	if kind := c.ExchangeType; kind != "" && !validExchangeType(kind) {
		return errExchangeType
	}
	if !printableText(c.Vhost) || !printableText(c.HostName) || !printableText(c.ContainerID) {
		return errBadText
	}
	if c.Prefix != "" {
		// The prefix reaches the broker as part of every address, so a
		// reserved or unencodable prefix is a link this driver could never
		// attach. `internal/rabbit` refuses it on a name it would build,
		// which is the same check the AMQP 0-9-1 driver makes.
		if _, err := rabbit.Derive(rabbit.Config{Prefix: c.Prefix, Topic: "probe"}); err != nil {
			return err
		}
	}
	// Everything the shared core owns is validated by the core, on the
	// configuration this one converts into: a second copy of those rules here
	// would be a second thing to keep in step.
	cc, err := c.CoreConfig()
	if err != nil {
		return err
	}
	return cc.Validate()
}

// validExchangeType reports whether the name is an exchange kind RabbitMQ has.
//
// The list is closed rather than open, which is the AMQP 0-9-1 driver's
// judgement kept: a typo such as "topics" would otherwise be recorded in the
// capability table as a topology this deployment does not have. The
// delayed-message plugin's type is accepted so that a caller can SAY the
// exchange is one, even though this profile has no verified way to publish a
// delayed message to it.
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

// printableText reports whether a configured string is safe to render.
//
// Everything Validate calls this on ends up in [Config.DSN], which ends up in
// a log line and in an error, so a control byte or an invalid UTF-8 sequence
// here produces output no log pipeline can carry. A non-ASCII virtual host is
// fine: what is refused is a control character, not a letter.
func printableText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// resolve fills in this module's own defaults, so that nothing below asks what
// a zero means.
func (c Config) resolve() Config {
	if c.ExchangeType == "" {
		c.ExchangeType = DefaultExchangeType
	}
	if c.Credit == 0 {
		c.Credit = DefaultCredit
	}
	return c
}

// CoreConfig is this configuration in the shared core's terms.
//
// It is half of this module's real API — [Config.Profile] is the other half —
// and three of the translations are the whole reason a typed RabbitMQ
// configuration exists rather than a `core.Config` with a profile beside it:
//
//   - the VIRTUAL HOST becomes the OPEN frame's `hostname` under
//     [VhostHostnamePrefix], because AMQP 1.0 has no virtual-host field and
//     RabbitMQ 4 overloads that one;
//   - an endpoint that names no scheme becomes an `amqp://` or `amqps://` URL
//     with the protocol's own default port, so that `host` and `host:5672`
//     and `amqp://host:5672` are one endpoint;
//   - CREDIT becomes the core's MaxCredit, which is the ceiling on what one
//     source may hold — the AMQP 1.0 answer to `basic.qos`, and the only
//     window this protocol has now that there is no confirm strategy to
//     choose.
func (c Config) CoreConfig() (core.Config, error) {
	r := c.resolve()
	addrs := make([]string, 0, len(r.Addrs))
	for _, addr := range r.Addrs {
		addrs = append(addrs, normaliseAddr(addr, r.TLS != nil))
	}
	host := r.HostName
	if r.Vhost != "" {
		host = VhostHostnamePrefix + r.Vhost
	}
	return core.Config{
		Addrs:              addrs,
		ContainerID:        r.ContainerID,
		HostName:           host,
		Properties:         r.Properties,
		SASL:               r.SASL,
		Username:           r.Username,
		Password:           r.Password,
		InsecurePlainSASL:  r.InsecurePlainSASL,
		TLS:                r.TLS,
		InsecureSkipVerify: r.InsecureSkipVerify,
		IdleTimeout:        r.IdleTimeout,
		MaxFrameSize:       r.MaxFrameSize,
		MaxSessions:        r.MaxSessions,
		ReceiverSettle:     r.ReceiverSettle,
		SenderSettle:       r.SenderSettle,
		MaxCredit:          r.Credit,
		NonDurable:         r.NonDurable,
		MaxPending:         r.MaxPending,
		DialTimeout:        r.DialTimeout,
		OperationTimeout:   r.OperationTimeout,
		WriteTimeout:       r.WriteTimeout,
		Backoff:            r.Backoff,
		ReconnectBase:      r.ReconnectBase,
		ReconnectMax:       r.ReconnectMax,
		ReconnectGrowth:    r.ReconnectGrowth,
		ReconnectAttempts:  r.ReconnectAttempts,
		NoReconnect:        r.NoReconnect,
		Publisher:          r.Publisher,
		Consumer:           r.Consumer,
		Clock:              r.Clock,
		Hooks:              r.Hooks,
	}, nil
}

// normaliseAddr turns one endpoint into an AMQP URL the core can dial.
//
// The scheme is added and not guessed at: an endpoint that already names one
// keeps it, so a list may mix a TLS node and a plaintext one and the core's own
// credential check still sees the difference.
func normaliseAddr(addr string, tlsConfigured bool) string {
	if strings.Contains(addr, "://") {
		return addr
	}
	scheme, port := "amqp://", core.DefaultPort
	if tlsConfigured {
		scheme, port = "amqps://", core.DefaultTLSPort
	}
	if _, p := splitHostPort(addr); p == "" {
		addr += ":" + port
	}
	return scheme + addr
}

// splitHostPort separates an endpoint's host from its port.
//
// It is the shared parser's own rule rather than a reading of its own, and that
// is load-bearing in both directions: [Config.DSN] escapes the host and leaves
// the port alone, so the two halves have to agree about where one ends and the
// other begins or a rendered configuration cannot be parsed back.
//
// The bracketed form is read first, because an IPv6 literal's own colons are
// not port separators. Outside brackets a colon is a separator only when what
// follows it is decimal digits, which is what keeps a host that merely contains
// a colon from being split at the wrong one. The first shape this got wrong was
// a host ending in "]" with no "[" — FuzzRabbitAMQP10DSN found
// `rABBitmq+Amqp10://]0` in three seconds, because the bracket test was a
// search for "]" rather than a prefix test, and the round trip then grew a
// second port on every rendering.
func splitHostPort(addr string) (host, port string) {
	if h, p, ok := splitBracketed(addr); ok {
		return h, p
	}
	i := strings.LastIndexByte(addr, ':')
	if i < 0 || !allDigits(addr[i+1:]) {
		return addr, ""
	}
	return addr[:i], addr[i+1:]
}

func splitBracketed(addr string) (host, port string, ok bool) {
	if !strings.HasPrefix(addr, "[") {
		return "", "", false
	}
	end := strings.LastIndexByte(addr, ']')
	if end < 2 {
		// No "]" at all, or "[]...", which names no host.
		return "", "", false
	}
	host = addr[1:end]
	if !strings.ContainsRune(host, ':') {
		return "", "", false
	}
	switch tail := addr[end+1:]; {
	case tail == "":
		return host, "", true
	case tail[0] == ':' && allDigits(tail[1:]):
		return host, tail[1:], true
	}
	return "", "", false
}

// allDigits reports whether s is one or more decimal digits, which is what the
// shared parser accepts as a port.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// String renders the configuration with every credential redacted (
// ).
//
// The credential is RENDERED redacted rather than omitted, and the difference
// is the point. These two renderings used to carry neither the user nor the
// password at all, which is safe and is not the same thing: an operator
// reading a log could not tell a configuration with a password from one
// without, which is the first question an authentication failure asks. Every
// other driver in the repository shows it, and the sibling
// `drivers/amqp10` shows it by delegating both renderings to its DSN.
//
// Nothing said so because this was the one driver package of sixteen with no
// test that its renderings redact: `make security-check` proves the two
// methods EXIST and cannot know which field is the credential.
func (c Config) String() string {
	r := c.resolve()
	var b strings.Builder
	b.Grow(160)
	b.WriteString(Scheme)
	b.WriteString("{addrs=[")
	for i, addr := range r.Addrs {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(normaliseAddr(addr, r.TLS != nil))
	}
	b.WriteString("] vhost=")
	b.WriteString(r.Vhost)
	if r.Username != "" {
		b.WriteString(" user=")
		b.WriteString(r.Username)
	}
	if r.Password != "" {
		b.WriteString(" password=")
		b.WriteString(dsn.Redacted)
	}
	b.WriteString(" prefix=")
	b.WriteString(r.Prefix)
	b.WriteString(" exchange=")
	b.WriteString(r.ExchangeType)
	b.WriteString(" queue=")
	b.WriteString(r.QueueType.String())
	b.WriteString(" deadletter=")
	b.WriteString(strconv.FormatBool(r.DeadLetter))
	b.WriteString(" credit=")
	b.WriteString(strconv.Itoa(r.Credit))
	b.WriteString(" receiversettle=")
	b.WriteString(r.ReceiverSettle.String())
	b.WriteString(" tls=")
	b.WriteString(strconv.FormatBool(r.TLS != nil))
	b.WriteByte('}')
	return b.String()
}

// LogValue is the slog form, redacted for the reason [Config.String] is: a
// structured logger reaches the fields directly rather than through String, so
// a credential left in one of them would reach the log whatever String did.
//
// It carries the same `user` and `password` fields, for the same reason: a
// structured log an operator queries is exactly where "is a password
// configured at all" gets asked.
func (c Config) LogValue() slog.Value {
	r := c.resolve()
	addrs := make([]string, 0, len(r.Addrs))
	for _, addr := range r.Addrs {
		addrs = append(addrs, normaliseAddr(addr, r.TLS != nil))
	}
	password := ""
	if r.Password != "" {
		password = dsn.Redacted
	}
	return slog.GroupValue(
		slog.Any("addrs", addrs),
		slog.String("vhost", r.Vhost),
		slog.String("user", r.Username),
		slog.String("password", password),
		slog.String("prefix", r.Prefix),
		slog.String("exchange_type", r.ExchangeType),
		slog.String("queue_type", r.QueueType.String()),
		slog.Bool("dead_letter", r.DeadLetter),
		slog.Int("credit", r.Credit),
		slog.String("receiver_settle", r.ReceiverSettle.String()),
		slog.Bool("tls", r.TLS != nil),
	)
}

// The configuration's refusals.
//
// Every one of them is CodeInvalidConfig: the caller's configuration cannot be
// used, which the specification distinguishes from a malformed argument on a call. Two of
// them — the declaration and the routing-key option — are refusals of
// something the OTHER RabbitMQ driver does, and each says so, because a caller
// who moved profiles is exactly who will hit them.
var (
	errNoAddrs = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: at least one broker endpoint is required")
	errPasswordWithoutUser = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a password needs a user name")
	errInsecureWithoutTLS = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: InsecureSkipVerify without TLS would dial in the clear, which is not \"less safe\" but \"not safe at all\"")
	errQueueType = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the queue type is not one this profile has capability claims for")
	errExchangeType = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the exchange type is not one RabbitMQ has")
	errDeclareUnsupported = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: Config.Declare cannot be honoured on the AMQP 1.0 profile: RabbitMQ 4's AMQP 1.0 endpoint creates no exchange, queue or binding, and an attach to an address nothing provisioned is answered with amqp:not-found; declare the topology with drivers/rabbitmq, the management API or rabbitmqctl")
	errKeyAsRoutingKey = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: Config.KeyAsRoutingKey cannot be honoured on the AMQP 1.0 profile: a sending link's address carries the routing key, so it is fixed when the link attaches and cannot vary per message")
	errVhostAndHostName = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: Config.Vhost and Config.HostName both write the OPEN frame's hostname, so only one of them may be set")
	errNegativeBound = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a bound cannot be negative")
	errNegativeDuration = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a timeout cannot be negative")
	errBadText = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a configured name contains a control character, which no log line or error could carry")
)

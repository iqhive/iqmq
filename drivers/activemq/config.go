package activemq

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The driver's identity.
const (
	// DriverName is the name this driver reports and the name its
	// conformance reports are keyed by. Both brokers answer to it, and the
	// PROFILE is which product a client is talking to.
	DriverName = "activemq"
	// Version is this driver's own version, which separates from the
	// client library's.
	Version = "0.1.0"
)

// The profile names, which are what a conformance report's `profile` field and
// `DriverInfo.Profile` carry.
const (
	// ProfileArtemis is Apache ActiveMQ Artemis.
	ProfileArtemis = "artemis"
	// ProfileClassic is Apache ActiveMQ Classic.
	ProfileClassic = "classic"
)

// Config is this driver's configuration.
//
// It is the broker's vocabulary and not the core's, and it is translated into
// [core.Config] by [Config.CoreConfig] rather than embedding it. The reason is
// the one `drivers/rabbitmq/amqp10` gives: a caller configuring an ActiveMQ
// driver should be reading about addresses, queues and virtual topics, and the
// core's own fields — the container id, the settle modes, the frame size — are
// passed through because they are the protocol's rather than because they are
// interesting here.
type Config struct {
	// Broker is which ActiveMQ this client is talking to. It is REQUIRED and
	// has no default; see the package documentation for why a default would
	// be a defect rather than a convenience.
	Broker activemq.Broker

	// Addrs are the broker endpoints, tried round-robin across reconnect
	// attempts. A bare `host` or `host:port` is given a scheme and the
	// default port; a URL keeps its own.
	//
	// A list is Artemis's failover pair and Classic's network of brokers.
	// What it is NOT is a cluster the driver follows: AMQP 1.0's
	// `connection-redirect` is classified as a retryable transport failure
	// by the core and not followed, so a redirect costs one reconnect
	// interval rather than being honoured.
	Addrs []string
	// Username is the SASL user. Artemis requires one by default; Classic's
	// pinned image allows anonymous connection unless
	// `ACTIVEMQ_CONNECTION_USER` is set.
	Username string
	// Password is the SASL password. It is an [dsn.Secret], so it is
	// redacted by [Config.String] and [Config.LogValue].
	Password dsn.Secret
	// HostName is the OPEN frame's `hostname`, for a broker behind a router
	// or serving several virtual hosts. Empty sends none.
	HostName string
	// ContainerID is this client's AMQP container id. Empty lets the core
	// generate one.
	//
	// It is worth setting on Artemis: the container id is what the broker
	// shows in its connection list, and a fleet whose clients all report
	// the same generated id is a fleet nobody can debug.
	ContainerID string
	// Properties are extra connection properties sent in the OPEN frame.
	Properties map[string]any
	// SASL selects the mechanism. The zero value negotiates.
	SASL core.SASLMechanism
	// InsecurePlainSASL permits PLAIN over a plaintext connection, which
	// sends the password in clear. The name is the specification's `Insecure*`
	// convention: the default refuses.
	InsecurePlainSASL bool
	// TLS is the TLS configuration. Non-nil dials `amqps`.
	TLS *tls.Config
	// InsecureSkipVerify disables certificate verification.
	InsecureSkipVerify bool

	// Prefix is prepended to every address and queue name so that two
	// deployments can share one broker. It is empty by default.
	//
	// On Classic it goes INSIDE the `VirtualTopic.` prefix
	// (`VirtualTopic.<prefix><topic>`), and the ordering was settled by the
	// broker rather than by reasoning: Classic's default
	// virtual-destination interceptor matches `VirtualTopic.>`, so
	// `iqmq.VirtualTopic.orders` is an ordinary topic that accepts every
	// publication and writes to no consumer queue, silently.
	// `internal/activemq.Config.Prefix` has the measurement.
	Prefix string

	// BrokerAutoCreates reports that the broker still creates a destination
	// from an attach.
	//
	// It is false by default, which is a statement about how this driver
	// expects to be DEPLOYED rather than about how the images ship: the specification
	// makes implicit topology creation a defect, and a broker that
	// auto-creates turns a typo in a topic name into an address that
	// receives every publication and delivers nothing. Artemis can be told
	// not to (`artemis create --no-autocreate`) and is what this driver's
	// conformance evidence is taken against; Classic cannot, so the field is
	// forced true there by the profile.
	//
	// Setting it changes no behaviour -- the driver creates nothing either
	// way -- it changes what the driver PAGE and `Profile.AutoCreates`
	// report, so that a deployment reading its own capability output is not
	// told a refusal it will not get.
	BrokerAutoCreates bool

	// Prioritised reports that the broker reorders a destination's messages
	// by priority.
	//
	// It exists for the same reason [Config.Scheduler] does and it is the
	// same silent absence: ActiveMQ Classic carries the AMQP header's
	// priority and dispatches in FIFO order unless the destination's policy
	// sets `prioritizedMessages="true"`, which the pinned image does not do
	// for queues at all. Measured with it off, a high-priority publication
	// arrives after a low-priority one -- so a driver that claimed
	// FeaturePriority regardless would promise an ordering the broker does
	// not provide.
	//
	// Artemis's queues are priority-ordered and this field is ignored there.
	Prioritised bool

	// Scheduler reports that the broker's message scheduler is enabled.
	//
	// It exists because Classic's is OFF by default and its absence is
	// silent: with `schedulerSupport` unset, a publication carrying
	// `AMQ_SCHEDULED_DELAY` is delivered immediately and the property
	// travels through to the consumer untouched. A driver that claimed
	// [mq.FeatureScheduledPublish] regardless would promise a delay the
	// broker discards, so the claim is withdrawn unless this is set.
	//
	// On Artemis the scheduler is always available and this field is
	// ignored.
	Scheduler bool

	// DeadLetterTopic routes a dead-lettered message to another TOPIC of
	// this driver's own rather than to the broker's dead-letter address.
	//
	// Empty is the usual case and means the broker's own policy handles a
	// `rejected` delivery — Artemis's `<dead-letter-address>` (DLQ in the
	// pinned image) and Classic's dead-letter strategy (ActiveMQ.DLQ). A
	// named topic makes the driver publish the message there and settle the
	// original only once that publication is accepted, which is the specification's
	// "a failed or ambiguous dead-letter publish never acknowledges the
	// original".
	DeadLetterTopic string

	// DeadLetter reports that the subscriptions' queues have a dead-letter
	// address configured, which is what makes `Nack(NackDeadLetter())`
	// reach one. It is a claim about the BROKER's configuration and the
	// driver cannot check it: neither product reports a queue's
	// dead-letter-address over AMQP 1.0. It is true by default because both
	// pinned images configure one for every address.
	DeadLetter bool
	// NoDeadLetter withdraws [Config.DeadLetter] for a deployment whose
	// address settings have none. It is a separate field rather than
	// `DeadLetter` defaulting to false because the zero Config should
	// describe the brokers as they ship.
	NoDeadLetter bool

	// Credit is the ceiling on the link credit one source may hold, which
	// is the prefetch. Zero takes the core's default.
	Credit int
	// MaxPending is how many asynchronous publications one sink may have
	// outstanding. Zero takes the core's default.
	MaxPending int
	// NonDurable publishes with the AMQP header's `durable` flag clear, so
	// the broker may lose the message on a restart. It is off by default.
	NonDurable bool
	// ReceiverSettle is when an acknowledgement becomes final.
	//
	// [core.ReceiverSettleSecond] is REFUSED on Artemis by
	// [Config.Validate], and that is measured rather than assumed: Artemis
	// answers an attach requesting `second` with `first`, and go-amqp
	// refuses the negotiation, so every source would fail at attach with a
	// message about settlement modes rather than about configuration.
	ReceiverSettle core.ReceiverSettleMode
	// SenderSettle is the sender's settlement mode.
	SenderSettle core.SenderSettleMode

	// IdleTimeout is the connection's idle timeout.
	IdleTimeout time.Duration
	// MaxFrameSize is the largest frame this client accepts.
	MaxFrameSize uint32
	// MaxSessions is the session ceiling.
	MaxSessions uint16
	// DialTimeout bounds one dial.
	DialTimeout time.Duration
	// OperationTimeout bounds one attach, settle or close.
	OperationTimeout time.Duration
	// WriteTimeout bounds one write.
	WriteTimeout time.Duration

	// Backoff is the reconnect policy. The zero value is the core's
	// [core.DefaultBackoff], which is full jitter and is named rather than
	// left to the variant family's `Auto` — whose provisional winner is
	// `none`, meaning no delay at all, which the specification forbids.
	Backoff core.BackoffPolicy
	// ReconnectBase, ReconnectMax and ReconnectGrowth shape the backoff.
	ReconnectBase   time.Duration
	ReconnectMax    time.Duration
	ReconnectGrowth float64
	// ReconnectAttempts bounds the reconnector's whole life rather than one
	// outage, which is how [driverkit.Reconnector] defines it and what
	// the maintenance notes records as the helper's fourth gap.
	ReconnectAttempts int
	// NoReconnect makes a lost connection terminal.
	NoReconnect bool

	// Publisher and Consumer are the core's per-link option sets.
	Publisher core.PublisherOptions
	Consumer  core.ConsumerOptions

	// Clock is the clock the driver reads. Nil uses the real one.
	Clock mq.Clock
	// Hooks are the driver's own events. The specification's `OnReconnect` is the
	// driver's to fire and nothing in the core fires it, so hooks passed to
	// [mq.NewClient] get no reconnect events and hooks passed to both get
	// every event twice — which is the item, unchanged through six
	// increments.
	Hooks mq.Hooks
}

// resolve fills the defaults, leaving the caller's value untouched.
func (c Config) resolve() Config {
	if len(c.Addrs) == 0 {
		c.Addrs = []string{"localhost:" + core.DefaultPort}
	}
	if c.Backoff == core.BackoffDefault {
		c.Backoff = core.DefaultBackoff
	}
	if !c.NoDeadLetter {
		c.DeadLetter = true
	} else {
		c.DeadLetter = false
	}
	return c
}

// Validate refuses a configuration before any I/O.
func (c Config) Validate() error {
	if !c.Broker.Valid() {
		return activemq.ErrNoBroker
	}
	if !c.SASL.Valid() || !c.ReceiverSettle.Valid() || !c.SenderSettle.Valid() || !c.Backoff.Valid() {
		return errBadEnum
	}
	if c.Broker == activemq.Artemis && c.ReceiverSettle.Resolve() == core.ReceiverSettleSecond {
		return errArtemisSettleSecond
	}
	if c.DeadLetter && c.NoDeadLetter {
		return errDeadLetterBoth
	}
	if c.Prefix != "" {
		if err := activemq.CheckName(c.Prefix, "prefix"); err != nil {
			return err
		}
	}
	if c.DeadLetterTopic != "" {
		if err := activemq.CheckTopic(c.DeadLetterTopic); err != nil {
			return err
		}
	}
	if c.Credit < 0 || c.MaxPending < 0 || c.ReconnectAttempts < 0 {
		return errNegative
	}
	if c.IdleTimeout < 0 || c.DialTimeout < 0 || c.OperationTimeout < 0 ||
		c.WriteTimeout < 0 || c.ReconnectBase < 0 || c.ReconnectMax < 0 {
		return errNegative
	}
	// The core validates the addresses, the credentials and the timeouts,
	// and it is the one that has to be satisfied, so it is asked rather than
	// imitated: a second reading of the same rules is a second thing to keep
	// in step.
	cc, err := c.CoreConfig()
	if err != nil {
		return err
	}
	return cc.Validate()
}

// Names derives the topology for one topic and one subscription.
//
// It is exported because Admin, the conformance harness and anybody debugging
// a deployment all need the same answer, and because the four combinations
// spell it differently: `activemq.Derive` is the only place that mapping
// lives.
func (c Config) Names(topic, subscription string) (activemq.Names, error) {
	return activemq.Derive(activemq.Config{
		Broker:          c.Broker,
		Prefix:          c.Prefix,
		Topic:           topic,
		Subscription:    subscription,
		DeadLetterTopic: c.DeadLetterTopic,
		// AMQP 1.0 carries Classic's `topic://` and `queue://` schemes,
		// which are the only thing that tells the broker which of the two
		// namespaces a name is in. STOMP says it another way, which is why
		// this is a flag and not a constant.
		Scheme: true,
	})
}

// CoreConfig translates this configuration into the shared core's.
func (c Config) CoreConfig() (core.Config, error) {
	r := c.resolve()
	addrs := make([]string, 0, len(r.Addrs))
	for _, a := range r.Addrs {
		addrs = append(addrs, normaliseAddr(a, r.TLS != nil))
	}
	return core.Config{
		Addrs:              addrs,
		ContainerID:        r.ContainerID,
		HostName:           r.HostName,
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
// The scheme is added rather than guessed at: an endpoint that already names
// one keeps it, so a failover list may mix a TLS node and a plaintext one and
// the core's credential check still sees the difference.
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

// splitHostPort separates an endpoint's host from its port, IPv6 literals
// included. It is the rule [Config.DSN] renders with, so the two have to agree
// or a rendered configuration cannot be parsed back.
func splitHostPort(s string) (host, port string) {
	if i := strings.LastIndex(s, "]"); i >= 0 {
		if j := strings.Index(s[i:], ":"); j >= 0 {
			return s[:i+1], s[i+j+1:]
		}
		return s, ""
	}
	i := strings.LastIndex(s, ":")
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i+1:]
}

// String renders the configuration as its DSN, with the password redacted.
func (c Config) String() string { return c.DSN() }

// LogValue is the redacting form `slog` uses.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

// addDuration appends a duration parameter to q when it is set.
func addDuration(q []string, key string, d time.Duration) []string {
	if d > 0 {
		return append(q, key+"="+d.String())
	}
	return q
}

// itoa is strconv.Itoa, named so that the DSN renderer reads as a list.
func itoa(n int) string { return strconv.Itoa(n) }

// The configuration refusals. The specification names no sentinel for a bad
// configuration and errors.Is matches by code, so a caller compares
// mq.CodeOf(err) against mq.CodeInvalidConfig.
var (
	errBadEnum = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: a configuration enum is outside its enumeration")
	errArtemisSettleSecond = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: Artemis does not support receiver-settle-mode \"second\": it answers an attach requesting it with \"first\" and go-amqp refuses the negotiation, so every source would fail at attach")
	errDeadLetterBoth = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: DeadLetter and NoDeadLetter are both set, and the driver cannot ask the broker which is true")
	errNegative = errcode.New(errcode.InvalidConfig,
		"iqmq/activemq: a count or a duration is negative")
	errNilRegistrar = errcode.New(errcode.InvalidArgument,
		"iqmq/activemq: Register was given no registry")
)

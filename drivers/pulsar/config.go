package pulsar

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The driver's identity.
const (
	// DriverName is the registered driver name.
	DriverName = "pulsar"
	// Version is this driver module's version.
	Version = "v0.0.0-dev"
	// ClientModule is the native client library.
	ClientModule = "github.com/apache/pulsar-client-go"
	// ClientVersion is that library's pinned version.
	ClientVersion = "v0.21.0"
)

// DefaultServiceURL is the binary-protocol endpoint a configuration that names
// none is dialled on.
const DefaultServiceURL = "pulsar://127.0.0.1:6650"

// The configuration defaults.
const (
	// DefaultConnectionTimeout bounds one connection attempt.
	DefaultConnectionTimeout = 10 * time.Second
	// DefaultOperationTimeout bounds one lookup or one producer or consumer
	// creation.
	DefaultOperationTimeout = 30 * time.Second
	// DefaultSendTimeout bounds one publication at the client. It is the
	// library's own default and it is a real bound: a publication that
	// times out has NOT been refused by the broker, which is why the driver
	// reports it as ambiguous.
	DefaultSendTimeout = 30 * time.Second
	// DefaultNackRedeliveryDelay is how long an undelayed negative
	// settlement holds a message back. Pulsar's own default is a minute,
	// which is longer than most handlers' retry budgets and longer than
	// every conformance scenario, so this driver lowers it and says so.
	DefaultNackRedeliveryDelay = time.Second
	// DefaultAckGroupingMaxSize is how many individual acknowledgements the
	// client groups into one frame.
	DefaultAckGroupingMaxSize = 1000
	// DefaultAckGroupingMaxTime is how long the client holds a group of
	// acknowledgements before sending it.
	DefaultAckGroupingMaxTime = 100 * time.Millisecond
	// DefaultMaxAttempts is how many deliveries a message may have before
	// the dead-letter route, when one is configured. It mirrors
	// pulsar.MaxReconsumeTimes.
	DefaultMaxAttempts = 16
	// DefaultAutoDiscoveryPeriod is how often a regex subscription looks
	// for new topics, and how often a producer looks for new partitions.
	// The library's own default is a minute; a topic created after a
	// pattern subscription then takes a minute to appear, which no test and
	// few operators would wait for.
	DefaultAutoDiscoveryPeriod = 5 * time.Second
	// DefaultMaxMessageBytes is the broker's own default
	// `maxMessageSize`, which the driver uses until a connection reports
	// the real one.
	DefaultMaxMessageBytes = 5 << 20
	// DefaultLeaseGuard is how long before a delivery's local lease expires
	// the driver stops vouching for it.
	DefaultLeaseGuard = 500 * time.Millisecond
	// DefaultAckTimeout is how long a delivery's lease lasts when the
	// configuration names none.
	//
	// It is the DRIVER's lease and not the broker's — see
	// Config.AckTimeout — and it is a minute rather than zero because the
	// alternative is a subscription in which an unsettled delivery is
	// never given back while the consumer stays connected. That is
	// Pulsar's own behaviour and it is not what the rest of this API
	// models: the settlement, LeaseExpires and the lease
	// scheduler all assume a delivery that comes back. A deployment that
	// wants Pulsar's behaviour exactly sets Config.DisableAckTimeout.
	DefaultAckTimeout = time.Minute
	// MaxDelay is the longest delayed publication or delayed redelivery
	// this driver will ask for. Pulsar's delayed-delivery tracker holds an
	// in-memory index entry per delayed message, so a very distant delivery
	// time is a broker-side leak rather than a schedule.
	MaxDelay = 30 * 24 * time.Hour
)

// SubscriptionKind is which of Pulsar's four subscription types a subscription
// is, which is the driver's single most consequential setting: the ordering
// scope is a property of it and of nothing else.
//
// It is on [Config] and not on a consumer because the specification makes a capability table
// constant for a backend's life. A subscription type that could be chosen per
// consumer would make one client advertise per-key ordering to a caller whose
// subscription had none.
type SubscriptionKind uint8

// The subscription kinds, in Pulsar's own order.
const (
	// SubscriptionShared is Pulsar's Shared: every consumer of the
	// subscription receives a share of the messages, round robin, and there
	// is NO ordering of any kind — not per key, not per partition. It is
	// this driver's default because it is the only type on which a second
	// consumer is an addition rather than a takeover.
	SubscriptionShared SubscriptionKind = iota
	// SubscriptionKeyShared is Pulsar's Key_Shared: messages sharing a key
	// go to one consumer, so per-key order is kept WHILE THE CONSUMER SET
	// IS STABLE. A consumer joining or leaving moves hash ranges, and
	// Pulsar then either blocks delivery of the moved ranges until the
	// previous owner's messages are settled (AllowOutOfOrderDelivery false,
	// which is what this driver sets) or delivers them at once to the new
	// owner out of order.
	SubscriptionKeyShared
	// SubscriptionExclusive is Pulsar's Exclusive: one consumer at a time,
	// and a second is refused. Delivery is in publication order within each
	// partition.
	SubscriptionExclusive
	// SubscriptionFailover is Pulsar's Failover: one active consumer and
	// standbys, with the same per-partition order as Exclusive while the
	// active consumer holds.
	SubscriptionFailover

	subscriptionKindCount
)

var subscriptionKindNames = [subscriptionKindCount]string{"shared", "keyshared", "exclusive", "failover"}

// String returns the kind's lower-case name, which is also its DSN spelling and
// the profile name its conformance report carries.
func (s SubscriptionKind) String() string {
	if s < subscriptionKindCount {
		return subscriptionKindNames[s]
	}
	return "SubscriptionKind(?)"
}

// Valid reports whether s names a subscription kind.
func (s SubscriptionKind) Valid() bool { return s < subscriptionKindCount }

// Native is the pulsar-client-go constant for this kind.
func (s SubscriptionKind) Native() pulsar.SubscriptionType {
	switch s {
	case SubscriptionShared:
		return pulsar.Shared
	case SubscriptionKeyShared:
		return pulsar.KeyShared
	case SubscriptionExclusive:
		return pulsar.Exclusive
	case SubscriptionFailover:
		return pulsar.Failover
	default:
		return pulsar.Shared
	}
}

// Exclusive reports whether this kind admits only one active consumer, which is
// what makes ConsumerGroups claim false for it.
func (s SubscriptionKind) Exclusive() bool {
	return s == SubscriptionExclusive || s == SubscriptionFailover
}

// SubscriptionKinds is every kind, for a table-driven test and for the DSN's
// enumeration.
func SubscriptionKinds() []SubscriptionKind {
	out := make([]SubscriptionKind, 0, subscriptionKindCount)
	for k := SubscriptionKind(0); k.Valid(); k++ {
		out = append(out, k)
	}
	return out
}

// CompressionKind is the wire compression a producer applies.
type CompressionKind uint8

// The compression kinds.
const (
	// CompressionNone sends the payload uncompressed, which is the
	// default: the specification makes a body's bytes the caller, and compression is
	// a deployment's choice rather than a driver's.
	CompressionNone CompressionKind = iota
	// CompressionLZ4 is Pulsar's usual choice.
	CompressionLZ4
	// CompressionZLib is zlib.
	CompressionZLib
	// CompressionZSTD is zstd.
	CompressionZSTD
	// CompressionSnappy is snappy.
	CompressionSnappy

	compressionKindCount
)

var compressionKindNames = [compressionKindCount]string{"none", "lz4", "zlib", "zstd", "snappy"}

// String returns the kind's lower-case name.
func (c CompressionKind) String() string {
	if c < compressionKindCount {
		return compressionKindNames[c]
	}
	return "CompressionKind(?)"
}

// Valid reports whether c names a compression kind.
func (c CompressionKind) Valid() bool { return c < compressionKindCount }

// Native is the pulsar-client-go constant for this kind.
func (c CompressionKind) Native() pulsar.CompressionType {
	switch c {
	case CompressionLZ4:
		return pulsar.LZ4
	case CompressionZLib:
		return pulsar.ZLib
	case CompressionZSTD:
		return pulsar.ZSTD
	case CompressionSnappy:
		return pulsar.SNAPPY
	case CompressionNone:
		return pulsar.NoCompression
	default:
		return pulsar.NoCompression
	}
}

// HashingKind is how a producer maps a message key onto a partition.
type HashingKind uint8

// The hashing kinds.
const (
	// HashingJavaString is Pulsar's default and the one the Java client
	// uses, so a Go producer and a Java producer put a key in the same
	// partition. It is this driver's default for exactly that reason.
	HashingJavaString HashingKind = iota
	// HashingMurmur3 is murmur3-32.
	HashingMurmur3

	hashingKindCount
)

var hashingKindNames = [hashingKindCount]string{"javastring", "murmur3"}

// String returns the kind's lower-case name.
func (h HashingKind) String() string {
	if h < hashingKindCount {
		return hashingKindNames[h]
	}
	return "HashingKind(?)"
}

// Valid reports whether h names a hashing kind.
func (h HashingKind) Valid() bool { return h < hashingKindCount }

// Native is the pulsar-client-go constant for this kind.
func (h HashingKind) Native() pulsar.HashingScheme {
	if h == HashingMurmur3 {
		return pulsar.Murmur3_32Hash
	}
	return pulsar.JavaStringHash
}

// Config is the Pulsar driver's configuration.
//
// Every field has a working zero value except [Config.ServiceURL], which
// defaults to the local standalone endpoint. [Config.Validate] is pure and is
// run before anything is dialled, so a typo is an immediate refusal rather than
// a lookup timeout.
type Config struct {
	// ServiceURL is the Pulsar binary-protocol endpoint, such as
	// "pulsar://broker:6650" or "pulsar+ssl://broker:6651". Empty means
	// DefaultServiceURL.
	ServiceURL string
	// AdminURL is the HTTP administrative endpoint, such as
	// "http://broker:8080". It is what Client.Admin needs; a configuration
	// without it has no administrative surface, and Admin says so rather
	// than guessing a port. It is reached over an HTTP transport of the
	// backend's own, cloned from http.DefaultTransport at Open: an https
	// AdminURL verifies with DefaultTransport's TLS configuration, NOT with
	// Config.TLS, and Close -- or an Open that is refused
	// -- releases its connections.
	AdminURL string

	// Domain is DomainPersistent (the default) or DomainNonPersistent for
	// topics named without a scheme.
	Domain string
	// Tenant is the tenant such topics live in. Empty means DefaultTenant.
	Tenant string
	// Namespace is the namespace inside the tenant. Empty means
	// DefaultNamespace.
	Namespace string
	// TopicPrefix is prepended to every topic's local name, which is how
	// two iqmq deployments share one namespace.
	TopicPrefix string

	// Token is a JWT bearer token. It is a dsn.Secret so that it is
	// redacted by every rendering.
	Token dsn.Secret
	// AuthName and AuthParams are pulsar-client-go's generic authentication
	// provider selection, for the mechanisms Token does not cover ("tls",
	// "athenz", "oauth2", "basic"). AuthParams is a secret for the same
	// reason Token is.
	AuthName   string
	AuthParams dsn.Secret
	// TLS is the transport configuration for a pulsar+ssl endpoint. It is a
	// *tls.Config rather than a set of file paths so that a caller can
	// supply an in-memory certificate. It is used exactly as given: set, it
	// is the whole TLS configuration and pulsar-client-go reads nothing
	// beside it, so Validate refuses it together with TLSTrustCertsFilePath,
	// AuthName "tls", or an InsecureSkipVerify it does not itself carry.
	// Nil is the usual case, and what a pulsar+ssl DSN
	// produces: the client then verifies against the system roots or
	// TLSTrustCertsFilePath and presents a TLS authentication provider's
	// certificate.
	TLS *tls.Config
	// TLSTrustCertsFilePath is the trust store, for a caller configuring
	// TLS by path rather than by value.
	TLSTrustCertsFilePath string
	// InsecureSkipVerify disables server verification. It is spelled the
	// standard library's way on purpose, and Validate refuses it without
	// a TLS endpoint.
	InsecureSkipVerify bool
	// ListenerName selects an advertised listener, for a broker behind a
	// proxy or in another network.
	ListenerName string

	// ConnectionTimeout bounds one connection attempt. Zero means
	// DefaultConnectionTimeout.
	ConnectionTimeout time.Duration
	// OperationTimeout bounds one lookup or one handle creation. Zero means
	// DefaultOperationTimeout.
	OperationTimeout time.Duration
	// KeepAliveInterval is how often the client pings a broker. Zero means
	// the library's own default.
	KeepAliveInterval time.Duration
	// MaxConnectionsPerBroker bounds the connection pool. Zero means the
	// library's own default of one.
	MaxConnectionsPerBroker int
	// MemoryLimitBytes bounds what the client buffers across every
	// producer. Zero means the library's own default; a negative value
	// disables the limit, which the specification forbids by default and which this
	// driver therefore refuses.
	MemoryLimitBytes int64
	// EnableTransactions turns on the transaction coordinator client, which
	// x/pulsartx needs. It is off by default because it costs a
	// coordinator connection and a broker that has not enabled
	// transactions refuses it.
	EnableTransactions bool

	// ProducerName is the producer name Pulsar reports on every message it
	// publishes. Empty means the broker generates one; a configured name
	// makes a second producer of the same name a CONFLICT, which is how
	// Pulsar's exclusive producer access works.
	ProducerName string
	// DisableBatching turns off producer batching. With batching on — the
	// default, and pulsar-client-go's — a publication waits up to
	// BatchingMaxPublishDelay for company, which is throughput at the cost
	// of latency, and it is what makes an entry hold several messages.
	DisableBatching bool
	// BatchingMaxPublishDelay is how long a batch waits for company. Zero
	// means the library's own 10 ms.
	BatchingMaxPublishDelay time.Duration
	// BatchingMaxMessages bounds one batch. Zero means the library's own
	// 1000.
	BatchingMaxMessages int
	// BatchingMaxSize bounds one batch's bytes. Zero means the library's
	// own 128 KiB.
	BatchingMaxSize int
	// MaxPendingMessages bounds the producer's own queue. Zero means the
	// library's own default.
	MaxPendingMessages int
	// BlockIfQueueFull makes a publication WAIT for room in the producer's
	// queue instead of being refused.
	//
	// It is false by default, and that is the answer rather than a
	// throughput choice: with it false the client answers a full queue with
	// ProducerQueueIsFull, which this driver maps to CodeBackpressure — a
	// distinct, non-retryable-at-the-driver signal the core's own
	// backpressure mode decides what to do with. With it true the driver
	// would block inside Publish for an unbounded time and the core could
	// not see it at all.
	BlockIfQueueFull bool
	// SendTimeout bounds one publication at the client. Zero means
	// DefaultSendTimeout; a negative value disables it, which makes a
	// publication wait for the caller's context alone.
	SendTimeout time.Duration
	// Compression is the wire compression. The default is none.
	Compression CompressionKind
	// Hashing is how a key chooses a partition. The default is Pulsar's own
	// JavaStringHash, so a Go and a Java producer agree.
	Hashing HashingKind
	// ProducerProperties are static properties stamped on the producer
	// itself, which Pulsar reports in its topic statistics. They are not
	// message headers.
	ProducerProperties map[string]string

	// SubscriptionType is which of Pulsar's four subscription types every
	// subscription of this client uses. The default is SubscriptionShared.
	// It decides the ordering claims of the capability table, so it is a
	// client-wide setting; see SubscriptionKind.
	SubscriptionType SubscriptionKind
	// AllowOutOfOrderDelivery loosens a Key_Shared subscription: a consumer
	// that joins receives its new hash ranges at once, even though the
	// previous owner still holds unsettled messages from them. It is false
	// by default, which is what makes the per-key ordering claim
	// answerable at all.
	AllowOutOfOrderDelivery bool
	// ReceiverQueueSize bounds the client's per-partition prefetch buffer.
	// Zero follows SourceConfig.Prefetch, which is the core's resolved
	// number and the one a caller actually set.
	ReceiverQueueSize int
	// AckGroupingMaxSize is how many individual acknowledgements the client
	// coalesces into one frame. Zero means DefaultAckGroupingMaxSize; one
	// disables grouping, so every acknowledgement is its own frame.
	AckGroupingMaxSize int
	// AckGroupingMaxTime is how long the client holds a group. Zero means
	// DefaultAckGroupingMaxTime.
	AckGroupingMaxTime time.Duration
	// AckWithResponse makes every acknowledgement a round trip the broker
	// answers, which turns the specification's "the driver took the settlement" from
	// "the frame was queued" into "the broker has it". It costs a round
	// trip per settlement and it is what FeatureBatchSettle's boundary
	// rests on, so the driver page states both.
	AckWithResponse bool
	// EnableBatchIndexAck lets an acknowledgement name ONE message of a
	// batched entry, which is ConsumerOptions.EnableBatchIndexAcknowledgment.
	//
	// It is FALSE by default, and that is rather than caution.
	// Without it, pulsar-client-go holds an individual acknowledgement back
	// until every message of the entry has been acknowledged and then sends
	// one frame for the entry — correct, at the cost of a batch with one
	// slow message keeping the whole entry unacknowledged. With it, the
	// frame carries an acknowledgement set naming the index; but the BROKER
	// honours an acknowledgement set only when its own
	// `acknowledgmentAtBatchIndexLevelEnabled` is true, and Apache Pulsar's
	// default is false. A client that sends a set to a broker that ignores
	// it has acknowledged the whole ENTRY, and every sibling in the batch is
	// marked deleted with no error anywhere.
	//
	// So the flag needs Config.AdminURL, and Open refuses it when the broker
	// reports the feature off. See AckRules and
	// TestCriticalBatchIndexAckDoesNotSettleASibling.
	EnableBatchIndexAck bool
	// NackRedeliveryDelay is how long an undelayed negative settlement
	// holds a message back. Zero means DefaultNackRedeliveryDelay.
	NackRedeliveryDelay time.Duration
	// RetryEnable turns on Pulsar's retry topic, which is what makes
	// ReconsumeLater — a per-message delayed redelivery — available.
	//
	// It costs something a caller has to know about: a consumer with the
	// retry topic enabled subscribes to TWO topics (the topic and its retry
	// topic), and a multi-topic consumer cannot Seek. So the driver
	// advertises SeekByTime and SeekBySequence as unsupported when it is on.
	RetryEnable bool
	// RetryTopic is the retry topic's name. Empty means Pulsar's own
	// `<topic>-<subscription>-RETRY`.
	RetryTopic string
	// DeadLetterTopic is where a rejected or exhausted message goes. Empty
	// means there is NO dead-letter destination: Reject is then refused
	// with ErrUnsupported and FeatureDeadLetter is not advertised, because
	// a driver that invented a destination would destroy a message the
	// caller believed was routed.
	DeadLetterTopic string
	// MaxAttempts is how many deliveries a message may have before Pulsar
	// routes it to DeadLetterTopic. Zero means DefaultMaxAttempts. It is
	// meaningless without a dead-letter topic.
	MaxAttempts uint32
	// AckTimeout is how long a delivery's lease lasts. Zero means
	// DefaultAckTimeout; Config.DisableAckTimeout removes the lease
	// altogether.
	//
	// It is the driver's, not the broker's, and that is the one place this
	// driver emulates something. Pulsar's broker redelivers an unsettled
	// message when the consumer disconnects and not on any timer, and
	// pulsar-client-go v0.21 removed the client-side acknowledgement
	// timeout the Java client still has. So a deployment that wants a
	// delivery given back after a while gets it here: the driver tracks
	// each delivery's deadline, hands the message back with a negative
	// acknowledgement one LeaseGuard before it, and refuses a settlement
	// that arrives after that. FeatureExtendLease is advertised as
	// EmulatedVolatile for exactly this reason — the deadline lives in this
	// process and is lost with it. Zero means no lease at all, which is
	// Pulsar's own behaviour.
	AckTimeout time.Duration
	// LeaseGuard is how long before AckTimeout the driver stops vouching
	// for a delivery. Zero means DefaultLeaseGuard, clamped to a fifth of
	// AckTimeout. It is meaningless without AckTimeout.
	LeaseGuard time.Duration
	// DisableAckTimeout turns the emulated lease off, which is Pulsar's own
	// behaviour: an unsettled delivery is given back when the consumer
	// disconnects and at no other time. With it set, deliveries carry no
	// LeaseExpires, FeatureExtendLease is not advertised, and a settlement
	// is never refused for being late.
	DisableAckTimeout bool
	// ReadCompacted reads the compacted view of a topic, which is the last
	// value per key rather than every message.
	ReadCompacted bool
	// AutoDiscoveryPeriod is how often a pattern subscription looks for new
	// topics and a producer for new partitions. Zero means
	// DefaultAutoDiscoveryPeriod.
	AutoDiscoveryPeriod time.Duration

	// Metadata is the mode a handle that named none gets. The zero
	// value is MetadataNativeStrict, which is the specification's default.
	Metadata mq.MetadataMode
	// MaxMessageBytes is the largest message this driver will publish. Zero
	// means DefaultMaxMessageBytes, which is the broker's own default; the
	// real figure is negotiated per connection and a broker that advertises
	// a smaller one refuses the publication.
	MaxMessageBytes int64
	// EnsureTopology creates the topic before a handle is opened. It is off
	// by default: Pulsar auto-creates a topic on first use when the
	// namespace allows it, which is the broker's policy and not this
	// driver's choice.
	EnsureTopology bool
	// PingTopic is an EXISTING topic Ping may look up when there is no
	// administrative endpoint. It is needed only for a backend that has
	// opened no handle yet: once a sink or a source exists, Ping looks up
	// that topic, which certainly exists. See Backend.Ping for why a
	// lookup of a topic nobody created is refused instead.
	PingTopic string
	// Partitions is how many partitions EnsureTopology gives a topic it
	// creates. Zero creates a non-partitioned topic, which is the only kind
	// on which Pulsar honours a Seek.
	Partitions int

	// Clock is the clock the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the driver's own observation points. OnReconnect is the one
	// the core cannot fire, because a Backend exists before the Client that
	// holds the core's hooks; a caller that installs one Hooks value on
	// both mq.WithHooks and here counts every reconnection twice.
	Hooks mq.Hooks
	// Logger is where pulsar-client-go's own logging goes. Nil discards it:
	// the library's default is logrus.StandardLogger(), which writes a
	// broker's every reconnection to standard error of a process that never
	// asked for it.
	Logger *slog.Logger
}

// Validate checks the configuration without any I/O. It is pure.
func (c Config) Validate() error {
	if err := c.validateEndpoints(); err != nil {
		return err
	}
	if err := c.validateNames(); err != nil {
		return err
	}
	return c.validateBounds()
}

// validateEndpoints checks the service and administrative URLs and the
// transport.
func (c Config) validateEndpoints() error {
	url := c.serviceURL()
	switch {
	case !strings.HasPrefix(url, "pulsar://") && !strings.HasPrefix(url, "pulsar+ssl://"):
		return errBadServiceURL
	case len(url) <= len("pulsar://"):
		return errBadServiceURL
	}
	if c.AdminURL != "" && !strings.HasPrefix(c.AdminURL, "http://") && !strings.HasPrefix(c.AdminURL, "https://") {
		return errBadAdminURL
	}
	if c.InsecureSkipVerify && !strings.HasPrefix(url, "pulsar+ssl://") {
		// Refusing is the only honest answer: the alternative is a client
		// that connected in the clear after the operator asked to skip
		// verification, which reads as "less safe" and is "no transport
		// security at all".
		return errInsecureWithoutTLS
	}
	if c.usesTLSAuth() && !strings.HasPrefix(url, "pulsar+ssl://") {
		// AuthName "tls" is a CLIENT CERTIFICATE, and a client certificate is
		// presented during a TLS handshake. pulsar-client-go picks the
		// transport from the scheme alone, so on "pulsar://" there is no
		// handshake to present it in: the provider is built, asked for
		// nothing, and the authentication silently does nothing.
		//
		// Measured on a socket rather than argued from the library's source. A
		// TLS port that REQUIRES a client certificate, dialled through this
		// driver's own clientOptions with authname=tls and a "pulsar://" URL,
		// saw no handshake at all and zero client certificates, where the same
		// authentication on "pulsar+ssl://" presents one
		// (`TestATLSAuthProviderIsNeverPresentedOnAPlaintextScheme` beside
		// `TestAPulsarSSLDSNPresentsItsTLSClientCertificate`).
		//
		// Refused rather than documented, which is the disposition
		// InsecureSkipVerify above and the specification's
		// TLS-beside-a-plaintext-scheme already have: the specification refuses a setting
		// the library would drop rather than dropping it, and an operator who
		// configured certificate authentication and got none is not in a
		// position to notice.
		return errTLSAuthWithoutTLS
	}
	if c.AuthName != "" && c.Token != "" {
		return errTwoAuthMechanisms
	}
	return c.validateTLS()
}

// usesTLSAuth reports whether AuthName selects pulsar-client-go's client
// certificate provider, under either of the two names it answers to.
func (c Config) usesTLSAuth() bool {
	return c.AuthName == "tls" || c.AuthName == "org.apache.pulsar.client.impl.auth.AuthenticationTls"
}

// validateTLS refuses the TLS settings pulsar-client-go would silently drop.
//
// A non-nil Config.TLS is used exactly as given: v0.21.0's
// connection.getTLSConfig returns it unchanged and never reads the trust file,
// the insecure flag or an authentication provider's client certificate beside
// it. So Config.TLS together with any of those is two answers to one question
// with one of them ignored, which the driver documentation says is refused at
// construction. Each has a spelling inside the
// *tls.Config -- RootCAs, InsecureSkipVerify, Certificates -- or works without
// one, which is what a pulsar+ssl DSN does.
func (c Config) validateTLS() error {
	if c.TLS == nil {
		return nil
	}
	switch {
	case c.TLSTrustCertsFilePath != "":
		return errTLSWithTrustFile
	case c.InsecureSkipVerify && !c.TLS.InsecureSkipVerify:
		return errTLSWithInsecure
	case c.usesTLSAuth():
		return errTLSWithTLSAuth
	}
	return nil
}

// validateNames checks the namespace coordinates and the topic names in the
// configuration.
func (c Config) validateNames() error {
	if c.Domain != "" && c.Domain != DomainPersistent && c.Domain != DomainNonPersistent {
		// The configuration's own code and not naming.go's: a domain in
		// a Config is a setting a deployment got wrong, and the specification's
		// codes are what a caller switches on to know which of the two
		// to go and look at.
		return errBadConfigDomain
	}
	for _, seg := range [...]string{c.Tenant, c.Namespace} {
		if seg == "" {
			continue
		}
		if err := CheckName(seg); err != nil {
			return err
		}
	}
	if c.TopicPrefix != "" {
		if err := CheckName(c.TopicPrefix + "x"); err != nil {
			return err
		}
	}
	for _, topic := range [...]string{c.DeadLetterTopic, c.RetryTopic, c.PingTopic} {
		if topic == "" {
			continue
		}
		if _, err := TopicOf(Config{Domain: c.Domain, Tenant: c.Tenant, Namespace: c.Namespace}, topic); err != nil {
			return err
		}
	}
	return nil
}

// validateBounds checks every number and enumeration.
func (c Config) validateBounds() error {
	switch {
	case !c.SubscriptionType.Valid():
		return errBadSubscriptionType
	case !c.Compression.Valid():
		return errBadCompression
	case !c.Hashing.Valid():
		return errBadHashing
	case !c.Metadata.Valid():
		return errMetadataMode
	case c.ConnectionTimeout < 0 || c.OperationTimeout < 0 || c.KeepAliveInterval < 0:
		return errNegativeTimeout
	case c.AckGroupingMaxTime < 0 || c.NackRedeliveryDelay < 0 || c.AutoDiscoveryPeriod < 0:
		return errNegativeTimeout
	case c.AckTimeout < 0 || c.LeaseGuard < 0:
		return errNegativeTimeout
	case c.MaxConnectionsPerBroker < 0 || c.MaxPendingMessages < 0 || c.ReceiverQueueSize < 0:
		return errNegativeBound
	case c.AckGroupingMaxSize < 0 || c.BatchingMaxMessages < 0 || c.BatchingMaxSize < 0:
		return errNegativeBound
	case c.Partitions < 0 || c.MaxMessageBytes < 0:
		return errNegativeBound
	case c.MemoryLimitBytes < 0:
		// The specification requires the client's buffers to be bounded by default,
		// and pulsar-client-go reads a negative memory limit as "no
		// limit". A deployment that really wants an unbounded client
		// buffer has to say so to the library directly, through
		// OpenWithClient.
		return errUnboundedMemory
	case c.AllowOutOfOrderDelivery && c.SubscriptionType != SubscriptionKeyShared:
		return errOutOfOrderWithoutKeyShared
	case c.TLS != nil && strings.HasPrefix(c.serviceURL(), "pulsar://"):
		// A *tls.Config beside a "pulsar://" service URL asks for two
		// different things, and the library grants the URL's: pulsar-client-go
		// reads TLS from the SCHEME alone (`PulsarServiceURI.UseTLS` is
		// `ServiceName == https || ServiceInfos contains "ssl"`), so
		// `ClientOptions.TLSConfig` does not make the connection TLS.
		// Measured on a socket: "pulsar://host" with a TLSConfig set writes a
		// plaintext Pulsar frame (00 00 00 25 …) while "pulsar+ssl://host"
		// writes a TLS ClientHello -- and Token and AuthParams travel on that
		// connection.
		return errTLSWithPlaintextURL
	case c.ServiceURL != "" && !serviceURLIsDialable(c.ServiceURL):
		// The typed twin of the DSN's dial guard: `dialableHost` ran only
		// inside ParseDSN, so a typed ServiceURL carried a host straight past
		// it.
		return errDSNHost
	case !c.DisableAckTimeout && c.LeaseGuard > 0 && c.LeaseGuard >= effectiveAckTimeout(c):
		// The comparison is on the EFFECTIVE AckTimeout, because a zero one
		// means "the default" to [Config.resolve] (which sets
		// DefaultAckTimeout) and "no lease at all" to this check -- so
		// "pulsar://h:6650?leaseguard=1h" resolved to AckTimeout=1m0s with
		// LeaseGuard=1h0m0s, precisely what this error describes, and was
		// accepted, while the same resolved pair written out in full
		// ("leaseguard=5m&acktimeout=1m") was refused.
		//
		// This is the fourth instance of the idiom the specification records and the one case of it that is refused rather than
		// reconciled: stretching AckTimeout up to an explicit LeaseGuard
		// would silently change how long the broker waits before
		// redelivering, so there is no reading of the pair that honours the
		// explicit value. The consequence of missing it is a lifecycle one
		// rather than a rendering one -- `Backend.leaseFor` hands every
		// delivery a guarded deadline already in the past, so none is ever
		// vouched for.
		return errLeaseGuardTooLong
	case c.DisableAckTimeout && (c.AckTimeout > 0 || c.LeaseGuard > 0):
		return errAckTimeoutBothWays
	case c.MaxAttempts > 0 && c.DeadLetterTopic == "":
		// MaxAttempts without a destination is a policy with nowhere to
		// go: Pulsar would route the message to a topic this driver
		// never named, and the caller would find neither the message nor
		// the reason. Saying so is better than a default destination.
		return errMaxAttemptsWithoutDLQ
	case c.RetryTopic != "" && !c.RetryEnable:
		return errRetryTopicWithoutRetry
	case c.EnableBatchIndexAck && c.AdminURL == "":
		return errBatchIndexAckWithoutAdmin
	}
	return nil
}

// resolve fills in every default, so that nothing below this point has to ask
// what a zero means.
func (c Config) resolve() Config {
	out := c
	out.ServiceURL = c.serviceURL()
	out.Domain = orDefault(c.Domain, DomainPersistent)
	out.Tenant = orDefault(c.Tenant, DefaultTenant)
	out.Namespace = orDefault(c.Namespace, DefaultNamespace)
	if out.ConnectionTimeout == 0 {
		out.ConnectionTimeout = DefaultConnectionTimeout
	}
	if out.OperationTimeout == 0 {
		out.OperationTimeout = DefaultOperationTimeout
	}
	if out.SendTimeout == 0 {
		out.SendTimeout = DefaultSendTimeout
	}
	if out.NackRedeliveryDelay == 0 {
		out.NackRedeliveryDelay = DefaultNackRedeliveryDelay
	}
	if out.AckGroupingMaxSize == 0 {
		out.AckGroupingMaxSize = DefaultAckGroupingMaxSize
	}
	if out.AckGroupingMaxTime == 0 {
		out.AckGroupingMaxTime = DefaultAckGroupingMaxTime
	}
	if out.AutoDiscoveryPeriod == 0 {
		out.AutoDiscoveryPeriod = DefaultAutoDiscoveryPeriod
	}
	if out.MaxMessageBytes == 0 {
		out.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if out.DeadLetterTopic != "" && out.MaxAttempts == 0 {
		out.MaxAttempts = DefaultMaxAttempts
	}
	if out.DisableAckTimeout {
		out.AckTimeout, out.LeaseGuard = 0, 0
	} else {
		if out.AckTimeout == 0 {
			out.AckTimeout = DefaultAckTimeout
		}
		if out.LeaseGuard == 0 {
			out.LeaseGuard = min(DefaultLeaseGuard, out.AckTimeout/5)
		}
	}
	return out
}

// serviceURL is the configured endpoint or the default.
func (c Config) serviceURL() string { return orDefault(c.ServiceURL, DefaultServiceURL) }

// HasDeadLetter reports whether this configuration names a dead-letter
// destination, which is what makes FeatureDeadLetter and Reject answerable.
func (c Config) HasDeadLetter() bool { return c.DeadLetterTopic != "" }

// HasLease reports whether this configuration emulates a delivery lease. It
// reads a RESOLVED configuration: before resolution a zero AckTimeout means the
// default rather than none.
func (c Config) HasLease() bool { return !c.DisableAckTimeout && c.AckTimeout > 0 }

// Secrets is every credential this configuration carries, for the conformance
// kit's redaction scenario and for nothing else.
func (c Config) Secrets() []string {
	var out []string
	for _, s := range [...]dsn.Secret{c.Token, c.AuthParams} {
		if v := s.Reveal(); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// String renders the configuration with every credential redacted.
func (c Config) String() string {
	var b strings.Builder
	b.WriteString("pulsar.Config{ServiceURL:")
	b.WriteString(c.serviceURL())
	if c.AdminURL != "" {
		b.WriteString(" AdminURL:")
		b.WriteString(c.AdminURL)
	}
	b.WriteString(" Topics:")
	b.WriteString(orDefault(c.Domain, DomainPersistent))
	b.WriteString("://")
	b.WriteString(orDefault(c.Tenant, DefaultTenant))
	b.WriteString("/")
	b.WriteString(orDefault(c.Namespace, DefaultNamespace))
	if c.TopicPrefix != "" {
		b.WriteString(" TopicPrefix:")
		b.WriteString(c.TopicPrefix)
	}
	b.WriteString(" SubscriptionType:")
	b.WriteString(c.SubscriptionType.String())
	b.WriteString(" Metadata:")
	b.WriteString(c.Metadata.String())
	if c.Token != "" {
		b.WriteString(" Token:")
		b.WriteString(c.Token.String())
	}
	if c.AuthName != "" {
		b.WriteString(" Auth:")
		b.WriteString(c.AuthName)
		b.WriteString("/")
		b.WriteString(c.AuthParams.String())
	}
	if c.DeadLetterTopic != "" {
		b.WriteString(" DeadLetterTopic:")
		b.WriteString(c.DeadLetterTopic)
		b.WriteString(" MaxAttempts:")
		b.WriteString(strconv.FormatUint(uint64(c.MaxAttempts), 10))
	}
	if c.AckTimeout > 0 {
		b.WriteString(" AckTimeout:")
		b.WriteString(c.AckTimeout.String())
	}
	b.WriteString("}")
	return b.String()
}

// LogValue renders the configuration for log/slog with every credential
// redacted.
func (c Config) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.String("serviceURL", c.serviceURL()),
		slog.String("namespace", orDefault(c.Domain, DomainPersistent)+"://"+
			orDefault(c.Tenant, DefaultTenant)+"/"+orDefault(c.Namespace, DefaultNamespace)),
		slog.String("subscriptionType", c.SubscriptionType.String()),
		slog.String("metadata", c.Metadata.String()),
	}
	if c.AdminURL != "" {
		attrs = append(attrs, slog.String("adminURL", c.AdminURL))
	}
	if c.Token != "" {
		attrs = append(attrs, slog.Any("token", c.Token))
	}
	if c.AuthName != "" {
		attrs = append(attrs, slog.String("auth", c.AuthName), slog.Any("authParams", c.AuthParams))
	}
	if c.DeadLetterTopic != "" {
		attrs = append(attrs, slog.String("deadLetterTopic", c.DeadLetterTopic),
			slog.Uint64("maxAttempts", uint64(c.MaxAttempts)))
	}
	if c.AckTimeout > 0 {
		attrs = append(attrs, slog.Duration("ackTimeout", c.AckTimeout))
	}
	return slog.GroupValue(attrs...)
}

// The configuration's refusals.
var (
	errBadServiceURL      = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: ServiceURL must be a pulsar:// or pulsar+ssl:// endpoint")
	errBadAdminURL        = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: AdminURL must be an http:// or https:// endpoint")
	errInsecureWithoutTLS = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: InsecureSkipVerify was asked for without a pulsar+ssl:// endpoint, which would connect in the clear rather than insecurely")
	errTwoAuthMechanisms  = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: Token and AuthName are two authentication mechanisms; configure one")
	errTLSWithTrustFile   = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: Config.TLS and TLSTrustCertsFilePath are both set, and pulsar-client-go uses Config.TLS as given and never reads the file; put the CA in Config.TLS.RootCAs or leave Config.TLS nil")
	errTLSWithInsecure    = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: InsecureSkipVerify is set beside a Config.TLS that verifies, and pulsar-client-go uses Config.TLS as given; set InsecureSkipVerify in Config.TLS or leave Config.TLS nil")
	errTLSAuthWithoutTLS  = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: AuthName \"tls\" is a client CERTIFICATE and a certificate is presented during a TLS handshake, but the service URL's \"pulsar://\" scheme means there is no handshake -- pulsar-client-go picks the transport from the scheme alone, so the certificate is never presented and the authentication silently does nothing; write \"pulsar+ssl://\", or use a mechanism that travels in the CONNECT frame (Token, or AuthName \"oauth2\", \"basic\" or \"athenz\")")
	errTLSWithTLSAuth             = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: AuthName \"tls\" is set beside Config.TLS, and pulsar-client-go never presents the provider's client certificate when Config.TLS is given; put the certificate in Config.TLS.Certificates or leave Config.TLS nil")
	errBadSubscriptionType        = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: SubscriptionType is not one of Pulsar's four")
	errBadConfigDomain            = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: Config.Domain is either persistent or non-persistent")
	errBadCompression             = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: Compression is not one of Pulsar's five")
	errBadHashing                 = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: Hashing is not one of Pulsar's two")
	errNegativeTimeout            = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: a timeout, delay or interval cannot be negative")
	errNegativeBound              = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: a count or size bound cannot be negative")
	errUnboundedMemory            = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: a negative MemoryLimitBytes asks pulsar-client-go for an unbounded client buffer, which the specification forbids by default")
	errOutOfOrderWithoutKeyShared = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: AllowOutOfOrderDelivery is a Key_Shared setting and means nothing on any other subscription type")
	errLeaseGuardTooLong     = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: LeaseGuard is not shorter than AckTimeout, so no delivery would ever be vouched for")
	errMaxAttemptsWithoutDLQ = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: MaxAttempts is the dead-letter policy's attempt bound and needs a DeadLetterTopic; without one Pulsar would route the message to a topic this driver never named")
	errRetryTopicWithoutRetry    = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: RetryTopic names Pulsar's retry topic and needs RetryEnable")
	errAckTimeoutBothWays        = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: DisableAckTimeout removes the emulated lease, so an AckTimeout or a LeaseGuard beside it asks for two different things")
	errBatchIndexAckWithoutAdmin = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: EnableBatchIndexAck needs Config.AdminURL, because a broker whose acknowledgmentAtBatchIndexLevelEnabled is false ignores the acknowledgement set and settles the WHOLE batched entry — and the only way to find out is to ask the administrative API ")
	errNilClient           = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the client is nil")
	errNilRegistrar        = errcode.New(errcode.InvalidArgument, "iqmq/pulsar: the DSN registrar is nil")
	errNoEndpoint          = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: the DSN names no broker endpoint")
	errTLSWithPlaintextURL = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: a TLS config is set beside a \"pulsar://\" service URL; pulsar-client-go picks the transport from the scheme and would connect in the clear, so the token would travel unprotected -- write \"pulsar+ssl://\" or drop the TLS config")
	errDSNHost    = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: a DSN host decodes to a byte a Pulsar service URL splits on or gives a meaning to -- \",\", \";\", \"@\", \"/\", \"?\", \"#\", a backslash, a space or a control character -- so it would dial somewhere the DSN does not name")
	errBadDSNPath = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: a Pulsar DSN's path is /tenant, /tenant/namespace or /tenant/namespace/prefix")
	errBadAuth    = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: pulsar-client-go refused the authentication provider named by Config.AuthName with Config.AuthParams; the library's own message is not carried here because it can echo the parameters, which is where a credential would be ")
	errBrokerRefusesBatchIndexAck = errcode.New(errcode.InvalidConfig,
		"iqmq/pulsar: EnableBatchIndexAck was asked for and the broker reports acknowledgmentAtBatchIndexLevelEnabled=false, so an individual acknowledgement of one message of a batch would settle every sibling in it; set acknowledgmentAtBatchIndexLevelEnabled=true on the broker or leave the option off")
	errMetadataMode = errcode.New(errcode.InvalidConfig, "iqmq/pulsar: the metadata mode is not one of the four")
)

// effectiveAckTimeout is AckTimeout once its zero has been read as "the
// default", which is what [Config.resolve] does and what the lease the source
// hands each delivery is built from.
func effectiveAckTimeout(c Config) time.Duration {
	if c.AckTimeout != 0 {
		return c.AckTimeout
	}
	return DefaultAckTimeout
}

// serviceURLIsDialable reports whether every broker a typed ServiceURL names
// is a host this driver would dial, applying the DSN's own rule to the struct
// path.
//
// The hosts are split the way pulsar-client-go's own splitURI splits them (on
// "," and on ";"), and the test is on the host alone: the scheme, any path and
// the port are separate positions.
func serviceURLIsDialable(serviceURL string) bool {
	rest := serviceURL
	if i := strings.Index(rest, "://"); i >= 0 {
		rest = rest[i+3:]
	}
	if i := strings.IndexAny(rest, "/?#"); i >= 0 {
		rest = rest[:i]
	}
	for _, h := range strings.FieldsFunc(rest, func(r rune) bool { return r == ',' || r == ';' }) {
		host, _ := splitAddr(h)
		if !dialableHost(host) {
			return false
		}
	}
	return true
}

package kafka

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DriverName is the driver's registered name and the scheme its DSN uses.
const DriverName = "kafka"

// Version is the version of this driver module. It is empty until the module
// is tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// DefaultBroker is the seed broker [Config] dials when none is given.
const DefaultBroker = "127.0.0.1:9092"

// The defaults a zero [Config] resolves to. Every one of them is a bound:
// The specification requires every queue to be bounded by default, so none of these may
// mean "unlimited" when it is left unset.
const (
	// DefaultCommitInterval is how often the driver commits the watermark
	// its tracker has advanced to. It is the width of the at-least-once
	// window in time, and 250 ms is a deliberate compromise: a commit per
	// record would cost a round trip per record, and Kafka's own
	// auto-commit default of five seconds is a five-second window.
	DefaultCommitInterval = 250 * time.Millisecond
	// DefaultRevokeWait bounds how long OnPartitionsRevoked waits for the
	// in-flight deliveries of a revoked partition to settle before it
	// commits the safe boundary and invalidates the generation. It has to
	// be well inside the group's rebalance timeout: a callback that
	// overruns it makes the member fall out of the group, which is a worse
	// outcome than a few redeliveries.
	DefaultRevokeWait = 5 * time.Second
	// DefaultFetchMaxWait is how long a fetch waits for records.
	DefaultFetchMaxWait = 500 * time.Millisecond
	// DefaultSessionTimeout is the group session timeout.
	DefaultSessionTimeout = 45 * time.Second
	// DefaultRebalanceTimeout is how long the coordinator waits for a
	// member to finish its revoke callback and rejoin.
	DefaultRebalanceTimeout = 60 * time.Second
	// DefaultHeartbeatInterval is the group heartbeat interval.
	DefaultHeartbeatInterval = 3 * time.Second
	// DefaultLinger is the producer's batching linger, and it is ZERO,
	// which is Kafka's own linger.ms default and franz-go's.
	//
	// A non-zero default looks attractive — it batches a burst — and it is
	// wrong for a synchronous publisher: Publish waits for the record's
	// promise, so every publication pays the linger before the record is
	// even sent. A five-millisecond default made the fidelity
	// scenario publish 180 messages a second and time out, which is what
	// found this. A caller that wants batching asks
	// for it, with PublishBatch, with the core's own batching
	// configuration, or by setting this field.
	DefaultLinger = 0
	// DefaultPartitions is how many partitions the typed administration
	// creates a topic with.
	DefaultPartitions = 1
	// DefaultReplicationFactor is the replication factor the typed
	// administration creates a topic with.
	DefaultReplicationFactor = -1
	// DefaultRequestTimeout bounds one administrative or commit round trip
	// whose caller gave no deadline.
	DefaultRequestTimeout = 30 * time.Second
	// DefaultDeliveryTimeout bounds how long the producer keeps retrying
	// one record before it gives up on it. It is Kafka's own
	// delivery.timeout.ms default, and it is not optional: franz-go retries
	// a RETRYABLE produce error — NOT_ENOUGH_REPLICAS, a leader election, a
	// coordinator move — until it succeeds, so a publication made on a
	// context with no deadline against a cluster that is down would never
	// return at all. The specification requires every operation to be bounded, and
	// giving up produces an AMBIGUOUS outcome rather than a definite
	// failure, which is the honest answer.
	DefaultDeliveryTimeout = 2 * time.Minute
)

// SASLMechanism names the SASL mechanism a [Config] authenticates with.
type SASLMechanism uint8

// The SASL mechanisms this driver can build from a typed configuration or a
// DSN. Anything else — OAUTHBEARER with a token source, GSSAPI, a custom
// mechanism — is reached through [OpenWithClient], where the caller builds the
// *kgo.Client and iqmq borrows it.
const (
	// SASLNone is the zero value: no SASL.
	SASLNone SASLMechanism = iota
	// SASLPlain is SASL/PLAIN.
	SASLPlain
	// SASLScramSHA256 is SASL/SCRAM-SHA-256.
	SASLScramSHA256
	// SASLScramSHA512 is SASL/SCRAM-SHA-512.
	SASLScramSHA512

	saslCount
)

var saslNames = [saslCount]string{"none", "plain", "scram-sha-256", "scram-sha-512"}

// String returns the mechanism's lower-case name, as a DSN spells it.
func (s SASLMechanism) String() string {
	if s < saslCount {
		return saslNames[s]
	}
	return "SASLMechanism(?)"
}

// Valid reports whether s names a mechanism.
func (s SASLMechanism) Valid() bool { return s < saslCount }

// SASLNames are the mechanism names a DSN accepts, in enumeration order.
func SASLNames() []string { return saslNames[:] }

// Config is the typed configuration of the Kafka driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). Every credential is a
// [github.com/iqhive/iqmq/dsn.Secret], so neither [Config.String] nor a slog
// record nor an error built from one can carry it.
//
// [Config.Validate] is pure: it performs no I/O and never dials, which is the specification's
// "validate config before starting background work".
type Config struct {
	// Brokers are the seed brokers. Empty means DefaultBroker.
	Brokers []string
	// ClientID is the client identifier the broker logs and reports in its
	// quota metrics. Empty means "iqmq".
	ClientID string
	// TopicPrefix is prepended to every topic name, which is how two iqmq
	// deployments share one cluster. Empty means the topic is used as is.
	TopicPrefix string

	// Username and Password are the SASL credentials.
	Username string
	Password dsn.Secret
	// SASL is the mechanism to authenticate with. It is required when a
	// user name is given and refused when one is not.
	SASL SASLMechanism
	// TLS is the TLS configuration. A non-nil value turns TLS on.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks so that it is greppable and obviously unsafe.
	InsecureSkipVerify bool

	// DisableIdempotency turns the producer's idempotence off, which also
	// weakens the acknowledgement to what the broker will accept without
	// it. It exists for a cluster whose ACLs do not grant IdempotentWrite,
	// and it changes the driver's capability table: FeatureIdempotentPublish
	// is not advertised.
	DisableIdempotency bool
	// Linger is the producer's batching linger. Zero means no linger, which
	// is DefaultLinger and is what a synchronous publisher wants: it waits
	// for the record's promise, so a linger is latency it pays and cannot
	// use. A caller that publishes in bursts and does not wait sets it.
	Linger time.Duration
	// BatchMaxBytes bounds one produced batch. Zero means franz-go's own
	// default, which is the broker's default message.max.bytes.
	BatchMaxBytes int32
	// MaxBufferedRecords bounds the producer's buffer, which is the specification's
	// "all queues bounded" for the publication side. Zero means franz-go's
	// own default (10,000).
	MaxBufferedRecords int
	// TransactionalID turns the producer into a transactional one, which is
	// what github.com/iqhive/iqmq/x/kafkatx needs. It is empty by default:
	// a transactional producer changes the consumer's isolation level and
	// the meaning of a produce, so it is never selected implicitly.
	TransactionalID string
	// ReadCommitted makes the consumer skip records of aborted and open
	// transactions. It is off by default and is what a consumer of a
	// transactional producer's topic wants.
	ReadCommitted bool

	// FetchMaxWait is how long one fetch waits for records. Zero means
	// DefaultFetchMaxWait.
	FetchMaxWait time.Duration
	// FetchMaxBytes bounds one fetch. Zero means franz-go's own default.
	FetchMaxBytes int32
	// SessionTimeout, RebalanceTimeout and HeartbeatInterval are the
	// group's own timings. Zero selects the defaults above.
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	HeartbeatInterval time.Duration
	// InstanceID is the static group membership identifier. Empty means
	// dynamic membership, which is the default and the right answer unless
	// the deployment has stable pod identities.
	InstanceID string

	// CommitInterval is how often the driver commits the watermark its
	// trackers have advanced to. Zero means DefaultCommitInterval; a
	// negative value means the driver commits only at a revoke, at Close
	// and when [Backend.Commit] is called.
	CommitInterval time.Duration
	// RevokeWait bounds the wait for in-flight deliveries in
	// OnPartitionsRevoked. Zero means DefaultRevokeWait.
	RevokeWait time.Duration

	// Tracker is the offset-tracker kind (ADR-019). The zero
	// value is Auto, which resolves to the bake-off winner.
	Tracker offset.Kind
	// TrackerWindow, TrackerMaxPending and TrackerMaxGap bound one
	// partition's tracking window and decide when the driver pauses the
	// partition instead of buffering. Zero selects the tracker's own
	// defaults.
	TrackerWindow     int
	TrackerMaxPending int
	TrackerMaxGap     int

	// Nack is the negative-settlement strategy. The zero
	// value is Auto, which resolves to block-partition: the only kind that
	// changes neither ordering nor durability.
	Nack nack.Kind
	// NackConfig configures that strategy — the attempt limit, the
	// dead-letter topic, the number of retry topics, the redelivery delay
	// and, for the custom kind, the decision function.
	NackConfig nack.Config

	// AutoCreateTopics lets a produce or a metadata request create a topic
	// the cluster does not have. It is off by default, which is the rule
	// that a driver creates no topology unless it is asked to.
	AutoCreateTopics bool
	// Partitions and ReplicationFactor are what the typed administration
	// creates a topic with. Zero selects the defaults above.
	Partitions        int32
	ReplicationFactor int16
	// RequestTimeout bounds one administrative or commit round trip whose
	// caller gave no deadline of its own. It is never a second deadline on
	// a caller that has one.
	RequestTimeout time.Duration
	// DeliveryTimeout bounds how long the producer retries one record
	// before giving up on it, whose outcome is then ambiguous. Zero means
	// DefaultDeliveryTimeout; a negative value means franz-go's own
	// default, which is to retry for ever.
	DeliveryTimeout time.Duration
	// UnboundedDelivery lets the IDEMPOTENT producer keep retrying a record
	// whose outcome it does not know, for ever, which is franz-go's own
	// default and is not iqmq's.
	//
	// franz-go refuses by default to let a context cancellation, a delivery
	// timeout or a retry limit fail a record that is in flight, because it
	// cannot tell "the broker never got it" from "the broker wrote it and
	// the reply was lost", and cancelling leaves its idempotent sequence
	// window inconsistent with the broker. The consequence is that a
	// Publish against a cluster answering REQUEST_TIMED_OUT NEVER RETURNS,
	// and the caller's own deadline cannot end it — which the specification's "all
	// operations bounded" forbids and which this driver's first ambiguity
	// test hung on.
	//
	// So the default here is the opposite: the driver sets
	// AllowIdempotentProduceCancellation, the delivery timeout bounds the
	// record, and the outcome is reported as ErrSettlementUnknown —
	// which is precisely what "the broker may have written it" means and is
	// the whole reason iqmq has an ambiguous outcome at all. Setting this
	// field restores franz-go's behaviour for a caller who would rather
	// wait for ever than reconcile.
	//
	// It has no effect on a transactional producer: franz-go refuses the
	// cancellation option together with a transactional id, so a producer
	// with [Config.TransactionalID] set is unbounded either way.
	UnboundedDelivery bool

	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks. Only OnReconnect is fired
	// from here — a driver has no access to the client's own hooks, so the
	// events this driver produces have nowhere else to go (recorded in
	// the maintenance notes).
	Hooks mq.Hooks
}

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if len(c.Brokers) == 0 {
		c.Brokers = []string{DefaultBroker}
	}
	if c.ClientID == "" {
		c.ClientID = "iqmq"
	}
	if c.FetchMaxWait == 0 {
		c.FetchMaxWait = DefaultFetchMaxWait
	}
	if c.SessionTimeout == 0 {
		c.SessionTimeout = DefaultSessionTimeout
	}
	if c.RebalanceTimeout == 0 {
		c.RebalanceTimeout = DefaultRebalanceTimeout
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.CommitInterval == 0 {
		c.CommitInterval = DefaultCommitInterval
	}
	if c.RevokeWait == 0 {
		c.RevokeWait = DefaultRevokeWait
	}
	if c.Partitions == 0 {
		c.Partitions = DefaultPartitions
	}
	if c.ReplicationFactor == 0 {
		c.ReplicationFactor = DefaultReplicationFactor
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = DefaultRequestTimeout
	}
	if c.DeliveryTimeout == 0 {
		c.DeliveryTimeout = DefaultDeliveryTimeout
	}
	return c
}

// linger is the producer linger a resolved configuration means. A negative
// value is read as none rather than refused, so that a caller switching the
// field off cannot accidentally ask for a negative duration.
func (c Config) linger() time.Duration {
	if c.Linger < 0 {
		return 0
	}
	return c.Linger
}

// trackerConfig is the per-partition tracking window, based at off.
func (c Config) trackerConfig(off int64) offset.Config {
	return offset.Config{
		Base:       off,
		Window:     c.TrackerWindow,
		MaxPending: c.TrackerMaxPending,
		MaxGap:     c.TrackerMaxGap,
	}
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It is pure on purpose: the specification requires a driver to refuse a configuration before
// it starts background work, and a validation that dialled would turn a typo
// into a connection timeout.
func (c Config) Validate() error {
	for _, b := range c.Brokers {
		if err := checkBroker(b); err != nil {
			return err
		}
	}
	if !printableText(c.ClientID) || !printableText(c.Username) ||
		!printableText(c.InstanceID) || !printableText(c.TransactionalID) {
		return errBadText
	}
	if c.TopicPrefix != "" && !validTopicPart(c.TopicPrefix) {
		return errBadPrefix
	}
	switch {
	case !c.SASL.Valid():
		return errBadSASL
	case c.SASL != SASLNone && c.Username == "":
		return errSASLWithoutUser
	case c.Username != "" && c.SASL == SASLNone:
		return errUserWithoutSASL
	case c.InsecureSkipVerify && c.TLS == nil:
		return errInsecureWithoutTLS
	}
	switch {
	case c.FetchMaxWait < 0 || c.SessionTimeout < 0 || c.RebalanceTimeout < 0 ||
		c.HeartbeatInterval < 0 || c.RevokeWait < 0 || c.RequestTimeout < 0:
		return errNegativeDuration
	case c.BatchMaxBytes < 0 || c.FetchMaxBytes < 0 || c.MaxBufferedRecords < 0:
		return errNegativeBound
	case c.Partitions < -1 || c.ReplicationFactor < -1:
		return errBadTopology
	}
	if !c.Tracker.Valid() {
		return errBadTracker
	}
	if err := c.trackerConfig(0).Validate(); err != nil {
		return err
	}
	if err := c.NackConfig.Validate(c.Nack); err != nil {
		return err
	}
	if c.NackConfig.DeadLetterTopic != "" && !validTopicName(c.NackConfig.DeadLetterTopic) {
		return errBadDeadLetter
	}
	return nil
}

// checkBroker refuses a seed broker that is not printable ASCII.
//
// It is the same rule NATS took for the same reason : a
// broker address goes into the dial and into every rendering of the
// configuration, so a byte that is not printable ASCII produces a log line and
// an error string that are not valid UTF-8. An international host name reaches
// Kafka as punycode, which is ASCII, so nothing legitimate is refused.
func checkBroker(b string) error {
	if strings.TrimSpace(b) == "" {
		return errNoBroker
	}
	for i := range len(b) {
		if c := b[i]; c <= ' ' || c >= 0x7f {
			return errBadBroker
		}
	}
	return nil
}

// printableText reports whether a configured string is safe to render.
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

// DSN renders the configuration as a connection string with the credential
// redacted, so a Config is safe to log or put in an error.
//
// It is not a round trip and is not meant to be one: the password is gone from
// the result on purpose, which is the whole reason the rendering exists.
func (c Config) DSN() string {
	cfg := c.resolve()
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	if cfg.Username != "" {
		b.WriteString(escapeUser(cfg.Username))
		if cfg.Password != "" {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i, br := range cfg.Brokers {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(renderBroker(br))
	}
	if cfg.TopicPrefix != "" {
		b.WriteByte('/')
		b.WriteString(escapePath(cfg.TopicPrefix))
	}
	q := make([]string, 0, 16)
	q = append(q,
		"clientid="+escapeQuery(cfg.ClientID),
		"linger="+cfg.Linger.String(),
		"fetchmaxwait="+cfg.FetchMaxWait.String(),
		"sessiontimeout="+cfg.SessionTimeout.String(),
		"rebalancetimeout="+cfg.RebalanceTimeout.String(),
		"heartbeatinterval="+cfg.HeartbeatInterval.String(),
		"commitinterval="+cfg.CommitInterval.String(),
		"revokewait="+cfg.RevokeWait.String(),
		"requesttimeout="+cfg.RequestTimeout.String(),
		"deliverytimeout="+cfg.DeliveryTimeout.String(),
		"tracker="+cfg.Tracker.String(),
		"nack="+cfg.Nack.String(),
		"partitions="+strconv.Itoa(int(cfg.Partitions)),
		"replicationfactor="+strconv.Itoa(int(cfg.ReplicationFactor)),
	)
	if cfg.SASL != SASLNone {
		q = append(q, "sasl="+cfg.SASL.String())
	}
	if cfg.TLS != nil {
		q = append(q, "tls=true")
	}
	if cfg.InsecureSkipVerify {
		q = append(q, "insecureskipverify=true")
	}
	if cfg.DisableIdempotency {
		q = append(q, "disableidempotency=true")
	}
	if cfg.UnboundedDelivery {
		q = append(q, "unboundeddelivery=true")
	}
	if cfg.AutoCreateTopics {
		q = append(q, "autocreatetopics=true")
	}
	if cfg.ReadCommitted {
		q = append(q, "readcommitted=true")
	}
	// The three bounds whose zero means "franz-go's own default": a value
	// the parser ACCEPTED and the rendering dropped, so the rendering read
	// back as the library default rather than as what was asked for.
	// Validate already refuses a negative one.
	if cfg.BatchMaxBytes != 0 {
		q = append(q, "batchmaxbytes="+strconv.Itoa(int(cfg.BatchMaxBytes)))
	}
	if cfg.FetchMaxBytes != 0 {
		q = append(q, "fetchmaxbytes="+strconv.Itoa(int(cfg.FetchMaxBytes)))
	}
	if cfg.MaxBufferedRecords != 0 {
		q = append(q, "maxbufferedrecords="+strconv.Itoa(cfg.MaxBufferedRecords))
	}
	if cfg.InstanceID != "" {
		q = append(q, "instanceid="+escapeQuery(cfg.InstanceID))
	}
	if cfg.TransactionalID != "" {
		q = append(q, "transactionalid="+escapeQuery(cfg.TransactionalID))
	}
	if cfg.NackConfig.DeadLetterTopic != "" {
		q = append(q, "deadlettertopic="+escapeQuery(cfg.NackConfig.DeadLetterTopic))
	}
	if cfg.NackConfig.MaxAttempts != 0 {
		q = append(q, "maxattempts="+strconv.FormatUint(uint64(cfg.NackConfig.MaxAttempts), 10))
	}
	if cfg.NackConfig.Retries != 0 {
		q = append(q, "retries="+strconv.Itoa(cfg.NackConfig.Retries))
	}
	if cfg.NackConfig.Delay != 0 {
		q = append(q, "nackdelay="+cfg.NackConfig.Delay.String())
	}
	b.WriteByte('?')
	b.WriteString(strings.Join(q, "&"))
	return b.String()
}

// String renders the configuration with the credential redacted.
func (c Config) String() string { return c.DSN() }

// LogValue renders the configuration for slog with the credential redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

// The configuration refusals. The specification names no sentinel for a bad configuration
// and errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errNoBroker           = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a seed broker in Brokers is empty")
	errBadBroker          = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a seed broker in Brokers is not printable ASCII; an international host name is dialled as punycode")
	errBadText            = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a configured identifier carries a control character or is not valid UTF-8")
	errBadPrefix          = errcode.New(errcode.InvalidConfig, "iqmq/kafka: TopicPrefix is not made of characters a Kafka topic name accepts")
	errBadSASL            = errcode.New(errcode.InvalidConfig, "iqmq/kafka: SASL is not one of the mechanisms this driver builds")
	errSASLWithoutUser    = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a SASL mechanism needs a user name")
	errUserWithoutSASL    = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a user name needs a SASL mechanism; name one rather than letting the driver guess")
	errInsecureWithoutTLS = errcode.New(errcode.InvalidConfig, "iqmq/kafka: InsecureSkipVerify has no meaning without TLS")
	errNegativeDuration   = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a timeout cannot be negative")
	errNegativeBound      = errcode.New(errcode.InvalidConfig, "iqmq/kafka: a byte or record bound cannot be negative")
	errBadTopology        = errcode.New(errcode.InvalidConfig, "iqmq/kafka: Partitions and ReplicationFactor are positive, or -1 for the cluster's default")
	errBadTracker         = errcode.New(errcode.InvalidConfig, "iqmq/kafka: Tracker is not one of the kinds")
	errBadDeadLetter      = errcode.New(errcode.InvalidConfig, "iqmq/kafka: the dead-letter topic is not a valid Kafka topic name")
	errNilRegistrar       = errcode.New(errcode.InvalidArgument, "iqmq/kafka: Register was given a nil Registrar")
	errNilClient          = errcode.New(errcode.InvalidArgument, "iqmq/kafka: OpenWithClient was given a nil client")
)

// stringer and logValuer are the two rendering surfaces the specification asks a
// configuration to redact through; the third, encoding/json, is
// [github.com/iqhive/iqmq/dsn.Secret]'s own and is proved there.
type (
	stringer  interface{ String() string }
	logValuer interface{ LogValue() slog.Value }
)

// compile-time proof that Config renders through both of them.
var (
	_ stringer  = Config{}
	_ logValuer = Config{}
)

// The escape sets, one per position of a DSN, each holding exactly the bytes
// the shared parser gives a meaning to THERE -- because it decodes every
// component, so each can hold any byte, and escaping more than a position
// needs makes a rendering unreadable and longer than what it parsed.
//
//   - a user ends at the first ":" (the password) and the authority at "/",
//     "?" or "#"; an "@" in it is safe, because the credential ends at the
//     LAST one;
//   - a broker is also split on "," (the next broker), on "@" and on ":" (the
//     port), and one that starts with "[" is read as a bracketed literal;
//   - the path runs from the first "/" to the "?";
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escUser  = "%:/?#"
	escHost  = "%,@:/?#"
	escPath  = "%?#"
	escQuery = "%+&#"
)

// escapeUser escapes a user name for the DSN's credential position.
func escapeUser(s string) string { return escapeSet(s, escUser) }

// escapePath escapes the topic prefix, which the DSN carries as its path.
func escapePath(s string) string { return escapeSet(s, escPath) }

// escapeQuery escapes a query parameter's value.
func escapeQuery(s string) string { return escapeSet(s, escQuery) }

// escapeHost escapes an unbracketed host, including a leading "[", which would
// otherwise open a bracketed literal.
func escapeHost(s string) string {
	out := escapeSet(s, escHost)
	if strings.HasPrefix(out, "[") {
		out = "%5B" + out[1:]
	}
	return out
}

// escapeSet percent-encodes every byte of s that is in set.
func escapeSet(s, set string) string {
	if !strings.ContainsAny(s, set) {
		return s
	}
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(set, c) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xF])
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// renderBroker renders one seed broker as a DSN endpoint that parses back to
// it.
//
// A host with a colon or a "%" in it is written as a bracketed literal, raw,
// which is how an IPv6 address reads and how it came in: the parser takes a
// bracketed host verbatim, without decoding it, so neither needs an escape
// there -- and a "%" escaped outside the brackets would triple, which is the
// one way a parsed host could otherwise render longer than it was written.
// That works unless the host holds a byte the authority is split on before the
// brackets are seen, and such a host is escaped instead, colons and all.
func renderBroker(a string) string {
	host, port := splitAddr(a)
	out := escapeHost(host)
	if bracketable(host) {
		out = "[" + host + "]"
	}
	if port == "" {
		return out
	}
	return out + ":" + port
}

// bracketable reports whether host is better written as a raw bracketed
// literal: it has a byte an unbracketed host would have to escape and that a
// bracketed one keeps verbatim, and none that splits the authority first.
func bracketable(host string) bool {
	return strings.ContainsAny(host, ":%") && !strings.ContainsAny(host, "]/?#@,")
}

// splitAddr is the inverse of `dsn.Host.String`, which is how [ParseDSN] turns
// an endpoint into a broker address: a host with a colon in it is bracketed,
// and a port is the decimal digits after the last colon outside the brackets.
func splitAddr(a string) (host, port string) {
	if strings.HasPrefix(a, "[") {
		if i := strings.LastIndex(a, "]:"); i > 0 && allDigits(a[i+2:]) && strings.ContainsRune(a[1:i], ':') {
			return a[1:i], a[i+2:]
		}
		if len(a) >= 2 && strings.HasSuffix(a, "]") && strings.ContainsRune(a[1:len(a)-1], ':') {
			return a[1 : len(a)-1], ""
		}
	}
	if i := strings.LastIndexByte(a, ':'); i >= 0 && allDigits(a[i+1:]) && !strings.ContainsRune(a[:i], ':') {
		return a[:i], a[i+1:]
	}
	return a, ""
}

// allDigits reports whether s is one or more decimal digits.
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

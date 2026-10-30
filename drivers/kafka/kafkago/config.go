package kafkago

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

// DriverName is the broker this driver speaks to, and it is deliberately the
// same string the franz-go driver reports: both are Kafka, so a caller that
// matches an [github.com/iqhive/iqmq.OpError] on its driver name gets "kafka"
// from either. What tells them apart is [ProfileName], which is
// [github.com/iqhive/iqmq.DriverInfo.Profile], and [Scheme], which is the DSN.
const DriverName = "kafka"

// ProfileName is the client library behind this driver, as the specification's
// DriverInfo.Profile and as the conformance report's profile — so the
// evidence for the two Kafka drivers lands in kafka-franz-go.json and
// kafka-kafkago.json rather than overwriting one another.
const ProfileName = "kafkago"

// Version is the version of this driver module. It is empty until the module
// is tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// clientVersion is the client library's version, held here so that the
// capability table and the conformance harness do not import the client for
// one constant.
const clientVersion = "v0.4.51"

// clientModule is that library's module path, held beside the version so the
// pair has ONE source in this package.
const clientModule = "github.com/segmentio/kafka-go"

// DefaultBroker is the seed broker [Config] dials when none is given.
const DefaultBroker = "127.0.0.1:9092"

// The defaults a zero [Config] resolves to. Every one of them is a bound:
// The specification requires every queue and every operation to be bounded by default, so
// none of these may mean "unlimited" when it is left unset.
const (
	// DefaultCommitInterval is how often the driver commits the watermark
	// its trackers have advanced to. It is the width of the at-least-once
	// window in time, and 250 ms is the same compromise the franz-go driver
	// makes: a commit per record would cost a round trip per record, and
	// Kafka's own auto-commit default of five seconds is a five-second
	// window.
	DefaultCommitInterval = 250 * time.Millisecond
	// DefaultRevokeWait bounds how long the driver waits for the in-flight
	// deliveries of an ending generation to settle before it commits the
	// safe boundary and invalidates the generation. It has to be well inside
	// the group's rebalance timeout: kafka-go will not rejoin until every
	// function the generation started has returned, so a wait that overruns
	// the rebalance timeout takes this member out of the group — which is a
	// worse outcome than a few redeliveries.
	DefaultRevokeWait = 5 * time.Second
	// DefaultFetchMaxWait is how long one fetch waits for records. It is
	// kafka-go's ReaderConfig.MaxWait, whose own default is TEN SECONDS —
	// far too long for a driver whose Fetch is a blocking call a caller
	// cancels, because a cancelled Fetch cannot return until the read in
	// flight does.
	DefaultFetchMaxWait = 500 * time.Millisecond
	// DefaultFetchMinBytes is how few bytes a fetch will return with. One,
	// which is "return as soon as there is anything": a larger floor trades
	// latency for batch size and is a caller's choice.
	DefaultFetchMinBytes = 1
	// DefaultFetchMaxBytes bounds one fetch response. It is kafka-go's own
	// default and the broker's usual message.max.bytes.
	DefaultFetchMaxBytes = 1 << 20
	// DefaultQueueCapacity is the driver's own read-ahead: the floor on how
	// many fetched records the source holds for a caller that has not asked
	// for them yet. It is the specification's "all queues bounded" for the consumption
	// side, and it is the driver's because the consume path fetches directly
	// rather than through a [kgo.Reader] whose buffer it would be.
	DefaultQueueCapacity = 100
	// DefaultSessionTimeout is the group session timeout.
	DefaultSessionTimeout = 45 * time.Second
	// DefaultRebalanceTimeout is how long the coordinator waits for a member
	// to rejoin.
	DefaultRebalanceTimeout = 60 * time.Second
	// DefaultHeartbeatInterval is the group heartbeat interval.
	DefaultHeartbeatInterval = 3 * time.Second
	// DefaultJoinBackoff is how long kafka-go waits before rejoining after a
	// failed generation. kafka-go's own default is five seconds, which makes
	// every transient coordinator error a five-second outage for the
	// subscription; one second is still a backoff and is inside the session
	// timeout.
	DefaultJoinBackoff = time.Second
	// DefaultPartitionWatchInterval is how often the group re-reads the
	// topic's partition list when [Config.WatchPartitionChanges] is on.
	DefaultPartitionWatchInterval = 5 * time.Second
	// DefaultBatchMaxRecords is how many records the writer accumulates
	// before it produces, and it is ONE.
	//
	// kafka-go has no immediate-flush path: a batch is produced when it is
	// full or when its timer fires, and there is no "the caller has stopped
	// publishing, send what you have". So a synchronous Publish with a batch
	// size above one waits out BatchTimeout on every publication — with
	// kafka-go's own defaults (batch 100, timer 1 s) a synchronous publisher
	// manages ONE MESSAGE A SECOND. That is the same failure a five
	// millisecond franz-go linger produced  and an order
	// of magnitude worse. A batch of one flushes at once and pays no timer,
	// which is what a publisher that waits for the reply wants; a caller
	// that publishes in batches raises it and pays the timer.
	DefaultBatchMaxRecords = 1
	// DefaultBatchTimeout is how long a partially filled batch waits. It is
	// only reachable when [Config.BatchMaxRecords] is above one, and it is
	// then the publication latency floor.
	DefaultBatchTimeout = 10 * time.Millisecond
	// DefaultMaxAttempts is how many times the writer tries one batch.
	//
	// It is also, on this client, THE NUMBER OF DUPLICATES ONE PUBLICATION
	// MAY PRODUCE: kafka-go sends no producer id, so the broker cannot
	// de-duplicate a retry whose predecessor was written and whose response
	// was lost. Three is kafka-go's documented ten
	// lowered to what a bounded publication can afford, and a deployment
	// that cannot tolerate a duplicate sets it to one and reconciles the
	// ambiguous outcome itself.
	DefaultMaxAttempts = 3
	// DefaultWriteTimeout bounds ONE produce round trip inside the writer.
	// It is not the caller's bound — the caller's context is — and it is the
	// only bound a publication made with no deadline has.
	DefaultWriteTimeout = 10 * time.Second
	// DefaultMetadataTTL is how long kafka-go's transport serves a cached
	// cluster layout before it re-discovers one, and on this client it is
	// ALSO how long a publication keeps failing after a leader election.
	//
	// kafka-go's transport refreshes metadata on a timer — randomised between
	// zero and this value — and on nothing else: a produce answered
	// NOT_LEADER_FOR_PARTITION is retried against the SAME stale leader,
	// because `refreshMetadata` is reached only after a CreateTopics.
	// franz-go refreshes on the error itself, so the same leader election
	// costs it one retry and costs this client every attempt inside the
	// window. kafka-go's own default is six seconds, which is up to six
	// seconds of definite publication failures after an ordinary rolling
	// restart — which is what TestCriticalLeaderChange found, on the first
	// run, as a NOT_LEADER_FOR_PARTITION reaching the caller.
	//
	// One second costs one metadata request per second per transport and
	// bounds the window to about half a second on average. A deployment that
	// would rather pay the requests lowers it further; one that never moves a
	// leader raises it.
	DefaultMetadataTTL = time.Second
	// DefaultWriteBackoffMin and DefaultWriteBackoffMax are the delays
	// between the writer's own attempts at one batch, and they are chosen so
	// that the attempts SPAN DefaultMetadataTTL.
	//
	// kafka-go computes the delay as `attempt² × min`, capped at max, so
	// three attempts at 250 ms and 1 s wait 1.25 s in total — long enough for
	// the transport's discovery loop to have replaced the stale layout, which
	// is the only thing that lets the retry find the new leader. kafka-go's
	// own defaults (100 ms and 1 s) wait 500 ms, which is inside a
	// six-second window and therefore always fails.
	DefaultWriteBackoffMin = 250 * time.Millisecond
	DefaultWriteBackoffMax = time.Second
	// DefaultPartitions is how many partitions the typed administration
	// creates a topic with.
	DefaultPartitions = 1
	// DefaultReplicationFactor is the replication factor the typed
	// administration creates a topic with; -1 asks for the cluster's own
	// default.
	DefaultReplicationFactor = -1
	// DefaultRequestTimeout bounds one administrative or commit round trip
	// whose caller gave no deadline.
	DefaultRequestTimeout = 30 * time.Second
)

// SASLMechanism names the SASL mechanism a [Config] authenticates with.
type SASLMechanism uint8

// The SASL mechanisms this driver builds from a typed configuration or a DSN.
// Anything else — OAUTHBEARER with a token source, GSSAPI, a custom mechanism
// — is reached through [OpenWithWriter], where the caller builds the
// *kafka.Transport and iqmq borrows the writer.
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

// GroupBalancer names the partition assignor this driver offers the
// coordinator.
//
// Both of them are EAGER, and that is the whole of what this type has to say:
// kafka-go has no cooperative assignor, so whichever one is chosen, a
// rebalance revokes the entire assignment and hands it back. The franz-go
// driver uses cooperative-sticky and keeps the partitions that did not move.
type GroupBalancer uint8

// The assignors kafka-go implements that this driver offers. Rack affinity is
// not offered: it needs a rack id this driver has no field for, and a
// misconfigured rack silently starves a member.
const (
	// BalancerRange gives each member a contiguous run of each topic's
	// partitions. It is kafka-go's own first default and the Java client's.
	BalancerRange GroupBalancer = iota
	// BalancerRoundRobin deals the partitions out one at a time, which
	// balances better across topics and moves more partitions per
	// rebalance.
	BalancerRoundRobin

	balancerCount
)

var balancerNames = [balancerCount]string{"range", "roundrobin"}

// String returns the assignor's lower-case name, as a DSN spells it.
func (b GroupBalancer) String() string {
	if b < balancerCount {
		return balancerNames[b]
	}
	return "GroupBalancer(?)"
}

// Valid reports whether b names an assignor.
func (b GroupBalancer) Valid() bool { return b < balancerCount }

// GroupBalancerNames are the assignor names a DSN accepts, in enumeration
// order.
func GroupBalancerNames() []string { return balancerNames[:] }

// Config is the typed configuration of the kafka-go Kafka driver.
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
	// SASL is the mechanism to authenticate with. It is required when a user
	// name is given and refused when one is not.
	SASL SASLMechanism
	// TLS is the TLS configuration. A non-nil value turns TLS on.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks so that it is greppable and obviously unsafe.
	InsecureSkipVerify bool

	// Balancer is the partition assignor offered to the coordinator. The zero
	// value is BalancerRange.
	Balancer GroupBalancer

	// BatchMaxRecords is how many records the writer accumulates per
	// partition before it produces. Zero means DefaultBatchMaxRecords, which
	// is one: see that constant for why a larger default makes a synchronous
	// publisher pay BatchTimeout per message.
	BatchMaxRecords int
	// BatchTimeout is how long a partially filled batch waits. Zero means
	// DefaultBatchTimeout. It is unreachable while BatchMaxRecords is one.
	BatchTimeout time.Duration
	// BatchMaxBytes bounds one produced batch. Zero means kafka-go's own
	// default, which is the broker's usual message.max.bytes.
	BatchMaxBytes int64
	// MaxAttempts is how many times the writer tries one batch before it
	// gives up. Zero means DefaultMaxAttempts. It is also the number of
	// copies one publication may leave in the log, because this client has
	// no idempotent producer. Above one, a retriable answer the writer gives
	// up on is reported as ambiguous, because kafka-go keeps only the last
	// attempt's answer and an earlier attempt may have been appended. A
	// writer handed to [OpenWithWriter] makes its own MaxAttempts attempts,
	// and the driver counts those instead.
	MaxAttempts int
	// WriteTimeout bounds one produce round trip inside the writer. Zero
	// means DefaultWriteTimeout.
	WriteTimeout time.Duration
	// MetadataTTL is how long the transport serves a cached cluster layout.
	// Zero means DefaultMetadataTTL. On this client it is also the width of
	// the window in which a publication fails after a leader election, which
	// is why the default is a second rather than kafka-go's six.
	MetadataTTL time.Duration
	// WriteBackoffMin and WriteBackoffMax bound the delay between the
	// writer's attempts at one batch. Zero selects the defaults above, which
	// are chosen so that MaxAttempts attempts span MetadataTTL.
	WriteBackoffMin time.Duration
	WriteBackoffMax time.Duration
	// AutoCreateTopics lets a produce or a metadata request create a topic
	// the cluster does not have. It is off by default, which is the rule
	// that a driver creates no topology unless it is asked to.
	AutoCreateTopics bool

	// FetchMinBytes is how few bytes a fetch will return with. Zero means
	// DefaultFetchMinBytes.
	//
	// Zero here means "the default, UNLESS the partner below was written":
	// the two are reconciled so that the explicit one wins and the defaulted
	// one yields, which is what keeps a one-sided setting working and keeps
	// the resolved pair self-consistent. A pair whose
	// BOTH sides are written and inconsistent is refused.
	// So "FetchMinBytes: 2 MiB" alone yields a FetchMaxBytes of 2 MiB rather
	// than the 1 MiB default.
	FetchMinBytes int
	// FetchMaxBytes bounds one fetch response. Zero means
	// DefaultFetchMaxBytes, subject to the reconciliation described on
	// FetchMinBytes.
	FetchMaxBytes int
	// FetchMaxWait is how long one fetch waits for records. Zero means
	// DefaultFetchMaxWait.
	FetchMaxWait time.Duration
	// QueueCapacity is the floor on the source's own bounded record queue —
	// the read-ahead every partition of a subscription shares. The channel is
	// the larger of this and the core's resolved prefetch, so a caller that
	// asks for a wide batch always has room for one. Zero means
	// DefaultQueueCapacity.
	QueueCapacity int
	// ReadCommitted makes the consumer skip records of aborted and open
	// transactions. It is off by default and is what a consumer of a
	// transactional producer's topic wants. This driver cannot PRODUCE
	// transactionally — kafka-go's writer has no transactional id — but it
	// can read a topic another producer wrote transactionally.
	ReadCommitted bool

	// SessionTimeout, RebalanceTimeout, HeartbeatInterval and JoinBackoff
	// are the group's own timings. Zero selects the defaults above.
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	HeartbeatInterval time.Duration
	JoinBackoff       time.Duration
	// WatchPartitionChanges makes the group rebalance when the topic gains a
	// partition. It is off by default: a rebalance is eager on this client,
	// so a poll that notices a new partition stops every member's
	// consumption, and a topic that does not grow pays nothing for the
	// polling either way.
	WatchPartitionChanges bool
	// PartitionWatchInterval is how often that poll happens. Zero means
	// DefaultPartitionWatchInterval.
	PartitionWatchInterval time.Duration

	// CommitInterval is how often the driver commits the watermark its
	// trackers have advanced to. Zero means DefaultCommitInterval; a
	// negative value means the driver commits only when a generation ends,
	// at Close and when [Backend.Commit] is called.
	CommitInterval time.Duration
	// RevokeWait bounds the wait for in-flight deliveries when a generation
	// ends. Zero means DefaultRevokeWait.
	RevokeWait time.Duration

	// Tracker is the offset-tracker kind (ADR-019). The zero
	// value is Auto, which resolves to the bake-off winner.
	Tracker offset.Kind
	// TrackerWindow, TrackerMaxPending and TrackerMaxGap bound one
	// partition's tracking window and decide when the driver stops reading a
	// partition instead of buffering. Zero selects the tracker's own
	// defaults.
	TrackerWindow     int
	TrackerMaxPending int
	TrackerMaxGap     int

	// Nack is the negative-settlement strategy. The zero value
	// is Auto, which resolves to block-partition: the only kind that changes
	// neither ordering nor durability.
	Nack nack.Kind
	// NackConfig configures that strategy — the attempt limit, the
	// dead-letter topic, the number of retry topics, the redelivery delay
	// and, for the custom kind, the decision function.
	NackConfig nack.Config

	// Partitions and ReplicationFactor are what the typed administration
	// creates a topic with. Zero selects the defaults above.
	Partitions        int32
	ReplicationFactor int16
	// RequestTimeout bounds one administrative or commit round trip whose
	// caller gave no deadline of its own. It is never a second deadline on a
	// caller that has one.
	RequestTimeout time.Duration

	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks. Only OnReconnect is fired
	// from here — kafka-go owns its own dial loop and offers no connection
	// hook at all, so this driver reports a reconnection when a generation
	// is re-established rather than when a socket is.
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
	if c.BatchMaxRecords == 0 {
		c.BatchMaxRecords = DefaultBatchMaxRecords
	}
	if c.BatchTimeout == 0 {
		c.BatchTimeout = DefaultBatchTimeout
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultMaxAttempts
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.MetadataTTL == 0 {
		c.MetadataTTL = DefaultMetadataTTL
	}
	if c.WriteBackoffMin == 0 {
		c.WriteBackoffMin = DefaultWriteBackoffMin
	}
	if c.WriteBackoffMax == 0 {
		c.WriteBackoffMax = DefaultWriteBackoffMax
	}
	// The fetch bounds are reconciled rather than defaulted independently:
	// "fetchminbytes=2097152" alone used to pass Validate (which read
	// FetchMaxBytes's zero as "not set") and then render
	// "fetchminbytes=2097152&fetchmaxbytes=1048576", which its own parser
	// refused -- the third instance of the idiom the specification records,
	// and the one the first round's numeric axis missed because its "large"
	// value was exactly DefaultFetchMaxBytes.
	c.FetchMinBytes, c.FetchMaxBytes = reconcileBounds(
		c.FetchMinBytes, c.FetchMaxBytes, DefaultFetchMinBytes, DefaultFetchMaxBytes,
	)
	if c.FetchMaxWait == 0 {
		c.FetchMaxWait = DefaultFetchMaxWait
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = DefaultQueueCapacity
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
	if c.JoinBackoff == 0 {
		c.JoinBackoff = DefaultJoinBackoff
	}
	if c.PartitionWatchInterval == 0 {
		c.PartitionWatchInterval = DefaultPartitionWatchInterval
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
	return c
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
	if !printableText(c.ClientID) || !printableText(c.Username) {
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
	case !c.Balancer.Valid():
		return errBadBalancer
	}
	switch {
	case c.FetchMaxWait < 0 || c.SessionTimeout < 0 || c.RebalanceTimeout < 0 ||
		c.HeartbeatInterval < 0 || c.JoinBackoff < 0 || c.RevokeWait < 0 ||
		c.RequestTimeout < 0 || c.WriteTimeout < 0 || c.BatchTimeout < 0 ||
		c.PartitionWatchInterval < 0 || c.MetadataTTL < 0 ||
		c.WriteBackoffMin < 0 || c.WriteBackoffMax < 0:
		return errNegativeDuration
	case c.BatchMaxBytes < 0 || c.BatchMaxRecords < 0 || c.FetchMinBytes < 0 ||
		c.FetchMaxBytes < 0 || c.QueueCapacity < 0 || c.MaxAttempts < 0:
		return errNegativeBound
	case c.Partitions < -1 || c.ReplicationFactor < -1:
		return errBadTopology
	case c.FetchMinBytes > 0 && c.FetchMaxBytes > 0 && c.FetchMinBytes > c.FetchMaxBytes:
		// kafka-go panics on this rather than refusing it, inside
		// NewReader, from a goroutine the driver started. The specification requires the
		// refusal here.
		return errFetchBounds
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
// It is the same rule NATS took for the same reason  and the
// franz-go driver repeats: a broker address goes into the dial and into every
// rendering of the configuration, so a byte that is not printable ASCII
// produces a log line and an error string that are not valid UTF-8. An
// international host name reaches Kafka as punycode, which is ASCII, so
// nothing legitimate is refused.
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
	q := make([]string, 0, 20)
	q = append(q,
		"clientid="+escapeQuery(cfg.ClientID),
		"balancer="+cfg.Balancer.String(),
		"batchmaxrecords="+strconv.Itoa(cfg.BatchMaxRecords),
		"batchtimeout="+cfg.BatchTimeout.String(),
		"maxattempts="+strconv.Itoa(cfg.MaxAttempts),
		"writetimeout="+cfg.WriteTimeout.String(),
		"metadatattl="+cfg.MetadataTTL.String(),
		"writebackoffmin="+cfg.WriteBackoffMin.String(),
		"writebackoffmax="+cfg.WriteBackoffMax.String(),
		"fetchminbytes="+strconv.Itoa(cfg.FetchMinBytes),
		"fetchmaxbytes="+strconv.Itoa(cfg.FetchMaxBytes),
		"fetchmaxwait="+cfg.FetchMaxWait.String(),
		"queuecapacity="+strconv.Itoa(cfg.QueueCapacity),
		"sessiontimeout="+cfg.SessionTimeout.String(),
		"rebalancetimeout="+cfg.RebalanceTimeout.String(),
		"heartbeatinterval="+cfg.HeartbeatInterval.String(),
		"joinbackoff="+cfg.JoinBackoff.String(),
		"commitinterval="+cfg.CommitInterval.String(),
		"revokewait="+cfg.RevokeWait.String(),
		"requesttimeout="+cfg.RequestTimeout.String(),
		"tracker="+cfg.Tracker.String(),
		"nack="+cfg.Nack.String(),
		// Unconditional, because the parser accepts it on its own: it used
		// to be written only beside watchpartitionchanges, so
		// "partitionwatchinterval=1h" parsed and then rendered as the 5s
		// default.
		"partitionwatchinterval="+cfg.PartitionWatchInterval.String(),
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
	if cfg.AutoCreateTopics {
		q = append(q, "autocreatetopics=true")
	}
	if cfg.ReadCommitted {
		q = append(q, "readcommitted=true")
	}
	if cfg.WatchPartitionChanges {
		q = append(q, "watchpartitionchanges=true")
	}
	if cfg.BatchMaxBytes > 0 {
		q = append(q, "batchmaxbytes="+strconv.FormatInt(cfg.BatchMaxBytes, 10))
	}
	if cfg.NackConfig.DeadLetterTopic != "" {
		q = append(q, "deadlettertopic="+escapeQuery(cfg.NackConfig.DeadLetterTopic))
	}
	if cfg.NackConfig.MaxAttempts != 0 {
		q = append(q, "nackmaxattempts="+strconv.FormatUint(uint64(cfg.NackConfig.MaxAttempts), 10))
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
	errNoBroker           = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a seed broker in Brokers is empty")
	errBadBroker          = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a seed broker in Brokers is not printable ASCII; an international host name is dialled as punycode")
	errBadText            = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a configured identifier carries a control character or is not valid UTF-8")
	errBadPrefix          = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: TopicPrefix is not made of characters a Kafka topic name accepts")
	errBadSASL            = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: SASL is not one of the mechanisms this driver builds")
	errSASLWithoutUser    = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a SASL mechanism needs a user name")
	errUserWithoutSASL    = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a user name needs a SASL mechanism; name one rather than letting the driver guess")
	errInsecureWithoutTLS = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: InsecureSkipVerify has no meaning without TLS")
	errBadBalancer        = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: Balancer is not one of the assignors kafka-go implements")
	errNegativeDuration   = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a timeout cannot be negative")
	errNegativeBound      = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: a byte or record bound cannot be negative")
	errBadTopology        = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: Partitions and ReplicationFactor are positive, or -1 for the cluster's default")
	errFetchBounds        = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: FetchMinBytes is above FetchMaxBytes; kafka-go panics on that inside NewReader rather than refusing it")
	errBadTracker         = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: Tracker is not one of the kinds")
	errBadDeadLetter      = errcode.New(errcode.InvalidConfig, "iqmq/kafkago: the dead-letter topic is not a valid Kafka topic name")
	errNilRegistrar       = errcode.New(errcode.InvalidArgument, "iqmq/kafkago: Register was given a nil Registrar")
	errNilWriter          = errcode.New(errcode.InvalidArgument, "iqmq/kafkago: OpenWithWriter was given a nil writer")
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

// reconcileBounds is the shared shape of the four "zero is unset vs zero is
// the default" defects the specification records.
//
// A cross-field check in Validate reads a zero as "not set", while resolve and
// [Config.DSN] read the same zero as "the default" -- so a configuration whose
// EFFECTIVE values are exactly what the check refuses passed it, and its own
// rendering then did not parse. Comparing the effective values in Validate
// closes that but over-refuses, turning a one-sided setting whose partner is
// merely defaulted into an error, and those are ordinary intents.
//
// So the EXPLICIT value wins and the DEFAULTED one yields: a defaulted low
// above an explicit high is pulled down to it, and a defaulted high below an
// explicit low is pushed up. Validate then refuses only a contradiction the
// operator actually wrote -- both sides non-zero and inconsistent, which is
// what it refused before this branch -- and the resolved pair is always
// self-consistent, which is what makes the rendering parse back.
func reconcileBounds[T ~int | ~int64](low, high, defLow, defHigh T) (T, T) {
	lowSet, highSet := low != 0, high != 0
	if !lowSet {
		low = defLow
	}
	if !highSet {
		high = defHigh
	}
	switch {
	case !lowSet && low > high:
		low = high
	case !highSet && high < low:
		high = low
	}
	return low, high
}

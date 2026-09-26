package gcppubsub

import (
	"log/slog"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DriverName is the driver's registered name and the scheme its DSN uses.
const DriverName = "gcppubsub"

// Version is the version of this driver module. It is empty until the module
// is tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// The service limits, read from the pinned client library and from the
// service's published quotas at that version, and recorded HERE rather than in
// the portable API — which is what the specification asks of a driver: "service limits
// discovered/documented from the tested API version, not hard-coded into the
// portable API".
//
// Every one of them is quoted with its source in docs/drivers/gcppubsub.md's
// "Verified facts" section, and every one is re-verified when the client pin
// moves.
const (
	// MaxMessageBytes is the largest publish request the service accepts,
	// which is also the largest single message: the library's
	// pubsub.MaxPublishRequestBytes.
	MaxMessageBytes = 10_000_000
	// MaxBatchMessages is the most messages one publish request carries:
	// the library's pubsub.MaxPublishRequestCount.
	MaxBatchMessages = 1000
	// MaxAttributes is the most attributes one message may carry.
	MaxAttributes = 100
	// MaxAttributeKeyBytes and MaxAttributeValueBytes bound one attribute.
	// The key limit is also what a header key is bounded by, and the value
	// limit is FeatureNativeHeaders' MaxHeaderBytes.
	MaxAttributeKeyBytes   = 256
	MaxAttributeValueBytes = 1024
	// MaxOrderingKeyBytes is the longest ordering key the service accepts.
	MaxOrderingKeyBytes = 1024
	// MinAckDeadline and MaxAckDeadline bound ackDeadlineSeconds on the
	// MANAGED service. The emulator accepts anything from one second, which
	// is a difference this driver does not paper over: see
	// [Config.StrictAckDeadline] and.
	MinAckDeadline = 10 * time.Second
	MaxAckDeadline = 600 * time.Second
	// MinRetention and MaxRetention bound messageRetentionDuration.
	MinRetention = 10 * time.Minute
	MaxRetention = 31 * 24 * time.Hour
	// MaxExtensionCeiling is the longest total automatic ack-deadline
	// extension the library will perform. It is the real bound on how long a
	// handler may hold a delivery, because the driver cannot extend a lease
	// itself (see [Backend] and).
	MaxExtensionCeiling = 60 * time.Minute
	// ReservedAttributePrefix is the attribute-name prefix the service
	// reserves for itself. A header whose key starts with it is refused
	// before any I/O.
	ReservedAttributePrefix = "goog"
)

// The defaults a zero [Config] resolves to.
const (
	// DefaultAckDeadline is ackDeadlineSeconds for a subscription this
	// driver creates and the lease a source reports when the consumer named
	// none. It is the service's own default.
	DefaultAckDeadline = 10 * time.Second
	// DefaultMaxExtension is how long the client library keeps a held
	// message's ack deadline alive automatically. It is deliberately much
	// shorter than the library's own hour: a lease nobody can end is a
	// message no other consumer can take, and the specification wants the window a
	// handler holds to be something an operator chose.
	DefaultMaxExtension = 10 * time.Minute
	// DefaultPublishDelayThreshold and DefaultPublishCountThreshold are the
	// publisher bundler's own defaults, restated so that this driver's
	// behaviour does not change under the client library.
	DefaultPublishDelayThreshold = 10 * time.Millisecond
	DefaultPublishCountThreshold = 100
	// DefaultPublishTimeout bounds one publish RPC inside the bundler.
	DefaultPublishTimeout = 60 * time.Second
	// DefaultAPITimeout bounds one administration call whose caller gave no
	// deadline of its own. It is never a second deadline on a caller that
	// has one.
	DefaultAPITimeout = 30 * time.Second
	// DefaultPrefetch is MaxOutstandingMessages when the consumer named no
	// prefetch. It is the ring's capacity too.
	DefaultPrefetch = 64
	// DefaultCompletionWorkers is how many goroutines drain
	// PublishResult.Get for publications nobody is waiting on. It is a FIXED
	// pool: the specification rule 3 forbids a goroutine per message, and the naive
	// asynchronous publish path on this library is exactly that.
	DefaultCompletionWorkers = 4
	// DefaultCompletionQueue is how many pending completions the pool holds
	// before an asynchronous publication waits for room. The specification requires
	// every queue to be bounded; this is that bound.
	DefaultCompletionQueue = 1024
)

// Config is the driver's configuration.
//
// The zero value is not usable: [Config.ProjectID] is required, because a
// Pub/Sub client is bound to one project and a driver that guessed would talk
// to whichever project the ambient environment named.
type Config struct {
	// ProjectID is the Google Cloud project. It is required.
	ProjectID string

	// Endpoint overrides the service endpoint. It is how the emulator is
	// reached ("127.0.0.1:8085"); empty means the real service.
	Endpoint string
	// InsecureTransport dials the endpoint without TLS and without
	// credentials, which is what an emulator needs and what a real endpoint
	// must never be given. It is named for what it costs,.
	InsecureTransport bool

	// CredentialsJSON is a SERVICE-ACCOUNT key, and only that. It is a
	// [github.com/iqhive/iqmq/dsn.Secret], so neither [Config.String] nor
	// [Config.LogValue] can render it.
	//
	// The credential TYPE is pinned, which is not a limitation for its own
	// sake: the client library's untyped credential options accept any
	// configuration, including an externally-sourced one whose URLs decide
	// where a token is fetched from, and are deprecated for that reason. A
	// deployment using an authorized user, workload identity federation, an
	// impersonated service account or a token source of its own builds the
	// client itself and hands it to [OpenWithClient].
	CredentialsJSON dsn.Secret
	// CredentialsFile is a path to a service-account key file, under the same
	// restriction as [Config.CredentialsJSON]. It is the lighter half of the
	// same escape hatch; [OpenWithClient] is the heavier one.
	CredentialsFile string

	// TopicPrefix and SubscriptionPrefix are prepended to every topic and
	// subscription ID this driver derives from a portable name, which is how
	// two deployments share one project.
	TopicPrefix        string
	SubscriptionPrefix string
	// SubscriptionIDs overrides the derived subscription identifier for one
	// (topic, subscription) pair, keyed by "<topic>/<subscription>". It is
	// the escape hatch for a deployment whose subscriptions are named by
	// something other than this driver — Terraform, a naming policy, a
	// subscription that already exists.
	SubscriptionIDs map[string]string

	// Ordering enables ordering keys. It is a CONSTRUCTION flag and not a
	// per-publication one because the library's is: a publisher built
	// without EnableMessageOrdering refuses every publication carrying an
	// ordering key, so the capability is conditional on this field
	// (the specification requires the condition to be stated, and capabilities() states
	// it).
	Ordering bool

	// AckDeadline is ackDeadlineSeconds for subscriptions this driver
	// creates, and the lease a source reports when the consumer named none.
	AckDeadline time.Duration
	// StrictAckDeadline refuses an acknowledgement deadline outside the
	// MANAGED service's ten-to-six-hundred-second range before any I/O.
	//
	// It is off by default and that is deliberate. The emulator accepts one
	// second, several scenarios ask for five hundred milliseconds, and
	// a driver that refused them locally would report "unsupported" for a
	// topology the broker under test builds happily. On the managed service
	// the range is enforced by the service itself, with an InvalidArgument
	// this driver maps and reports. Turn it on to find out at construction
	// instead.
	StrictAckDeadline bool
	// MaxExtension is how long the client library keeps extending a held
	// message's ack deadline. It is this driver's whole lease-extension
	// story; see [Config.AckDeadline] and docs/drivers/gcppubsub.md.
	MaxExtension time.Duration

	// Prefetch is MaxOutstandingMessages and the bounded ring's capacity
	// when a consumer names none.
	Prefetch int
	// MaxOutstandingBytes is the library's byte-side flow control. Zero
	// leaves the library's own default.
	MaxOutstandingBytes int

	// The publisher bundler. Zero values resolve to the Default* constants
	// above rather than to the library's, so that this driver's publication
	// behaviour is this driver's.
	// PublishCountThreshold and PublishByteThreshold are the bundle sizes
	// that flush a batch; PublishDelayThreshold is the linger that flushes a
	// batch that has not reached either.
	PublishCountThreshold int
	PublishByteThreshold  int
	PublishDelayThreshold time.Duration
	// PublishTimeout bounds one bundle's publish RPC, after which the
	// publication is ambiguous in the sense rather than failed.
	PublishTimeout time.Duration
	// PublishGoroutines is how many bundles the library may have in flight
	// at once.
	PublishGoroutines int
	// PublishFlowMaxOutstandingMessages and PublishFlowMaxOutstandingBytes
	// bound what the bundler holds. Zero leaves the library's default.
	PublishFlowMaxOutstandingMessages int
	PublishFlowMaxOutstandingBytes    int
	// Compression turns on the library's publish-side compression, above
	// CompressionThreshold bytes.
	Compression          bool
	CompressionThreshold int

	// CompletionWorkers and CompletionQueue size the fixed completion-worker
	// pool that drains PublishResult.Get for publications nobody is waiting
	// on.
	CompletionWorkers int
	CompletionQueue   int

	// ExactlyOnce creates subscriptions with enableExactlyOnceDelivery and
	// settles through AckWithResult, so that a settlement the service
	// refused is reported rather than swallowed.
	//
	// It does NOT make delivery exactly-once in the sense and this driver
	// never claims that it does: an unacknowledged message whose deadline
	// expires is still redelivered. What it buys is a settlement with an
	// OUTCOME.
	ExactlyOnce bool

	// Filter is a server-side filter expression applied to subscriptions
	// this driver creates. Empty means none.
	Filter string

	// RetainAcked and Retention are retainAckedMessages and
	// messageRetentionDuration for subscriptions this driver creates. They
	// are what a seek back in time has to read.
	RetainAcked bool
	Retention   time.Duration

	// DeadLetterTopic and MaxDeliveryAttempts are the dead-letter policy for
	// subscriptions this driver creates. MaxDeliveryAttempts is also what
	// makes DeliveryAttempt present on a delivery at all, which is why
	// FeatureDeadLetter and the attempt count are one condition rather than
	// two.
	DeadLetterTopic     string
	MaxDeliveryAttempts int

	// AutoCreate creates a topic or a subscription this driver is asked for
	// and cannot find. It is off by default: the specification makes provisioning opt-in,
	// and a driver that created a subscription for a typo would leave a
	// backlog nobody reads and a bill nobody expects.
	AutoCreate bool

	// APITimeout bounds one administration call whose caller gave no
	// deadline of its own.
	APITimeout time.Duration

	// Clock is the clock this driver reads. Nil means the real one.
	Clock mq.Clock
	// Hooks are the observation points, carried to the client.
	Hooks mq.Hooks
}

// Validate reports whether the configuration is usable. It is pure: no I/O, no
// clock, no environment.
func (c Config) Validate() error {
	if c.ProjectID == "" {
		return errNoProject
	}
	if strings.ContainsAny(c.ProjectID, "/ \t") {
		return errBadProject
	}
	if c.AckDeadline < 0 || c.MaxExtension < 0 || c.Retention < 0 ||
		c.APITimeout < 0 || c.PublishDelayThreshold < 0 || c.PublishTimeout < 0 {
		return errNegativeDuration
	}
	if c.Prefetch < 0 || c.CompletionWorkers < 0 || c.CompletionQueue < 0 ||
		c.MaxDeliveryAttempts < 0 || c.MaxOutstandingBytes < 0 ||
		c.PublishCountThreshold < 0 || c.PublishByteThreshold < 0 {
		return errNegativeCount
	}
	if c.AckDeadline > MaxAckDeadline {
		return errAckDeadlineTooLong
	}
	if c.StrictAckDeadline && c.AckDeadline > 0 && c.AckDeadline < MinAckDeadline {
		return errAckDeadlineTooShort
	}
	if c.MaxExtension > MaxExtensionCeiling {
		return errMaxExtensionTooLong
	}
	if c.Retention > 0 && (c.Retention < MinRetention || c.Retention > MaxRetention) {
		return errRetentionRange
	}
	if c.DeadLetterTopic != "" && (c.MaxDeliveryAttempts < 5 || c.MaxDeliveryAttempts > 100) {
		// The service's own range for maxDeliveryAttempts. A policy outside
		// it is refused at CreateSubscription, so refusing it here turns a
		// deployment mistake into a construction failure.
		return errMaxDeliveryAttempts
	}
	if c.DeadLetterTopic == "" && c.MaxDeliveryAttempts != 0 {
		// An attempt count with nowhere to send the message names nothing:
		// there is no dead-letter policy to put it in, so the number would be
		// carried and silently ignored. FuzzV1CompatDSN found it through the
		// DSN's rendering, which dropped it for the same reason.
		return errAttemptsWithoutDeadLetter
	}
	if c.CredentialsJSON != "" && c.CredentialsFile != "" {
		return errTwoCredentials
	}
	if c.InsecureTransport && c.Endpoint == "" {
		return errInsecureWithoutEndpoint
	}
	if err := checkID("topic prefix", c.TopicPrefix); err != nil {
		return err
	}
	return checkID("subscription prefix", c.SubscriptionPrefix)
}

// resolve fills the zero values in, so that every reader of a Config sees one
// set of numbers.
func (c Config) resolve() Config {
	if c.AckDeadline <= 0 {
		c.AckDeadline = DefaultAckDeadline
	}
	if c.MaxExtension <= 0 {
		c.MaxExtension = DefaultMaxExtension
	}
	if c.Prefetch <= 0 {
		c.Prefetch = DefaultPrefetch
	}
	if c.PublishCountThreshold <= 0 {
		c.PublishCountThreshold = DefaultPublishCountThreshold
	}
	if c.PublishDelayThreshold <= 0 {
		c.PublishDelayThreshold = DefaultPublishDelayThreshold
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.CompletionWorkers <= 0 {
		c.CompletionWorkers = DefaultCompletionWorkers
	}
	if c.CompletionQueue <= 0 {
		c.CompletionQueue = DefaultCompletionQueue
	}
	if c.APITimeout <= 0 {
		c.APITimeout = DefaultAPITimeout
	}
	return c
}

// Profile is what DriverInfo.Profile reports: the client generation, which is the arm. It is never empty and it is never hidden, because the two
// generations are two client libraries behind one capability table and a
// caller reading "gcppubsub" needs to know which one answered.
//
// It is resolved from the client at open; this method is the fallback for a
// configuration that has not been opened yet.
func (c Config) Profile() string { return "v2" }

// String renders the configuration with every credential redacted, so a Config
// is safe in a log line or an error.
func (c Config) String() string {
	var b strings.Builder
	b.WriteString("gcppubsub.Config{ProjectID:")
	b.WriteString(strconv.Quote(c.ProjectID))
	if c.Endpoint != "" {
		b.WriteString(" Endpoint:")
		b.WriteString(strconv.Quote(c.Endpoint))
	}
	if c.InsecureTransport {
		b.WriteString(" InsecureTransport:true")
	}
	if c.CredentialsJSON != "" {
		b.WriteString(" CredentialsJSON:")
		b.WriteString(dsn.Redacted)
	}
	if c.CredentialsFile != "" {
		b.WriteString(" CredentialsFile:")
		b.WriteString(strconv.Quote(c.CredentialsFile))
	}
	if c.TopicPrefix != "" {
		b.WriteString(" TopicPrefix:")
		b.WriteString(strconv.Quote(c.TopicPrefix))
	}
	if c.SubscriptionPrefix != "" {
		b.WriteString(" SubscriptionPrefix:")
		b.WriteString(strconv.Quote(c.SubscriptionPrefix))
	}
	if c.Ordering {
		b.WriteString(" Ordering:true")
	}
	if c.ExactlyOnce {
		b.WriteString(" ExactlyOnce:true")
	}
	if c.DeadLetterTopic != "" {
		b.WriteString(" DeadLetterTopic:")
		b.WriteString(strconv.Quote(c.DeadLetterTopic))
	}
	b.WriteByte('}')
	return b.String()
}

// GoString is String, so that %#v cannot print a credential either.
func (c Config) GoString() string { return c.String() }

// LogValue is String for log/slog, for the same reason.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// The two interfaces a credential-bearing configuration must render through,
// asserted so that a field added without a rendering is a build failure.
type (
	stringer  interface{ String() string }
	logValuer interface{ LogValue() slog.Value }
)

// compile-time proof that Config renders through both of them.
var (
	_ stringer  = Config{}
	_ logValuer = Config{}
)

// checkID refuses a topic or subscription identifier the service would.
//
// The grammar is the service's: 3 to 255 characters, starting with a letter,
// and made of letters, digits and the punctuation below. A prefix is checked
// against the character set but not the length, because a prefix is not an
// identifier on its own.
func checkID(what, s string) error {
	if s == "" {
		return nil
	}
	for i := range len(s) {
		if !idByte(s[i]) {
			return errcode.New(errcode.InvalidConfig,
				"iqmq/gcppubsub: the "+what+" contains a character the service's resource-name grammar does not allow")
		}
	}
	return nil
}

// idByte reports whether b may appear in a Pub/Sub resource identifier.
func idByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	case b == '-' || b == '_' || b == '.' || b == '~' || b == '+' || b == '%':
		return true
	default:
		return false
	}
}

// The configuration's refusals. The specification names no sentinel for a bad
// configuration and errors.Is matches by code, so a caller compares
// mq.CodeOf(err) against mq.CodeInvalidConfig.
var (
	errNoProject = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: ProjectID is required; a Pub/Sub client is bound to one project and this driver will not guess which")
	errBadProject = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: ProjectID is a bare project identifier, not a resource path")
	errNegativeDuration = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: a negative duration names no setting")
	errNegativeCount = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: a negative count names no setting")
	errAckDeadlineTooLong = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the acknowledgement deadline is longer than the service's ten minutes")
	errAckDeadlineTooShort = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: StrictAckDeadline is set and the acknowledgement deadline is shorter than the managed service's ten seconds")
	errMaxExtensionTooLong = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: MaxExtension is longer than the client library's one-hour ceiling")
	errRetentionRange = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: the retention period is outside the service's ten minutes to thirty-one days")
	errMaxDeliveryAttempts = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: a dead-letter policy needs MaxDeliveryAttempts between 5 and 100, which is the service's own range")
	errAttemptsWithoutDeadLetter = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: MaxDeliveryAttempts names nothing without a DeadLetterTopic: there is no dead-letter policy to put it in")
	errTwoCredentials = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: CredentialsJSON and CredentialsFile name two credentials; set one, or neither and use OpenWithClient")
	errInsecureWithoutEndpoint = errcode.New(errcode.InvalidConfig,
		"iqmq/gcppubsub: InsecureTransport without an Endpoint would dial the managed service without TLS; name the emulator's endpoint")
)

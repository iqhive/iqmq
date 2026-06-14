package sqs

import (
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/driverkit/flush"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// DriverName is the driver's registered name and the scheme its DSN uses.
const DriverName = "sqs"

// Version is the version of this driver module. It is empty until the module is
// tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// ClientModule is the native client this driver wraps, reported through
// [bitbucket.org/iqhive/iqmq.DriverInfo].
const ClientModule = "github.com/aws/aws-sdk-go-v2/service/sqs"

// ClientVersion is the pinned version of that client. Every service limit this
// driver advertises was read out of this version's generated documentation, so
// the two move together (see docs/drivers/sqs.md).
const ClientVersion = "v1.42.29"

// The service limits, read from the pinned SDK's generated documentation and
// recorded here rather than in the portable API, which is what the specification asks of a
// driver: "service limits discovered/documented from the tested API version,
// not hard-coded into the portable API".
//
// Every one of them is quoted with its source in docs/drivers/sqs.md's
// "Verified facts" section, and every one of them is re-verified when
// [ClientVersion] moves.
const (
	// MaxBatchEntries is the entry limit of SendMessageBatch,
	// DeleteMessageBatch and ChangeMessageVisibilityBatch. It is the whole
	// reason this driver has a batch settler: ten is the smallest batch of
	// any broker iqmq speaks to.
	MaxBatchEntries = 10
	// MaxMessageBytes is the largest message the service accepts, and the
	// largest sum of one batch's payloads.
	MaxMessageBytes = 1 << 20
	// MaxAttributes is the most message attributes one message may carry. A
	// message that needs more is enveloped under MetadataAdaptive and
	// refused under MetadataNativeStrict.
	MaxAttributes = 10
	// MaxDelay is the longest per-message timer the service accepts. FIFO
	// queues take no per-message delay at all.
	MaxDelay = 15 * time.Minute
	// MaxVisibility is the longest visibility timeout the service grants.
	MaxVisibility = 12 * time.Hour
	// MaxWaitTime is the longest long poll the service allows.
	MaxWaitTime = 20 * time.Second
	// MaxQueueNameLen is the longest queue name the service accepts,
	// including a FIFO queue's ".fifo" suffix.
	MaxQueueNameLen = 80
)

// The defaults a zero [Config] resolves to.
const (
	// DefaultRegion is the region a configuration that names none is built
	// with. It is not a guess at the caller's region: the SDK needs one to
	// sign a request, and a driver pointed at an emulator has no real
	// region at all.
	DefaultRegion = "us-east-1"
	// DefaultPollers is how many concurrent ReceiveMessage loops one source
	// runs.
	DefaultPollers = 1
	// DefaultWaitTime is the long poll one ReceiveMessage asks for. Twenty
	// seconds is the service's maximum and is what makes an idle consumer
	// cost one request per twenty seconds rather than one per round trip.
	DefaultWaitTime = 20 * time.Second
	// DefaultAckDeadline is the visibility timeout a source asks for when
	// SourceConfig.AckDeadline is zero. It is the service's own default.
	DefaultAckDeadline = 30 * time.Second
	// DefaultRetention is the message retention period a queue this driver
	// creates is given, in the service's own default.
	DefaultRetention = 4 * 24 * time.Hour
	// DefaultAPITimeout bounds one API call whose caller gave no deadline
	// of its own. It is never a second deadline on a caller that has one.
	DefaultAPITimeout = 30 * time.Second
)

// BodyEncoding is what this driver does with a body the service's own
// character set cannot carry (the binary-body policy for this driver).
//
// An SQS body is text: the service accepts only the XML character set
// (#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF), so an
// arbitrary byte string has no native representation at all. iqmq promises a
// []byte body everywhere, and this enumeration is how the promise is kept,
// with all three answers shipped and none of them reached by accident.
type BodyEncoding uint8

// The body encodings.
const (
	// BodyAuto passes a body the service can carry through unchanged and
	// base64-encodes one it cannot, marking the encoded message with the
	// reserved iqmq-enc attribute. It is the zero value: it is the only
	// answer that is both lossless and leaves an ordinary text message
	// readable by a non-iqmq consumer.
	BodyAuto BodyEncoding = iota
	// BodyText refuses a body the service cannot carry, with
	// ErrUnsupported and before any I/O. It is for a deployment whose
	// consumers are not iqmq and that would rather find out at the
	// publication than read base64.
	BodyText
	// BodyBase64 encodes every body, so every message on the queue has the
	// same shape and none of them is readable without decoding. It costs
	// a third of the size limit.
	BodyBase64

	bodyEncodingCount
)

var bodyEncodingNames = [bodyEncodingCount]string{"auto", "text", "base64"}

// String returns the encoding's lower-case name.
func (b BodyEncoding) String() string {
	if b < bodyEncodingCount {
		return bodyEncodingNames[b]
	}
	return "BodyEncoding(?)"
}

// Valid reports whether b names an encoding.
func (b BodyEncoding) Valid() bool { return b < bodyEncodingCount }

// Config is the typed configuration of the SQS driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). The secret access key and the session
// token are [bitbucket.org/iqhive/iqmq/dsn.Secret]s, so neither
// [Config.String] nor a slog record nor an error built from one can carry them
// The access key ID is NOT a secret and is rendered, which is
// what makes a redacted DSN still say which principal it was built for.
//
// [Config.Validate] is pure: it performs no I/O and never calls the service,
// which is the specification's "validate config before starting background work".
type Config struct {
	// Region is the AWS region. Empty means DefaultRegion.
	Region string
	// Endpoint is the full endpoint URL, scheme included, such as
	// "http://127.0.0.1:9324" for ElasticMQ. Empty means the SDK's own
	// regional endpoint for Region.
	Endpoint string
	// AccessKeyID is the access key. It is not a secret and is rendered by
	// String and DSN.
	AccessKeyID string
	// SecretAccessKey is the secret half of the key pair.
	SecretAccessKey dsn.Secret
	// SessionToken is the session token of a temporary credential.
	SessionToken dsn.Secret
	// Credentials is a credential provider the caller built, which is how a
	// deployment reaches the default chain — environment, instance role,
	// SSO — without this module depending on the SDK's config, sts and sso
	// modules. It wins over AccessKeyID and SecretAccessKey when it is set.
	Credentials aws.CredentialsProvider
	// AccountID is the account the queues belong to, used when a queue is
	// addressed by ARN. Empty means the driver resolves an ARN from the
	// queue's own attributes.
	AccountID string
	// QueuePrefix is prepended to every mapped queue name, which is how two
	// iqmq deployments share one account.
	QueuePrefix string
	// FIFO selects the FIFO profile: queue names carry the ".fifo" suffix
	// the service requires, Key becomes MessageGroupId and DedupKey becomes
	// MessageDeduplicationId. It is reported as
	// DriverInfo.Profile ("fifo" or "standard"), because a caller who read
	// "sqs" and got FIFO ordering would be reading a capability table that
	// belongs to the other profile.
	FIFO bool
	// ContentBasedDeduplication turns on the FIFO queue attribute of the
	// same name for queues this driver creates. Without it, and without a
	// DedupKey, a FIFO publication is refused by the service.
	ContentBasedDeduplication bool
	// Resolver maps a portable topic to a queue name. Nil means
	// [QueueName], which is the mapping docs/drivers/sqs.md documents.
	Resolver func(topic string) (string, error)
	// QueueURLs maps a portable topic straight to a queue URL, skipping
	// both the resolver and GetQueueUrl. It is the escape hatch for a
	// deployment whose queues are not named after their topics at all.
	QueueURLs map[string]string
	// AutoCreate lets the driver create a queue it cannot resolve, which
	// The specification makes opt-in: a driver that provisioned by default would turn a
	// typo into a new queue.
	AutoCreate bool
	// Pollers is how many concurrent ReceiveMessage loops one source runs.
	// Zero means DefaultPollers. Each one is a long poll, so N pollers is
	// N concurrent requests and up to N*MaxMessages messages in flight.
	Pollers int
	// MaxMessages is how many messages one ReceiveMessage asks for. Zero
	// means MaxBatchEntries, which is the service's maximum.
	MaxMessages int
	// WaitTime is the long poll one ReceiveMessage asks for. Zero means
	// DefaultWaitTime; it may not exceed MaxWaitTime.
	WaitTime time.Duration
	// AckDeadline is the visibility timeout a source asks for when
	// SourceConfig.AckDeadline is zero. Zero means DefaultAckDeadline.
	AckDeadline time.Duration
	// LeaseGuard is how long before a delivery's visibility window closes
	// the driver stops trusting it: a settlement whose window expires
	// within LeaseGuard is refused with ErrLeaseLost without an API call.
	// Zero means the window has to have passed already.
	//
	// It exists because the service will accept a delete for a receipt
	// handle whose message another consumer now holds, so the only place
	// that refusal can happen is here.
	LeaseGuard time.Duration
	// Settle is the batch-settler flush policy,. The zero value
	// is flush.Auto, which resolves to the recorded bake-off winner —
	// flush.Immediate, the one arm that holds no settlement a caller is
	// waiting for. flush.Hybrid is a tenth of the requests for a consumer
	// whose handlers settle CONCURRENTLY, and costs a whole SettleInterval
	// per settlement for one that settles in a loop.
	Settle flush.Kind
	// SettleMaxItems is the batch size the count and hybrid policies send
	// at, and the ceiling every policy splits a larger batch on. Zero means
	// MaxBatchEntries, which is also the service's own limit.
	SettleMaxItems int
	// SettleInterval is how often the time and hybrid policies send. Zero
	// means flush.DefaultInterval.
	SettleInterval time.Duration
	// Body is the binary-body policy for this driver.
	Body BodyEncoding
	// MaxMessageBytes overrides the service limit this driver advertises
	// and enforces, for a queue configured with a smaller
	// MaximumMessageSize. Zero means MaxMessageBytes.
	MaxMessageBytes int64
	// DeadLetterQueue is the queue name messages are redriven to once they
	// have been received MaxReceiveCount times. It is a queue NAME and not
	// a topic: a dead-letter queue is usually shared by several topics.
	DeadLetterQueue string
	// MaxReceiveCount is how many receives a message survives before the
	// service moves it to DeadLetterQueue. Zero means no redrive policy.
	MaxReceiveCount int
	// Retention is the message retention period of a queue this driver
	// creates. Zero means DefaultRetention.
	Retention time.Duration
	// InsecureTransport dials the endpoint over plain HTTP. It is named the
	// way the specification asks so that it is greppable and obviously unsafe, and it
	// only ever applies to Endpoint: a regional AWS endpoint is always
	// HTTPS.
	InsecureTransport bool
	// APITimeout bounds one API call whose caller's context has no deadline
	// of its own. Zero means DefaultAPITimeout. It is never a second
	// deadline on a caller that has one.
	APITimeout time.Duration
	// Clock is the time source the driver reads. Nil means the wall clock.
	// The local lease check reads it, so a test that drives a visibility
	// window drives this.
	Clock mq.Clock
	// Hooks are the observability hooks.
	Hooks mq.Hooks
}

// Profile is the profile name this configuration selects, which is what
// DriverInfo.Profile reports.
func (c Config) Profile() string {
	if c.FIFO {
		return "fifo"
	}
	return "standard"
}

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if c.Region == "" {
		c.Region = DefaultRegion
	}
	if c.Pollers == 0 {
		c.Pollers = DefaultPollers
	}
	if c.MaxMessages == 0 {
		c.MaxMessages = MaxBatchEntries
	}
	if c.WaitTime == 0 {
		c.WaitTime = DefaultWaitTime
	}
	if c.AckDeadline == 0 {
		c.AckDeadline = DefaultAckDeadline
	}
	if c.SettleMaxItems == 0 {
		c.SettleMaxItems = MaxBatchEntries
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = MaxMessageBytes
	}
	if c.Retention == 0 {
		c.Retention = DefaultRetention
	}
	if c.APITimeout == 0 {
		c.APITimeout = DefaultAPITimeout
	}
	return c
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It is pure on purpose: the specification requires a driver to refuse a configuration before
// it starts background work, and a validation that called the service would
// turn a typo into a signing failure against a real endpoint.
func (c Config) Validate() error {
	switch {
	case !c.Body.Valid():
		return errBodyEncoding
	case !c.Settle.Valid():
		return errSettleKind
	case c.Pollers < 0 || c.MaxMessages < 0 || c.SettleMaxItems < 0 || c.MaxReceiveCount < 0:
		return errNegativeCount
	case c.MaxMessages > MaxBatchEntries:
		return errMaxMessages
	case c.SettleMaxItems > MaxBatchEntries:
		return errSettleItems
	case c.WaitTime < 0 || c.AckDeadline < 0 || c.LeaseGuard < 0 ||
		c.SettleInterval < 0 || c.Retention < 0 || c.APITimeout < 0:
		return errNegativeDuration
	case c.WaitTime > MaxWaitTime:
		return errWaitTime
	case c.AckDeadline > MaxVisibility:
		return errAckDeadline
	case c.MaxMessageBytes < 0 || c.MaxMessageBytes > MaxMessageBytes:
		return errMessageBytes
	}
	if !printableText(c.Region) || !printableText(c.Endpoint) ||
		!printableText(c.AccessKeyID) || !printableText(c.AccountID) {
		return errBadText
	}
	if c.SecretAccessKey != "" && c.AccessKeyID == "" {
		return errSecretWithoutKey
	}
	if c.AccessKeyID != "" && c.SecretAccessKey == "" {
		return errKeyWithoutSecret
	}
	if c.Endpoint != "" {
		if !strings.HasPrefix(c.Endpoint, "http://") && !strings.HasPrefix(c.Endpoint, "https://") {
			return errEndpointScheme
		}
		if strings.HasPrefix(c.Endpoint, "http://") && !c.InsecureTransport {
			return errInsecureEndpoint
		}
		// An endpoint is a scheme, a host and a port. A userinfo in it
		// would reach Config.DSN -- and therefore Config.String and
		// Config.LogValue, which are that rendering -- as the bytes of a
		// password, which the specification forbids; the AWS SDK signs with the
		// credentials in this Config and does not read one out of the
		// endpoint, so it would be a credential that leaks and does
		// nothing.
		if strings.ContainsRune(c.Endpoint, '@') {
			return errEndpointCredential
		}
		if strings.HasPrefix(c.Endpoint, "https://") && c.InsecureTransport {
			// The mirror of the case above. InsecureTransport MEANS plain
			// HTTP -- it is what makes ParseDSN build an "http://" endpoint
			// and what the DSN renders as `insecuretransport=true` -- so the
			// pair contradicts itself, and the rendering resolved it the
			// unsafe way: "https://h" with InsecureTransport rendered
			// "sqs://h?...&insecuretransport=true", which parses back to
			// "http://h". A rendering an operator pastes must not downgrade
			// the transport.
			return errInsecureWithTLSEndpoint
		}
		// The typed twin of the DSN's dial guard. `dialableHost` ran only
		// inside ParseDSN, so a typed Config carried a host straight past it
		// -- the same redirect the DSN refuses, reachable by writing the
		// struct instead of the string. The test is on
		// the HOST, after the scheme, the credential and the port are
		// removed, because those are separate positions with their own rules.
		if host, _ := splitAddr(endpointAuthority(c.Endpoint)); !dialableHost(host) {
			return errDSNHost
		}
		if endpointAuthorityHost(c.Endpoint) == RegionalHost {
			// "aws" is the sentinel the authority uses for "the SDK's own
			// regional endpoint" , so an endpoint whose
			// host is literally that renders a DSN that parses back to NO
			// endpoint. It names no real AWS endpoint either.
			return errEndpointIsSentinel
		}
	}
	if c.InsecureTransport && c.Endpoint == "" {
		return errInsecureWithoutEndpoint
	}
	if c.QueuePrefix != "" && !validNamePart(c.QueuePrefix) {
		return errPrefix
	}
	if c.DeadLetterQueue != "" {
		if err := CheckQueueName(c.DeadLetterQueue, c.FIFO); err != nil {
			// CheckQueueName answers CodeInvalidArgument, which is right
			// for a name that arrives at a publication and wrong for one
			// that arrives in a Config: the specification makes a configuration
			// failure comparable by CodeInvalidConfig, and a caller
			// switching on the code would not find this one. The
			// underlying refusal is preserved for errors.Is.
			return errDeadLetterName(err)
		}
	}
	if c.MaxReceiveCount > 0 && c.DeadLetterQueue == "" {
		return errRedriveWithoutQueue
	}
	if c.ContentBasedDeduplication && !c.FIFO {
		return errDedupWithoutFIFO
	}
	for topic, url := range c.QueueURLs {
		if topic == "" || !printableText(url) || url == "" {
			return errQueueURL
		}
	}
	return nil
}

// printableText reports whether a configured string is safe to render.
//
// Everything Validate calls this on ends up in [Config.DSN], which ends up in
// a log line and in an error, so a control byte or an invalid UTF-8 sequence
// here produces output no log pipeline can carry. It is the specification's
// rule, applied to this driver's own strings.
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

// DSN renders the configuration as a connection string with the credentials
// redacted, so a Config is safe to log or put in an error.
//
// It is not a round trip and is not meant to be one: the secret access key and
// the session token are gone from the result on purpose, which is the whole
// reason the rendering exists. The access key ID stays, because it is not a
// secret and because a redacted DSN that did not say which principal it was
// built for would be useless in a log.
func (c Config) DSN() string {
	cfg := c.resolve()
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	if cfg.AccessKeyID != "" {
		b.WriteString(escapeUser(cfg.AccessKeyID))
		b.WriteByte(':')
		b.WriteString(dsn.Redacted)
		b.WriteByte('@')
	}
	if cfg.Endpoint != "" {
		b.WriteString(renderEndpoint(cfg.Endpoint))
	} else {
		// RegionalHost, not nothing: the shared grammar refuses an authority
		// that carries a credential and no host , so a
		// production configuration -- an access key and the SDK's own
		// regional endpoint -- used to render a DSN that did not parse at
		// all. "aws" is the documented spelling of exactly this, and
		// [ParseDSN] turns it back into an empty Endpoint.
		b.WriteString(RegionalHost)
	}
	if cfg.QueuePrefix != "" {
		b.WriteByte('/')
		b.WriteString(escapePath(cfg.QueuePrefix))
	}
	q := make([]string, 0, 20)
	q = append(q,
		"region="+escapeQuery(cfg.Region),
		"pollers="+strconv.Itoa(cfg.Pollers),
		"maxmessages="+strconv.Itoa(cfg.MaxMessages),
		"waittime="+cfg.WaitTime.String(),
		"ackdeadline="+cfg.AckDeadline.String(),
		"leaseguard="+cfg.LeaseGuard.String(),
		"settle="+cfg.Settle.String(),
		"settlemaxitems="+strconv.Itoa(cfg.SettleMaxItems),
		"body="+cfg.Body.String(),
		"maxmessagebytes="+strconv.FormatInt(cfg.MaxMessageBytes, 10),
		"retention="+cfg.Retention.String(),
		"apitimeout="+cfg.APITimeout.String(),
	)
	if cfg.SettleInterval != 0 {
		q = append(q, "settleinterval="+cfg.SettleInterval.String())
	}
	if cfg.FIFO {
		q = append(q, "fifo=true")
	}
	if cfg.ContentBasedDeduplication {
		q = append(q, "contentbaseddedup=true")
	}
	if cfg.AutoCreate {
		q = append(q, "autocreate=true")
	}
	if cfg.AccountID != "" {
		q = append(q, "accountid="+escapeQuery(cfg.AccountID))
	}
	if cfg.DeadLetterQueue != "" {
		q = append(q, "deadletterqueue="+escapeQuery(cfg.DeadLetterQueue))
	}
	if cfg.MaxReceiveCount > 0 {
		q = append(q, "maxreceivecount="+strconv.Itoa(cfg.MaxReceiveCount))
	}
	if cfg.SessionToken != "" {
		q = append(q, "sessiontoken="+dsn.Redacted)
	}
	if cfg.InsecureTransport {
		q = append(q, "insecuretransport=true")
	}
	b.WriteByte('?')
	b.WriteString(strings.Join(q, "&"))
	return b.String()
}

// String renders the configuration with the credentials redacted.
func (c Config) String() string { return c.DSN() }

// LogValue renders the configuration for slog with the credentials redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

// The configuration refusals. The specification names no sentinel for a bad configuration
// and errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errBodyEncoding            = errcode.New(errcode.InvalidConfig, "iqmq/sqs: Body is not one of auto, text or base64")
	errSettleKind              = errcode.New(errcode.InvalidConfig, "iqmq/sqs: Settle is not one of the kinds")
	errNegativeCount           = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a configured count cannot be negative")
	errMaxMessages             = errcode.New(errcode.InvalidConfig, "iqmq/sqs: MaxMessages cannot exceed the service's ten messages per ReceiveMessage")
	errSettleItems             = errcode.New(errcode.InvalidConfig, "iqmq/sqs: SettleMaxItems cannot exceed the service's ten entries per batch")
	errNegativeDuration        = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a configured duration cannot be negative")
	errWaitTime                = errcode.New(errcode.InvalidConfig, "iqmq/sqs: WaitTime cannot exceed the service's twenty-second long poll")
	errAckDeadline             = errcode.New(errcode.InvalidConfig, "iqmq/sqs: AckDeadline cannot exceed the service's twelve-hour visibility timeout")
	errMessageBytes            = errcode.New(errcode.InvalidConfig, "iqmq/sqs: MaxMessageBytes must be between zero and the service's one-mebibyte message limit")
	errBadText                 = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a configured region, endpoint, key or account carries a control character or is not valid UTF-8")
	errSecretWithoutKey        = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a secret access key needs an access key ID")
	errKeyWithoutSecret        = errcode.New(errcode.InvalidConfig, "iqmq/sqs: an access key ID needs a secret access key; leave both empty and set Credentials for the default chain")
	errEndpointScheme          = errcode.New(errcode.InvalidConfig, "iqmq/sqs: Endpoint must begin with http:// or https://")
	errInsecureEndpoint        = errcode.New(errcode.InvalidConfig, "iqmq/sqs: an http:// endpoint needs InsecureTransport, which is how a plaintext endpoint stays deliberate")
	errInsecureWithoutEndpoint = errcode.New(errcode.InvalidConfig, "iqmq/sqs: InsecureTransport has no meaning without Endpoint; a regional AWS endpoint is always HTTPS")
	errPrefix                  = errcode.New(errcode.InvalidConfig, "iqmq/sqs: QueuePrefix is not made of the alphanumerics, hyphens and underscores a queue name accepts")
	errRedriveWithoutQueue     = errcode.New(errcode.InvalidConfig, "iqmq/sqs: MaxReceiveCount has no meaning without DeadLetterQueue")
	errDedupWithoutFIFO        = errcode.New(errcode.InvalidConfig, "iqmq/sqs: ContentBasedDeduplication is a FIFO queue attribute")
	errQueueURL                = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a QueueURLs entry has an empty topic or an unrenderable URL")
	errTwoEndpoints            = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a DSN names one endpoint; the SDK does what a region implies")
	errRegionalPort            = errcode.New(errcode.InvalidConfig, "iqmq/sqs: the host \"aws\" means the region's own endpoint and takes no port")
	errEndpointCredential      = errcode.New(errcode.InvalidConfig, "iqmq/sqs: Endpoint carries a credential; it is a scheme, a host and a port, and the access key goes in AccessKeyID and SecretAccessKey")
	errInsecureWithTLSEndpoint = errcode.New(errcode.InvalidConfig, "iqmq/sqs: InsecureTransport means plain HTTP and Endpoint is https://; the two contradict each other and a rendering of the pair parses back as http://")
	errEndpointIsSentinel      = errcode.New(errcode.InvalidConfig, "iqmq/sqs: Endpoint's host is \"aws\", which is the DSN's sentinel for the SDK's own regional endpoint; leave Endpoint empty for that")
	errDSNHost                 = errcode.New(errcode.InvalidConfig, "iqmq/sqs: a DSN host decodes to a byte an endpoint URL gives a meaning to -- \"@\", \"/\", \"?\", \"#\", a backslash, a space or a control character -- so the SDK would call somewhere the DSN does not name")
	errNilRegistrar            = errcode.New(errcode.InvalidArgument, "iqmq/sqs: Register was given a nil Registrar")
	errDeadLetterQueueName     = errcode.New(errcode.InvalidConfig, "iqmq/sqs: DeadLetterQueue is not a queue name the service accepts")
	errNilClient               = errcode.New(errcode.InvalidArgument, "iqmq/sqs: OpenWithClient was given a nil client")
)

// errDeadLetterName restates a name refusal as a configuration refusal,
// keeping the original reachable through errors.Is.
func errDeadLetterName(cause error) error {
	return &configError{cause: cause}
}

// configError is a configuration refusal that carries the reason underneath.
type configError struct{ cause error }

// Error names the setting and the reason.
func (e *configError) Error() string {
	return errDeadLetterQueueName.Error() + ": " + e.cause.Error()
}

// Unwrap exposes the underlying refusal, so errors.Is finds it.
func (e *configError) Unwrap() error { return e.cause }

// Is matches the configuration sentinel as well as the cause, which is what
// makes mq.CodeOf answer CodeInvalidConfig.
func (e *configError) Is(target error) bool { return target == errDeadLetterQueueName }

// Code is the portable code, in the form mq.CodeOf reads: it walks
// the chain looking for errcode.Coder, so a method returning mq.ErrorCode is
// invisible to it.
func (e *configError) Code() errcode.Code { return errcode.InvalidConfig }

// stringer and logValuer are the two rendering surfaces the specification asks a
// configuration to redact through; the third, encoding/json, is
// [bitbucket.org/iqhive/iqmq/dsn.Secret]'s own and is proved there.
type (
	stringer  interface{ String() string }
	logValuer interface{ LogValue() slog.Value }
)

// compile-time proof that Config renders through both of them.
var (
	_ stringer  = Config{}
	_ logValuer = Config{}
)

// endpointAuthorityHost is an endpoint's host with the scheme, any port and
// anything after the authority removed.
//
// Comparing `endpointHost(c.Endpoint)` against RegionalHost tested the whole
// post-scheme string, so the sentinel check was defeated by anything that
// made that string longer: "https://aws/" and "http://aws:9324" both passed
// and then rendered "sqs://aws" and "sqs://aws:9324", which parse back to NO
// endpoint and to an endpoint the DSN cannot name.
func endpointAuthorityHost(endpoint string) string {
	host, _ := splitAddr(endpointAuthority(endpoint))
	return host
}

// endpointAuthority is an endpoint with the scheme and any base PATH removed,
// and nothing else.
//
// It cuts at the first "/" only, deliberately. A path is legitimate -- the SDK
// honours LocalStack's "http://localhost:4566/<account>" -- but a "?" or a "#"
// before one is not: it ends the authority at the SDK and the call goes to the
// truncated host. Cutting at "/?#" here, which is what the first version did,
// hid exactly those two from the dial guard, so "https://a?b" passed.
func endpointAuthority(endpoint string) string {
	rest := endpointHost(endpoint)
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		rest = rest[:i]
	}
	return rest
}

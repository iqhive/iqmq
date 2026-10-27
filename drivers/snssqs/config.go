package snssqs

import (
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/flush"
	"github.com/iqhive/iqmq/drivers/sqs"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// DriverName is the driver's registered name and the scheme its DSN uses.
const DriverName = "snssqs"

// Version is the version of this driver module. It is empty until the module
// is tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// ClientModule is the native client this driver wraps for the publish side,
// reported through [github.com/iqhive/iqmq.DriverInfo]. The consume side is
// `drivers/sqs` and its own client, which
// [github.com/iqhive/iqmq/drivers/sqs.ClientModule] names.
const ClientModule = "github.com/aws/aws-sdk-go-v2/service/sns"

// ClientVersion is the pinned version of that client. Every service limit this
// driver advertises was read out of this version's generated documentation, so
// the two move together (see docs/drivers/snssqs.md).
const ClientVersion = "v1.47.5"

// The service limits, read from the pinned SDKs' generated documentation and
// recorded here rather than in the portable API, which is the specification's "service
// limits discovered/documented from the tested API version, not hard-coded
// into the portable API".
//
// Two of the three differ from the SQS driver's, and both differences are the
// publish side's: SNS takes a QUARTER of the message SQS does, and it takes no
// per-message timer at all.
const (
	// MaxBatchEntries is the entry limit of PublishBatch. It is the same ten
	// as SendMessageBatch, which is what makes one batch policy right for
	// both halves.
	MaxBatchEntries = 10
	// MaxMessageBytes is the largest message SNS accepts, and the largest
	// sum of one PublishBatch's payloads: 256 KiB, a QUARTER of the mebibyte
	// an SQS queue holds. A publication between the two limits is accepted
	// by drivers/sqs and refused here, which is the difference a caller
	// porting from one to the other meets first.
	MaxMessageBytes = 256 * 1024
	// MaxAttributes is the most message attributes one publication may
	// carry. A message that needs more is enveloped under MetadataAdaptive
	// and refused under MetadataNativeStrict.
	MaxAttributes = sqs.MaxMessageAttributes
	// MaxSubscriptionsPerTopic is how many subscriptions the service allows
	// on one topic, which is how wide a fan-out can be.
	MaxSubscriptionsPerTopic = 12_500_000
	// MaxFilterPolicyBytes is the largest filter policy the service accepts.
	MaxFilterPolicyBytes = 256 * 1024
)

// The defaults a zero [Config] resolves to. They are the SQS driver's, because
// the consume side IS that driver.
const (
	// DefaultRegion is the region a configuration that names none is built
	// with. It is not a guess at the caller's region: the SDK needs one to
	// sign a request, and a driver pointed at an emulator has no real region
	// at all.
	DefaultRegion = sqs.DefaultRegion
	// DefaultAPITimeout bounds one API call whose caller gave no deadline of
	// its own. It is never a second deadline on a caller that has one.
	DefaultAPITimeout = sqs.DefaultAPITimeout
)

// FilterScope is what an SNS subscription filter policy is evaluated against
// (the condition on FeatureServerFilter).
//
// It is an enumeration rather than a string because the two answers mean
// different things about iqmq's own metadata, and a typo that silently
// selected the default would change which messages a subscriber receives.
type FilterScope uint8

// The filter scopes.
const (
	// FilterScopeAttributes evaluates the policy against the message
	// ATTRIBUTES, which is the service's default. On this driver a header
	// becomes an attribute, so a policy written against header names works —
	// unless the message took the envelope, in which case every header is in
	// the body and the only attribute left is the envelope marker. See
	// docs/drivers/snssqs.md's "known gotchas".
	FilterScopeAttributes FilterScope = iota
	// FilterScopeBody evaluates the policy against the message BODY, parsed
	// as JSON by the service. It works for a caller whose payload is JSON
	// and is useless for one whose payload is not — and for an ENVELOPED
	// message, whose body is iqmq's own framing rather than the payload.
	FilterScopeBody

	filterScopeCount
)

var filterScopeNames = [filterScopeCount]string{"attributes", "body"}

// The subscription attribute values the service takes for each scope.
var filterScopeValues = [filterScopeCount]string{"MessageAttributes", "MessageBody"}

// String returns the scope's lower-case name.
func (f FilterScope) String() string {
	if f < filterScopeCount {
		return filterScopeNames[f]
	}
	return "FilterScope(?)"
}

// Valid reports whether f names a scope.
func (f FilterScope) Valid() bool { return f < filterScopeCount }

// Attribute is the value the service's FilterPolicyScope attribute takes.
func (f FilterScope) Attribute() string {
	if f < filterScopeCount {
		return filterScopeValues[f]
	}
	return ""
}

// parseFilterScope is [FilterScope]'s slug parser.
func parseFilterScope(s string) (FilterScope, bool) {
	for i, name := range filterScopeNames {
		if name == s {
			return FilterScope(i), true
		}
	}
	return 0, false
}

// Config is the typed configuration of the SNS-to-SQS fan-out driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). The secret access key and the session
// token are [github.com/iqhive/iqmq/dsn.Secret]s, so neither [Config.String]
// nor a slog record nor an error built from one can carry them.
//
// The shape to hold in mind while reading it: a TOPIC is an SNS topic and a
// SUBSCRIPTION is an SQS queue subscribed to it with raw message delivery.
// Everything on the publish side is SNS's; everything from the queue inward —
// the long poll, the visibility window, the settlement, the redrive — is
// `drivers/sqs`, because it is that driver.
//
// [Config.Validate] is pure: it performs no I/O and never calls either
// service, which is the specification's "validate config before starting background work".
type Config struct {
	// Region is the AWS region for both services. Empty means
	// DefaultRegion.
	Region string
	// Endpoint is the full endpoint URL, scheme included, for BOTH SNS and
	// SQS — "http://127.0.0.1:4566" for LocalStack, whose edge endpoint
	// serves every service. Empty means the SDK's own regional endpoints.
	//
	// There is one field rather than two because the deployment that needs
	// two is one pointing SNS at the real service and SQS at an emulator,
	// which is not a deployment: a real SNS topic cannot deliver into a
	// queue on a laptop.
	Endpoint string
	// Partition is the ARN partition, "aws" unless empty means
	// DefaultPartition. It is here because an ARN needs one and a
	// government or China region has its own.
	Partition string
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
	// AccountID is the account the topics and queues belong to. It is
	// needed to BUILD a topic ARN without a lookup; empty means the driver
	// resolves an ARN by creating or listing.
	AccountID string
	// TopicPrefix is prepended to every mapped SNS topic name, which is how
	// two iqmq deployments share one account.
	TopicPrefix string
	// QueuePrefix is prepended to every mapped SQS queue name.
	QueuePrefix string
	// Resolver maps a portable topic to an SNS topic name. Nil means
	// [TopicName].
	Resolver func(topic string) (string, error)
	// TopicARNs maps a portable topic straight to a topic ARN, skipping both
	// the resolver and the lookup. It is the escape hatch for a deployment
	// whose topics are not named after their iqmq topics at all.
	TopicARNs map[string]string
	// QueueResolver maps a (topic, subscription) pair to the SQS queue name
	// that subscription's messages arrive in. Nil means [QueueName].
	//
	// It takes BOTH halves because that is what a fan-out subscription is:
	// one topic's messages arrive in one queue per subscription, and a
	// resolver that saw only the topic could not tell two subscriptions
	// apart.
	QueueResolver func(topic, subscription string) (string, error)
	// AutoCreate lets the driver create the topic, the queue and the
	// subscription it cannot resolve, which the specification makes opt-in: a driver that
	// provisioned by default would turn a typo into a new topic, a new queue
	// and a monthly bill.
	AutoCreate bool
	// FilterPolicies maps a SUBSCRIPTION name to the SNS filter policy JSON
	// applied to it when this driver creates the subscription. It is the
	// condition on this driver's FeatureServerFilter claim: without an entry
	// there is no server-side filter at all.
	//
	// A policy is applied at Subscribe and is NEVER written over an existing
	// subscription's: changing which messages an existing subscriber
	// receives is an operator's decision, and [TypedAdmin.SetFilterPolicy]
	// is where it lives.
	FilterPolicies map[string]string
	// FilterPolicyScope is what those policies are evaluated against. The
	// zero value is FilterScopeAttributes, which is the service's default.
	FilterPolicyScope FilterScope
	// NoQueuePolicy stops the driver writing the SQS access policy that lets
	// SNS deliver into a queue it has just created. Set it when the
	// deployment manages queue policies itself.
	//
	// The policy is only ever written for a queue that has none, and only
	// when this driver creates the subscription: a queue that already
	// carries a Policy is a queue somebody configured, and a driver that
	// silently rewrote IAM would be a worse failure than a subscription that
	// needs one line of Terraform. That policy is read, and one with no
	// statement letting the topic deliver is refused at open; this setting
	// skips the check as well.
	NoQueuePolicy bool
	// Pollers is how many concurrent ReceiveMessage loops one source runs.
	// Zero means the SQS driver's default.
	Pollers int
	// MaxMessages is how many messages one ReceiveMessage asks for. Zero
	// means the service's maximum of ten.
	MaxMessages int
	// WaitTime is the long poll one ReceiveMessage asks for. Zero means the
	// SQS driver's default of twenty seconds. The service takes whole
	// seconds, and the SQS driver rounds a fraction up.
	WaitTime time.Duration
	// AckDeadline is the visibility timeout a source asks for when
	// SourceConfig.AckDeadline is zero.
	AckDeadline time.Duration
	// LeaseGuard is how long before a delivery's visibility window closes
	// the driver stops trusting it.
	LeaseGuard time.Duration
	// Settle is the batch-settler flush policy,.
	Settle flush.Kind
	// SettleMaxItems is the batch size the count and hybrid policies send
	// at. Zero means ten.
	SettleMaxItems int
	// SettleInterval is how often the time and hybrid policies send.
	SettleInterval time.Duration
	// Body is the binary-body policy. It applies to the SNS
	// publication, whose Message is text under the same character set an SQS
	// body is.
	Body sqs.BodyEncoding
	// MaxMessageBytes overrides the message limit this driver advertises and
	// enforces. Zero means MaxMessageBytes, which is SNS's 256 KiB and NOT
	// the mebibyte an SQS queue holds.
	MaxMessageBytes int64
	// DeadLetterQueue is the queue name messages are redriven to once they
	// have been received MaxReceiveCount times. It is the QUEUE's redrive
	// policy and is therefore per subscription, which is the only place a
	// fan-out can put one: a message that N subscribers failed to handle
	// failed N times independently.
	DeadLetterQueue string
	// MaxReceiveCount is how many receives a message survives before the
	// service moves it to DeadLetterQueue. Zero means no redrive policy.
	MaxReceiveCount int
	// Retention is the message retention period of a queue this driver
	// creates.
	Retention time.Duration
	// InsecureTransport dials the endpoint over plain HTTP. It is named the
	// way the specification asks so that it is greppable and obviously unsafe, and it
	// only ever applies to Endpoint: a regional AWS endpoint is always
	// HTTPS.
	InsecureTransport bool
	// APITimeout bounds one API call whose caller's context has no deadline
	// of its own. Zero means DefaultAPITimeout.
	APITimeout time.Duration
	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks.
	Hooks mq.Hooks
}

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if c.Region == "" {
		c.Region = DefaultRegion
	}
	if c.Partition == "" {
		c.Partition = DefaultPartition
	}
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = MaxMessageBytes
	}
	if c.APITimeout == 0 {
		c.APITimeout = DefaultAPITimeout
	}
	return c
}

// sqsConfig is the configuration the inner SQS driver is built with.
//
// The interesting lines are the two that are NOT forwarded. The resolver is
// the identity, because the queue name is composed by THIS driver out of a
// topic and a subscription and the inner driver is handed the answer; and the
// hooks are left off, because the client this driver returns wires them once
// and a second wiring would fire every observation point twice.
func (c Config) sqsConfig() sqs.Config {
	r := c.resolve()
	return sqs.Config{
		Region:            r.Region,
		Endpoint:          r.Endpoint,
		AccessKeyID:       r.AccessKeyID,
		SecretAccessKey:   r.SecretAccessKey,
		SessionToken:      r.SessionToken,
		Credentials:       r.Credentials,
		AccountID:         r.AccountID,
		Resolver:          identityQueueName,
		AutoCreate:        r.AutoCreate,
		Pollers:           r.Pollers,
		MaxMessages:       r.MaxMessages,
		WaitTime:          r.WaitTime,
		AckDeadline:       r.AckDeadline,
		LeaseGuard:        r.LeaseGuard,
		Settle:            r.Settle,
		SettleMaxItems:    r.SettleMaxItems,
		SettleInterval:    r.SettleInterval,
		Body:              r.Body,
		MaxMessageBytes:   r.MaxMessageBytes,
		DeadLetterQueue:   r.DeadLetterQueue,
		MaxReceiveCount:   r.MaxReceiveCount,
		Retention:         r.Retention,
		InsecureTransport: r.InsecureTransport,
		APITimeout:        r.APITimeout,
		Clock:             r.Clock,
	}
}

// identityQueueName is the inner driver's resolver: this driver has already
// composed the queue name, so the inner one validates it and looks it up.
func identityQueueName(name string) (string, error) { return name, nil }

// Validate reports whether the configuration can be used, without any I/O.
//
// It ends by validating the configuration the inner SQS driver will be built
// with, so that every refusal the specification wrote — a negative count, a poll longer than
// the service allows, an http:// endpoint with no InsecureTransport — is this
// driver's refusal too, with no second copy to keep in step.
func (c Config) Validate() error {
	switch {
	case !c.Body.Valid():
		return errBodyEncoding
	case !c.Settle.Valid():
		return errSettleKind
	case !c.FilterPolicyScope.Valid():
		return errFilterScope
	case c.MaxMessageBytes < 0 || c.MaxMessageBytes > MaxMessageBytes:
		return errMessageBytes
	}
	if !printableText(c.Region) || !printableText(c.Endpoint) ||
		!printableText(c.AccessKeyID) || !printableText(c.AccountID) ||
		!printableText(c.Partition) {
		return errBadText
	}
	if c.TopicPrefix != "" && !validNamePart(c.TopicPrefix) {
		return errTopicPrefix
	}
	if c.QueuePrefix != "" && !validNamePart(c.QueuePrefix) {
		return errQueuePrefix
	}
	for topic, arn := range c.TopicARNs {
		if topic == "" || !printableText(arn) || !IsTopicARN(arn) {
			return errTopicARN
		}
	}
	for sub, policy := range c.FilterPolicies {
		if err := CheckFilterPolicy(sub, policy); err != nil {
			return err
		}
	}
	// The inner driver's own refusals, applied to the configuration it will
	// really be built with. It is given the resolved copy, because that is
	// what OpenBackend hands it -- and its refusals are renamed, because a
	// caller of THIS driver set the field on a Config of this driver's type.
	return renameInnerRefusal(c.sqsConfig().Validate())
}

// The two prefixes every configuration refusal carries: the inner driver's, and
// this one's.
const (
	innerRefusalPrefix = "iqmq/" + sqs.DriverName + ": "
	ownRefusalPrefix   = "iqmq/" + DriverName + ": "
)

// renameInnerRefusal puts THIS driver's name on a refusal the inner SQS
// driver made.
//
// [Config.Validate] ends by validating the configuration the inner driver will
// be built with, which is what keeps the refusals from having a second copy
// here to keep in step -- and it meant that 28 refusals an snssqs user will
// meet reached them under the inner driver's name: `iqmq/sqs: MaxMessages
// cannot exceed the service's ten messages per ReceiveMessage`, about a field
// they set on an `snssqs.Config`, naming a driver their code never mentions.
// `sqsConfig` forwards every one of those fields one for one, so the setting
// the refusal is about is always the caller's own.
//
// It substitutes the PREFIX and nothing else, so the rule holds for every
// refusal the inner driver can make rather than for a list of them that would
// go stale -- which is the same reason Validate delegates in the first place.
// A refusal that does not carry the prefix is passed through untouched.
//
// The IDENTITY is preserved: `Unwrap` keeps `errors.Is` working against the
// inner driver's sentinels, and the wrapper deliberately implements no
// `Code()`, so `mq.CodeOf` walks past it to the inner error's own code.
func renameInnerRefusal(err error) error {
	if err == nil {
		return nil
	}
	if !strings.HasPrefix(err.Error(), innerRefusalPrefix) {
		return err
	}
	return innerRefusal{err: err}
}

// innerRefusal is one of the inner SQS driver's refusals, reported under this
// driver's name.
type innerRefusal struct{ err error }

// Error is the inner message with this driver's prefix in place of the inner
// driver's.
func (e innerRefusal) Error() string {
	return ownRefusalPrefix + strings.TrimPrefix(e.err.Error(), innerRefusalPrefix)
}

// Unwrap keeps the inner sentinel reachable, which is what makes errors.Is and
// mq.CodeOf answer as they did before the rename.
func (e innerRefusal) Unwrap() error { return e.err }

// CheckFilterPolicy reports whether a filter policy is one the service will
// take, as far as that can be answered without calling it.
//
// What it can answer is the part a typo gets wrong: a policy has to be a JSON
// OBJECT — the service's grammar is a map from attribute name to a list of
// matchers — and it has to be inside the service's size limit. What it cannot
// answer is whether the matchers mean anything, which only SNS knows; the specification's
// conditional claim says the condition is "a policy the service accepts", and
// this is the half a pure Validate can hold.
func CheckFilterPolicy(sub, policy string) error {
	if sub == "" {
		return errFilterSubscription
	}
	if !validNamePart(escapeNamePart(sub)) {
		return errFilterSubscription
	}
	if policy == "" {
		return errFilterEmpty
	}
	if len(policy) > MaxFilterPolicyBytes {
		return errFilterTooLong
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policy), &obj); err != nil {
		return errFilterNotJSON
	}
	if len(obj) == 0 {
		// An empty object is a policy that matches everything, which is
		// what NO policy already means. Accepting it would advertise a
		// filter that filters nothing.
		return errFilterEmpty
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
// the session token are gone from the result on purpose. The access key ID
// stays, because it is not a secret and because a redacted DSN that did not
// say which principal it was built for would be useless in a log.
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
		b.WriteString(RegionalHost)
	}
	if cfg.TopicPrefix != "" {
		b.WriteByte('/')
		b.WriteString(escapePath(cfg.TopicPrefix))
	}
	q := make([]string, 0, 24)
	q = append(q,
		"region="+escapeQuery(cfg.Region),
		"partition="+escapeQuery(cfg.Partition),
		"body="+cfg.Body.String(),
		"filterscope="+cfg.FilterPolicyScope.String(),
		"maxmessagebytes="+strconv.FormatInt(cfg.MaxMessageBytes, 10),
		"apitimeout="+cfg.APITimeout.String(),
		"settle="+cfg.Settle.String(),
	)
	for _, kv := range [...]struct {
		key string
		val string
		on  bool
	}{
		{"queueprefix", escapeQuery(cfg.QueuePrefix), cfg.QueuePrefix != ""},
		{"accountid", escapeQuery(cfg.AccountID), cfg.AccountID != ""},
		{"pollers", strconv.Itoa(cfg.Pollers), cfg.Pollers != 0},
		{"maxmessages", strconv.Itoa(cfg.MaxMessages), cfg.MaxMessages != 0},
		{"waittime", cfg.WaitTime.String(), cfg.WaitTime != 0},
		{"ackdeadline", cfg.AckDeadline.String(), cfg.AckDeadline != 0},
		{"leaseguard", cfg.LeaseGuard.String(), cfg.LeaseGuard != 0},
		{"settlemaxitems", strconv.Itoa(cfg.SettleMaxItems), cfg.SettleMaxItems != 0},
		{"settleinterval", cfg.SettleInterval.String(), cfg.SettleInterval != 0},
		{"deadletterqueue", escapeQuery(cfg.DeadLetterQueue), cfg.DeadLetterQueue != ""},
		{"maxreceivecount", strconv.Itoa(cfg.MaxReceiveCount), cfg.MaxReceiveCount != 0},
		{"retention", cfg.Retention.String(), cfg.Retention != 0},
		{"autocreate", "true", cfg.AutoCreate},
		{"noqueuepolicy", "true", cfg.NoQueuePolicy},
		{"insecuretransport", "true", cfg.InsecureTransport},
		{"sessiontoken", dsn.Redacted, cfg.SessionToken != ""},
	} {
		if kv.on {
			q = append(q, kv.key+"="+kv.val)
		}
	}
	b.WriteByte('?')
	b.WriteString(strings.Join(q, "&"))
	return b.String()
}

// String renders the configuration with the credentials redacted.
func (c Config) String() string { return c.DSN() }

// LogValue renders the configuration for slog with the credentials redacted.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.DSN()) }

// endpointHost renders the endpoint's host for a DSN.
func endpointHost(endpoint string) string {
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
}

// The configuration refusals. The specification names no sentinel for a bad configuration
// and errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errBodyEncoding       = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: Body is not one of auto, text or base64")
	errSettleKind         = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: Settle is not one of auto, immediate, count, time or hybrid")
	errFilterScope        = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: FilterPolicyScope is neither attributes nor body")
	errMessageBytes       = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: MaxMessageBytes must be between zero and SNS's 256 KiB message limit, which is a quarter of what an SQS queue holds")
	errBadText            = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a configured region, endpoint, partition, key or account carries a control character or is not valid UTF-8")
	errTopicPrefix        = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: TopicPrefix is not made of the alphanumerics, hyphens and underscores an SNS topic name accepts")
	errTopicARN           = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a TopicARNs entry has an empty topic or a value that is not an SNS topic ARN")
	errFilterSubscription = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a FilterPolicies key is not a subscription name this driver can map to a queue")
	errFilterEmpty        = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a filter policy with no conditions matches everything, which is what no policy already means")
	errFilterTooLong      = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: the filter policy is longer than the service's 256 KiB limit")
	errFilterNotJSON      = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a filter policy is a JSON object mapping an attribute name to a list of matchers, and this one does not parse as one")
	errTwoEndpoints       = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a DSN names one endpoint; the SDK does what a region implies")
	errRegionalPort       = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: the host \"aws\" means the region's own endpoints and takes no port")
	errDSNHost            = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: a DSN host decodes to a byte an endpoint URL gives a meaning to -- \"@\", \"/\", \"?\", \"#\", a backslash, a space or a control character -- so the SDK would call somewhere the DSN does not name")
	errInsecureRegional   = errcode.New(errcode.InvalidConfig, "iqmq/snssqs: InsecureTransport has no meaning without an endpoint; a regional AWS endpoint is always HTTPS")
	errNilRegistrar       = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: Register was given a nil Registrar")
	errNilSNSClient       = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: OpenWithClients was given a nil SNS client")
	errNilSQSClient       = errcode.New(errcode.InvalidArgument, "iqmq/snssqs: OpenWithClients was given a nil SQS client")
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

package jetstream

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// DriverName is the driver's registered name.
const DriverName = "jetstream"

// Version is the version of this driver module. It is empty until the module is
// tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// DefaultAddr is the address [Config] dials when none is given.
const DefaultAddr = "nats://127.0.0.1:4222"

// The defaults a zero [Config] resolves to. Every one of them is a bound: the specification
// requires every queue to be bounded by default, so none of these may mean
// "unlimited" when it is left unset.
const (
	// DefaultStream is the stream a configuration that names none is bound
	// to.
	DefaultStream = "IQMQ"
	// DefaultConsumerPrefix is prepended to every durable consumer name.
	DefaultConsumerPrefix = "iqmq-"
	// DefaultScheduleSubject is the first token of the subject root scheduled
	// publications are written to; the stream's name is the second. It is a
	// sibling of the topic tree rather than a child of it, so that a wildcard
	// subscription under [Config.SubjectPrefix] cannot receive the driver's
	// own schedule messages.
	DefaultScheduleSubject = "iqmqsched"
	// DefaultAckWait is the lease a consumer that asks for none gets. It is
	// the server's own default, stated here so that the driver's answer to
	// DeliveryInfo.LeaseExpires does not depend on a version.
	DefaultAckWait = 30 * time.Second
	// DefaultDuplicateWindow is the Nats-Msg-Id de-duplication window a
	// stream this driver creates gets. It is the server's own default.
	DefaultDuplicateWindow = 2 * time.Minute
	// DefaultPublishAsyncMaxPending bounds the asynchronous publications
	// outstanding at once, which is what makes PublishAsync and PublishBatch
	// bounded rather than a buffer. It is the client library's own default.
	DefaultPublishAsyncMaxPending = 4000
	// DefaultPublishTimeout bounds a publication, a settlement round trip
	// and a JetStream API call whose caller gave no deadline.
	DefaultPublishTimeout = 10 * time.Second
	// DefaultConnectTimeout bounds the initial dial.
	DefaultConnectTimeout = 5 * time.Second
	// DefaultReconnectWait is the first delay between reconnection attempts.
	DefaultReconnectWait = 250 * time.Millisecond
	// DefaultDrainTimeout bounds the drain a Close performs.
	DefaultDrainTimeout = 30 * time.Second
	// DefaultPendingMessages and DefaultPendingBytes are the per-subscription
	// pending limits handed to SetPendingLimits.
	DefaultPendingMessages = 64 << 10
	DefaultPendingBytes    = 64 << 20
)

// StorageKind is the stream storage a [Config] asks for.
//
// It is an enumeration rather than a bool because the answer decides whether
// [bitbucket.org/iqhive/iqmq.FeatureDurableStorage] is a native claim or a
// conditional one: a memory-backed stream loses everything the server was
// holding when it restarts, and a driver that advertised durability over one
// would be lying about the only property this profile exists for.
type StorageKind uint8

// The storage kinds.
const (
	// StorageFile is a file-backed stream, which is the default.
	StorageFile StorageKind = iota
	// StorageMemory is a memory-backed stream: fast, and durable over
	// nothing.
	StorageMemory

	storageKindCount
)

var storageKindNames = [storageKindCount]string{"file", "memory"}

// String returns the kind's lower-case name.
func (s StorageKind) String() string {
	if s < storageKindCount {
		return storageKindNames[s]
	}
	return "StorageKind(?)"
}

// Valid reports whether s names a storage kind.
func (s StorageKind) Valid() bool { return s < storageKindCount }

// Config is the typed configuration of the JetStream driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). Every credential is a
// [bitbucket.org/iqhive/iqmq/dsn.Secret], so neither [Config.String] nor a slog
// record nor an error built from one can carry it.
//
// [Config.Validate] is pure: it performs no I/O and never dials, which is the specification's
// "validate config before starting background work".
type Config struct {
	// Addrs are the servers to dial, in the order they are tried. Empty
	// means DefaultAddr.
	Addrs []string
	// Name is the connection name the server reports in its monitoring
	// endpoints. Empty means "iqmq".
	Name string
	// Username and Password are the user credentials.
	Username string
	Password dsn.Secret
	// Token is the authentication token, an alternative to a user and a
	// password.
	Token dsn.Secret
	// CredentialsFile is a NATS credentials file (.creds) to authenticate
	// with. Authentication that needs a signing callback has no form a typed
	// Config or a DSN can carry and is reached through [OpenWithClient].
	CredentialsFile string
	// TLS is the TLS configuration. A non-nil value turns TLS on.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks so that it is greppable and obviously unsafe.
	InsecureSkipVerify bool

	// Stream is the stream every topic of this client lives in. Empty means
	// DefaultStream.
	Stream string
	// StreamSubjects are the subjects the stream captures. It is only read
	// when ManageStream is true; empty then means SubjectPrefix + ".>" plus
	// the schedule tree beside it, and ManageStream with neither a prefix nor
	// an explicit list is refused.
	StreamSubjects []string
	// ManageStream lets the driver create the stream at [Open] and bring its
	// mutable settings up to date. It is off by default: the specification forbids a
	// driver from provisioning topology behind the caller's back, so
	// creating a stream is something the deployment asks for.
	ManageStream bool
	// Storage is the stream storage this driver asks for when it creates the
	// stream. It is also what [Config.Validate] reads to decide whether a
	// durability claim is possible at all; the claim itself comes from the
	// stream the client is really bound to.
	Storage StorageKind
	// Replicas is the stream's replica count. Zero means the server's
	// default of one.
	Replicas int
	// MaxAge is how long the stream keeps a message. Zero means for ever.
	MaxAge time.Duration
	// MaxMsgs and MaxBytes bound the stream. Zero means the server's default
	// of unlimited, which is the broker's own policy and not a queue iqmq
	// owns.
	MaxMsgs  int64
	MaxBytes int64
	// MaxMsgSize is the stream's own per-message ceiling. Zero means the
	// server's default of unlimited; when it is set it is also what
	// Limits.MaxMessageBytes reports, because the smaller of the stream's
	// ceiling and the server's max_payload is the only answer that cannot lie
	// in the dangerous direction.
	MaxMsgSize int32
	// DuplicateWindow is how long the stream remembers a Nats-Msg-Id for
	// de-duplication. Zero means DefaultDuplicateWindow.
	DuplicateWindow time.Duration
	// SubjectPrefix is prepended to every mapped subject as its own token,
	// which is how two iqmq deployments share one stream tree. Empty means
	// the topic maps to the subject unchanged.
	SubjectPrefix string

	// ConsumerPrefix is prepended to every durable consumer name. Empty
	// means DefaultConsumerPrefix.
	ConsumerPrefix string
	// AckWait is the lease a delivery gets when the consumer asks for none.
	// Zero means DefaultAckWait.
	AckWait time.Duration
	// MaxDeliver bounds the deliveries of one record. Zero means the
	// server's default of unlimited; iqmq's own retry policy is the core's
	// and the specification gives redelivery accounting to exactly one layer, so a
	// deployment that wants a ceiling here sets it deliberately.
	MaxDeliver int
	// ConsumerInactiveThreshold deletes a durable consumer this long after
	// its last pull request. Zero leaves durables in place, which is what
	// makes a durable subscription durable.
	ConsumerInactiveThreshold time.Duration

	// DeadLetterTopic is where [bitbucket.org/iqhive/iqmq.Delivery.Reject]
	// routes a message before it terminates the original. Empty means there
	// is no destination, and Reject reports ErrUnsupported and settles
	// nothing.
	DeadLetterTopic string
	// ScheduleSubject is the subject root scheduled publications are written
	// to. Empty means DefaultScheduleSubject; a configuration that sets it
	// to the empty string through a DSN cannot, which is why
	// DisableSchedules exists.
	ScheduleSubject string
	// DisableSchedules refuses every scheduled publication whatever the
	// server supports, for a deployment that does not want the driver
	// writing to a second subject tree.
	DisableSchedules bool
	// DoubleAck makes a positive settlement a request/reply round trip, so
	// that Ack returns only once the server has recorded it. It costs one
	// round trip per acknowledgement and is what a deployment that cannot
	// tolerate a redelivery after a lost link asks for.
	DoubleAck bool

	// PublishAsyncMaxPending bounds the asynchronous publications
	// outstanding at once. Zero means DefaultPublishAsyncMaxPending; it is
	// never unlimited.
	PublishAsyncMaxPending int
	// PublishTimeout bounds a publication, a settlement and a JetStream API
	// call whose caller's context has no deadline. It is never a second
	// deadline on a caller that has one.
	PublishTimeout time.Duration

	// APIPrefix is the JetStream API prefix, for an account that exports it
	// under a name of its own. Empty means the default.
	APIPrefix string
	// Domain is the JetStream domain, for a leaf node reaching a hub's
	// streams. It is exclusive with APIPrefix.
	Domain string

	// PendingMessages and PendingBytes are the per-subscription limits given
	// to SetPendingLimits.
	PendingMessages int
	PendingBytes    int
	// ConnectTimeout bounds the initial dial.
	ConnectTimeout time.Duration
	// ReconnectWait is the first delay between reconnection attempts.
	ReconnectWait time.Duration
	// MaxReconnects bounds the reconnection attempts. Zero means FOR EVER,
	// which is what a long-lived client wants and what
	// [bitbucket.org/iqhive/iqmq/driverkit.Reconnector] says of its own
	// MaxAttempts; a negative value means the same thing explicitly.
	//
	// It is not the client library's own default, which is 60 attempts and
	// then a permanent close. A reset storm that outlasts 60 attempts --
	// about fifteen seconds at the default ReconnectWait -- left the client
	// closed for good, so every publish made after the network healed
	// returned mq.ErrClosed and nothing ever revived it. `make chaos
	// DRIVER=jetstream`'s reset-mid-burst scenario reported exactly that:
	// `resumed=false` with `codes=[Closed ...]` over about sixty probes sent
	// after the toxic had been removed. A caller who
	// wants to give up says so with a positive value.
	MaxReconnects int
	// PingInterval and MaxPingsOut are the connection's own liveness probe.
	PingInterval time.Duration
	MaxPingsOut  int
	// DrainTimeout bounds the drain performed when iqmq owns the connection
	// and closes it, and the wait for a closing consumer's buffer to empty.
	DrainTimeout time.Duration

	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks. Only OnReconnect is fired
	// from here — a driver has no access to the client's own hooks, so the
	// reconnection and consumer events this driver produces have nowhere
	// else to go (finding recorded in the maintenance notes).
	Hooks mq.Hooks
}

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if len(c.Addrs) == 0 {
		c.Addrs = []string{DefaultAddr}
	}
	if c.Name == "" {
		c.Name = "iqmq"
	}
	if c.Stream == "" {
		c.Stream = DefaultStream
	}
	if c.ConsumerPrefix == "" {
		c.ConsumerPrefix = DefaultConsumerPrefix
	}
	if c.ScheduleSubject == "" {
		c.ScheduleSubject = DefaultScheduleSubject
	}
	if c.AckWait == 0 {
		c.AckWait = DefaultAckWait
	}
	if c.DuplicateWindow == 0 {
		c.DuplicateWindow = DefaultDuplicateWindow
	}
	if c.PublishAsyncMaxPending == 0 {
		c.PublishAsyncMaxPending = DefaultPublishAsyncMaxPending
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.ReconnectWait == 0 {
		c.ReconnectWait = DefaultReconnectWait
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	if c.MaxReconnects == 0 {
		// Negative is the client library's spelling of "for ever". Resolved
		// here rather than left to the library so that String(), the DSN
		// round trip and Open all agree about what this connection will do.
		c.MaxReconnects = -1
	}
	if c.PendingMessages == 0 {
		c.PendingMessages = DefaultPendingMessages
	}
	if c.PendingBytes == 0 {
		c.PendingBytes = DefaultPendingBytes
	}
	if len(c.StreamSubjects) == 0 {
		c.StreamSubjects = c.defaultStreamSubjects()
	}
	return c
}

// defaultStreamSubjects is the subject set a stream this driver creates
// captures: everything under the prefix, and the schedule tree beside it.
//
// The schedule tree is a SIBLING of the topic tree and not a child of it, which
// is the point: a consumer whose topic is a wildcard under the prefix would
// otherwise receive the driver's own schedule messages as though they were the
// application's.
//
// It returns nothing when there is no prefix, and [Config.Validate] refuses that
// combination rather than guessing. The guess that suggests itself — ">", every
// subject — is not merely broad: JetStream REFUSES a stream that captures every
// subject unless it is also created with NoAck, which would discard every
// acknowledgement the profile exists for. So a driver that is asked to manage a
// stream is told which subjects it owns.
func (c Config) defaultStreamSubjects() []string {
	if c.SubjectPrefix == "" {
		return nil
	}
	subjects := []string{c.SubjectPrefix + ".>"}
	if !c.DisableSchedules {
		subjects = append(subjects, c.scheduleRoot()+".>")
	}
	return subjects
}

// scheduleRoot is the subject root schedule messages are written to.
//
// The stream's name is part of it, and that is not decoration: JetStream refuses
// two streams in one account whose subjects overlap, so a schedule root shared
// by every stream would make a second iqmq stream in the same account
// impossible to create.
func (c Config) scheduleRoot() string {
	root := c.ScheduleSubject
	if root == "" {
		root = DefaultScheduleSubject
	}
	stream := c.Stream
	if stream == "" {
		stream = DefaultStream
	}
	return root + "." + stream
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It is pure on purpose: the specification requires a driver to refuse a configuration before
// it starts background work, and a validation that dialled would turn a typo
// into a five-second timeout.
func (c Config) Validate() error {
	for _, a := range c.Addrs {
		if err := checkAddr(a); err != nil {
			return err
		}
	}
	if c.Stream != "" {
		if err := CheckName(c.Stream); err != nil {
			return errStreamName
		}
	}
	if c.SubjectPrefix != "" {
		if err := CheckSubject(c.SubjectPrefix, false); err != nil {
			return errPrefix
		}
	}
	if c.ConsumerPrefix != "" {
		if err := CheckName(c.ConsumerPrefix); err != nil {
			return errConsumerPrefix
		}
	}
	if c.ScheduleSubject != "" {
		if err := CheckSubject(c.ScheduleSubject, false); err != nil {
			return errScheduleSubject
		}
	}
	if c.ManageStream && len(c.StreamSubjects) == 0 && c.SubjectPrefix == "" {
		return errNoStreamSubjects
	}
	if c.DeadLetterTopic != "" {
		if _, err := Subject(c.SubjectPrefix, c.DeadLetterTopic); err != nil {
			return errDeadLetterTopic
		}
	}
	for _, s := range c.StreamSubjects {
		if err := CheckSubject(s, true); err != nil {
			return errStreamSubject
		}
	}
	if !printableText(c.Name) || !printableText(c.CredentialsFile) ||
		!printableText(c.Username) || !printableText(c.APIPrefix) || !printableText(c.Domain) {
		return errBadText
	}
	if !c.Storage.Valid() {
		return errStorageKind
	}
	switch {
	case c.PendingMessages < 0 || c.PendingBytes < 0:
		return errPendingLimits
	case c.Replicas < 0 || c.MaxMsgs < 0 || c.MaxBytes < 0 || c.MaxDeliver < 0 || c.MaxMsgSize < 0:
		return errNegativeBound
	case c.PublishAsyncMaxPending < 0:
		return errAsyncPending
	case c.AckWait < 0 || c.MaxAge < 0 || c.DuplicateWindow < 0 ||
		c.ConsumerInactiveThreshold < 0 || c.PublishTimeout < 0 ||
		c.ConnectTimeout < 0 || c.ReconnectWait < 0 || c.PingInterval < 0 ||
		c.DrainTimeout < 0:
		return errNegativeDuration
	case c.MaxPingsOut < 0:
		return errPings
	}
	if c.APIPrefix != "" && c.Domain != "" {
		return errPrefixAndDomain
	}
	if c.Password != "" && c.Username == "" {
		return errPasswordWithoutUser
	}
	n := 0
	for _, set := range [...]bool{c.Username != "", c.Token != "", c.CredentialsFile != ""} {
		if set {
			n++
		}
	}
	if n > 1 {
		return errTooManyCredentials
	}
	if c.InsecureSkipVerify && c.TLS == nil {
		return errInsecureWithoutTLS
	}
	return nil
}

// checkAddr refuses a server address that is not printable ASCII.
//
// It is not fussiness. An address goes into the dial and into every rendering
// of the configuration, so a byte that is not printable ASCII produces a log
// line and an error string that are not valid UTF-8. A non-ASCII host name
// reaches NATS as punycode, which is ASCII, so nothing legitimate is refused
// here (the specification found the first instance of this from a fuzzed DSN; the rule is the same here).
func checkAddr(a string) error {
	if strings.TrimSpace(a) == "" {
		return errNoAddress
	}
	for i := range len(a) {
		if c := a[i]; c <= ' ' || c >= 0x7f {
			return errBadAddress
		}
	}
	return nil
}

// printableText reports whether a configured string is safe to render.
//
// Everything Validate calls this on ends up in [Config.DSN], which ends up in a
// log line and in an error, so a control byte or an invalid UTF-8 sequence here
// produces output no log pipeline can carry.
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
// the result on purpose, which is the whole reason the rendering exists. Every
// other parameter is rendered, so what comes back does name the same broker,
// the same stream and the same behaviour.
//
// Every free-text component is escaped for the position it lands in, because
// the shared parser DECODES every one of them: a host, a user or a stream name
// that came out of a DSN can hold any byte, and one written back raw is a
// different DSN or none. FuzzJetStreamDSN found "natsjs://%2C0" rendering its
// host as ",0", which the parser splits into two endpoints and refuses.
func (c Config) DSN() string {
	cfg := c.resolve()
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	if cfg.Username != "" || cfg.Password != "" {
		b.WriteString(escapeUser(cfg.Username))
		if cfg.Password != "" {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i, a := range cfg.Addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(renderAddr(strings.TrimPrefix(strings.TrimPrefix(a, "nats://"), "tls://")))
	}
	if cfg.SubjectPrefix != "" {
		b.WriteByte('/')
		b.WriteString(escapePath(cfg.SubjectPrefix))
	}
	q := make([]string, 0, 24)
	q = append(q,
		"name="+escapeQuery(cfg.Name),
		"stream="+escapeQuery(cfg.Stream),
		"storage="+cfg.Storage.String(),
		"consumerprefix="+escapeQuery(cfg.ConsumerPrefix),
		"schedulesubject="+escapeQuery(cfg.ScheduleSubject),
		"ackwait="+cfg.AckWait.String(),
		"duplicatewindow="+cfg.DuplicateWindow.String(),
		"publishasyncmaxpending="+strconv.Itoa(cfg.PublishAsyncMaxPending),
		"publishtimeout="+cfg.PublishTimeout.String(),
		"pendingmessages="+strconv.Itoa(cfg.PendingMessages),
		"pendingbytes="+strconv.Itoa(cfg.PendingBytes),
		"connecttimeout="+cfg.ConnectTimeout.String(),
		"reconnectwait="+cfg.ReconnectWait.String(),
		"draintimeout="+cfg.DrainTimeout.String(),
	)
	if cfg.ManageStream {
		q = append(q, "managestream=true")
	}
	if len(c.StreamSubjects) > 0 {
		q = append(q, "streamsubjects="+escapeQuery(strings.Join(c.StreamSubjects, ";")))
	}
	if cfg.Replicas != 0 {
		q = append(q, "replicas="+strconv.Itoa(cfg.Replicas))
	}
	if cfg.MaxAge != 0 {
		q = append(q, "maxage="+cfg.MaxAge.String())
	}
	if cfg.MaxMsgs != 0 {
		q = append(q, "maxmsgs="+strconv.FormatInt(cfg.MaxMsgs, 10))
	}
	if cfg.MaxBytes != 0 {
		q = append(q, "maxbytes="+strconv.FormatInt(cfg.MaxBytes, 10))
	}
	if cfg.MaxMsgSize != 0 {
		q = append(q, "maxmsgsize="+strconv.FormatInt(int64(cfg.MaxMsgSize), 10))
	}
	if cfg.MaxDeliver != 0 {
		q = append(q, "maxdeliver="+strconv.Itoa(cfg.MaxDeliver))
	}
	if cfg.ConsumerInactiveThreshold != 0 {
		q = append(q, "inactivethreshold="+cfg.ConsumerInactiveThreshold.String())
	}
	if cfg.DeadLetterTopic != "" {
		q = append(q, "deadlettertopic="+escapeQuery(cfg.DeadLetterTopic))
	}
	if cfg.DisableSchedules {
		q = append(q, "disableschedules=true")
	}
	if cfg.DoubleAck {
		q = append(q, "doubleack=true")
	}
	if cfg.APIPrefix != "" {
		q = append(q, "apiprefix="+escapeQuery(cfg.APIPrefix))
	}
	if cfg.Domain != "" {
		q = append(q, "domain="+escapeQuery(cfg.Domain))
	}
	if cfg.MaxReconnects != 0 {
		q = append(q, "maxreconnects="+strconv.Itoa(cfg.MaxReconnects))
	}
	if cfg.PingInterval != 0 {
		q = append(q, "pinginterval="+cfg.PingInterval.String())
	}
	if cfg.MaxPingsOut != 0 {
		q = append(q, "maxpingsout="+strconv.Itoa(cfg.MaxPingsOut))
	}
	if cfg.Token != "" {
		q = append(q, "token="+dsn.Redacted)
	}
	if cfg.CredentialsFile != "" {
		q = append(q, "creds="+escapeQuery(cfg.CredentialsFile))
	}
	if cfg.TLS != nil {
		q = append(q, "tls=true")
	}
	if cfg.InsecureSkipVerify {
		q = append(q, "insecureskipverify=true")
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
	errNoAddress        = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: an address in Addrs is empty")
	errBadAddress       = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: an address in Addrs is not printable ASCII; an international host name is dialled as punycode")
	errBadText          = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: a configured name, user or path carries a control character or is not valid UTF-8")
	errStreamName       = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: Stream is not a name JetStream accepts")
	errConsumerPrefix   = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: ConsumerPrefix is not a name JetStream accepts")
	errPrefix           = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: SubjectPrefix is not a subject token")
	errScheduleSubject  = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: ScheduleSubject is not a subject token")
	errStreamSubject    = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: a StreamSubjects entry is not a subject or pattern")
	errNoStreamSubjects = errcode.New(errcode.InvalidConfig,
		"iqmq/jetstream: ManageStream needs a SubjectPrefix or an explicit StreamSubjects; a stream that captured every subject would have to be created with NoAck, which discards every acknowledgement")
	errDeadLetterTopic     = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: DeadLetterTopic does not map to a subject")
	errStorageKind         = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: Storage is neither file nor memory")
	errPendingLimits       = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: the pending limits cannot be negative")
	errNegativeBound       = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: a stream or consumer bound cannot be negative")
	errAsyncPending        = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: PublishAsyncMaxPending cannot be negative; the specification forbids an unbounded queue")
	errNegativeDuration    = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: a timeout cannot be negative")
	errPings               = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: MaxPingsOut cannot be negative")
	errPrefixAndDomain     = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: name an APIPrefix or a Domain, not both")
	errPasswordWithoutUser = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: a password needs a user name")
	errTooManyCredentials  = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: name a user, a token or a credentials file, not several")
	errInsecureWithoutTLS  = errcode.New(errcode.InvalidConfig, "iqmq/jetstream: InsecureSkipVerify has no meaning without TLS")
	errNilRegistrar        = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: Register was given a nil Registrar")
	errNilConn             = errcode.New(errcode.InvalidArgument, "iqmq/jetstream: OpenWithClient was given a nil connection")
)

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

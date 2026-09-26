package redis

import (
	"crypto/tls"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/streams"
)

// DriverName is the driver's registered name and the scheme its DSN uses.
const DriverName = "redis"

// Version is the version of this driver module. It is empty until the module
// is tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// DefaultAddr is the address [Config] dials when none is given.
const DefaultAddr = "127.0.0.1:6379"

// The defaults a zero [Config] resolves to.
//
// Every one of them is a bound. The specification requires every queue to be bounded by
// default, and the specification requires a lease to have a length, so none of these may
// mean "unlimited" or "for ever" when it is left unset.
const (
	// DefaultAckDeadline is the lease a delivery gets, and therefore the
	// reclaim loop's min-idle: an entry idle for longer than this has
	// outlived its holder's claim on it.
	//
	// Thirty seconds is long enough for an ordinary handler and short enough
	// that a crashed consumer's work is picked up promptly. Redis grants no
	// lease of its own — nothing expires, nothing redelivers — so this figure
	// is entirely the driver's, which is why it is advertised as
	// FeatureExtendLease's condition rather than as a broker limit.
	DefaultAckDeadline = 30 * time.Second
	// DefaultClaimInterval is how often the mandatory reclaim loop sweeps.
	//
	// It is a third of the deadline, so a message whose holder died is
	// redelivered within one and a third deadlines rather than two. Sweeping
	// every deadline would double the worst-case redelivery latency for no
	// saving worth having: one XPENDING and one XAUTOCLAIM per shard is a
	// pipelined round trip.
	DefaultClaimInterval = DefaultAckDeadline / 3
	// DefaultClaimCount bounds how many entries one sweep round claims per
	// shard, so a pending list with a million entries in it is not one
	// command that blocks the server.
	DefaultClaimCount = 128
	// DefaultBlock is how long XREADGROUP parks when nothing is available.
	//
	// It is a bound rather than a preference: BLOCK 0 in Redis means "for
	// ever", and a fetch that cannot be woken is what the specification forbids. Five
	// seconds keeps an idle consumer at a fifth of a command per second and
	// keeps Close's latency below the block.
	DefaultBlock = 5 * time.Second
	// DefaultDialTimeout bounds the initial connection.
	DefaultDialTimeout = 5 * time.Second
	// DefaultMoverInterval is how often the delayed mover looks for parked
	// entries whose time has come. It bounds the zset strategy's delay
	// accuracy: a redelivery is late by at most this.
	DefaultMoverInterval = 250 * time.Millisecond
	// DefaultMoverCount bounds how many parked entries one mover pass
	// releases per shard.
	DefaultMoverCount = 128
	// DefaultDurableTimeout bounds the WAITAOF a ConfirmDurable publication
	// waits on.
	DefaultDurableTimeout = 5 * time.Second
)

// Config is the typed configuration of the Redis Streams driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). Every credential is a
// [github.com/iqhive/iqmq/dsn.Secret], so neither [Config.String] nor a slog
// record nor an error built from one can carry it.
//
// [Config.Validate] is pure: it performs no I/O and never dials, which is the specification's
// "validate config before starting background work".
type Config struct {
	// Addrs are the servers to dial. One address is a standalone server or a
	// cluster entry point; several are a cluster's seed nodes.
	//
	// Cluster and standalone are not configured differently here: rueidis
	// discovers a cluster from its seeds. What DOES differ is what a topic
	// costs — see [streams.Keys] — and the driver page says so.
	Addrs []string
	// Username and Password are the ACL credentials. A password with no
	// username is Redis's legacy `requirepass`, which is accepted.
	Username string
	Password dsn.Secret
	// DB is the logical database to select. It is refused for a cluster,
	// which has only database 0, and refusing rather than ignoring it is the
	// difference between a misconfiguration and a silent one.
	DB int
	// TLS is the TLS configuration. A non-nil value turns TLS on.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks so that it is greppable and obviously unsafe.
	InsecureSkipVerify bool
	// KeyPrefix is prepended to every key this driver builds, INSIDE the hash
	// tag, so that two iqmq deployments can share one Redis instance without
	// either one's shards being spread across a cluster's slots.
	//
	// It becomes part of the hash tag rather than sitting outside it: a
	// prefix outside the tag would leave the tag as the bare topic, so two
	// deployments' topics of the same name would collide on one slot and
	// their Lua routes would be able to touch each other's keys.
	KeyPrefix string
	// Partitions is how many shards a topic has. Zero and one both mean one.
	//
	// Shards buy ordering lanes and parallel consumers within one
	// subscription; they do NOT spread a topic across a cluster's nodes,
	// because every shard of a topic carries the topic's hash tag and
	// therefore lands on one slot. That is the price of being able to move a
	// message between two of a topic's keys atomically.
	Partitions int
	// MaxLen trims every stream this driver writes to approximately this many
	// entries (XADD MAXLEN ~ n). Zero leaves streams untrimmed.
	//
	// Untrimmed is the default deliberately. A stream is a log, and a driver
	// that trimmed by default would delete a subscription's unread messages
	// to save memory nobody said was short. The retention section says so.
	MaxLen int64
	// AutoCreate allows this driver to create a stream and a consumer group
	// that do not exist. It is off by default, which is the rule: nothing
	// creates topology behind an operator's back.
	AutoCreate bool
	// AckDeadline is the lease a delivery gets. Zero means
	// DefaultAckDeadline.
	AckDeadline time.Duration
	// ClaimInterval is how often the reclaim loop sweeps. Zero means
	// DefaultClaimInterval.
	ClaimInterval time.Duration
	// ClaimCount bounds one sweep round per shard. Zero means
	// DefaultClaimCount.
	ClaimCount int
	// Block is how long XREADGROUP parks. Zero means DefaultBlock; a
	// negative value is refused, because BLOCK 0 means "for ever".
	Block time.Duration
	// Nack is the negative-settlement strategy. The zero value
	// is Auto, which resolves to the recorded bake-off winner.
	Nack streams.Kind
	// AckGuard is how strongly an acknowledgement checks that the delivery it
	// settles is still this consumer's. The zero value is the owner guard,
	// which is the only setting under which a cross-process reclaim cannot
	// make this driver acknowledge another consumer's work.
	AckGuard streams.AckGuard
	// ConsumerPrefix is the first part of each source's consumer name; the
	// driver appends a nonce, always.
	//
	// The nonce is not configurable and the reason is load-bearing: the
	// guarded acknowledgement tells "somebody else claimed this entry" from
	// "we still hold it" by comparing the pending row's consumer name to this
	// source's, so two sources sharing a name would each believe the other's
	// deliveries were their own. Empty means the host name.
	ConsumerPrefix string
	// MoverInterval and MoverCount drive the delayed mover of the zset nack
	// strategy. Zero means the defaults.
	MoverInterval time.Duration
	MoverCount    int
	// DurableLocal and DurableReplicas are WAITAOF's arguments for a
	// ConfirmDurable publication: how many local and replica append-only
	// files the write must reach.
	//
	// DurableLocal defaults to 1 and DurableReplicas to 0, which is "the
	// write is in this server's AOF". Asking for replicas needs replicas, and
	// asking for the local AOF needs `appendonly yes`: both are conditions
	// the capability table states, and a server that cannot meet them refuses
	// ConfirmDurable at open rather than at the first publication.
	DurableLocal    int
	DurableReplicas int
	// DurableTimeout bounds the WAITAOF. Zero means DefaultDurableTimeout.
	DurableTimeout time.Duration
	// DialTimeout bounds the initial connection. Zero means
	// DefaultDialTimeout.
	DialTimeout time.Duration
	// DeadLetterTopic and DeadLetterSuffix name where a Reject routes, and
	// BOTH are empty by default — which means a Reject is refused with
	// ErrUnsupported.
	//
	// That is the rule and it is not a detail. A driver that invented a
	// destination out of the topic's name would make a Reject succeed on a
	// subscription nobody gave a dead-letter destination to, and the message
	// would be moved somewhere the operator has no consumer for — which is
	// indistinguishable from destroying it. The suite's
	// dlq/failed-publish-never-acks scenario is exactly that check, and it
	// failed against a real server while every unit test passed.
	//
	// DeadLetterTopic names a topic, resolved through the same key naming as
	// any other, so its key carries its OWN hash tag: the Lua move cannot
	// touch two slots, so the move becomes two commands and the duplicate
	// window between them is the documented cost.
	//
	// DeadLetterSuffix names a key beside the topic's own
	// (`{topic}` + suffix), which shares the tag and keeps the move atomic.
	// It is the setting to prefer when the deployment does not need the
	// dead-letter messages to be a topic of their own.
	//
	// DeadLetterTopic wins when both are set.
	DeadLetterTopic  string
	DeadLetterSuffix string
	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks.
	Hooks mq.Hooks
}

// DefaultDeadLetterSuffix is the suffix a deployment usually gives a
// dead-letter stream. It is NOT applied by default: see
// [Config.DeadLetterSuffix].
const DefaultDeadLetterSuffix = ":dlq"

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if len(c.Addrs) == 0 {
		c.Addrs = []string{DefaultAddr}
	}
	if c.Partitions < 1 {
		c.Partitions = 1
	}
	if c.AckDeadline == 0 {
		c.AckDeadline = DefaultAckDeadline
	}
	if c.ClaimInterval == 0 {
		c.ClaimInterval = c.AckDeadline / 3
		if c.ClaimInterval <= 0 {
			c.ClaimInterval = DefaultClaimInterval
		}
	}
	if c.ClaimCount == 0 {
		c.ClaimCount = DefaultClaimCount
	}
	if c.Block == 0 {
		c.Block = DefaultBlock
	}
	if c.MoverInterval == 0 {
		c.MoverInterval = DefaultMoverInterval
	}
	if c.MoverCount == 0 {
		c.MoverCount = DefaultMoverCount
	}
	if c.DurableLocal == 0 && c.DurableReplicas == 0 {
		c.DurableLocal = 1
	}
	if c.DurableTimeout == 0 {
		c.DurableTimeout = DefaultDurableTimeout
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	return c
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It is pure on purpose: the specification requires a driver to refuse a configuration before
// it starts background work, and a validation that dialled would turn a typo
// into a five-second timeout.
func (c Config) Validate() error {
	// An EMPTY Addrs is not an error: resolve fills in DefaultAddr, and a
	// backend built over a Commander the caller supplied
	// ([BackendWithCommander]) dials nothing at all. What is an error is an
	// address that is present and unusable.
	for _, a := range c.Addrs {
		if err := checkAddr(a); err != nil {
			return err
		}
	}
	if !printableText(c.Username) || !printableText(c.KeyPrefix) ||
		!printableText(c.ConsumerPrefix) || !printableText(c.DeadLetterSuffix) ||
		!printableText(c.DeadLetterTopic) {
		return errBadText
	}
	if strings.ContainsAny(c.KeyPrefix, "{}") {
		// The prefix goes INSIDE the hash tag, so a brace in it would end
		// the tag early and leave the shards of one topic on different
		// slots — which the Lua routes cannot work across.
		return errPrefixBraces
	}
	switch {
	case c.DB < 0:
		return errDB
	case c.Partitions < 0:
		return errPartitions
	case c.MaxLen < 0:
		return errMaxLen
	case c.AckDeadline < 0 || c.ClaimInterval < 0 || c.Block < 0 ||
		c.MoverInterval < 0 || c.DurableTimeout < 0 || c.DialTimeout < 0:
		return errNegativeDuration
	case c.ClaimCount < 0 || c.MoverCount < 0:
		return errNegativeCount
	case c.DurableLocal < 0 || c.DurableReplicas < 0:
		return errDurableCounts
	}
	if !c.Nack.Valid() {
		return errNackKind
	}
	if !c.AckGuard.Valid() {
		return errAckGuard
	}
	if c.Password != "" && c.Username == "" {
		// Redis's legacy `requirepass` is a password with no user, and the
		// ACL form is both. Neither is an error; a USERNAME with no password
		// is, because the ACL needs both.
		_ = c.Password
	}
	if c.Username != "" && c.Password == "" {
		return errUserWithoutPassword
	}
	if c.InsecureSkipVerify && c.TLS == nil {
		return errInsecureWithoutTLS
	}
	if c.DB > 0 && len(c.Addrs) > 1 {
		// A cluster has only database 0. Refusing names the mistake; a
		// driver that ignored the setting would talk to a database the
		// operator did not choose.
		return errDBWithCluster
	}
	return nil
}

// checkAddr refuses a server address that is not printable ASCII.
//
// It is not fussiness. An address goes into the dial and into every rendering
// of the configuration, so a byte that is not printable ASCII produces a log
// line and an error string that are not valid UTF-8. An international host
// name reaches Redis as punycode, which is ASCII, so nothing legitimate is
// refused here.
func checkAddr(a string) error {
	if strings.TrimSpace(a) == "" {
		return errNoAddress
	}
	for i := range len(a) {
		if ch := a[i]; ch <= ' ' || ch >= 0x7f {
			return errBadAddress
		}
	}
	return nil
}

// printableText reports whether a configured string is safe to render.
//
// Everything Validate calls this on ends up in [Config.DSN], which ends up in
// a log line and in an error, so a control byte or an invalid UTF-8 sequence
// here produces output no log pipeline can carry.
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
// other parameter is rendered, so what comes back does name the same broker
// and the same behaviour.
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
		b.WriteString(renderAddr(a))
	}
	if cfg.DB > 0 {
		b.WriteByte('/')
		b.WriteString(strconv.Itoa(cfg.DB))
	}
	q := []string{
		"partitions=" + strconv.Itoa(cfg.Partitions),
		"ackdeadline=" + cfg.AckDeadline.String(),
		"claiminterval=" + cfg.ClaimInterval.String(),
		"claimcount=" + strconv.Itoa(cfg.ClaimCount),
		"block=" + cfg.Block.String(),
		"nack=" + cfg.Nack.Resolve().String(),
		"ackguard=" + cfg.AckGuard.String(),
		"moverinterval=" + cfg.MoverInterval.String(),
		"movercount=" + strconv.Itoa(cfg.MoverCount),
		"durablelocal=" + strconv.Itoa(cfg.DurableLocal),
		"durablereplicas=" + strconv.Itoa(cfg.DurableReplicas),
		"durabletimeout=" + cfg.DurableTimeout.String(),
		"dialtimeout=" + cfg.DialTimeout.String(),
	}
	if cfg.DeadLetterTopic != "" {
		q = append(q, "deadlettertopic="+escapeQuery(cfg.DeadLetterTopic))
	}
	if cfg.DeadLetterSuffix != "" {
		q = append(q, "deadlettersuffix="+escapeQuery(cfg.DeadLetterSuffix))
	}
	if cfg.MaxLen > 0 {
		q = append(q, "maxlen="+strconv.FormatInt(cfg.MaxLen, 10))
	}
	if cfg.AutoCreate {
		q = append(q, "autocreate=true")
	}
	if cfg.KeyPrefix != "" {
		q = append(q, "keyprefix="+escapeQuery(cfg.KeyPrefix))
	}
	if cfg.ConsumerPrefix != "" {
		q = append(q, "consumerprefix="+escapeQuery(cfg.ConsumerPrefix))
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

// deadLetterKey is the stream a Reject routes to, or "" when the
// configuration names no destination.
//
// An empty answer is what makes Reject report ErrUnsupported, which is the specification's
// requirement for a subscription with no dead-letter destination.
func (c Config) deadLetterKey(topic string) (string, error) {
	switch {
	case c.DeadLetterTopic != "":
		keys, err := c.keysFor(c.DeadLetterTopic)
		if err != nil {
			return "", err
		}
		return keys.Stream(0), nil
	case c.DeadLetterSuffix != "":
		keys, err := c.keysFor(topic)
		if err != nil {
			return "", err
		}
		return keys.Stream(0) + c.DeadLetterSuffix, nil
	}
	return "", nil
}

// keysFor is the key naming of one topic under this configuration.
//
// The prefix goes inside the hash tag, which is what keeps a prefixed
// deployment's shards on one slot: [streams.NewKeys] tags the whole name it is
// given, so the name has to be the prefix and the topic together.
func (c Config) keysFor(topic string) (streams.Keys, error) {
	return streams.NewKeys(c.KeyPrefix+topic, c.Partitions)
}

// The configuration refusals. The specification names no sentinel for a bad configuration
// and errors.Is matches by code, so a caller compares mq.CodeOf(err) against
// mq.CodeInvalidConfig.
var (
	errNoAddress           = errcode.New(errcode.InvalidConfig, "iqmq/redis: an address in Addrs is empty")
	errBadAddress          = errcode.New(errcode.InvalidConfig, "iqmq/redis: an address in Addrs is not printable ASCII; an international host name is dialled as punycode")
	errBadText             = errcode.New(errcode.InvalidConfig, "iqmq/redis: a configured name or prefix carries a control character or is not valid UTF-8")
	errPrefixBraces        = errcode.New(errcode.InvalidConfig, "iqmq/redis: KeyPrefix may not contain '{' or '}': it goes inside the hash tag, and a brace in it would leave a topic's shards on different cluster slots")
	errDB                  = errcode.New(errcode.InvalidConfig, "iqmq/redis: DB cannot be negative")
	errDBWithCluster       = errcode.New(errcode.InvalidConfig, "iqmq/redis: a Redis Cluster has only database 0, so DB cannot be set beside several addresses")
	errPartitions          = errcode.New(errcode.InvalidConfig, "iqmq/redis: Partitions cannot be negative")
	errMaxLen              = errcode.New(errcode.InvalidConfig, "iqmq/redis: MaxLen cannot be negative")
	errNegativeDuration    = errcode.New(errcode.InvalidConfig, "iqmq/redis: a timeout cannot be negative")
	errNegativeCount       = errcode.New(errcode.InvalidConfig, "iqmq/redis: a count cannot be negative")
	errDurableCounts       = errcode.New(errcode.InvalidConfig, "iqmq/redis: WAITAOF's local and replica counts cannot be negative")
	errNackKind            = errcode.New(errcode.InvalidConfig, "iqmq/redis: Nack is not one of the strategies")
	errAckGuard            = errcode.New(errcode.InvalidConfig, "iqmq/redis: AckGuard is not one of the acknowledgement guards")
	errUserWithoutPassword = errcode.New(errcode.InvalidConfig, "iqmq/redis: an ACL user name needs a password")
	errInsecureWithoutTLS  = errcode.New(errcode.InvalidConfig, "iqmq/redis: InsecureSkipVerify has no meaning without TLS")
	errNilRegistrar        = errcode.New(errcode.InvalidArgument, "iqmq/redis: Register was given a nil Registrar")
	errNilClient           = errcode.New(errcode.InvalidArgument, "iqmq/redis: OpenWithClient was given a nil client")
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
//   - an address is also split on "," (the next node), on "@" and on ":" (the
//     port), and one that starts with "[" is read as a bracketed literal;
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space.
//
// There is no path set: this DSN's path is the logical database NUMBER, which
// holds only digits.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escUser  = "%:/?#"
	escHost  = "%,@:/?#"
	escQuery = "%+&#"
)

// escapeUser escapes an ACL user name for the DSN's credential position.
func escapeUser(s string) string { return escapeSet(s, escUser) }

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

// renderAddr renders one node address as a DSN endpoint that parses back to
// it.
//
// A host with a colon or a "%" in it is written as a bracketed literal, raw,
// which is how an IPv6 address reads and how it came in: the parser takes a
// bracketed host verbatim, without decoding it, so neither needs an escape
// there -- and a "%" escaped outside the brackets would triple, which is the
// one way a parsed host could otherwise render longer than it was written.
func renderAddr(a string) string {
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
// an endpoint into an address: a host with a colon in it is bracketed, and a
// port is the decimal digits after the last colon outside the brackets.
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

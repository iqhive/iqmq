package nats

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
)

// DriverName is the driver's registered name and the scheme its DSN uses.
const DriverName = "nats"

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
	// DefaultPendingMessages is the per-subscription pending-message limit
	// handed to SetPendingLimits.
	DefaultPendingMessages = 64 << 10
	// DefaultPendingBytes is the per-subscription pending-byte limit.
	DefaultPendingBytes = 64 << 20
	// DefaultConnectTimeout bounds the initial dial.
	DefaultConnectTimeout = 5 * time.Second
	// DefaultFlushTimeout bounds a confirmation round trip whose caller gave
	// no deadline of its own.
	DefaultFlushTimeout = 10 * time.Second
	// DefaultReconnectWait is the first delay between reconnection attempts.
	DefaultReconnectWait = 250 * time.Millisecond
	// DefaultDrainTimeout is nats.go's bound on a drain of the connection.
	DefaultDrainTimeout = 30 * time.Second
)

// Config is the typed configuration of the NATS Core driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). Every credential is a
// [github.com/iqhive/iqmq/dsn.Secret], so neither [Config.String] nor a slog
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
	// with. NKey and JWT authentication that is not a credentials file is
	// reached through [OpenWithClient], where the caller builds the
	// *nats.Conn and iqmq borrows it: an authentication scheme that needs a
	// signing callback has no form a typed Config or a DSN can carry.
	CredentialsFile string
	// TLS is the TLS configuration. A non-nil value turns TLS on.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks so that it is greppable and obviously unsafe.
	// It applies to TLS as well, on a copy, so a typed Config and a DSN
	// that set it behave alike.
	InsecureSkipVerify bool
	// SubjectPrefix is prepended to every mapped subject as its own token,
	// which is how two iqmq deployments share one NATS cluster. Empty means
	// the topic maps to the subject unchanged.
	SubjectPrefix string
	// PendingMessages and PendingBytes are the per-subscription limits given
	// to SetPendingLimits. Exceeding either makes the subscriber a slow
	// consumer: the server drops what it cannot deliver, nats.go reports it
	// through the asynchronous error handler and this driver's health
	// becomes Degraded.
	PendingMessages int
	PendingBytes    int
	// ConnectTimeout bounds the initial dial, for each server. Open checks
	// its context before the dial and not during it, because nats.go's
	// connect takes none, so this is what bounds a server that accepts the
	// connection and never answers.
	ConnectTimeout time.Duration
	// FlushTimeout bounds the confirmation round trip of a publication whose
	// caller's context has no deadline. It is never a second deadline on a
	// caller that has one.
	FlushTimeout time.Duration
	// ReconnectWait is the first delay between reconnection attempts.
	ReconnectWait time.Duration
	// MaxReconnects bounds the reconnection attempts. Zero means FOR EVER,
	// which is what a long-lived client wants and what
	// [github.com/iqhive/iqmq/driverkit.Reconnector] says of its own
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
	// DrainTimeout is nats.go's bound on a drain of a connection the driver
	// dials, which a caller starts on [Backend.Conn]. Close does not drain:
	// it closes every source and sink, then the connection when iqmq owns
	// it, and nats.go writes what is buffered before it closes the socket.
	DrainTimeout time.Duration
	// NoEcho asks the server not to deliver a connection its own
	// publications, which is what a process that both publishes and consumes
	// one subject usually wants and is off by default because it changes
	// what a subscriber sees.
	NoEcho bool
	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks. Only OnReconnect is fired
	// from here — a driver has no access to the client's own hooks, so the
	// reconnection and slow-consumer events this driver produces have
	// nowhere else to go (finding recorded in the maintenance notes).
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
	if c.PendingMessages == 0 {
		c.PendingMessages = DefaultPendingMessages
	}
	if c.PendingBytes == 0 {
		c.PendingBytes = DefaultPendingBytes
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.FlushTimeout == 0 {
		c.FlushTimeout = DefaultFlushTimeout
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
	return c
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
	if c.SubjectPrefix != "" {
		if err := CheckSubject(c.SubjectPrefix, false); err != nil {
			return errPrefix
		}
	}
	if !printableText(c.Name) || !printableText(c.CredentialsFile) || !printableText(c.Username) {
		return errBadText
	}
	switch {
	case c.PendingMessages < 0 || c.PendingBytes < 0:
		return errPendingLimits
	case c.ConnectTimeout < 0 || c.FlushTimeout < 0 || c.ReconnectWait < 0 ||
		c.PingInterval < 0 || c.DrainTimeout < 0:
		return errNegativeDuration
	case c.MaxPingsOut < 0:
		return errPings
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
// line and an error string that are not valid UTF-8 — which FuzzNATSDSN found
// on the first run, from a DSN whose authority was a single 0xc5 byte. A
// non-ASCII host name reaches NATS as punycode, which is ASCII, so nothing
// legitimate is refused here.
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
// produces output no log pipeline can carry. FuzzNATSDSN found the first two
// (a raw 0xc5 in the authority and a 0x9a in creds=) within a second of being
// written.
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
// other parameter is rendered, so what comes back does name the same broker and
// the same behaviour.
//
// Every free-text component is escaped for the position it lands in, because
// the shared parser DECODES every one of them: a host, a user or a name that
// came out of a DSN can hold any byte, and one written back raw is a different
// DSN or none. The JetStream driver's renderer had the same shape and
// FuzzJetStreamDSN found it there first.
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
	q := make([]string, 0, 8)
	q = append(q,
		"name="+escapeQuery(cfg.Name),
		"pendingmessages="+strconv.Itoa(cfg.PendingMessages),
		"pendingbytes="+strconv.Itoa(cfg.PendingBytes),
		"connecttimeout="+cfg.ConnectTimeout.String(),
		"flushtimeout="+cfg.FlushTimeout.String(),
		"reconnectwait="+cfg.ReconnectWait.String(),
		"draintimeout="+cfg.DrainTimeout.String(),
	)
	if cfg.MaxReconnects != 0 {
		q = append(q, "maxreconnects="+strconv.Itoa(cfg.MaxReconnects))
	}
	// Both are DSN parameters, and the rendering used to leave them out, so a
	// configuration that set either came back from its own rendering without
	// it. TestEverySettingSurvivesTheRendering found it once it compared fields
	// rather than renderings.
	if cfg.PingInterval != 0 {
		q = append(q, "pinginterval="+cfg.PingInterval.String())
	}
	if cfg.MaxPingsOut != 0 {
		q = append(q, "maxpingsout="+strconv.Itoa(cfg.MaxPingsOut))
	}
	if cfg.NoEcho {
		q = append(q, "noecho=true")
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
	errNoAddress           = errcode.New(errcode.InvalidConfig, "iqmq/nats: an address in Addrs is empty")
	errBadText             = errcode.New(errcode.InvalidConfig, "iqmq/nats: a configured name, user or path carries a control character or is not valid UTF-8")
	errBadAddress          = errcode.New(errcode.InvalidConfig, "iqmq/nats: an address in Addrs is not printable ASCII; an international host name is dialled as punycode")
	errPrefix              = errcode.New(errcode.InvalidConfig, "iqmq/nats: SubjectPrefix is not a subject token")
	errPendingLimits       = errcode.New(errcode.InvalidConfig, "iqmq/nats: the pending limits cannot be negative")
	errNegativeDuration    = errcode.New(errcode.InvalidConfig, "iqmq/nats: a timeout cannot be negative")
	errPings               = errcode.New(errcode.InvalidConfig, "iqmq/nats: MaxPingsOut cannot be negative")
	errPasswordWithoutUser = errcode.New(errcode.InvalidConfig, "iqmq/nats: a password needs a user name")
	errTooManyCredentials  = errcode.New(errcode.InvalidConfig, "iqmq/nats: name a user, a token or a credentials file, not several")
	errInsecureWithoutTLS  = errcode.New(errcode.InvalidConfig, "iqmq/nats: InsecureSkipVerify has no meaning without TLS")
	errNilRegistrar        = errcode.New(errcode.InvalidArgument, "iqmq/nats: Register was given a nil Registrar")
	errNilConn             = errcode.New(errcode.InvalidArgument, "iqmq/nats: OpenWithClient was given a nil connection")
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

// The escapers a rendering needs, one per position, so that a rendered DSN
// parses back to the configuration it came from.
//
// The shared parser DECODES every component except a bracketed host, so a
// component can hold any byte and only an escaped one survives the trip.
// Each position escapes exactly the bytes the parser gives
// a meaning to THERE, and no more, for two reasons. A log line is read by a
// person: "deadlettertopic=orders/dlq", "alice@example.com@host" and
// "[::1]:4222" are what they should see. And an escape triples a byte, so a
// rendering that escaped more than its input had to would grow past
// `dsn.MaxLen` and stop parsing, which is a failure the escaping would have
// caused. With these sets a parsed endpoint, user, path or value never renders
// longer than it was written.
//
//   - a user ends at the first ":" (the password) and the authority at "/",
//     "?" or "#"; an "@" in it is safe, because the credential ends at the
//     LAST one;
//   - a host is also split on "," (the next endpoint), on "@" and on ":" (the
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

// escapeUser escapes a user name.
func escapeUser(s string) string { return escapeSet(s, escUser) }

// escapePath escapes the path.
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

// renderAddr renders a dial address as a DSN endpoint that parses back to it.
//
// A host with a colon or a "%" in it is written as a bracketed literal, raw,
// which is how an IPv6 address reads and how it came in: the parser takes a
// bracketed host verbatim, without decoding it, so neither needs an escape
// there -- and a "%" escaped outside the brackets would triple, which is the
// one way a parsed host could otherwise render longer than it was written.
// That works unless the host holds a byte the authority is split on before
// the brackets are seen, and such a host is escaped instead, colons and all.
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
// port is the decimal digits after the last colon outside the brackets. It is
// exact for every address that function produces; anything else comes back
// whole as the host.
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

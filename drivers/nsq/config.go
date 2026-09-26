package nsq

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
const DriverName = "nsq"

// Version is the version of this driver module. It is empty until the module is
// tagged: the specification makes an empty string "the driver did not say" rather than a
// claim.
const Version = ""

// ClientModule and ClientVersion name the client library this driver wraps.
const (
	ClientModule  = "github.com/nsqio/go-nsq"
	ClientVersion = "v1.1.0"
)

// DefaultAddr is the nsqd address [Config] dials when none is given.
const DefaultAddr = "127.0.0.1:4150"

// The defaults a zero [Config] resolves to.
//
// The three that mirror an nsqd flag say so, because they are the one class of
// default this driver cannot discover: nsqd does not report its own limits over
// either protocol, so an operator who raises `--max-msg-size` has to raise
// [Config.MaxMessageBytes] with it  -- and, when the new
// limit exceeds the body that carries it, `--max-body-size` and
// [Config.MaxBodyBytes] too. That last pair is REFUSED rather than reconciled:
// raising MaxBodyBytes to match would be this driver claiming an nsqd setting
// it cannot change, and every MPUB it then considered legal would come back
// E_BAD_BODY. The reverse -- an explicit MaxBodyBytes pulling a defaulted
// MaxMessageBytes down to it -- is applied silently, because it only makes
// this driver stricter than the broker.
const (
	// DefaultMaxMessageBytes mirrors nsqd's `--max-msg-size` default. It is
	// the limit this driver advertises and enforces before any I/O.
	DefaultMaxMessageBytes = 1 << 20
	// DefaultMaxBodyBytes mirrors nsqd's `--max-body-size` default, which
	// bounds a whole MPUB rather than one message.
	DefaultMaxBodyBytes = 5 << 20
	// DefaultMaxRequeueDelay mirrors nsqd's `--max-req-timeout` default,
	// which bounds a REQ's delay and a DPUB's.
	DefaultMaxRequeueDelay = time.Hour
	// DefaultAckDeadline is the per-message timeout asked of nsqd, which is
	// nsqd's own `--msg-timeout` default.
	DefaultAckDeadline = time.Minute
	// MinAckDeadline mirrors nsqd's `--min-msg-timeout` default, which is the
	// shortest per-message timeout nsqd will grant. A source asking for less
	// is refused here rather than by the IDENTIFY handshake, which would
	// arrive as every operation on the subscription failing with an error
	// about a handshake.
	MinAckDeadline = time.Second
	// DefaultMaxAckDeadline mirrors nsqd's `--max-msg-timeout` default, which
	// is the longest per-message timeout nsqd will grant. It is the
	// advertised MaxLease, and an AckDeadline past it is refused by
	// nsqd's IDENTIFY handshake rather than by a publication.
	DefaultMaxAckDeadline = 15 * time.Minute
	// DefaultLeaseGuard is how long before a lease expires this driver stops
	// vouching for the delivery. See [Config.LeaseGuard].
	DefaultLeaseGuard = 2 * time.Second
	// DefaultDialTimeout bounds one connection attempt.
	DefaultDialTimeout = 5 * time.Second
	// DefaultReadTimeout and DefaultWriteTimeout are the connection
	// deadlines. The read timeout must exceed the heartbeat interval or
	// go-nsq reports a dead connection on an idle one.
	DefaultReadTimeout  = 60 * time.Second
	DefaultWriteTimeout = 5 * time.Second
	// DefaultHeartbeatInterval is how often nsqd is asked to send a
	// heartbeat.
	DefaultHeartbeatInterval = 30 * time.Second
	// DefaultLookupdPollInterval is how often nsqlookupd is asked which nsqd
	// instances hold the topic. It is short relative to go-nsq's own default
	// of a minute, because a topic created moments ago is the normal case in
	// a test and a minute of it is not.
	DefaultLookupdPollInterval = 5 * time.Second
	// DefaultOutputBufferTimeout is how long nsqd may hold a write before
	// flushing it to this client. go-nsq's default is 250 ms, which adds a
	// quarter of a second to the latency of every otherwise-idle delivery;
	// this is nsqd's own floor instead.
	DefaultOutputBufferTimeout = MinOutputBufferTimeout
	// MinOutputBufferTimeout mirrors nsqd's `--min-output-buffer-timeout`
	// default, which is the smallest value nsqd will accept in the IDENTIFY
	// handshake. A client that asks for less is refused with
	// `E_BAD_BODY IDENTIFY output buffer timeout (n) is invalid` — and that
	// refusal arrives as a failed CONNECTION rather than as a failed
	// publication, so every operation on the client fails with an error
	// about a handshake. Refusing it here instead is the specification's "validate config
	// before starting background work".
	MinOutputBufferTimeout = 25 * time.Millisecond
	// DefaultAPITimeout bounds one nsqd HTTP administrative request.
	DefaultAPITimeout = 10 * time.Second
	// DefaultMaxInFlight is the per-consumer RDY budget used when the core
	// resolves no prefetch. The core always resolves one, so this is a
	// backstop rather than a policy.
	DefaultMaxInFlight = 1
)

// Config is the typed configuration of the NSQ driver.
//
// A production program writes one of these; a DSN is the convenience form and
// parses into exactly this ([ParseDSN]). Every credential is a
// [github.com/iqhive/iqmq/dsn.Secret], so neither [Config.String] nor a slog
// record nor an error built from one can carry it.
//
// [Config.Validate] is pure: it performs no I/O and never dials, which is the specification's
// "validate config before starting background work".
type Config struct {
	// Addrs are the nsqd TCP addresses. Publication always goes directly to
	// one of these — nsqlookupd is a discovery service for CONSUMERS and
	// cannot be published through — so a configuration with no Addrs can
	// consume and not publish, and Validate says so.
	Addrs []string
	// LookupdAddrs are the nsqlookupd HTTP addresses a consumer discovers
	// nsqd instances through. When it is empty a consumer connects to Addrs
	// directly, which is correct for one nsqd and wrong for a cluster: a
	// direct consumer never learns about an nsqd that joins later.
	//
	// Each may be a full URL -- go-nsq prepends "http://" only when there is
	// no scheme, and keeps a path and a query -- so "http://lookupd:4161"
	// and "https://lookupd:4161/lookup" are both accepted.
	//
	// DISCOVERY IS CLEARTEXT UNLESS THE ENTRY SAYS "https://", AND [Config.TLS]
	// DOES NOT CHANGE THAT. TLS here is nsqd's, on the TCP connection; the
	// scheme in this list is what decides what discovery does, and go-nsq
	// prepends "http://" when there is none -- so a scheme-less entry beside a
	// TLS config is cleartext too. Measured on the wire rather than inferred:
	// `lookupd_wire_test.go` reads the first bytes off a loopback listener for
	// all three spellings.
	//
	// It is NOT refused, and that is a decision rather than an oversight.
	// nsqlookupd is a different service from nsqd, and a
	// TLS nsqd behind a plain-HTTP nsqlookupd on a private network is a real
	// layout. What that cleartext request discloses is the topic name in the
	// query and the nsqd host names in the answer.
	//
	// It discloses no CREDENTIAL, and that took a correction rather than a
	// reading. go-nsq sends [Config.AuthSecret] to
	// nsqLOOKUPD as well as to nsqd unless told not to -- its own
	// `LookupdAuthorization` defaults to TRUE -- so an `http://` lookupd
	// address beside an AuthSecret put `Authorization: Bearer <secret>` on a
	// cleartext wire. Measured, and the opposite of what was recorded. This
	// driver turns that field off (see `clientConfig`), so the secret goes to
	// nsqd's AUTH handshake and nowhere else.
	//
	// Anyone for whom the topic and host names matter writes "https://", and
	// then [Config.TLS] IS what the poller uses: its roots, its client
	// certificate and its InsecureSkipVerify. That last part was not true
	// before, because go-nsq's own lookupd transport leaves TLSClientConfig
	// unset (see `newConsumer`).
	//
	// It may NOT carry user information. go-nsq would forward HTTP basic
	// auth in such a URL, and this driver refuses it: the credential would
	// reach every rendering of the Config and so every log line, and
	// an "@" in one of these is also the byte that moves the request to
	// another host, which is what the specification found. A lookupd behind
	// basic auth needs a proxy or an unauthenticated path; nsqd's own shared
	// secret goes in AuthSecret.
	LookupdAddrs []string
	// AdminAddrs are the nsqd HTTP addresses the Admin surface uses, each a
	// HOST AND PORT and not a URL: the Admin surface builds
	// "http://" + addr + path by concatenation, so an entry carrying a
	// scheme would produce "http://http://...". An "@", "/", "?", "#" or a
	// backslash in one is refused, because each of them moves the request
	// that carries AuthSecret as a bearer token.
	//
	// Empty means "derive from Addrs by adding one to each TCP port", which is
	// nsqd's own default layout (4150 and 4151) and is documented as a guess
	// rather than a discovery. The Admin surface talks to
	// them over an HTTP transport of the backend's own, cloned from
	// http.DefaultTransport at Open, so what an application set there before
	// Open applies and its pool is released by Close.
	AdminAddrs []string
	// TopicPrefix is prepended to every mapped topic, which is how two iqmq
	// deployments share one nsqd. Empty means the topic maps unchanged.
	TopicPrefix string
	// ClientID, Hostname and UserAgent are the identifiers nsqd reports in
	// its own statistics. Empty means go-nsq's defaults.
	ClientID  string
	Hostname  string
	UserAgent string
	// AuthSecret is the shared secret for nsqd's AUTH handshake.
	AuthSecret dsn.Secret
	// TLS is the TLS configuration. A non-nil value turns TLS on, for the
	// protocol connection and for the Admin surface's HTTPS requests alike,
	// and both verify each nsqd against its own host name: go-nsq replaces
	// ServerName with the host it dials, and the Admin surface clears it.
	TLS *tls.Config
	// InsecureSkipVerify disables server certificate verification. It is
	// named the way the specification asks so that it is greppable and obviously unsafe.
	// It takes effect through a DSN only: ParseDSN writes it into the
	// tls.Config it builds. Set on a Config beside a caller's own TLS it is
	// validated and NOT applied -- set TLS.InsecureSkipVerify instead
	// (hack/FOLLOWUPS.md records the gap).
	InsecureSkipVerify bool
	// Deflate and Snappy select a wire compression. They are mutually
	// exclusive and nsqd has to have the matching option enabled.
	Deflate bool
	// DeflateLevel is the deflate level to ask nsqd for, 1 to 9. Zero leaves
	// go-nsq's default and is only read when Deflate is set.
	DeflateLevel int
	// Snappy is the other of the two, and excludes Deflate.
	Snappy bool
	// EnsureTopology creates the topic and the channel when a source or a
	// sink is opened. It is OFF by default, which is the specification's "no hidden
	// topology creation" — and on NSQ that rule needs saying twice, because
	// nsqd creates a topic on first publish whatever any client asks. What
	// this flag controls is whether the DRIVER creates one deliberately,
	// through nsqd's HTTP API, so that a consumer does not sit on an empty
	// channel that does not exist yet.
	EnsureTopology bool
	// MaxMessageBytes is the largest single message this driver will
	// publish, mirroring nsqd's `--max-msg-size`. It is both the advertised
	// limit and the bound enforced before any I/O.
	MaxMessageBytes int64
	// MaxBodyBytes is the largest MPUB body this driver will send, mirroring
	// nsqd's `--max-body-size`. A batch larger than this is split.
	MaxBodyBytes int64
	// MaxRequeueDelay bounds a requeue delay and a scheduled publication,
	// mirroring nsqd's `--max-req-timeout`.
	MaxRequeueDelay time.Duration
	// AckDeadline is the per-message timeout asked of nsqd (`msg_timeout` in
	// the IDENTIFY handshake). It is what a delivery's lease is, and the
	// source's own AckDeadline overrides it.
	AckDeadline time.Duration
	// MaxAckDeadline mirrors nsqd's `--max-msg-timeout`: the longest
	// per-message timeout nsqd will grant. It is advertised as the specification's
	// MaxLease and a source asking for more is refused before the
	// connection is made, because nsqd refuses it in the IDENTIFY handshake
	// and the failure would otherwise arrive as a dead subscription.
	MaxAckDeadline time.Duration
	// LeaseGuard is how long before a lease expires this driver stops
	// vouching for a delivery.
	//
	// It exists because nsqd answers no settlement. A FIN for a message
	// whose timeout has already expired is accepted and silently settles
	// whatever nsqd now has under that identifier — which after a
	// redelivery is another consumer's work. There is no verb that asks "do
	// I still hold this?", so the only place the refusal can happen is
	// locally, before the frame is written.
	LeaseGuard time.Duration
	// DialTimeout, ReadTimeout and WriteTimeout are the connection
	// deadlines.
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// HeartbeatInterval is how often nsqd sends this client a heartbeat. It
	// must be less than ReadTimeout.
	HeartbeatInterval time.Duration
	// LookupdPollInterval is how often nsqlookupd is polled. When there is
	// no lookupd it is how often a direct connection is retried.
	LookupdPollInterval time.Duration
	// OutputBufferTimeout is how long nsqd may hold a write before flushing
	// it to this client. It is either zero (the default) or at least
	// [MinOutputBufferTimeout]: nsqd refuses less in its IDENTIFY handshake.
	OutputBufferTimeout time.Duration
	// APITimeout bounds one nsqd HTTP administrative request.
	APITimeout time.Duration
	// Metadata is the metadata mode a publisher or consumer that did not
	// name one gets. The zero value, MetadataNativeStrict, is "no override":
	// every handle keeps the mode the core hands the driver.
	//
	// It exists because the specification makes the header-less-backend envelope an
	// INTEROP-AFFECTING emulation, available "under EmulationAll, or
	// explicit metadata mode" — and the SPI carries no emulation policy at
	// all, so there is nowhere else for a deployment to say "yes, envelope
	// on this broker" once rather than on every handle. On a broker with no
	// headers that is the difference between a portable API and one that
	// refuses a key.
	//
	// A handle that names MetadataNativeStrict EXPLICITLY is
	// indistinguishable from one that named nothing, so setting this field
	// means no handle on this client can be in strict mode. The conformance
	// suite documents `metadata/native-strict-rejection` as inapplicable for
	// exactly that reason, and the driver's own tests cover the refusal.
	Metadata mq.MetadataMode
	// Clock is the time source the driver reads. Nil means the wall clock.
	Clock mq.Clock
	// Hooks are the observability hooks. Only OnReconnect is fired
	// from here: the rest belong to the core, which has the publication and
	// the delivery.
	Hooks mq.Hooks
}

// resolve returns a copy of cfg with every default filled in, so that nothing
// below this point has to ask what a zero means.
func (c Config) resolve() Config {
	if len(c.Addrs) == 0 && len(c.LookupdAddrs) == 0 {
		c.Addrs = []string{DefaultAddr}
	}
	// These two mirror nsqd FLAGS (--max-msg-size and --max-body-size), which
	// is why the reconciliation here is one-directional where the is symmetric. Pulling the message limit DOWN to an explicit
	// body limit is safe: it makes this driver refuse more locally than the
	// broker would. Pushing the body limit UP to an explicit message limit is
	// not -- it would be this driver claiming a broker setting it cannot
	// change, and MPUBs it then considered legal come back E_BAD_BODY. That
	// case is refused in Validate instead.
	if c.MaxMessageBytes == 0 {
		c.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.MaxBodyBytes < c.MaxMessageBytes {
		c.MaxMessageBytes = c.MaxBodyBytes
	}
	if c.MaxRequeueDelay == 0 {
		c.MaxRequeueDelay = DefaultMaxRequeueDelay
	}
	if c.AckDeadline == 0 {
		c.AckDeadline = DefaultAckDeadline
	}
	if c.MaxAckDeadline == 0 {
		c.MaxAckDeadline = DefaultMaxAckDeadline
	}
	if c.LeaseGuard == 0 {
		c.LeaseGuard = DefaultLeaseGuard
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = DefaultDialTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.LookupdPollInterval == 0 {
		c.LookupdPollInterval = DefaultLookupdPollInterval
	}
	if c.OutputBufferTimeout == 0 {
		c.OutputBufferTimeout = DefaultOutputBufferTimeout
	}
	if c.APITimeout == 0 {
		c.APITimeout = DefaultAPITimeout
	}
	if c.DeflateLevel == 0 {
		c.DeflateLevel = 6
	}
	return c
}

// Validate reports whether the configuration can be used, without any I/O.
//
// It is pure on purpose: the specification requires a driver to refuse a configuration before
// it starts background work, and a validation that dialled would turn a typo
// into a five-second timeout.
func (c Config) Validate() error {
	if len(c.Addrs) == 0 && len(c.LookupdAddrs) == 0 {
		return errNoAddress
	}
	for _, a := range c.Addrs {
		// A host and a port: go-nsq dials it over TCP, and adminAddrs turns
		// it into an nsqd HTTP address by adding one to the port.
		if err := checkAddr(a, true); err != nil {
			return err
		}
	}
	for _, a := range c.LookupdAddrs {
		// May be a full URL; go-nsq's buildLookupAddr keeps a path and a
		// query and prepends "http://" only when there is no scheme.
		if err := checkAddr(a, false); err != nil {
			return err
		}
	}
	for _, a := range c.AdminAddrs {
		// A host and a port: Backend.do concatenates it into an HTTP URL.
		if err := checkAddr(a, true); err != nil {
			return err
		}
	}
	if c.TopicPrefix != "" {
		if err := CheckTopic(c.TopicPrefix + "x"); err != nil {
			return errPrefix
		}
	}
	if !printableText(c.ClientID) || !printableText(c.Hostname) || !printableText(c.UserAgent) {
		return errBadText
	}
	switch {
	case c.MaxMessageBytes < 0 || c.MaxBodyBytes < 0:
		return errNegativeLimit
	case c.MaxMessageBytes > 0 && effectiveBodyBytes(c) < c.MaxMessageBytes:
		// An MPUB body has to be able to carry at least one message, or
		// every batch would be split into pieces that still do not fit.
		//
		// An explicit MaxMessageBytes above the EFFECTIVE body limit is
		// refused, whether the body limit was written or defaulted, because
		// both constants mirror an nsqd flag: raising the message limit past
		// the body that carries it is a configuration nsqd answers with
		// E_BAD_BODY, and silently raising the body to match would be this
		// driver claiming a broker setting it cannot change. The error names
		// MaxBodyBytes, which is what the operator has to raise too.
		//
		// The other direction is NOT refused: a one-sided
		// "maxbodybytes=524288" means "this broker's max body is 512 KiB",
		// and resolve pulls the defaulted message limit down to it, which
		// only makes this driver stricter than the broker.
		return errBodySmallerThanMessage
	case c.MaxAckDeadline < 0:
		return errNegativeDuration
	case c.AckDeadline < 0 || c.LeaseGuard < 0 || c.MaxRequeueDelay < 0 ||
		c.DialTimeout < 0 || c.ReadTimeout < 0 || c.WriteTimeout < 0 ||
		c.HeartbeatInterval < 0 || c.LookupdPollInterval < 0 ||
		c.OutputBufferTimeout < 0 || c.APITimeout < 0:
		return errNegativeDuration
	}
	r := c.resolve()
	if r.OutputBufferTimeout < MinOutputBufferTimeout {
		return errOutputBufferTooSmall
	}
	if r.AckDeadline < MinAckDeadline {
		return errAckDeadlineBelowMin
	}
	if r.HeartbeatInterval >= r.ReadTimeout {
		// go-nsq reads with a deadline and nsqd only writes a heartbeat;
		// an interval at or past the deadline makes every idle connection
		// look dead, which reads as a broker that keeps dropping out.
		return errHeartbeatPastRead
	}
	if r.AckDeadline > r.MaxAckDeadline {
		// nsqd refuses a msg_timeout past --max-msg-timeout in the
		// IDENTIFY handshake, so the alternative to refusing here is a
		// subscription that never receives anything.
		return errAckDeadlinePastMax
	}
	if r.LeaseGuard >= r.AckDeadline {
		// A guard at or past the deadline refuses every settlement,
		// including the first.
		return errGuardPastDeadline
	}
	if c.Deflate && c.Snappy {
		return errBothCompressions
	}
	if c.DeflateLevel != 0 && (c.DeflateLevel < 1 || c.DeflateLevel > 9) {
		return errDeflateLevel
	}
	if !c.Metadata.Valid() {
		return errMetadataMode
	}
	if c.InsecureSkipVerify && c.TLS == nil {
		return errInsecureWithoutTLS
	}
	return nil
}

// checkAddr refuses an address that is not printable ASCII, and one that would
// send a request somewhere the configuration does not name.
//
// The printable-ASCII half is not fussiness. An address goes into the dial and
// into every rendering of the configuration, so a byte that is not printable
// ASCII produces a log line and an error string that are not valid UTF-8. A
// non-ASCII host name reaches the resolver as punycode, which is ASCII, so
// nothing legitimate is refused here (the same reasoning as).
//
// The second half is this driver's dial-side twin, which the specification's
// sweep MISSED because it looked only for a driver that builds a dial URL out
// of a DSN HOST. nsq builds HTTP URLs out of its three ADDRESS LISTS instead:
// `Backend.do` concatenates "http://" + addr + path (admin.go) and then sets
// `Authorization: Bearer <AuthSecret>` on that request, and go-nsq's
// `buildLookupAddr` (consumer.go) does the same for every LookupdAddrs entry.
// Measured against an httptest listener with the exact concatenation:
// `nsq://h:4150?admin=tok%40OTHER&authsecret=s3cret` sent
// "Bearer s3cret" to OTHER, and so did the same shape in `lookupd=` and in the
// plain address (which `adminAddrs` turns into an admin address by port+1).
//
// An "@" is refused in EVERY list, because it is user information wherever
// net/url reads one of these and it is the byte that moves the host. "/", "?"
// and "#" are refused only where the list is a host and a port: `Addrs` are
// nsqd TCP addresses and `AdminAddrs` are nsqd HTTP addresses, so anything
// that ends the authority there moves the request. `LookupdAddrs` may be full
// URLs -- go-nsq prepends "http://" only when the address has no "://", and
// keeps a path and a query -- which is the form this repository's own doc.go
// and example use, so a "/" in one is legitimate and stays.
func checkAddr(a string, hostPortOnly bool) error {
	if strings.TrimSpace(a) == "" {
		return errNoAddress
	}
	for i := range len(a) {
		if ch := a[i]; ch <= ' ' || ch >= 0x7f {
			return errBadAddress
		}
	}
	if strings.ContainsRune(a, '@') {
		return errAddrCredential
	}
	if strings.ContainsAny(a, "\\") {
		return errAddrNotHostPort
	}
	if hostPortOnly && strings.ContainsAny(a, "/?#") {
		return errAddrNotHostPort
	}
	// nsq joined the dial-redirect family and was
	// then left out of the specification's "%" rule, which it qualifies for by the same
	// mechanism: `Backend.do` and go-nsq's buildLookupAddr both hand their
	// concatenated string to net/url, which rejects every "%XY" in a host but
	// "%25" -- so "nsq://a%25b:4150" fails at the HTTP request with "invalid
	// URL escape" rather than at construction. The exception is the IPv6 zone
	// id, whose "%" always sits beside the literal's own colons.
	if host, _ := splitAddr(hostOf(a)); strings.ContainsRune(host, '%') && !strings.ContainsRune(host, ':') {
		return errAddrNotHostPort
	}
	return nil
}

// hostOf is an address with any scheme and anything after the authority
// removed, which is what a lookupd entry may legitimately carry.
func hostOf(a string) string {
	if i := strings.Index(a, "://"); i >= 0 {
		a = a[i+3:]
	}
	if i := strings.IndexAny(a, "/?#"); i >= 0 {
		a = a[:i]
	}
	return a
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
// It is not a round trip and is not meant to be one: the secret is gone from
// the result on purpose, which is the whole reason the rendering exists.
func (c Config) DSN() string {
	cfg := c.resolve()
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	for i, a := range cfg.Addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(renderAddr(a))
	}
	if cfg.TopicPrefix != "" {
		b.WriteByte('/')
		b.WriteString(escapePath(cfg.TopicPrefix))
	}
	q := make([]string, 0, 16)
	if len(cfg.LookupdAddrs) > 0 {
		q = append(q, "lookupd="+escapeList(cfg.LookupdAddrs))
	}
	if len(cfg.AdminAddrs) > 0 {
		q = append(q, "admin="+escapeList(cfg.AdminAddrs))
	}
	q = append(q,
		"maxmessagebytes="+strconv.FormatInt(cfg.MaxMessageBytes, 10),
		"maxbodybytes="+strconv.FormatInt(cfg.MaxBodyBytes, 10),
		"maxrequeuedelay="+cfg.MaxRequeueDelay.String(),
		"ackdeadline="+cfg.AckDeadline.String(),
		"maxackdeadline="+cfg.MaxAckDeadline.String(),
		"leaseguard="+cfg.LeaseGuard.String(),
		"dialtimeout="+cfg.DialTimeout.String(),
		"readtimeout="+cfg.ReadTimeout.String(),
		"writetimeout="+cfg.WriteTimeout.String(),
		"heartbeatinterval="+cfg.HeartbeatInterval.String(),
		"lookupdpollinterval="+cfg.LookupdPollInterval.String(),
		"outputbuffertimeout="+cfg.OutputBufferTimeout.String(),
		"apitimeout="+cfg.APITimeout.String(),
		// Unconditional, because the parser accepts it on its own: it used
		// to be written only beside "deflate=true", so "deflatelevel=9"
		// parsed and then rendered as the default 6.
		"deflatelevel="+strconv.Itoa(cfg.DeflateLevel),
	)
	if cfg.ClientID != "" {
		q = append(q, "clientid="+escapeQuery(cfg.ClientID))
	}
	if cfg.Hostname != "" {
		q = append(q, "hostname="+escapeQuery(cfg.Hostname))
	}
	if cfg.UserAgent != "" {
		q = append(q, "useragent="+escapeQuery(cfg.UserAgent))
	}
	if cfg.AuthSecret != "" {
		q = append(q, "authsecret="+dsn.Redacted)
	}
	if cfg.Metadata != mq.MetadataNativeStrict {
		q = append(q, "metadata="+cfg.Metadata.String())
	}
	if cfg.EnsureTopology {
		q = append(q, "ensuretopology=true")
	}
	if cfg.Deflate {
		q = append(q, "deflate=true")
	}
	if cfg.Snappy {
		q = append(q, "snappy=true")
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

type (
	stringer  interface{ String() string }
	logValuer interface{ LogValue() slog.Value }
)

// The compile-time proof that a Config cannot be logged un-redacted: both
// renderings go through DSN, and slog prefers LogValue over String.
var (
	_ stringer  = Config{}
	_ logValuer = Config{}
)

// The configuration refusals. They are preallocated because the specification forbids
// fmt.Errorf on a driver's hot paths and because a sentinel is what errors.Is
// can match.
var (
	errNoAddress    = errcode.New(errcode.InvalidConfig, "iqmq/nsq: no nsqd address and no nsqlookupd address; one of them is needed")
	errBadAddress   = errcode.New(errcode.InvalidConfig, "iqmq/nsq: an address contains a byte that is not printable ASCII")
	errBadText      = errcode.New(errcode.InvalidConfig, "iqmq/nsq: a configured identifier contains a control byte or invalid UTF-8")
	errPrefix       = errcode.New(errcode.InvalidConfig, "iqmq/nsq: the topic prefix is not a legal part of an NSQ topic name")
	errNilRegistrar = errcode.New(errcode.InvalidArgument, "iqmq/nsq: the DSN registrar is nil")
	errNilProducer  = errcode.New(errcode.InvalidArgument, "iqmq/nsq: the producer is nil")

	errNegativeLimit  = errcode.New(errcode.InvalidConfig, "iqmq/nsq: a size limit is negative")
	errAddrCredential = errcode.New(errcode.InvalidConfig,
		"iqmq/nsq: an address carries user information; an \"@\" makes the HTTP request this driver builds from it go to a different host, and the auth secret goes in AuthSecret")
	errAddrNotHostPort = errcode.New(errcode.InvalidConfig,
		"iqmq/nsq: an nsqd address is a host and a port; a \"/\", \"?\", \"#\" or backslash in one moves the HTTP request this driver builds from it (a lookupd address may be a full URL)")
	errBodySmallerThanMessage = errcode.New(errcode.InvalidConfig, "iqmq/nsq: MaxBodyBytes is smaller than MaxMessageBytes, so no batch could ever carry one message")
	errNegativeDuration       = errcode.New(errcode.InvalidConfig, "iqmq/nsq: a configured duration is negative")
	errOutputBufferTooSmall   = errcode.New(errcode.InvalidConfig, "iqmq/nsq: OutputBufferTimeout is below MinOutputBufferTimeout, which mirrors nsqd's --min-output-buffer-timeout; nsqd refuses less in the IDENTIFY handshake, and every operation on the client would then fail with an error about a handshake")
	errHeartbeatPastRead      = errcode.New(errcode.InvalidConfig, "iqmq/nsq: HeartbeatInterval is at or past ReadTimeout, so an idle connection would look dead")
	errAckDeadlineBelowMin    = errcode.New(errcode.InvalidConfig, "iqmq/nsq: AckDeadline is shorter than MinAckDeadline, which mirrors nsqd's --min-msg-timeout; nsqd refuses less in the IDENTIFY handshake")
	errAckDeadlinePastMax     = errcode.New(errcode.InvalidConfig, "iqmq/nsq: AckDeadline is longer than MaxAckDeadline, which mirrors nsqd's --max-msg-timeout; nsqd would refuse the IDENTIFY handshake")
	errGuardPastDeadline      = errcode.New(errcode.InvalidConfig, "iqmq/nsq: LeaseGuard is at or past AckDeadline, so every settlement would be refused")
	errBothCompressions       = errcode.New(errcode.InvalidConfig, "iqmq/nsq: Deflate and Snappy cannot both be set")
	errDeflateLevel           = errcode.New(errcode.InvalidConfig, "iqmq/nsq: DeflateLevel is outside 1..9")
	errInsecureWithoutTLS     = errcode.New(errcode.InvalidConfig, "iqmq/nsq: InsecureSkipVerify without TLS would dial in the clear, which is not what it reads as")
)

// The escape sets, one per position of a DSN, each holding exactly the bytes
// the shared parser gives a meaning to THERE -- because it decodes every
// component, so each can hold any byte, and escaping more than a position
// needs makes a rendering unreadable and longer than what it parsed.
//
//   - an address is split on "," (the next nsqd), on "@" (there is no user in
//     an NSQ DSN, but the grammar still reads one) and on ":" (the port), and
//     one that starts with "[" is read as a bracketed literal;
//   - the path runs from the first "/" to the "?";
//   - a query value runs from its "=" to the next "&", and a "+" in it decodes
//     as a space. A "," is NOT escaped there: `lookupd` and `admin` are
//     comma-separated lists this driver splits AFTER the shared parser has
//     decoded the value, so the comma is the list's own separator and an
//     escape of it would decode straight back into one.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escHost  = "%,@:/?#"
	escPath  = "%?#"
	escQuery = "%+&#"
)

// escapePath escapes the topic prefix, which the DSN carries as its path.
func escapePath(s string) string { return escapeSet(s, escPath) }

// escapeQuery escapes a query parameter's value.
func escapeQuery(s string) string { return escapeSet(s, escQuery) }

// escapeList escapes each entry of a comma-separated parameter and joins them
// with the separator [stringList] splits on.
func escapeList(list []string) string {
	out := make([]string, len(list))
	for i, v := range list {
		out[i] = escapeQuery(v)
	}
	return strings.Join(out, ",")
}

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

// renderAddr renders one nsqd address as a DSN endpoint that parses back to
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

// effectiveBodyBytes is MaxBodyBytes once its zero has been read as "the
// nsqd default", which is what [Config.resolve] does.
func effectiveBodyBytes(c Config) int64 {
	if c.MaxBodyBytes != 0 {
		return c.MaxBodyBytes
	}
	return DefaultMaxBodyBytes
}

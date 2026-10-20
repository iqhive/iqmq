package rabbitmq

import (
	"context"
	"crypto/tls"
	"math"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/backoff"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/rabbit/confirm"
	"github.com/iqhive/iqmq/internal/rabbit/delay"
)

// Scheme is the DSN scheme this driver answers:
//
//	rabbitmq://user:password@host:5672,host2:5672/vhost?declare=true&queuetype=quorum
//
// It is the AMQP 0-9-1 profile alone. The AMQP 1.0 profile has its own scheme
// in its own module, and this one deliberately does not claim it: the specification says
// the profile is "never silently substituted", and a driver that answered both
// schemes would be exactly that substitution with a different spelling.
//
// "amqp" is not claimed either, for the same reason plus one more: it is the
// name of a protocol that four brokers in this repository speak, so a registry
// entry under it would be a race between the drivers that registered first.
const Scheme = "rabbitmq"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each
// of them with a registry it owns.
func Register(r dsn.Registrar) error {
	if r == nil {
		return errNilRegistrar
	}
	return r.Register(Scheme, OpenDSN)
}

// OpenDSN opens a client from a DSN.
func OpenDSN(ctx context.Context, s string) (*mq.Client, error) {
	cfg, err := ParseDSN(s)
	if err != nil {
		return nil, err
	}
	return Open(ctx, cfg)
}

// ParseDSN turns a RabbitMQ DSN into a [Config].
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table: the shared parser owns the grammar, the 8 KiB bound, the percent
// decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver
// does not have into a construction error naming it, rather than a setting
// nobody notices was ignored (ADR-010).
//
// The parameters, all optional:
//
//	vhost=<name>                   the virtual host, overriding the path
//	tls=true|false                 dial with TLS using the platform roots
//	insecureskipverify=true|false  with tls=true, do not verify the server
//	prefix=<string>                prepended to every exchange and queue name
//	exchangetype=<kind>            direct|topic|fanout|headers
//	queuetype=quorum|classic       the subscription queue's implementation
//	declare=true|false             let the driver create its own topology
//	deadletter=true|false          declare and bind `<queue>.dlx`/`.dlq`
//	deadlettertopic=<topic>        dead-letter to this TOPIC instead of `.dlq`
//	queuemaxlength=<n>             `x-max-length` on a subscription's queue
//	maxpriority=<n>                `x-max-priority`, without which a priority
//	                               is ignored by the broker
//	mandatory=true|false           publish WITH the `mandatory` flag, so that
//	                               an unroutable publication is Rejected
//	keyasroutingkey=true|false     the message key is the routing key
//	confirm=permessage|windowed|async the publisher-confirm strategy
//	confirmwindow=<n>              outstanding publications under the last two
//	delay=ttldlx|plugin the delayed-redelivery strategy
//	delayladder=<1s;5s;30s>        the TTL strategy's bucket ladder
//	delayqueuettl=<duration>       `x-expires` on a delay queue
//	detectdelayplugin=true|false   probe for the delayed-message exchange
//	heartbeat=<duration>           the AMQP heartbeat interval
//	dialtimeout=<duration>         bound on one dial attempt
//	publishtimeout=<duration>      bound on a confirm or settlement round trip
//	channelmax=<n>, framesize=<n>  connection tuning
//	connectionname=<string>        what `list_connections` shows
//	reconnectbase=<duration>       first delay after a lost connection
//	reconnectmax=<duration>        cap on that delay
//	maxreconnects=<n>              attempts; zero for "until Close"
//	backoff=<kind> the jitter policy
//	consumertagprefix=<string>     prepended to each source's consumer tag
//
// The path names the virtual host, so `rabbitmq://host/acme` uses the `acme`
// vhost. A DSN with no path uses the broker's default, which is `/`.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg := Config{
		Username: c.Username,
		Password: c.Password,
		Vhost:    c.Path,
	}
	p := &c.Params
	var (
		useTLS, insecure bool
		ladder           string
		exchangeType     string
	)
	for _, step := range [...]func() error{
		func() error { return p.String("vhost", func(v string) { cfg.Vhost = v }) },
		func() error { return p.Bool("tls", func(v bool) { useTLS = v }) },
		func() error { return p.Bool("insecureskipverify", func(v bool) { insecure = v }) },
		func() error { return p.String("prefix", func(v string) { cfg.Prefix = v }) },
		func() error { return p.String("exchangetype", func(v string) { exchangeType = v }) },
		func() error {
			return p.Enum("queuetype", queueTypeNames[:], func(i int) { cfg.QueueType = QueueType(i) })
		},
		func() error { return p.Bool("declare", func(v bool) { cfg.Declare = v }) },
		func() error { return p.Bool("deadletter", func(v bool) { cfg.DeadLetter = v }) },
		func() error { return p.String("deadlettertopic", func(v string) { cfg.DeadLetterTopic = v }) },
		func() error { return p.Int64("queuemaxlength", func(v int64) { cfg.QueueMaxLength = v }) },
		func() error { return uint8Param(p, "maxpriority", func(v uint8) { cfg.MaxPriority = v }) },
		func() error { return p.Bool("mandatory", func(v bool) { cfg.Mandatory = v }) },
		func() error { return p.Bool("keyasroutingkey", func(v bool) { cfg.KeyAsRoutingKey = v }) },
		func() error {
			return p.Enum("confirm", kindNames(confirm.Auto), func(i int) { cfg.ConfirmKind = confirm.Kind(i) })
		},
		func() error { return p.PositiveInt("confirmwindow", func(v int) { cfg.ConfirmWindow = v }) },
		func() error {
			return p.Enum("delay", kindNames(delay.Auto), func(i int) { cfg.DelayKind = delay.Kind(i) })
		},
		func() error { return p.String("delayladder", func(v string) { ladder = v }) },
		func() error { return p.Duration("delayqueuettl", func(v time.Duration) { cfg.DelayQueueTTL = v }) },
		func() error { return p.Bool("detectdelayplugin", func(v bool) { cfg.DetectDelayPlugin = v }) },
		func() error { return p.Duration("heartbeat", func(v time.Duration) { cfg.Heartbeat = v }) },
		func() error { return p.Duration("dialtimeout", func(v time.Duration) { cfg.DialTimeout = v }) },
		func() error { return p.Duration("publishtimeout", func(v time.Duration) { cfg.PublishTimeout = v }) },
		func() error { return uint16Param(p, "channelmax", func(v uint16) { cfg.ChannelMax = v }) },
		func() error { return p.Int("framesize", func(v int) { cfg.FrameSize = v }) },
		func() error { return p.String("connectionname", func(v string) { cfg.ConnectionName = v }) },
		func() error { return p.Duration("reconnectbase", func(v time.Duration) { cfg.ReconnectBase = v }) },
		func() error { return p.Duration("reconnectmax", func(v time.Duration) { cfg.ReconnectMax = v }) },
		func() error { return p.PositiveInt("maxreconnects", func(v int) { cfg.MaxReconnects = v }) },
		func() error {
			return p.Enum("backoff", kindNames(backoff.Auto), func(i int) { cfg.BackoffPolicy = backoff.Kind(i) })
		},
		func() error { return p.String("consumertagprefix", func(v string) { cfg.ConsumerTagPrefix = v }) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.ExchangeType = exchangeType
	for _, h := range c.Hosts {
		if !dialableHost(h.Host) {
			return Config{}, errDSNHost
		}
		if h.Port == "" {
			if useTLS {
				h.Port = DefaultTLSPort
			} else {
				h.Port = DefaultPort
			}
		}
		cfg.Addrs = append(cfg.Addrs, h.String())
	}
	if ladder != "" {
		// ";" and not ",": a comma separates the DSN's own endpoints, and a
		// duration list that used one would be ambiguous to read even where
		// the grammar allowed it. It is the same choice `natsjs`'s
		// `streamsubjects` made.
		for part := range strings.SplitSeq(ladder, ";") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			d, err := time.ParseDuration(part)
			if err != nil {
				return Config{}, errDelayLadder
			}
			cfg.DelayLadder = append(cfg.DelayLadder, d)
		}
	}
	if useTLS {
		// A DSN cannot carry a *tls.Config, so "tls=true" means the platform
		// roots and the server name taken from the address, which is the
		// only safe default a connection string can ask for.
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // the field is the operator's explicit request and Validate refuses it without TLS
		cfg.InsecureSkipVerify = insecure
	} else if insecure {
		// Refusing is the only honest answer: the alternative is a DSN that
		// dialled in the clear after the operator asked to skip
		// verification.
		return Config{}, errInsecureWithoutTLS
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DSN renders the configuration as a DSN with every credential redacted, which
// is the round trip the specification asks for: the result parses back to the same
// configuration except for the password, which cannot be recovered from a
// redacted string and must not be.
func (c Config) DSN() string {
	var b strings.Builder
	b.WriteString(Scheme)
	b.WriteString("://")
	if c.Username != "" {
		b.WriteString(escapeUser(c.Username))
		if c.Password != "" {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i, addr := range c.Addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(renderAddr(addr))
	}
	b.WriteByte('/')
	b.WriteString(escapeVhost(c.Vhost))

	var q query
	q.Bool("tls", c.usesTLS())
	q.Bool("insecureskipverify", c.InsecureSkipVerify)
	q.String("prefix", c.Prefix)
	q.String("exchangetype", c.ExchangeType)
	if c.QueueType != QueueQuorum {
		q.String("queuetype", c.QueueType.String())
	}
	q.Bool("declare", c.Declare)
	q.Bool("deadletter", c.DeadLetter)
	q.String("deadlettertopic", c.DeadLetterTopic)
	q.Int64("queuemaxlength", c.QueueMaxLength)
	q.Int("maxpriority", int(c.MaxPriority))
	q.Bool("mandatory", c.Mandatory)
	q.Bool("keyasroutingkey", c.KeyAsRoutingKey)
	if c.ConfirmKind != confirm.Auto {
		q.String("confirm", c.ConfirmKind.String())
	}
	q.Int("confirmwindow", c.ConfirmWindow)
	if c.DelayKind != delay.Auto {
		q.String("delay", c.DelayKind.String())
	}
	if len(c.DelayLadder) > 0 {
		parts := make([]string, 0, len(c.DelayLadder))
		for _, d := range c.DelayLadder {
			parts = append(parts, d.String())
		}
		q.String("delayladder", strings.Join(parts, ";"))
	}
	q.Duration("delayqueuettl", c.DelayQueueTTL)
	q.Bool("detectdelayplugin", c.DetectDelayPlugin)
	q.Duration("heartbeat", c.Heartbeat)
	q.Duration("dialtimeout", c.DialTimeout)
	q.Duration("publishtimeout", c.PublishTimeout)
	q.Int("channelmax", int(c.ChannelMax))
	q.Int("framesize", c.FrameSize)
	q.String("connectionname", c.ConnectionName)
	q.Duration("reconnectbase", c.ReconnectBase)
	q.Duration("reconnectmax", c.ReconnectMax)
	q.Int("maxreconnects", c.MaxReconnects)
	if c.BackoffPolicy != backoff.Auto {
		q.String("backoff", c.BackoffPolicy.String())
	}
	q.String("consumertagprefix", c.ConsumerTagPrefix)
	b.WriteString(q.render())
	return b.String()
}

// kindNames is a variant family's slug table in the form the DSN parser's
// `Enum` wants: indexed by the kind's own value, starting at Auto, so that
// `Enum` hands back an index this driver converts with a cast rather than with
// a lookup table of its own.
//
// It is generic over the three families a DSN can select from — the confirm
// strategy, the delay strategy and the backoff policy —
// because all three have the same shape and a table written out three times is
// three places for a slug to drift from the family that owns it.
func kindNames[K interface {
	~uint8
	Valid() bool
	String() string
}](first K) []string {
	out := make([]string, 0, 8)
	for k := first; k.Valid(); k++ {
		out = append(out, k.String())
	}
	return out
}

// query is the DSN's parameter list.
//
// It renders in the order the parameters are added and omits a parameter whose
// value is the zero the parser would read as "not set", which is what makes
// [Config.DSN] parse back to the same configuration: a rendered
// `confirmwindow=0` would be refused by `PositiveInt`, and a rendered
// `prefix=` would be an empty prefix rather than none.
type query struct{ parts []string }

func (q *query) String(key, v string) {
	if v != "" {
		q.parts = append(q.parts, key+"="+urlEncode(v))
	}
}

func (q *query) Bool(key string, v bool) {
	if v {
		q.parts = append(q.parts, key+"=true")
	}
}

func (q *query) Int(key string, v int) {
	if v > 0 {
		q.parts = append(q.parts, key+"="+strconv.Itoa(v))
	}
}

func (q *query) Int64(key string, v int64) {
	if v > 0 {
		q.parts = append(q.parts, key+"="+strconv.FormatInt(v, 10))
	}
}

func (q *query) Duration(key string, v time.Duration) {
	if v > 0 {
		q.parts = append(q.parts, key+"="+v.String())
	}
}

// render is the whole query string, empty when nothing was added.
func (q *query) render() string {
	if len(q.parts) == 0 {
		return ""
	}
	return "?" + strings.Join(q.parts, "&")
}

// urlEncode percent-encodes the characters that would end a parameter or a
// value, or that the query decoder gives a meaning to. It is deliberately
// minimal: a prefix or a connection name the broker accepts should read as
// itself in a DSN a person has to paste.
//
// "+" is here because the shared parser decodes a "+" in a query VALUE as a
// space and nowhere else, so `prefix=a+b` used to come back as "a b".
// The rest were already escaped; escaping "=", "?" and
// " " is more than the grammar needs, and they stay because narrowing the set
// would change a rendering nothing complained about.
func urlEncode(s string) string {
	if !strings.ContainsAny(s, "&=?#%+ ") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 6)
	const hex = "0123456789ABCDEF"
	for i := range len(s) {
		switch ch := s[i]; ch {
		case '&', '=', '?', '#', '%', '+', ' ':
			b.WriteByte('%')
			b.WriteByte(hex[ch>>4])
			b.WriteByte(hex[ch&0xf])
		default:
			b.WriteByte(ch)
		}
	}
	return b.String()
}

// The escape sets, one per position of a DSN, each holding exactly the bytes
// the shared parser gives a meaning to THERE -- because it decodes every
// component, so each can hold any byte, and escaping more than a position
// needs makes a rendering unreadable and longer than what it parsed.
//
//   - a user ends at the first ":" (the password) and the authority at "/",
//     "?" or "#"; an "@" in it is safe, because the credential ends at the
//     LAST one;
//   - a host is also split on "," (the next endpoint) and on ":" (the port),
//     and one that starts with "[" is read as a bracketed literal. "@" is not
//     in the set because [Config.Validate] refuses an address that holds one.
//
// "%" is in every set because it begins an escape, and "#" because a DSN with
// one anywhere is refused.
const (
	escUser = "%:/?#"
	escHost = "%,:/?#"
)

// escapeUser escapes a user name for the DSN's credential position.
func escapeUser(s string) string { return escapeSet(s, escUser) }

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
// The scheme goes: a DSN names the protocol once, in its own scheme, and the
// TLS choice travels as `tls=`. What is left is escaped for the host position,
// except that a host with a colon or a "%" in it is written as a bracketed
// literal, raw, which is how an IPv6 address reads and how it came in: the
// parser takes a bracketed host verbatim, without decoding it, so neither
// needs an escape there -- and a "%" escaped outside the brackets would
// triple, which is the one way a parsed host could otherwise render longer
// than it was written. That works unless the host holds a byte the authority
// is split on before the brackets are seen, and such a host is escaped
// instead, colons and all.
func renderAddr(a string) string {
	if i := strings.Index(a, "://"); i >= 0 {
		a = a[i+3:]
	}
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

// uint8Param and uint16Param read a bounded integer parameter, refusing a
// value the field cannot hold rather than truncating it: a `maxpriority=300`
// that silently became 44 would declare a queue with a priority ceiling the
// operator did not ask for.
func uint8Param(p *dsn.Params, key string, set func(uint8)) error {
	var n int
	if err := p.PositiveInt(key, func(v int) { n = v }); err != nil {
		return err
	}
	if n > math.MaxUint8 {
		return errParamRange
	}
	set(uint8(n))
	return nil
}

func uint16Param(p *dsn.Params, key string, set func(uint16)) error {
	var n int
	if err := p.PositiveInt(key, func(v int) { n = v }); err != nil {
		return err
	}
	if n > math.MaxUint16 {
		return errParamRange
	}
	set(uint16(n))
	return nil
}

// dialableHost reports whether a host the shared parser DECODED can be written
// into the AMQP URL [Config.url] builds and still name only itself.
//
// amqp091-go reads that URL with net/url, which gives "@" (user information),
// "/" (the virtual host), "?" (a query) and "#" (a fragment) a meaning of
// their own, and a DSN writes any of them into a host as an escape. Measured
// against amqp091-go v1.14.0's own ParseURI: "rabbitmq://tok%40real-host:5672/v"
// dialled real-host as the user "tok"; "a%2Fb" dialled "a"; "a%3Fb" and
// "a%23b" dialled "a" AND lost the virtual host; and with a credential in the
// DSN, "rabbitmq://u:p@tok%40real-host:5672/v" dialled real-host with the
// password "p@tok". A backslash, a space or a control byte made ParseURI
// refuse the URL outright, so the configuration failed at the dial instead of
// at construction.
//
// A host name or an IP literal holds none of them, so such a host is refused
// rather than escaped: there is no spelling of it that dials what the DSN
// names.
//
// A comma is deliberately NOT here, which is where this driver differs from
// the NATS ones: nats.go reads its server string as a comma-separated list
// before net/url sees it , while amqp091-go reads one URL
// and kept "a,b" as the host in the same measurement. The comma is a
// separator of this driver's own DSN rendering, and [renderAddr] escapes it.
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c == 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\':
			return false
		}
	}
	// A "%" in a DECODED host is refused too, unless the host also holds a
	// ":". The shared parser decodes once, so "%25" puts a literal "%" in
	// the host and "%2540" puts back the escape it came from -- and every
	// client library here reads its URL with net/url, which rejects any
	// "%XY" in a host except "%25". Measured: "tok%2540real-host" and
	// "a%25b" each produced "invalid URL escape" at the DIAL rather than at
	// construction, which is the outcome this set refuses a backslash for.
	// It is not a second redirect (net/url cannot be steered to another host
	// this way), it is the same late-failure class.
	//
	// The one legitimate "%" in a host is an IPv6 zone id, which always
	// arrives inside brackets and therefore always beside a ":"
	// ("[fe80::1%25eth0]"), so that exception is what the colon tests.
	if strings.ContainsRune(host, '%') && !strings.ContainsRune(host, ':') {
		return false
	}
	return true
}

// The DSN's own refusals.
var (
	errNilRegistrar = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: Register needs a registry")
	errDSNHost = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: a DSN host decodes to a byte an AMQP URL gives a meaning to -- \"@\", \"/\", \"?\", \"#\", a backslash, a space or a control character -- so it would dial somewhere the DSN does not name")
	errDelayLadder = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: delayladder is a \";\"-separated list of durations, ascending")
	errParamRange = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: the parameter's value is outside the range its field can hold")
)

package amqp10

import (
	"context"
	"crypto/tls"
	"strconv"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Scheme is the DSN scheme this core answers:
//
//	amqp10://user:password@host:5672,host2:5672?sasl=plain&tls=true
//
// It is the GENERIC AMQP 1.0 profile, and a broker-specific module
// registers its own scheme instead — "activemq" for Artemis and Classic,
// "rabbitmq+amqp10" for RabbitMQ 4's AMQP 1.0 endpoint, which is a SUFFIXED
// form because "rabbitmq" is the AMQP 0-9-1 driver's and the two profiles'
// capability tables differ in eleven claims. A module that answered this
// scheme as well would make a DSN ambiguous about which profile's capabilities
// the caller gets, which is exactly what ADR-010's the specification chose separate
// modules to prevent.
const Scheme = "amqp10"

// Register registers a profile with a DSN registry under a scheme
// (ADR-012).
//
// The scheme AND the profile are parameters because this module is a core and
// not a driver: it has no capabilities of its own to register, and a registry
// entry that opened a client with no profile would open one that cannot address
// anything. A broker-specific module calls this with its own scheme and its own
// profile, which is one line rather than a second copy of the DSN table.
//
// A driver exposes this and nothing else: it never reaches for a global registry
// of its own, so a library that composes two brokers registers each of them with
// a registry it owns.
//
// The scheme is registered and matched in the shared grammar's canonical
// spelling, because a scheme is case-insensitive (RFC 3986): "MyBroker"
// registers "mybroker", and the opener answers "MyBroker://" and "mybroker://"
// alike. It used to hand the opener the scheme as spelled, which a registry
// that lower-cases then listed as registered and answered with
// ErrDriverNotRegistered.
func Register(r dsn.Registrar, scheme string, profile Profile) error {
	if r == nil {
		return errNilRegistrar
	}
	scheme, ok := canonicalScheme(scheme)
	if !ok {
		return errBadDSNScheme
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	return r.Register(scheme, func(ctx context.Context, s string) (*mq.Client, error) {
		cfg, err := ParseDSNScheme(scheme, s)
		if err != nil {
			return nil, err
		}
		return Open(ctx, profile, cfg)
	})
}

// ParseDSN turns an AMQP 1.0 DSN under [Scheme] into a [Config].
func ParseDSN(s string) (Config, error) { return ParseDSNScheme(Scheme, s) }

// ParseDSNScheme is [ParseDSN] for a broker-specific module's own scheme.
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this core's parameter table,
// which is the point: the shared parser owns the grammar, the 8 KiB bound, the
// percent decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this core does
// not have into a construction error naming it, rather than a setting nobody
// notices was ignored (ADR-010).
//
// A broker-specific module that has parameters of its own reads them from the
// same [github.com/iqhive/iqmq/dsn.Config] and then calls Strict itself; the
// read marks this function leaves are visible through the shared parameter
// storage, so the two halves of the table cannot disagree about what was read.
// [ParseInto] is that entry point.
//
// The parameters, all optional:
//
//	containerid=<string>           the container-id sent in the OPEN frame
//	hostname=<string>              the hostname sent in OPEN and used for TLS SNI
//	sasl=auto|none|anonymous|plain|external
//	tls=true|false                 dial with TLS using the platform roots
//	insecureskipverify=true|false  with tls=true, do not verify the server
//	insecureplainsasl=true|false   allow SASL PLAIN without TLS
//	sendersettle=unsettled|settled|mixed
//	receiversettle=first|second
//	idletimeout=<duration>         longest silence before the peer is gone
//	maxframesize=<n>               largest frame this connection accepts
//	maxsessions=<n>                sessions, and therefore links, per connection
//	maxcredit=<n>                  cap on one source's link credit
//	maxpending=<n>                 asynchronous publications per sink
//	nondurable=true|false          publish with the AMQP durable flag clear
//	dialtimeout=<duration>         bound on one dial attempt
//	operationtimeout=<duration>    bound on an attach, detach or disposition
//	writetimeout=<duration>        bound on one frame write
//	backoff=none|constant|fulljitter|equaljitter|decorrelated
//	reconnectbase=<duration>       first delay between dial attempts
//	reconnectmax=<duration>        cap on every delay
//	reconnectgrowth=<float>        factor between consecutive delays
//	reconnectattempts=<n>          attempts per reconnection; 0 means for ever
//	noreconnect=true|false         do not re-dial at all
//
// There is no path parameter and no topic prefix. An AMQP 1.0 address is the
// profile's business ([Profile.SinkAddress]), so a prefix in the DSN would be a
// second naming rule competing with the profile's own — and the one that lost
// would be the one nobody noticed.
func ParseDSNScheme(scheme, s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	// dsn.Parse lower-cases the DSN's scheme, so the one it is compared with
	// is canonicalised the same way; a string that is not a scheme names no
	// DSN at all.
	scheme, ok := canonicalScheme(scheme)
	if !ok || c.Scheme != scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg, err := ParseInto(&c)
	if err != nil {
		return Config{}, err
	}
	if err := c.Params.Strict(); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// canonicalScheme is a caller's scheme in the shared grammar's spelling, with
// the empty string standing for [Scheme]. It reports false for a string that is
// not exactly one scheme -- "a://b" included, which the grammar reads as "a".
func canonicalScheme(scheme string) (string, bool) {
	if scheme == "" {
		return Scheme, true
	}
	canon, err := dsn.Scheme(scheme + "://")
	if err != nil || len(canon) != len(scheme) {
		return "", false
	}
	return canon, true
}

// ParseInto reads this core's parameters out of an already-parsed DSN, WITHOUT
// calling [github.com/iqhive/iqmq/dsn.Params.Strict].
//
// It is the entry point a broker-specific module uses: it reads the core's
// parameters, then its own, then calls Strict once — so a parameter neither half
// claims is still a construction error naming it, and neither half has to know
// the other's table. A module that called [ParseDSNScheme] instead would have
// its own parameters rejected by the core's Strict.
func ParseInto(c *dsn.Config) (Config, error) {
	cfg := Config{
		Username: c.Username,
		Password: c.Password,
	}
	p := &c.Params
	var (
		useTLS, insecure bool
		sasl, sndMode    string
		rcvMode, backoff string
	)
	for _, step := range [...]func() error{
		func() error { return p.String("containerid", &cfg.ContainerID) },
		func() error { return p.String("hostname", &cfg.HostName) },
		func() error { return p.String("sasl", &sasl) },
		func() error { return p.Bool("tls", &useTLS) },
		func() error { return p.Bool("insecureskipverify", &insecure) },
		func() error { return p.Bool("insecureplainsasl", &cfg.InsecurePlainSASL) },
		func() error { return p.String("sendersettle", &sndMode) },
		func() error { return p.String("receiversettle", &rcvMode) },
		func() error { return p.String("backoff", &backoff) },
		func() error { return p.Duration("idletimeout", &cfg.IdleTimeout) },
		func() error { return uint32Param(p, "maxframesize", &cfg.MaxFrameSize) },
		func() error { return uint16Param(p, "maxsessions", &cfg.MaxSessions) },
		// Positive-only, because the struct's zero already means "the
		// default": a DSN that wrote maxcredit=0 would otherwise read as a
		// setting that was honoured (the shape  recorded).
		func() error { return p.PositiveInt("maxcredit", &cfg.MaxCredit) },
		func() error { return p.PositiveInt("maxpending", &cfg.MaxPending) },
		func() error { return p.Bool("nondurable", &cfg.NonDurable) },
		func() error { return p.Duration("dialtimeout", &cfg.DialTimeout) },
		func() error { return p.Duration("operationtimeout", &cfg.OperationTimeout) },
		func() error { return p.Duration("writetimeout", &cfg.WriteTimeout) },
		func() error { return p.Duration("reconnectbase", &cfg.ReconnectBase) },
		func() error { return p.Duration("reconnectmax", &cfg.ReconnectMax) },
		func() error { return floatParam(p, "reconnectgrowth", &cfg.ReconnectGrowth) },
		func() error { return p.PositiveInt("reconnectattempts", &cfg.ReconnectAttempts) },
		func() error { return p.Bool("noreconnect", &cfg.NoReconnect) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	for _, h := range c.Hosts {
		if !dialableHost(h.Host) {
			return Config{}, errDSNHost
		}
		port := h.Port
		if port == "" {
			port = DefaultPort
			if useTLS {
				port = DefaultTLSPort
			}
		}
		host := h
		host.Port = port
		prefix := "amqp://"
		if useTLS {
			prefix = "amqps://"
		}
		cfg.Addrs = append(cfg.Addrs, prefix+host.String())
	}
	if err := namedValue(sasl, saslMechanismNames[:], func(i int) { cfg.SASL = SASLMechanism(i) }); err != nil {
		return Config{}, dsnValueErr("sasl")
	}
	if err := namedValue(sndMode, senderSettleModeNames[:], func(i int) { cfg.SenderSettle = SenderSettleMode(i) }); err != nil {
		return Config{}, dsnValueErr("sendersettle")
	}
	if err := namedValue(rcvMode, receiverSettleModeNames[:], func(i int) { cfg.ReceiverSettle = ReceiverSettleMode(i) }); err != nil {
		return Config{}, dsnValueErr("receiversettle")
	}
	if err := namedValue(backoff, backoffPolicyNames[:], func(i int) { cfg.Backoff = BackoffPolicy(i) }); err != nil {
		return Config{}, dsnValueErr("backoff")
	}
	if useTLS {
		// A DSN cannot carry a *tls.Config, so "tls=true" means the platform
		// roots and the server name taken from the address, which is the only
		// safe default a connection string can ask for.
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // the field is the operator's explicit request and Validate refuses it without TLS
		cfg.InsecureSkipVerify = insecure
	} else if insecure {
		// Refusing is the only honest answer: the alternative is a DSN that
		// dialled in the clear after the operator asked to skip verification,
		// which reads as "less safe" and is "no TLS at all".
		return Config{}, errInsecureWithoutTLS
	}
	return cfg, nil
}

// dialableHost reports whether a host the shared parser DECODED can be written
// into the AMQP URL [ParseInto] builds and still name only itself.
//
// go-amqp reads that URL with net/url, which gives "@" (user information),
// "/" (a path), "?" (a query) and "#" (a fragment) a meaning of their own, and
// a DSN writes any of them into a host as an escape. Measured against go-amqp
// v1.7.0's own dialConn: "amqp10://tok%40real-host:5672" dialled real-host AND
// -- because a userinfo in the URL becomes SASLTypePlain -- offered SASL PLAIN
// as "tok" over a cleartext connection, which is the mechanism [Config.Validate]
// refuses to reach any other way; "a%2Fb", "a%3Fb" and "a%23b" each dialled "a"
// and lost the port with it; and a backslash made net/url refuse the address at
// the dial, so the configuration failed later with an error about a string the
// operator never wrote.
//
// A host name or an IP literal holds none of them, so such a host is refused
// rather than escaped: there is no spelling of it that dials what the DSN
// names. A space, a control byte and anything above ASCII
// are refused by [checkAddr] already and are here too, so that the refusal
// names the host rather than the address it was about to become.
//
// A comma is deliberately NOT here: go-amqp dials one address and the same
// measurement kept "a,b" as net/url's host, where nats.go would have read two
// servers. The separator that matters is the client
// library's, not a rule shared between drivers.
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c >= 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\':
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

// namedValue reads one of an enumeration's names, accepting the empty string as
// "the DSN did not say".
//
// It is not [github.com/iqhive/iqmq/dsn.Params.Enum] because every enumeration
// in this module has a "default" name at index zero that means exactly what
// omitting the parameter means — so Enum's index would be right and the empty
// string has to be distinguished first, which Enum cannot do for a value it
// never sees.
func namedValue(v string, names []string, set func(int)) error {
	if v == "" {
		return nil
	}
	for i, n := range names {
		if n == v {
			set(i)
			return nil
		}
	}
	return errBadEnumValue
}

// uint32Param reads a positive parameter into a uint32 field.
func uint32Param(p *dsn.Params, key string, dst *uint32) error {
	var n int
	if err := p.PositiveInt(key, &n); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n > 1<<32-1 {
		return dsnValueErr(key)
	}
	*dst = uint32(n)
	return nil
}

// uint16Param reads a positive parameter into a uint16 field.
func uint16Param(p *dsn.Params, key string, dst *uint16) error {
	var n int
	if err := p.PositiveInt(key, &n); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n > 1<<16-1 {
		return dsnValueErr(key)
	}
	*dst = uint16(n)
	return nil
}

// floatParam reads a float parameter, which the shared table has no accessor
// for: every other numeric parameter in the repository is an integer or a
// duration.
//
// Recorded in this increment's report as a fourth [github.com/iqhive/iqmq/dsn.Params]
// accessor a driver has needed — the first three were the and the specification's.
func floatParam(p *dsn.Params, key string, dst *float64) error {
	v, ok := p.Lookup(key)
	if !ok {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return dsnValueErr(key)
	}
	*dst = f
	return nil
}

// dsnValueErr names the parameter whose value was refused, in the same words the
// shared parser uses so that a caller sees one style of message.
func dsnValueErr(key string) error {
	return &dsnParamError{key: key}
}

// dsnParamError is a refused parameter value.
//
// It carries CodeInvalidConfig through the wrapped sentinel, which is how
// errors.Is reaches the model, and it names the parameter because a failure
// that does not say which parameter is a failure a caller has to guess at.
type dsnParamError struct{ key string }

// Error renders the refusal. The key is quoted when it is not plain text: a
// parameter name comes off the wire after percent decoding, so it can carry any
// byte at all, and a message that interpolated one raw would not be valid UTF-8
// (the failure  recorded).
func (e *dsnParamError) Error() string {
	return "iqmq/amqp10: the DSN gives a parameter a value it cannot take: " + quoteFact(e.key)
}

// Unwrap reaches the sentinel errors.Is matches by code.
func (e *dsnParamError) Unwrap() error { return errBadEnumValue }

// errBadEnumValue is the code a refused parameter value carries. The specification names no
// sentinel for a bad configuration and errors.Is matches by code, so a caller
// compares mq.CodeOf(err) against mq.CodeInvalidConfig.
var errBadEnumValue = errcode.New(errcode.InvalidConfig,
	"iqmq/amqp10: the DSN gives a parameter a value it cannot take")

// errDSNHost is the refusal of a host [dialableHost] rejects.
var errDSNHost = errcode.New(errcode.InvalidConfig,
	"iqmq/amqp10: a DSN host decodes to a byte an AMQP URL gives a meaning to -- \"@\", \"/\", \"?\", \"#\", a backslash, a space, a control character or a byte above ASCII -- so it would dial somewhere the DSN does not name")

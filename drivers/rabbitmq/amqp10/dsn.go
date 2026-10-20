package amqp10

import (
	"context"
	"strconv"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/errcode"
)

// Scheme is the DSN scheme this profile answers:
//
//	rabbitmq+amqp10://user:password@host:5672,host2:5672?vhost=acme&queuetype=quorum
//
// It is deliberately neither of the two obvious names, and both refusals are the specification's "never silently substituted" written down as a scheme:
//
//   - NOT "rabbitmq", which `drivers/rabbitmq` claims. A registry in which one
//     scheme could open either profile would make a connection string
//     ambiguous about which capability table the caller gets, and the two
//     tables really differ — no request/reply, no delayed redelivery, no
//     ordering by key, no wildcard subscription. Whichever module registered
//     first would decide, which is a race that changes the semantics of a
//     deployment's DSN.
//   - NOT "amqp10", which the shared core's generic profile claims. That
//     scheme means "some AMQP 1.0 broker with a profile the caller supplies",
//     and this one means RabbitMQ 4 — including the `/exchanges/...` address
//     form, the `vhost:` hostname and the version gate.
//
// The "+" is RFC 3986's own sub-scheme separator and the shared parser accepts
// it, so `rabbitmq+amqp10` reads as what it is: the RabbitMQ driver over the
// AMQP 1.0 protocol.
const Scheme = "rabbitmq+amqp10"

// Register registers this profile with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each
// of them with a registry it owns. Registering this module and
// `drivers/rabbitmq` with one registry is the supported arrangement — the two
// schemes do not collide — and it is how a deployment moves one application
// from one profile to the other by editing a connection string.
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

// ParseDSN turns a DSN under [Scheme] into a [Config].
//
// It is two parameter tables over one parsed connection string: the shared
// core's, through [github.com/iqhive/iqmq/drivers/amqp10.ParseInto], and this
// profile's own. That is the split the core's own documentation asks for — the
// core reads its parameters, the profile reads its own, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] is called ONCE at the end, so a
// parameter neither half claims is a construction error naming it rather than
// a setting nobody notices was ignored (ADR-010).
//
// This profile's parameters, all optional:
//
//	vhost=<name>                   the virtual host, which reaches RabbitMQ as
//	                               the OPEN frame's `hostname` under "vhost:"
//	prefix=<string>                prepended to every exchange and queue name
//	exchangetype=<kind>            direct|topic|fanout|headers
//	queuetype=quorum|classic       what the subscription's queue was declared
//	                               as, which two capability claims move with
//	deadletter=true|false          the queues have a dead-letter exchange
//	maxpriority=<n>                the queues were declared with x-max-priority
//	credit=<n>                     the link credit ceiling of one source
//
// and the core's, which this profile passes through unchanged: `containerid`,
// `hostname`, `sasl`, `tls`, `insecureskipverify`, `insecureplainsasl`,
// `sendersettle`, `receiversettle`, `idletimeout`, `maxframesize`,
// `maxsessions`, `maxcredit`, `maxpending`, `nondurable`, `dialtimeout`,
// `operationtimeout`, `writetimeout`, `backoff`, `reconnectbase`,
// `reconnectmax`, `reconnectgrowth`, `reconnectattempts` and `noreconnect`.
//
// Two parameters the AMQP 0-9-1 driver has are absent rather than accepted and
// ignored: `declare` and `keyasroutingkey`. Neither can be honoured here (see
// [Config.Declare] and [Config.KeyAsRoutingKey]), so a DSN carrying one is
// refused by Strict, naming it — which is the answer the specification wants for an option
// a driver cannot provide.
//
// The virtual host is a PARAMETER and not the path, which is the one place a
// DSN for this profile does not look like the other driver's. AMQP 1.0 has no
// virtual-host field, so there is no path to put one in; a DSN with a path is
// refused rather than silently ignored.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	if c.Path != "" {
		return Config{}, errPathNotVhost
	}
	cc, err := core.ParseInto(&c)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Username:           cc.Username,
		Password:           cc.Password,
		HostName:           cc.HostName,
		ContainerID:        cc.ContainerID,
		Properties:         cc.Properties,
		SASL:               cc.SASL,
		InsecurePlainSASL:  cc.InsecurePlainSASL,
		TLS:                cc.TLS,
		InsecureSkipVerify: cc.InsecureSkipVerify,
		IdleTimeout:        cc.IdleTimeout,
		MaxFrameSize:       cc.MaxFrameSize,
		MaxSessions:        cc.MaxSessions,
		ReceiverSettle:     cc.ReceiverSettle,
		SenderSettle:       cc.SenderSettle,
		Credit:             cc.MaxCredit,
		MaxPending:         cc.MaxPending,
		NonDurable:         cc.NonDurable,
		DialTimeout:        cc.DialTimeout,
		OperationTimeout:   cc.OperationTimeout,
		WriteTimeout:       cc.WriteTimeout,
		Backoff:            cc.Backoff,
		ReconnectBase:      cc.ReconnectBase,
		ReconnectMax:       cc.ReconnectMax,
		ReconnectGrowth:    cc.ReconnectGrowth,
		ReconnectAttempts:  cc.ReconnectAttempts,
		NoReconnect:        cc.NoReconnect,
		Addrs:              cc.Addrs,
	}
	p := &c.Params
	var exchangeType string
	for _, step := range [...]func() error{
		func() error { return p.String("vhost", func(v string) { cfg.Vhost = v }) },
		func() error { return p.String("prefix", func(v string) { cfg.Prefix = v }) },
		func() error { return p.String("exchangetype", func(v string) { exchangeType = v }) },
		func() error {
			return p.Enum("queuetype", queueTypeNames[:], func(i int) { cfg.QueueType = QueueType(i) })
		},
		func() error { return p.Bool("deadletter", func(v bool) { cfg.DeadLetter = v }) },
		func() error { return uint8Param(p, "maxpriority", func(v uint8) { cfg.MaxPriority = v }) },
		// Positive-only, because the struct's zero already means "the
		// default": a DSN that wrote credit=0 would otherwise read as a
		// setting that was honoured.
		func() error { return p.PositiveInt("credit", func(v int) { cfg.Credit = v }) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.ExchangeType = exchangeType
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DSN renders the configuration as a connection string with the credential
// redacted, which is the round trip the specification asks for: what comes back parses into
// the same configuration except for the password, which cannot be recovered
// from a redacted string and must not be.
//
// Everything else really does round-trip, including the parameters the shared
// core owns, and the test that holds it is a comparison of the parsed result
// against the original with the password cleared. A rendering that could not be
// parsed back would make this useless for the one thing it exists for: a log
// line a person then pastes.
func (c Config) DSN() string {
	r := c.resolve()
	var b strings.Builder
	b.Grow(256)
	b.WriteString(Scheme)
	b.WriteString("://")
	if r.Username != "" {
		b.WriteString(escapeDSN(r.Username))
		if r.Password != "" {
			b.WriteByte(':')
			b.WriteString(dsn.Redacted)
		}
		b.WriteByte('@')
	}
	for i, addr := range r.Addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(renderAddr(addr))
	}
	q := make([]string, 0, 24)
	add := func(key, value string) {
		if value != "" {
			q = append(q, key+"="+escapeDSN(value))
		}
	}
	addBool := func(key string, v bool) {
		if v {
			q = append(q, key+"=true")
		}
	}
	add("vhost", r.Vhost)
	add("hostname", r.HostName)
	add("containerid", r.ContainerID)
	add("prefix", r.Prefix)
	add("exchangetype", r.ExchangeType)
	q = append(q,
		"queuetype="+r.QueueType.String(),
		"credit="+strconv.Itoa(r.Credit),
		"sasl="+r.SASL.String(),
		"receiversettle="+r.ReceiverSettle.String(),
		"sendersettle="+r.SenderSettle.String(),
		"backoff="+r.Backoff.String(),
	)
	addBool("deadletter", r.DeadLetter)
	if r.MaxPriority != 0 {
		q = append(q, "maxpriority="+strconv.FormatUint(uint64(r.MaxPriority), 10))
	}
	addBool("nondurable", r.NonDurable)
	addBool("tls", usesTLS(r.Addrs, r.TLS != nil))
	addBool("insecureskipverify", r.InsecureSkipVerify)
	addBool("insecureplainsasl", r.InsecurePlainSASL)
	q = addDuration(q, "idletimeout", r.IdleTimeout)
	q = addDuration(q, "dialtimeout", r.DialTimeout)
	q = addDuration(q, "operationtimeout", r.OperationTimeout)
	q = addDuration(q, "writetimeout", r.WriteTimeout)
	q = addDuration(q, "reconnectbase", r.ReconnectBase)
	q = addDuration(q, "reconnectmax", r.ReconnectMax)
	if r.MaxFrameSize != 0 {
		q = append(q, "maxframesize="+strconv.FormatUint(uint64(r.MaxFrameSize), 10))
	}
	if r.MaxSessions != 0 {
		q = append(q, "maxsessions="+strconv.FormatUint(uint64(r.MaxSessions), 10))
	}
	if r.MaxPending != 0 {
		q = append(q, "maxpending="+strconv.Itoa(r.MaxPending))
	}
	if r.ReconnectGrowth != 0 {
		q = append(q, "reconnectgrowth="+strconv.FormatFloat(r.ReconnectGrowth, 'g', -1, 64))
	}
	if r.ReconnectAttempts != 0 {
		q = append(q, "reconnectattempts="+strconv.Itoa(r.ReconnectAttempts))
	}
	addBool("noreconnect", r.NoReconnect)
	b.WriteByte('?')
	b.WriteString(strings.Join(q, "&"))
	return b.String()
}

// addDuration appends a duration parameter, omitting the zero that means "the
// default": a rendered `idletimeout=0s` would be read back as a request to
// disable the idle timeout rather than as a default.
func addDuration(q []string, key string, d time.Duration) []string {
	if d != 0 {
		return append(q, key+"="+d.String())
	}
	return q
}

// renderAddr renders one endpoint as a DSN authority [ParseDSN] reads back.
//
// The scheme is stripped, because the DSN carries its own, and the host is
// ESCAPED while the port is not. The escaping is the half that matters: a host
// name that came out of a DSN's own percent-decoding can hold any byte at all,
// including the "%" that begins an escape, so a rendering that emitted it raw
// would produce a connection string this driver's own parser refuses — which
// is the failure the shared core recorded for the same rendering.
func renderAddr(addr string) string {
	for _, prefix := range [...]string{"amqps://", "amqp+ssl://", "amqp://"} {
		if rest, ok := strings.CutPrefix(addr, prefix); ok {
			addr = rest
			break
		}
	}
	host, port := splitHostPort(addr)
	out := escapeDSN(host)
	if port != "" {
		out += ":" + port
	}
	return out
}

// escapeDSNSet is the set of bytes a DSN's authority cannot carry literally.
// It is the shared parser's own, which is what makes the two halves agree:
// anything this escapes, [github.com/iqhive/iqmq/dsn.Parse] decodes.
const escapeDSNSet = "%+ @:/?#&=[],"

// escapeDSN percent-encodes one authority or parameter component.
func escapeDSN(s string) string {
	if !strings.ContainsAny(s, escapeDSNSet) {
		return s
	}
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := range len(s) {
		c := s[i]
		if strings.IndexByte(escapeDSNSet, c) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// uint8Param reads a positive parameter into a uint8 field, refusing one that
// does not fit rather than wrapping it: `maxpriority=256` is a caller's
// mistake and a clamp would hide it.
func uint8Param(p *dsn.Params, key string, set func(uint8)) error {
	var n int
	if err := p.PositiveInt(key, func(v int) { n = v }); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n > 255 {
		return errMaxPriorityRange
	}
	set(uint8(n))
	return nil
}

// The DSN's own refusals.
var (
	errNilRegistrar = errcode.New(errcode.InvalidArgument,
		"iqmq/rabbitmq: Register was given no registry")
	errPathNotVhost = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: an AMQP 1.0 DSN has no path: AMQP 1.0 has no virtual-host field, so the virtual host is the `vhost` parameter and reaches RabbitMQ as the connection's hostname")
	errMaxPriorityRange = errcode.New(errcode.InvalidConfig,
		"iqmq/rabbitmq: maxpriority is a queue's x-max-priority and has to fit in a byte")
)

// addrUsesTLS reports whether this one address is dialled over TLS.
//
// The SCHEME wins when the address states one, because [normaliseAddr] keeps
// it and go-amqp picks the transport from it alone (v1.7.0 conn.go:238-246,
// measured on a socket: "amqp://host" with a TLSConfig set writes the
// cleartext AMQP header). `renderAddr` strips the scheme from the DSN, so
// reading `TLS != nil` as "TLS is on" made the rendered `tls=` disagree with
// the dial and a TLS configuration render a PLAINTEXT DSN -- the same defect
// The specification closed in the core, reached through the wrapper that
// carries its own renderer.
func addrUsesTLS(a string, haveTLSConfig bool) bool {
	if strings.HasPrefix(a, "amqps://") || strings.HasPrefix(a, "amqp+ssl://") {
		return true
	}
	if strings.HasPrefix(a, "amqp://") {
		return false
	}
	return haveTLSConfig
}

// usesTLS reports whether every address is dialled over TLS, which is what the
// DSN's single `tls=` can honestly say. A list that disagrees has no rendering
// and [Config.Validate] refuses it through the core.
func usesTLS(addrs []string, haveTLSConfig bool) bool {
	if len(addrs) == 0 {
		return haveTLSConfig
	}
	for _, a := range addrs {
		if !addrUsesTLS(a, haveTLSConfig) {
			return false
		}
	}
	return true
}

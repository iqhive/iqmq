package activemq

import (
	"context"
	"strconv"
	"strings"

	mq "github.com/iqhive/iqmq"
	core "github.com/iqhive/iqmq/drivers/amqp10"
	"github.com/iqhive/iqmq/dsn"
	"github.com/iqhive/iqmq/internal/activemq"
)

// Scheme is the DSN scheme this driver answers:
//
//	activemq://artemis:artemis@localhost:5672?broker=artemis&prefix=iqmq.
//	activemq://localhost:5672?broker=classic&scheduler=true
//
// It is one scheme for two products and the product is a required PARAMETER,
// which is the opposite of the obvious design and is the same decision
// [Config.Broker] documents: a scheme per product would let a deployment move
// from one to the other by editing four characters of a connection string, and
// the two products' topic mappings name nothing in common. Making it a
// parameter means a DSN that does not say is refused rather than guessed at.
//
// It is NOT "amqp10", which the shared core's generic profile claims — that
// scheme means "some AMQP 1.0 broker with a profile the caller supplies" —
// and it is not shared with `drivers/activemq/stomp`, which registers
// `activemq+stomp`. A registry in which one scheme could open either protocol
// would make a connection string ambiguous about which capability table the
// caller gets, and the two tables really differ: no nack at all over STOMP, no
// selector over AMQP.
const Scheme = "activemq"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each
// of them with a registry it owns. Registering this module and
// `drivers/activemq/stomp` with one registry is the supported arrangement —
// the two schemes do not collide — and it is how a deployment moves one
// application from one protocol to the other by editing a connection string.
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
// core's, through [core.ParseInto], and this driver's own.
// [dsn.Params.Strict] is called ONCE at the end, so a parameter neither half
// claims is a construction error naming it rather than a setting nobody
// notices was ignored (ADR-010).
//
// This driver's parameters:
//
//	broker=artemis|classic    REQUIRED: which ActiveMQ
//	prefix=<string>           prepended to every address and queue name
//	scheduler=true|false      the broker's message scheduler is enabled
//	prioritised=true|false    the broker's destination policy reorders by priority
//	                          (Classic's is off by default)
//	autocreate=true|false     the broker still creates a destination from an
//	                          attach
//	deadletter=true|false     the destinations have a dead-letter address
//	deadlettertopic=<topic>   dead-letter to another of this driver's topics
//	credit=<n>                the link credit ceiling of one source
//
// and the core's, passed through unchanged: `containerid`, `hostname`, `sasl`,
// `tls`, `insecureskipverify`, `insecureplainsasl`, `sendersettle`,
// `receiversettle`, `idletimeout`, `maxframesize`, `maxsessions`, `maxcredit`,
// `maxpending`, `nondurable`, `dialtimeout`, `operationtimeout`,
// `writetimeout`, `backoff`, `reconnectbase`, `reconnectmax`,
// `reconnectgrowth`, `reconnectattempts` and `noreconnect`.
//
// A DSN with a PATH is refused rather than ignored. AMQP 1.0 has no
// virtual-host field and neither product has virtual hosts, so a path could
// only be a caller's mistaken expectation and the answer to an option a
// driver cannot provide is to refuse it.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	if c.Path != "" {
		return Config{}, errPathUnsupported
	}
	cc, err := core.ParseInto(&c)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Addrs:              cc.Addrs,
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
	}
	p := &c.Params
	broker := ""
	noDeadLetter := false
	hadDeadLetter := false
	for _, step := range [...]func() error{
		func() error { return p.String("broker", &broker) },
		func() error { return p.String("prefix", &cfg.Prefix) },
		func() error { return p.String("deadlettertopic", &cfg.DeadLetterTopic) },
		func() error { return p.Bool("scheduler", &cfg.Scheduler) },
		func() error { return p.Bool("prioritised", &cfg.Prioritised) },
		func() error { return p.Bool("autocreate", &cfg.BrokerAutoCreates) },
		func() error {
			// `deadletter` is a TRISTATE on the wire and two booleans in
			// the struct, because the struct's default is true: a DSN that
			// says nothing must keep the default, and `deadletter=false`
			// has to be able to turn it off.
			var v bool
			if err := p.Bool("deadletter", &v); err != nil {
				return err
			}
			hadDeadLetter = p.Has("deadletter")
			noDeadLetter = hadDeadLetter && !v
			return nil
		},
		func() error { return p.PositiveInt("credit", &cfg.Credit) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.NoDeadLetter = noDeadLetter
	if broker == "" {
		return Config{}, errDSNNoBroker
	}
	b, ok := activemq.ParseBroker(strings.ToLower(broker))
	if !ok {
		return Config{}, errDSNBadBroker
	}
	cfg.Broker = b
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// DSN renders the configuration as a connection string with the credential
// redacted, which is the round trip the specification asks for: what comes back parses into
// the same configuration except for the password, which cannot be recovered
// from a redacted string and must not be.
func (c Config) DSN() string {
	r := c.resolve()
	var b strings.Builder
	b.Grow(224)
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
	// The broker is first and unconditional: it is the one parameter without
	// which the string cannot be parsed back at all.
	q = append(q, "broker="+r.Broker.String())
	add("prefix", r.Prefix)
	add("hostname", r.HostName)
	add("containerid", r.ContainerID)
	add("deadlettertopic", r.DeadLetterTopic)
	// Credit is rendered only when it is set. The parser reads it with
	// PositiveInt, so `credit=0` is a value it cannot take -- and the zero
	// already means "the core's default", so rendering it would make the
	// round trip unparseable for every configuration that named none.
	if r.Credit != 0 {
		q = append(q, "credit="+itoa(r.Credit))
	}
	q = append(q,
		"sasl="+r.SASL.String(),
		"receiversettle="+r.ReceiverSettle.String(),
		"sendersettle="+r.SenderSettle.String(),
		"backoff="+r.Backoff.String(),
	)
	addBool("scheduler", r.Scheduler)
	addBool("prioritised", r.Prioritised)
	addBool("autocreate", r.BrokerAutoCreates)
	// `deadletter` is rendered only when it is FALSE, because true is the
	// default and rendering it would make the round trip depend on which of
	// the two struct fields the parser set.
	if !r.DeadLetter {
		q = append(q, "deadletter=false")
	}
	addBool("nondurable", r.NonDurable)
	addBool("tls", usesTLS(r.Addrs, r.TLS != nil))
	addBool("insecureskipverify", r.InsecureSkipVerify)
	addBool("insecureplainsasl", r.InsecurePlainSASL)
	addDuration(&q, "idletimeout", r.IdleTimeout)
	addDuration(&q, "dialtimeout", r.DialTimeout)
	addDuration(&q, "operationtimeout", r.OperationTimeout)
	addDuration(&q, "writetimeout", r.WriteTimeout)
	addDuration(&q, "reconnectbase", r.ReconnectBase)
	addDuration(&q, "reconnectmax", r.ReconnectMax)
	if r.MaxFrameSize != 0 {
		q = append(q, "maxframesize="+strconv.FormatUint(uint64(r.MaxFrameSize), 10))
	}
	if r.MaxSessions != 0 {
		q = append(q, "maxsessions="+strconv.FormatUint(uint64(r.MaxSessions), 10))
	}
	if r.MaxPending != 0 {
		q = append(q, "maxpending="+itoa(r.MaxPending))
	}
	if r.ReconnectGrowth != 0 {
		q = append(q, "reconnectgrowth="+strconv.FormatFloat(r.ReconnectGrowth, 'g', -1, 64))
	}
	if r.ReconnectAttempts != 0 {
		q = append(q, "reconnectattempts="+itoa(r.ReconnectAttempts))
	}
	addBool("noreconnect", r.NoReconnect)
	if len(q) > 0 {
		b.WriteByte('?')
		b.WriteString(strings.Join(q, "&"))
	}
	return b.String()
}

// renderAddr strips the scheme a normalised address carries, so that
// [ParseDSN] reads back the endpoint list the caller gave.
func renderAddr(a string) string {
	if i := strings.Index(a, "://"); i >= 0 {
		return a[i+3:]
	}
	return a
}

// escapeDSN percent-encodes the characters that mean something to the shared
// parser. It is the same set `drivers/rabbitmq/amqp10` escapes, for the same
// reason: a rendered DSN has to parse back.
func escapeDSN(s string) string {
	const set = "%+ @:/?#&=[],"
	if !strings.ContainsAny(s, set) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	const hex = "0123456789ABCDEF"
	for i := range len(s) {
		ch := s[i]
		if strings.IndexByte(set, ch) >= 0 {
			b.WriteByte('%')
			b.WriteByte(hex[ch>>4])
			b.WriteByte(hex[ch&0xF])
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

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

package nats

import (
	"context"
	"crypto/tls"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/errcode"
)

// Scheme is the DSN scheme this driver answers:
//
//	nats://user:password@host:4222,host2:4222/prefix?creds=/path.creds&tls=true
//
// It is the Core profile alone. "natsjs://" is the JetStream driver's and is
// deliberately not claimed here: ADR-010's the specification chose separate modules
// precisely so that a caller cannot confuse one profile's capabilities with the
// other's, and a Core driver that answered the JetStream scheme would undo it.
const Scheme = DriverName

// DefaultPort is the port a DSN endpoint that names none is dialled on.
const DefaultPort = "4222"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each of
// them with a registry it owns.
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

// ParseDSN turns a NATS DSN into a [Config].
//
// It is [bitbucket.org/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [bitbucket.org/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver does
// not have into a construction error naming it, rather than a setting nobody
// notices was ignored (ADR-010).
//
// The parameters, all optional:
//
//	name=<string>              connection name the server reports
//	creds=<path> NATS credentials file
//	token=<secret>             token authentication
//	tls=true|false             dial with TLS using the platform roots
//	insecureskipverify=true|false   with tls=true, do not verify the server
//	pendingmessages=<n>        per-subscription SetPendingLimits, messages
//	pendingbytes=<n>           per-subscription SetPendingLimits, bytes
//	connecttimeout=<duration>  bound on the initial dial
//	flushtimeout=<duration>    bound on a confirmation round trip
//	reconnectwait=<duration>   first delay between reconnection attempts
//	maxreconnects=<n>          attempts; negative for "for ever"
//	pinginterval=<duration>    connection liveness probe
//	maxpingsout=<n>            unanswered pings before the link is down
//	draintimeout=<duration>    bound on the drain a Close performs
//	noecho=true|false          do not deliver a connection its own publications
//
// The path names [Config.SubjectPrefix], so nats://host/acme prefixes every
// subject with "acme.".
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg := Config{
		Username:      c.Username,
		Password:      c.Password,
		SubjectPrefix: c.Path,
	}
	for _, h := range c.Hosts {
		if !dialableHost(h.Host) {
			return Config{}, errDSNHost
		}
		if h.Port == "" {
			h.Port = DefaultPort
		}
		cfg.Addrs = append(cfg.Addrs, "nats://"+h.String())
	}
	p := &c.Params
	var token string
	var useTLS, insecure bool
	for _, step := range [...]func() error{
		func() error { return p.String("name", &cfg.Name) },
		func() error { return p.String("creds", &cfg.CredentialsFile) },
		func() error { return p.String("token", &token) },
		func() error { return p.Bool("tls", &useTLS) },
		func() error { return p.Bool("insecureskipverify", &insecure) },
		func() error { return p.Int("pendingmessages", &cfg.PendingMessages) },
		func() error { return p.Int("pendingbytes", &cfg.PendingBytes) },
		func() error { return p.Duration("connecttimeout", &cfg.ConnectTimeout) },
		func() error { return p.Duration("flushtimeout", &cfg.FlushTimeout) },
		func() error { return p.Duration("reconnectwait", &cfg.ReconnectWait) },
		func() error { return p.Int("maxreconnects", &cfg.MaxReconnects) },
		func() error { return p.Duration("pinginterval", &cfg.PingInterval) },
		func() error { return p.Int("maxpingsout", &cfg.MaxPingsOut) },
		func() error { return p.Duration("draintimeout", &cfg.DrainTimeout) },
		func() error { return p.Bool("noecho", &cfg.NoEcho) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.Token = dsn.Secret(token)
	if useTLS {
		// A DSN cannot carry a *tls.Config, so "tls=true" means the
		// platform roots and the server name taken from the address, which
		// is the only safe default a connection string can ask for.
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: insecure} //nolint:gosec // the field is the operator's explicit request and Validate refuses it without TLS
		cfg.InsecureSkipVerify = insecure
	} else if insecure {
		// Refusing is the only honest answer: the alternative is a DSN
		// that dialled in the clear after the operator asked to skip
		// verification, which reads as "less safe" and is "no TLS at all".
		return Config{}, errInsecureWithoutTLS
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// dialableHost reports whether a host the shared parser DECODED can be written
// into the server URL "nats://<host>:<port>" and still name only itself.
//
// nats.go reads that URL with net/url, which gives "@" (user information), "/"
// (a path), "?" (a query) and "#" (a fragment) a meaning of their own, and a
// DSN writes any of them into a host as an escape: "natsjs://tok%40real-host"
// dialled real-host with the token "tok", and "a%2Fb" dialled "a". nats.go
// also read the server string as a comma-separated LIST before net/url saw it,
// so "nowhere.invalid%2C127.0.0.1" was two servers and the second was dialled.
// A host name or an IP literal holds none of them, nor a backslash, a space or
// a control byte, so such a host is refused rather than escaped: there is no
// spelling of it that dials what the DSN names.
func dialableHost(host string) bool {
	for i := range len(host) {
		switch c := host[i]; {
		case c <= ' ', c == 0x7f, c == '@', c == '/', c == '?', c == '#', c == '\\', c == ',':
			return false
		}
	}
	return true
}

// errDSNHost is the refusal of a host [dialableHost] rejects.
var errDSNHost = errcode.New(errcode.InvalidConfig,
	"iqmq/nats: a DSN host decodes to a byte a server URL gives a meaning to -- \"@\", \"/\", \"?\", \"#\", \",\", a backslash, a space or a control character -- so it would dial somewhere the DSN does not name")

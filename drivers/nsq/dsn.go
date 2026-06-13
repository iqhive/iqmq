package nsq

import (
	"context"
	"crypto/tls"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
)

// Scheme is the DSN scheme this driver answers:
//
//	nsq://nsqd:4150,nsqd2:4150/prefix?lookupd=http://lookupd:4161&maxinflight=200
const Scheme = DriverName

// DefaultPort is the port a DSN endpoint that names none is dialled on. It is
// nsqd's TCP port, not its HTTP one: the authority of an NSQ DSN names the
// brokers this driver PUBLISHES through, and publication is the TCP protocol.
const DefaultPort = "4150"

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

// ParseDSN turns an NSQ DSN into a [Config].
//
// It is [bitbucket.org/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [bitbucket.org/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver does
// not have into a construction error naming it, rather than a setting nobody
// notices was ignored (ADR-010).
//
// The authority is the nsqd TCP endpoints; the path names [Config.TopicPrefix].
// The parameters, all optional:
//
//	lookupd=<addr>[,<addr>]    the nsqlookupd HTTP addresses
//	admin=<addr>[,<addr>]      the nsqd HTTP addresses for Admin
//	clientid=<string>          the client identifier nsqd reports
//	hostname=<string>          the host name nsqd reports
//	useragent=<string>         the user agent nsqd reports
//	authsecret=<secret>        nsqd's AUTH shared secret
//	ensuretopology=true|false  create the topic and channel deliberately
//	metadata=nativestrict|portableenvelope|adaptive|lenientdrop
//	                           the mode a handle that named none gets
//	maxmessagebytes=<n>        mirror of nsqd's --max-msg-size
//	maxbodybytes=<n>           mirror of nsqd's --max-body-size
//	maxrequeuedelay=<duration> mirror of nsqd's --max-req-timeout
//	ackdeadline=<duration>     the per-message timeout asked of nsqd
//	maxackdeadline=<duration>  mirror of nsqd's --max-msg-timeout
//	leaseguard=<duration>      how early a lease stops being vouched for
//	dialtimeout=<duration>     bound on one connection attempt
//	readtimeout=<duration>     connection read deadline
//	writetimeout=<duration>    connection write deadline
//	heartbeatinterval=<duration>  how often nsqd sends a heartbeat
//	lookupdpollinterval=<duration>  how often nsqlookupd is polled
//	outputbuffertimeout=<duration>  how long nsqd may hold a write
//	apitimeout=<duration>      bound on one nsqd HTTP request
//	deflate=true|false         wire compression
//	deflatelevel=<1..9>        with deflate
//	snappy=true|false          wire compression
//	tls=true|false             dial with TLS using the platform roots
//	insecureskipverify=true|false   with tls=true, do not verify the server
//
// `max_in_flight` is deliberately NOT a parameter, though the example DSN
// shows one. Prefetch is the CORE's: it is resolved from the consumer
// configuration and handed to the driver in `SourceConfig.Prefetch`, and a DSN
// that set it would be a second owner of one number — the driver would then
// have to choose between them, silently.
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg := Config{TopicPrefix: c.Path}
	for _, h := range c.Hosts {
		if h.Port == "" {
			h.Port = DefaultPort
		}
		cfg.Addrs = append(cfg.Addrs, h.String())
	}
	p := &c.Params
	var secret string
	var useTLS, insecure bool
	for _, step := range [...]func() error{
		func() error { return stringList(p, "lookupd", &cfg.LookupdAddrs) },
		func() error { return stringList(p, "admin", &cfg.AdminAddrs) },
		func() error { return p.String("clientid", &cfg.ClientID) },
		func() error { return p.String("hostname", &cfg.Hostname) },
		func() error { return p.String("useragent", &cfg.UserAgent) },
		func() error { return p.String("authsecret", &secret) },
		func() error { return p.Bool("ensuretopology", &cfg.EnsureTopology) },
		func() error { return metadataParam(p, &cfg.Metadata) },
		func() error { return p.Int64("maxmessagebytes", &cfg.MaxMessageBytes) },
		func() error { return p.Int64("maxbodybytes", &cfg.MaxBodyBytes) },
		func() error { return p.Duration("maxrequeuedelay", &cfg.MaxRequeueDelay) },
		func() error { return p.Duration("ackdeadline", &cfg.AckDeadline) },
		func() error { return p.Duration("maxackdeadline", &cfg.MaxAckDeadline) },
		func() error { return p.Duration("leaseguard", &cfg.LeaseGuard) },
		func() error { return p.Duration("dialtimeout", &cfg.DialTimeout) },
		func() error { return p.Duration("readtimeout", &cfg.ReadTimeout) },
		func() error { return p.Duration("writetimeout", &cfg.WriteTimeout) },
		func() error { return p.Duration("heartbeatinterval", &cfg.HeartbeatInterval) },
		func() error { return p.Duration("lookupdpollinterval", &cfg.LookupdPollInterval) },
		func() error { return p.Duration("outputbuffertimeout", &cfg.OutputBufferTimeout) },
		func() error { return p.Duration("apitimeout", &cfg.APITimeout) },
		func() error { return p.Bool("deflate", &cfg.Deflate) },
		func() error { return p.Int("deflatelevel", &cfg.DeflateLevel) },
		func() error { return p.Bool("snappy", &cfg.Snappy) },
		func() error { return p.Bool("tls", &useTLS) },
		func() error { return p.Bool("insecureskipverify", &insecure) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	cfg.AuthSecret = dsn.Secret(secret)
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

// stringList reads a comma-separated address list.
//
// The separator is a comma and not a repeated key, because the shared parser
// refuses a repeated key outright — `iqmq/dsn: the DSN repeats a parameter` —
// and that refusal is right for every scalar it was written for. It does mean
// a driver with a list-valued parameter has to pick its own separator, and
// this one picks the same character the DSN's own authority separates
// endpoints with.
//
// An empty element is dropped rather than becoming an empty address, so a
// trailing comma is not a configuration error a person has to find.
func stringList(p *dsn.Params, key string, dst *[]string) error {
	var raw string
	if err := p.String(key, &raw); err != nil {
		return err
	}
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*dst = append(*dst, part)
		}
	}
	return nil
}

// metadataParam reads the mode by its canonical lower-case name, which is
// the one mq.MetadataMode.String() prints — so the DSN and every log line that
// renders a mode agree, rather than a DSN spelling of its own.
func metadataParam(p *dsn.Params, dst *mq.MetadataMode) error {
	names := make([]string, 0, 4)
	for m := mq.MetadataNativeStrict; m.Valid(); m++ {
		names = append(names, m.String())
	}
	return p.Enum("metadata", names, func(i int) { *dst = mq.MetadataMode(i) })
}

package nsq

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/dsn"
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
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver does
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
		func() error {
			return stringList(p, "lookupd", func(v string) { cfg.LookupdAddrs = append(cfg.LookupdAddrs, v) })
		},
		func() error {
			return stringList(p, "admin", func(v string) { cfg.AdminAddrs = append(cfg.AdminAddrs, v) })
		},
		func() error { return p.String("clientid", func(v string) { cfg.ClientID = v }) },
		func() error { return p.String("hostname", func(v string) { cfg.Hostname = v }) },
		func() error { return p.String("useragent", func(v string) { cfg.UserAgent = v }) },
		func() error { return p.String("authsecret", func(v string) { secret = v }) },
		func() error { return p.Bool("ensuretopology", func(v bool) { cfg.EnsureTopology = v }) },
		func() error { return metadataParam(p, &cfg.Metadata) },
		func() error { return p.Int64("maxmessagebytes", func(v int64) { cfg.MaxMessageBytes = v }) },
		func() error { return p.Int64("maxbodybytes", func(v int64) { cfg.MaxBodyBytes = v }) },
		func() error { return p.Duration("maxrequeuedelay", func(v time.Duration) { cfg.MaxRequeueDelay = v }) },
		func() error { return p.Duration("ackdeadline", func(v time.Duration) { cfg.AckDeadline = v }) },
		func() error { return p.Duration("maxackdeadline", func(v time.Duration) { cfg.MaxAckDeadline = v }) },
		func() error { return p.Duration("leaseguard", func(v time.Duration) { cfg.LeaseGuard = v }) },
		func() error { return p.Duration("dialtimeout", func(v time.Duration) { cfg.DialTimeout = v }) },
		func() error { return p.Duration("readtimeout", func(v time.Duration) { cfg.ReadTimeout = v }) },
		func() error { return p.Duration("writetimeout", func(v time.Duration) { cfg.WriteTimeout = v }) },
		func() error {
			return p.Duration("heartbeatinterval", func(v time.Duration) { cfg.HeartbeatInterval = v })
		},
		func() error {
			return p.Duration("lookupdpollinterval", func(v time.Duration) { cfg.LookupdPollInterval = v })
		},
		func() error {
			return p.Duration("outputbuffertimeout", func(v time.Duration) { cfg.OutputBufferTimeout = v })
		},
		func() error { return p.Duration("apitimeout", func(v time.Duration) { cfg.APITimeout = v }) },
		func() error { return p.Bool("deflate", func(v bool) { cfg.Deflate = v }) },
		func() error { return p.Int("deflatelevel", func(v int) { cfg.DeflateLevel = v }) },
		func() error { return p.Bool("snappy", func(v bool) { cfg.Snappy = v }) },
		func() error { return p.Bool("tls", func(v bool) { useTLS = v }) },
		func() error { return p.Bool("insecureskipverify", func(v bool) { insecure = v }) },
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

// stringList reads a comma-separated address list and calls add with each address.
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
func stringList(p *dsn.Params, key string, add func(string)) error {
	var raw string
	if err := p.String(key, func(v string) { raw = v }); err != nil {
		return err
	}
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			add(part)
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

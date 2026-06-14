package redis

import (
	"context"
	"crypto/tls"
	"strconv"
	"strings"

	mq "bitbucket.org/iqhive/iqmq"
	"bitbucket.org/iqhive/iqmq/dsn"
	"bitbucket.org/iqhive/iqmq/internal/streams"
)

// Scheme is the DSN scheme this driver answers:
//
//	redis://user:password@host:6379,host2:6379/0?partitions=4&nack=zsetmover
//
// It is the rueidis driver alone. `drivers/redis/goredis` registers no scheme
// of its own — it is the same broker behind a different client library, so a
// DSN naming "redis" has to mean one thing, and which client library serves it
// is a build-time choice a connection string has no business making.
const Scheme = DriverName

// DefaultPort is the port a DSN endpoint that names none is dialled on.
const DefaultPort = "6379"

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

// ParseDSN turns a Redis DSN into a [Config].
//
// It is [bitbucket.org/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [bitbucket.org/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver
// does not have into a construction error naming it, rather than a setting
// nobody notices was ignored (ADR-010).
//
// The path is the logical database number, which is Redis's own convention:
// redis://host/3 is database 3.
//
// The parameters, all optional:
//
//	keyprefix=<string>          prepended to every key, inside the hash tag
//	partitions=<n>              shards per topic (>= 1)
//	maxlen=<n>                  XADD MAXLEN ~ n on every stream written
//	autocreate=true|false       may create streams and consumer groups
//	ackdeadline=<duration>      the lease, and the reclaim loop's min-idle
//	claiminterval=<duration>    how often the reclaim loop sweeps
//	claimcount=<n>              entries per sweep round per shard
//	block=<duration>            XREADGROUP BLOCK
//	nack=<kind>                 leavepending|zsetmover|readd|stop
//	ackguard=owner|none         how strongly an ack checks ownership
//	consumerprefix=<string>     first part of each source's consumer name
//	moverinterval=<duration>    delayed-mover pass interval
//	movercount=<n>              parked entries released per pass per shard
//	durablelocal=<n>            WAITAOF's numlocal for ConfirmDurable
//	durablereplicas=<n>         WAITAOF's numreplicas
//	durabletimeout=<duration>   bound on the WAITAOF
//	dialtimeout=<duration>      bound on the initial connection
//	deadlettertopic=<string>    the topic a Reject routes to (none by default)
//	deadlettersuffix=<string>   a key beside the topic's own, sharing its tag
//	tls=true|false              dial with TLS using the platform roots
//	insecureskipverify=true|false   with tls=true, do not verify the server
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
	}
	for _, h := range c.Hosts {
		if h.Port == "" {
			h.Port = DefaultPort
		}
		cfg.Addrs = append(cfg.Addrs, h.String())
	}
	if path := strings.TrimPrefix(c.Path, "/"); path != "" {
		db, derr := strconv.Atoi(path)
		if derr != nil || db < 0 {
			return Config{}, errBadDatabase
		}
		cfg.DB = db
	}
	p := &c.Params
	var (
		useTLS, insecure bool
		nackName         = ""
		guardName        = ""
	)
	for _, step := range [...]func() error{
		func() error { return p.String("keyprefix", &cfg.KeyPrefix) },
		func() error { return p.Int("partitions", &cfg.Partitions) },
		func() error { return p.Int64("maxlen", &cfg.MaxLen) },
		func() error { return p.Bool("autocreate", &cfg.AutoCreate) },
		func() error { return p.Duration("ackdeadline", &cfg.AckDeadline) },
		func() error { return p.Duration("claiminterval", &cfg.ClaimInterval) },
		func() error { return p.Int("claimcount", &cfg.ClaimCount) },
		func() error { return p.Duration("block", &cfg.Block) },
		func() error { return p.String("nack", &nackName) },
		func() error { return p.String("ackguard", &guardName) },
		func() error { return p.String("consumerprefix", &cfg.ConsumerPrefix) },
		func() error { return p.Duration("moverinterval", &cfg.MoverInterval) },
		func() error { return p.Int("movercount", &cfg.MoverCount) },
		func() error { return p.Int("durablelocal", &cfg.DurableLocal) },
		func() error { return p.Int("durablereplicas", &cfg.DurableReplicas) },
		func() error { return p.Duration("durabletimeout", &cfg.DurableTimeout) },
		func() error { return p.Duration("dialtimeout", &cfg.DialTimeout) },
		func() error { return p.String("deadlettertopic", &cfg.DeadLetterTopic) },
		func() error { return p.String("deadlettersuffix", &cfg.DeadLetterSuffix) },
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
	// The two enumerations are read as strings and mapped here rather than
	// through dsn.Params.Enum, because Enum accepts any name in its slice
	// INCLUDING index 0 and cannot refuse its own zero name — which is one of
	// the three accessors the maintenance notes records as missing from `dsn`
	// (the sentinel-or-value reader and the positive-only integer are the
	// other two). Here it matters: "auto" is a real spelling of the nack kind
	// and "owner" of the guard, so the mapping has to distinguish "the
	// parameter was absent" from "the parameter named the default", and
	// Params.Enum's set callback cannot.
	if nackName != "" {
		kind, ok := streams.ParseKind(nackName)
		if !ok || kind == streams.Auto {
			return Config{}, errNackKind
		}
		cfg.Nack = kind
	}
	if guardName != "" {
		guard, ok := streams.ParseAckGuard(guardName)
		if !ok {
			return Config{}, errAckGuard
		}
		cfg.AckGuard = guard
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
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

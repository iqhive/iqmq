package kafka

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
	"github.com/iqhive/iqmq/dsn"
)

// Scheme is the DSN scheme this driver answers:
//
//	kafka://user:password@broker1:9092,broker2:9092/prefix?sasl=scram-sha-512&tls=true
//
// It is the franz-go driver. `drivers/kafka/kafkago` is a separate module with
// a scheme of its own: two client libraries with different rebalance
// behaviour must not be reachable through one connection string, because a
// caller reading a DSN cannot tell which of them it got.
const Scheme = DriverName

// DefaultPort is the port a DSN endpoint that names none is dialled on.
const DefaultPort = "9092"

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

// ParseDSN turns a Kafka DSN into a [Config].
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver
// does not have into a construction error naming it, rather than a setting
// nobody notices was ignored (ADR-010).
//
// The path names [Config.TopicPrefix], so kafka://host/acme. prefixes every
// topic with "acme." — the prefix is used verbatim, separator included,
// because Kafka has no topic separator of its own and inventing one would
// make two deployments' names collide differently from how the operator's own
// tooling reads them.
//
// The parameters, all optional:
//
//	clientid=<string>            the client identifier the broker logs
//	sasl=plain|scram-sha-256|scram-sha-512
//	tls=true|false               dial with TLS using the platform roots
//	insecureskipverify=true|false   with tls=true, do not verify the server
//	disableidempotency=true|false   turn the idempotent producer off
//	unboundeddelivery=true|false    let an in-flight record retry for ever
//	autocreatetopics=true|false  let a produce create a topic
//	readcommitted=true|false     skip aborted and open transactions
//	instanceid=<string>          static group membership identifier
//	transactionalid=<string>     make the producer transactional
//	linger=<duration>            producer batching linger
//	batchmaxbytes=<n>            bound one produced batch
//	maxbufferedrecords=<n>       bound the producer's buffer
//	fetchmaxwait=<duration>      how long a fetch waits for records
//	fetchmaxbytes=<n>            bound one fetch
//	sessiontimeout=<duration>    group session timeout
//	rebalancetimeout=<duration>  group rebalance timeout
//	heartbeatinterval=<duration> group heartbeat interval
//	commitinterval=<duration>    how often the watermark is committed
//	revokewait=<duration>        bound on the wait in OnPartitionsRevoked
//	requesttimeout=<duration>    bound on an administrative round trip
//	deliverytimeout=<duration>   bound on the producer's retries of one record
//	partitions=<n>               partitions the typed Admin creates with
//	replicationfactor=<n>        replication factor the typed Admin uses
//	tracker=<kind> the offset tracker kind
//	nack=<kind> the nack strategy kind
//	deadlettertopic=<topic>      where a rejected record is routed
//	maxattempts=<n>              deliveries before the failure route
//	retries=<n>                  how many retry topics to walk
//	nackdelay=<duration>         hold before a redelivery
func ParseDSN(s string) (Config, error) {
	c, err := dsn.Parse(s)
	if err != nil {
		return Config{}, err
	}
	if c.Scheme != Scheme {
		return Config{}, mq.ErrDriverNotRegistered
	}
	cfg := Config{
		Username:    c.Username,
		Password:    c.Password,
		TopicPrefix: c.Path,
	}
	for _, h := range c.Hosts {
		if h.Port == "" {
			h.Port = DefaultPort
		}
		cfg.Brokers = append(cfg.Brokers, h.String())
	}
	p := &c.Params
	var (
		useTLS, insecure   bool
		batchMax, fetchMax int
		maxAttempts        int
	)
	for _, step := range [...]func() error{
		func() error { return p.String("clientid", func(v string) { cfg.ClientID = v }) },
		func() error { return enumParam(p, "sasl", SASLNames(), func(i int) { cfg.SASL = SASLMechanism(i) }) },
		func() error { return p.Bool("tls", func(v bool) { useTLS = v }) },
		func() error { return p.Bool("insecureskipverify", func(v bool) { insecure = v }) },
		func() error { return p.Bool("disableidempotency", func(v bool) { cfg.DisableIdempotency = v }) },
		func() error { return p.Bool("unboundeddelivery", func(v bool) { cfg.UnboundedDelivery = v }) },
		func() error { return p.Bool("autocreatetopics", func(v bool) { cfg.AutoCreateTopics = v }) },
		func() error { return p.Bool("readcommitted", func(v bool) { cfg.ReadCommitted = v }) },
		func() error { return p.String("instanceid", func(v string) { cfg.InstanceID = v }) },
		func() error { return p.String("transactionalid", func(v string) { cfg.TransactionalID = v }) },
		func() error { return p.Duration("linger", func(v time.Duration) { cfg.Linger = v }) },
		func() error { return p.Int("batchmaxbytes", func(v int) { batchMax = v }) },
		func() error { return p.Int("maxbufferedrecords", func(v int) { cfg.MaxBufferedRecords = v }) },
		func() error { return p.Duration("fetchmaxwait", func(v time.Duration) { cfg.FetchMaxWait = v }) },
		func() error { return p.Int("fetchmaxbytes", func(v int) { fetchMax = v }) },
		func() error { return p.Duration("sessiontimeout", func(v time.Duration) { cfg.SessionTimeout = v }) },
		func() error {
			return p.Duration("rebalancetimeout", func(v time.Duration) { cfg.RebalanceTimeout = v })
		},
		func() error {
			return p.Duration("heartbeatinterval", func(v time.Duration) { cfg.HeartbeatInterval = v })
		},
		func() error { return p.Duration("commitinterval", func(v time.Duration) { cfg.CommitInterval = v }) },
		func() error { return p.Duration("revokewait", func(v time.Duration) { cfg.RevokeWait = v }) },
		func() error { return p.Duration("requesttimeout", func(v time.Duration) { cfg.RequestTimeout = v }) },
		func() error { return p.Duration("deliverytimeout", func(v time.Duration) { cfg.DeliveryTimeout = v }) },
		func() error { return intoInt32(p, "partitions", func(v int32) { cfg.Partitions = v }) },
		func() error { return intoInt16(p, "replicationfactor", func(v int16) { cfg.ReplicationFactor = v }) },
		func() error { return kindParam(p, "tracker", offset.ParseKind, &cfg.Tracker) },
		func() error { return kindParam(p, "nack", nack.ParseKind, &cfg.Nack) },
		func() error {
			return p.String("deadlettertopic", func(v string) { cfg.NackConfig.DeadLetterTopic = v })
		},
		func() error { return p.Int("maxattempts", func(v int) { maxAttempts = v }) },
		func() error { return p.Int("retries", func(v int) { cfg.NackConfig.Retries = v }) },
		func() error { return p.Duration("nackdelay", func(v time.Duration) { cfg.NackConfig.Delay = v }) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	if batchMax < 0 || fetchMax < 0 || maxAttempts < 0 || batchMax > 1<<31-1 || fetchMax > 1<<31-1 || maxAttempts > 1<<32-1 {
		return Config{}, errNegativeBound
	}
	cfg.BatchMaxBytes, cfg.FetchMaxBytes = int32(batchMax), int32(fetchMax)
	cfg.NackConfig.MaxAttempts = uint32(maxAttempts)
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

// enumParam reads a parameter whose values are the names of an enumeration.
//
// It exists because [github.com/iqhive/iqmq/dsn.Params.Enum] accepts index
// zero, and this driver's zero name is "none": a DSN that said `sasl=none`
// would be indistinguishable from one that said nothing, which is the third
// of the three accessors the specification found missing from the dsn package.
// Naming the default explicitly is accepted here — it
// means the same thing — so what this wrapper really adds is the refusal of a
// name the enumeration does not have, which Enum already does, and a place to
// record that the gap is still open.
func enumParam(p *dsn.Params, key string, names []string, set func(int)) error {
	return p.Enum(key, names, set)
}

// kindParam reads a variant-family kind by its slug.
func kindParam[K any](p *dsn.Params, key string, parse func(string) (K, bool), dst *K) error {
	var slug string
	if err := p.String(key, func(v string) { slug = v }); err != nil {
		return err
	}
	if slug == "" {
		return nil
	}
	k, ok := parse(strings.ToLower(slug))
	if !ok {
		return errUnknownKind
	}
	*dst = k
	return nil
}

// intoInt32 reads an integer parameter into an int32 field.
func intoInt32(p *dsn.Params, key string, set func(int32)) error {
	var v int
	if err := p.Int(key, func(w int) { v = w }); err != nil {
		return err
	}
	if v == 0 {
		return nil
	}
	if v < -1 || v > 1<<31-1 {
		return errBadTopology
	}
	set(int32(v))
	return nil
}

// intoInt16 reads an integer parameter into an int16 field.
func intoInt16(p *dsn.Params, key string, set func(int16)) error {
	var v int
	if err := p.Int(key, func(w int) { v = w }); err != nil {
		return err
	}
	if v == 0 {
		return nil
	}
	if v < -1 || v > 1<<15-1 {
		return errBadTopology
	}
	set(int16(v))
	return nil
}

package kafkago

import (
	"context"
	"crypto/tls"
	"strings"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
	"github.com/iqhive/iqmq/dsn"
)

// Scheme is the DSN scheme this driver answers:
//
//	kafkago://user:password@broker1:9092,broker2:9092/prefix?sasl=scram-sha-512&tls=true
//
// It is NOT `kafka://`, which belongs to the franz-go driver, and the
// separation is deliberate: the two drivers have different rebalance
// behaviour, different producer guarantees and different capability tables, so
// two client libraries must not be reachable through one connection string —
// a caller reading a DSN could not tell which of them it got. The franz-go
// driver's own documentation says the same from the other side.
const Scheme = "kafkago"

// DefaultPort is the port a DSN endpoint that names none is dialled on.
const DefaultPort = "9092"

// Register registers this driver with a DSN registry under [Scheme]
// (ADR-012).
//
// A driver exposes this and nothing else: it never reaches for a global
// registry of its own, so a library that composes two brokers registers each
// of them with a registry it owns — and a library that wants BOTH Kafka
// drivers registers both, which is possible precisely because the schemes
// differ.
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

// ParseDSN turns a kafkago DSN into a [Config].
//
// It is [github.com/iqhive/iqmq/dsn.Parse] plus this driver's own parameter
// table, which is the point: the shared parser owns the grammar, the 8 KiB
// bound, the percent decoding and the credential redaction, and
// [github.com/iqhive/iqmq/dsn.Params.Strict] turns a parameter this driver
// does not have into a construction error naming it, rather than a setting
// nobody notices was ignored (ADR-010).
//
// The path names [Config.TopicPrefix], so kafkago://host/acme. prefixes every
// topic with "acme." — the prefix is used verbatim, separator included,
// because Kafka has no topic separator of its own and inventing one would make
// two deployments' names collide differently from how the operator's own
// tooling reads them.
//
// The parameters, all optional. Three of the franz-go driver's are ABSENT
// rather than ignored, and asking for one is an error naming it:
// `disableidempotency` (there is no idempotent producer to disable),
// `transactionalid` (there is no transactional producer) and
// `instanceid` (kafka-go has no static group membership).
//
//	clientid=<string>            the client identifier the broker logs
//	sasl=plain|scram-sha-256|scram-sha-512
//	tls=true|false               dial with TLS using the platform roots
//	insecureskipverify=true|false   with tls=true, do not verify the server
//	balancer=range|roundrobin    the partition assignor offered to the group
//	autocreatetopics=true|false  let a produce create a topic
//	readcommitted=true|false     skip aborted and open transactions
//	batchmaxrecords=<n>          records the writer batches before producing
//	batchtimeout=<duration>      how long a partial batch waits
//	batchmaxbytes=<n>            bound one produced batch
//	maxattempts=<n>              tries per batch, and duplicates per publish
//	writetimeout=<duration>      bound on one produce round trip
//	metadatattl=<duration>       how long a cached cluster layout is served
//	writebackoffmin=<duration>   smallest delay between produce attempts
//	writebackoffmax=<duration>   largest delay between produce attempts
//	fetchminbytes=<n>            how few bytes a fetch returns with
//	fetchmaxbytes=<n>            bound one fetch response
//	fetchmaxwait=<duration>      how long a fetch waits for records
//	queuecapacity=<n>            bound one partition reader's buffer
//	sessiontimeout=<duration>    group session timeout
//	rebalancetimeout=<duration>  group rebalance timeout
//	heartbeatinterval=<duration> group heartbeat interval
//	joinbackoff=<duration>       wait before rejoining after a failure
//	watchpartitionchanges=true|false  rebalance when the topic grows
//	partitionwatchinterval=<duration> how often that is checked
//	commitinterval=<duration>    how often the watermark is committed
//	revokewait=<duration>        bound on the wait when a generation ends
//	requesttimeout=<duration>    bound on an administrative round trip
//	partitions=<n>               partitions the typed Admin creates with
//	replicationfactor=<n>        replication factor the typed Admin uses
//	tracker=<kind> the offset tracker kind
//	nack=<kind> the nack strategy kind
//	deadlettertopic=<topic>      where a rejected record is routed
//	nackmaxattempts=<n>          deliveries before the failure route
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
		useTLS, insecure bool
		batchMax         int
		nackAttempts     int
	)
	for _, step := range [...]func() error{
		func() error { return p.String("clientid", &cfg.ClientID) },
		func() error { return enumParam(p, "sasl", SASLNames(), func(i int) { cfg.SASL = SASLMechanism(i) }) },
		func() error {
			return enumParam(p, "balancer", GroupBalancerNames(), func(i int) { cfg.Balancer = GroupBalancer(i) })
		},
		func() error { return p.Bool("tls", &useTLS) },
		func() error { return p.Bool("insecureskipverify", &insecure) },
		func() error { return p.Bool("autocreatetopics", &cfg.AutoCreateTopics) },
		func() error { return p.Bool("readcommitted", &cfg.ReadCommitted) },
		func() error { return p.Bool("watchpartitionchanges", &cfg.WatchPartitionChanges) },
		func() error { return p.Int("batchmaxrecords", &cfg.BatchMaxRecords) },
		func() error { return p.Duration("batchtimeout", &cfg.BatchTimeout) },
		func() error { return p.Int("batchmaxbytes", &batchMax) },
		func() error { return p.Int("maxattempts", &cfg.MaxAttempts) },
		func() error { return p.Duration("writetimeout", &cfg.WriteTimeout) },
		func() error { return p.Duration("metadatattl", &cfg.MetadataTTL) },
		func() error { return p.Duration("writebackoffmin", &cfg.WriteBackoffMin) },
		func() error { return p.Duration("writebackoffmax", &cfg.WriteBackoffMax) },
		func() error { return p.Int("fetchminbytes", &cfg.FetchMinBytes) },
		func() error { return p.Int("fetchmaxbytes", &cfg.FetchMaxBytes) },
		func() error { return p.Duration("fetchmaxwait", &cfg.FetchMaxWait) },
		func() error { return p.Int("queuecapacity", &cfg.QueueCapacity) },
		func() error { return p.Duration("sessiontimeout", &cfg.SessionTimeout) },
		func() error { return p.Duration("rebalancetimeout", &cfg.RebalanceTimeout) },
		func() error { return p.Duration("heartbeatinterval", &cfg.HeartbeatInterval) },
		func() error { return p.Duration("joinbackoff", &cfg.JoinBackoff) },
		func() error { return p.Duration("partitionwatchinterval", &cfg.PartitionWatchInterval) },
		func() error { return p.Duration("commitinterval", &cfg.CommitInterval) },
		func() error { return p.Duration("revokewait", &cfg.RevokeWait) },
		func() error { return p.Duration("requesttimeout", &cfg.RequestTimeout) },
		func() error { return intoInt32(p, "partitions", &cfg.Partitions) },
		func() error { return intoInt16(p, "replicationfactor", &cfg.ReplicationFactor) },
		func() error { return kindParam(p, "tracker", offset.ParseKind, &cfg.Tracker) },
		func() error { return kindParam(p, "nack", nack.ParseKind, &cfg.Nack) },
		func() error { return p.String("deadlettertopic", &cfg.NackConfig.DeadLetterTopic) },
		func() error { return p.Int("nackmaxattempts", &nackAttempts) },
		func() error { return p.Int("retries", &cfg.NackConfig.Retries) },
		func() error { return p.Duration("nackdelay", &cfg.NackConfig.Delay) },
	} {
		if err := step(); err != nil {
			return Config{}, err
		}
	}
	if err := p.Strict(); err != nil {
		return Config{}, err
	}
	if batchMax < 0 || nackAttempts < 0 || nackAttempts > 1<<32-1 {
		return Config{}, errNegativeBound
	}
	cfg.BatchMaxBytes = int64(batchMax)
	// Bounded above, so the conversion cannot wrap.
	cfg.NackConfig.MaxAttempts = uint32(nackAttempts)
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

// enumParam reads a parameter whose values are the names of an enumeration.
//
// It exists because [github.com/iqhive/iqmq/dsn.Params.Enum] accepts index
// zero, and this driver has two enumerations whose zero name is meaningful —
// "none" for SASL and "range" for the assignor — so a DSN that said
// `sasl=none` is indistinguishable from one that said nothing. Naming the
// default explicitly is accepted here, because it means the same thing; what
// the wrapper adds is a place to record that the gap in the dsn package is
// still open.
func enumParam(p *dsn.Params, key string, names []string, set func(int)) error {
	return p.Enum(key, names, set)
}

// kindParam reads a variant-family kind by its slug.
func kindParam[K any](p *dsn.Params, key string, parse func(string) (K, bool), dst *K) error {
	var slug string
	if err := p.String(key, &slug); err != nil {
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
func intoInt32(p *dsn.Params, key string, dst *int32) error {
	var v int
	if err := p.Int(key, &v); err != nil {
		return err
	}
	if v == 0 {
		return nil
	}
	if v < -1 || v > 1<<31-1 {
		return errBadTopology
	}
	*dst = int32(v)
	return nil
}

// intoInt16 reads an integer parameter into an int16 field.
func intoInt16(p *dsn.Params, key string, dst *int16) error {
	var v int
	if err := p.Int(key, &v); err != nil {
		return err
	}
	if v == 0 {
		return nil
	}
	if v < -1 || v > 1<<15-1 {
		return errBadTopology
	}
	*dst = int16(v)
	return nil
}

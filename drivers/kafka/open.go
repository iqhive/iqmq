package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
)

// Open dials Kafka and returns a client.
//
// It performs two pieces of I/O and no more: the metadata round trip that
// proves the seed brokers answer, and the DescribeConfigs that reads the
// cluster's own record-size bound for the capability table. It creates NO
// topology — a topic is created by [Backend.Admin] or, when
// [Config.AutoCreateTopics] says so, by the cluster itself — because the specification
// forbids a driver provisioning behind a caller's back.
//
// The configuration is validated before the dial, so a typo is an immediate
// refusal rather than a connection timeout.
func Open(ctx context.Context, cfg Config) (*mq.Client, error) {
	b, err := OpenBackend(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// OpenWithClient wraps a *kgo.Client the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a client the application still uses, and one that never
// closed would leak the client it dialled itself.
//
// It is also the way in for everything a typed [Config] cannot carry — an
// OAUTHBEARER token source, GSSAPI, a custom dialler, a hook into franz-go's
// own metrics, a compression codec. The one thing it must NOT be is a group
// consumer: this client is the producer, and every source opens a client of
// its own because a franz-go client consumes at most one group.
func OpenWithClient(ctx context.Context, cl *kgo.Client, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithClient(ctx, cl, own, cfg)
	if err != nil {
		return nil, err
	}
	return clientFor(b, cfg), nil
}

// clientFor builds the client around a backend, carrying the configuration's
// clock and hooks.
func clientFor(b *Backend, cfg Config) *mq.Client {
	opts := make([]mq.ClientOption, 0, 2)
	if cfg.Clock != nil {
		opts = append(opts, mq.WithClock(cfg.Clock))
	}
	if hooksSet(cfg.Hooks) {
		opts = append(opts, mq.WithHooks(cfg.Hooks))
	}
	return mq.NewClient(b, opts...)
}

// hooksSet reports whether any observation point is wired. mq.Hooks holds
// function fields, so it is not comparable and the question has to be asked
// field by field.
func hooksSet(h mq.Hooks) bool {
	return h.OnPublishStart != nil || h.OnPublishDone != nil || h.OnReceive != nil ||
		h.OnSettle != nil || h.OnReconnect != nil
}

// OpenBackend dials Kafka and returns the [github.com/iqhive/iqmq.Backend]
// itself, for a caller composing the SPI directly.
func OpenBackend(ctx context.Context, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	b := &Backend{cfg: resolved, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	b.health.Connecting()
	opts, err := b.producerOptions(resolved)
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, b.errs.Map(mq.OpOpen, "", "", err)
	}
	// The dial itself. kgo.NewClient does no I/O, so without this a broken
	// address would be discovered by the first publication rather than by
	// Open, and the specification requires a configuration refusal to happen at
	// construction.
	pctx, cancel := b.bound(ctx)
	err = cl.Ping(pctx)
	cancel()
	if err != nil {
		cl.Close()
		b.health.Degraded(err)
		b.health.Close()
		return nil, b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	if err := b.init(ctx, cl, driverkit.ClientOwned); err != nil {
		cl.Close()
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// BackendWithClient is [OpenWithClient] without the client wrapper.
func BackendWithClient(ctx context.Context, cl *kgo.Client, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cl == nil {
		return nil, errNilClient
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	b := &Backend{cfg: cfg.resolve(), errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	if err := b.init(ctx, cl, own); err != nil {
		return nil, err
	}
	return b, nil
}

// producerOptions turns the configuration into the producer client's options.
//
// The two that are not negotiable are the point of the row: acks=all and an
// idempotent producer, which together make a record's promise a durable
// confirmation. They are franz-go's own defaults, and they are written out
// here anyway — a default that matters is one a reader should be able to see,
// and [Config.DisableIdempotency] is the only thing that moves them.
func (b *Backend) producerOptions(cfg Config) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(Partitioner()),
		kgo.ProducerLinger(cfg.linger()),
		kgo.WithHooks(&clientHooks{b: b}),
	}
	if cfg.DisableIdempotency {
		// acks=all without idempotence still means every in-sync replica
		// has the record, so the confirmation boundary does not move: what
		// is lost is the broker's de-duplication of the producer's own
		// retries, which the capability table stops claiming.
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	if cfg.BatchMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes))
	}
	if cfg.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(cfg.MaxBufferedRecords))
	}
	if cfg.TransactionalID != "" {
		// The ledger is how x/kafkatx learns that a record of its
		// transaction failed, which franz-go would commit without.
		opts = append(opts, kgo.TransactionalID(cfg.TransactionalID), kgo.WithHooks(&recordLedger{}))
	}
	if cfg.AutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	if cfg.RequestTimeout > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(cfg.RequestTimeout))
	}
	if cfg.DeliveryTimeout > 0 {
		opts = append(opts, kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout))
	}
	if !cfg.UnboundedDelivery && cfg.TransactionalID == "" {
		// the specification's "all operations bounded", and the reason has an
		// ambiguous outcome. Without this, franz-go will not let the
		// delivery timeout or the caller's own cancellation end a record
		// whose fate it does not know, so a Publish against a cluster
		// answering REQUEST_TIMED_OUT never returns at all. With it, the
		// record is failed and this driver reports the outcome as
		// unknown, which is what it is.
		opts = append(opts, kgo.AllowIdempotentProduceCancellation())
	}
	common, err := commonOptions(cfg)
	if err != nil {
		return nil, err
	}
	return append(opts, common...), nil
}

// commonOptions are the transport and authentication options every client of
// this backend shares — the producer and every source's group client.
func commonOptions(cfg Config) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	if cfg.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLS))
	}
	switch cfg.SASL {
	case SASLNone:
	case SASLPlain:
		opts = append(opts, kgo.SASL(plain.Auth{
			User: cfg.Username, Pass: cfg.Password.Reveal(),
		}.AsMechanism()))
	case SASLScramSHA256:
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.Username, Pass: cfg.Password.Reveal(),
		}.AsSha256Mechanism()))
	case SASLScramSHA512:
		opts = append(opts, kgo.SASL(scram.Auth{
			User: cfg.Username, Pass: cfg.Password.Reveal(),
		}.AsSha512Mechanism()))
	default:
		return nil, errBadSASL
	}
	return opts, nil
}

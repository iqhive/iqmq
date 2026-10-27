package kafkago

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"time"

	kgo "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

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

// OpenWithWriter wraps a [kgo.Writer] the caller already has.
//
// own says who closes it, and there is no default: a driver that always closed
// would tear down a writer the application still uses, and one that never
// closed would leak the writer it built itself.
//
// It is also the way in for everything a typed [Config] cannot carry — an
// OAUTHBEARER mechanism, GSSAPI, a custom dialler, a compression codec, a
// logger. What it cannot give the CONSUMER side is any of those: kafka-go's
// consumer group takes a [kgo.Dialer] rather than a transport, and a writer
// carries no dialler, so a source opened on a borrowed writer still dials with
// the typed configuration's TLS and SASL. A deployment whose consumer needs
// something else configures it in [Config] as well.
//
// Two fields of the borrowed writer are OVERWRITTEN, because the driver's
// contract depends on them: Completion, which is how a receipt and an
// asynchronous outcome get back to the caller, and Balancer, which is how a
// key reaches Kafka's own partition. A borrowed writer whose Async is true is
// used for both the synchronous and the asynchronous paths, and a synchronous
// Publish on it then reports admission rather than the broker's reply — so
// Async false is what a caller should hand over, and the driver does not
// change it either way because changing it silently would make a caller's own
// publications behave differently from what it configured.
func OpenWithWriter(ctx context.Context, w *kgo.Writer, own driverkit.Ownership, cfg Config) (*mq.Client, error) {
	b, err := BackendWithWriter(ctx, w, own, cfg)
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
	mech, err := mechanism(resolved)
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	b.links = newLinks()
	b.tr = transportFor(resolved, mech, b.links)
	b.dialer = dialerFor(resolved, mech)
	b.cl = &kgo.Client{
		Addr:      kgo.TCP(resolved.Brokers...),
		Transport: b.tr,
		Timeout:   resolved.RequestTimeout,
	}
	b.w = b.writerFor(resolved, false)
	b.wa = b.writerFor(resolved, true)
	// The dial itself. Nothing above does any I/O — kafka-go builds a writer
	// and a transport without touching the network — so without this a broken
	// address would be discovered by the first publication rather than by
	// Open, and the specification requires a configuration refusal to happen at construction.
	pctx, cancel := b.bound(ctx)
	_, err = b.cl.Metadata(pctx, &kgo.MetadataRequest{})
	cancel()
	if err != nil {
		_ = b.closeClients()
		b.health.Degraded(err)
		b.health.Close()
		return nil, b.errs.MapNet(mq.OpOpen, "", "", err, false)
	}
	if err := b.init(ctx, driverkit.ClientOwned); err != nil {
		_ = b.closeClients()
		b.health.Close()
		return nil, err
	}
	return b, nil
}

// BackendWithWriter is [OpenWithWriter] without the client wrapper.
func BackendWithWriter(ctx context.Context, w *kgo.Writer, own driverkit.Ownership, cfg Config) (*Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if w == nil {
		return nil, errNilWriter
	}
	if err := driverkit.CheckOwnership(own); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	resolved := cfg.resolve()
	b := &Backend{cfg: resolved, errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	mech, err := mechanism(resolved)
	if err != nil {
		b.health.Degraded(err)
		b.health.Close()
		return nil, err
	}
	// The borrowed writer may carry a transport of its own, which is the
	// caller's and is not closed here. What the driver needs its own of is the
	// protocol client and the consumer dialler.
	b.dialer = dialerFor(resolved, mech)
	b.cl = &kgo.Client{
		Addr:      kgo.TCP(resolved.Brokers...),
		Transport: w.Transport,
		Timeout:   resolved.RequestTimeout,
	}
	w.Balancer = Partitioner(resolved.BatchMaxRecords)
	w.Completion = b.completion
	b.w, b.wa = w, w
	if err := b.init(ctx, own); err != nil {
		return nil, err
	}
	return b, nil
}

// writerFor builds one producer.
//
// Two of its fields are the row's own requirements and neither is kafka-go's
// default. RequiredAcks is RequireAll where kafka-go's zero value is
// RequireNone — fire and forget, which would make every confirmation mode a
// lie — and BatchSize is one where kafka-go's default is a hundred records
// behind a one-second timer, which makes a synchronous publisher produce one
// message a second (see [DefaultBatchMaxRecords]).
func (b *Backend) writerFor(cfg Config, async bool) *kgo.Writer {
	w := &kgo.Writer{
		Addr:                   kgo.TCP(cfg.Brokers...),
		Transport:              b.tr,
		Balancer:               Partitioner(cfg.BatchMaxRecords),
		RequiredAcks:           kgo.RequireAll,
		Async:                  async,
		BatchSize:              cfg.BatchMaxRecords,
		BatchTimeout:           cfg.BatchTimeout,
		MaxAttempts:            cfg.MaxAttempts,
		WriteTimeout:           cfg.WriteTimeout,
		ReadTimeout:            cfg.RequestTimeout,
		WriteBackoffMin:        cfg.WriteBackoffMin,
		WriteBackoffMax:        cfg.WriteBackoffMax,
		AllowAutoTopicCreation: cfg.AutoCreateTopics,
		Completion:             b.completion,
	}
	if cfg.BatchMaxBytes > 0 {
		w.BatchBytes = cfg.BatchMaxBytes
	}
	return w
}

// mechanism builds the SASL mechanism a resolved configuration names.
//
// A nil mechanism with a nil error is the answer for SASLNone and is not an
// invalid value: `Transport.SASL` and `Dialer.SASLMechanism` are nil for a
// cluster with no authentication, which is what a plaintext broker wants.
func mechanism(cfg Config) (sasl.Mechanism, error) {
	switch cfg.SASL {
	case SASLNone:
		return nil, nil //nolint:nilnil // no SASL is a nil mechanism and not a failure; see above
	case SASLPlain:
		return plain.Mechanism{Username: cfg.Username, Password: cfg.Password.Reveal()}, nil
	case SASLScramSHA256:
		return scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password.Reveal())
	case SASLScramSHA512:
		return scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password.Reveal())
	default:
		return nil, errBadSASL
	}
}

// transportFor is the transport the producer and the protocol client share.
// It dials through ls, which is what lets Close end the round trips the
// writers are still making ([links]).
func transportFor(cfg Config, mech sasl.Mechanism, ls *links) *kgo.Transport {
	return &kgo.Transport{
		Dial:        ls.dial,
		ClientID:    cfg.ClientID,
		DialTimeout: cfg.RequestTimeout,
		MetadataTTL: cfg.MetadataTTL,
		TLS:         tlsFor(cfg),
		SASL:        mech,
	}
}

// links are the connections the transport has open, and the means by which
// Close ends what kafka-go's writers are still sending.
//
// [kgo.Writer.Close] waits for every batch its partition writers hold, and a
// partition writer tries a failed batch again up to [Config.MaxAttempts]
// times, each attempt bounded by [Config.WriteTimeout] and each retry behind a
// time.Sleep of up to [Config.WriteBackoffMax] that nothing interrupts. Close
// waited out every one of them: three attempts of 10 seconds each by default.
//
// Cancelling the round trips is not enough and is not safe. A produce's
// context is the writer's own, so a caller has no way in; and a round trip
// whose context ends returns at once while kafka-go's connection goroutine,
// which took the request before the context ended, goes on encoding the
// records out of the caller's storage and the driver's pooled buffer. The
// writer's completion would then hand both back while they were still being
// read. Closing the CONNECTION instead fails the round trip from inside that
// goroutine, after the request is encoded, so nothing the writer holds is
// read after the batch completes. A dial under way when the links are cut
// ends with them, because no record is read before a connection exists, and
// a dial after the cut is refused. The writer retries none of these failures
// (it retries a Temporary error and a short list of transient network ones,
// and a closed connection is neither), so each batch ends at its next
// attempt, and the only wait left is a backoff sleep already under way.
type links struct {
	// life ends when the links are cut, and a dial under way ends with it.
	life context.Context
	end  context.CancelFunc

	mu   sync.Mutex
	open map[*link]struct{}
}

// newLinks returns links nothing has cut.
func newLinks() *links {
	life, end := context.WithCancel(context.Background())
	return &links{life: life, end: end, open: map[*link]struct{}{}}
}

// link is one connection the transport dialled, which forgets itself when
// kafka-go closes it.
type link struct {
	net.Conn
	ls *links
}

// Close closes the connection and drops it from the set a cut closes.
func (l *link) Close() error {
	l.ls.mu.Lock()
	delete(l.ls.open, l)
	l.ls.mu.Unlock()
	return l.Conn.Close()
}

// dialPerAddress is kafka-go's own default dialler's bound on one address,
// kept so that the transport dials as it did before it dialled through links.
// The transport's DialTimeout still bounds the connection as a whole.
const dialPerAddress = 3 * time.Second

// dial is the transport's dialler: kafka-go's default one, ended by a cut.
func (ls *links) dial(ctx context.Context, network, address string) (net.Conn, error) {
	if ls.life.Err() != nil {
		return nil, errLinksCut
	}
	dctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(ls.life, cancel) //nolint:contextcheck // the cut ends the dial as well as ctx, which still bounds it
	defer stop()
	d := net.Dialer{Timeout: dialPerAddress}
	conn, err := d.DialContext(dctx, network, address)
	if err != nil {
		if ls.life.Err() != nil {
			return nil, errLinksCut
		}
		return nil, err
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.life.Err() != nil {
		// The cut ran while this dial was under way, and did not see it.
		_ = conn.Close()
		return nil, errLinksCut
	}
	l := &link{Conn: conn, ls: ls}
	ls.open[l] = struct{}{}
	return l, nil
}

// cutAll ends every dial, closes every open connection and refuses every dial
// from now on. It is idempotent, and nil links, a borrowed writer's, are
// never cut.
func (ls *links) cutAll() {
	if ls == nil {
		return
	}
	ls.end()
	ls.mu.Lock()
	open := ls.open
	ls.open = map[*link]struct{}{}
	ls.mu.Unlock()
	for l := range open {
		_ = l.Conn.Close()
	}
}

// dialerFor is the dialler kafka-go's consumer group and partition readers
// take. It is a second object carrying the same TLS and SASL as the transport,
// because the consumer half of kafka-go predates the transport and has never
// been moved onto it.
func dialerFor(cfg Config, mech sasl.Mechanism) *kgo.Dialer {
	return &kgo.Dialer{
		ClientID:      cfg.ClientID,
		Timeout:       cfg.RequestTimeout,
		DualStack:     true,
		TLS:           tlsFor(cfg),
		SASLMechanism: mech,
	}
}

// tlsFor is the TLS configuration to dial with, or nil for plaintext.
func tlsFor(cfg Config) *tls.Config {
	if cfg.TLS == nil {
		return nil
	}
	return cfg.TLS
}

// groupBalancers is the assignor list offered to the coordinator.
//
// One entry, not a fallback list: the coordinator picks the first protocol
// every member supports, and a member that offered two would let a member
// configured differently decide which one the group uses. Naming one makes a
// misconfigured fleet fail to form a group — INCONSISTENT_GROUP_PROTOCOL —
// rather than silently assign partitions by a strategy nobody chose.
func groupBalancers(cfg Config) []kgo.GroupBalancer {
	switch cfg.Balancer {
	case BalancerRoundRobin:
		return []kgo.GroupBalancer{kgo.RoundRobinGroupBalancer{}}
	case BalancerRange:
		return []kgo.GroupBalancer{kgo.RangeGroupBalancer{}}
	default:
		return []kgo.GroupBalancer{kgo.RangeGroupBalancer{}}
	}
}

// startOffset maps the start position onto the offset kafka-go's group
// resolves a partition with no committed offset to.
//
// kafka-go offers exactly two — FirstOffset and LastOffset — so a start
// position by TIME or by SEQUENCE cannot be expressed here at all. That is
// what [Backend.ensureGroupPosition] is for: the resolved offset is COMMITTED
// for the group before the group is joined, so this value is only ever the
// fallback for a topic that did not exist yet.
func startOffset(start mq.StartPosition) int64 {
	switch start.Kind() {
	case mq.StartKindEarliest:
		return kgo.FirstOffset
	case mq.StartKindLatest, mq.StartKindUnset:
		return kgo.LastOffset
	case mq.StartKindTime, mq.StartKindSequence:
		// Neither can be expressed as a group start offset. The committed
		// position is what carries them, and the honest fallback for a topic
		// with no records at all is the end: a time or a sequence in a log
		// that does not exist resolves to "nothing yet".
		return kgo.LastOffset
	default:
		return kgo.LastOffset
	}
}

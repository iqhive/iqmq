package memory

import (
	"context"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/errcode"
)

// The broker is the driver's backend, its topology surface and its health
// reporter. Asserting it here is what makes a change to any of the three a
// compile failure rather than a capability that quietly stops being offered.
var (
	_ mq.Backend      = (*Broker)(nil)
	_ mq.AdminBackend = (*Broker)(nil)
	_ mq.Backend      = borrowed{}
	_ mq.AdminBackend = borrowed{}
)

// DriverName is the driver's registered name, and the scheme its DSN uses.
const DriverName = "memory"

// Version is the version of this driver. It is the module's, and it is empty
// until the module is tagged: the specification makes an empty string "the driver did not
// say" rather than a claim.
const Version = ""

// Info describes the driver.
//
// The client library is this repository's own engine, so ClientName names it
// and BrokerVersion is empty: there is no broker behind the driver to report
// a version for.
func (b *Broker) Info() mq.DriverInfo {
	return mq.DriverInfo{
		Name:          DriverName,
		DriverVersion: Version,
		ClientName:    "github.com/iqhive/iqmq/internal/engine",
	}
}

// Capabilities is what this broker can and cannot do. It is constant
// for the life of the broker, which is what lets the core compile it into
// booleans at construction (rule 4).
func (b *Broker) Capabilities() mq.Capabilities { return b.caps }

// Native returns the broker itself, which is the escape hatch for this
// driver: client.Native().(*memory.Broker).
func (b *Broker) Native() any { return b }

// Ping is the liveness check: no I/O, no topology, no consumption.
func (b *Broker) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.eng.Closed() {
		return mq.ErrClosed
	}
	return nil
}

// Health is the broker's current state. It performs no I/O.
//
// A memory broker runs one background goroutine — the scheduler — and nothing
// that can reconnect, so it is Ready until it is Closed and never Degraded.
func (b *Broker) Health() mq.HealthSnapshot {
	if b.eng.Closed() {
		return mq.HealthSnapshot{State: mq.HealthClosed}
	}
	return mq.HealthSnapshot{State: mq.HealthReady}
}

// capabilities builds the capability set once, at construction, from the
// broker's own configuration (the specification's "a driver builds this once, at open,
// from its profile").
//
// Every claim below is a claim about this engine, and the ones that are not
// native say what they are instead: an in-process guarantee that a restart
// destroys is EmulatedVolatile, which the specification forbids from satisfying a
// durability requirement, and a guarantee that needs a topology option is
// Conditional with the option named in its note.
func (b *Broker) capabilities() (mq.Capabilities, error) {
	var caps mq.Capabilities
	// Capabilities.Set fails only for a feature outside the inventory; the
	// first such failure is kept and returned once the table is built.
	var setErr error
	set := func(f mq.Feature, s mq.Support) {
		if setErr != nil {
			return
		}
		setErr = caps.Set(f, s)
	}
	native := func(f mq.Feature, scope mq.Scope) {
		set(f, mq.Support{Level: mq.Native, Scope: scope})
	}
	// Settlement, fan-out and groups are the engine's own.
	native(mq.FeatureAtLeastOnce, mq.ScopeTopic)
	native(mq.FeatureExplicitAck, mq.ScopeTopic)
	native(mq.FeatureNack, mq.ScopeTopic)
	native(mq.FeatureConsumerGroups, mq.ScopeGroup)
	native(mq.FeatureFanOut, mq.ScopeTopic)
	native(mq.FeatureExtendLease, mq.ScopeTopic)
	native(mq.FeatureBatchReceive, mq.ScopeTopic)
	native(mq.FeatureBatchPublish, mq.ScopeTopic)
	native(mq.FeatureDelayedRedelivery, mq.ScopeTopic)
	native(mq.FeatureScheduledPublish, mq.ScopeTopic)
	native(mq.FeatureMessageTTL, mq.ScopeTopic)
	native(mq.FeatureNativeHeaders, mq.ScopeTopic)
	native(mq.FeatureBinaryHeaders, mq.ScopeTopic)
	native(mq.FeatureDuplicateHeaders, mq.ScopeTopic)
	native(mq.FeatureAdmin, mq.ScopeTopic)
	native(mq.FeatureOrderedByKey, mq.ScopeKey)
	native(mq.FeatureOrderedByPartition, mq.ScopePartition)
	// A publication returns once the record is in every target lane, which
	// is the whole of the confirmation this broker can offer.
	native(mq.FeaturePublisherConfirm, mq.ScopeTopic)
	// Nothing here survives the process, so nothing durable is advertised —
	// the specification's "never advertises durable storage". A subscription outliving
	// its consumers is real but volatile, and ranks a volatile
	// emulation below conditional support for exactly this reason.
	set(mq.FeatureDurableSubscriptions, mq.Support{
		Level: mq.EmulatedVolatile, Scope: mq.ScopeGroup,
		Note: "a subscription outlives its consumers but not the process; nothing is persisted",
	})
	// FeatureExactlyOnceDelivery is NOT advertised, and the reason is worth
	// stating because this driver did advertise it until the conformance
	// kit grew a probe for the delivery half of the claim.
	//
	set(mq.FeatureOrderedGlobally, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeTopic,
		Note: "TopologyConfig.Partitions of 1, which is the default",
	})
	set(mq.FeaturePriority, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeTopic,
		Note: "TopologyConfig.Priority",
	})
	set(mq.FeatureDeduplication, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeTopic,
		Note: "TopologyConfig.DedupWindow",
	})
	set(mq.FeatureDeadLetter, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeTopic,
		Note: "TopologyConfig.DeadLetterTopic, which must already exist; an explicit Reject publishes to it and then acknowledges",
	})
	set(mq.FeatureReplay, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeTopic,
		Note: "TopologyKind of stream with a Retention bound",
	})
	set(mq.FeatureSeekByTime, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeGroup,
		Note: "at subscription open on a stream topic; a live source cannot be re-seeked",
	})
	set(mq.FeatureSeekBySequence, mq.Support{
		Level: mq.Conditional, Scope: mq.ScopeGroup,
		Note: "at subscription open on a stream topic; a live source cannot be re-seeked",
	})
	caps.SetLimits(b.limits())
	if setErr != nil {
		return mq.Capabilities{}, setErr
	}
	return caps, nil
}

// limits is the limit half. Only what the broker really knows is
// advertised: an unknown limit is never an overloaded zero, and nothing in
// iqmq enforces a limit whose Known is false.
func (b *Broker) limits() mq.Limits {
	var lim mq.Limits
	if b.topo.MaxMessageSize > 0 {
		// The default topology's limit, which is what every topic the
		// broker creates is given. A topic created with a different one
		// through the admin surface enforces its own, and the engine is
		// what enforces it: Capabilities is constant per backend and
		// cannot describe a topic.
		lim.MaxMessageBytes = mq.Limit[int64]{Value: int64(b.topo.MaxMessageSize), Known: true}
	}
	// MaxLease is deliberately NOT advertised, and the topology's
	// AckDeadline is not it.
	//
	// The specification defines MaxLease as the longest acknowledgement deadline the
	// broker GRANTS, and this broker has no longest: `Consumer.Extend` moves
	// the deadline to at least `now + d` for any d and the lane's scheduler
	// is a list move, so an extension of an hour is granted exactly as
	// readily as one of a second. AckDeadline is the lease a delivery
	// starts with -- a DEFAULT -- and Limits has no field for that.
	//
	// It was advertised as AckDeadline until the conformance kit grew a
	// probe for the difference, which caught this driver on its first run:
	// the table said ten seconds and `Extend(11s)` was granted, so a caller
	// reading the limit was told something the broker does not do.
	// `drivers/pulsar` had already reached the same conclusion for its own
	// emulated lease, and `TestMemoryAdvertisesNoLeaseCeiling` is what keeps
	// the two agreeing.
	return lim
}

// OpenSink opens a publication channel to one destination.
//
// ConfirmDurable is refused here, which is ADR-010's this broker has
// no durable boundary to define, and a publisher asking for one fails at
// construction rather than at its first message.
func (b *Broker) OpenSink(ctx context.Context, cfg mq.SinkConfig) (mq.Sink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.eng.Closed() {
		return nil, mq.ErrClosed
	}
	if cfg.Confirmation.Resolve() == mq.ConfirmDurable {
		return nil, mq.ErrUnsupported
	}
	mode, err := resolveMetadata(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	t, err := b.topicFor(cfg.Topic)
	if err != nil {
		return nil, err
	}
	s := &sink{
		b: b, topic: t, name: cfg.Topic, meta: mode, wildcards: t.Topology().Wildcards,
		stop: make(chan struct{}),
	}
	if err := b.track(nil, s); err != nil {
		return nil, err
	}
	return s, nil
}

// OpenSource opens a subscription.
//
// Every parameter the source needs is in cfg, which is why nothing below
// consults the broker's configuration again: the subscription is created with
// the deadline and start position cfg names, and the consumer with the
// prefetch it names.
func (b *Broker) OpenSource(ctx context.Context, cfg mq.SourceConfig) (mq.Source, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.eng.Closed() {
		return nil, mq.ErrClosed
	}
	if cfg.Prefetch <= 0 {
		// The core resolves the default before opening a source, so a
		// driver that sees zero here has been handed an unresolved config.
		return nil, errUnresolvedPrefetch
	}
	mode, err := resolveMetadata(cfg.Metadata)
	if err != nil {
		return nil, err
	}
	t, err := b.topicFor(cfg.Topic)
	if err != nil {
		return nil, err
	}
	start, err := startOf(cfg.Start, t.Topology())
	if err != nil {
		return nil, err
	}
	sub, err := t.Subscribe(cfg.Subscription, engine.SubscriptionConfig{
		AckDeadline: cfg.AckDeadline,
		Start:       start,
	})
	if err != nil {
		return nil, err
	}
	cons, err := sub.Attach(engine.ConsumerConfig{Prefetch: cfg.Prefetch})
	if err != nil {
		return nil, err
	}
	src := newSource(b, t, sub, cons, cfg, mode)
	if err := b.track(src, nil); err != nil {
		_ = cons.Close()
		return nil, err
	}
	return src, nil
}

// Admin returns the topology surface.
func (b *Broker) Admin() (mq.Admin, error) {
	if b.eng.Closed() {
		return nil, mq.ErrClosed
	}
	return &admin{b: b}, nil
}

// startOf maps the start position onto the engine's.
//
// A position other than Latest needs a retained log to read from, so asking
// for one on a topology that retains nothing is ErrUnsupported rather than a
// silent Latest: FeatureReplay says the support is conditional and this is
// the condition.
func startOf(pos mq.StartPosition, topo engine.TopologyConfig) (engine.StartPosition, error) {
	resolved := pos.Resolve()
	if resolved.Kind() == mq.StartKindLatest {
		return engine.StartPosition{Kind: engine.StartLatest}, nil
	}
	if topo.Kind != engine.TopologyStream {
		return engine.StartPosition{}, mq.ErrUnsupported
	}
	switch resolved.Kind() {
	case mq.StartKindEarliest:
		return engine.StartPosition{Kind: engine.StartEarliest}, nil
	case mq.StartKindTime:
		return engine.StartPosition{Kind: engine.StartAtTime, At: resolved.Time()}, nil
	case mq.StartKindSequence:
		return engine.StartPosition{Kind: engine.StartAtSeq, Seq: resolved.Sequence()}, nil
	case mq.StartKindUnset, mq.StartKindLatest:
		return engine.StartPosition{Kind: engine.StartLatest}, nil
	default:
		return engine.StartPosition{}, mq.ErrUnsupported
	}
}

// metadataMode is what the driver does with a message's metadata.
type metadataMode uint8

const (
	// metaNative carries every field natively, which this broker can do for
	// all of them: the engine stores the *mq.Message itself.
	metaNative metadataMode = iota
	// metaEnvelope wraps the message in the portable binary envelope of the
	// public wire package, which is what MetadataPortableEnvelope asks for
	// explicitly.
	metaEnvelope
)

// resolveMetadata maps the four modes onto what this driver does.
//
// Three of them come out the same: the engine has a native field for every
// piece of metadata a message carries — binary values, duplicate keys,
// ordering, the typed properties and the ID — so nothing is ever unsupported,
// MetadataAdaptive never needs the envelope and MetadataLenientDrop never
// drops anything. The fourth is explicit and is honoured as written: it
// always wraps, because the whole point is that the envelope is never
// applied by accident and never skipped when it was asked for.
func resolveMetadata(m mq.MetadataMode) (metadataMode, error) {
	switch m {
	case mq.MetadataNativeStrict, mq.MetadataAdaptive, mq.MetadataLenientDrop:
		return metaNative, nil
	case mq.MetadataPortableEnvelope:
		return metaEnvelope, nil
	default:
		return metaNative, errMetadataMode
	}
}

// The SPI protocol failures.
var (
	errUnresolvedPrefetch = errcode.New(errcode.InvalidConfig, "iqmq/memory: SourceConfig.Prefetch is not positive; the core resolves it before opening a source")
	errMetadataMode       = errcode.New(errcode.InvalidConfig, "iqmq/memory: SinkConfig.Metadata is not a metadata mode")
)

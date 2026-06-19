// Package otel instruments iqmq with OpenTelemetry: the API's
// four spans, its histograms and gauges, and W3C trace-context propagation
// through a message's headers.
//
// # Pinned semantic conventions
//
// The attribute names come from one pinned convention version,
// [SemconvVersion], and nothing here derives an attribute name from the OTel
// module's own release. The messaging conventions have renamed attributes
// between releases, and an attribute that silently renames breaks every
// dashboard built on it with nothing at compile time to notice. ADR-024
// records the version and what moving it costs.
//
// # Where each span comes from
//
//	publish   mq.PublishMiddleware   parented to the caller's span; injects
//	                                 the traceparent into the message
//	process   mq.Middleware          parented to the PUBLISHER's span, read
//	                                 back out of the delivery's headers
//	receive   mq.Hooks.OnReceive     a root span over one fetch
//	settle    mq.Hooks.OnSettle      a root span over one settlement
//
// The first two are proper children and carry the trace across the broker.
// The last two cannot be: mq.Hooks carries no context.Context, so there is
// nothing to parent them to and nothing to put them in — they are recorded
// with explicit start and end timestamps derived from the event's own
// duration, and a reader sees them beside the trace rather than inside it.
// That is a gap in the hook surface rather than a choice, it is recorded in
// the maintenance notes, and [Config.Spans] turns either of them off for a
// program that would rather have no span than an unparented one.
//
// # What it costs
//
// A [Config] with no TracerProvider and no MeterProvider produces an
// instrumentation whose Hooks() is the ZERO mq.Hooks and whose middlewares
// return the handler and the publish function unchanged — so a client built
// with it runs the specification's "disabled hooks/observer path: 0 allocs, ≤ 1
// predictable branch" exactly as an uninstrumented one does, and there is no
// branch per message because there is no code per message.
//
// With it enabled, a span costs what the OTel SDK charges for a span; what
// this package adds is bounded and stated by TestAllocs. The attribute set,
// the span name and the start option of every (operation, topic,
// subscription) combination are built once and reused, so no span start
// formats a name or allocates an attribute slice.
//
// # Cardinality
//
// Topic and subscription are attribute values, and a wildcard subscription
// resolves to whatever concrete topic the broker delivered.
// [Cardinality] bounds how many combinations are cached and folds the rest
// onto one overflow set. A message identifier is never an attribute.
package otel

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	mq "bitbucket.org/iqhive/iqmq"
)

// name is what this package calls itself in an error and as the
// instrumentation scope.
const name = "bitbucket.org/iqhive/iqmq/integrations/otel"

// DefaultMaxSeries bounds the cached attribute combinations when
// [Cardinality.MaxSeries] is zero.
const DefaultMaxSeries = 128

// DefaultOverflow is the attribute value a combination past the limit is
// folded into.
const DefaultOverflow = "other"

// ErrNoSystem (CodeInvalidConfig) reports a Config with tracing or metrics
// enabled and no System.
//
// `messaging.system` is required by the convention and it is the attribute
// every messaging dashboard groups by, so guessing it — or leaving it out —
// produces spans that look right and cannot be queried. It is the driver's
// name for the broker family: "kafka", "rabbitmq", "aws_sqs", "gcp_pubsub".
var ErrNoSystem error = &mq.OpError{
	Driver: name, Operation: "New", Code: mq.CodeInvalidConfig,
	Err: errors.New("Config.System is empty and messaging.system is required"),
}

// Spans selects which of the four spans are created. The zero value
// creates all four.
type Spans struct {
	// NoPublish suppresses the publish span. The publish MIDDLEWARE still
	// injects the trace context, because a consumer's process span needs a
	// parent whether or not the producer recorded one of its own.
	NoPublish bool
	// NoReceive suppresses the unparented receive span.
	NoReceive bool
	// NoProcess suppresses the process span.
	NoProcess bool
	// NoSettle suppresses the unparented settle span, which is the one there
	// is most of: one per message rather than one per fetch.
	NoSettle bool
}

// Cardinality bounds the attribute combinations this package caches (the specification's
// "cardinality controls").
type Cardinality struct {
	// MaxSeries is how many (operation, topic, subscription) combinations
	// are cached. Zero means DefaultMaxSeries and a negative value means
	// unbounded, following the repository's convention that zero is "unset"
	// and only an explicit negative removes a bound.
	MaxSeries int
	// Overflow is the destination name a combination past MaxSeries is
	// folded onto. Empty means DefaultOverflow.
	Overflow string
	// OmitTopic reports the destination as Overflow for every topic. It is
	// the control for a deployment whose topic names are per-tenant.
	OmitTopic bool
	// OmitSubscription does the same for the consumer group name.
	OmitSubscription bool
}

func (c Cardinality) resolve() Cardinality {
	if c.MaxSeries == 0 {
		c.MaxSeries = DefaultMaxSeries
	}
	if c.Overflow == "" {
		c.Overflow = DefaultOverflow
	}
	return c
}

// Config configures [New].
type Config struct {
	// TracerProvider is where spans go. Nil disables every span, and the
	// middlewares then return what they were given unchanged.
	TracerProvider trace.TracerProvider
	// MeterProvider is where the instruments go. Nil disables every metric.
	MeterProvider metric.MeterProvider
	// System is `messaging.system`: the broker family, as the driver names
	// it. Required.
	System string
	// Propagator replaces this package's W3C trace-context propagation with
	// an OpenTelemetry propagator of the caller's choosing, which is how
	// baggage is carried (the specification names it). Nil uses [Inject] and [Extract],
	// which render and parse through the root module's own W3C
	// implementation.
	Propagator propagation.TextMapPropagator
	// Spans selects which spans are created.
	Spans Spans
	// Clock is the clock the histograms and the unparented spans are
	// measured on. Nil takes the wall clock. It is here so that a test under
	// testing/synctest measures what the fake clock says, the same way every
	// middleware that measures takes an mq.Clock.
	Clock mq.Clock
	// Cardinality bounds the cached attribute combinations.
	Cardinality Cardinality
}

// Instrumentation is the tracer, the instruments and the adapters that feed
// them.
//
// It is safe for concurrent use. The zero value is not usable; [New] returns
// a ready one.
type Instrumentation struct {
	cfg        Config
	card       Cardinality
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator

	tracing bool
	metrics bool

	// sets caches the per-(operation, topic, subscription) span name,
	// attribute set and start option. A span start is then an atomic load
	// and a map lookup, and formats nothing.
	sets cache[setKey, *attrSet]

	inst instruments
}

// New builds the instrumentation.
//
// Both providers are optional and a Config with neither is the disabled
// path: Hooks() is the zero mq.Hooks, and the middlewares are the identity.
// System is required as soon as either is set.
func New(cfg Config) (*Instrumentation, error) {
	i := &Instrumentation{
		cfg:        cfg,
		card:       cfg.Cardinality.resolve(),
		propagator: cfg.Propagator,
		tracing:    cfg.TracerProvider != nil,
		metrics:    cfg.MeterProvider != nil,
	}
	if !i.tracing && !i.metrics {
		return i, nil
	}
	if cfg.System == "" {
		return nil, ErrNoSystem
	}
	// The system name reaches every span and every measurement, so it is
	// made safe once here rather than on each of them.
	i.cfg.System = attributeValue(cfg.System)
	if i.tracing {
		i.tracer = cfg.TracerProvider.Tracer(name, trace.WithSchemaURL(SchemaURL))
	}
	if i.metrics {
		meter := cfg.MeterProvider.Meter(name, metric.WithSchemaURL(SchemaURL))
		if err := i.initMetrics(meter); err != nil {
			return nil, err
		}
	}
	i.sets.limit = i.card.MaxSeries
	return i, nil
}

// Enabled reports whether anything is instrumented at all. It is what a
// caller checks before building the handles it would only register for
// observation.
func (i *Instrumentation) Enabled() bool { return i.tracing || i.metrics }

// setKey identifies one cached attribute combination. variant distinguishes
// the three settlements, which share an operation TYPE and have three
// operation names; it is zero for every other operation.
type setKey struct {
	op         opKind
	variant    uint8
	topic, sub string
}

// attrSet is everything a span start and a metric record need for one
// combination, built once.
type attrSet struct {
	name  string
	attrs []attribute.KeyValue
	// kindOpt and attrOpt are the cached span-start options, and start is
	// the two of them as the slice a variadic call needs. Caching the
	// OPTIONS and not just the attribute slice is what keeps tracer.Start
	// from allocating at the call site: trace.WithAttributes returns an
	// interface value, and boxing it per span would be one allocation a
	// message. The two are also held separately because the unparented
	// spans pass a timestamp alongside them, and appending to a shared
	// slice is not something two goroutines may do.
	kindOpt trace.SpanStartOption
	attrOpt trace.SpanStartOption
	start   []trace.SpanStartOption
	// record and observe are the cached metric options, for the same
	// reason: one metric.MeasurementOption wrapped in each of the two
	// option slices the metric API takes, built once rather than boxed per
	// measurement.
	record  []metric.RecordOption
	observe []metric.ObserveOption
	// set is the attribute set both of them carry.
	set attribute.Set
}

// destination is the topic as it may be reported, under the cardinality
// controls.
func (i *Instrumentation) destination(topic string) string {
	if i.card.OmitTopic {
		return i.card.Overflow
	}
	return attributeValue(topic)
}

// group is the subscription as it may be reported.
func (i *Instrumentation) group(sub string) string {
	if i.card.OmitSubscription {
		return i.card.Overflow
	}
	return attributeValue(sub)
}

// attributeValue makes a broker-supplied string safe to use as a span name
// and an attribute value.
//
// The specification says a delivery's topic is the CONCRETE one the broker reported, not
// the pattern a subscription named, and the specification says broker-delivered metadata is
// untrusted — so a wildcard subscription, a NATS subject or a queue somebody
// else created can put arbitrary bytes into a destination name. An OTLP
// exporter encodes a span name and an attribute value into protobuf `string`
// fields, which proto3 requires to be valid UTF-8, so invalid bytes here
// become an export that fails for the whole batch: every span in it lost, not
// just the one with the odd topic. Invalid bytes therefore become U+FFFD and
// the name keeps the rest of itself.
//
// The scan is allocation-free for the ordinary case and happens once per
// cached combination on the span path, because the value it returns IS the
// cache key.
func attributeValue(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}

// setFor resolves one combination's cached state.
func (i *Instrumentation) setFor(op opKind, topic, sub string) *attrSet {
	return i.setForVariant(op, 0, topic, sub)
}

// setForVariant is setFor for an operation that has variants, which is the
// settlement.
func (i *Instrumentation) setForVariant(op opKind, variant uint8, topic, sub string) *attrSet {
	k := setKey{op: op, variant: variant, topic: i.destination(topic), sub: i.group(sub)}
	return i.sets.get(k, i.buildSet, i.fold)
}

// fold is the key a combination past the cardinality limit is reported under:
// the same OPERATION on the overflow destination.
//
// The operation is kept deliberately. Folding every overflow onto one set
// would put a publication into a span called "process other", which is a
// wrong answer rather than a coarse one. The folded keys are bounded by the
// operations themselves -- four, plus two more for the other two
// settlements -- so the fold adds a fixed handful of sets and not a series
// per topic.
func (i *Instrumentation) fold(k setKey) setKey {
	k.topic = i.card.Overflow
	if k.sub != "" {
		k.sub = i.card.Overflow
	}
	return k
}

func (i *Instrumentation) buildSet(k setKey) *attrSet {
	s := &attrSet{
		name:  spanName(k.op, k.variant, k.topic),
		attrs: attrsFor(k.op, k.variant, i.cfg.System, k.topic, k.sub),
	}
	s.set = attribute.NewSet(s.attrs...)
	s.kindOpt = trace.WithSpanKind(opSpanKinds[k.op])
	s.attrOpt = trace.WithAttributes(s.attrs...)
	s.start = []trace.SpanStartOption{s.kindOpt, s.attrOpt}
	mo := metric.WithAttributeSet(s.set)
	s.record = []metric.RecordOption{mo}
	s.observe = []metric.ObserveOption{mo}
	return s
}

// cache is a copy-on-write map from an attribute key to its built state.
//
// A read is an atomic load and a map lookup and allocates nothing; the mutex
// is taken only to admit a key the map does not hold, at most MaxSeries times
// plus once for the overflow key. Nothing is called while it is held.
type cache[K comparable, V any] struct {
	mu    sync.Mutex
	m     atomic.Pointer[map[K]V]
	limit int
}

func (c *cache[K, V]) get(k K, build func(K) V, fold func(K) K) V {
	if m := c.m.Load(); m != nil {
		if v, ok := (*m)[k]; ok {
			return v
		}
	}
	return c.admit(k, build, fold)
}

func (c *cache[K, V]) admit(k K, build func(K) V, fold func(K) K) V {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.locked(k, build, fold, true)
}

// locked admits a key with the mutex held. mayFold is false on the recursive
// call for the folded key, which is admitted whether or not the limit is
// reached: the folded keys are a bounded set, and refusing them would leave
// an event with nowhere to go.
func (c *cache[K, V]) locked(k K, build func(K) V, fold func(K) K, mayFold bool) V {
	old := c.m.Load()
	if old != nil {
		if v, ok := (*old)[k]; ok {
			return v
		}
	}
	n := 0
	if old != nil {
		n = len(*old)
	}
	if mayFold && c.limit >= 0 && n >= c.limit {
		if fk := fold(k); fk != k {
			return c.locked(fk, build, fold, false)
		}
	}
	next := make(map[K]V, n+1)
	if old != nil {
		for kk, vv := range *old {
			next[kk] = vv
		}
	}
	v := build(k)
	next[k] = v
	c.m.Store(&next)
	return v
}

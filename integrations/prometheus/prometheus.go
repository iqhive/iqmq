// Package prometheus exposes iqmq's observation points as Prometheus metrics
// under the API's canonical names.
//
// # A metric name is an API
//
// The specification fixes the names — `iqmq_publish_messages_total{outcome}`,
// `iqmq_settle_duration_seconds{kind}` and the rest — so this package offers
// no namespace or subsystem knob: a dashboard, an alert rule and a recording
// rule are all built on the name, and renaming one is a breaking change no
// compiler catches. [TestMetricNamesGolden] compares the whole registered set
// against testdata/metrics.golden, so a rename fails a test rather than a
// pager.
//
// # Every metric here has a source that can make it non-zero
//
// A metric that is always zero reads as evidence: "no publish failures" and
// "nobody wired up publish failures" look identical on a graph. So the golden
// test drives every metric from a real client and asserts the registered set
// is exactly the golden set, which makes an unsourced metric impossible to
// add. One of the names -- iqmq_option_ignored_total -- has no source in
// this repository at all and is therefore NOT registered, and four more are
// registered with a qualification a reader needs. [Unsourced] names every one
// of them and says why, and the metrics notes repeats it.
//
// # What it costs
//
// Nothing at all when it is not installed: a [Metrics] is only reached
// through the [mq.Hooks] you install, and a client built without them runs
// the specification's "disabled hooks/observer path: 0 allocs, ≤ 1 predictable branch" —
// the core builds no event and reads no clock. With the hooks installed every
// event path allocates zero: the per-series children are built once, on the
// first event for a label combination, and found afterwards through one
// atomic load and one map lookup. TestAllocs pins both halves.
//
// # Cardinality
//
// Topic and subscription are label values and both come from the program's
// own configuration — except that a wildcard subscription resolves to
// whatever concrete topic the broker delivered, which the program does not
// choose. [Cardinality] bounds how many combinations each family may have and
// folds the rest into one overflow series, and a message identifier is never
// a label.
package prometheus

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	prom "github.com/prometheus/client_golang/prometheus"

	mq "github.com/iqhive/iqmq"
)

// name is what this package calls itself in an error.
const name = "iqmq/integrations/prometheus"

// Namespace prefixes every metric name the specification fixes.
const Namespace = "iqmq"

// DefaultMaxSeries bounds each metric family's label combinations when
// [Cardinality.MaxSeries] is zero.
const DefaultMaxSeries = 128

// DefaultOverflow is the label value a combination past the limit is folded
// into.
const DefaultOverflow = "other"

// Label names. They are part of the exposed API for the same reason the
// metric names are, and the golden test pins them too.
const (
	labelTopic        = "topic"
	labelSubscription = "subscription"
	labelOutcome      = "outcome"
	labelKind         = "kind"
	labelPolicy       = "policy"
	labelDriver       = "driver"
)

// Outcome label values. They are the specification's own, shared with
// middleware.OutcomeSuccess and middleware.OutcomeFailure so that
// iqmq_handler_duration_seconds and iqmq_publish_messages_total agree.
const (
	outcomeSuccess = "success"
	outcomeFailure = "failure"
	outcomePanic   = "panic"
)

// Construction failures. Each carries a portable code, so errors.Is against
// the root package's sentinels holds.
var (
	// ErrNoRegisterer (CodeInvalidConfig) reports a Config with no
	// Registerer. It is an error rather than a default to
	// prometheus.DefaultRegisterer, because the specification's "no global logger" rule
	// is a rule about globals and not about logging: a program that
	// collects metrics nobody exposes should fail at construction, not at
	// the first scrape that finds nothing.
	ErrNoRegisterer error = &mq.OpError{
		Driver: name, Operation: "New", Code: mq.CodeInvalidConfig,
		Err: errors.New("Config.Registerer is nil"),
	}
	// ErrBadBuckets (CodeInvalidConfig) reports a bucket list that is empty
	// or not strictly increasing.
	ErrBadBuckets error = &mq.OpError{
		Driver: name, Operation: "New", Code: mq.CodeInvalidConfig,
		Err: errors.New("histogram buckets must be non-empty and strictly increasing"),
	}
)

// DefaultDurationBuckets are the publish, settle and handler latency buckets:
// 100 µs to 10 s, a little over two per decade. A messaging client's
// interesting range spans a local memory engine (single-digit microseconds)
// and a cross-region broker (hundreds of milliseconds), and
// prometheus.DefBuckets starts at 5 ms, which puts every in-process
// publication in the first bucket.
var DefaultDurationBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
	0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// DefaultBatchBuckets are the iqmq_publish_batch_size buckets. They stop at
// 1000 because every driver's native batch limit is below it (SQS allows 10,
// Pub/Sub 1000) and a larger batch is the core's own, which bounds.
var DefaultBatchBuckets = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

// Cardinality bounds the label combinations each metric family may have
// (the specification's "cardinality controls").
//
// The zero value takes the defaults: [DefaultMaxSeries] combinations and
// [DefaultOverflow] for the rest.
type Cardinality struct {
	// MaxSeries is how many distinct label combinations one family may
	// have. Zero means DefaultMaxSeries and a negative value means
	// unbounded, which follows the repository's convention that zero is
	// "unset" and only an explicit negative removes a bound.
	MaxSeries int
	// Overflow is the label value every combination past MaxSeries is
	// folded into. Empty means DefaultOverflow. It is one series and not a
	// dropped observation: a fold that discarded the event would make the
	// totals disagree with the broker's.
	Overflow string
	// OmitTopic replaces every topic label value with the empty string,
	// which PromQL treats as an absent label. It is the control for a
	// deployment whose topic names are per-tenant.
	OmitTopic bool
	// OmitSubscription does the same for the subscription label.
	OmitSubscription bool
}

// resolve fills the zero fields.
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
	// Registerer is where the metrics are registered. It is required.
	Registerer prom.Registerer
	// DurationBuckets are the latency buckets for publish, settle and
	// handler duration. Nil takes DefaultDurationBuckets.
	DurationBuckets []float64
	// BatchBuckets are the iqmq_publish_batch_size buckets. Nil takes
	// DefaultBatchBuckets.
	BatchBuckets []float64
	// Cardinality bounds the label combinations.
	Cardinality Cardinality
}

// Metrics is the registered metric set and the adapters that feed it.
//
// It is safe for concurrent use. The zero value is not usable; [New] returns
// a registered one.
type Metrics struct {
	card Cardinality

	// The event-sourced families. Each is fed by a hook or by the handler
	// middleware, never by both and never also from a stats snapshot: a
	// quantity with two sources is a quantity that double-counts.
	publishMessages *prom.CounterVec
	publishBytes    *prom.CounterVec
	publishDuration *prom.HistogramVec
	publishBatch    *prom.HistogramVec
	consumeMessages *prom.CounterVec
	handlerDuration *prom.HistogramVec
	settleDuration  *prom.HistogramVec
	settleFailures  *prom.CounterVec
	leaseExtensions *prom.CounterVec
	reconnects      *prom.CounterVec

	// The snapshot-sourced families, collected on scrape from the handles a
	// caller registered. They are here because no hook carries them.
	stats *statsCollector

	pub publisherSeries
	con consumerSeries
}

// New registers the metric set on cfg.Registerer and returns the adapters.
//
// It returns an error rather than panicking, because a duplicate
// registration is the ordinary failure — two clients in one process sharing
// one registry — and a program that meets it should be able to say so.
func New(cfg Config) (*Metrics, error) {
	if cfg.Registerer == nil {
		return nil, ErrNoRegisterer
	}
	dur := cfg.DurationBuckets
	if dur == nil {
		dur = DefaultDurationBuckets
	}
	batch := cfg.BatchBuckets
	if batch == nil {
		batch = DefaultBatchBuckets
	}
	if !increasing(dur) || !increasing(batch) {
		return nil, ErrBadBuckets
	}

	m := &Metrics{card: cfg.Cardinality.resolve()}
	m.publishMessages = prom.NewCounterVec(prom.CounterOpts{
		Namespace: Namespace, Name: "publish_messages_total",
		Help: "Messages handed to the driver, by outcome.",
	}, []string{labelTopic, labelOutcome})
	m.publishBytes = prom.NewCounterVec(prom.CounterOpts{
		Namespace: Namespace, Name: "publish_bytes_total",
		Help: "Body bytes of every completed publication, successful or not.",
	}, []string{labelTopic})
	m.publishDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: Namespace, Name: "publish_duration_seconds",
		Help: "Time from handing a publication to the driver to its outcome.", Buckets: dur,
	}, []string{labelTopic, labelOutcome})
	m.publishBatch = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: Namespace, Name: "publish_batch_size",
		Help: "Messages per publication: 1 for a single publish.", Buckets: batch,
	}, []string{labelTopic})
	m.consumeMessages = prom.NewCounterVec(prom.CounterOpts{
		Namespace: Namespace, Name: "consume_messages_total",
		Help: "Deliveries returned by a fetch.",
	}, []string{labelTopic, labelSubscription})
	m.handlerDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: Namespace, Name: "handler_duration_seconds",
		Help: "Handler call duration, by outcome.", Buckets: dur,
	}, []string{labelTopic, labelSubscription, labelOutcome})
	m.settleDuration = prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: Namespace, Name: "settle_duration_seconds",
		Help: "Settlement duration, by kind.", Buckets: dur,
	}, []string{labelTopic, labelSubscription, labelKind})
	// The `kind` label is not in the spelling of this name and is here
	// for the reason the specification records: a failed ack and a failed
	// nack are different problems, and the histogram beside it already
	// splits them.
	m.settleFailures = prom.NewCounterVec(prom.CounterOpts{
		Namespace: Namespace, Name: "settle_failures_total",
		Help: "Settlements that failed, ambiguous outcomes included.",
	}, []string{labelTopic, labelSubscription, labelKind})
	m.leaseExtensions = prom.NewCounterVec(prom.CounterOpts{
		Namespace: Namespace, Name: "lease_extensions_total",
		Help: "Lease extensions, by outcome.",
	}, []string{labelTopic, labelSubscription, labelOutcome})
	m.reconnects = prom.NewCounterVec(prom.CounterOpts{
		Namespace: Namespace, Name: "reconnects_total",
		Help: "Driver reconnections, by outcome. Fed by a driver's own Hooks config; see Unsourced.",
	}, []string{labelDriver, labelOutcome})

	m.stats = newStatsCollector(&m.card)
	m.pub.init(m)
	m.con.init(m)

	for _, c := range m.collectors() {
		if err := cfg.Registerer.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// collectors is every collector New registers, in a fixed order so that a
// failed registration leaves a predictable prefix registered.
func (m *Metrics) collectors() []prom.Collector {
	return []prom.Collector{
		m.publishMessages, m.publishBytes, m.publishDuration, m.publishBatch,
		m.consumeMessages, m.handlerDuration, m.settleDuration, m.settleFailures,
		m.leaseExtensions, m.reconnects, m.stats,
	}
}

// Unregister removes every metric this Metrics registered, so that a test or
// a client that is closing can hand its registry back. It reports whether
// every collector was found.
func (m *Metrics) Unregister(r prom.Registerer) bool {
	ok := true
	for _, c := range m.collectors() {
		if !r.Unregister(c) {
			ok = false
		}
	}
	return ok
}

// increasing reports whether s is non-empty and strictly increasing.
func increasing(s []float64) bool {
	if len(s) == 0 {
		return false
	}
	for i := 1; i < len(s); i++ {
		if s[i] <= s[i-1] {
			return false
		}
	}
	return true
}

// topicLabel is the topic's label value under the cardinality controls. The
// bound itself is applied by the series cache, which is the only place that
// knows how many combinations it has already admitted.
func (m *Metrics) topicLabel(topic string) string {
	if m.card.OmitTopic {
		return ""
	}
	return labelValue(topic)
}

// subLabel is the subscription's label value.
func (m *Metrics) subLabel(sub string) string {
	if m.card.OmitSubscription {
		return ""
	}
	return labelValue(sub)
}

// labelValue makes a broker-supplied string safe to use as a Prometheus label
// value.
//
// prometheus.CounterVec.WithLabelValues PANICS on a label value that is not
// valid UTF-8, and MustNewConstMetric does the same — on the goroutine that
// published, fetched or settled, inside the hook. The specification says a delivery's
// topic is the CONCRETE one the broker reported, not the pattern a
// subscription named, and the specification says broker-delivered metadata is untrusted: a
// wildcard subscription, a NATS subject, a Redis key or a queue somebody else
// created can therefore put arbitrary bytes into this label. An integration
// that crashed the program it observes, on the broker's word, would be the
// worst possible version of "an observability integration must not change
// what it observes" — so invalid bytes become U+FFFD and the series keeps the
// rest of the name.
//
// The check is on the event path and is allocation-free for the ordinary
// case: utf8.ValidString scans and returns, and nothing is built unless the
// scan fails. FuzzMetricLabels is what holds it to that.
func labelValue(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}

// conKey identifies one consumer series: a topic and a subscription.
type conKey struct {
	topic, sub string
}

// cache is a copy-on-write map from a label key to the per-series children,
// bounded by the cardinality control.
//
// A read is an atomic load and a map lookup and allocates nothing, which is
// what puts the event paths at zero allocations per message; the mutex is
// taken only to admit a key the map does not hold, which happens at most
// MaxSeries times per family plus once for the overflow key. Nothing is
// called while the mutex is held.
type cache[K comparable, V any] struct {
	mu    sync.Mutex
	m     atomic.Pointer[map[K]V]
	limit int
	// over is the overflow series, built on the first key past the limit.
	over     V
	overOnce sync.Once
}

// get returns the series for k, building it when the key is new and the family has room, and folding onto the overflow series when it has not.
func (c *cache[K, V]) get(k K, build func(K) V, overflow func() V) V {
	if m := c.m.Load(); m != nil {
		if v, ok := (*m)[k]; ok {
			return v
		}
	}
	return c.admit(k, build, overflow)
}

// admit builds the series for a key the map does not hold.
func (c *cache[K, V]) admit(k K, build func(K) V, overflow func() V) V {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	if c.limit >= 0 && n >= c.limit {
		c.overOnce.Do(func() { c.over = overflow() })
		return c.over
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

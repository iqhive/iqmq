package prometheus

import (
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// Inspector is the memory engine's inspection surface: iqmq/memory's *Broker
// implements it. It is what feeds the four iqmq_memory_* metrics, which
// have no other source — the engine's per-lane state is not in ConsumerStats
// and no hook carries it.
//
// The interface names iqmq/memory's own type rather than restating it,
// because memory.Snapshot is the source and a restated copy of it would drift
// the first time the engine gained a counter. memory is part of the ROOT
// module, so this costs no dependency (root and memory are stdlib-only).
type Inspector interface {
	// Inspect reports one subscription's lane state.
	Inspect(topic, sub string) (memory.Snapshot, error)
}

// consumerSource is one registered consumer handle.
type consumerSource struct {
	topic, sub string
	stats      func() mq.ConsumerStats
}

// publisherSource is one registered publisher handle.
type publisherSource struct {
	topic  string
	policy string
	stats  func() mq.PublisherStats
}

// memorySource is one registered memory subscription.
type memorySource struct {
	topic, sub string
	in         Inspector
}

// statsCollector collects the families no hook carries: the gauges, and the
// counters the core keeps in ConsumerStats and PublisherStats rather than
// emitting as events.
//
// It reads its sources on scrape, which is what a gauge is: a value at the
// moment it is asked for. Nothing here is a mirror of a family the hooks
// feed — a quantity with two sources double-counts, so iqmq_in_flight is
// collected here and iqmq_consume_messages_total is not, even though
// ConsumerStats carries Received too.
type statsCollector struct {
	card *Cardinality

	mu         sync.RWMutex
	consumers  map[conKey]consumerSource
	publishers map[string]publisherSource
	memories   map[conKey]memorySource
	queues     map[conKey]mq.QueueStats

	inFlight     *prom.Desc
	redeliveries *prom.Desc
	pending      *prom.Desc
	backpressure *prom.Desc
	dropped      *prom.Desc
	queueDepth   *prom.Desc
	lag          *prom.Desc
	memReady     *prom.Desc
	memDelayed   *prom.Desc
	memInFlight  *prom.Desc
	memBytes     *prom.Desc
}

func newStatsCollector(card *Cardinality) *statsCollector {
	topicSub := []string{labelTopic, labelSubscription}
	return &statsCollector{
		card:       card,
		consumers:  map[conKey]consumerSource{},
		publishers: map[string]publisherSource{},
		memories:   map[conKey]memorySource{},
		queues:     map[conKey]mq.QueueStats{},

		inFlight: prom.NewDesc(Namespace+"_in_flight",
			"Deliveries handed out and not yet settled.", topicSub, nil),
		redeliveries: prom.NewDesc(Namespace+"_redeliveries_total",
			"Deliveries the broker reported as redeliveries.", topicSub, nil),
		pending: prom.NewDesc(Namespace+"_pending_publishes",
			"Publications admitted and not yet completed.", []string{labelTopic}, nil),
		backpressure: prom.NewDesc(Namespace+"_backpressure_events_total",
			"Publications that met a full client-side buffer.", []string{labelTopic}, nil),
		dropped: prom.NewDesc(Namespace+"_dropped_total",
			"Publications discarded by a drop-on-full backpressure mode.",
			[]string{labelTopic, labelPolicy}, nil),
		queueDepth: prom.NewDesc(Namespace+"_queue_depth",
			"Messages the broker reports as waiting, from the last Admin.Stats observed.",
			topicSub, nil),
		lag: prom.NewDesc(Namespace+"_lag",
			"How far behind a subscription is, from the last Admin.Stats observed.",
			topicSub, nil),
		memReady: prom.NewDesc(Namespace+"_memory_ready",
			"Memory engine: records waiting for a consumer.", topicSub, nil),
		memDelayed: prom.NewDesc(Namespace+"_memory_delayed",
			"Memory engine: records waiting for their due time.", topicSub, nil),
		memInFlight: prom.NewDesc(Namespace+"_memory_inflight",
			"Memory engine: leased records.", topicSub, nil),
		memBytes: prom.NewDesc(Namespace+"_memory_retained_bytes",
			"Memory engine: accounted payload bytes of every record the lane holds.",
			topicSub, nil),
	}
}

// Describe sends every descriptor, so a duplicate registration is refused at
// registration rather than at the first scrape.
func (s *statsCollector) Describe(ch chan<- *prom.Desc) {
	for _, d := range s.descs() {
		ch <- d
	}
}

func (s *statsCollector) descs() []*prom.Desc {
	return []*prom.Desc{
		s.inFlight, s.redeliveries, s.pending, s.backpressure, s.dropped,
		s.queueDepth, s.lag, s.memReady, s.memDelayed, s.memInFlight, s.memBytes,
	}
}

// Collect reads every registered source.
//
// The read lock is held across the source calls, which are Stats() snapshots
// and one Inspect() — bounded, lock-free counter reads in the core. It is not
// held across anything a caller supplied beyond that, and nothing on a
// message path takes this lock at all.
func (s *statsCollector) Collect(ch chan<- prom.Metric) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, c := range s.consumers {
		st := c.stats()
		ch <- prom.MustNewConstMetric(s.inFlight, prom.GaugeValue, float64(st.InFlight), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.redeliveries, prom.CounterValue, float64(st.Redelivered), k.topic, k.sub)
	}
	for topic, p := range s.publishers {
		st := p.stats()
		ch <- prom.MustNewConstMetric(s.pending, prom.GaugeValue, float64(st.Pending), topic)
		ch <- prom.MustNewConstMetric(s.backpressure, prom.CounterValue, float64(st.BackpressureEvents), topic)
		ch <- prom.MustNewConstMetric(s.dropped, prom.CounterValue, float64(st.Drops), topic, p.policy)
	}
	for k, q := range s.queues {
		// An unknown depth is not a zero depth: a broker that cannot
		// report one gets no series rather than a graph that says the queue
		// is empty.
		if q.Depth.Known {
			ch <- prom.MustNewConstMetric(s.queueDepth, prom.GaugeValue, float64(q.Depth.Value), k.topic, k.sub)
		}
		if q.Lag.Known {
			ch <- prom.MustNewConstMetric(s.lag, prom.GaugeValue, float64(q.Lag.Value), k.topic, k.sub)
		}
	}
	for k, m := range s.memories {
		snap, err := m.in.Inspect(m.topic, m.sub)
		if err != nil {
			// A subscription the engine does not have yet is not a
			// subscription with zero of everything.
			continue
		}
		ch <- prom.MustNewConstMetric(s.memReady, prom.GaugeValue, float64(snap.Ready), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.memDelayed, prom.GaugeValue, float64(snap.Delayed), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.memInFlight, prom.GaugeValue, float64(snap.InFlight), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.memBytes, prom.GaugeValue, float64(snap.Bytes), k.topic, k.sub)
	}
}

// room reports whether another key fits under the cardinality limit.
func (s *statsCollector) room(n int) bool {
	return s.card.MaxSeries < 0 || n < s.card.MaxSeries
}

// AddConsumer registers a consumer handle's counters as the source of
// iqmq_in_flight and iqmq_redeliveries_total for one (topic, subscription).
//
//	m.AddConsumer("orders", "billing", consumer.Stats)
//
// Registering the same pair twice replaces the first, so a reopened consumer
// does not produce two samples with one label set — which is a scrape error
// and not a duplicate graph. It returns false when the cardinality limit is
// reached, in which case nothing was registered: folding two subscriptions'
// in-flight counts onto one series would report neither.
//
// Call [Metrics.RemoveConsumer] when the handle closes. A closed handle's
// Stats() is safe to read — it reports what it counted — but a scrape that
// keeps reporting a consumer nobody has is a stale graph.
func (m *Metrics) AddConsumer(topic, sub string, stats func() mq.ConsumerStats) bool {
	if stats == nil {
		return false
	}
	k := conKey{m.topicLabel(topic), m.subLabel(sub)}
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.consumers[k]; !ok && !s.room(len(s.consumers)) {
		return false
	}
	s.consumers[k] = consumerSource{topic: topic, sub: sub, stats: stats}
	return true
}

// RemoveConsumer stops collecting a consumer's counters.
func (m *Metrics) RemoveConsumer(topic, sub string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.consumers, conKey{m.topicLabel(topic), m.subLabel(sub)})
}

// AddPublisher registers a publisher handle's counters as the source of
// iqmq_pending_publishes, iqmq_backpressure_events_total and
// iqmq_dropped_total.
//
// mode is the backpressure mode the publisher was configured with and it
// becomes the `policy` label of iqmq_dropped_total. It is a parameter because
// PublisherStats does not carry it and PublisherHandle has no accessor for
// it: the label the specification names has no source in the core, so the only honest
// forms are to be told or to omit the metric.
func (m *Metrics) AddPublisher(topic string, mode mq.BackpressureMode, stats func() mq.PublisherStats) bool {
	if stats == nil {
		return false
	}
	k := m.topicLabel(topic)
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.publishers[k]; !ok && !s.room(len(s.publishers)) {
		return false
	}
	s.publishers[k] = publisherSource{topic: topic, policy: mode.String(), stats: stats}
	return true
}

// RemovePublisher stops collecting a publisher's counters.
func (m *Metrics) RemovePublisher(topic string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.publishers, m.topicLabel(topic))
}

// AddMemory registers a memory broker as the source of the four
// iqmq_memory_* gauges for one subscription.
//
//	client, broker := memory.NewForTest(t)
//	m.AddMemory("orders", "billing", broker)
func (m *Metrics) AddMemory(topic, sub string, in Inspector) bool {
	if in == nil {
		return false
	}
	k := conKey{m.topicLabel(topic), m.subLabel(sub)}
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.memories[k]; !ok && !s.room(len(s.memories)) {
		return false
	}
	s.memories[k] = memorySource{topic: topic, sub: sub, in: in}
	return true
}

// RemoveMemory stops collecting a memory subscription's gauges.
func (m *Metrics) RemoveMemory(topic, sub string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.memories, conKey{m.topicLabel(topic), m.subLabel(sub)})
}

// ObserveQueueStats records what Admin.Stats reported, which is the source of
// iqmq_queue_depth.
//
// It is push and not pull because the answer is a broker round trip: a
// collector that called Admin.Stats on every scrape would put an RPC — with
// its own timeout, failure and rate limit — inside /metrics. A caller polls
// on its own schedule and hands the answer over:
//
//	st, err := admin.Stats(ctx, "orders", "billing")
//	if err == nil {
//	    m.ObserveQueueStats("orders", "billing", st)
//	}
//
// It feeds iqmq_queue_depth and iqmq_lag. An unknown value produces no series
// at all, so a broker that cannot report a depth is never graphed as an empty
// queue and one that cannot report lag is never graphed as caught up. It
// returns false when the cardinality limit is reached and the observation was
// dropped.
func (m *Metrics) ObserveQueueStats(topic, sub string, st mq.QueueStats) bool {
	k := conKey{m.topicLabel(topic), m.subLabel(sub)}
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queues[k]; !ok && !s.room(len(s.queues)) {
		return false
	}
	s.queues[k] = st
	return true
}

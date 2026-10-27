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
	// label is the series the source's samples go to: its topic and
	// subscription under the cardinality controls, which with Omit* is a
	// series several sources share.
	label conKey
	stats func() mq.ConsumerStats
}

// publisherSource is one registered publisher handle.
type publisherSource struct {
	topic  string
	policy string
	// label is the topic label value; see consumerSource.label.
	label string
	stats func() mq.PublisherStats
}

// memorySource is one registered memory subscription.
type memorySource struct {
	topic, sub string
	// label is the series; see consumerSource.label.
	label conKey
	in    Inspector
}

// queueSource is the last Admin.Stats answer observed for one queue.
type queueSource struct {
	// label is the series; see consumerSource.label.
	label conKey
	stats mq.QueueStats
}

// dropKey is one iqmq_dropped_total series: a topic label and a policy.
type dropKey struct {
	topic, policy string
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
//
// The sources are keyed by the handle's OWN topic and subscription, not by
// the label values they are exposed under. With OmitTopic or
// OmitSubscription several handles share one label set, and keying by it made
// the second registration replace the first, made removing one handle delete
// another's source, and moved a counter series from one handle's count to
// another's -- backwards, to the scrape. So the samples of a shared series
// are summed over its sources, which is the fold the hook-fed families
// already make, and a counter a removed handle had counted is kept on the
// series for as long as other handles still feed it. The answers
// ObserveQueueStats records are kept the same way, per queue, and summed per
// series.
type statsCollector struct {
	card *Cardinality

	mu         sync.RWMutex
	consumers  map[conKey]consumerSource
	publishers map[string]publisherSource
	memories   map[conKey]memorySource
	queues     map[conKey]queueSource

	// queueLabels counts the observed queues feeding each series. The
	// registrations above find a series by scanning their sources, which
	// happens once per handle. An observation happens once per poll per
	// queue, and the deployments whose queues share a series are the ones
	// with a queue per tenant, so a scan there would cost every poll of
	// every tenant a pass over all the others.
	queueLabels map[conKey]int

	// The counts removed sources had made on a series still fed by others,
	// keyed by the series.
	retiredRedeliveries map[conKey]int64
	retiredBackpressure map[string]int64
	retiredDrops        map[dropKey]int64

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
		queues:     map[conKey]queueSource{},

		queueLabels: map[conKey]int{},

		retiredRedeliveries: map[conKey]int64{},
		retiredBackpressure: map[string]int64{},
		retiredDrops:        map[dropKey]int64{},

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

	// One sample per series, summed over the sources that share it: two
	// samples with one label set fail the whole scrape.
	type consumerSums struct{ inFlight, redelivered int64 }
	cons := make(map[conKey]consumerSums, len(s.consumers))
	for _, c := range s.consumers {
		st := c.stats()
		sum := cons[c.label]
		sum.inFlight += st.InFlight
		sum.redelivered += st.Redelivered
		cons[c.label] = sum
	}
	for k, sum := range cons {
		ch <- prom.MustNewConstMetric(s.inFlight, prom.GaugeValue, float64(sum.inFlight), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.redeliveries, prom.CounterValue, float64(sum.redelivered+s.retiredRedeliveries[k]), k.topic, k.sub)
	}
	type publisherSums struct{ pending, backpressure int64 }
	pubs := make(map[string]publisherSums, len(s.publishers))
	drops := make(map[dropKey]int64, len(s.publishers))
	for _, p := range s.publishers {
		st := p.stats()
		sum := pubs[p.label]
		sum.pending += st.Pending
		sum.backpressure += st.BackpressureEvents
		pubs[p.label] = sum
		drops[dropKey{p.label, p.policy}] += st.Drops
	}
	for topic, sum := range pubs {
		ch <- prom.MustNewConstMetric(s.pending, prom.GaugeValue, float64(sum.pending), topic)
		ch <- prom.MustNewConstMetric(s.backpressure, prom.CounterValue, float64(sum.backpressure+s.retiredBackpressure[topic]), topic)
	}
	for k, n := range drops {
		ch <- prom.MustNewConstMetric(s.dropped, prom.CounterValue, float64(n+s.retiredDrops[k]), k.topic, k.policy)
	}
	type queueSums struct {
		depth, lag           int64
		depthKnown, lagKnown bool
	}
	queues := make(map[conKey]queueSums, len(s.queueLabels))
	for _, q := range s.queues {
		sum := queues[q.label]
		if q.stats.Depth.Known {
			sum.depth += q.stats.Depth.Value
			sum.depthKnown = true
		}
		if q.stats.Lag.Known {
			sum.lag += q.stats.Lag.Value
			sum.lagKnown = true
		}
		queues[q.label] = sum
	}
	for k, sum := range queues {
		// An unknown depth is not a zero depth: a series none of whose
		// queues could report one is not exposed, rather than graphed as an
		// empty queue, and a queue that could not adds nothing to a series
		// others feed, as a memory subscription the engine does not have
		// yet adds nothing to its own.
		if sum.depthKnown {
			ch <- prom.MustNewConstMetric(s.queueDepth, prom.GaugeValue, float64(sum.depth), k.topic, k.sub)
		}
		if sum.lagKnown {
			ch <- prom.MustNewConstMetric(s.lag, prom.GaugeValue, float64(sum.lag), k.topic, k.sub)
		}
	}
	type memorySums struct {
		ready, delayed, inFlight int
		bytes                    int64
	}
	mems := make(map[conKey]memorySums, len(s.memories))
	for _, m := range s.memories {
		snap, err := m.in.Inspect(m.topic, m.sub)
		if err != nil {
			// A subscription the engine does not have yet is not a
			// subscription with zero of everything.
			continue
		}
		sum := mems[m.label]
		sum.ready += snap.Ready
		sum.delayed += snap.Delayed
		sum.inFlight += snap.InFlight
		sum.bytes += snap.Bytes
		mems[m.label] = sum
	}
	for k, sum := range mems {
		ch <- prom.MustNewConstMetric(s.memReady, prom.GaugeValue, float64(sum.ready), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.memDelayed, prom.GaugeValue, float64(sum.delayed), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.memInFlight, prom.GaugeValue, float64(sum.inFlight), k.topic, k.sub)
		ch <- prom.MustNewConstMetric(s.memBytes, prom.GaugeValue, float64(sum.bytes), k.topic, k.sub)
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
// The pair is the handle's own, not its label values. Under
// [Cardinality].OmitTopic or OmitSubscription two different pairs can share
// one label set: both are kept, the series reports their sum, and removing
// one leaves the other -- and what the removed one counted -- on it.
//
// Call [Metrics.RemoveConsumer] when the handle closes. A closed handle's
// Stats() is safe to read — it reports what it counted — but a scrape that
// keeps reporting a consumer nobody has is a stale graph.
func (m *Metrics) AddConsumer(topic, sub string, stats func() mq.ConsumerStats) bool {
	if stats == nil {
		return false
	}
	label := conKey{m.topicLabel(topic), m.subLabel(sub)}
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.consumerFeeds(label) && !s.room(s.consumerSeries()) {
		return false
	}
	s.retireConsumer(conKey{topic, sub})
	s.consumers[conKey{topic, sub}] = consumerSource{topic: topic, sub: sub, label: label, stats: stats}
	return true
}

// RemoveConsumer stops collecting a consumer's counters.
func (m *Metrics) RemoveConsumer(topic, sub string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireConsumer(conKey{topic, sub})
}

// retireConsumer drops the source registered for the handle's own topic and
// subscription k, keeping the redeliveries it counted on its series for as
// long as other sources feed it: without them the sum would drop, which a
// scrape reads as a counter reset and rate() as a burst of everything the
// remaining sources ever counted.
func (s *statsCollector) retireConsumer(k conKey) {
	c, ok := s.consumers[k]
	if !ok {
		return
	}
	delete(s.consumers, k)
	s.retiredRedeliveries[c.label] += c.stats().Redelivered
	if !s.consumerFeeds(c.label) {
		delete(s.retiredRedeliveries, c.label)
	}
}

// consumerFeeds reports whether a registered consumer feeds the series label.
func (s *statsCollector) consumerFeeds(label conKey) bool {
	for _, c := range s.consumers {
		if c.label == label {
			return true
		}
	}
	return false
}

// consumerSeries is how many series the registered consumers feed, which is
// what the cardinality limit bounds.
func (s *statsCollector) consumerSeries() int {
	series := make(map[conKey]struct{}, len(s.consumers))
	for _, c := range s.consumers {
		series[c.label] = struct{}{}
	}
	return len(series)
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
	label := m.topicLabel(topic)
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.publisherFeeds(label) && !s.room(s.publisherSeries()) {
		return false
	}
	s.retirePublisher(topic)
	s.publishers[topic] = publisherSource{topic: topic, policy: mode.String(), label: label, stats: stats}
	return true
}

// RemovePublisher stops collecting a publisher's counters.
func (m *Metrics) RemovePublisher(topic string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retirePublisher(topic)
}

// retirePublisher is retireConsumer for the publisher registered for topic,
// whose counters are iqmq_backpressure_events_total and, per policy,
// iqmq_dropped_total.
func (s *statsCollector) retirePublisher(topic string) {
	p, ok := s.publishers[topic]
	if !ok {
		return
	}
	delete(s.publishers, topic)
	st := p.stats()
	s.retiredBackpressure[p.label] += st.BackpressureEvents
	if !s.publisherFeeds(p.label) {
		delete(s.retiredBackpressure, p.label)
	}
	d := dropKey{p.label, p.policy}
	s.retiredDrops[d] += st.Drops
	if !s.dropFeeds(d) {
		delete(s.retiredDrops, d)
	}
}

// publisherFeeds reports whether a registered publisher feeds the series
// label.
func (s *statsCollector) publisherFeeds(label string) bool {
	for _, p := range s.publishers {
		if p.label == label {
			return true
		}
	}
	return false
}

// dropFeeds reports whether a registered publisher feeds the
// iqmq_dropped_total series d.
func (s *statsCollector) dropFeeds(d dropKey) bool {
	for _, p := range s.publishers {
		if p.label == d.topic && p.policy == d.policy {
			return true
		}
	}
	return false
}

// publisherSeries is consumerSeries for the publishers.
func (s *statsCollector) publisherSeries() int {
	series := make(map[string]struct{}, len(s.publishers))
	for _, p := range s.publishers {
		series[p.label] = struct{}{}
	}
	return len(series)
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
	label := conKey{m.topicLabel(topic), m.subLabel(sub)}
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.memoryFeeds(label) && !s.room(s.memorySeries()) {
		return false
	}
	s.memories[conKey{topic, sub}] = memorySource{topic: topic, sub: sub, label: label, in: in}
	return true
}

// memoryFeeds reports whether a registered memory subscription feeds the
// series label. The memory families are gauges, so a removed source has
// nothing to leave behind on its series.
func (s *statsCollector) memoryFeeds(label conKey) bool {
	for _, m := range s.memories {
		if m.label == label {
			return true
		}
	}
	return false
}

// memorySeries is consumerSeries for the memory subscriptions.
func (s *statsCollector) memorySeries() int {
	series := make(map[conKey]struct{}, len(s.memories))
	for _, m := range s.memories {
		series[m.label] = struct{}{}
	}
	return len(series)
}

// RemoveMemory stops collecting a memory subscription's gauges.
func (m *Metrics) RemoveMemory(topic, sub string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.memories, conKey{topic, sub})
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
// It feeds iqmq_queue_depth and iqmq_lag. The queue is the caller's own
// topic and subscription, not its label values: under [Cardinality].OmitTopic
// or OmitSubscription several queues share one series, and the series is the
// sum of the last answer each of them gave, as it is for the handles
// [Metrics.AddConsumer] registers. Keying the answers by the series made each
// queue's overwrite the others', so the series reported whichever queue was
// polled last as if it were all of them.
//
// An unknown value adds nothing to the sum, and a series none of whose queues
// knows a value is not exposed at all, so a broker that cannot report a depth
// is never graphed as an empty queue and one that cannot report lag is never
// graphed as caught up. An answer that knows neither value removes the queue,
// as [Metrics.RemoveQueueStats] does, because it has nothing left to add.
//
// It returns false when the cardinality limit is reached and the observation
// was dropped. The limit counts series, as AddConsumer's does: a queue whose
// series other queues already feed is always admitted.
func (m *Metrics) ObserveQueueStats(topic, sub string, st mq.QueueStats) bool {
	k := conKey{topic, sub}
	label := conKey{m.topicLabel(topic), m.subLabel(sub)}
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	if !st.Depth.Known && !st.Lag.Known {
		s.forgetQueue(k)
		return true
	}
	_, observed := s.queues[k]
	if !observed && !s.queueAdmits(label) {
		return false
	}
	if !observed {
		s.queueLabels[label]++
	}
	s.queues[k] = queueSource{label: label, stats: st}
	return true
}

// RemoveQueueStats forgets what [Metrics.ObserveQueueStats] recorded for one
// queue.
//
// Call it when you stop polling a queue. Its last answer otherwise stays on
// iqmq_queue_depth and iqmq_lag for good, and under [Cardinality].OmitTopic
// or OmitSubscription it stays inside the sum of a series other queues still
// feed, where nothing shows that it is stale. The answers are kept per queue
// so that a series can be their sum, which is also why nothing bounds them
// but the queues you observe: a deployment that folds a queue per tenant
// removes a tenant's queue when the tenant goes.
func (m *Metrics) RemoveQueueStats(topic, sub string) {
	s := m.stats
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetQueue(conKey{topic, sub})
}

// queueAdmits reports whether a queue not observed yet may feed the series
// label: other queues feed it already, or there is room for another series.
func (s *statsCollector) queueAdmits(label conKey) bool {
	return s.queueLabels[label] > 0 || s.room(len(s.queueLabels))
}

// forgetQueue drops the answer observed for the queue k, and the series with
// it once no other queue feeds it. The queue families are gauges, so nothing
// is kept for the series the way a removed handle's counter is.
func (s *statsCollector) forgetQueue(k conKey) {
	q, ok := s.queues[k]
	if !ok {
		return
	}
	delete(s.queues, k)
	s.queueLabels[q.label]--
	if s.queueLabels[q.label] == 0 {
		delete(s.queueLabels, q.label)
	}
}

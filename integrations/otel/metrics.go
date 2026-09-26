package otel

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/metric"

	mq "github.com/iqhive/iqmq"
)

// Instrument names.
//
// They carry no unit suffix, deliberately. An OpenTelemetry instrument names
// its unit in a field, and every OTLP-to-Prometheus exporter appends the
// suffix from it — so `iqmq_publish_duration` with unit "s" is exported as the canonical `iqmq_publish_duration_seconds`, and naming the
// instrument `iqmq_publish_duration_seconds` here would export
// `iqmq_publish_duration_seconds_seconds`. The canonical names are what
// arrives at a Prometheus server through either integration.
const (
	// InstrumentPublishDuration is the publish-latency histogram. It is
	// exported as iqmq_publish_duration_seconds.
	InstrumentPublishDuration = "iqmq_publish_duration"
	// InstrumentHandlerDuration is the processing-time histogram. It is
	// exported as iqmq_handler_duration_seconds.
	InstrumentHandlerDuration = "iqmq_handler_duration"
	// InstrumentEndToEndDuration is the end-to-end-latency histogram:
	// the time from the publisher stamping Properties.CreatedAt to the
	// handler being called. It is exported as
	// iqmq_end_to_end_duration_seconds. The canonical list names no metric
	// for it, so the name is this increment's.
	InstrumentEndToEndDuration = "iqmq_end_to_end_duration"
	// InstrumentInFlight is the in-flight gauge.
	InstrumentInFlight = "iqmq_in_flight"
	// InstrumentLag is the lag gauge.
	InstrumentLag = "iqmq_lag"
)

// unitSeconds and unitMessages are the UCUM units of the instruments above.
const (
	unitSeconds  = "s"
	unitMessages = "{message}"
)

// instruments is the metric half of the instrumentation.
type instruments struct {
	publishDuration metric.Float64Histogram
	handlerDuration metric.Float64Histogram
	e2eDuration     metric.Float64Histogram

	mu        sync.RWMutex
	consumers map[gaugeKey]func() mq.ConsumerStats
	lags      map[gaugeKey]int64
}

// gaugeKey identifies one observable-gauge series.
type gaugeKey struct{ topic, sub string }

// initMetrics builds the instruments and registers the gauge callbacks.
func (i *Instrumentation) initMetrics(meter metric.Meter) error {
	in := &i.inst
	in.consumers = map[gaugeKey]func() mq.ConsumerStats{}
	in.lags = map[gaugeKey]int64{}

	var err error
	if in.publishDuration, err = meter.Float64Histogram(InstrumentPublishDuration,
		metric.WithUnit(unitSeconds),
		metric.WithDescription("Time from handing a publication to the driver to its outcome."),
	); err != nil {
		return err
	}
	if in.handlerDuration, err = meter.Float64Histogram(InstrumentHandlerDuration,
		metric.WithUnit(unitSeconds),
		metric.WithDescription("Handler call duration."),
	); err != nil {
		return err
	}
	if in.e2eDuration, err = meter.Float64Histogram(InstrumentEndToEndDuration,
		metric.WithUnit(unitSeconds),
		metric.WithDescription("Time from the message's creation timestamp to the handler being called."),
	); err != nil {
		return err
	}

	// The gauges are OBSERVABLE, so a series exists only when a callback
	// observes it. That is what keeps the lag gauge honest: no SPI
	// carries lag at v0 and ConsumerStats.Lag is always unknown, so a lag
	// series appears only for a subscription whose Admin.Stats reported one
	// through ObserveQueueStats, and never as a zero standing in for an
	// answer nobody has.
	if _, err = meter.Int64ObservableGauge(InstrumentInFlight,
		metric.WithUnit(unitMessages),
		metric.WithDescription("Deliveries handed out and not yet settled."),
		metric.WithInt64Callback(i.observeInFlight),
	); err != nil {
		return err
	}
	if _, err = meter.Int64ObservableGauge(InstrumentLag,
		metric.WithUnit(unitMessages),
		metric.WithDescription("How far behind a subscription is, when the broker reports it."),
		metric.WithInt64Callback(i.observeLag),
	); err != nil {
		return err
	}
	return nil
}

// observeInFlight reports every registered consumer's in-flight count.
func (i *Instrumentation) observeInFlight(_ context.Context, o metric.Int64Observer) error {
	in := &i.inst
	in.mu.RLock()
	defer in.mu.RUnlock()
	for k, stats := range in.consumers {
		st := stats()
		o.Observe(st.InFlight, i.setFor(opProcess, k.topic, k.sub).observe...)
	}
	return nil
}

// observeLag reports every lag a broker has answered with.
func (i *Instrumentation) observeLag(_ context.Context, o metric.Int64Observer) error {
	in := &i.inst
	in.mu.RLock()
	defer in.mu.RUnlock()
	for k, lag := range in.lags {
		o.Observe(lag, i.setFor(opProcess, k.topic, k.sub).observe...)
	}
	return nil
}

// room reports whether another gauge key fits under the cardinality limit.
func (i *Instrumentation) room(n int) bool {
	return i.card.MaxSeries < 0 || n < i.card.MaxSeries
}

// AddConsumer registers a consumer handle as the source of the in-flight
// gauge for one (topic, subscription).
//
//	i.AddConsumer("orders", "billing", consumer.Stats)
//
// Registering the same pair twice replaces the first. It returns false when
// metrics are disabled or the cardinality limit is reached, in which case
// nothing was registered — folding two subscriptions' in-flight counts onto
// one series would report neither.
func (i *Instrumentation) AddConsumer(topic, sub string, stats func() mq.ConsumerStats) bool {
	if !i.metrics || stats == nil {
		return false
	}
	in := &i.inst
	k := gaugeKey{topic, sub}
	in.mu.Lock()
	defer in.mu.Unlock()
	if _, ok := in.consumers[k]; !ok && !i.room(len(in.consumers)) {
		return false
	}
	in.consumers[k] = stats
	return true
}

// RemoveConsumer stops observing a consumer's in-flight count.
func (i *Instrumentation) RemoveConsumer(topic, sub string) {
	if !i.metrics {
		return
	}
	in := &i.inst
	in.mu.Lock()
	defer in.mu.Unlock()
	delete(in.consumers, gaugeKey{topic, sub})
}

// ObserveQueueStats records what Admin.Stats reported, which is the only
// source of the lag gauge.
//
// It is push and not pull because the answer is a broker round trip: a
// callback that called Admin.Stats would put an RPC inside a metric export.
// An unknown lag observes nothing at all, so a driver that cannot report lag
// produces no series rather than a graph that says the subscription is caught
// up.
func (i *Instrumentation) ObserveQueueStats(topic, sub string, st mq.QueueStats) bool {
	if !i.metrics {
		return false
	}
	in := &i.inst
	k := gaugeKey{topic, sub}
	in.mu.Lock()
	defer in.mu.Unlock()
	if !st.Lag.Known {
		delete(in.lags, k)
		return true
	}
	if _, ok := in.lags[k]; !ok && !i.room(len(in.lags)) {
		return false
	}
	in.lags[k] = st.Lag.Value
	return true
}

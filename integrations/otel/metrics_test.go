package otel_test

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	mq "github.com/iqhive/iqmq"

	otelmq "github.com/iqhive/iqmq/integrations/otel"
)

// metering returns an instrumentation whose instruments land in a manual
// reader.
func metering(t *testing.T, cfg otelmq.Config) (*otelmq.Instrumentation, *sdkmetric.ManualReader) {
	t.Helper()
	r := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	cfg.MeterProvider = mp
	if cfg.System == "" {
		cfg.System = "memory"
	}
	i, err := otelmq.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return i, r
}

// collected returns the instruments the reader has, by name.
func collected(t *testing.T, r *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

// TestInstrumentNamesAndUnits pins the names and the units, which together
// are what a Prometheus exporter turns into the canonical metric names.
//
// An instrument named `iqmq_publish_duration` with unit "s" is exported as
// `iqmq_publish_duration_seconds`; naming the instrument with the suffix
// already on it would export the suffix twice. So the pin
// is the pair, and the test says what the exported name is.
func TestInstrumentNamesAndUnits(t *testing.T) {
	i, r := metering(t, otelmq.Config{})
	driveEveryInstrument(t, i)

	want := []struct{ name, unit, exported string }{
		{otelmq.InstrumentPublishDuration, "s", "iqmq_publish_duration_seconds"},
		{otelmq.InstrumentHandlerDuration, "s", "iqmq_handler_duration_seconds"},
		{otelmq.InstrumentEndToEndDuration, "s", "iqmq_end_to_end_duration_seconds"},
		{otelmq.InstrumentInFlight, "{message}", "iqmq_in_flight"},
		{otelmq.InstrumentLag, "{message}", "iqmq_lag"},
	}
	got := collected(t, r)
	for _, w := range want {
		m, ok := got[w.name]
		if !ok {
			names := make([]string, 0, len(got))
			for n := range got {
				names = append(names, n)
			}
			t.Errorf("no instrument called %q (exported as %q); the reader has %v",
				w.name, w.exported, names)
			continue
		}
		if m.Unit != w.unit {
			t.Errorf("%s: unit %q, want %q — the exported name depends on it", w.name, m.Unit, w.unit)
		}
		if m.Description == "" {
			t.Errorf("%s: no description", w.name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("the reader has %d instruments, want %d: OpenTelemetry "+
			"sentence names three histograms and two gauges", len(got), len(want))
	}
}

// driveEveryInstrument records one observation on each instrument.
func driveEveryInstrument(t *testing.T, i *otelmq.Instrumentation) {
	t.Helper()
	const (
		topic = "orders"
		sub   = "billing"
	)
	i.Hooks().OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: topic, Messages: 1, Bytes: 8, Duration: 2 * time.Millisecond,
	})

	d := &mq.Delivery{}
	d.Properties.CreatedAt = time.Now().Add(-time.Second)
	h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}

	if !i.AddConsumer(topic, sub, func() mq.ConsumerStats {
		return mq.ConsumerStats{InFlight: 3}
	}) {
		t.Fatal("AddConsumer refused")
	}
	if !i.ObserveQueueStats(topic, sub, mq.QueueStats{Lag: mq.Limit[int64]{Value: 12, Known: true}}) {
		t.Fatal("ObserveQueueStats refused")
	}
}

// TestAnUnknownLagIsNoDataPoint is in the OpenTelemetry exposition, and
// it is the whole reason the lag gauge is OBSERVABLE rather than a value this
// package holds: ConsumerStats.Lag is always unknown at v0 because no SPI
// carries lag, so a lag gauge that reported a number would be reporting zero
// for ever, and a graph that says every subscription is caught up is worse
// than a graph with nothing on it.
func TestAnUnknownLagIsNoDataPoint(t *testing.T) {
	i, r := metering(t, otelmq.Config{})
	if !i.AddConsumer("orders", "billing", func() mq.ConsumerStats {
		return mq.ConsumerStats{InFlight: 1}
	}) {
		t.Fatal("AddConsumer refused")
	}
	got := collected(t, r)
	if m, ok := got[otelmq.InstrumentLag]; ok {
		if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) != 0 {
			t.Errorf("the lag gauge has %d data points with nothing observed", len(g.DataPoints))
		}
	}
	// In-flight has one, because a consumer handle is a source that can make
	// it non-zero.
	m, ok := got[otelmq.InstrumentInFlight]
	if !ok {
		t.Fatal("the in-flight gauge has no data at all")
	}
	g, ok := m.Data.(metricdata.Gauge[int64])
	if !ok || len(g.DataPoints) != 1 {
		t.Fatalf("in-flight = %#v, want one data point", m.Data)
	}
	if g.DataPoints[0].Value != 1 {
		t.Errorf("in-flight = %d, want 1", g.DataPoints[0].Value)
	}

	// Once a broker has answered, the lag has a data point; and when it
	// stops answering, it stops having one rather than freezing.
	if !i.ObserveQueueStats("orders", "billing", mq.QueueStats{
		Lag: mq.Limit[int64]{Value: 7, Known: true},
	}) {
		t.Fatal("ObserveQueueStats refused")
	}
	lag := collected(t, r)[otelmq.InstrumentLag]
	lg, ok := lag.Data.(metricdata.Gauge[int64])
	if !ok || len(lg.DataPoints) != 1 || lg.DataPoints[0].Value != 7 {
		t.Fatalf("lag = %#v, want one data point of 7", lag.Data)
	}
	if !i.ObserveQueueStats("orders", "billing", mq.QueueStats{}) {
		t.Fatal("ObserveQueueStats refused")
	}
	lag = collected(t, r)[otelmq.InstrumentLag]
	if lg, ok := lag.Data.(metricdata.Gauge[int64]); ok && len(lg.DataPoints) != 0 {
		t.Errorf("the lag gauge kept %d stale data points after the broker stopped reporting",
			len(lg.DataPoints))
	}
}

// TestEndToEndNeedsACreationTimestamp pins the other honest absence: the specification
// says the publish path never reads the clock on its own, so a message only
// carries Properties.CreatedAt when the publisher was configured to stamp
// one. A message without it contributes no observation rather than one
// measured from the zero time, which the mutation test measured at 9.2e9
// seconds -- two hundred and ninety-two years.
func TestEndToEndNeedsACreationTimestamp(t *testing.T) {
	i, r := metering(t, otelmq.Config{})
	h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	if err := h.Handle(context.Background(), &mq.Delivery{}); err != nil {
		t.Fatal(err)
	}
	got := collected(t, r)
	if m, ok := got[otelmq.InstrumentEndToEndDuration]; ok {
		if hist, ok := m.Data.(metricdata.Histogram[float64]); ok && len(hist.DataPoints) != 0 {
			t.Errorf("a message with no CreatedAt produced %d end-to-end observations",
				len(hist.DataPoints))
		}
	}
	if _, ok := got[otelmq.InstrumentHandlerDuration]; !ok {
		t.Error("the handler-duration histogram has no data: it needs no timestamp from the message")
	}

	d := &mq.Delivery{}
	d.Properties.CreatedAt = time.Now().Add(-250 * time.Millisecond)
	if err := h.Handle(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	m := collected(t, r)[otelmq.InstrumentEndToEndDuration]
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 {
		t.Fatalf("end-to-end = %#v, want one data point", m.Data)
	}
	if got := hist.DataPoints[0].Count; got != 1 {
		t.Errorf("end-to-end count = %d, want 1", got)
	}
	if sum := hist.DataPoints[0].Sum; sum < 0.2 || sum > 5 {
		t.Errorf("end-to-end sum = %v seconds, want about 0.25", sum)
	}
}

// TestMetricsWithoutTracing pins that the two halves are independent: a
// program that exports metrics and no traces installs OnPublishDone and no
// span at all.
func TestMetricsWithoutTracing(t *testing.T) {
	i, r := metering(t, otelmq.Config{})
	h := i.Hooks()
	if h.OnPublishDone == nil {
		t.Error("metrics are on and OnPublishDone is not installed")
	}
	if h.OnReceive != nil || h.OnSettle != nil {
		t.Error("the span hooks are installed with no tracer: the core would build events nobody reads")
	}
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{Topic: "orders", Messages: 1, Duration: time.Millisecond})
	m := collected(t, r)[otelmq.InstrumentPublishDuration]
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
		t.Fatalf("publish duration = %#v, want one observation", m.Data)
	}
}

// fixedClock is an mq.Clock stopped at one instant, so that a duration this
// package measures is exactly what the test arranged rather than whatever the
// machine took.
type fixedClock struct{ at time.Time }

func (c fixedClock) Now() time.Time { return c.at }

func (c fixedClock) NewTimer(time.Duration) mq.Timer {
	panic("integrations/otel does not schedule anything")
}

// TestTheConfiguredClockIsTheOneMeasured pins Config.Clock, which is what
// makes a test under testing/synctest measure what the fake clock says
// instead of the wall clock — the same contract every middleware that
// measures already has.
//
// It is checked on the unparented spans rather than on a histogram because
// their start and end timestamps are DERIVED from the event's own duration,
// so a clock this package ignored would put them at the wrong point in a
// trace and nothing else would notice.
func TestTheConfiguredClockIsTheOneMeasured(t *testing.T) {
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	i, sr := tracing(t, otelmq.Config{Clock: fixedClock{at: at}})
	i.Hooks().OnReceive(context.Background(), mq.ReceiveEvent{
		Topic: "orders", Subscription: "billing", Messages: 1, Duration: 5 * time.Second,
	})
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if got := spans[0].EndTime(); !got.Equal(at) {
		t.Errorf("span end = %v, want the configured clock's %v", got, at)
	}
	if got, want := spans[0].StartTime(), at.Add(-5*time.Second); !got.Equal(want) {
		t.Errorf("span start = %v, want %v: the start is the end less the event's own Duration",
			got, want)
	}
}

// TestRemoveConsumerStopsTheGauge covers RemoveConsumer, which is what a
// closing consumer calls: an observable gauge that kept reporting a handle
// nobody holds would sit at whatever it last counted for ever.
func TestRemoveConsumerStopsTheGauge(t *testing.T) {
	i, r := metering(t, otelmq.Config{})
	if !i.AddConsumer("orders", "billing", func() mq.ConsumerStats {
		return mq.ConsumerStats{InFlight: 4}
	}) {
		t.Fatal("AddConsumer refused")
	}
	m := collected(t, r)[otelmq.InstrumentInFlight]
	if g, ok := m.Data.(metricdata.Gauge[int64]); !ok || len(g.DataPoints) != 1 {
		t.Fatalf("in-flight = %#v, want one data point", m.Data)
	}
	i.RemoveConsumer("orders", "billing")
	m = collected(t, r)[otelmq.InstrumentInFlight]
	if g, ok := m.Data.(metricdata.Gauge[int64]); ok && len(g.DataPoints) != 0 {
		t.Errorf("in-flight kept %d data points after the consumer was removed", len(g.DataPoints))
	}
	// A nil source is refused rather than stored: a callback that called it
	// would panic inside a metric export.
	if i.AddConsumer("orders", "billing", nil) {
		t.Error("AddConsumer accepted a nil stats function")
	}
}

// TestAPanickingHandlerStillRecordsItsDuration is the metrics half of
// TestAPanickingHandlerStillEndsItsSpan: a panicking call is a handler call
// and the histogram counts it.
func TestAPanickingHandlerStillRecordsItsDuration(t *testing.T) {
	i, r := metering(t, otelmq.Config{})
	h := i.Middleware()(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { panic("boom") }))
	if v := handlePanicking(h); v != "boom" {
		t.Fatalf("the panic reached the caller as %v, want the handler's own value", v)
	}
	m := collected(t, r)[otelmq.InstrumentHandlerDuration]
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
		t.Fatalf("handler duration = %#v, want one observation", m.Data)
	}
}

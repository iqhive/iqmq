package prometheus_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
	"github.com/iqhive/iqmq/middleware"

	iqmqprom "github.com/iqhive/iqmq/integrations/prometheus"
)

// sample reads one sample of one metric family by its label values, and says
// what it found when the sample is not there. A metric assertion that cannot
// name the series it wanted is the kind that passes for the wrong reason.
func sample(t *testing.T, reg *prom.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if !matches(m, labels) {
				continue
			}
			switch f.GetType() {
			case dto.MetricType_COUNTER:
				return m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				return m.GetGauge().GetValue()
			case dto.MetricType_HISTOGRAM:
				return float64(m.GetHistogram().GetSampleCount())
			default:
				t.Fatalf("%s: unexpected metric type %v", name, f.GetType())
			}
		}
		t.Fatalf("%s: no sample with labels %v; the family has %d", name, labels, len(f.GetMetric()))
	}
	t.Fatalf("%s: no such metric family", name)
	return 0
}

// matches reports whether m carries every label in want.
func matches(m *dto.Metric, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, l := range m.GetLabel() {
			if l.GetName() == k && l.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// absent reports whether a family has no sample with these labels.
func absent(t *testing.T, reg *prom.Registry, name string, labels map[string]string) bool {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matches(m, labels) {
				return false
			}
		}
	}
	return true
}

// newMetrics registers a metric set on a fresh registry.
func newMetrics(t *testing.T, card iqmqprom.Cardinality) (*prom.Registry, *iqmqprom.Metrics) {
	t.Helper()
	reg := prom.NewPedanticRegistry()
	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg, Cardinality: card})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return reg, m
}

// TestAgainstARealClient is the wiring, end to end: the hooks this package
// returns are installed on a real client over the memory engine, and what the
// engine does moves the metrics.
//
// It is the test that would fail if the hook fields were wired to the wrong
// events — which a unit test that calls the hook functions directly cannot
// notice, because it names the field itself.
func TestAgainstARealClient(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})

	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	cl := mq.NewClient(b, mq.WithHooks(m.Hooks()))
	t.Cleanup(func() { _ = cl.Close() })

	ctx := context.Background()
	const (
		topic = "orders"
		sub   = "billing"
	)
	// The consumer is opened first, deliberately: the memory engine's Queue
	// profile holds a record for the subscriptions that exist when it is
	// published, so a subscription opened afterwards would find nothing and
	// the Receive below would block until the test timed out.
	c, err := cl.Consumer(ctx, mq.ConsumerConfig{Topic: topic, Subscription: sub})
	if err != nil {
		t.Fatalf("opening the consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if !m.AddConsumer(topic, sub, c.Stats) {
		t.Fatal("AddConsumer refused")
	}

	for range 3 {
		if err := cl.Publish(ctx, topic, &mq.Message{Body: []byte("0123456789")}); err != nil {
			t.Fatalf("publishing: %v", err)
		}
	}
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": topic, "outcome": "success",
	}); got != 3 {
		t.Errorf("iqmq_publish_messages_total{outcome=success} = %v, want 3", got)
	}
	if got := sample(t, reg, "iqmq_publish_bytes_total", map[string]string{"topic": topic}); got != 30 {
		t.Errorf("iqmq_publish_bytes_total = %v, want 30", got)
	}
	if got := sample(t, reg, "iqmq_publish_duration_seconds", map[string]string{
		"topic": topic, "outcome": "success",
	}); got != 3 {
		t.Errorf("iqmq_publish_duration_seconds count = %v, want 3", got)
	}
	if got := sample(t, reg, "iqmq_publish_batch_size", map[string]string{"topic": topic}); got != 3 {
		t.Errorf("iqmq_publish_batch_size count = %v, want 3", got)
	}

	buf := make([]mq.Delivery, 3)
	n, err := c.Receive(ctx, buf)
	if err != nil {
		t.Fatalf("receiving: %v", err)
	}
	if n == 0 {
		t.Fatal("Receive returned nothing")
	}
	if got := sample(t, reg, "iqmq_consume_messages_total", map[string]string{
		"topic": topic, "subscription": sub,
	}); got != float64(n) {
		t.Errorf("iqmq_consume_messages_total = %v, want %d", got, n)
	}
	if got := sample(t, reg, "iqmq_in_flight", map[string]string{
		"topic": topic, "subscription": sub,
	}); got != float64(n) {
		t.Errorf("iqmq_in_flight = %v, want %d", got, n)
	}

	if err := buf[0].Ack(); err != nil {
		t.Fatalf("acking: %v", err)
	}
	if got := sample(t, reg, "iqmq_settle_duration_seconds", map[string]string{
		"topic": topic, "subscription": sub, "kind": "ack",
	}); got != 1 {
		t.Errorf("iqmq_settle_duration_seconds{kind=ack} count = %v, want 1", got)
	}

	// A second Ack of the same delivery is ErrAlreadySettled, which is a
	// settlement that failed: it is the reachable failure on this driver and
	// it is what iqmq_settle_failures_total counts.
	if err := buf[0].Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("re-acking = %v, want ErrAlreadySettled", err)
	}
	if got := sample(t, reg, "iqmq_settle_failures_total", map[string]string{
		"topic": topic, "subscription": sub, "kind": "ack",
	}); got != 1 {
		t.Errorf("iqmq_settle_failures_total = %v, want 1", got)
	}

	// An extension is not a settlement and is counted in its own family.
	if err := buf[1].Extend(5 * time.Second); err != nil {
		t.Fatalf("extending: %v", err)
	}
	if got := sample(t, reg, "iqmq_lease_extensions_total", map[string]string{
		"topic": topic, "subscription": sub, "outcome": "success",
	}); got != 1 {
		t.Errorf("iqmq_lease_extensions_total{outcome=success} = %v, want 1", got)
	}
	if !absent(t, reg, "iqmq_settle_failures_total", map[string]string{"kind": "extend"}) {
		t.Error("an extension was counted in iqmq_settle_failures_total ")
	}

	if err := buf[1].Nack(); err != nil {
		t.Fatalf("nacking: %v", err)
	}
	if got := sample(t, reg, "iqmq_settle_duration_seconds", map[string]string{
		"topic": topic, "subscription": sub, "kind": "nack",
	}); got != 1 {
		t.Errorf("iqmq_settle_duration_seconds{kind=nack} count = %v, want 1", got)
	}
	_ = buf[2].Ack()
}

// TestAFailedPublishIsCountedAsOne is the failure half of the publish
// families, driven by the memory engine's own fault injector so that the
// failure comes back from the DRIVER: a publication refused after the core
// handed it over is what OnPublishDone reports, and a sink that could not be
// opened at all never reaches the hook (a gap this increment recorded rather
// than worked around).
func TestAFailedPublishIsCountedAsOne(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	cl := mq.NewClient(b, mq.WithHooks(m.Hooks()))
	t.Cleanup(func() { _ = cl.Close() })

	b.Faults().RejectPublish(1)
	err = cl.Publish(context.Background(), "orders", &mq.Message{Body: []byte("x")})
	if err == nil {
		t.Fatal("the injected publish rejection did not fail the publication")
	}
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "orders", "outcome": "failure",
	}); got != 1 {
		t.Errorf("iqmq_publish_messages_total{outcome=failure} = %v, want 1", got)
	}
	// The success series exists at zero, which is deliberate: a counter with
	// an outcome label whose other value is a MISSING series makes
	// rate(...{outcome="failure"}) unusable until the first failure. What
	// would be wrong is the failure landing on both.
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "orders", "outcome": "success",
	}); got != 0 {
		t.Errorf("a failed publication also counted %v successes", got)
	}
	if got := sample(t, reg, "iqmq_publish_duration_seconds", map[string]string{
		"topic": "orders", "outcome": "failure",
	}); got != 1 {
		t.Errorf("iqmq_publish_duration_seconds{outcome=failure} count = %v, want 1", got)
	}
}

// TestAPartialBatchSplitsTheOutcome pins the one publish outcome that is
// neither: the *mq.BatchError is index-aligned, so a batch of five with two
// failures is three successes and two failures and not five of either.
func TestAPartialBatchSplitsTheOutcome(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	h := m.Hooks()
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: "orders", Messages: 5, Bytes: 50, Duration: time.Millisecond,
		Err: &mq.BatchError{Errs: []error{nil, errors.New("no"), nil, errors.New("no"), nil}},
	})
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "orders", "outcome": "success",
	}); got != 3 {
		t.Errorf("success = %v, want 3", got)
	}
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "orders", "outcome": "failure",
	}); got != 2 {
		t.Errorf("failure = %v, want 2", got)
	}
}

// TestCardinalityFoldsPastTheLimit is the cardinality control: past the
// limit a new topic lands on one overflow series rather than adding a series
// per topic, and the observation is still counted.
func TestCardinalityFoldsPastTheLimit(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{MaxSeries: 2})
	h := m.Hooks()
	for _, topic := range []string{"a", "b", "c", "d"} {
		h.OnPublishDone(context.Background(), mq.PublishDoneEvent{Topic: topic, Messages: 1, Bytes: 1, Duration: time.Millisecond})
	}
	for _, topic := range []string{"a", "b"} {
		if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
			"topic": topic, "outcome": "success",
		}); got != 1 {
			t.Errorf("topic %q = %v, want 1", topic, got)
		}
	}
	// c and d folded together, and nothing was dropped: the fold is a
	// relabelling, not a discard.
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "other", "outcome": "success",
	}); got != 2 {
		t.Errorf("overflow series = %v, want 2 (c and d folded)", got)
	}
	for _, topic := range []string{"c", "d"} {
		if !absent(t, reg, "iqmq_publish_messages_total", map[string]string{"topic": topic}) {
			t.Errorf("topic %q got a series of its own past the limit", topic)
		}
	}
}

// TestCardinalityCanOmitTheTopic is the other control: a deployment whose
// topic names are per-tenant drops the label value, which PromQL reads as an
// absent label.
func TestCardinalityCanOmitTheTopic(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{OmitTopic: true, OmitSubscription: true})
	h := m.Hooks()
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{Topic: "tenant-42", Messages: 1, Bytes: 1, Duration: time.Millisecond})
	h.OnReceive(context.Background(), mq.ReceiveEvent{Topic: "tenant-42", Subscription: "s-42", Messages: 1})
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "", "outcome": "success",
	}); got != 1 {
		t.Errorf("omitted topic = %v, want 1", got)
	}
	if !absent(t, reg, "iqmq_publish_messages_total", map[string]string{"topic": "tenant-42"}) {
		t.Error("the topic name was exposed with OmitTopic set")
	}
	if !absent(t, reg, "iqmq_consume_messages_total", map[string]string{"subscription": "s-42"}) {
		t.Error("the subscription name was exposed with OmitSubscription set")
	}
}

// TestAnUnknownQueueDepthIsNotZero is in the exposition: a broker that
// cannot report a depth gets no series, because a zero would be graphed as an
// empty queue.
func TestAnUnknownQueueDepthIsNotZero(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	if !m.ObserveQueueStats("orders", "billing", mq.QueueStats{}) {
		t.Fatal("ObserveQueueStats refused")
	}
	if !absent(t, reg, "iqmq_queue_depth", nil) {
		t.Error("an unknown depth produced a series")
	}
	if !absent(t, reg, "iqmq_lag", nil) {
		t.Error("an unknown lag produced a series")
	}
	if !m.ObserveQueueStats("orders", "billing", mq.QueueStats{
		Depth: mq.Limit[int64]{Value: 4, Known: true},
		Lag:   mq.Limit[int64]{Value: 9, Known: true},
	}) {
		t.Fatal("ObserveQueueStats refused")
	}
	if got := sample(t, reg, "iqmq_queue_depth", nil); got != 4 {
		t.Errorf("iqmq_queue_depth = %v, want 4", got)
	}
	if got := sample(t, reg, "iqmq_lag", nil); got != 9 {
		t.Errorf("iqmq_lag = %v, want 9", got)
	}
}

// TestRegistrationFailures pins what New refuses.
func TestRegistrationFailures(t *testing.T) {
	if _, err := iqmqprom.New(iqmqprom.Config{}); !errors.Is(err, iqmqprom.ErrNoRegisterer) {
		t.Errorf("New with no Registerer = %v, want ErrNoRegisterer", err)
	}
	reg := prom.NewPedanticRegistry()
	if _, err := iqmqprom.New(iqmqprom.Config{
		Registerer: reg, DurationBuckets: []float64{1, 1},
	}); !errors.Is(err, iqmqprom.ErrBadBuckets) {
		t.Errorf("New with repeated buckets = %v, want ErrBadBuckets", err)
	}
	if _, err := iqmqprom.New(iqmqprom.Config{
		Registerer: reg, BatchBuckets: []float64{},
	}); !errors.Is(err, iqmqprom.ErrBadBuckets) {
		t.Errorf("New with no batch buckets = %v, want ErrBadBuckets", err)
	}
	if _, err := iqmqprom.New(iqmqprom.Config{Registerer: reg}); err != nil {
		t.Fatalf("New: %v", err)
	}
	// A second set on the same registry is a duplicate registration, which
	// is an error and not a panic: two clients in one process sharing one
	// registry is the ordinary case that meets it.
	if _, err := iqmqprom.New(iqmqprom.Config{Registerer: reg}); err == nil {
		t.Error("registering the metric set twice on one registry succeeded")
	}
}

// TestUnregisterHandsTheRegistryBack pins that a closing client can remove
// what it registered, which is what lets a test or a reloading process build
// a second metric set.
func TestUnregisterHandsTheRegistryBack(t *testing.T) {
	reg := prom.NewPedanticRegistry()
	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	if !m.Unregister(reg) {
		t.Fatal("Unregister did not find every collector")
	}
	if _, err := iqmqprom.New(iqmqprom.Config{Registerer: reg}); err != nil {
		t.Fatalf("re-registering after Unregister: %v", err)
	}
}

// TestTheHandlerMiddlewareRecordsTheOutcome covers the one family that comes
// from the middleware chain rather than from a hook.
func TestTheHandlerMiddlewareRecordsTheOutcome(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	cases := []struct {
		outcome string
		err     error
	}{
		{"success", nil},
		{"failure", errors.New("no")},
		{"panic", &mq.PanicError{Value: "boom"}},
	}
	for _, c := range cases {
		h := m.Middleware(nil)(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return c.err }))
		d := &mq.Delivery{}
		if err := h.Handle(context.Background(), d); !errors.Is(err, c.err) {
			t.Fatalf("Handle = %v, want %v", err, c.err)
		}
		if got := sample(t, reg, "iqmq_handler_duration_seconds", map[string]string{
			"outcome": c.outcome,
		}); got != 1 {
			t.Errorf("iqmq_handler_duration_seconds{outcome=%s} count = %v, want 1", c.outcome, got)
		}
	}
}

// TestAnEmptyFetchIsNotAnEvent pins that a fetch which returned nothing does
// not move iqmq_consume_messages_total: Fetch blocks until it has a
// delivery, so an empty return is a closing consumer.
func TestAnEmptyFetchIsNotAnEvent(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	h := m.Hooks()
	h.OnReceive(context.Background(), mq.ReceiveEvent{Topic: "orders", Subscription: "billing", Messages: 0})
	if !absent(t, reg, "iqmq_consume_messages_total", nil) {
		t.Error("an empty fetch created a consume series")
	}
}

// TestRecorderSatisfiesMiddlewareRecorder is the compile-time half of a claim
// the package documentation makes: HandlerRecorder restates
// middleware.Recorder so that this module's dependency on the root module is
// the SPI and not the middleware set, and a *Metrics's recorder therefore
// drops into middleware.Metrics unchanged.
//
// Two declarations of one contract drift the moment one of them gains a
// method, and the drift would show up as a compile error in a CALLER's
// program rather than here. This is the assignment that moves it here, and
// the run below is the other half: the observation really does reach the
// histogram through the middleware and not only through this package's own.
func TestRecorderSatisfiesMiddlewareRecorder(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	var rec middleware.Recorder = m.Recorder()

	h := middleware.Metrics(rec, nil)(mq.HandlerFunc(
		func(context.Context, *mq.Delivery) error { return nil },
	))
	if err := h.Handle(context.Background(), &mq.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if got := sample(t, reg, "iqmq_handler_duration_seconds", map[string]string{
		"outcome": middleware.OutcomeSuccess,
	}); got != 1 {
		t.Errorf("iqmq_handler_duration_seconds count = %v, want 1", got)
	}
}

// TestTheRegisteredBucketsAreTheDocumentedOnes pins the histogram bucket
// boundaries, which are as much of the API as the metric name is: every
// histogram_quantile() a dashboard computes is interpolated between these
// numbers, so changing one silently changes every latency panel built on it.
func TestTheRegisteredBucketsAreTheDocumentedOnes(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	m.Hooks().OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: "orders", Messages: 1, Duration: time.Millisecond,
	})
	if got := buckets(t, reg, "iqmq_publish_duration_seconds"); !slices.Equal(got, iqmqprom.DefaultDurationBuckets) {
		t.Errorf("publish duration buckets = %v, want DefaultDurationBuckets %v",
			got, iqmqprom.DefaultDurationBuckets)
	}
	if got := buckets(t, reg, "iqmq_publish_batch_size"); !slices.Equal(got, iqmqprom.DefaultBatchBuckets) {
		t.Errorf("batch size buckets = %v, want DefaultBatchBuckets %v",
			got, iqmqprom.DefaultBatchBuckets)
	}

	// And a caller's own list is honoured, which is the half that says the
	// defaults are a default rather than a constant.
	own := []float64{0.5, 1, 2}
	reg2 := prom.NewPedanticRegistry()
	m2, err := iqmqprom.New(iqmqprom.Config{Registerer: reg2, DurationBuckets: own})
	if err != nil {
		t.Fatal(err)
	}
	m2.Hooks().OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: "orders", Messages: 1, Duration: time.Millisecond,
	})
	if got := buckets(t, reg2, "iqmq_publish_duration_seconds"); !slices.Equal(got, own) {
		t.Errorf("configured buckets = %v, want %v", got, own)
	}
}

// buckets returns one histogram family's upper bounds.
func buckets(t *testing.T, reg *prom.Registry, name string) []float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			out := make([]float64, 0, len(metric.GetHistogram().GetBucket()))
			for _, b := range metric.GetHistogram().GetBucket() {
				out = append(out, b.GetUpperBound())
			}
			return out
		}
	}
	t.Fatalf("%s: no histogram with a sample", name)
	return nil
}

// TestANegativeMaxSeriesIsUnbounded covers the explicit escape from the
// cardinality bound, which follows the repository's convention that zero
// means "unset" and only a negative value removes a limit.
func TestANegativeMaxSeriesIsUnbounded(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{MaxSeries: -1})
	h := m.Hooks()
	const n = iqmqprom.DefaultMaxSeries + 20
	for i := range n {
		h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
			Topic: "topic-" + strconv.Itoa(i), Messages: 1, Duration: time.Millisecond,
		})
	}
	// Every topic got its own series and none folded onto the overflow value.
	if !absent(t, reg, "iqmq_publish_messages_total", map[string]string{"topic": "other"}) {
		t.Error("a topic folded onto the overflow series with MaxSeries negative")
	}
	for _, i := range []int{0, n / 2, n - 1} {
		topic := "topic-" + strconv.Itoa(i)
		if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
			"topic": topic, "outcome": "success",
		}); got != 1 {
			t.Errorf("topic %q = %v, want 1", topic, got)
		}
	}
}

// TestRemovingASourceStopsItsSeries covers the four Remove methods, which are
// what a closing consumer or publisher calls: a scrape that keeps reporting a
// handle nobody holds is a stale graph, and the gauges would sit at whatever
// the handle last counted for ever.
func TestRemovingASourceStopsItsSeries(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	const (
		topic = "orders"
		sub   = "billing"
	)
	if !m.AddConsumer(topic, sub, func() mq.ConsumerStats {
		return mq.ConsumerStats{InFlight: 2}
	}) {
		t.Fatal("AddConsumer refused")
	}
	if !m.AddPublisher(topic, mq.BackpressureBlock, func() mq.PublisherStats {
		return mq.PublisherStats{Pending: 1}
	}) {
		t.Fatal("AddPublisher refused")
	}
	if !m.AddMemory(topic, sub, fakeInspector{snap: memory.Snapshot{Ready: 3}}) {
		t.Fatal("AddMemory refused")
	}
	for _, name := range []string{"iqmq_in_flight", "iqmq_pending_publishes", "iqmq_memory_ready"} {
		if absent(t, reg, name, nil) {
			t.Fatalf("%s has no series after its source was registered", name)
		}
	}

	m.RemoveConsumer(topic, sub)
	m.RemovePublisher(topic)
	m.RemoveMemory(topic, sub)
	for _, name := range []string{"iqmq_in_flight", "iqmq_pending_publishes", "iqmq_memory_ready"} {
		if !absent(t, reg, name, nil) {
			t.Errorf("%s still has a series after its source was removed", name)
		}
	}

	// A nil source is refused rather than stored, because a collector that
	// called it would panic on the scrape and take the whole /metrics
	// response with it.
	if m.AddConsumer(topic, sub, nil) {
		t.Error("AddConsumer accepted a nil stats function")
	}
	if m.AddPublisher(topic, mq.BackpressureBlock, nil) {
		t.Error("AddPublisher accepted a nil stats function")
	}
	if m.AddMemory(topic, sub, nil) {
		t.Error("AddMemory accepted a nil Inspector")
	}
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather after the refusals: %v", err)
	}
}

// TestTheMiddlewareUsesTheClockItWasGiven pins Metrics.Middleware's clock
// parameter, which was otherwise a public argument nothing exercised.
func TestTheMiddlewareUsesTheClockItWasGiven(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	clk := &steppingClock{step: 250 * time.Millisecond}
	h := m.Middleware(clk)(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { return nil }))
	if err := h.Handle(context.Background(), &mq.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if clk.reads != 2 {
		t.Errorf("the middleware read the clock %d times, want 2 (a start and an end)", clk.reads)
	}
	if got := histogramSum(t, reg, "iqmq_handler_duration_seconds"); got != 0.25 {
		t.Errorf("handler duration sum = %v seconds, want exactly 0.25 from the fake clock", got)
	}
}

// steppingClock advances by a fixed step on every read, so a measured
// duration is exactly one step and owes nothing to the machine.
type steppingClock struct {
	step  time.Duration
	reads int
	at    time.Time
}

func (c *steppingClock) Now() time.Time {
	now := c.at
	c.at = c.at.Add(c.step)
	c.reads++
	return now
}

func (c *steppingClock) NewTimer(time.Duration) mq.Timer {
	panic("integrations/prometheus does not schedule anything")
}

// histogramSum returns one histogram family's sample sum.
func histogramSum(t *testing.T, reg *prom.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if metric.GetHistogram().GetSampleCount() > 0 {
				return metric.GetHistogram().GetSampleSum()
			}
		}
	}
	t.Fatalf("%s: no histogram with a sample", name)
	return 0
}

// TestTheMiddlewareRecordsAPanicItDidNotRecover pins the panic outcome when
// nothing inside the middleware recovers. The runner recovers a panic
// outside the whole chain, so the handler never returns a *mq.PanicError
// here and the panic would otherwise go unrecorded. The panic must still
// reach the caller with its own value.
func TestTheMiddlewareRecordsAPanicItDidNotRecover(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	h := m.Middleware(nil)(mq.HandlerFunc(func(context.Context, *mq.Delivery) error { panic("boom") }))
	if v := handlePanicking(h); v != "boom" {
		t.Fatalf("the panic reached the caller as %v, want the handler's own value", v)
	}
	if got := sample(t, reg, "iqmq_handler_duration_seconds", map[string]string{
		"outcome": "panic",
	}); got != 1 {
		t.Errorf("iqmq_handler_duration_seconds{outcome=panic} count = %v, want 1", got)
	}
}

// handlePanicking calls h the way the runner does, recovering a panic, and
// returns what it panicked with.
func handlePanicking(h mq.Handler) (v any) {
	defer func() { v = recover() }()
	_ = h.Handle(context.Background(), &mq.Delivery{})
	return nil
}

// TestAnUnknownOutcomeIsBoundedByMaxSeries pins the cardinality control on
// the one handler series whose outcome label is not one of the fixed three.
// The outcome is a caller's string, so past the limit it folds onto the
// overflow series like any other label, and nothing is dropped.
func TestAnUnknownOutcomeIsBoundedByMaxSeries(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{MaxSeries: 2})
	rec := m.Recorder()
	for _, o := range []string{"o1", "o2", "o3", "o4"} {
		rec.HandlerDuration("orders", "billing", o, time.Millisecond)
	}
	for _, o := range []string{"o1", "o2"} {
		if got := sample(t, reg, "iqmq_handler_duration_seconds", map[string]string{
			"topic": "orders", "subscription": "billing", "outcome": o,
		}); got != 1 {
			t.Errorf("outcome %q count = %v, want 1", o, got)
		}
	}
	for _, o := range []string{"o3", "o4"} {
		if !absent(t, reg, "iqmq_handler_duration_seconds", map[string]string{"outcome": o}) {
			t.Errorf("outcome %q got a series of its own past the limit", o)
		}
	}
	if got := sample(t, reg, "iqmq_handler_duration_seconds", map[string]string{
		"topic": "other", "subscription": "other", "outcome": "other",
	}); got != 2 {
		t.Errorf("overflow series count = %v, want 2 (o3 and o4 folded)", got)
	}
}

package hooks_test

import (
	"context"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/integrations/otel"
	"github.com/iqhive/iqmq/integrations/prometheus"

	"github.com/iqhive/iqmq/benchmarks/hooks"
)

// BenchmarkHooks is the observer row, which had no benchmark
// anywhere in this repository before this increment:
//
//	Disabled hooks/observer path | 0 allocs, <= 1 predictable branch
//
// Three arms over one identical publication, so the DIFFERENCE between them is
// the observer path's cost :
//
//	BenchmarkHooks/disabled the zero mq.Hooks -- the specification's own row
//	BenchmarkHooks/prometheus   integrations/prometheus's hook set
//	BenchmarkHooks/otel         integrations/otel's, under non-recording
//	                            providers, which is the shape TestAllocs
//	                            already pins at three to five allocations
//
// The driver discards and allocates nothing per message, so the `disabled`
// arm's allocs/op is the figure and not the driver's.
func BenchmarkHooks(b *testing.B) {
	for _, arm := range arms(b) {
		b.Run(arm.name, func(b *testing.B) {
			ctx := context.Background()
			client := mq.NewClient(newBackend(b), mq.WithHooks(arm.hooks))
			b.Cleanup(func() { _ = client.Close() })
			p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
			if err != nil {
				b.Fatalf("opening a publisher: %v", err)
			}
			b.Cleanup(func() { _ = p.Close() })
			m := &mq.Message{Topic: "orders", Key: []byte("cust-42"), Body: []byte("body")}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := p.Publish(ctx, m); err != nil {
					b.Fatalf("publish: %v", err)
				}
			}
		})
	}
}

type arm struct {
	name  string
	hooks mq.Hooks
}

func arms(tb testing.TB) []arm {
	tb.Helper()
	metrics, err := prometheus.New(prometheus.Config{Registerer: prom.NewRegistry()})
	if err != nil {
		tb.Fatalf("prometheus.New: %v", err)
	}
	// Non-recording providers: a span that records would measure an exporter
	// rather than the instrumentation, and the allocation counts
	// integrations/otel's own TestAllocs pins are this shape's.
	inst, err := otel.New(otel.Config{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
		System:         "iqmq",
		Propagator:     propagation.TraceContext{},
	})
	if err != nil {
		tb.Fatalf("otel.New: %v", err)
	}
	return []arm{
		{"disabled", mq.Hooks{}},
		{"prometheus", metrics.Hooks()},
		{"otel", inst.Hooks()},
	}
}

// TestTheDisabledArmAllocatesNothing is the exact form of the row, which a
// budget median cannot express: the figure is a COUNT, and `testing.AllocsPerRun`
// is the check rule 1 asks for on every core hot path. The benchmark
// beside it is what the gate holds to the same number over time and what gives
// the two integrations a nanosecond figure at all.
func TestTheDisabledArmAllocatesNothing(t *testing.T) {
	ctx := context.Background()
	client := mq.NewClient(newBackend(t), mq.WithHooks(mq.Hooks{}))
	t.Cleanup(func() { _ = client.Close() })
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("opening a publisher: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	m := &mq.Message{Topic: "orders", Key: []byte("cust-42"), Body: []byte("body")}
	if err := p.Publish(ctx, m); err != nil {
		t.Fatalf("publish: %v", err)
	}
	got := testing.AllocsPerRun(200, func() {
		if err := p.Publish(ctx, m); err != nil {
			t.Fatalf("publish: %v", err)
		}
	})
	if got != 0 {
		t.Errorf("a publication with the zero mq.Hooks allocates %v per operation; the specification requires 0 on the disabled observer path", got)
	}
}

// TestAllocs is the name `make allocs` selects, in every module (the specification made that every module rather than the root only). It is the same
// assertion: the disabled observer path allocates nothing.
//
// It is NOT skipped under the race detector, and that is deliberate rather
// than an oversight. Its neighbours are -- `integrations/prometheus` and
// `integrations/otel` both carry an `allocsRaceDetector` build-tagged constant
// and skip, because instrumentation stops their counts (three to five per
// event) being a fixed number. This assertion is 0 against not-0 on a path
// with no allocation at all, where the detector adds shadow memory rather than
// Go heap objects, and it was verified passing under `go test -race`. If a
// future toolchain makes it flake, the established answer is beside it: a
// build-tagged constant and a skip.
func TestAllocs(t *testing.T) {
	TestTheDisabledArmAllocatesNothing(t)
}

func newBackend(tb testing.TB) hooks.Backend {
	tb.Helper()
	be, err := hooks.NewBackend()
	if err != nil {
		tb.Fatal(err)
	}
	return be
}

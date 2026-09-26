package prometheus_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"

	iqmqprom "github.com/iqhive/iqmq/integrations/prometheus"
)

var update = flag.Bool("update", false, "rewrite testdata/metrics.golden")

const goldenPath = "testdata/metrics.golden"

// TestMetricNamesGolden pins the whole exposed metric surface: every metric
// name, its type, its label names and its help text (the API's
// canonical names).
//
// A metric name is an API. A dashboard, an alert rule and a recording rule
// are all written against the name and the label names, and renaming either
// breaks every one of them with nothing at compile time to notice — so the
// check is a byte comparison against a committed file and the fix for an
// intended change is to run `go test ./... -update` and commit the diff.
//
// Label VALUES are deliberately not in the golden: a topic name is the
// program's data, not this package's API. What is pinned is that `topic` is a
// label of iqmq_publish_messages_total and `kind` is a label of
// iqmq_settle_duration_seconds.
//
// The rendering comes from prometheus.Registry.Gather, which returns only the
// families that actually have a sample. So this test is also the proof that
// every registered metric HAS a source: it drives each one from the surface a
// program would use, and a metric nothing can feed cannot appear in the
// golden file.
func TestMetricNamesGolden(t *testing.T) {
	reg := prom.NewPedanticRegistry()
	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	driveEveryMetric(t, m)

	got := render(t, reg)
	if *update {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", goldenPath)
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("reading the golden metric names: %v\n\nthe current surface is:\n%s", err, got)
	}
	if got != string(want) {
		t.Errorf("the exposed metric surface changed.\n--- got ---\n%s\n--- want (%s) ---\n%s\n"+
			"A metric name is an API: if the change is intended, run "+
			"`go test ./... -update` and commit %s.", got, goldenPath, want, goldenPath)
	}
}

// render writes the registry's metric surface in a stable, diffable form:
// one metric per block, name, type, sorted label names and help.
func render(t *testing.T, reg *prom.Registry) string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	sort.Slice(families, func(i, j int) bool { return families[i].GetName() < families[j].GetName() })
	var b strings.Builder
	b.WriteString("# the API's canonical metric names as this package exposes them.\n")
	b.WriteString("# Written by TestMetricNamesGolden; regenerate with `go test ./... -update`.\n")
	for _, f := range families {
		labels := map[string]bool{}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = true
			}
		}
		names := make([]string, 0, len(labels))
		for n := range labels {
			names = append(names, n)
		}
		slices.Sort(names)
		b.WriteString("\n")
		b.WriteString(f.GetName())
		b.WriteString("\n  type:   ")
		b.WriteString(strings.ToLower(dto.MetricType_name[int32(f.GetType())]))
		b.WriteString("\n  labels: ")
		if len(names) == 0 {
			b.WriteString("(none)")
		} else {
			b.WriteString(strings.Join(names, ","))
		}
		b.WriteString("\n  help:   ")
		b.WriteString(f.GetHelp())
		b.WriteString("\n")
	}
	return b.String()
}

// fakeInspector is a memory broker's inspection surface, with a fixed answer.
type fakeInspector struct {
	snap memory.Snapshot
	err  error
}

func (f fakeInspector) Inspect(_, _ string) (memory.Snapshot, error) { return f.snap, f.err }

// driveEveryMetric feeds every registered family once, through the surface a
// program uses: the hooks, the handler recorder and the registered handles.
func driveEveryMetric(t *testing.T, m *iqmqprom.Metrics) {
	t.Helper()
	const (
		topic = "orders"
		sub   = "billing"
	)
	h := m.Hooks()

	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{Topic: topic, Messages: 1, Bytes: 120, Duration: 2 * time.Millisecond})
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: topic, Messages: 1, Bytes: 120, Duration: time.Millisecond,
		Err: errors.New("refused"),
	})
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: topic, Messages: 3, Bytes: 360, Duration: 3 * time.Millisecond,
		Err: &mq.BatchError{Errs: []error{nil, errors.New("refused"), nil}},
	})

	h.OnReceive(context.Background(), mq.ReceiveEvent{Topic: topic, Subscription: sub, Messages: 4, Duration: time.Millisecond})

	h.OnSettle(context.Background(), mq.SettleEvent{Topic: topic, Subscription: sub, Kind: mq.SettleAck, Duration: time.Millisecond})
	h.OnSettle(context.Background(), mq.SettleEvent{
		Topic: topic, Subscription: sub, Kind: mq.SettleAck,
		Duration: time.Millisecond, Err: mq.ErrLeaseLost,
	})
	h.OnSettle(context.Background(), mq.SettleEvent{Topic: topic, Subscription: sub, Kind: mq.SettleNack, Duration: time.Millisecond})
	h.OnSettle(context.Background(), mq.SettleEvent{Topic: topic, Subscription: sub, Kind: mq.SettleExtend, Duration: time.Millisecond})
	h.OnSettle(context.Background(), mq.SettleEvent{
		Topic: topic, Subscription: sub, Kind: mq.SettleExtend,
		Duration: time.Millisecond, Err: mq.ErrLeaseLost,
	})

	h.OnReconnect(context.Background(), mq.ReconnectEvent{Driver: "memory", Attempt: 1})
	h.OnReconnect(context.Background(), mq.ReconnectEvent{Driver: "memory", Attempt: 2, Err: errors.New("refused")})

	rec := m.Recorder()
	rec.HandlerDuration(topic, sub, "success", time.Millisecond)
	rec.HandlerDuration(topic, sub, "failure", time.Millisecond)
	rec.HandlerDuration(topic, sub, "panic", time.Millisecond)

	if !m.AddConsumer(topic, sub, func() mq.ConsumerStats {
		return mq.ConsumerStats{InFlight: 2, Redelivered: 7}
	}) {
		t.Fatal("AddConsumer refused")
	}
	if !m.AddPublisher(topic, mq.BackpressureDropNewest, func() mq.PublisherStats {
		return mq.PublisherStats{Pending: 1, BackpressureEvents: 5, Drops: 3}
	}) {
		t.Fatal("AddPublisher refused")
	}
	if !m.AddMemory(topic, sub, fakeInspector{snap: memory.Snapshot{
		Ready: 9, Delayed: 2, InFlight: 1, Bytes: 4096,
	}}) {
		t.Fatal("AddMemory refused")
	}
	if !m.ObserveQueueStats(topic, sub, mq.QueueStats{
		Depth: mq.Limit[int64]{Value: 11, Known: true},
		Lag:   mq.Limit[int64]{Value: 3, Known: true},
	}) {
		t.Fatal("ObserveQueueStats refused")
	}
}

// TestUnsourcedMatchesTheRegistry holds Unsourced to the registry in both
// directions: a metric it says is not registered must be absent, and one it
// says is registered must be present.
//
// Without it the list is prose, and prose is what goes stale: the specification names
// twenty-one metrics and the honest answer for one of them is "no source",
// which is only worth anything if it stays true.
func TestUnsourcedMatchesTheRegistry(t *testing.T) {
	reg := prom.NewPedanticRegistry()
	m, err := iqmqprom.New(iqmqprom.Config{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	driveEveryMetric(t, m)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, f := range families {
		present[f.GetName()] = true
	}
	gaps := iqmqprom.Unsourced()
	if len(gaps) == 0 {
		t.Fatal("Unsourced is empty: the specification names at least one metric with no source in the core")
	}
	for _, g := range gaps {
		if g.Reason == "" {
			t.Errorf("%s: a gap with no reason is not a gap", g.Metric)
		}
		switch {
		case g.Registered && !present[g.Metric]:
			t.Errorf("Unsourced says %s is registered, and the registry does not have it", g.Metric)
		case !g.Registered && present[g.Metric]:
			t.Errorf("Unsourced says %s is NOT registered, and the registry has it", g.Metric)
		}
	}
}

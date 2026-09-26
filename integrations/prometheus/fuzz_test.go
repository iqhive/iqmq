package prometheus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	prom "github.com/prometheus/client_golang/prometheus"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"

	iqmqprom "github.com/iqhive/iqmq/integrations/prometheus"
)

// FuzzMetricLabels fuzzes the label values a BROKER chooses.
//
// The specification makes a delivery's topic the concrete one the broker reported rather
// than the pattern a subscription named, and the specification makes broker-delivered
// metadata untrusted — so a wildcard subscription, a NATS subject, a Redis
// key or a queue somebody else created decides what goes into the `topic` and
// `subscription` labels of every family here. The three properties this holds
// them to are:
//
//   - **No event path panics.** This one is not theoretical: it found that
//     prometheus.CounterVec.WithLabelValues PANICS on a label value that is
//     not valid UTF-8, which would have crashed the program iqmq was
//     observing, on the publish path, on the broker's word.
//   - **Gather always succeeds.** A registry that cannot be gathered serves a
//     500 for /metrics — every metric lost, not just the odd one — so one bad
//     topic name must not be able to poison the exposition.
//   - **Every exposed label value is valid UTF-8 and every metric well
//     formed**, which is what makes the second property hold in an exporter
//     this test does not run.
func FuzzMetricLabels(f *testing.F) {
	seeds := []struct{ topic, sub string }{
		{"orders", "billing"},
		{"", ""},
		{"\xff\xfe", "ok"},                        // invalid UTF-8: the panic above
		{"ok", "\x80"},                            // a lone continuation byte
		{"orders\n# HELP injected", "billing"},    // exposition-format injection
		{"orders\"quoted\\", "billing\"quoted\\"}, // quoting and escaping
		{strings.Repeat("t", 4096), strings.Repeat("s", 4096)}, // unbounded names
		{"\x00", "\x00"},            // NUL
		{"ünïcödé", "sübscription"}, // valid multi-byte
		{"other", "other"},          // collides with the overflow value
		{"日本語", "订阅"},
		{"\xed\xa0\x80", "ok"}, // a UTF-16 surrogate, invalid in UTF-8
	}
	for _, s := range seeds {
		f.Add(s.topic, s.sub)
	}

	f.Fuzz(func(t *testing.T, topic, sub string) {
		reg := prom.NewPedanticRegistry()
		m, err := iqmqprom.New(iqmqprom.Config{
			Registerer:  reg,
			Cardinality: iqmqprom.Cardinality{MaxSeries: 4},
		})
		if err != nil {
			t.Fatal(err)
		}

		h := m.Hooks()
		h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
			Topic: topic, Messages: 2, Bytes: 16, Duration: time.Millisecond,
		})
		h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
			Topic: topic, Messages: 2, Bytes: 16, Duration: time.Millisecond,
			Err: &mq.BatchError{Errs: []error{nil, errors.New("no")}},
		})
		h.OnReceive(context.Background(), mq.ReceiveEvent{
			Topic: topic, Subscription: sub, Messages: 3, Duration: time.Millisecond,
		})
		for _, k := range []mq.SettleKind{mq.SettleAck, mq.SettleNack, mq.SettleExtend} {
			h.OnSettle(context.Background(), mq.SettleEvent{
				Topic: topic, Subscription: sub, Kind: k,
				Duration: time.Millisecond, Err: mq.ErrLeaseLost,
			})
		}
		h.OnReconnect(context.Background(), mq.ReconnectEvent{Driver: topic, Attempt: 1, Err: errors.New("no")})
		m.Recorder().HandlerDuration(topic, sub, "success", time.Millisecond)
		m.Recorder().HandlerDuration(topic, sub, topic, time.Millisecond)

		m.AddConsumer(topic, sub, func() mq.ConsumerStats {
			return mq.ConsumerStats{InFlight: 1, Redelivered: 2}
		})
		m.AddPublisher(topic, mq.BackpressureDropNewest, func() mq.PublisherStats {
			return mq.PublisherStats{Pending: 1, BackpressureEvents: 1, Drops: 1}
		})
		m.AddMemory(topic, sub, fakeInspector{snap: memory.Snapshot{Ready: 1, Bytes: 2}})
		m.ObserveQueueStats(topic, sub, mq.QueueStats{
			Depth: mq.Limit[int64]{Value: 1, Known: true},
			Lag:   mq.Limit[int64]{Value: 2, Known: true},
		})

		families, err := reg.Gather()
		if err != nil {
			t.Fatalf("Gather failed for topic %q, subscription %q: %v\n"+
				"a registry that cannot be gathered serves a 500 for /metrics, "+
				"so one broker-chosen name must not be able to poison the exposition",
				topic, sub, err)
		}
		if len(families) == 0 {
			t.Fatal("nothing was registered at all")
		}
		for _, fam := range families {
			if fam.GetName() == "" {
				t.Error("a metric family with no name")
			}
			for _, metric := range fam.GetMetric() {
				for _, l := range metric.GetLabel() {
					if !utf8.ValidString(l.GetValue()) {
						t.Errorf("%s: label %s = %q is not valid UTF-8; an exporter "+
							"that encodes it into protobuf would fail for the whole batch",
							fam.GetName(), l.GetName(), l.GetValue())
					}
					if !utf8.ValidString(l.GetName()) {
						t.Errorf("%s: label name %q is not valid UTF-8", fam.GetName(), l.GetName())
					}
				}
			}
		}
	})
}

// TestABrokerSuppliedTopicCannotPanicTheHook is the regression for the defect
// FuzzMetricLabels found: a delivery whose topic is not valid UTF-8 used to
// panic inside WithLabelValues, on the goroutine that published or settled.
func TestABrokerSuppliedTopicCannotPanicTheHook(t *testing.T) {
	reg, m := newMetrics(t, iqmqprom.Cardinality{})
	h := m.Hooks()
	h.OnPublishDone(context.Background(), mq.PublishDoneEvent{
		Topic: "orders\xff\xfe", Messages: 1, Bytes: 8, Duration: time.Millisecond,
	})
	if _, err := reg.Gather(); err != nil {
		t.Fatalf("Gather: %v", err)
	}
	// The series keeps the readable part of the name, so the metric is still
	// attributable: strings.ToValidUTF8 replaces each RUN of invalid bytes
	// with one U+FFFD, so the whole name is not lost to one bad byte.
	if got := sample(t, reg, "iqmq_publish_messages_total", map[string]string{
		"topic": "orders\uFFFD", "outcome": "success",
	}); got != 1 {
		t.Errorf("the sanitised series = %v, want 1", got)
	}
}

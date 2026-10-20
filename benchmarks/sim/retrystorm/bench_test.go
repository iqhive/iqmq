package retrystorm

import (
	"testing"

	"github.com/iqhive/iqmq/internal/backoff"
)

var benchSink Result

// BenchmarkBackoff/Storm/<jitter>/clients=10000/outage=30s runs the
// experiment once per iteration and reports the metrics that decide the family: peak-concurrency (the height of the recovery wave), recover-s
// (how long the population takes to drain it) and attempts (the load the
// policy put on a broker that was down). The wall-clock time of the
// benchmark itself is incidental — it measures the simulation, not the
// policy — so these runs are compared on the reported metrics, which are
// deterministic and therefore host-independent.
func BenchmarkBackoff(b *testing.B) {
	b.Run("Storm", func(b *testing.B) {
		for _, k := range backoff.Kinds() {
			b.Run(k.String()+"/clients=10000/outage=30s", func(b *testing.B) {
				p := stormParams(k)
				var r Result
				for b.Loop() {
					r = mustRunEvents(b, p)
				}
				benchSink = r
				b.ReportMetric(float64(r.PeakAfterOutage), "peak-concurrency")
				b.ReportMetric(r.Recover.Seconds(), "recover-s")
				b.ReportMetric(float64(r.Attempts), "attempts")
				b.ReportMetric(float64(r.Failed), "abandoned")
			})
		}
	})
}

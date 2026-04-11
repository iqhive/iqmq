package nack

import (
	"testing"

	mq "bitbucket.org/iqhive/iqmq"
)


// benchRecords is the partition size one iteration drives. It is large enough
// that building the simulation is noise beside the workload and small enough
// that one iteration is microseconds, so a full-profile arm stays far inside
// the harness's three-second ceiling per benchmark.
const benchRecords = 512

// BenchmarkKafkaNack measures every kind.
func BenchmarkKafkaNack(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			b.Run("plan", func(b *testing.B) { benchPlan(b, k) })
			b.Run("failrate=0.1", func(b *testing.B) { benchWorkload(b, k, 1000) })
			b.Run("failrate=1", func(b *testing.B) { benchWorkload(b, k, 100) })
			b.Run("failrate=10", func(b *testing.B) { benchWorkload(b, k, 10) })
		})
	}
}

// benchPlan measures the decision alone.
func benchPlan(b *testing.B, k Kind) {
	s := mustStrategy(b, k, configFor(k))
	d := Delivery{Topic: "orders", Partition: 3, Offset: 1 << 20, Attempt: 2}
	var sink Op
	b.ReportAllocs()
	for b.Loop() {
		p, err := s.Plan(d, mq.NackOptions{})
		if err != nil {
			b.Fatal(err)
		}
		sink = p.Op
	}
	_ = sink
}

// benchWorkload drives one partition through the strategy at one failure rate
// and reports the criterion's own metrics.
func benchWorkload(b *testing.B, k Kind, failEvery int) {
	s := mustStrategy(b, k, configFor(k))
	var deliveries, reorder, ticks, runs int64
	var lag float64
	b.ReportAllocs()
	for b.Loop() {
		sm := newSim(b, s, simConfig{
			Records: benchRecords, FailEvery: failEvery, RestartOnStop: true,
		})
		sm.run()
		deliveries += int64(sm.delivered)
		reorder += int64(sm.violations)
		ticks += sm.ticks
		lag += sm.lag()
		runs++
	}
	if runs == 0 {
		return
	}
	per := float64(runs * benchRecords)
	b.ReportMetric(float64(ticks)/per, "ticks/record")
	b.ReportMetric(float64(deliveries)/per, "deliveries/record")
	b.ReportMetric(float64(reorder)/per, "reorder/record")
	b.ReportMetric(lag/float64(runs), "lag")
}

package flush

import (
	"testing"
	"time"
)


// benchSettlements is how many settlements one iteration drives. It is large
// enough that building the simulation is noise beside the workload and small
// enough that one iteration is microseconds, so a full-profile arm stays far
// inside the harness's three-second ceiling per benchmark.
const benchSettlements = 512

// BenchmarkSettleFlush measures every kind.
func BenchmarkSettleFlush(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			b.Run("decide", func(b *testing.B) { benchDecide(b, k) })
			b.Run("rate=1", func(b *testing.B) { benchRate(b, k, 1) })
			b.Run("rate=10", func(b *testing.B) { benchRate(b, k, 10) })
			b.Run("rate=100", func(b *testing.B) { benchRate(b, k, 100) })
		})
	}
}

// benchDecide measures the decision alone.
func benchDecide(b *testing.B, k Kind) {
	p, err := New(k, Config{})
	if err != nil {
		b.Fatal(err)
	}
	n := p.MaxItems()
	var sink bool
	b.ReportAllocs()
	for b.Loop() {
		sink = p.Submitted(n-1) || p.Ticked(n-1)
	}
	_ = sink
}

// benchRate drives one kind at one arrival rate and reports the criterion's
// own metrics.
func benchRate(b *testing.B, k Kind, perInterval int) {
	interval := DefaultInterval
	cfg := simConfig{
		Settlements: benchSettlements,
		Every:       max(int64(interval/time.Microsecond)/int64(perInterval), 1),
		MaxItems:    DefaultMaxItems,
		Interval:    interval,
	}
	var trips, sent, held int64
	var latency int64
	runs := int64(0)
	b.ReportAllocs()
	for b.Loop() {
		s := newSim(b, k, cfg)
		s.run()
		trips += int64(s.roundTrips)
		sent += int64(s.sent)
		latency += s.latency
		held += int64(s.held)
		runs++
	}
	if runs == 0 || sent == 0 {
		return
	}
	b.ReportMetric(float64(trips)/float64(sent), "roundtrips/settlement")
	b.ReportMetric(float64(latency)/float64(sent), "latency/settlement")
	b.ReportMetric(float64(held)/float64(runs), "held")
}

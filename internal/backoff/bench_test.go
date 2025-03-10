package backoff

import (
	"testing"
	"time"
)

var benchSink time.Duration

// BenchmarkBackoff/<kind>/{Next,Interval,Chain} measures one retry
// decision. Chain walks a whole retry sequence (the shape a runner
// executes when a message keeps failing), which is where the kinds that
// depend on the previous delay differ from those that do not.
//
// The retry-storm simulation that decides this family — 10 000 clients
// through a 30-second outage, reporting peak-concurrency and recover-s —
// lives in benchmarks/sim/retrystorm, because it needs the benchmarks
// module's simulation harness.
func BenchmarkBackoff(b *testing.B) {
	cfg := Config{Base: 200 * time.Millisecond, Max: 30 * time.Second, Growth: 2}
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			p := NewWith(k, cfg)
			b.Run("Next", func(b *testing.B) {
				b.ReportAllocs()
				var word uint64
				var prev, sink time.Duration
				for b.Loop() {
					word += 0x9E3779B97F4A7C15
					prev = p.Next(5, prev, word)
					sink += prev
				}
				benchSink = sink
			})
			b.Run("Interval", func(b *testing.B) {
				b.ReportAllocs()
				var sink time.Duration
				n := 0
				for b.Loop() {
					n = n%16 + 1
					sink += p.Interval(n)
				}
				benchSink = sink
			})
			b.Run("Chain", func(b *testing.B) {
				b.ReportAllocs()
				var sink time.Duration
				var word uint64
				for b.Loop() {
					var prev time.Duration
					for attempt := 1; attempt <= 8; attempt++ {
						word += 0x9E3779B97F4A7C15
						prev = p.Next(attempt, prev, word)
						sink += prev
					}
				}
				benchSink = sink
			})
		})
	}
}

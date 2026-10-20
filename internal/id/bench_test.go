package id

import (
	"testing"

	"github.com/iqhive/iqmq/internal/clock"
)

var (
	benchSink   int
	benchString string
	benchValue  Value
)

// BenchmarkID/<kind>/{Append,Next,Value,Parse,Text}[/P=<n>] measures one
// identifier through each entry point. Append into a caller's buffer is
// the publisher's hot path (0 allocs); Next is the convenience form (one
// string). The parallel variants show how the shared sequence counter and
// random stream behave under contention, which is what separates the
// kinds once the formatting cost is equal.
func BenchmarkID(b *testing.B) {
	for _, k := range Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			g := mustNew(b, k)
			buf := make([]byte, 0, 64)
			text := g.Next()
			b.Run("Append", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchSink += len(g.Append(buf))
				}
			})
			b.Run("Next", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchString = g.Next()
				}
			})
			b.Run("Value", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					benchValue = g.NextValue()
				}
			})
			b.Run("Parse", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					v, err := Parse(k, text)
					if err != nil {
						b.Fatal(err)
					}
					benchValue = v
				}
			})
			b.Run("Text", func(b *testing.B) {
				v := g.NextValue()
				b.ReportAllocs()
				for b.Loop() {
					benchSink += len(v.AppendText(buf))
				}
			})
			for _, p := range []int{1, 8, 64} {
				b.Run("Append/P="+itoa(p), func(b *testing.B) {
					b.SetParallelism(p)
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						local := make([]byte, 0, 64)
						n := 0
						for pb.Next() {
							n += len(g.Append(local))
						}
						benchSink += n
					})
				})
			}
			// The fake clock removes the wall-clock read from the
			// measurement, isolating the formatting and sequencing cost.
			b.Run("Append/clock=fake", func(b *testing.B) {
				fg := mustNewWith(b, k, Config{Clock: clock.NewFake(modelStart), Seed: 1})
				b.ReportAllocs()
				for b.Loop() {
					benchSink += len(fg.Append(buf))
				}
			})
		})
	}
}

// itoa avoids fmt in a benchmark name.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

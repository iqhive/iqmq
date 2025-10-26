package leasectx

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// benchBatches returns the batch sizes measured: one context per delivery,
// and one per delivery of a hundred-delivery fetch, which is what a driver
// with prefetch actually hands the runner.
func benchBatches() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 10, 100, 1000}
	}
	return []int{1, 100}
}

// BenchmarkRun/Deadline/<kind>/batch=<n> is the per-delivery cost of giving a
// handler a context that ends with its lease. The
// timers/op metric is what the family is about: the reference arms one
// runtime timer per delivery, the candidate arms one per scheduler wake.
func BenchmarkRun(b *testing.B) {
	b.Run("Deadline", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, batch := range benchBatches() {
					b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
						benchDeadline(b, k, batch)
					})
				}
			})
		}
	})
	// The handler's own view: a context that is only ever asked whether it is
	// done, which is what a well-behaved handler does on its slow paths.
	b.Run("DeadlineSelect", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				benchSelect(b, k)
			})
		}
	})
}

func benchDeadline(b *testing.B, k Kind, batch int) {
	s := New(k, Config{})
	defer func() { _ = s.Close() }()
	releases := make([]Release, 0, batch)
	parent := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		n := min(batch, b.N-i)
		// A batch shares one lease deadline, as one fetch does.
		deadline := time.Now().Add(time.Hour)
		releases = releases[:0]
		for range n {
			_, release := s.Derive(parent, deadline)
			releases = append(releases, release)
		}
		for _, release := range releases {
			release()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(s.Timers())/float64(b.N), "timers/op")
	if s.Live() != 0 {
		b.Fatalf("Live = %d", s.Live())
	}
}

func benchSelect(b *testing.B, k Kind) {
	s := New(k, Config{})
	defer func() { _ = s.Close() }()
	parent := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		ctx, release := s.Derive(parent, time.Now().Add(time.Hour))
		select {
		case <-ctx.Done():
			b.Fatal("lease expired during the benchmark")
		default:
		}
		release()
	}
	b.ReportMetric(float64(s.Timers())/float64(b.N), "timers/op")
}

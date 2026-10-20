package wait

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkMemory/Wait/<kind>/occupancy=<pct> moves b.N items from one
// producer to one consumer through the wait protocol. occupancy is how far
// the producer may run ahead: 0 forces a wait per item, 100 never waits.
// Wake latency (Notify to Wait return) is reported as p50/p99/p99.9.
// IdleCPU reports the processor share a parked waiter burns; ColdWake the
// wake latency after IQMQ_BENCH_COLD_IDLE (default 100ms) of idleness.
func BenchmarkMemory(b *testing.B) {
	b.Run("Wait", func(b *testing.B) {
		for _, k := range Kinds() {
			b.Run(k.String(), func(b *testing.B) {
				for _, occ := range []int{0, 50, 95, 100} {
					b.Run(fmt.Sprintf("occupancy=%d", occ), func(b *testing.B) { benchOccupancy(b, k, occ) })
				}
				b.Run("IdleCPU", func(b *testing.B) { benchIdleCPU(b, k) })
				b.Run("ColdWake", func(b *testing.B) { benchColdWake(b, k) })
			})
		}
	})
}

func aheadFor(occ int) int64 {
	switch occ {
	case 0:
		return 1
	case 50:
		return 2
	case 95:
		return 20
	default:
		return 1 << 40
	}
}

func benchOccupancy(b *testing.B, k Kind, occ int) {
	s := mustNew(b, k)
	ahead := aheadFor(occ)
	var avail, consumed atomic.Int64
	var stamp atomic.Int64
	total := int64(b.N)
	samples := make([]int64, 0, min(b.N, 1<<16))
	ctx := context.Background()
	done := make(chan struct{})
	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		defer close(done)
		for produced := int64(0); produced < total; {
			if produced-consumed.Load() >= ahead {
				runtime.Gosched()
				continue
			}
			avail.Add(1)
			produced++
			if s.HasWaiters() {
				stamp.Store(time.Now().UnixNano())
				s.Notify()
			}
		}
	}()
	for consumed.Load() < total {
		if take(&avail) {
			consumed.Add(1)
			continue
		}
		tk := s.Prepare()
		if avail.Load() > 0 {
			s.Cancel(tk)
			continue
		}
		if err := s.Wait(ctx, tk); err != nil {
			b.Fatal(err)
		}
		if st := stamp.Load(); st != 0 && len(samples) < cap(samples) {
			samples = append(samples, time.Now().UnixNano()-st)
		}
	}
	<-done
	b.StopTimer()
	reportQuantiles(b, samples)
}

func reportQuantiles(b *testing.B, samples []int64) {
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	q := func(p float64) float64 {
		i := int(p * float64(len(samples)-1))
		return float64(samples[i])
	}
	b.ReportMetric(q(0.50), "p50-ns")
	b.ReportMetric(q(0.99), "p99-ns")
	b.ReportMetric(q(0.999), "p999-ns")
	b.ReportMetric(float64(len(samples)), "waits")
}

func benchIdleCPU(b *testing.B, k Kind) {
	const idle = 20 * time.Millisecond
	s := mustNew(b, k)
	var cpu time.Duration
	var wall time.Duration
	for range b.N {
		ctx, cancel := context.WithTimeout(context.Background(), idle)
		tk := s.Prepare()
		c0, w0 := cpuTime(), time.Now()
		_ = s.Wait(ctx, tk)
		cpu += cpuTime() - c0
		wall += time.Since(w0)
		cancel()
	}
	if wall > 0 {
		b.ReportMetric(100*float64(cpu)/float64(wall), "cpu-pct")
	}
}

func benchColdWake(b *testing.B, k Kind) {
	idle := 100 * time.Millisecond
	if v := os.Getenv("IQMQ_BENCH_COLD_IDLE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			idle = d
		}
	}
	s := mustNew(b, k)
	ctx := context.Background()
	samples := make([]int64, 0, b.N)
	// b.N is tiny here, so setup would otherwise dominate allocs/op.
	b.ResetTimer()
	for range b.N {
		tk := s.Prepare()
		var stamp atomic.Int64
		go func() {
			time.Sleep(idle)
			stamp.Store(time.Now().UnixNano())
			s.Notify()
		}()
		if err := s.Wait(ctx, tk); err != nil {
			b.Fatal(err)
		}
		samples = append(samples, time.Now().UnixNano()-stamp.Load())
	}
	reportQuantiles(b, samples)
}

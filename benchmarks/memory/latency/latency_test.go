// Package latency holds the end-to-end latency benchmarks of the memory
// engine.
//
// The canonical names are
//
//	BenchmarkMemory/Latency/closed/size=<n>/mode=<copymode>
//	BenchmarkMemory/Latency/open/rate=<pct>/size=<n>/mode=<copymode>
//
// with the dimensions in that fixed order: the loop shape, the offered rate
// as a percentage of the measured closed-loop capacity, the body size and the
// ownership mode of the memory engine.
//
// The two shapes answer different questions and both are needed. The closed
// loop measures the service time of one publish/receive/acknowledge cycle
// with nothing else in flight: it is the number the specification's "p99 < 20 µs" refers
// to, and it cannot report queueing, because a slow operation delays the next
// request and so hides its own backlog. The open loop measures the latency a
// client would see at a fixed offered rate: the send schedule is fixed in
// advance from the calibrated capacity, latency is measured from the
// *intended* send time rather than from the actual one, and an operation that
// runs late therefore carries the delay of everything it held up. That is the
// coordinated-omission correction, and it is why the open loop
// is swept at 50, 80, 100 and 120 percent of capacity: the interesting number
// is where the curve turns up.
package latency

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/iqhive/iqmq/benchmarks/internal/latency"
	"github.com/iqhive/iqmq/benchmarks/internal/load"
	"github.com/iqhive/iqmq/internal/engine"
	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/payload"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
)

// sizes are the body sizes swept; the default profile measures one.
func sizes() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{0, 64, 1 << 10, 64 << 10}
	}
	return []int{64}
}

// modes are the ownership modes swept (the memory engine). All
// three are measured in the default profile: the bake-off compares what each
// costs, it does not choose between them.
func modes() []engine.CopyMode {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return engine.CopyModes()
	}
	return []engine.CopyMode{engine.CopySafe, engine.CopyOwned}
}

// calibration is how long the open loop measures the closed-loop capacity it
// paces against. Long enough that the pools are warm and short enough that a
// sweep of four rates is still a benchmark rather than a soak.
const calibration = 200 * time.Millisecond

// openOps is the number of paced operations one open-loop iteration issues.
// The open loop is not a b.Loop() benchmark — the schedule, not the loop,
// decides how much work is done — so one iteration is a whole run at one
// rate.
const openOps = 20000

// rig is one engine with a topic, a subscription and a consumer, plus the
// scratch a cycle needs.
type rig struct {
	eng  *engine.Engine[*payload.Payload]
	top  *engine.Topic[*payload.Payload]
	c    *engine.Consumer[*payload.Payload]
	dst  []engine.Claim[*payload.Payload]
	body []byte
	mode engine.CopyMode
	free []*payload.Payload
}

// newRig builds the engine the latency benchmarks measure: one lane, a
// retaining pool and leases that never expire, so that what is measured is
// the publish/receive/acknowledge path and nothing else.
func newRig(tb testing.TB, size int, mode engine.CopyMode) *rig {
	tb.Helper()
	eng, err := engine.New(engine.Config[*payload.Payload]{
		Kinds:    engine.KindSet{Pool: pool.FreeList, Queue: queue.MutexRing},
		CopyMode: mode,
		Clone:    func(p *payload.Payload) *payload.Payload { return p.Clone() },
		DefaultTopology: engine.TopologyConfig{
			Partitions:       1,
			CapacityMessages: 1 << 16,
			AckDeadline:      -1,
		},
	})
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	tb.Cleanup(func() { _ = eng.Close() })
	top, err := eng.CreateTopic("bench", engine.TopologyConfig{})
	if err != nil {
		tb.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", engine.SubscriptionConfig{})
	if err != nil {
		tb.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(engine.ConsumerConfig{})
	if err != nil {
		tb.Fatalf("Attach: %v", err)
	}
	return &rig{
		eng: eng, top: top, c: c, mode: mode,
		dst:  make([]engine.Claim[*payload.Payload], 1),
		body: make([]byte, size),
	}
}

// get returns a payload to publish: the same one every time for a safe
// publication, which keeps the caller's buffer, and one from a free list for
// an owned or borrowed one, whose release returns it.
func (r *rig) get() *payload.Payload {
	if r.mode.Copies() {
		return &payload.Payload{Body: r.body}
	}
	if n := len(r.free); n > 0 {
		p := r.free[n-1]
		r.free = r.free[:n-1]
		return p
	}
	return &payload.Payload{Body: make([]byte, len(r.body))}
}

// release is the ownership callback of an owned or borrowed publication.
func (r *rig) release(p *payload.Payload) { r.free = append(r.free, p) }

// cycle publishes one record, receives it and acknowledges it. It returns
// false when the engine refused the publication, which the caller counts
// rather than reporting as latency.
func (r *rig) cycle(ctx context.Context) error {
	p := engine.Publish[*payload.Payload]{
		Payload: r.get(), Size: len(r.body), Partition: 0, Mode: r.mode,
	}
	if !r.mode.Copies() {
		p.Release = r.release
	}
	if err := r.top.Publish(ctx, p); err != nil {
		return err
	}
	n, err := r.c.Fetch(ctx, r.dst)
	if err != nil {
		return err
	}
	for i := range n {
		if err := r.c.Ack(r.dst[i].Env, r.dst[i].Gen); err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkMemory is the engine's layer-2 latency macro benchmark.
func BenchmarkMemory(b *testing.B) {
	b.Run("Latency", func(b *testing.B) {
		b.Run("closed", func(b *testing.B) {
			for _, size := range sizes() {
				for _, m := range modes() {
					b.Run(fmt.Sprintf("size=%d/mode=%v", size, m), func(b *testing.B) {
						runClosed(b, size, m)
					})
				}
			}
		})
		b.Run("open", func(b *testing.B) {
			for _, size := range sizes() {
				for _, m := range modes() {
					runOpen(b, size, m)
				}
			}
		})
	})
}

// runClosed measures one publish/receive/acknowledge cycle per operation with
// nothing else in flight, and reports the per-operation latency distribution
// alongside the wall time.
func runClosed(b *testing.B, size int, m engine.CopyMode) {
	b.Helper()
	r := newRig(b, size, m)
	ctx := context.Background()
	hist := &latency.Histogram{}
	// Warm the pools and the free list so the first iterations do not
	// dominate the tail.
	for range 1024 {
		if err := r.cycle(ctx); err != nil {
			b.Fatalf("warm-up: %v", err)
		}
	}
	var failure error
	for b.Loop() {
		start := time.Now()
		if err := r.cycle(ctx); err != nil {
			if failure == nil {
				failure = err
			}
			continue
		}
		hist.RecordSince(start)
	}
	b.StopTimer()
	if failure != nil {
		b.Fatalf("cycle: %v", failure)
	}
	hist.Report(b)
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(hist.Count())/secs, "msgs/s")
	}
}

// runOpen paces the same cycle at a fixed fraction of the measured capacity
// and measures latency from the *intended* send time, so that an operation
// that ran late carries the delay of everything it held up. It is the
// coordinated-omission-resistant shape of the specification B.
func runOpen(b *testing.B, size int, m engine.CopyMode) {
	b.Helper()
	capacity := calibrate(b, size, m)
	rates := load.Rates(capacity)
	for _, name := range []string{"rate=50pct", "rate=80pct", "rate=100pct", "rate=120pct"} {
		b.Run(fmt.Sprintf("%s/size=%d/mode=%v", name, size, m), func(b *testing.B) {
			runOpenAt(b, size, m, rates[name], capacity)
		})
	}
}

// calibrate measures the closed-loop capacity the open loop paces against.
func calibrate(b *testing.B, size int, m engine.CopyMode) float64 {
	b.Helper()
	r := newRig(b, size, m)
	ctx := context.Background()
	var failure error
	c := load.Calibrate(calibration, func() {
		if err := r.cycle(ctx); err != nil && failure == nil {
			failure = err
		}
	})
	if failure != nil {
		b.Fatalf("calibration: %v", failure)
	}
	if c <= 0 {
		b.Fatalf("calibration measured %v operations per second", c)
	}
	return c
}

// runOpenAt issues openOps operations on a fixed schedule and reports the
// latency measured from the intended send times, the achieved rate and the
// backlog the schedule ended with.
func runOpenAt(b *testing.B, size int, m engine.CopyMode, rate, capacity float64) {
	b.Helper()
	r := newRig(b, size, m)
	ctx := context.Background()
	hist := &latency.Histogram{}
	var failure error
	var refused int64
	for b.Loop() {
		hist.Reset()
		refused = 0
		p := load.NewPacer(rate)
		for range openOps {
			intended := p.Wait()
			err := r.cycle(ctx)
			if err != nil {
				if errors.Is(err, errcode.ErrQueueFull) {
					refused++
					continue
				}
				if failure == nil {
					failure = err
				}
				continue
			}
			// Latency from the *intended* send time: an operation that
			// started late carries the delay of everything it held up.
			hist.Record(int64(time.Since(intended)))
		}
	}
	b.StopTimer()
	if failure != nil {
		b.Fatalf("cycle: %v", failure)
	}
	hist.Report(b)
	b.ReportMetric(rate, "offered/s")
	b.ReportMetric(capacity, "capacity/s")
	if refused > 0 {
		b.ReportMetric(float64(refused), "refused")
	}
}

// TestLatencyHarness checks the benchmark harness itself, so that a broken
// harness fails the test suite rather than reporting a meaningless number.
// It is what keeps the "open-loop bench emits p99-ns" gate of plan increment
// The specification honest: the metric is asserted, not assumed.
func TestLatencyHarness(t *testing.T) {
	for _, m := range engine.CopyModes() {
		t.Run("closed/"+m.String(), func(t *testing.T) {
			res := testing.Benchmark(func(b *testing.B) { runClosed(b, 64, m) })
			if res.N == 0 {
				t.Fatalf("benchmark did not run: %+v", res)
			}
			for _, want := range []string{"p50-ns", "p99-ns", "p999-ns", "msgs/s"} {
				if _, ok := res.Extra[want]; !ok {
					t.Fatalf("closed loop reports no %s: %+v", want, res.Extra)
				}
			}
		})
	}
	t.Run("open", func(t *testing.T) {
		res := testing.Benchmark(func(b *testing.B) {
			runOpenAt(b, 64, engine.CopyOwned, 200000, 400000)
		})
		if res.N == 0 {
			t.Fatalf("benchmark did not run: %+v", res)
		}
		for _, want := range []string{"p50-ns", "p99-ns", "p999-ns", "offered/s", "capacity/s"} {
			if _, ok := res.Extra[want]; !ok {
				t.Fatalf("open loop reports no %s: %+v", want, res.Extra)
			}
		}
	})
}

package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/queue"
)

// The slice-3 arms of the layer-2 macro benchmark. The canonical names are
//
//	BenchmarkMemory/OrderedRetry/<kind>/ordering=<mode>/failrate=<pct>
//	BenchmarkMemory/Delayed/lead=<duration>
//	BenchmarkMemory/LeaseExpiry/inflight=<n>
//	BenchmarkMemory/Overflow/<policy>/capacity=<n>
//
// with the dimensions in that fixed order. OrderedRetry is the bake-off
// benchmark of The retry placement is what changes between its
// arms, the ordering mode says whether a redelivery blocks its lane, and the
// failure rate is how often one happens — which is the axis the placements
// part company on.

// benchRetryLanes and benchOrderings are the arms swept. Every kind is
// measured: the bake-off (the specification) decides between them and cannot decide on
// kinds it never saw.
func benchRetryLanes() []RetryLane { return RetryLanes() }

func benchOrderings() []OrderingMode { return OrderingModes() }

// benchFailRates is the percentage of deliveries that fail.
func benchFailRates() []int { return []int{0, 10, 50} }

// retryCapacity is the lane capacity of the retry benchmark: deep enough that
// a publisher is never the bottleneck.
const retryCapacity = 4096

// runOrderedRetry drives b.N publications through one lane whose consumers
// fail the first delivery of a fraction of the records, and reports the
// settlement rate. A record is failed at most once, so the loop terminates
// whatever the placement does with it.
func runOrderedRetry(b *testing.B, k RetryLane, ordering OrderingMode, failrate int) {
	b.Helper()
	// The architecture is pinned for the reason benchTopic gives: nothing in
	// this benchmark's name says which one it ran on, and the strict-ordering
	// arms cannot run at all on one that does not order what it holds.
	ks := KindSet{Arch: featureArch, RetryLane: k, Queue: queue.MutexRing}
	eng, err := New(Config[int64]{
		Kinds: ks,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Kind:             TopologyQueue,
			Partitions:       1,
			CapacityMessages: retryCapacity,
			AckDeadline:      -1,
			Ordering:         ordering,
			Overflow:         OverflowReject,
		},
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()
	top, err := eng.CreateTopic("bench", TopologyConfig{})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var settled, retried atomic.Int64
	var wg sync.WaitGroup
	for range 2 {
		c, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			b.Fatalf("Attach: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]Claim[int64], 32)
			for {
				got, err := c.Fetch(ctx, dst)
				if err != nil {
					return
				}
				for j := range got {
					cl := dst[j]
					fail := cl.Info.Attempts == 1 && int(cl.Payload%100) < failrate
					if fail {
						err = c.Nack(cl.Env, cl.Gen, 0)
						retried.Add(1)
					} else {
						err = c.Ack(cl.Env, cl.Gen)
						settled.Add(1)
					}
					if err != nil {
						cancel()
						return
					}
				}
			}
		}()
	}
	// The loop body stays free of b.Fatalf and of an early return; see
	// runFanOut for why.
	var refused int64
	var failure error
	i := int64(0)
	for b.Loop() {
		i++
		err := top.Publish(ctx, Publish[int64]{Payload: i, Size: 64, Partition: 0})
		if err == nil {
			continue
		}
		if errors.Is(err, errcode.ErrQueueFull) {
			refused++
			runtime.Gosched()
			continue
		}
		failure = err
		break
	}
	b.StopTimer()
	cancel()
	wg.Wait()
	if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, errcode.ErrClosed) {
		b.Fatalf("publish: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(settled.Load())/secs, "msgs/s")
	}
	if n := b.N; n > 0 {
		b.ReportMetric(float64(retried.Load())/float64(n), "retries/op")
		b.ReportMetric(float64(refused)/float64(n), "refused/op")
	}
}

// benchLeads is how far ahead a delayed publication is scheduled.
func benchLeads() []time.Duration { return []time.Duration{0, time.Millisecond} }

// runDelayed drives b.N delayed publications through the scheduler and
// reports how many were delivered.
func runDelayed(b *testing.B, lead time.Duration) {
	b.Helper()
	eng, top, sub, cancel := benchTopic(b, TopologyConfig{
		Kind:             TopologyQueue,
		Partitions:       1,
		CapacityMessages: retryCapacity,
		AckDeadline:      -1,
		Overflow:         OverflowReject,
	})
	defer cancel()
	defer func() { _ = eng.Close() }()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var settled atomic.Int64
	var wg sync.WaitGroup
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		b.Fatalf("Attach: %v", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dst := make([]Claim[int64], 32)
		for {
			got, err := c.Fetch(ctx, dst)
			if err != nil {
				return
			}
			for j := range got {
				if err := c.Ack(dst[j].Env, dst[j].Gen); err != nil {
					stop()
					return
				}
			}
			settled.Add(int64(got))
		}
	}()
	var refused int64
	var failure error
	i := int64(0)
	for b.Loop() {
		i++
		err := top.Publish(ctx, Publish[int64]{Payload: i, Size: 64, Partition: 0, Delay: lead})
		if err == nil {
			continue
		}
		if errors.Is(err, errcode.ErrQueueFull) {
			refused++
			runtime.Gosched()
			continue
		}
		failure = err
		break
	}
	b.StopTimer()
	stop()
	wg.Wait()
	if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, errcode.ErrClosed) {
		b.Fatalf("publish: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(settled.Load())/secs, "msgs/s")
	}
	if n := b.N; n > 0 {
		b.ReportMetric(float64(refused)/float64(n), "refused/op")
	}
	_ = sub
}

// benchInflight is the number of leases the expiry sweep walks.
func benchInflight() []int { return []int{1, 64} }

// runLeaseExpiry measures the sweep that reclaims expired leases and returns
// their records to the lane: b.N sweeps over inflight leases, each of which
// is redelivered and acknowledged.
func runLeaseExpiry(b *testing.B, inflight int) {
	b.Helper()
	eng, top, sub, cancel := benchTopic(b, TopologyConfig{
		Kind:             TopologyQueue,
		Partitions:       1,
		CapacityMessages: retryCapacity,
		// The lease is already over when it is taken, so a sweep reclaims
		// every one of them without the benchmark waiting on a clock.
		AckDeadline: time.Nanosecond,
		Overflow:    OverflowReject,
	})
	defer cancel()
	defer func() { _ = eng.Close() }()
	ctx := context.Background()
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		b.Fatalf("Attach: %v", err)
	}
	for i := range inflight {
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 64, Partition: 0}); err != nil {
			b.Fatalf("publish: %v", err)
		}
	}
	dst := make([]Claim[int64], inflight)
	reclaimed := int64(0)
	var failure error
	for b.Loop() {
		n, err := c.Fetch(ctx, dst)
		if err != nil {
			failure = err
			break
		}
		if n == 0 {
			continue
		}
		reclaimed += int64(eng.Faults().ExpireLeases())
	}
	b.StopTimer()
	if failure != nil {
		b.Fatalf("fetch: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(reclaimed)/secs, "msgs/s")
	}
	if n := b.N; n > 0 {
		b.ReportMetric(float64(reclaimed)/float64(n), "expired/op")
	}
	_ = sub
}

// benchOverflowCaps is the lane capacity the overflow benchmark runs at: a
// shallow one, so that the publisher outruns its consumer and the policies
// have something to decide, and a deep one where none of them does.
func benchOverflowCaps() []int { return []int{8, 1024} }

// runOverflow measures publication into a lane that is kept at its capacity
// by a consumer slower than the publisher, under one overflow policy.
func runOverflow(b *testing.B, policy OverflowPolicy, capacity int) {
	b.Helper()
	eng, top, sub, cancel := benchTopic(b, TopologyConfig{
		Kind:             TopologyQueue,
		Partitions:       1,
		CapacityMessages: capacity,
		AckDeadline:      -1,
		Overflow:         policy,
	})
	defer cancel()
	defer func() { _ = eng.Close() }()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var settled atomic.Int64
	var wg sync.WaitGroup
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		b.Fatalf("Attach: %v", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dst := make([]Claim[int64], 8)
		for {
			got, err := c.Fetch(ctx, dst)
			if err != nil {
				return
			}
			for j := range got {
				if err := c.Ack(dst[j].Env, dst[j].Gen); err != nil {
					stop()
					return
				}
			}
			settled.Add(int64(got))
		}
	}()
	var refused int64
	var failure error
	i := int64(0)
	for b.Loop() {
		i++
		err := top.Publish(ctx, Publish[int64]{Payload: i, Size: 64, Partition: 0})
		if err == nil {
			continue
		}
		if errors.Is(err, errcode.ErrQueueFull) {
			refused++
			runtime.Gosched()
			continue
		}
		failure = err
		break
	}
	b.StopTimer()
	stop()
	wg.Wait()
	if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, errcode.ErrClosed) {
		b.Fatalf("publish: %v", failure)
	}
	snap := sub.Inspect()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(settled.Load())/secs, "msgs/s")
	}
	if n := b.N; n > 0 {
		b.ReportMetric(float64(refused)/float64(n), "refused/op")
		b.ReportMetric(float64(snap.Overflowed+snap.Dropped)/float64(n), "lost/op")
	}
}

// benchTopic builds an engine with one topic and one subscription for the
// slice-3 benchmark arms.
func benchTopic(b *testing.B, cfg TopologyConfig) (*Engine[int64], *Topic[int64], *Subscription[int64], func()) {
	b.Helper()
	eng, err := New(Config[int64]{
		// The architecture is pinned, not left to ArchAuto: nothing in this
		// benchmark's name says which one it ran on, and the strict-ordering
		// arms cannot run at all on an architecture that does not order what
		// it holds -- CreateTopic refuses strict ordering there, so the rows would silently disappear from the family's
		// grid if the bake-off bound a shared log.
		Kinds:           KindSet{Arch: featureArch, Queue: queue.MutexRing},
		Clone:           func(v int64) int64 { return v },
		DefaultTopology: cfg,
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	top, err := eng.CreateTopic("bench", TopologyConfig{})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	return eng, top, sub, func() {}
}

// TestRetryBenchHarness checks the slice-3 benchmark harnesses themselves, so
// that a broken one fails the test suite rather than reporting a meaningless
// number or wedging a benchmark run. Every arm is capped to harnessIters
// publications: what is under test is the harness.
func TestRetryBenchHarness(t *testing.T) {
	capBenchTime(t, harnessIters)
	run := func(t *testing.T, fn func(*testing.B)) {
		t.Helper()
		r := testing.Benchmark(fn)
		if r.N == 0 {
			t.Fatalf("benchmark did not run: %+v", r)
		}
		if _, ok := r.Extra["msgs/s"]; !ok {
			t.Fatalf("no msgs/s metric: %+v", r.Extra)
		}
	}
	for _, k := range benchRetryLanes() {
		for _, o := range benchOrderings() {
			t.Run(fmt.Sprintf("OrderedRetry/%v/ordering=%v", k, o), func(t *testing.T) {
				run(t, func(b *testing.B) { runOrderedRetry(b, k, o, 50) })
			})
		}
	}
	t.Run("Delayed", func(t *testing.T) {
		run(t, func(b *testing.B) { runDelayed(b, time.Millisecond) })
	})
	t.Run("LeaseExpiry", func(t *testing.T) {
		run(t, func(b *testing.B) { runLeaseExpiry(b, 8) })
	})
	for _, p := range OverflowPolicies() {
		t.Run("Overflow/"+p.String(), func(t *testing.T) {
			run(t, func(b *testing.B) { runOverflow(b, p, 8) })
		})
	}
}

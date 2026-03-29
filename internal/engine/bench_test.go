package engine

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/errcode"
	"bitbucket.org/iqhive/iqmq/internal/pool"
	"bitbucket.org/iqhive/iqmq/internal/queue"
)

// The fan-out benchmark of the performance budgets layer 2 and /. The
// canonical name is
//
//	BenchmarkMemory/FanOut/<subscriptions>/arch=<kind>/policy=<kind>/slow=<n>
//
// with the dimensions in that fixed order: the number of subscriptions one
// publication fans out to (the specification asks for a cost linear in subscriptions and
// no additional body copies), the storage architecture, the admission
// policy and the number of subscriptions that never consume — the
// slow-subscriber arm, which is where the architectures and the policies part
// company. IQMQ_BENCH_MATRIX=full widens the sweep.

// fanoutSubs is the fan-out width swept.
func fanoutSubs() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 4, 16, 64, 256}
	}
	return []int{1, 4, 16, 64}
}

// benchArches and benchPolicies are the kinds swept. Every kind of both
// families is measured in the default profile: the bake-off (the specification) decides
// between them and cannot decide on kinds it never saw.
func benchArches() []Arch { return Arches() }

func benchPolicies() []FanoutPolicy { return FanoutPolicies() }

// benchSlow is the number of subscriptions that never consume.
func benchSlow() []int { return []int{0, 1} }

// fanoutCapacity is the per-subscription lane capacity of the benchmark. It
// is large enough that a fan-out with no slow subscriber never fills, and
// small enough that one that does fills quickly.
const fanoutCapacity = 1024

// BenchmarkMemory is the engine's own layer-2 macro benchmark.
func BenchmarkMemory(b *testing.B) {
	b.Run("FanOut", func(b *testing.B) {
		for _, n := range fanoutSubs() {
			for _, a := range benchArches() {
				for _, p := range benchPolicies() {
					for _, slow := range benchSlow() {
						if slow >= n {
							continue
						}
						name := fmt.Sprintf("%d/arch=%v/policy=%v/slow=%d", n, a, p, slow)
						b.Run(name, func(b *testing.B) { runFanOut(b, n, a, p, slow) })
					}
				}
			}
		}
	})
	b.Run("PubSubAck", func(b *testing.B) {
		for _, arm := range copyArms() {
			for _, rc := range copyRuns() {
				b.Run(arm.slug+"/"+rc.name(), func(b *testing.B) {
					runPubSubAck(b, arm, rc.sh, rc.size, rc.recycle)
				})
			}
		}
	})
	b.Run("OrderedRetry", func(b *testing.B) {
		for _, k := range benchRetryLanes() {
			for _, o := range benchOrderings() {
				for _, rate := range benchFailRates() {
					name := fmt.Sprintf("%v/ordering=%v/failrate=%d", k, o, rate)
					b.Run(name, func(b *testing.B) { runOrderedRetry(b, k, o, rate) })
				}
			}
		}
	})
	b.Run("Delayed", func(b *testing.B) {
		for _, lead := range benchLeads() {
			b.Run(fmt.Sprintf("lead=%v", lead), func(b *testing.B) { runDelayed(b, lead) })
		}
	})
	b.Run("LeaseExpiry", func(b *testing.B) {
		for _, n := range benchInflight() {
			b.Run(fmt.Sprintf("inflight=%d", n), func(b *testing.B) { runLeaseExpiry(b, n) })
		}
	})
	b.Run("RetentionReplay", func(b *testing.B) {
		for _, r := range benchRetentions() {
			for _, pos := range benchStarts() {
				name := fmt.Sprintf("retained=%d/start=%v", r, pos)
				b.Run(name, func(b *testing.B) { runRetentionReplay(b, r, pos) })
			}
		}
	})
	b.Run("CloseUnderLoad", func(b *testing.B) {
		for _, n := range benchLoaders() {
			for _, inflight := range benchCloseInflight() {
				name := fmt.Sprintf("publishers=%d/inflight=%d", n, inflight)
				b.Run(name, func(b *testing.B) { runCloseUnderLoad(b, n, inflight) })
			}
		}
	})
	b.Run("Latency", func(b *testing.B) {
		b.Run("closed", runLatencyClosed)
		for _, r := range benchRates() {
			b.Run(fmt.Sprintf("open/rate=%d", r), func(b *testing.B) { runLatencyOpen(b, r) })
		}
	})
	b.Run("Overflow", func(b *testing.B) {
		for _, p := range OverflowPolicies() {
			for _, n := range benchOverflowCaps() {
				b.Run(fmt.Sprintf("%v/capacity=%d", p, n), func(b *testing.B) { runOverflow(b, p, n) })
			}
		}
	})
}

// runFanOut drives b.N publications through one topic with n subscriptions,
// of which the first slow ones never consume. The loop is closed on the
// subscriptions that do consume: a publication that no subscription can take
// is counted and the publisher moves on, so the arm where a slow subscriber
// blocks the topic measures how fast the engine refuses rather than
// deadlocking.
func runFanOut(b *testing.B, n int, a Arch, p FanoutPolicy, slow int) {
	b.Helper()
	eng, err := New(Config[int64]{
		Kinds: KindSet{Arch: a, Queue: queue.MutexRing},
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Kind:             TopologyTopic,
			Fanout:           p,
			Partitions:       1,
			CapacityMessages: fanoutCapacity,
			AckDeadline:      -1,
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var settled atomic.Int64
	var wg sync.WaitGroup
	for i := range n {
		sub, err := top.Subscribe(fmt.Sprintf("s%d", i), SubscriptionConfig{})
		if err != nil {
			b.Fatalf("Subscribe: %v", err)
		}
		if i < slow {
			continue // the slow subscriber attaches nothing and never drains
		}
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
						b.Errorf("ack: %v", err)
						cancel()
						return
					}
				}
				settled.Add(int64(got))
			}
		}()
	}

	// The loop body stays free of b.Fatalf and of an early return: go1.27.0
	// crashes in escape analysis (types.(*Type).HasPointers on a nil type)
	// when a b.Loop() body of this function also contains one, so the
	// publisher records what went wrong and the benchmark reports it after
	// the loop.
	var refused, partial int64
	var failure error
	i := int64(0)
	for b.Loop() {
		i++
		err := top.Publish(ctx, Publish[int64]{Payload: i, Size: 64, Partition: 0})
		if err == nil {
			continue
		}
		if errors.Is(err, errcode.ErrPartial) {
			// The Independent policy admitted the record to the
			// subscriptions with room and refused it for the rest. The
			// record was admitted, so this is not a refusal, and the
			// publisher goes on without yielding.
			partial++
			continue
		}
		if errors.Is(err, errcode.ErrQueueFull) {
			refused++
			// The consumers are draining as fast as they can; yielding keeps
			// a full topic from spinning a core on refusals.
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
		b.ReportMetric(float64(refused)/float64(n), "refused/op")
		b.ReportMetric(float64(partial)/float64(n), "partial/op")
	}
}

// harnessIters is how many publications each harness run makes. The harness
// is a correctness test, not a measurement, and it runs inside `go test` on a
// shared machine: a run left at the default benchmark time would saturate
// every core for a second, eight times over, on every `make ci` — enough to
// push another agent's guarded benchmark to verdict=contended.
const harnessIters = "50x"

// capBenchTime holds testing.Benchmark's iteration count to v for the rest of
// the test. The flag is what testing.Benchmark reads, there is no per-call
// setting, and `go test` runs the tests before the benchmarks, so restoring it
// on cleanup leaves a real benchmark run with the time the caller asked for.
func capBenchTime(t *testing.T, v string) {
	t.Helper()
	f := flag.Lookup("test.benchtime")
	if f == nil {
		return // not a test binary built by `go test`; leave the default
	}
	old := f.Value.String()
	if err := f.Value.Set(v); err != nil {
		t.Fatalf("benchtime %s: %v", v, err)
	}
	t.Cleanup(func() {
		if err := f.Value.Set(old); err != nil {
			t.Errorf("restoring benchtime %s: %v", old, err)
		}
	})
}

// TestFanOutBenchHarness checks the benchmark harness itself, so that a
// broken harness fails the test suite rather than reporting a meaningless
// number or wedging a benchmark run. The slow-subscriber arm is included
// because that is where a policy that cannot refuse would deadlock. Each run
// is capped to harnessIters publications: what is under test is the harness,
// and the numbers it produces here are never read.
func TestFanOutBenchHarness(t *testing.T) {
	capBenchTime(t, harnessIters)
	// Every architecture and every policy is covered, paired rather than
	// crossed: a benchmark run costs a second of wall clock, and the
	// differential tests already cross the two families.
	arches, policies := Arches(), FanoutPolicies()
	type run struct {
		arch   Arch
		policy FanoutPolicy
		slow   int
	}
	runs := make([]run, 0, len(arches)+len(policies))
	for i, a := range arches {
		runs = append(runs, run{a, policies[i%len(policies)], 0})
	}
	for i, p := range policies {
		runs = append(runs, run{arches[i%len(arches)], p, 1})
	}
	for _, rc := range runs {
		t.Run(fmt.Sprintf("arch=%v/policy=%v/slow=%d", rc.arch, rc.policy, rc.slow), func(t *testing.T) {
			r := testing.Benchmark(func(b *testing.B) { runFanOut(b, 4, rc.arch, rc.policy, rc.slow) })
			if r.N == 0 {
				t.Fatalf("benchmark did not run: %+v", r)
			}
			if _, ok := r.Extra["msgs/s"]; !ok {
				t.Fatalf("no msgs/s metric: %+v", r.Extra)
			}
		})
	}
}

//
//	BenchmarkMemory/RetentionReplay/retained=<n>/start=<position>
//	BenchmarkMemory/CloseUnderLoad/publishers=<n>/inflight=<n>
//
// RetentionReplay measures what a replay costs per record: the retained log
// is filled once and every iteration seeks the subscription back to a start
// position, which re-admits the retained records. The per-operation cost is
// therefore the cost of one Seek over a log of the swept depth, and the
// msgs/s metric reports the records that replay moved.
//
// CloseUnderLoad measures the shutdown path of the message contract with work
// in flight: publishers and consumers are running when Close is called, and
// each iteration builds a loaded engine and closes it. It is the benchmark
// that would show a close whose cost grows with the number of records it has
// to release, and it is where a close that forgets a goroutine shows up as a
// leak rather than as a number.

// benchRetentions is the retained-log depth swept.
func benchRetentions() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{16, 256, 4096, 65536}
	}
	return []int{16, 256, 4096}
}

// benchStarts are the replay positions swept: from the earliest retained
// record and from the middle of the log, which is where the position has to
// be searched for rather than being the whole log.
func benchStarts() []StartKind { return []StartKind{StartEarliest, StartAtSeq} }

// benchLoaders is the number of publisher goroutines running when the engine
// is closed.
func benchLoaders() []int {
	if os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return []int{1, 4, 16}
	}
	return []int{1, 8}
}

// benchCloseInflight is the number of records left leased when the engine is
// closed, which is what its drain has to reclaim.
func benchCloseInflight() []int { return []int{0, 64} }

// runRetentionReplay fills a stream's retained log once and measures one Seek
// per iteration.
func runRetentionReplay(b *testing.B, retained int, pos StartKind) {
	b.Helper()
	eng, err := New(Config[int64]{
		Kinds: KindSet{Queue: queue.MutexRing, Pool: benchPool()},
		DefaultTopology: TopologyConfig{
			Partitions:       1,
			CapacityMessages: retained * 2,
			AckDeadline:      -1,
		},
	})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()
	top, err := eng.CreateTopic("bench", TopologyConfig{
		Kind:      TopologyStream,
		Retention: Retention{MaxMessages: retained},
	})
	if err != nil {
		b.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		b.Fatalf("Subscribe: %v", err)
	}
	ctx := context.Background()
	for i := range retained {
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 64, Partition: 0}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
	start := StartPosition{Kind: pos}
	if pos == StartAtSeq {
		// Half the log: the position has to be found rather than being the
		// whole of it.
		start.Seq = uint64(retained / 2)
	}
	var moved int64
	var failure error
	for b.Loop() {
		n, err := sub.Seek(start)
		if err != nil && failure == nil {
			failure = err
		}
		moved += int64(n)
	}
	b.StopTimer()
	if failure != nil {
		b.Fatalf("Seek: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(moved)/secs, "msgs/s")
	}
	b.ReportMetric(float64(moved)/float64(b.N), "replayed/op")
}

// runCloseUnderLoad builds a loaded engine and closes it, once per iteration.
// The build is not timed: what is measured is Close with publishers blocked
// on capacity, records in flight and a consumer attached.
func runCloseUnderLoad(b *testing.B, publishers, inflight int) {
	b.Helper()
	dst := make([]Claim[int64], inflight+1)
	for b.Loop() {
		b.StopTimer()
		eng, wg := loadedEngine(b, publishers, inflight, dst)
		b.StartTimer()
		if err := eng.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
		b.StopTimer()
		// Every parked publisher must come back: a close that leaves one
		// waiting is a leak the benchmark would otherwise hide, and the next
		// iteration would run alongside it.
		wg.Wait()
		b.StartTimer()
	}
}

// loadedEngine returns an engine with publishers parked on a full lane and
// records in flight, together with the group the publishers report to.
func loadedEngine(b *testing.B, publishers, inflight int, dst []Claim[int64]) (
	*Engine[int64], *sync.WaitGroup,
) {
	b.Helper()
	eng, err := New(Config[int64]{
		Kinds: KindSet{Queue: queue.MutexRing, Pool: benchPool()},
		DefaultTopology: TopologyConfig{
			Partitions:       1,
			CapacityMessages: inflight + 32,
			AckDeadline:      -1,
			Overflow:         OverflowBlock,
		},
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
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		b.Fatalf("Attach: %v", err)
	}
	ctx := context.Background()
	// Fill the lane, then lease part of it: the drain has both a ready set
	// and an in-flight list to reclaim.
	for i := range inflight + 32 {
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 64, Partition: 0}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
	if inflight > 0 {
		if _, err := c.Fetch(ctx, dst[:inflight]); err != nil {
			b.Fatalf("Fetch: %v", err)
		}
	}
	// The publishers now park on a full lane under OverflowBlock; Close is
	// what releases them.
	var wg sync.WaitGroup
	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = top.Publish(ctx, Publish[int64]{Payload: -1, Size: 64, Partition: 0})
		}()
	}
	// A publisher counts as parked only once it is inside Topic.waitRoom,
	// which the subscription's own room-waiter gauge reports. Counting it
	// before the Publish call — as this handshake first did — let the arm
	// measure a close whose publishers had not yet reached the blocking
	// path, which is the thing the arm exists to measure.
	for sub.room.waiting.Load() < int64(publishers) {
		runtime.Gosched()
	}
	return eng, &wg
}

// benchPool is the pool kind the slice-4 benchmarks are assembled with: the
// retaining kind, so that what is measured is the engine rather than the
// allocator.
func benchPool() pool.Kind { return pool.FreeList }

//
//	BenchmarkMemory/Latency/closed
//	BenchmarkMemory/Latency/open/rate=<percent>
//
// closed is the closed-loop measurement: one publication, one fetch and one
// acknowledgement per iteration, so the engine paces the sender and what the
// quantiles describe is the service time of an unloaded engine.
//
// open is the open-loop one, which is the measurement the specification's "p99 < 20 µs"
// is about. Its publications are paced at a fraction of the capacity measured
// just before the run, and each record's latency is taken from the time it
// was *due* to be sent rather than from the time it was sent: a sender that
// waits for the engine before offering the next record reports queueing delay
// as zero, which is the coordinated omission the specification B names, and it is
// at and above capacity that the difference is the whole answer. Both arms
// report p50-ns, p99-ns and p999-ns.

// benchRates are the offered rates, as a percentage of the measured capacity.
// The overloaded arm is the point of the sweep: it is the one where a queue
// forms and where the difference between the two loops is visible.
func benchRates() []int { return []int{50, 80, 100, 120} }

// latencyCapacity is what the rig sustains with one publisher offering
// records as fast as it can and one consumer draining them, in records per
// second, measured once per process. It is a pacing reference rather than a
// result: the open loop needs a rate to offer, it needs the same one for
// every arm of one run, and it has to be the rate of the *pipeline* — a
// publisher and a consumer that take turns on one goroutine measure
// something the two of them running at once never see.
var latencyCapacity = sync.OnceValue(func() float64 {
	const probe = 1 << 14
	eng, top, _, c := latencyRig(nil)
	defer func() { _ = eng.Close() }()
	samples := make([]int64, probe)
	start := time.Now()
	done := drainLatency(c, samples, nil)
	ctx := context.Background()
	for i := range probe {
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 64, Partition: 0}); err != nil {
			return 0
		}
	}
	<-done
	secs := time.Since(start).Seconds()
	if secs <= 0 {
		return 0
	}
	return probe / secs
})

// drainLatency starts the consumer side of the latency rig: it fetches and
// acknowledges until it has seen one record per sample slot, timing each one
// from its due time when it has one. The channel it returns is closed when it
// is done.
func drainLatency(c *Consumer[int64], samples []int64, due []time.Time) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx := context.Background()
		dst := make([]Claim[int64], 64)
		for got := 0; got < len(samples); {
			m, err := c.Fetch(ctx, dst)
			if err != nil {
				return
			}
			at := time.Now()
			for j := range m {
				if i := dst[j].Payload; due != nil && int(i) < len(due) {
					samples[i] = int64(at.Sub(due[i]))
				}
				if err := c.Ack(dst[j].Env, dst[j].Gen); err != nil {
					return
				}
			}
			got += m
		}
	}()
	return done
}

// latencyRig builds the one-publisher, one-consumer engine both arms measure.
// Its lane is large enough that the overloaded arm queues rather than being
// refused: an open loop that offers more than the engine takes is measuring
// the queue, and a refusal would take the record out of the measurement
// instead of showing its delay. b may be nil, for the capacity probe.
func latencyRig(b *testing.B) (*Engine[int64], *Topic[int64], *Subscription[int64], *Consumer[int64]) {
	fail := func(format string, args ...any) {
		if b != nil {
			b.Fatalf(format, args...)
		}
		panic(fmt.Sprintf(format, args...))
	}
	eng, err := New(Config[int64]{
		Kinds: KindSet{Queue: queue.MutexRing, Pool: benchPool()},
		// The rig follows the architecture the bake-off bound, and a copying
		// one is refused outright without a Clone (Topic.publish): without
		// this the whole latency arm panicked in New the moment ArchAuto
		// resolved to FullCopy. The hook is never called under the
		// architectures that share a record.
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions:       1,
			CapacityMessages: 1 << 20,
			AckDeadline:      -1,
		},
	})
	if err != nil {
		fail("New: %v", err)
	}
	top, err := eng.CreateTopic("bench", TopologyConfig{})
	if err != nil {
		fail("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		fail("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		fail("Attach: %v", err)
	}
	return eng, top, sub, c
}

// runLatencyClosed measures one publish/fetch/acknowledge round trip per
// iteration.
func runLatencyClosed(b *testing.B) {
	b.Helper()
	eng, top, _, c := latencyRig(b)
	defer func() { _ = eng.Close() }()
	ctx := context.Background()
	dst := make([]Claim[int64], 1)
	samples := make([]int64, 0, 1024)
	for b.Loop() {
		at := time.Now()
		if err := top.Publish(ctx, Publish[int64]{Payload: 1, Size: 64, Partition: 0}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		n, err := c.Fetch(ctx, dst)
		if err != nil || n != 1 {
			b.Fatalf("Fetch = %d, %v", n, err)
		}
		samples = append(samples, int64(time.Since(at)))
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			b.Fatalf("Ack: %v", err)
		}
	}
	b.StopTimer()
	reportLatency(b, samples)
}

// runLatencyOpen offers records at pct per cent of the measured capacity and
// measures each one from the moment it was due to be sent.
func runLatencyOpen(b *testing.B, pct int) {
	b.Helper()
	rate := latencyCapacity() * float64(pct) / 100
	if rate <= 0 {
		b.Fatal("the capacity probe measured nothing to pace against")
	}
	interval := time.Duration(float64(time.Second) / rate)
	eng, top, _, c := latencyRig(b)
	defer func() { _ = eng.Close() }()
	ctx := context.Background()
	n := b.N
	// The schedule is written before the consumer exists, so what the
	// consumer reads is what the publisher intended and neither goroutine
	// waits for the other to say so.
	due := make([]time.Time, n)
	samples := make([]int64, n)
	start := time.Now().Add(interval)
	for i := range due {
		due[i] = start.Add(time.Duration(i) * interval)
	}
	done := drainLatency(c, samples, due)
	b.ResetTimer()
	for i := range n {
		waitUntil(due[i])
		if err := top.Publish(ctx, Publish[int64]{Payload: int64(i), Size: 64, Partition: 0}); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
	<-done
	b.StopTimer()
	reportLatency(b, samples)
	b.ReportMetric(rate, "offered/s")
}

// waitUntil parks until at. The intervals an open loop paces at are shorter
// than the kernel can sleep for — a whole run is a couple of microseconds per
// record — so a sleep is used only for the part of the wait that is long
// enough to be one, and the tail is spun out. A sender that cannot keep to
// the schedule reports its own lateness as the engine's latency, which is the
// one thing this measurement must not do.
func waitUntil(at time.Time) {
	const spin = 100 * time.Microsecond
	for {
		d := time.Until(at)
		switch {
		case d <= 0:
			return
		case d > spin:
			time.Sleep(d - spin)
		}
	}
}

func reportLatency(b *testing.B, samples []int64) {
	b.Helper()
	if len(samples) == 0 {
		return
	}
	slices.Sort(samples)
	q := func(p float64) float64 {
		return float64(samples[int(p*float64(len(samples)-1))])
	}
	b.ReportMetric(q(0.50), "p50-ns")
	b.ReportMetric(q(0.99), "p99-ns")
	b.ReportMetric(q(0.999), "p999-ns")
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(len(samples))/secs, "msgs/s")
	}
}

// TestLatencyBenchHarness checks the latency harness, and with it the specification's own gate: the open-loop arm must emit p99-ns. A benchmark that stopped
// reporting it would otherwise fail the row's gate rather than the suite, and
// only on a machine quiet enough to run benchmarks on.
func TestLatencyBenchHarness(t *testing.T) {
	capBenchTime(t, harnessIters)
	for _, arm := range []struct {
		name string
		run  func(b *testing.B)
	}{
		{"closed", runLatencyClosed},
		{"open/rate=50", func(b *testing.B) { runLatencyOpen(b, 50) }},
		{"open/rate=120", func(b *testing.B) { runLatencyOpen(b, 120) }},
	} {
		t.Run(arm.name, func(t *testing.T) {
			r := testing.Benchmark(arm.run)
			if r.N == 0 {
				t.Fatalf("benchmark did not run: %+v", r)
			}
			for _, m := range []string{"p50-ns", "p99-ns", "p999-ns"} {
				if _, ok := r.Extra[m]; !ok {
					t.Fatalf("no %s metric: %+v", m, r.Extra)
				}
			}
		})
	}
}

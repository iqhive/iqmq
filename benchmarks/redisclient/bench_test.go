package redisclient_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/streams"

	"github.com/iqhive/iqmq/benchmarks/redisclient"
)

// BenchmarkRedisClient/<kind>/<op>/<dim>= measures one client library's cost
// for each of the operations a Redis Streams driver actually performs
//
// the criterion is "throughput/connection & allocs", and both halves are
// the client library's: the server underneath is the same in-process
// `fakestreams` behind `respsrv` for both arms, reached over loopback TCP, so
// the difference between two rows is the library's command building, its
// connection model, its pipelining and its reply parsing — and nothing else.
// allocs/op is in the criterion because a driver that allocates per command
// allocates per MESSAGE, which the hot-path rules bound.
//
// The five operations are own list:
//
//	publish   one XADD, the publish hot path
//	pipeline  a batch of XADDs at three depths: the "pipelining depth sweep",
//	          which is where the two connection models differ most — rueidis
//	          multiplexes every command onto one connection whether it was
//	          asked to or not, go-redis pipelines only when told
//	consume   XREADGROUP over this consumer's own pending entries, which is
//	          the fetch hot path AND the largest reply the driver parses
//	reclaim XAUTOCLAIM over a pending-entries list of 10,000, which is the specification's "reclaim under 10k PEL"
//	block     XREADGROUP with BLOCK where an entry is already available: the
//	          blocking path's round trip, which rueidis serves from a
//	          connection pool of its own and go-redis from the shared one
//
// Every row's ns/op is one round trip to a server in the same process, so the
// absolute figures are far below what a network gives; what the family is
// decided on is the RATIO between two arms measured the same way, and the
// driver page says so.
func BenchmarkRedisClient(b *testing.B) {
	for _, k := range redisclient.Kinds() {
		b.Run(k.String(), func(b *testing.B) {
			b.Run("publish", func(b *testing.B) {
				b.Run("depth=1", func(b *testing.B) { benchPublish(b, k) })
			})
			b.Run("pipeline", func(b *testing.B) {
				for _, depth := range benchDepths() {
					b.Run("depth="+strconv.Itoa(depth), func(b *testing.B) { benchPipeline(b, k, depth) })
				}
			})
			b.Run("consume", func(b *testing.B) {
				for _, count := range benchCounts() {
					b.Run("count="+strconv.Itoa(count), func(b *testing.B) { benchConsume(b, k, count) })
				}
			})
			b.Run("reclaim", func(b *testing.B) {
				for _, pel := range benchPELs() {
					b.Run("pel="+strconv.Itoa(pel), func(b *testing.B) { benchReclaim(b, k, pel) })
				}
			})
			b.Run("block", func(b *testing.B) {
				b.Run("depth=1", func(b *testing.B) { benchBlock(b, k) })
			})
		})
	}
}

// benchDepths is the pipelining depth sweep. One is in it deliberately: the
// crossover between "a pipeline of one" and a plain command is where an
// auto-pipelining client stops paying for the batch machinery.
func benchDepths() []int {
	if fullMatrix() {
		return []int{1, 8, 64, 256}
	}
	return []int{1, 8, 64}
}

// benchCounts is how many entries one fetch reads.
func benchCounts() []int {
	if fullMatrix() {
		return []int{1, 16, 128}
	}
	return []int{1, 16}
}

// benchPELs is the pending-entries-list size the reclaim sweep runs over.
// 10,000 is own figure.
func benchPELs() []int {
	if fullMatrix() {
		return []int{100, 10000}
	}
	return []int{10000}
}

// fullMatrix reports whether the campaign asked for the sweep.
func fullMatrix() bool {
	switch benchMatrix() {
	case "full", "huge":
		return true
	}
	return false
}

// benchPublish is one XADD per iteration.
func benchPublish(b *testing.B, k redisclient.Kind) {
	a := newArm(b, k)
	ctx := context.Background()
	cmd := a.xadd("payload")
	if _, err := a.client.Do(ctx, cmd); err != nil {
		b.Fatalf("XADD: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := a.client.Do(ctx, cmd); err != nil {
			b.Fatalf("XADD: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(1, "cmds/op")
}

// benchPipeline is one DoMulti of depth XADDs per iteration.
//
// ns/op is the BATCH's cost, and ns/cmd beside it is the per-command cost the
// depths are compared on: a pipeline of 64 that is 64 times the cost of one
// command is not a pipeline.
func benchPipeline(b *testing.B, k redisclient.Kind, depth int) {
	a := newArm(b, k)
	ctx := context.Background()
	cmds := make([]streams.Cmd, depth)
	for i := range cmds {
		cmds[i] = a.xadd("payload-" + strconv.Itoa(i))
	}
	results, err := a.client.DoMulti(ctx, cmds)
	if err != nil {
		b.Fatalf("DoMulti: %v", err)
	}
	for i, res := range results {
		if res.Err != nil {
			b.Fatalf("DoMulti command %d: %v", i, res.Err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := a.client.DoMulti(ctx, cmds); err != nil {
			b.Fatalf("DoMulti: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*depth), "ns/cmd")
	b.ReportMetric(float64(depth), "cmds/op")
}

// benchConsume is one XREADGROUP per iteration, reading this consumer's own
// pending entries.
//
// Reading the pending list rather than new entries is what makes the benchmark
// repeatable: `>` delivers each entry once, so a loop over new entries
// measures a full stream for the first few iterations and an empty one for the
// rest. Reading from "0" returns the same count every time, which is the
// reply-parsing cost the fetch path pays per message.
func benchConsume(b *testing.B, k redisclient.Kind, count int) {
	a := newArm(b, k)
	ctx := context.Background()
	seed(b, a, count)
	read, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: a.group, Consumer: benchName, Keys: []string{a.stream()},
		IDs: []string{streams.IDZero}, Count: count,
	})
	if err != nil {
		b.Fatalf("XReadGroup: %v", err)
	}
	v, err := a.client.Do(ctx, read)
	if err != nil {
		b.Fatalf("XREADGROUP: %v", err)
	}
	entries, err := streams.ParseXReadGroup(v, a.keys.Shard, nil)
	if err != nil {
		b.Fatalf("ParseXReadGroup: %v", err)
	}
	if len(entries) != count {
		b.Fatalf("the seeded pending list returned %d entries, want %d", len(entries), count)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := a.client.Do(ctx, read); err != nil {
			b.Fatalf("XREADGROUP: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(1, "cmds/op")
	b.ReportMetric(float64(count), "entries/op")
}

// benchReclaim is one XAUTOCLAIM round per iteration over a pending-entries
// list of pel entries, which is the mandatory reclaim sweep's cost.
//
// min-idle is zero so that every iteration claims the same 128 entries from
// the same cursor: the fake's clock is frozen, so an idle filter would make
// the reply size depend on how long the setup took.
func benchReclaim(b *testing.B, k redisclient.Kind, pel int) {
	a := newArm(b, k)
	ctx := context.Background()
	seed(b, a, pel)
	claim, err := streams.XAutoClaim(streams.XAutoClaimArgs{
		Key: a.stream(), Group: a.group, Consumer: "sweeper",
		Start: streams.IDZero, Count: 128,
	})
	if err != nil {
		b.Fatalf("XAutoClaim: %v", err)
	}
	if _, err := a.client.Do(ctx, claim); err != nil {
		b.Fatalf("XAUTOCLAIM: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := a.client.Do(ctx, claim); err != nil {
			b.Fatalf("XAUTOCLAIM: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(1, "cmds/op")
	b.ReportMetric(float64(pel), "pel/op")
}

// benchBlock is one XREADGROUP with BLOCK per iteration, with an entry already
// available so that the server answers at once.
//
// It measures the BLOCKING PATH's round trip rather than the block itself: a
// blocking command is a different code path in both libraries — rueidis routes
// it to a connection pool of its own so that it cannot stall the multiplexed
// one, and go-redis serves it from the shared pool under a read timeout the
// driver had to widen  — and that path is what every idle
// consumer in a deployment sits in.
func benchBlock(b *testing.B, k redisclient.Kind) {
	a := newArm(b, k)
	ctx := context.Background()
	seed(b, a, 1)
	read, err := streams.XReadGroup(streams.XReadGroupArgs{
		Group: a.group, Consumer: benchName, Keys: []string{a.stream()},
		IDs: []string{streams.IDZero}, Count: 1, Block: 50 * time.Millisecond,
	})
	if err != nil {
		b.Fatalf("XReadGroup: %v", err)
	}
	if !read.Blocking {
		b.Fatal("the command is not marked Blocking, so this benchmark is not measuring the blocking path")
	}
	if _, err := a.client.Do(ctx, read); err != nil {
		b.Fatalf("XREADGROUP: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := a.client.Do(ctx, read); err != nil {
			b.Fatalf("XREADGROUP: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(1, "cmds/op")
}

// seed publishes n entries and delivers all of them to benchName, so that the
// consumer's pending list holds exactly n.
//
// It publishes in pipelined chunks because it is setup and not measurement:
// ten thousand round trips one at a time is the difference between a
// four-second benchmark and a sub-second one, and the harness's per-benchmark
// ceiling counts one ITERATION rather than the setup.
func seed(tb testing.TB, a *arm, n int) {
	tb.Helper()
	ctx := context.Background()
	const chunk = 256
	for start := 0; start < n; start += chunk {
		end := min(start+chunk, n)
		cmds := make([]streams.Cmd, 0, end-start)
		for i := start; i < end; i++ {
			cmds = append(cmds, a.xadd("payload-"+strconv.Itoa(i)))
		}
		results, err := a.client.DoMulti(ctx, cmds)
		if err != nil {
			tb.Fatalf("seeding: %v", err)
		}
		for i, res := range results {
			if res.Err != nil {
				tb.Fatalf("seeding command %d: %v", start+i, res.Err)
			}
		}
	}
	// One read per chunk moves the entries into this consumer's pending
	// list, which is what both the consume and the reclaim rows need.
	for delivered := 0; delivered < n; {
		read, err := streams.XReadGroup(streams.XReadGroupArgs{
			Group: a.group, Consumer: benchName, Keys: []string{a.stream()},
			IDs: []string{streams.IDNew}, Count: chunk,
		})
		if err != nil {
			tb.Fatalf("XReadGroup: %v", err)
		}
		v, err := a.client.Do(ctx, read)
		if err != nil {
			tb.Fatalf("seeding read: %v", err)
		}
		entries, err := streams.ParseXReadGroup(v, a.keys.Shard, nil)
		if err != nil {
			tb.Fatalf("seeding parse: %v", err)
		}
		if len(entries) == 0 {
			tb.Fatalf("the seeded stream delivered %d of %d entries and then stopped", delivered, n)
		}
		delivered += len(entries)
	}
}

package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/queue"
)

// The payload-ownership benchmark of the performance budgets layer 2 and plan
// The specification. The canonical name is
//
//	BenchmarkMemory/PubSubAck/<mode>/<concurrency>/bytes=<n>/reuse=<on|off>
//
// with the dimensions in that fixed order: the ownership contract the
// publication is admitted under (safe / owned / borrowed, plus the
// zero-copy receive), the concurrency shape, and the payload size,
// which is the axis the four modes part company on — what a mode costs is a
// copy or a scrub of the body, so a family measured at one size measures
// nothing about it.
//
// The last dimension is whether the publisher reuses the body the engine
// hands back, which is the specification's "owned" against its "reuse".
//
// The specification gives this one name in two orders: spells it
// `BenchmarkMemory/PubSubAck/{safe,owned,reuse}/<pool>`, mode first, and the specification
// spells it `BenchmarkMemory/PubSubAck/{1P1C,4P4C,8P8C}/{safe,owned,reuse}`,
// shape first. The mode leads here, which is the order, because
// selects one kind with
// `-bench '^BenchmarkMemory/PubSubAck/<slug>(/|$)'` and `bakeoff decide`
// splits the kind out of the segment straight after the root: against a
// shape-first name every one of those filters selects nothing, which was
// measured against the benchmark this one replaced
// (adr/ADR-017-assumptions.md).
//
// The arms are measured through a full publish/fetch/settle round trip
// because that is the only place the difference between them exists: a safe
// publication pays one Config.Clone at admission and gets its buffer back at
// once, an owned one pays no copy and gets the buffer back at settlement with
// ZeroOnRelease applied, a borrowed one pays neither copy nor scrub, and the
// zero-copy receive is an owned publication whose delivery aliases the
// engine's payload with the use-after-free canary armed, so the scrub at
// settlement is Config.Poison. TestPubSubAckArmsDiffer holds each arm to that
// contract; four arms driven through one path would produce four
// indistinguishable numbers and `bakeoff decide` would then bind a winner
// picked out of noise.

// copyArm is one arm of The slug the bake-off filters on, the
// ownership contract a publication is admitted under, and the way a delivery
// hands the payload to its consumer.
type copyArm struct {
	slug string
	mode CopyMode
	recv ReceiveMode
}

// copyArms are the four arms TestCopyModeKindsList announces, in that order.
// The first three are the copy modes of a publication received the default
// way; the fourth is the zero-copy receive, which is an owned publication —
// the mode has nothing to say about a copying one, whose consumer never sees
// the caller's storage at all.
func copyArms() []copyArm {
	arms := make([]copyArm, 0, len(CopyModes())+1)
	for _, m := range CopyModes() {
		arms = append(arms, copyArm{slug: m.String(), mode: m, recv: ReceiveShared})
	}
	return append(arms, copyArm{slug: ReceiveZeroCopy.String(), mode: CopyOwned, recv: ReceiveZeroCopy})
}

// copyShape is one concurrency shape the number of publishers and
// of competing consumers.
type copyShape struct {
	name       string
	pubs, cons int
}

// copyShapes are the shapes swept. 1P1C is the one the throughput
// criterion is taken the concurrent shapes are
// where a mode's cost meets contention on the lane and on the release
// callback.
func copyShapes() []copyShape {
	return []copyShape{{"1P1C", 1, 1}, {"4P4C", 4, 4}, {"8P8C", 8, 8}}
}

// copyBodies are the payload sizes.
func copyBodies() []int { return []int{0, 64, 1 << 10, 64 << 10, 1 << 20} }

// copyBodiesFor is the size sweep at one shape. Every size is measured at
// 1P1C; the concurrent shapes take the two ends that matter — one cache line,
// where the harness dominates any copy, and 64 KiB, where the copy dominates
// everything else — so that the family costs nine benchmarks per arm rather
// than fifteen. A full-profile campaign spends 40 s per benchmark per arm,
// and the family has four arms. IQMQ_BENCH_MATRIX=full crosses them.
func copyBodiesFor(sh copyShape) []int {
	if sh.pubs == 1 || os.Getenv("IQMQ_BENCH_MATRIX") == "full" {
		return copyBodies()
	}
	return []int{64, 64 << 10}
}

// copyNoReuseBodies are the sizes measured a second time with the publisher's
// buffer reuse switched off, at 1P1C only. That pair is the "owned" against its "reuse": one engine contract under two caller
// disciplines. It is measured at one cache line, where the
// caller's allocation is the whole difference, and at 64 KiB, where the
// difference is a page the allocator has to clear and the engine never sees.
// Two rows per arm rather than a second full sweep, because a full-profile
// campaign spends forty seconds on every row of every arm.
func copyNoReuseBodies() []int { return []int{64, 64 << 10} }

// copyRun is one point of an arm's sweep.
type copyRun struct {
	sh      copyShape
	size    int
	recycle bool
}

// name is the part of the benchmark name that follows the arm's slug.
func (r copyRun) name() string {
	reuse := "off"
	if r.recycle {
		reuse = "on"
	}
	return fmt.Sprintf("%s/bytes=%d/reuse=%s", r.sh.name, r.size, reuse)
}

// copyRuns enumerates one arm's sweep: eleven rows, identical across the four
// arms, so that `bakeoff decide` has a common benchmark set to compare on.
func copyRuns() []copyRun {
	shapes := copyShapes()
	runs := make([]copyRun, 0, 16)
	for _, sh := range shapes {
		for _, size := range copyBodiesFor(sh) {
			runs = append(runs, copyRun{sh, size, true})
		}
	}
	for _, size := range copyNoReuseBodies() {
		runs = append(runs, copyRun{shapes[0], size, false})
	}
	return runs
}

// copyInflightBytes caps what one arm may have outstanding. The pool depth is
// what keeps a publisher from waiting on its consumer for the next body, and
// a depth chosen for 64 bytes would be a gigabyte at 1 MiB.
const copyInflightBytes = 8 << 20

// copyDepth is how many bodies each of pubs publishers owns.
func copyDepth(size, pubs int) int {
	if size <= 0 || pubs <= 0 {
		return 256
	}
	return min(max(copyInflightBytes/(size*pubs), 4), 256)
}

// copySink keeps the consumer's read of the payload it was handed from being
// optimised away: a receive benchmark that never looks at the body would
// measure a delivery nobody consumed, and under the zero-copy mode the read
// is the whole point of the mode.
var copySink atomic.Uint64

// poisonBody writes the use-after-free canary over a body. It fills by
// doubling copies rather than by a byte loop, because what the zero-copy arm
// is measured against is the ZeroOnRelease scrub, which the compiler turns
// into a memclr: a canary written a byte at a time would report the cost of
// the loop rather than the cost of the mode.
func poisonBody(b []byte) {
	if len(b) == 0 {
		return
	}
	b[0] = canaryByte
	for i := 1; i < len(b); i *= 2 {
		copy(b[i:], b[:i])
	}
}

// copyStall is how long a run may go without settling a record before it is
// given up. A body that never comes home -- the engine's recurring defect, a
// record placed while its subscription or the engine is closing whose Release
// never runs (hack/PLAYBOOK.md) -- would otherwise park its publisher on the
// pool for ever, and a benchmark that hangs takes the rest of a fifteen-hour
// campaign's budget with it. The watchdog turns the hang into a reported
// failure and costs nothing per operation.
const copyStall = 60 * time.Second

// watchStall cancels the run when the settled count has not moved for
// copyStall, and records that that is why it ended.
func watchStall(ctx context.Context, cancel context.CancelFunc, settled *atomic.Int64, stalled *atomic.Bool) {
	go func() {
		const checks = 4
		t := time.NewTicker(copyStall / checks)
		defer t.Stop()
		last, missed := int64(-1), 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			if n := settled.Load(); n != last {
				last, missed = n, 0
				continue
			}
			if missed++; missed >= checks {
				stalled.Store(true)
				cancel()
				return
			}
		}
	}()
}

// copyPublisher owns the bodies one publisher publishes and takes them back
// from the engine's release callback. It is the buffer reuse the ownership
// modes exist for: a borrowed or owned body may not be touched again until
// the engine says it is done with it, and the pool is how the benchmark obeys
// that without allocating one body per operation.
type copyPublisher struct {
	free    chan []byte
	release func([]byte)
	part    int
	size    int
	// obs is the counter set to report to, read once at construction. A
	// publisher built while nothing observes carries nil and the record
	// calls below are one not-taken branch on a field already in cache.
	obs *copyCounters
	// recycle is the reuse axis: with it the publisher
	// hands the body the engine released back to its own free list, without
	// it the body is dropped and the next publication allocates a fresh one.
	// The specification spells the two as separate arms, "owned" and "reuse"; they are
	// one engine contract under two caller disciplines, which is why this is
	// a dimension and not a fifth kind.
	recycle bool
}

func newCopyPublisher(part, depth, size int, recycle bool) *copyPublisher {
	p := &copyPublisher{
		free: make(chan []byte, depth), part: part, size: size,
		recycle: recycle, obs: copyObserver.Load(),
	}
	// The release callback is bound once here rather than written as `p.give`
	// at the publish site: a bound method value allocates, and it allocated
	// the same sixteen bytes in all four arms, which put a constant floor
	// under the one column this family is about. The specification asks for zero
	// allocations per message with reuse, and three of the four arms now
	// report exactly that.
	p.release = p.give
	if p.recycle {
		for range depth {
			p.free <- make([]byte, size)
		}
	}
	return p
}

// take blocks until one of the publisher's own bodies is free again. A
// publisher that does not recycle allocates instead, which is the difference
// The specification prices as "0 allocations per message with reuse".
func (p *copyPublisher) take(ctx context.Context) ([]byte, bool) {
	if !p.recycle {
		b := make([]byte, p.size)
		if o := p.obs; o != nil {
			o.fresh.Add(1)
		}
		return b, true
	}
	select {
	case b := <-p.free:
		if o := p.obs; o != nil {
			o.recycled.Add(1)
		}
		return b, true
	default:
	}
	select {
	case b := <-p.free:
		if o := p.obs; o != nil {
			o.recycled.Add(1)
		}
		return b, true
	case <-ctx.Done():
		return nil, false
	}
}

// bodyLen is the length of the bodies this publisher actually allocates. It is
// what the bytes/s metric is derived from: reporting the size the caller
// *asked* for turns a body-size bug into a throughput figure -- a row named
// bytes=1048576 whose publisher allocates 64 reported 1.05 TB/s -- so the
// metric is taken from the storage rather than from the parameter.
func (p *copyPublisher) bodyLen() int { return p.size }

// give is the Publish.Release callback: the engine has finished with the body
// and the publisher may use it again. It runs on whichever goroutine settled
// the last delivery and never blocks, because the channel has room for every
// body the publisher owns.
func (p *copyPublisher) give(b []byte) {
	if !p.recycle {
		return // the engine is done with it and nothing wants it back
	}
	p.free <- b
}

// copyCounters records what every rig and publisher built while it is armed
// actually did. The hook counts say which ownership contract the engine ran;
// the publish-site counts say which *row* of the sweep ran, which is the other
// half of a name that must not lie: `bytes=` and `reuse=` are dimensions of
// the benchmark name, and nothing but this ties them to the body the publisher
// handed over or to whether it recycled it.
type copyCounters struct {
	clones, zeros, poisons atomic.Int64
	// publications and bodyBytes are the count of admitted publications and
	// the total length of the bodies they carried, so that bodyBytes divided
	// by publications is the size the row's `bytes=` claims.
	publications, bodyBytes atomic.Int64
	// fresh and recycled are how the publisher obtained those bodies: fresh
	// from the allocator, recycled from its own free list. They are the row's
	// `reuse=` dimension.
	fresh, recycled atomic.Int64
	// parts is a bitmask of the lanes published to, which is the publisher
	// count of the row's concurrency shape.
	parts atomic.Uint64
}

// copyObserver, when armed, makes every rig newCopyRig builds report its copy
// hooks into it.
//
// It is a package-level switch rather than a parameter of newCopyRig, and that
// is the whole point: nothing a test passes reaches runPubSubAck, which builds
// its own rig and publishes at its own publish site. A test that only inspects
// the arm table proves nothing about the code the benchmark actually drives --
// a hardcoded Mode at the publish site, or a lost case in the arm list, would
// leave every declaration-level assertion green while two or four arms
// measured one contract. TestPubSubAckBodyIsTheArm arms this and runs the
// benchmark itself.
//
// Unarmed it costs one atomic load per rig, which is once per benchmark, and
// nothing per operation.
var copyObserver atomic.Pointer[copyCounters]

// observeCopyHooks arms the observer for the rest of the test.
func observeCopyHooks(tb testing.TB) *copyCounters {
	tb.Helper()
	c := &copyCounters{}
	copyObserver.Store(c)
	tb.Cleanup(func() { copyObserver.Store(nil) })
	return c
}

// copyRig is the publish/fetch/settle round trip one arm is
// measured on, and the thing TestPubSubAckArmsDiffer holds to its arm's
// contract. Every other variant family is pinned to its reference kind: what
// this family measures is the ownership rule, and a lock-free lane or a
// pooled envelope would move the numbers for reasons of its own.
type copyRig struct {
	arm  copyArm
	size int
	eng  *Engine[[]byte]
	top  *Topic[[]byte]
	sub  *Subscription[[]byte]
}

// newCopyRig builds the engine, topic and subscription one arm is driven
// through. It is the only rig in the family: the benchmark and every test of
// it are measured on this engine, so a test that observes one observes what
// the other measures.
func newCopyRig(tb testing.TB, arm copyArm, size, pubs int) *copyRig {
	tb.Helper()
	rig := &copyRig{arm: arm, size: size}
	clone := func(b []byte) []byte {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	zero := func(b []byte) { clear(b) }
	poison := poisonBody
	if c := copyObserver.Load(); c != nil {
		plain := clone
		clone = func(b []byte) []byte { c.clones.Add(1); return plain(b) }
		zero = func(b []byte) { c.zeros.Add(1); clear(b) }
		poison = func(b []byte) { c.poisons.Add(1); poisonBody(b) }
	}
	depth := copyDepth(size, pubs)
	eng, err := New(Config[[]byte]{
		// PerSubEnvelopes rather than the family's Auto: the full-copy
		// architecture copies one payload per subscription whatever the
		// publication asked for (Topic.copiesFor), which would make every
		// arm a safe arm.
		Kinds:  KindSet{Arch: PerSubEnvelopes, Queue: queue.MutexRing, Pool: benchPool()},
		Clone:  clone,
		Zero:   zero,
		Poison: poison,
		// Config.CopyMode is deliberately left at the engine's own default.
		// The specification is the family whose kind is chosen per publication rather than
		// bound at build time, so the arm's identity
		// must rest on the Mode the publish site passes and on nothing else:
		// pinning it here as well made the publish site inert, and a
		// mutation that dropped `Mode:` changed no number and failed no
		// test. The receive mode is engine-wide by contract and stays here.
		Receive: arm.recv,
		DefaultTopology: TopologyConfig{
			// A queue is competing consumers over one subscription, which is
			// the NP/NC shape one reference per record also makes
			// the release exactly the settlement of one delivery.
			Kind:       TopologyQueue,
			Partitions: pubs,
			// Deep enough that the publisher's pool, not the lane, is what
			// paces it, and OverflowBlock rather than a refusal so that no
			// arm can win by refusing faster than the others admit.
			CapacityMessages: depth + 32,
			Overflow:         OverflowBlock,
			AckDeadline:      -1,
		},
	})
	if err != nil {
		tb.Fatalf("New(%s): %v", arm.slug, err)
	}
	tb.Cleanup(func() { _ = eng.Close() })
	top, err := eng.CreateTopic("bench", TopologyConfig{})
	if err != nil {
		tb.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		tb.Fatalf("Subscribe: %v", err)
	}
	rig.eng, rig.top, rig.sub = eng, top, sub
	return rig
}

// publishOne takes one of the publisher's bodies and publishes it under the
// arm's mode. A refused publication admitted nothing and the engine will not
// call Release, so the body comes back here instead (Publish.Release).
func (rig *copyRig) publishOne(ctx context.Context, p *copyPublisher) error {
	body, ok := p.take(ctx)
	if !ok {
		return ctx.Err()
	}
	err := rig.top.Publish(ctx, Publish[[]byte]{
		Payload:   body,
		Size:      rig.size,
		Partition: p.part,
		Mode:      rig.arm.mode,
		Release:   p.release,
	})
	if err != nil && !isPartial(err) {
		p.give(body)
		return err
	}
	if o := p.obs; o != nil {
		o.publications.Add(1)
		o.bodyBytes.Add(int64(len(body)))
		o.parts.Or(1 << (p.part % 64))
	}
	return nil
}

// consume attaches n competing consumers and drains the subscription until
// ctx is done. Each consumer reads the body it was handed *before* it settles
// the delivery, which is the order the zero-copy contract requires (the
// data is invalid after settlement) and the reason the family is measured
// through a receive rather than through a publication alone.
func (rig *copyRig) consume(
	ctx context.Context, tb testing.TB, n int, wg *sync.WaitGroup, settled *atomic.Int64,
) {
	tb.Helper()
	for range n {
		c, err := rig.sub.Attach(ConsumerConfig{})
		if err != nil {
			tb.Fatalf("Attach: %v", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]Claim[[]byte], 32)
			var seen byte
			defer func() { copySink.Add(uint64(seen)) }()
			for {
				got, err := c.Fetch(ctx, dst)
				if err != nil {
					return
				}
				for j := range got {
					cl := dst[j]
					if body := cl.Payload; len(body) > 0 {
						seen ^= body[0] ^ body[len(body)-1]
					}
					if err := c.Ack(cl.Env, cl.Gen); err != nil {
						return
					}
				}
				settled.Add(int64(got))
			}
		}()
	}
}

// runPubSubAck drives b.N publications of one body size through one arm and
// settles every one of them.
//
// The single-publisher shape publishes on the benchmark's own goroutine; a
// concurrent shape hands one token per iteration to whichever publisher is
// free, so the total number of publications is exactly b.N whatever the
// shape. A shape whose publishers each ran their own loop would publish
// several records per iteration and -benchmem would then divide one shape's
// worth of allocation by another shape's operation count, which is precisely
// the column this family is measured on.
func runPubSubAck(b *testing.B, arm copyArm, sh copyShape, size int, recycle bool) {
	b.Helper()
	rig := newCopyRig(b, arm, size, sh.pubs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var settled atomic.Int64
	var stalled atomic.Bool
	var consumers sync.WaitGroup
	rig.consume(ctx, b, sh.cons, &consumers, &settled)
	watchStall(ctx, cancel, &settled, &stalled)

	depth := copyDepth(size, sh.pubs)
	pubs := make([]*copyPublisher, sh.pubs)
	for i := range pubs {
		pubs[i] = newCopyPublisher(i, depth, size, recycle)
	}
	var failMu sync.Mutex
	var failure error
	fail := func(err error) {
		failMu.Lock()
		if failure == nil {
			failure = err
		}
		failMu.Unlock()
	}
	tokens := make(chan struct{}, sh.pubs)
	var publishers sync.WaitGroup
	if sh.pubs > 1 {
		for _, p := range pubs {
			publishers.Add(1)
			go func() {
				defer publishers.Done()
				for range tokens {
					if err := rig.publishOne(ctx, p); err != nil {
						fail(err)
						return
					}
				}
			}()
		}
	}

	// The loop body stays free of b.Fatalf and of an early return other than
	// the recorded break: go1.27.0 crashes in escape analysis when a
	// b.Loop() body contains one (runFanOut).
	solo := pubs[0]
	single := sh.pubs == 1
	for b.Loop() {
		if single {
			if err := rig.publishOne(ctx, solo); err != nil {
				fail(err)
				break
			}
			continue
		}
		// A publisher that stopped on an error reads no more tokens, and
		// the channel is only pubs deep: without the second case the loop
		// would park here rather than report what went wrong.
		select {
		case tokens <- struct{}{}:
		case <-ctx.Done():
		}
	}
	b.StopTimer()
	// The publishers stop before the consumers do: one parked on a full lane
	// under OverflowBlock is released by the drain, and a cancelled consumer
	// would leave it there.
	close(tokens)
	publishers.Wait()
	cancel()
	consumers.Wait()
	if stalled.Load() {
		b.Fatalf("no record settled for %v: a payload the engine admitted was never released, "+
			"so every one of the publisher's bodies is still with the engine (terminal release)", copyStall)
	}
	if failure != nil && !errors.Is(failure, context.Canceled) && !errors.Is(failure, errcode.ErrClosed) {
		b.Fatalf("publish: %v", failure)
	}
	if secs := b.Elapsed().Seconds(); secs > 0 {
		msgs := float64(settled.Load())
		b.ReportMetric(msgs/secs, "msgs/s")
		b.ReportMetric(msgs*float64(solo.bodyLen())/secs, "bytes/s")
	}
}

// copySignature is what one arm does to a payload, as the
// caller of the engine sees it. It is the whole observable difference between
// the four arms: the copies the engine took, the release-time scrub it
// applied, whether the body came home at admission or at settlement, and
// whether the delivery aliased the publisher's own storage.
type copySignature struct {
	clones            int64 // Config.Clone calls
	zeros             int64 // Config.Zero calls (ZeroOnRelease)
	poisons           int64 // Config.Poison calls (the canary)
	releasedAtPublish bool  // the body came home before the record was fetched
	aliasesBody       bool  // the delivery aliased the publisher's own array
}

// copyExpect is one arm's contract: the modes it must be wired to, and the
// signature it must produce in a default build and under iqmq_debug.
type copyExpect struct {
	mode CopyMode
	recv ReceiveMode
	sig  copySignature
	dbg  copySignature
}

// copyExpectations is written out rather than derived. A table computed from
// CopyMode.Copies, CopyMode.Scrubs and ReceiveMode.poisons would agree with
// the engine by construction — it would still agree if two arms were wired to
// one path — which is the oracle-that-cannot-fail this repository has shipped
// before.
//
// The iqmq_debug column is a second literal, not a rule. Under the tag
// ReceiveMode.poisons arms the canary for the shared receive too, so a safe
// or owned publication is poisoned where a default build zeroes it, and the
// owned and zero-copy arms then scrub identically; TestPubSubAckArmsDiffer
// allows that one collision and no other.
var copyExpectations = map[string]copyExpect{
	"safe": {
		mode: CopySafe, recv: ReceiveShared,
		sig: copySignature{clones: 1, zeros: 1, poisons: 0, releasedAtPublish: true, aliasesBody: false},
		dbg: copySignature{clones: 1, zeros: 0, poisons: 1, releasedAtPublish: true, aliasesBody: false},
	},
	"owned": {
		mode: CopyOwned, recv: ReceiveShared,
		sig: copySignature{clones: 0, zeros: 1, poisons: 0, releasedAtPublish: false, aliasesBody: true},
		dbg: copySignature{clones: 0, zeros: 0, poisons: 1, releasedAtPublish: false, aliasesBody: true},
	},
	"borrowed": {
		mode: CopyBorrowed, recv: ReceiveShared,
		sig: copySignature{clones: 0, zeros: 0, poisons: 0, releasedAtPublish: false, aliasesBody: true},
		dbg: copySignature{clones: 0, zeros: 0, poisons: 0, releasedAtPublish: false, aliasesBody: true},
	},
	"zerocopy": {
		mode: CopyOwned, recv: ReceiveZeroCopy,
		sig: copySignature{clones: 0, zeros: 0, poisons: 1, releasedAtPublish: false, aliasesBody: true},
		dbg: copySignature{clones: 0, zeros: 0, poisons: 1, releasedAtPublish: false, aliasesBody: true},
	},
}

// TestPubSubAckArmsDiffer proves that each arm of BenchmarkMemory/PubSubAck
// is the mode its name claims, and that no two arms are wired to the same
// path.
//
// It is the check the family's bake-off rests on. Four arms driven through one
// code path would produce four numbers that differ only by noise, `bakeoff
// decide` would pick the lowest of them, and ADR-009 would carry a measured
// row that measured nothing — which is exactly the "oracles that agree by
// construction" defect hack/PLAYBOOK.md names. Every arm is driven through
// newCopyRig, the same rig runPubSubAck is measured on, so a rewiring of the
// benchmark is a failure here rather than a quiet change of number.
func TestPubSubAckArmsDiffer(t *testing.T) {
	arms := copyArms()
	if len(arms) != len(copyExpectations) {
		t.Fatalf("copyArms() has %d arms, copyExpectations %d", len(arms), len(copyExpectations))
	}
	got := make(map[string]copySignature, len(arms))
	for _, arm := range arms {
		want, ok := copyExpectations[arm.slug]
		if !ok {
			t.Fatalf("arm %q has no expected contract; add it to copyExpectations", arm.slug)
		}
		t.Run(arm.slug, func(t *testing.T) {
			if arm.mode != want.mode || arm.recv != want.recv {
				t.Fatalf("the %q arm publishes under %s/%s, want %s/%s: "+
					"BenchmarkMemory/PubSubAck/%s therefore measures a mode other than the one it is named for",
					arm.slug, arm.mode.Ident(), arm.recv.Ident(),
					want.mode.Ident(), want.recv.Ident(), arm.slug)
			}
			sig := observeCopyArm(t, arm)
			exp := want.sig
			if debugCanary {
				exp = want.dbg
			}
			if sig != exp {
				t.Fatalf("the %q arm did %+v to its payload, want %+v: "+
					"the benchmark path is not the %s contract of the memory engine",
					arm.slug, sig, exp, arm.slug)
			}
			got[arm.slug] = sig
		})
	}
	if t.Failed() {
		return
	}
	for i, a := range arms {
		for _, other := range arms[i+1:] {
			if got[a.slug] != got[other.slug] {
				continue
			}
			if debugCanary && collapsesUnderDebug(a.slug, other.slug) {
				// Documented in copyExpectations: the tag poisons the shared
				// receive too, so these two scrub the same way by design.
				continue
			}
			t.Fatalf("the %q and %q arms are indistinguishable: both did %+v to their payload. "+
				"Two arms are wired to the same code path, so "+
				"BenchmarkMemory/PubSubAck reports the same number for both and "+
				"`bakeoff decide A29` would bind a winner picked out of noise",
				a.slug, other.slug, got[a.slug])
		}
	}
}

// collapsesUnderDebug reports the one arm pair whose observable signatures
// are allowed to coincide, and only in an iqmq_debug build: the tag arms the
// use-after-free canary for the shared receive as well, so an owned
// publication is poisoned there exactly as a zero-copy one is
// (ReceiveMode.poisons).
func collapsesUnderDebug(a, b string) bool {
	return (a == "owned" && b == "zerocopy") || (a == "zerocopy" && b == "owned")
}

// observeCopyArm drives one publish/fetch/settle round trip through the arm's
// own rig and reports what the engine did to the payload.
func observeCopyArm(t *testing.T, arm copyArm) copySignature {
	t.Helper()
	const size = 64
	observeCopyHooks(t)
	rig := newCopyRig(t, arm, size, 1)
	c, err := rig.sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	body := make([]byte, size)
	for i := range body {
		// No zero and no canary byte, so either scrub is visible in the body.
		body[i] = byte(i) | 1
	}
	home := make(chan []byte, 1)
	if err := rig.top.Publish(context.Background(), Publish[[]byte]{
		Payload: body, Size: size, Partition: 0, Mode: arm.mode,
		Release: func(b []byte) { home <- b },
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	var sig copySignature
	select {
	case <-home:
		sig.releasedAtPublish = true
	default:
	}
	dst := make([]Claim[[]byte], 1)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch: %v", err)
	}
	if n != 1 {
		t.Fatalf("fetched %d records, want 1", n)
	}
	sig.aliasesBody = len(dst[0].Payload) == size && &dst[0].Payload[0] == &body[0]
	if !dst[0].Alive() {
		t.Fatal("a live delivery reports itself dead")
	}
	if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !sig.releasedAtPublish {
		select {
		case <-home:
		case <-time.After(10 * time.Second):
			t.Fatalf("the %q arm never released the payload it took", arm.slug)
		}
	}
	select {
	case <-home:
		t.Fatalf("the %q arm released the payload twice", arm.slug)
	default:
	}
	hooks := copyObserver.Load()
	sig.clones, sig.zeros, sig.poisons = hooks.clones.Load(), hooks.zeros.Load(), hooks.poisons.Load()
	return sig
}

// TestPubSubAckBenchHarness checks the benchmark harness itself, so that a
// broken harness fails the test suite rather than wedging a campaign or
// reporting a meaningless number. Every arm is run at 1P1C, and two of them
// at 4P4C, which is the other publisher harness. Each run is capped to
// harnessIters publications: what is under test is the harness.
func TestPubSubAckBenchHarness(t *testing.T) {
	capBenchTime(t, harnessIters)
	type run struct {
		arm copyArm
		rc  copyRun
	}
	shapes := copyShapes()
	var runs []run
	for i, arm := range copyArms() {
		// The no-reuse row has a publisher that never waits on its pool, and
		// the concurrent shape has the token harness; both are covered, the
		// second on alternating arms so the test stays cheap.
		runs = append(runs,
			run{arm, copyRun{shapes[0], 64, true}},
			run{arm, copyRun{shapes[0], 1 << 10, false}},
		)
		if i%2 == 0 {
			runs = append(runs, run{arm, copyRun{shapes[1], 1 << 10, true}})
		}
	}
	for _, rr := range runs {
		t.Run(rr.arm.slug+"/"+rr.rc.name(), func(t *testing.T) {
			r := testing.Benchmark(func(b *testing.B) {
				runPubSubAck(b, rr.arm, rr.rc.sh, rr.rc.size, rr.rc.recycle)
			})
			if r.N == 0 {
				t.Fatalf("benchmark did not run: %+v", r)
			}
			for _, m := range []string{"msgs/s", "bytes/s"} {
				if _, ok := r.Extra[m]; !ok {
					t.Fatalf("no %s metric: %+v", m, r.Extra)
				}
			}
		})
	}
}

// TestPubSubAckBodyIsTheArm is the check that ties every row of
// BenchmarkMemory/PubSubAck to the name it is published under.
//
// TestPubSubAckArmsDiffer above works from the arm table and from a rig a test
// builds; neither reaches the publish site runPubSubAck actually drives. So a
// `Mode:` hardcoded in publishOne, a `Mode:` dropped so every publication
// takes the engine's default, a body size hardcoded in the publisher, or the
// reuse discipline forced one way would leave a row measuring something other
// than its name while the arm table, `make bench-lint`, `make ci` and
// all stayed green -- and `bakeoff decide `
// would bind ADR-009 from rows whose names lie. A review produced two such
// states: four arms within 16 per cent of each other with a "winner" at
// 7031 ns/op, and a row named bytes=1048576 whose publisher allocated 64,
// reporting 1.05 TB/s.
//
// This test therefore runs runPubSubAck itself, once per row of the sweep,
// with the copy hooks and the publish site observed, and holds all four
// segments of the name to what the body did:
//
//   - <mode>: the hook counts, one per publication where the arm's contract
//     says one and none where it says none, against the literal
//     copyExpectations table rather than a second derived oracle;
//   - bytes=<n>: the total length of the bodies handed to Publish, divided by
//     the publications that carried them;
//   - reuse=<on|off>: whether those bodies came from the allocator or from the
//     publisher's own free list;
//   - <concurrency>: that no publication reached a lane the shape does not
//     have. The exact publisher count is not asserted, because which of eight
//     goroutines happens to take a token is the scheduler's business; a
//     collapsed shape changes the row *set*, which TestPubSubAckNames holds.
func TestPubSubAckBodyIsTheArm(t *testing.T) {
	for _, arm := range copyArms() {
		want, ok := copyExpectations[arm.slug]
		if !ok {
			t.Fatalf("arm %q has no expected contract; add it to copyExpectations", arm.slug)
		}
		exp := want.sig
		if debugCanary {
			exp = want.dbg
		}
		for _, rc := range copyRuns() {
			t.Run(arm.slug+"/"+rc.name(), func(t *testing.T) {
				// A fixed iteration count, so that testing.Benchmark makes
				// exactly one run of exactly this many publications and the
				// observed counts can be compared against it. A duration-based
				// benchtime calibrates by running the body several times, and
				// the counters would then cover every one of those runs while
				// r.N reported only the last.
				iters := copyProbeIters(rc.size)
				capBenchTime(t, fmt.Sprintf("%dx", iters))
				o := observeCopyHooks(t)
				r := testing.Benchmark(func(b *testing.B) {
					runPubSubAck(b, arm, rc.sh, rc.size, rc.recycle)
				})
				if r.N != iters {
					t.Fatalf("the benchmark ran %d iterations, want %d: the observed counts "+
						"cannot be compared against a publication count that is not fixed", r.N, iters)
				}
				n := int64(r.N)
				row := "BenchmarkMemory/PubSubAck/" + arm.slug + "/" + rc.name()

				type hookCounts struct{ clones, zeros, poisons int64 }
				gotHooks := hookCounts{o.clones.Load(), o.zeros.Load(), o.poisons.Load()}
				wantHooks := hookCounts{exp.clones * n, exp.zeros * n, exp.poisons * n}
				if gotHooks != wantHooks {
					t.Fatalf("%s ran %d publications and the engine did %+v to them, want %+v "+
						"(one per publication where the %s contract of the memory engine says one, "+
						"none where it says none). The benchmark body is not driving the contract its "+
						"name claims, so its number is another arm's number under this arm's name.",
						row, n, gotHooks, wantHooks, arm.slug)
				}

				pubs := o.publications.Load()
				if pubs != n {
					t.Fatalf("%s ran %d iterations but admitted %d publications: the row's "+
						"per-operation figures are divided by an operation count the engine "+
						"never saw", row, n, pubs)
				}
				if got, wantBytes := o.bodyBytes.Load(), int64(rc.size)*pubs; got != wantBytes {
					t.Fatalf("%s handed Publish %d bytes over %d publications (%d each), but its "+
						"name claims bytes=%d. The row measures a body size other than the one it "+
						"is named for, and its bytes/s is that lie multiplied by a throughput.",
						row, got, pubs, got/max(pubs, 1), rc.size)
				}

				fresh, recycled := o.fresh.Load(), o.recycled.Load()
				wantFresh, wantRecycled := pubs, int64(0)
				if rc.recycle {
					wantFresh, wantRecycled = 0, pubs
				}
				if fresh != wantFresh || recycled != wantRecycled {
					t.Fatalf("%s took %d bodies from the allocator and %d from the publisher's free "+
						"list, want %d and %d. The row measures the other buffer discipline: "+
						"reuse=%s is what the performance budgets prices as \"0 allocations per message "+
						"with reuse\".",
						row, fresh, recycled, wantFresh, wantRecycled,
						map[bool]string{true: "on", false: "off"}[rc.recycle])
				}

				if parts, lanes := o.parts.Load(), uint64(1)<<rc.sh.pubs-1; parts&^lanes != 0 {
					t.Fatalf("%s published to lanes %#x, but a %s shape has only %d: %#x",
						row, parts, rc.sh.name, rc.sh.pubs, lanes)
				} else if parts == 0 {
					t.Fatalf("%s published to no lane at all", row)
				}
			})
		}
	}
}

// copyProbeIters is how many publications TestPubSubAckBodyIsTheArm drives
// through one row. Every assertion it makes is a ratio, so any count will do;
// the big bodies take fewer because the test runs under -race on every
// `make ci` and a megabyte row would otherwise move two hundred megabytes to
// prove an equality.
func copyProbeIters(size int) int {
	switch {
	case size >= 1<<20:
		return 20
	case size >= 64<<10:
		return 50
	default:
		return 200
	}
}

// TestPubSubAckNames pins the benchmark names the bake-off filters on. Family
// The specification is measured with `-bench '^BenchmarkMemory/PubSubAck/<slug>(|$)'`, one
// slug per kind announced by TestCopyModeKindsList, and a rename that leaves
// that filter selecting nothing is the failure this repository has already
// spent a campaign.
func TestPubSubAckNames(t *testing.T) {
	slugs := make([]string, 0, len(CopyModes())+1)
	for _, m := range CopyModes() {
		slugs = append(slugs, m.String())
	}
	slugs = append(slugs, ReceiveZeroCopy.String())
	arms := copyArms()
	if len(arms) != len(slugs) {
		t.Fatalf("copyArms() has %d arms, TestCopyModeKindsList announces %d kinds", len(arms), len(slugs))
	}
	for i, want := range slugs {
		if arms[i].slug != want {
			t.Fatalf("arm %d is %q, want %q: the bake-off filter names the kinds "+
				"TestCopyModeKindsList prints", i, arms[i].slug, want)
		}
	}
	for _, arm := range arms {
		n := 0
		for _, sh := range copyShapes() {
			n += len(copyBodiesFor(sh))
		}
		if n == 0 {
			t.Fatalf("the %q arm has no benchmark", arm.slug)
		}
	}
}

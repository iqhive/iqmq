package engine

import (
	"context"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/hash"
	"github.com/iqhive/iqmq/internal/pool"
	"github.com/iqhive/iqmq/internal/queue"
	"github.com/iqhive/iqmq/internal/registry"
	"github.com/iqhive/iqmq/internal/stats"
	"github.com/iqhive/iqmq/internal/timer"
	"github.com/iqhive/iqmq/internal/wait"
)

// newTestEngine builds an engine with the given kinds and topology.
func newTestEngine(t *testing.T, ks KindSet, top TopologyConfig) *Engine[int64] {
	t.Helper()
	e, err := New(Config[int64]{Kinds: ks, Clone: func(v int64) int64 { return v }, DefaultTopology: top})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// queueSub builds a topic with one subscription and one consumer.
func queueSub(t *testing.T, e *Engine[int64]) (*Topic[int64], *Subscription[int64], *Consumer[int64]) {
	t.Helper()
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	return top, sub, c
}

// poll returns a context that is already done, so Fetch never blocks.
func poll(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestKindSetResolveAndString(t *testing.T) {
	ks := KindSet{}.Resolve()
	if ks.Arch != ArchAutoKind || ks.Queue != queue.AutoKind || ks.Wait != wait.AutoKind ||
		ks.Timer != timer.AutoKind ||
		ks.Hash != hash.AutoKind || ks.Pool != pool.AutoKind || ks.Registry != registry.AutoKind ||
		ks.Stats != stats.AutoKind || ks.RetryLane != RetryLaneAutoKind {
		t.Fatalf("Auto resolved to %v", ks)
	}
	safe := KindSet{Queue: queue.Chan, Safe: true}.Resolve()
	if safe != (KindSet{
		Arch:  PerSubEnvelopes,
		Queue: queue.MutexRing, Wait: wait.Chan, Timer: timer.Heap, Hash: hash.FNV1a,
		Pool: pool.None, Registry: registry.RWMutexMap, Stats: stats.Atomic,
		RetryLane: BlockLane, Safe: true,
	}) {
		t.Fatalf("Safe resolved to %v", safe)
	}
	s := KindSet{}.String()
	for _, want := range []string{
		"arch=", "queue=", "wait=", "timer=", "hash=", "pool=", "registry=", "stats=", "retry=",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("KindSet.String() = %q, missing %q", s, want)
		}
	}
	if !strings.HasSuffix(KindSet{Safe: true}.String(), "/safe") {
		t.Fatalf("safe set renders as %q", KindSet{Safe: true}.String())
	}
	// Ordered folds in every family that can reorder what a lane holds: the
	// ready queue, the architecture — Arch.Ordered is false for a shared
	// log, whose redeliveries are served before the log — and the retry
	// placement. Asserting it followed the queue kind alone made the
	// assertion a statement about one of them, and it failed as soon as the
	// bake-off bound a kind that reorders in one of the others
	// (adr/ADR-017-assumptions.md).
	//
	// The scheduler kind (the specification) is deliberately not a term: every store sorts
	// the batch it promotes, so a wheel's unordered firing never reaches a
	// consumer, and TestStrictOrderingHoldsOnEverySchedulerKind holds the
	// engine to it kind by kind. timer.AutoKind is
	// still read here so that this assertion names what it leaves out.
	wantOrdered := queue.AutoKind.Ordered() &&
		ArchAutoKind.Ordered() && RetryLaneAutoKind.Ordered()
	if got := (KindSet{}).Ordered(); got != wantOrdered {
		t.Fatalf("KindSet{}.Ordered() = %v, want %v (queue=%v arch=%v retry=%v;"+
			" timer=%v ordered=%v, which Ordered does not fold in)",
			got, wantOrdered, queue.AutoKind.Ordered(),
			ArchAutoKind.Ordered(), RetryLaneAutoKind.Ordered(),
			timer.AutoKind, timer.AutoKind.Ordered())
	}
}

// TestQueueKindsAreMPMC pins the capability narrowing: a lane is fed and
// drained by any number of goroutines, so only MPMC kinds are accepted.
func TestQueueKindsAreMPMC(t *testing.T) {
	kinds := QueueKinds()
	if len(kinds) == 0 || len(kinds) >= len(queue.Kinds()) {
		t.Fatalf("QueueKinds() = %v of %v", kinds, queue.Kinds())
	}
	for _, k := range kinds {
		if !k.Modes().Has(queue.ModeMPMC) {
			t.Fatalf("%v does not support MPMC", k)
		}
	}
	for _, k := range []queue.Kind{queue.SPSC, queue.MPSC} {
		if _, err := New(Config[int64]{Kinds: KindSet{Queue: k}}); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("New with queue %v: %v", k, err)
		}
	}
	// Safe overrides the field, so it is accepted whatever was asked for.
	//
	// And the engine it builds is CLOSED rather than discarded, which is the
	// only New in this package's tests that succeeds and was not: New starts
	// the engine's scheduler goroutine and Close is what stops it, so an
	// engine nobody closes leaves that goroutine behind for the rest of the
	// process -- one per run of this package, invisible at -count=1 and
	// eighteen live engines deep into the -race -count=20 loop the
	// park/notify family's gate runs (TestMain in goroutines_test.go is the
	// check that keeps it that way).
	safe, err := New(Config[int64]{Kinds: KindSet{Queue: queue.SPSC, Safe: true}, Clone: func(v int64) int64 { return v }})
	if err != nil {
		t.Fatalf("safe set rejected: %v", err)
	}
	if err := safe.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New(Config[int64]{Kinds: KindSet{Safe: true}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Safe without Clone: %v", err)
	}
	if _, err := New(Config[int64]{Kinds: KindSet{Queue: queue.Kind(200)}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("undefined kind: %v", err)
	}
	if _, err := New(Config[int64]{DefaultTopology: TopologyConfig{Partitions: -1}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative partitions: %v", err)
	}
	if _, err := New(Config[int64]{DefaultTopology: TopologyConfig{Partitions: 1 << 20}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("absurd partition count: %v", err)
	}
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	if got := e.Kinds(); got != (KindSet{}).Resolve() {
		t.Fatalf("Kinds() = %v", got)
	}
	if e.Clock() == nil {
		t.Fatal("Clock() is nil")
	}
}

func TestTopologyDefaults(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := top.Topology()
	if cfg.Partitions != DefaultPartitions || cfg.CapacityMessages != DefaultCapacityMessages ||
		cfg.AckDeadline != DefaultAckDeadline {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	// The engine default is the base for a topic's zero fields.
	e2 := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 4, AckDeadline: time.Second})
	top2, err := e2.CreateTopic("t", TopologyConfig{CapacityMessages: 7})
	if err != nil {
		t.Fatal(err)
	}
	if c := top2.Topology(); c.Partitions != 4 || c.AckDeadline != time.Second || c.CapacityMessages != 7 {
		t.Fatalf("merge failed: %+v", c)
	}
	if top2.Name() != "t" {
		t.Fatalf("Name() = %q", top2.Name())
	}
}

func TestCreateTopicIsIdempotentAndValidated(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	a, err := e.CreateTopic("orders.eu", TopologyConfig{Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	b, err := e.CreateTopic("orders.eu", TopologyConfig{Partitions: 8})
	if err != nil {
		t.Fatal(err)
	}
	if a != b || b.Topology().Partitions != 2 {
		t.Fatal("CreateTopic is not idempotent")
	}
	if _, err := e.CreateTopic("", TopologyConfig{}); err == nil {
		t.Fatal("empty topic name accepted")
	}
	if _, err := e.Topic("missing"); !errors.Is(err, errcode.ErrTopicNotFound) {
		t.Fatalf("Topic(missing): %v", err)
	}
	got, err := e.Topic("orders.eu")
	if err != nil || got != a {
		t.Fatalf("Topic() = %v, %v", got, err)
	}
	n := 0
	e.Topics(func(*Topic[int64]) bool { n++; return true })
	if n != 1 {
		t.Fatalf("Topics visited %d topics", n)
	}
}

func TestSubscribeIsIdempotent(t *testing.T) {
	// The second Subscribe names a per-subscription capacity, which a shared
	// partition log refuses rather than ignores.
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 3})
	top, _ := e.CreateTopic("t", TopologyConfig{})
	a, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := top.Subscribe("s", SubscriptionConfig{CapacityMessages: 9})
	if err != nil || a != b {
		t.Fatalf("Subscribe is not idempotent: %v %v", b, err)
	}
	if a.Name() != "s" || a.Partitions() != 3 {
		t.Fatalf("subscription %q has %d lanes", a.Name(), a.Partitions())
	}
	if _, err := top.Subscription("nope"); !errors.Is(err, ErrNoSubscription) {
		t.Fatalf("Subscription(nope): %v", err)
	}
	if got := top.Subscriptions(); len(got) != 1 || got[0] != a {
		t.Fatalf("Subscriptions() = %v", got)
	}
	if _, err := top.Subscribe("", SubscriptionConfig{}); err == nil {
		t.Fatal("empty subscription name accepted")
	}
}

// TestPublishWithoutSubscriptionReleases: a queue topic with no subscription
// discards the record and hands the payload straight back.
func TestPublishWithoutSubscriptionReleases(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	released := 0
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 1, Size: 8, Release: func(int64) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("payload released %d times", released)
	}
	if _, _, discarded := e.Totals(); discarded != 1 {
		t.Fatalf("discarded = %d", discarded)
	}
}

// TestKeyedPublishIsStable checks that a key always reaches the same lane and
// that the lane is the one the hash family computes.
func TestKeyedPublishIsStable(t *testing.T) {
	e := newTestEngine(t, KindSet{Hash: hash.Wyhash}, TopologyConfig{Partitions: 8})
	top, sub, c := queueSub(t, e)
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("orders/eu/1"), {0xff, 0x00}}
	want := map[string]int{}
	for _, k := range keys {
		h, err := hash.Bytes(hash.Wyhash, 0, k)
		if err != nil {
			t.Fatal(err)
		}
		want[string(k)] = hash.Lane(h, 8)
	}
	for round := range 3 {
		for _, k := range keys {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: int64(round), Key: k, Size: 4, Partition: -1,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	dst := make([]Claim[int64], 16)
	n, err := c.Fetch(poll(t), dst)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(keys)*3 {
		t.Fatalf("fetched %d of %d", n, len(keys)*3)
	}
	for i := range n {
		k := string(dst[i].Info.Key)
		if got := int(dst[i].Info.Partition); got != want[k] {
			t.Fatalf("key %q landed in lane %d, want %d", k, got, want[k])
		}
	}
	if sub.Inspect().InFlight != n {
		t.Fatalf("in-flight %d", sub.Inspect().InFlight)
	}
}

// TestRoundRobinSpreadsUnkeyedRecords checks the unkeyed distribution.
func TestRoundRobinSpreadsUnkeyedRecords(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 4})
	top, _, c := queueSub(t, e)
	for i := range 8 {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1, Partition: -1}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 8)
	n, err := c.Fetch(poll(t), dst)
	if err != nil || n != 8 {
		t.Fatalf("fetch: n=%d err=%v", n, err)
	}
	lanes := map[int32]int{}
	for i := range n {
		lanes[dst[i].Info.Partition]++
	}
	if len(lanes) != 4 {
		t.Fatalf("round robin used %d of 4 lanes: %v", len(lanes), lanes)
	}
}

// TestExplicitPartitionWraps checks the explicit-partition override.
func TestExplicitPartitionWraps(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 3})
	top, _, c := queueSub(t, e)
	for _, p := range []int{0, 1, 2, 5} {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(p), Size: 1, Partition: p}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 4)
	n, _ := c.Fetch(poll(t), dst)
	got := map[int64]int32{}
	for i := range n {
		got[dst[i].Payload] = dst[i].Info.Partition
	}
	if got[5] != 2 {
		t.Fatalf("partition 5 of 3 landed in lane %d", got[5])
	}
}

// TestAnEnormousDelayIsNeverDue pins the saturation in due: a delay past
// the end of the scheduler's clock would otherwise wrap to a time in the
// past and deliver the record at once.
func TestAnEnormousDelayIsNeverDue(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	top, _, c := queueSub(t, e)
	p := Publish[int64]{Payload: 1, Size: 1, Delay: time.Duration(math.MaxInt64)}
	if got := due(&p, e.now()); got != math.MaxInt64 {
		t.Fatalf("due of a MaxInt64 delay = %d, want it saturated to %d", got, int64(math.MaxInt64))
	}
	if err := top.Publish(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.Fetch(poll(t), make([]Claim[int64], 1)); n != 0 {
		t.Fatalf("a record delayed by %v was delivered at once", time.Duration(math.MaxInt64))
	}
}

// TestEnormousDurationsSaturate pins the same saturation everywhere else the
// engine adds a duration it was given to the clock. Past the end of the
// clock the sum wrapped to a time in the past, which turned each duration
// into its opposite: a lease that should never run out expired at once, so
// did an extension, a negative acknowledgement delayed to the end of time
// was redelivered at once, a dedup window that should remember a key for
// ever forgot it at once, and a retention age that should keep a record for
// ever trimmed it at once. The memory DSN accepts any Go duration for each.
func TestEnormousDurationsSaturate(t *testing.T) {
	const forever = time.Duration(math.MaxInt64)
	publishOne := func(t *testing.T, top *Topic[int64]) {
		t.Helper()
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	fetchOne := func(t *testing.T, c *Consumer[int64]) Claim[int64] {
		t.Helper()
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatalf("fetched %d records, want 1", n)
		}
		return dst[0]
	}
	t.Run("AckDeadline", func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, AckDeadline: forever})
		top, sub, c := queueSub(t, e)
		publishOne(t, top)
		fetchOne(t, c)
		if n := e.Faults().ExpireLeases(); n != 0 {
			t.Fatalf("a lease of %v expired at once (%+v)", forever, sub.Inspect())
		}
	})
	t.Run("Extend", func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, sub, c := queueSub(t, e)
		publishOne(t, top)
		cl := fetchOne(t, c)
		if err := c.Extend(cl.Env, cl.Gen, forever); err != nil {
			t.Fatalf("Extend: %v", err)
		}
		if n := e.Faults().ExpireLeases(); n != 0 {
			t.Fatalf("a lease extended by %v expired at once (%+v)", forever, sub.Inspect())
		}
	})
	t.Run("NackDelay", func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, sub, c := queueSub(t, e)
		publishOne(t, top)
		cl := fetchOne(t, c)
		if err := c.Nack(cl.Env, cl.Gen, forever); err != nil {
			t.Fatalf("Nack: %v", err)
		}
		if n, _ := c.Fetch(poll(t), make([]Claim[int64], 1)); n != 0 {
			t.Fatalf("a record nacked with a delay of %v was redelivered at once (%+v)", forever, sub.Inspect())
		}
	})
	t.Run("DedupWindow", func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, DedupWindow: forever})
		top, sub, _ := queueSub(t, e)
		for range 2 {
			if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1, DedupKey: []byte("k")}); err != nil {
				t.Fatal(err)
			}
		}
		if s := sub.Inspect(); s.Published != 1 || s.Deduplicated != 1 {
			t.Fatalf("a window of %v forgot the key at once: %+v", forever, s)
		}
	})
	t.Run("RetentionMaxAge", func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, err := e.CreateTopic("t", TopologyConfig{Kind: TopologyStream, Retention: Retention{MaxAge: forever}})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		publishOne(t, top)
		if n, _ := top.Retained(); n != 1 {
			t.Fatalf("a retention age of %v kept %d records, want 1", forever, n)
		}
	})
}

// TestCapacityRejectsAtomically: a subscription at its limit refuses the
// publication for every subscription, and nothing is admitted anywhere.
func TestCapacityRejectsAtomically(t *testing.T) {
	// One subscription at its own limit while another has room is a
	// per-subscription capacity, which a shared partition log has no place
	// for: one reservation admits a publication for every subscription at
	// once.
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 2})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	slow, err := top.Subscribe("slow", SubscriptionConfig{CapacityMessages: 1})
	if err != nil {
		t.Fatal(err)
	}
	fast, err := top.Subscribe("fast", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	err = top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 1})
	if !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("second publish: %v", err)
	}
	if s := slow.Inspect(); s.Published != 1 || s.Rejected != 1 {
		t.Fatalf("slow: %+v", s)
	}
	if s := fast.Inspect(); s.Published != 1 || s.Rejected != 1 {
		t.Fatalf("fast subscription was admitted while the slow one refused: %+v", s)
	}
}

// TestByteCapacity refuses on the byte budget.
func TestByteCapacity(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityBytes: 10})
	top, sub, _ := queueSub(t, e)
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 8}); err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 8}); !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("byte capacity: %v", err)
	}
	if s := sub.Inspect(); s.Bytes != 8 {
		t.Fatalf("bytes = %d", s.Bytes)
	}
}

// TestEngineTotals bounds the whole engine.
func TestEngineTotals(t *testing.T) {
	e, err := New(Config[int64]{MaxTotalMessages: 2, MaxTotalBytes: 24})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	top, _ := e.CreateTopic("t", TopologyConfig{})
	if _, err := top.Subscribe("s", SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 8}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 3, Size: 8}); !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("engine limit: %v", err)
	}
	msgs, bytes, _ := e.Totals()
	if msgs != 2 || bytes != 16 {
		t.Fatalf("totals = %d, %d", msgs, bytes)
	}
}

// TestMaxMessageSize refuses oversized publications before admission.
func TestMaxMessageSize(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{MaxMessageSize: 16})
	top, sub, _ := queueSub(t, e)
	released := 0
	err := top.Publish(context.Background(), Publish[int64]{
		Payload: 1, Size: 17, Release: func(int64) { released++ },
	})
	if !errors.Is(err, errcode.ErrMessageTooLarge) {
		t.Fatalf("oversized publish: %v", err)
	}
	if released != 0 {
		t.Fatal("a refused publication must leave ownership with the caller")
	}
	if s := sub.Inspect(); s.Published != 0 {
		t.Fatalf("%+v", s)
	}
}

// TestPublishBatchStopsAtFirstFailure checks the batch contract.
func TestPublishBatchStopsAtFirstFailure(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 2})
	top, sub, _ := queueSub(t, e)
	batch := make([]Publish[int64], 4)
	for i := range batch {
		batch[i] = Publish[int64]{Payload: int64(i), Size: 1}
	}
	n, err := top.PublishBatch(context.Background(), batch)
	if n != 2 || !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("PublishBatch = %d, %v", n, err)
	}
	if s := sub.Inspect(); s.Published != 2 || s.Ready != 2 {
		t.Fatalf("%+v", s)
	}
	n, err = top.PublishBatch(context.Background(), nil)
	if n != 0 || err != nil {
		t.Fatalf("empty batch = %d, %v", n, err)
	}
}

// TestPublishBatchPartialIsNotARefusal: under the Independent policy an
// element the engine admitted to some of the topic's subscriptions and
// refused for the rest is reported by Publish as errcode.ErrPartial. The
// batch must count it as taken and carry on, because the engine owns that
// payload and has released it: stopping there and naming its index would
// have the caller retry a record that is already delivered, and — for an
// owned publication — believe it still owns a payload the engine freed
// (adr/ADR-017-assumptions.md).
func TestPublishBatchPartialIsNotARefusal(t *testing.T) {
	// A partial admission needs a per-subscription reservation to be partial
	// about: under a shared partition log Independent behaves as Atomic,
	// because one reservation decides for every subscription, and no publication is ever partial there.
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
		Kind: TopologyTopic, Fanout: Independent, Partitions: 1,
		CapacityMessages: 1, AckDeadline: -1,
	})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := top.Subscribe("slow", SubscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	fast, err := top.Subscribe("fast", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := fast.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	released := map[int64]int{}
	batch := func(vs ...int64) []Publish[int64] {
		ps := make([]Publish[int64], len(vs))
		for i, v := range vs {
			ps[i] = Publish[int64]{Payload: v, Size: 1, Release: func(x int64) { released[x]++ }}
		}
		return ps
	}
	dst := make([]Claim[int64], 1)
	drain := func(want int64) {
		t.Helper()
		if n, err := c.Fetch(poll(t), dst); n != 1 || err != nil {
			t.Fatalf("fetch: n=%d err=%v", n, err)
		}
		if dst[0].Payload != want {
			t.Fatalf("fetched %d, want %d", dst[0].Payload, want)
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatal(err)
		}
	}
	// One record fills both lanes; draining the fast one leaves exactly one
	// subscription full, so the next publication is a partial admission.
	if n, err := top.PublishBatch(context.Background(), batch(1)); n != 1 || err != nil {
		t.Fatalf("first batch = %d, %v", n, err)
	}
	drain(1)

	// Element 0 is admitted to "fast" and refused by "slow"; element 1 finds
	// both of them full and is refused outright. The batch stops at element
	// 1, which is the element the caller retries.
	n, err := top.PublishBatch(context.Background(), batch(2, 3))
	if n != 1 || !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("PublishBatch = %d, %v; want 1, ErrQueueFull: element 0 was admitted", n, err)
	}
	if errors.Is(err, errcode.ErrPartial) {
		t.Fatalf("the batch reported the partial element as the one to retry: %v", err)
	}
	if got := released[2]; got != 0 {
		t.Fatalf("an admitted payload came back %d times while the engine holds it", got)
	}
	if got := released[3]; got != 0 {
		t.Fatalf("a refused payload was released %d times: ownership stays with the caller", got)
	}
	drain(2)

	// A batch in which every element is a partial admission has nothing to
	// retry: it reports the whole length beside errcode.ErrPartial.
	if n, err := top.PublishBatch(context.Background(), batch(4)); n != 1 || !errors.Is(err, errcode.ErrPartial) {
		t.Fatalf("wholly partial batch = %d, %v; want 1, ErrPartial", n, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int64{1, 2, 4} {
		if got := released[v]; got != 1 {
			t.Fatalf("payload %d, which the engine took, was released %d times, want once", v, got)
		}
	}
	if got := released[3]; got != 0 {
		t.Fatalf("the refused payload was released %d times, want never", got)
	}
}

// TestSubscriptionIsolation: subscriptions have independent settlement state
// and independent backpressure.
func TestSubscriptionIsolation(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := top.Subscribe("a", SubscriptionConfig{})
	b, _ := top.Subscribe("b", SubscriptionConfig{})
	ca, _ := a.Attach(ConsumerConfig{})
	released := 0
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 1, Size: 4, Release: func(int64) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 1)
	if n, _ := ca.Fetch(poll(t), dst); n != 1 {
		t.Fatal("subscription a saw nothing")
	}
	if err := ca.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if released != 0 {
		t.Fatal("the payload was released while subscription b still holds it")
	}
	if s := b.Inspect(); s.Ready != 1 || s.Acked != 0 {
		t.Fatalf("subscription b was disturbed: %+v", s)
	}
	cb, _ := b.Attach(ConsumerConfig{})
	if n, _ := cb.Fetch(poll(t), dst); n != 1 {
		t.Fatal("subscription b saw nothing")
	}
	if err := cb.Ack(dst[0].Env, dst[0].Gen); err != nil {
		t.Fatal(err)
	}
	if released != 1 {
		t.Fatalf("payload released %d times after the last settlement", released)
	}
}

// TestSettlementErrors walks the settlement misuse table.
func TestSettlementErrors(t *testing.T) {
	e := newTestEngine(t, KindSet{Pool: pool.None}, TopologyConfig{Partitions: 1})
	top, sub, c := queueSub(t, e)
	other, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 1)
	if n, _ := c.Fetch(poll(t), dst); n != 1 {
		t.Fatal("nothing fetched")
	}
	cl := dst[0]
	if err := other.Ack(cl.Env, cl.Gen); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("foreign consumer: %v", err)
	}
	if err := c.Ack(cl.Env, cl.Gen+1); !errors.Is(err, errcode.ErrLeaseLost) {
		t.Fatalf("stale generation: %v", err)
	}
	if err := c.Ack(nil, cl.Gen); !errors.Is(err, errcode.ErrNotSettleable) {
		t.Fatalf("nil envelope: %v", err)
	}
	if err := c.Ack(&Envelope[int64]{}, 0); !errors.Is(err, errcode.ErrNotSettleable) {
		t.Fatalf("zero envelope: %v", err)
	}
	if err := c.Extend(cl.Env, cl.Gen, time.Minute); err != nil {
		t.Fatalf("extend: %v", err)
	}
	if err := c.Ack(cl.Env, cl.Gen); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := c.Ack(cl.Env, cl.Gen); !errors.Is(err, errcode.ErrAlreadySettled) {
		t.Fatalf("double ack: %v", err)
	}
	if err := c.Nack(cl.Env, cl.Gen, 0); !errors.Is(err, errcode.ErrAlreadySettled) {
		t.Fatalf("nack after ack: %v", err)
	}
	if err := c.Extend(cl.Env, cl.Gen, time.Minute); !errors.Is(err, errcode.ErrAlreadySettled) {
		t.Fatalf("extend after ack: %v", err)
	}
	if s := sub.Inspect(); s.Acked != 1 || s.ActiveConsumers != 2 {
		t.Fatalf("%+v", s)
	}
}

// TestConsumerCloseRequeues follows the close policy.
func TestConsumerCloseRequeues(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, sub, c := queueSub(t, e)
	for i := range 3 {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 3)
	n, _ := c.Fetch(poll(t), dst)
	if n != 3 {
		t.Fatalf("fetched %d", n)
	}
	held := dst[0]
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("Close is not idempotent")
	}
	s := sub.Inspect()
	if s.Ready != 3 || s.InFlight != 0 || s.Requeued != 3 || s.ActiveConsumers != 0 {
		t.Fatalf("%+v", s)
	}
	if err := c.Ack(held.Env, held.Gen); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("ack after consumer close: %v", err)
	}
	next, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// The records went back through the retry placement. The
	// default kind blocks the lane, so they come back one at a time, each
	// after the previous one is settled; a placement that does not seal
	// hands the whole batch over at once.
	seen := 0
	for range 3 {
		n, _ = next.Fetch(poll(t), dst)
		if n == 0 {
			break
		}
		for i := range n {
			if dst[i].Info.Attempts != 2 {
				t.Fatalf("attempts = %d after redelivery", dst[i].Info.Attempts)
			}
			if err := next.Ack(dst[i].Env, dst[i].Gen); err != nil {
				t.Fatalf("ack: %v", err)
			}
			seen++
		}
	}
	if seen != 3 {
		t.Fatalf("redelivered %d of 3", seen)
	}
}

// TestLeaseExpiryRedelivers drives the fault injector.
func TestLeaseExpiryRedelivers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, AckDeadline: 30 * time.Millisecond})
		top, sub, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("nothing fetched")
		}
		if n := e.Faults().ExpireLeases(); n != 0 {
			t.Fatalf("a live lease expired: %d", n)
		}
		time.Sleep(10 * time.Millisecond)
		if err := c.Extend(dst[0].Env, dst[0].Gen, 60*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(40 * time.Millisecond)
		synctest.Wait()
		if n := e.Faults().ExpireLeases(); n != 0 {
			t.Fatalf("an extended lease expired: %d", n)
		}
		time.Sleep(40 * time.Millisecond)
		// The engine scheduler holds one entry per lane on the lease head,
		// so the deadline reclaims the lease on its own: the fault injector
		// finds nothing left to do.
		synctest.Wait()
		if n := e.Faults().ExpireLeases(); n != 0 {
			t.Fatalf("the scheduler left an expired lease behind: %d", n)
		}
		s := sub.Inspect()
		if s.Ready != 1 || s.Expired != 1 || s.Requeued != 1 {
			t.Fatalf("%+v", s)
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); !errors.Is(err, errcode.ErrLeaseLost) {
			t.Fatalf("ack after expiry: %v", err)
		}
		if n, _ := c.Fetch(poll(t), dst); n != 1 || dst[0].Info.Attempts != 2 {
			t.Fatalf("redelivery: n=%d attempts=%d", n, dst[0].Info.Attempts)
		}
	})
}

// TestDelayedDelivery: a delayed record is never delivered before it is due.
func TestDelayedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, sub, c := queueSub(t, e)
		at := time.Now().Add(50 * time.Millisecond)
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1, Delay: 20 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 2, Size: 1, DeliverAt: at}); err != nil {
			t.Fatal(err)
		}
		dst := make([]Claim[int64], 2)
		if n, _ := c.Fetch(poll(t), dst); n != 0 {
			t.Fatalf("delivered %d records early", n)
		}
		if s := sub.Inspect(); s.Delayed != 2 || s.Ready != 0 {
			t.Fatalf("%+v", s)
		}
		time.Sleep(25 * time.Millisecond)
		n, _ := c.Fetch(poll(t), dst)
		if n != 1 || dst[0].Payload != 1 {
			t.Fatalf("n=%d payload=%d", n, dst[0].Payload)
		}
		time.Sleep(30 * time.Millisecond)
		n, _ = c.Fetch(poll(t), dst)
		if n != 1 || dst[0].Payload != 2 {
			t.Fatalf("n=%d payload=%d", n, dst[0].Payload)
		}
		// A due time in the past is delivered at once.
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: 3, Size: 1, DeliverAt: time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatalf("a record due in the past was held back: %d", n)
		}
	})
}

// TestNackDelayRequeues holds a nacked record back.
func TestNackDelayRequeues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, sub, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
			t.Fatal(err)
		}
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("nothing fetched")
		}
		if err := c.Nack(dst[0].Env, dst[0].Gen, 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if s := sub.Inspect(); s.Delayed != 1 || s.Nacked != 1 || s.Requeued != 1 {
			t.Fatalf("%+v", s)
		}
		if n, _ := c.Fetch(poll(t), dst); n != 0 {
			t.Fatal("a delayed nack was redelivered early")
		}
		time.Sleep(25 * time.Millisecond)
		if n, _ := c.Fetch(poll(t), dst); n != 1 || dst[0].Info.Attempts != 2 {
			t.Fatalf("redelivery after the nack delay failed")
		}
	})
}

// TestFetchBlocksUntilPublished checks the wait wiring for every strategy: a
// fetch that finds nothing waits, and a publish wakes it. It runs outside a
// synctest bubble because the spinning strategies never block durably, which
// is exactly what a bubble waits for.
func TestFetchBlocksUntilPublished(t *testing.T) {
	for _, wk := range wait.Kinds() {
		t.Run(wk.String(), func(t *testing.T) {
			t.Parallel()
			e := newTestEngine(t, KindSet{Wait: wk}, TopologyConfig{Partitions: 1})
			top, _, c := queueSub(t, e)
			done := make(chan int64, 1)
			go func() {
				dst := make([]Claim[int64], 1)
				n, err := c.Fetch(context.Background(), dst)
				if err != nil || n != 1 {
					done <- -1
					return
				}
				done <- dst[0].Payload
			}()
			// The fetch loop re-checks the lane after registering as a
			// waiter, so the publication is seen whichever order the two
			// reach it: there is no window for a lost wake-up.
			if err := top.Publish(context.Background(), Publish[int64]{Payload: 42, Size: 1}); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-done:
				if got != 42 {
					t.Fatalf("blocked fetch returned %d", got)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the fetch never woke: lost wake-up")
			}
		})
	}
}

// TestFetchWakesOnClose: a parked consumer is woken by Close, whatever the
// wait strategy.
func TestFetchWakesOnClose(t *testing.T) {
	for _, wk := range wait.Kinds() {
		t.Run(wk.String(), func(t *testing.T) {
			t.Parallel()
			e := newTestEngine(t, KindSet{Wait: wk}, TopologyConfig{Partitions: 1})
			_, _, c := queueSub(t, e)
			done := make(chan error, 1)
			go func() {
				_, err := c.Fetch(context.Background(), make([]Claim[int64], 1))
				done <- err
			}()
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, errcode.ErrClosed) {
					t.Fatalf("parked fetch returned %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not wake the parked consumer")
			}
		})
	}
}

// TestFetchHonoursContext returns the context error rather than blocking.
func TestFetchHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		_, _, c := queueSub(t, e)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.Fetch(ctx, make([]Claim[int64], 1))
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled fetch: %v", err)
		}
		if n, err := c.Fetch(context.Background(), nil); n != 0 || err != nil {
			t.Fatalf("empty destination: %d, %v", n, err)
		}
	})
}

// TestPrefetchBoundsFetch checks ConsumerConfig.Prefetch.
func TestPrefetchBoundsFetch(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, sub, _ := queueSub(t, e)
	c, err := sub.Attach(ConsumerConfig{Prefetch: 2})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	n, _ := c.Fetch(poll(t), make([]Claim[int64], 5))
	if n != 2 {
		t.Fatalf("prefetch 2 returned %d claims", n)
	}
	if c.ID() == 0 || c.Subscription() != sub {
		t.Fatal("consumer identity is wrong")
	}
}

// TestSubscriptionCloseDetaches drops the subscription's records and stops
// future publications from reaching it.
func TestSubscriptionCloseDetaches(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := top.Subscribe("a", SubscriptionConfig{})
	b, _ := top.Subscribe("b", SubscriptionConfig{})
	released := 0
	pub := func(v int64) {
		if err := top.Publish(context.Background(), Publish[int64]{
			Payload: v, Size: 1, Release: func(int64) { released++ },
		}); err != nil {
			t.Fatal(err)
		}
	}
	pub(1)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal("Close is not idempotent")
	}
	if !a.Closed() {
		t.Fatal("Closed() is false after Close")
	}
	if s := a.Inspect(); s.Ready != 0 || s.Dropped != 1 {
		t.Fatalf("%+v", s)
	}
	if released != 0 {
		t.Fatal("the payload was released while subscription b holds it")
	}
	pub(2)
	if s := b.Inspect(); s.Published != 2 {
		t.Fatalf("b: %+v", s)
	}
	if s := a.Inspect(); s.Published != 1 {
		t.Fatalf("a still receives publications: %+v", s)
	}
	if _, err := a.Attach(ConsumerConfig{}); !errors.Is(err, errcode.ErrClosed) {
		t.Fatalf("attach to a closed subscription: %v", err)
	}
}

// TestCloseReleasesEverything checks the close policy and the post-close
// contract.
func TestCloseReleasesEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 2})
		top, sub, c := queueSub(t, e)
		released := map[int64]int{}
		pub := func(v int64, delay time.Duration) {
			if err := top.Publish(context.Background(), Publish[int64]{
				Payload: v, Size: 2, Partition: -1, Delay: delay,
				Release: func(x int64) { released[x]++ },
			}); err != nil {
				t.Fatal(err)
			}
		}
		pub(1, 0)
		pub(2, 0)
		pub(3, time.Hour)
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("nothing fetched")
		}
		held := dst[0]
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal("Close is not idempotent")
		}
		if !e.Closed() {
			t.Fatal("Closed() is false")
		}
		for v := int64(1); v <= 3; v++ {
			if released[v] != 1 {
				t.Fatalf("payload %d released %d times", v, released[v])
			}
		}
		s := sub.Inspect()
		if s.Ready != 0 || s.InFlight != 0 || s.Delayed != 0 || s.Dropped != 3 {
			t.Fatalf("%+v", s)
		}
		if !s.Conserved() {
			t.Fatalf("not conserved: %+v", s)
		}
		if err := c.Ack(held.Env, held.Gen); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("ack after close: %v", err)
		}
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 9, Size: 1}); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("publish after close: %v", err)
		}
		if _, err := e.CreateTopic("u", TopologyConfig{}); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("CreateTopic after close: %v", err)
		}
		if _, err := c.Fetch(context.Background(), dst); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("fetch after close: %v", err)
		}
	})
}

// TestWaitIdle drains and then reports idle.
func TestWaitIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
		top, _, c := queueSub(t, e)
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1, Delay: time.Second}); err != nil {
			t.Fatal(err)
		}
		if err := e.WaitIdle(context.Background(), IdleDelivered); err != nil {
			t.Fatalf("WaitIdle ignoring delayed: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := e.WaitIdle(ctx, IdleIncludingDelayed); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitIdle including delayed: %v", err)
		}
		time.Sleep(2 * time.Second)
		dst := make([]Claim[int64], 1)
		if n, _ := c.Fetch(poll(t), dst); n != 1 {
			t.Fatal("the delayed record never came due")
		}
		if err := c.Ack(dst[0].Env, dst[0].Gen); err != nil {
			t.Fatal(err)
		}
		if err := e.WaitIdle(context.Background(), IdleIncludingDelayed); err != nil {
			t.Fatalf("WaitIdle after the drain: %v", err)
		}
	})
}

// TestSafeCopyIsIndependent: under the Safe kind set each subscription gets
// its own clone and the publisher's own value comes straight back.
func TestSafeCopyIsIndependent(t *testing.T) {
	clones := 0
	e, err := New(Config[int64]{
		Kinds:           KindSet{Safe: true},
		Clone:           func(v int64) int64 { clones++; return v * 10 },
		Zero:            func(int64) {},
		DefaultTopology: TopologyConfig{Partitions: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	top, _ := e.CreateTopic("t", TopologyConfig{})
	a, _ := top.Subscribe("a", SubscriptionConfig{})
	b, _ := top.Subscribe("b", SubscriptionConfig{})
	released := 0
	if err := top.Publish(context.Background(), Publish[int64]{
		Payload: 7, Size: 1, Release: func(int64) { released++ },
	}); err != nil {
		t.Fatal(err)
	}
	if clones != 2 || released != 1 {
		t.Fatalf("clones=%d released=%d", clones, released)
	}
	dst := make([]Claim[int64], 1)
	for _, s := range []*Subscription[int64]{a, b} {
		c, _ := s.Attach(ConsumerConfig{})
		if n, _ := c.Fetch(poll(t), dst); n != 1 || dst[0].Payload != 70 {
			t.Fatalf("subscription %s got %d", s.Name(), dst[0].Payload)
		}
	}
}

// TestUnboundedLane admits far more than the default capacity.
func TestUnboundedLane(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: -1})
	top, sub, _ := queueSub(t, e)
	const n = DefaultCapacityMessages + 100
	for i := range n {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(i), Size: 1}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if s := sub.Inspect(); s.Ready != n {
		t.Fatalf("ready = %d of %d", s.Ready, n)
	}
}

// TestPublishHonoursContext refuses a publication on a cancelled context.
func TestPublishHonoursContext(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{})
	top, sub, _ := queueSub(t, e)
	if err := top.Publish(poll(t), Publish[int64]{Payload: 1, Size: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled publish: %v", err)
	}
	if s := sub.Inspect(); s.Published != 0 {
		t.Fatalf("%+v", s)
	}
}

// TestEnvelopeAccessors covers the diagnostic accessors.
func TestEnvelopeAccessors(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1})
	top, _, c := queueSub(t, e)
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 3}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 1)
	if n, _ := c.Fetch(poll(t), dst); n != 1 {
		t.Fatal("nothing fetched")
	}
	env := dst[0].Env
	if env.Gen() != dst[0].Gen || env.Attempts() != 1 || env.Seq() != dst[0].Info.Seq {
		t.Fatalf("accessors: gen=%d attempts=%d seq=%d", env.Gen(), env.Attempts(), env.Seq())
	}
	if dst[0].Info.Size != 3 {
		t.Fatalf("size = %d", dst[0].Info.Size)
	}
}

// TestARecordOfTwoGiBOrMoreIsBookedAtItsSize publishes a record whose
// accounted size does not fit in 32 bits. The engine booked it at its 64-bit
// size and kept a 32-bit copy on the record, so the settlement gave back a
// negative size: the lane and the engine stayed full for good, and the
// delivery reported a negative Size. Checked on every architecture, the
// shared log's slot included, and on a stream replay, which copies the size
// from the retained entry.
func TestARecordOfTwoGiBOrMoreIsBookedAtItsSize(t *testing.T) {
	if math.MaxInt < 3<<30 {
		t.Skip("Publish.Size is an int, which cannot name a record this large here")
	}
	// size is a variable, not a constant, so that the file still compiles
	// where an int has 32 bits.
	size := int64(3) << 30
	topo := TopologyConfig{
		Partitions: 1, CapacityBytes: 4 << 30, AckDeadline: -1, Overflow: OverflowReject,
	}
	for _, arch := range Arches() {
		t.Run(arch.String(), func(t *testing.T) {
			e, err := New(Config[int64]{
				Kinds: KindSet{Arch: arch}, Clone: func(v int64) int64 { return v },
				DefaultTopology: topo, MaxTotalBytes: 4 << 30,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = e.Close() })
			top, sub, c := queueSub(t, e)
			for round := range 2 {
				if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: int(size)}); err != nil {
					t.Fatalf("round %d: Publish: %v", round, err)
				}
				if snap := sub.Inspect(); snap.Bytes != size {
					t.Fatalf("round %d: subscription holds %d bytes, want %d", round, snap.Bytes, size)
				}
				cl := fetchSeqs(t, c, 1)
				if len(cl) != 1 {
					t.Fatalf("round %d: fetch = %v", round, cl)
				}
				if got := cl[0].Info.Size; got != size {
					t.Fatalf("round %d: Info.Size = %d, want %d", round, got, size)
				}
				if err := c.Ack(cl[0].Env, cl[0].Gen); err != nil {
					t.Fatalf("round %d: Ack: %v", round, err)
				}
				if msgs, bytes, _ := e.Totals(); msgs != 0 || bytes != 0 {
					t.Fatalf("round %d: engine totals after the ack: msgs=%d bytes=%d", round, msgs, bytes)
				}
				if snap := sub.Inspect(); snap.Bytes != 0 {
					t.Fatalf("round %d: subscription holds %d bytes after the ack", round, snap.Bytes)
				}
			}
		})
	}
	t.Run("stream-replay", func(t *testing.T) {
		e, err := New(Config[int64]{
			Kinds: KindSet{Arch: featureArch}, Clone: func(v int64) int64 { return v },
			DefaultTopology: topo,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = e.Close() })
		top, err := e.CreateTopic("t", TopologyConfig{
			Kind: TopologyStream, Retention: Retention{MaxMessages: 1},
		})
		if err != nil {
			t.Fatalf("CreateTopic: %v", err)
		}
		if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: int(size), Mode: CopySafe}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{Start: StartPosition{Kind: StartEarliest}})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		c, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		cl := fetchSeqs(t, c, 1)
		if len(cl) != 1 {
			t.Fatalf("fetch = %v", cl)
		}
		if got := cl[0].Info.Size; got != size {
			t.Fatalf("replayed Info.Size = %d, want %d", got, size)
		}
		if err := c.Ack(cl[0].Env, cl[0].Gen); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		if snap := sub.Inspect(); snap.Bytes != 0 {
			t.Fatalf("subscription holds %d bytes after the ack", snap.Bytes)
		}
	})
}

// TestConsumerCloseWakesItsOwnParkedFetch is the wake-on-close obligation at
// the level a driver's Source.Close reaches it: closing ONE consumer, rather
// than the subscription or the engine, must unblock that consumer's parked
// Fetch (the message contract).
//
// Consumer.Close used to notify the subscription's waiters only when it had
// requeued something, so a consumer that was parked on an EMPTY lane — which
// is the only state a parked fetch can be in — was never woken and its Fetch
// waited for its context instead. It is the same shape as the withdrawn
// reservations and a state change that satisfies
// a waiter's condition, announced on one path and not on the other.
//
// It is run for every wait kind because the parking is the wait strategy's,
// and a kind that loses a wake loses it here.
func TestConsumerCloseWakesItsOwnParkedFetch(t *testing.T) {
	for _, wk := range wait.Kinds() {
		t.Run(wk.String(), func(t *testing.T) {
			t.Parallel()
			e := newTestEngine(t, KindSet{Wait: wk}, TopologyConfig{Partitions: 1})
			_, sub, c := queueSub(t, e)
			done := make(chan error, 1)
			go func() {
				_, err := c.Fetch(context.Background(), make([]Claim[int64], 1))
				done <- err
			}()
			// Synchronise on the consumer actually being parked rather than
			// on a sleep: a sleep proves nothing on a loaded host, where the
			// fetching goroutine may not have run yet.
			for !sub.HasWaiters() {
				runtime.Gosched()
			}
			if err := c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, errcode.ErrClosed) {
					t.Fatalf("parked fetch returned %v, want ErrClosed", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Consumer.Close did not wake its own parked fetch")
			}
		})
	}
}

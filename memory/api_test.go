package memory_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

//
// The point is not coverage: the engine keeps its own tests. The point is
// that the adapter does not LOSE a property the engine has — a default it
// overrides, an error it flattens, a gauge it stops reporting — which is a
// thing only a test written against the public surface can notice. Each test
// below names the engine test it corresponds to.
//
// Three of the properties cannot be expressed here at all, and they are
// named in the hand-off rather than approximated: the envelope accessors
// (Envelope is opaque by contract and no public type exposes Seq or
// Attempts), a settlement presenting a forged generation (the settlement
// handle is unexported, so a caller cannot build a stale one), and the
// subscription's Seek, explicit Rebalance and assignment table (no public
// surface reaches them in).

// ---------------------------------------------------------------------------
// Configuration and topology
// ---------------------------------------------------------------------------

// TestConfigIsValidated re-expresses TestNewValidatesConfig: a configuration
// the engine cannot honour is refused where it was written.
func TestConfigIsValidated(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		cfg  memory.Config
		code mq.ErrorCode
	}{
		{"negative total messages", memory.Config{MaxTotalMessages: -1}, mq.CodeInvalidConfig},
		{"negative total bytes", memory.Config{MaxTotalBytes: -1}, mq.CodeInvalidConfig},
		{"negative scheduler resolution", memory.Config{SchedulerResolution: -time.Second}, mq.CodeInvalidConfig},
		{"topology kind out of range", memory.Config{
			DefaultTopology: memory.TopologyConfig{Kind: memory.TopologyKind(99)},
		}, mq.CodeInvalidConfig},
		{"ordering out of range", memory.Config{
			DefaultTopology: memory.TopologyConfig{Ordering: memory.OrderingMode(99)},
		}, mq.CodeInvalidConfig},
		{"overflow out of range", memory.Config{
			DefaultTopology: memory.TopologyConfig{Overflow: memory.OverflowPolicy(99)},
		}, mq.CodeInvalidConfig},
		{"fanout out of range", memory.Config{
			DefaultTopology: memory.TopologyConfig{Fanout: memory.FanoutPolicy(99)},
		}, mq.CodeInvalidConfig},
		{"MaxInFlight is not a separate bound", memory.Config{
			DefaultTopology: memory.TopologyConfig{MaxInFlight: 8},
		}, mq.CodeInvalidConfig},
		{"MaxDelayed is not a separate bound", memory.Config{
			DefaultTopology: memory.TopologyConfig{MaxDelayed: 8},
		}, mq.CodeInvalidConfig},
		{"a lane needs an MPMC ready queue", memory.Config{
			Engine: memory.EngineConfig{Queue: memory.QueueSPSC},
		}, mq.CodeInvalidConfig},
		{"a scheduler finer than the kind provides", memory.Config{
			SchedulerResolution: time.Microsecond,
			Engine:              memory.EngineConfig{Timer: memory.TimerHashedWheel},
		}, mq.CodeInvalidConfig},
		{"a stream retention on a topic", memory.Config{
			DefaultTopology: memory.TopologyConfig{
				Kind:      memory.TopologyTopic,
				Retention: memory.Retention{MaxMessages: 4},
			},
		}, mq.CodeInvalidConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := memory.NewBroker(tc.cfg); mq.CodeOf(err) != tc.code {
				t.Fatalf("NewBroker: %v (code %v), want %v", err, mq.CodeOf(err), tc.code)
			}
			if _, err := memory.Open(ctx, tc.cfg); mq.CodeOf(err) != tc.code {
				t.Fatalf("Open: %v (code %v), want %v", err, mq.CodeOf(err), tc.code)
			}
		})
	}
}

// TestTopologyDefaults re-expresses TestTopologyDefaults: the zero topology is
// ADR-022's bounded, ordered configuration, and its two opt-outs are
// negative values rather than zero.
func TestTopologyDefaults(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t)
	// One partition: every delivery comes from lane 0, whatever the key.
	dd := receiveViaSPI(t, ctx, b, "orders", "billing", func() {
		publish(t, ctx, b, "orders", &mq.Message{Key: []byte("a"), Body: []byte("1")})
		publish(t, ctx, b, "orders", &mq.Message{Key: []byte("b"), Body: []byte("2")})
		publish(t, ctx, b, "orders", &mq.Message{Key: []byte("c"), Body: []byte("3")})
	}, 3)
	for i := range dd {
		if !dd[i].Info.HasPartition || dd[i].Info.Partition != 0 {
			t.Fatalf("delivery %d came from partition %d (has=%v), want lane 0 on a one-partition default",
				i, dd[i].Info.Partition, dd[i].Info.HasPartition)
		}
	}
	// A 30-second lease, which the delivery reports as its expiry.
	want := b.Clock().Now().Add(memory.DefaultAckDeadline)
	if got := dd[0].Info.LeaseExpires; got.Before(want.Add(-time.Second)) || got.After(want.Add(time.Second)) {
		t.Fatalf("lease expires at %v, want about %v", got, want)
	}
	// Bounded at 65,536 records: a publisher that overruns it would block
	// under the default Block policy, so the bound is read from the refusal
	// a Reject broker gives instead.
	if memory.DefaultCapacityMessages != 65536 {
		t.Fatalf("DefaultCapacityMessages is %d, want ADR-022's 65,536", memory.DefaultCapacityMessages)
	}
	if memory.DefaultPartitions != 1 {
		t.Fatalf("DefaultPartitions is %d, want ADR-022's 1", memory.DefaultPartitions)
	}
}

// TestTopologyDefaultOverflowBlocks pins the overflow default, which the
// engine's own default contradicts: the engine reads a zero policy as
// Reject and this adapter applies Block above it.
func TestTopologyDefaultOverflowBlocks(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{CapacityMessages: 1}))
	// A subscription has to exist for a publication to be admitted to
	// anything; without one the topic discards it and the lane never fills.
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "t", Subscription: "s"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "t"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("one")}); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	// The lane now holds its one record. A second publication must PARK
	// rather than be refused, so it ends with the context rather than with
	// ErrQueueFull — which is how "Block" is observable without a consumer.
	deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = p.Publish(deadline, &mq.Message{Body: []byte("two")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a full lane answered %v (code %v), want the publisher to park until its context ended",
			err, mq.CodeOf(err))
	}
	if snap, ierr := b.Inspect("t", "s"); ierr != nil || snap.Ready != 1 || snap.Rejected != 0 {
		t.Fatalf("Inspect %+v err %v, want one ready record and no rejection", snap, ierr)
	}
}

// TestUnboundedLaneIsAskedForWithANegativeCapacity re-expresses
// TestUnboundedLane, and pins the asymmetry ADR-022 the specification requires: "I did
// not set it" and "I want no limit" are not the same value.
func TestUnboundedLaneIsAskedForWithANegativeCapacity(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: -1,
		Overflow:         memory.OverflowReject,
	}))
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "t", Subscription: "s"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "t"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	// Far more than any bounded default would hold, with nothing consuming.
	const n = 70000
	for i := range n {
		if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	snap, err := b.Inspect("t", "s")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != n {
		t.Fatalf("an unbounded lane holds %d records, want %d", snap.Ready, n)
	}
}

// TestPartitionsOptOutIsNegative pins the other half a
// negative count asks for GOMAXPROCS lanes, and zero is the ordered default.
func TestPartitionsOptOutIsNegative(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Partitions: -1}))
	seen := map[int32]bool{}
	dd := receiveViaSPI(t, ctx, b, "orders", "billing", func() {
		for i := range 64 {
			publish(t, ctx, b, "orders", &mq.Message{Body: []byte{byte(i)}})
		}
	}, 64)
	for i := range dd {
		seen[dd[i].Info.Partition] = true
	}
	if len(seen) < 2 {
		t.Fatalf("a GOMAXPROCS-partitioned topic served %d distinct lanes, want more than one", len(seen))
	}
}

// ---------------------------------------------------------------------------
// Topology administration
// ---------------------------------------------------------------------------

// TestAutoCreateIsOffByDefault pins ADR-016's Open never creates
// production topology, and NewForTest does.
func TestAutoCreateIsOffByDefault(t *testing.T) {
	ctx := context.Background()
	client, err := memory.Open(ctx, memory.Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"}); !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("Publisher on an unknown topic: %v, want ErrTopicNotFound", err)
	}
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("Consumer on an unknown topic: %v, want ErrTopicNotFound", err)
	}
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
		t.Fatalf("EnsureTopic: %v", err)
	}
	if _, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"}); err != nil {
		t.Fatalf("Publisher after EnsureTopic: %v", err)
	}
}

// TestEnsureTopicIsIdempotentAndValidated re-expresses
// TestCreateTopicIsIdempotentAndValidated.
func TestEnsureTopicIsIdempotentAndValidated(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	for range 3 {
		if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: "orders"}); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
	}
	names := []string{}
	for name, err := range adm.ListTopics(ctx) {
		if err != nil {
			t.Fatalf("ListTopics: %v", err)
		}
		names = append(names, name)
	}
	if len(names) != 1 || names[0] != "orders" {
		t.Fatalf("ListTopics returned %v, want exactly [orders]", names)
	}
	if err := adm.EnsureTopic(ctx, mq.TopicSpec{Name: ""}); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("EnsureTopic with no name: %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestEnsureSubscriptionIsIdempotent re-expresses TestSubscribeIsIdempotent,
// and pins what a subscription created before a publication is for: the
// records arrive whether or not a consumer is attached.
func TestEnsureSubscriptionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	for range 3 {
		if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: "billing"}); err != nil {
			t.Fatalf("EnsureSubscription: %v", err)
		}
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != 1 || snap.ActiveConsumers != 0 {
		t.Fatalf("snapshot %+v, want one ready record and no consumer", snap)
	}
	if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: ""}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("EnsureSubscription with no name: %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestDeleteSubscriptionDetaches re-expresses TestSubscriptionCloseDetaches:
// the subscription stops receiving publications and a consumer still on it
// answers ErrClosed.
func TestDeleteSubscriptionDetaches(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.DeleteSubscription(ctx, "orders", "billing"); err != nil {
		t.Fatalf("DeleteSubscription: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish after the subscription went: %v", err)
	}
	if _, err := b.Inspect("orders", "billing"); mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("Inspect of a deleted subscription: %v (code %v)", err, mq.CodeOf(err))
	}
	if _, err := c.Receive(ctx, make([]mq.Delivery, 1)); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Receive on a detached subscription: %v, want ErrClosed", err)
	}
	if _, err := adm.Stats(ctx, "orders", "billing"); mq.CodeOf(err) != mq.CodeNotFound {
		t.Fatalf("Stats of a deleted subscription: %v (code %v)", err, mq.CodeOf(err))
	}
}

// TestDeleteTopicIsUnsupported pins the engine's registry
// has no removal, and the adapter says so rather than half-deleting.
func TestDeleteTopicIsUnsupported(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t, memory.TestTopics("orders"))
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.DeleteTopic(ctx, "orders"); !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("DeleteTopic: %v, want ErrUnsupported", err)
	}
	// A topic that does not exist is reported as missing rather than as
	// unsupported: a caller deleting something absent learns which.
	if err := adm.DeleteTopic(ctx, "absent"); !errors.Is(err, mq.ErrTopicNotFound) {
		t.Fatalf("DeleteTopic of an unknown topic: %v, want ErrTopicNotFound", err)
	}
}

// TestPurgeDiscardsTheReadyBacklog pins the emulation, including what it does NOT reach: an in-flight record and one that is not
// yet due.
func TestPurgeDiscardsTheReadyBacklog(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	for range 8 {
		if err := p.Publish(ctx, &mq.Message{Body: []byte("ready")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("later")}, mq.Delay(time.Hour)); err != nil {
		t.Fatalf("delayed Publish: %v", err)
	}
	// Take one delivery and hold its lease across the purge.
	dst := make([]mq.Delivery, 1)
	if n, rerr := c.Receive(ctx, dst); rerr != nil || n != 1 {
		t.Fatalf("Receive: %d, %v", n, rerr)
	}
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	if err := adm.Purge(ctx, "orders", "billing"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != 0 {
		t.Fatalf("Purge left %d ready records", snap.Ready)
	}
	if snap.InFlight != 1 {
		t.Fatalf("Purge took the in-flight record: InFlight=%d, want 1", snap.InFlight)
	}
	if snap.Delayed != 1 {
		t.Fatalf("Purge took the delayed record: Delayed=%d, want 1", snap.Delayed)
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatalf("the held delivery is still settleable: %v", err)
	}
}

// TestStatsReportsDepthInFlightAndLag pins the three figures, each of
// them Known: a broker that can report a figure must not answer with an
// unknown zero.
func TestStatsReportsDepthInFlightAndLag(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	for range 4 {
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	dst := make([]mq.Delivery, 1)
	if n, rerr := c.Receive(ctx, dst); rerr != nil || n != 1 {
		t.Fatalf("Receive: %d, %v", n, rerr)
	}
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	st, err := adm.Stats(ctx, "orders", "billing")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if !st.Depth.Known || !st.InFlight.Known || !st.Lag.Known {
		t.Fatalf("Stats %+v: this broker knows all three figures", st)
	}
	if st.Depth.Value != 3 || st.InFlight.Value != 1 || st.Lag.Value != 3 {
		t.Fatalf("Stats %+v, want depth 3, in flight 1, lag 3", st)
	}
	_ = dst[0].Ack()
}

// ---------------------------------------------------------------------------
// Capabilities and the strict defaults of ADR-010
// ---------------------------------------------------------------------------

// TestConfirmDurableFailsAtConstruction is ADR-010's this broker has
// no durable boundary to define, so a publisher that asks for one fails where
// it was configured rather than at its first message.
func TestConfirmDurableFailsAtConstruction(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	_, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders", Confirmation: mq.ConfirmDurable})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("ConfirmDurable: %v, want ErrUnsupported", err)
	}
	for _, mode := range []mq.ConfirmationMode{mq.ConfirmDefault, mq.ConfirmNone, mq.ConfirmAccepted} {
		p, perr := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders", Confirmation: mode})
		if perr != nil {
			t.Fatalf("Confirmation %v: %v", mode, perr)
		}
		if cerr := p.Close(); cerr != nil {
			t.Fatalf("Close: %v", cerr)
		}
	}
}

// TestCapabilitiesAreHonest checks the claims a caller's Requirements are
// checked against: what the broker says it does natively, what it says it
// emulates volatilely, and what it says it cannot do at all.
func TestCapabilitiesAreHonest(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	caps := client.Capabilities()
	for _, f := range []mq.Feature{
		mq.FeatureAtLeastOnce, mq.FeatureExplicitAck, mq.FeatureNack,
		mq.FeatureConsumerGroups, mq.FeatureFanOut, mq.FeatureExtendLease,
		mq.FeatureBatchReceive, mq.FeatureBatchPublish,
		mq.FeatureDelayedRedelivery, mq.FeatureScheduledPublish, mq.FeatureMessageTTL,
		mq.FeatureNativeHeaders, mq.FeatureBinaryHeaders, mq.FeatureDuplicateHeaders,
		mq.FeatureAdmin, mq.FeatureOrderedByKey, mq.FeatureOrderedByPartition,
		mq.FeaturePublisherConfirm,
	} {
		if got := caps.Support(f).Level; got != mq.Native {
			t.Errorf("%v is %v, want native", f, got)
		}
	}
	// The specification: never persistent, never advertises durable storage.
	for _, f := range []mq.Feature{
		mq.FeatureDurableStorage, mq.FeatureDurableConfirm, mq.FeatureTransactions,
		mq.FeatureRequestReply, mq.FeatureAtomicBatchPublish, mq.FeatureBatchSettle,
		mq.FeatureReceipts, mq.FeatureNativeTracing, mq.FeatureIdempotentPublish,
		mq.FeatureServerFilter, mq.FeatureWildcardSubscribe,
		// At-least-once with a lease: a delivery nobody settled comes
		// back, so a consumer CAN see one publication twice and
		// "de-duplicated at-least-once delivery" is not what this
		// broker offers. The publication de-duplication it does offer
		// is FeatureDeduplication, checked below.
		mq.FeatureExactlyOnceDelivery,
	} {
		if got := caps.Support(f).Level; got != mq.Unsupported {
			t.Errorf("%v is %v, want unsupported", f, got)
		}
	}
	// A volatile emulation never satisfies a durability requirement, which
	// is what makes this the right level for an in-process guarantee.
	for _, f := range []mq.Feature{mq.FeatureDurableSubscriptions} {
		sup := caps.Support(f)
		if sup.Level != mq.EmulatedVolatile {
			t.Errorf("%v is %v, want emulatedvolatile", f, sup.Level)
		}
		if sup.Note == "" {
			t.Errorf("%v claims an emulation and explains nothing", f)
		}
		if sup.Level.Durable() {
			t.Errorf("%v claims to be durable", f)
		}
	}
	// Every conditional support states its condition, which the specification requires.
	for _, f := range []mq.Feature{
		mq.FeatureOrderedGlobally, mq.FeaturePriority, mq.FeatureDeduplication,
		mq.FeatureDeadLetter, mq.FeatureReplay, mq.FeatureSeekByTime, mq.FeatureSeekBySequence,
	} {
		sup := caps.Support(f)
		if sup.Level != mq.Conditional {
			t.Errorf("%v is %v, want conditional", f, sup.Level)
		}
		if sup.Note == "" {
			t.Errorf("%v is conditional and names no condition", f)
		}
	}
	// A requirement the broker cannot meet fails at construction.
	_, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic:        "orders",
		Subscription: "billing",
		Requirements: []mq.Requirement{mq.RequireAtLeast(mq.FeatureDurableStorage, mq.Native)},
	})
	if !errors.Is(err, mq.ErrUnsupported) {
		t.Fatalf("a durable-storage requirement: %v, want ErrUnsupported", err)
	}
}

// TestInfoNamesTheDriver pins the rule that an empty field is "the driver
// did not say" rather than a claim.
func TestInfoNamesTheDriver(t *testing.T) {
	client, b := memory.NewForTest(t)
	info := client.Info()
	if info.Name != memory.DriverName {
		t.Fatalf("Info().Name is %q, want %q", info.Name, memory.DriverName)
	}
	if info.BrokerVersion != "" {
		t.Fatalf("Info().BrokerVersion is %q: there is no broker to report one", info.BrokerVersion)
	}
	if !strings.Contains(info.ClientName, "engine") {
		t.Fatalf("Info().ClientName is %q, want the engine it wraps", info.ClientName)
	}
	// the escape hatch, and the type the specification documents it as.
	got, ok := client.Native().(*memory.Broker)
	if !ok || got != b {
		t.Fatalf("client.Native() is %T, want the *memory.Broker it was opened over", client.Native())
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if h := client.Health(); h.State != mq.HealthReady {
		t.Fatalf("Health is %v, want ready", h.State)
	}
}

// TestMemoryAdvertisesNoLeaseCeiling pins that this broker claims no
// MaxLease, because it has none.
//
// It used to advertise the default topology's `AckDeadline`, and those are two
// different facts: `AckDeadline` is the lease a delivery STARTS with, and
// MaxLease is the longest one the broker grants. `engine.Consumer.Extend` sets
// the deadline to `now + d` for any d -- the lane's scheduler is a list move,
// not a timer per delivery -- so an extension of an hour is granted exactly as
// readily as one of a second, and a table that said ten seconds was telling a
// caller something this broker does not do.
//
// The conformance kit's `capability/ExtendLease/works` now asks every driver
// that advertises the limit to refuse an extension past it, and it caught this
// one on its first run: advertised 10s, `Extend(11s)` granted.
// `drivers/pulsar` had already reached the same conclusion for its own
// emulated lease and states it in its own capability test; this is the other
// half of that pair.
func TestMemoryAdvertisesNoLeaseCeiling(t *testing.T) {
	client, _ := memory.NewForTest(t)
	if lim := client.Capabilities().Limits().MaxLease; lim.Known {
		t.Errorf("MaxLease is advertised as %v. This broker's Extend has no ceiling -- the engine sets "+
			"the deadline to now+d for any d -- so a caller told a maximum is told something false, and "+
			"the conformance kit's own probe fails the driver for granting past its claim", lim.Value)
	}

	// The default lease itself is unchanged: withdrawing the CLAIM must not
	// change what a delivery gets.
	//
	// The consumer is opened BEFORE the publication, because the default
	// topology fans out at publication time and a subscription created
	// afterwards has nothing to receive. The context is bounded so that a
	// mistake of that shape fails in a second rather than hanging until the
	// package's timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c2, _ := memory.NewForTest(t)
	cons, err := c2.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("opening a consumer: %v", err)
	}
	defer func() { _ = cons.Close() }()
	if err := c2.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	dst := make([]mq.Delivery, 1)
	n, err := cons.Receive(ctx, dst)
	if err != nil || n != 1 {
		t.Fatalf("receiving: n=%d err=%v", n, err)
	}
	d := dst[0]
	if d.Info().LeaseExpires.IsZero() {
		t.Error("the delivery has no lease deadline at all; withdrawing the advertised maximum must not " +
			"change what a delivery is actually granted")
	}
	// And the extension the probe would make is still accepted, which is
	// the behaviour that makes the claim wrong rather than the behaviour.
	if err := d.Extend(time.Hour); err != nil {
		t.Errorf("Extend(1h) was refused with %v; this broker grants any extension, which is exactly why "+
			"it must not advertise a ceiling", err)
	}
	_ = d.Ack()
}

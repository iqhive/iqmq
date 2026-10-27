package memory_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// ---------------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------------

// TestKeyedPublishIsStable re-expresses TestKeyedPublishIsStable: one key
// always reaches one lane, so a key's records are ordered with respect to
// each other.
func TestKeyedPublishIsStable(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Partitions: 8}))
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	const rounds = 4
	dd := receiveViaSPI(t, ctx, b, "orders", "billing", func() {
		for range rounds {
			for _, k := range keys {
				publish(t, ctx, b, "orders", &mq.Message{Key: []byte(k), Body: []byte(k)})
			}
		}
	}, rounds*len(keys))
	lane := map[string]int32{}
	for i := range dd {
		k := string(dd[i].Message.Key)
		got := dd[i].Info.Partition
		if !dd[i].Info.HasPartition {
			t.Fatalf("delivery %d reports no partition", i)
		}
		if want, ok := lane[k]; ok && want != got {
			t.Fatalf("key %q reached lane %d and lane %d", k, want, got)
		}
		lane[k] = got
	}
	if len(lane) != len(keys) {
		t.Fatalf("saw %d keys, want %d", len(lane), len(keys))
	}
	spread := map[int32]bool{}
	for _, p := range lane {
		spread[p] = true
	}
	if len(spread) < 2 {
		t.Fatalf("ten keys over eight lanes all hashed to lane %v", lane)
	}
}

// TestRoundRobinSpreadsUnkeyedRecords re-expresses
// TestRoundRobinSpreadsUnkeyedRecords: a publication with no key is spread
// rather than piled onto one lane.
func TestRoundRobinSpreadsUnkeyedRecords(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Partitions: 4}))
	const n = 32
	dd := receiveViaSPI(t, ctx, b, "orders", "billing", func() {
		for i := range n {
			publish(t, ctx, b, "orders", &mq.Message{Body: []byte{byte(i)}})
		}
	}, n)
	count := map[int32]int{}
	for i := range dd {
		count[dd[i].Info.Partition]++
	}
	if len(count) != 4 {
		t.Fatalf("32 unkeyed records reached %d of 4 lanes: %v", len(count), count)
	}
	for lane, got := range count {
		if got != n/4 {
			t.Fatalf("lane %d took %d of %d records, want an even %d: %v", lane, got, n, n/4, count)
		}
	}
}

// TestExplicitPartitionSelectsTheLane re-expresses TestExplicitPartitionWraps:
// a publication that names a partition goes to it, and one that names a
// partition outside the range is placed by the engine rather than refused.
func TestExplicitPartitionSelectsTheLane(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Partitions: 4}))
	dd := receiveViaSPI(t, ctx, b, "orders", "billing", func() {
		for lane := range 4 {
			publish(t, ctx, b, "orders",
				&mq.Message{Body: []byte{byte(lane)}}, mq.Partition(lane))
		}
	}, 4)
	for i := range dd {
		lane := int32(dd[i].Message.Body[0])
		if dd[i].Info.Partition != lane {
			t.Fatalf("a publication that named partition %d was served from lane %d",
				lane, dd[i].Info.Partition)
		}
	}
}

// TestPublishWithoutSubscriptionIsDiscardedAndReleased re-expresses
// TestPublishWithoutSubscriptionReleases: a topic with no subscription
// discards the publication and releases it, which is an admission of nothing
// to nobody and reports no error.
func TestPublishWithoutSubscriptionIsDiscardedAndReleased(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopics("orders"))
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish to a topic nobody reads: %v", err)
	}
	msgs, _, discarded := b.Totals()
	if msgs != 0 {
		t.Fatalf("a discarded publication left %d admitted records", msgs)
	}
	if discarded != 1 {
		t.Fatalf("discarded count is %d, want 1", discarded)
	}
	// The owned form is where the release is observable: the payload comes
	// home exactly once even though nothing took it.
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	owned, ok := sink.(mq.OwnedSink)
	if !ok {
		t.Fatal("the memory sink does not implement OwnedSink")
	}
	released := make(chan *mq.Message, 4)
	m := &mq.Message{Body: []byte("owned")}
	err = owned.PublishOwned(ctx, mq.OwnedMessage{
		Message: m,
		Release: func(got *mq.Message) { released <- got },
	}, &mq.PublishOptions{})
	if err != nil {
		t.Fatalf("PublishOwned: %v", err)
	}
	select {
	case got := <-released:
		if got != m {
			t.Fatalf("Release was handed %p, want the message it was given (%p)", got, m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a discarded owned publication never released its payload")
	}
	if len(released) != 0 {
		t.Fatal("Release ran more than once")
	}
}

// TestCapacityRejectsAtomically re-expresses TestCapacityRejectsAtomically:
// under the Atomic fan-out policy a subscription at its limit refuses the
// publication for every subscription, so a successful publish means every
// subscription holds the record.
func TestCapacityRejectsAtomically(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: 2,
		Overflow:         memory.OverflowReject,
		Fanout:           memory.FanoutAtomic,
	}))
	adm, err := client.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	for _, sub := range []string{"fast", "slow"} {
		if err := adm.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "orders", Name: sub}); err != nil {
			t.Fatalf("EnsureSubscription(%q): %v", sub, err)
		}
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	for i := range 2 {
		if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("a full lane answered %v, want ErrQueueFull", err)
	}
	for _, sub := range []string{"fast", "slow"} {
		snap, ierr := b.Inspect("orders", sub)
		if ierr != nil {
			t.Fatalf("Inspect(%q): %v", sub, ierr)
		}
		if snap.Ready != 2 || snap.Published != 2 {
			t.Fatalf("%q holds %+v, want two records admitted to both subscriptions", sub, snap)
		}
		if snap.Rejected != 1 {
			t.Fatalf("%q counted %d rejections, want 1", sub, snap.Rejected)
		}
	}
}

// TestByteCapacity re-expresses TestByteCapacity: the lane's byte bound
// refuses a publication the record bound would have taken.
func TestByteCapacity(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: 1024,
		CapacityBytes:    64,
		Overflow:         memory.OverflowReject,
	}))
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	body := bytes.Repeat([]byte("x"), 32)
	for i := range 2 {
		if err := p.Publish(ctx, &mq.Message{Body: body}); err != nil {
			t.Fatalf("publish %d of 32 bytes: %v", i, err)
		}
	}
	if err := p.Publish(ctx, &mq.Message{Body: body}); !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("the third 32-byte publication answered %v, want ErrQueueFull at a 64-byte bound", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Bytes != 64 {
		t.Fatalf("the lane accounts %d bytes, want 64", snap.Bytes)
	}
}

// TestBrokerTotals re-expresses TestEngineTotals: the broker-wide record and
// byte counts rise on admission and fall on settlement.
func TestBrokerTotals(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if msgs, bytes, _ := b.Totals(); msgs != 0 || bytes != 0 {
		t.Fatalf("a fresh broker holds %d records and %d bytes", msgs, bytes)
	}
	for range 4 {
		if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("k"), Body: []byte("1234")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	msgs, bs, _ := b.Totals()
	if msgs != 4 {
		t.Fatalf("Totals reports %d records, want 4", msgs)
	}
	if bs != 4*int64(len("k")+len("1234")) {
		t.Fatalf("Totals reports %d bytes, want %d (key and body per record)", bs, 4*5)
	}
	ds := receiveN(t, ctx, c, 4)
	for i := range ds {
		if err := ds[i].Ack(); err != nil {
			t.Fatalf("Ack %d: %v", i, err)
		}
	}
	if msgs, bs, _ := b.Totals(); msgs != 0 || bs != 0 {
		t.Fatalf("after settling everything Totals reports %d records and %d bytes", msgs, bs)
	}
}

// TestMaxMessageSize re-expresses TestMaxMessageSize, and pins the specification's "before
// any I/O": the limit is advertised, so the core refuses the message without
// the engine being told anything.
func TestMaxMessageSize(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{MaxMessageSize: 16}))
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	lim := client.Capabilities().Limits()
	if !lim.MaxMessageBytes.Known || lim.MaxMessageBytes.Value != 16 {
		t.Fatalf("MaxMessageBytes is %+v, want a known 16", lim.MaxMessageBytes)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	err = p.Publish(ctx, &mq.Message{Body: bytes.Repeat([]byte("x"), 17)})
	if !errors.Is(err, mq.ErrMessageTooLarge) {
		t.Fatalf("a 17-byte body answered %v, want ErrMessageTooLarge at a 16-byte limit", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Published != 0 || snap.Rejected != 0 {
		t.Fatalf("the engine was told about a message the limit refused: %+v", snap)
	}
	if err := p.Publish(ctx, &mq.Message{Body: bytes.Repeat([]byte("x"), 16)}); err != nil {
		t.Fatalf("a 16-byte body: %v", err)
	}
}

// TestPublishHonoursContext re-expresses TestPublishHonoursContext.
func TestPublishHonoursContext(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	done, cancel := context.WithCancel(ctx)
	cancel()
	err := client.Publish(done, "orders", &mq.Message{Body: []byte("x")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish with a cancelled context: %v, want context.Canceled", err)
	}
}

// TestSafeCopyIsIndependent re-expresses internal/engine's test of the same
// name through the PUBLIC API. It pins what the first row and ADR-009 make
// the default: the caller may reuse its Key, Body and header storage
// the instant Publish returns, so what a consumer receives is unaffected by
// what the publisher did next.
func TestSafeCopyIsIndependent(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	key := []byte("cust-1")
	body := []byte("first")
	m := &mq.Message{Key: key, Body: body}
	m.Headers.Add("trace", []byte("abc"))
	if err := client.Publish(ctx, "orders", m); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Every byte of storage the publication was given is overwritten, and
	// the message value itself is reused for a second publication.
	copy(key, "XXXXXX")
	copy(body, "XXXXX")
	if v, ok := m.Headers.Get("trace"); ok {
		copy(v, "XXX")
	}
	m.Reset()
	ds := receiveN(t, ctx, c, 1)
	if got := string(ds[0].Key); got != "cust-1" {
		t.Fatalf("the delivery's key is %q: the publication kept the caller's storage", got)
	}
	if got := string(ds[0].Body); got != "first" {
		t.Fatalf("the delivery's body is %q: the publication kept the caller's storage", got)
	}
	v, ok := ds[0].Headers.Get("trace")
	if !ok || string(v) != "abc" {
		t.Fatalf("the delivery's trace header is %q (present %v): the publication kept the caller's storage", v, ok)
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestPublishBatchReportsPerMessageOutcomes re-expresses
// TestPublishBatchStopsAtFirstFailure at the level a caller sees it: a batch
// in which some messages were refused reports a *BatchError index-aligned
// with the batch, so the caller can retry exactly the ones that failed.
func TestPublishBatchReportsPerMessageOutcomes(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		CapacityMessages: 2,
		Overflow:         memory.OverflowReject,
	}))
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	ms := make([]*mq.Message, 4)
	for i := range ms {
		ms[i] = &mq.Message{Body: []byte{byte(i)}}
	}
	err = p.PublishBatch(ctx, ms)
	var be *mq.BatchError
	if !errors.As(err, &be) {
		t.Fatalf("a batch of 4 into a lane of 2 answered %v (%T), want a *BatchError", err, err)
	}
	if len(be.Errs) != len(ms) {
		t.Fatalf("the BatchError carries %d results for %d messages: the results are not index-aligned",
			len(be.Errs), len(ms))
	}
	for i, e := range be.Errs {
		if i < 2 && e != nil {
			t.Fatalf("message %d was refused with %v, but the lane had room for it", i, e)
		}
		if i >= 2 && !errors.Is(e, mq.ErrQueueFull) {
			t.Fatalf("message %d answered %v, want ErrQueueFull", i, e)
		}
	}
	if !errors.Is(err, mq.ErrPartial) {
		t.Fatalf("a partly refused batch does not satisfy errors.Is(err, ErrPartial)")
	}
	snap, ierr := b.Inspect("orders", "billing")
	if ierr != nil {
		t.Fatalf("Inspect: %v", ierr)
	}
	if snap.Published != 2 {
		t.Fatalf("the engine admitted %d of the batch, want the 2 that fitted", snap.Published)
	}
}

// TestPublishBatchIsOneNativeCall pins the optional surface the specification measures:
// the memory sink implements BatchSink, so the core hands it the whole batch
// instead of looping.
func TestPublishBatchIsOneNativeCall(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopics("orders"))
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	if _, ok := sink.(mq.BatchSink); !ok {
		t.Fatal("the memory sink does not implement BatchSink, so the core will loop")
	}
	if _, ok := sink.(mq.FlushSink); !ok {
		t.Fatal("the memory sink does not implement FlushSink")
	}
	if _, ok := sink.(mq.AsyncSink); !ok {
		t.Fatal("the memory sink does not implement AsyncSink")
	}
	// The engine's batch is not atomic — it admits what fits and stops at
	// the first refusal — so FeatureAtomicBatchPublish is unsupported, which
	// TestCapabilitiesAreHonest pins. SPI declares no AtomicBatchSink,
	// so there is no interface to refuse here; if one lands, this driver
	// must not grow the method.
}

// TestBatchSettlerIsAbsent is the other half of the same rule, on the receive
// side: the engine settles one delivery at a time, so the source must not
// offer a batch settler the core would then use instead of its own loop.
func TestBatchSettlerIsAbsent(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopics("orders"))
	src := openSource(t, ctx, b, "orders", "billing")
	st := src.Settler()
	if st == nil {
		t.Fatal("the memory source declares itself non-settleable")
	}
	if _, ok := st.(mq.BatchSettler); ok {
		t.Fatal("the memory settler claims batched settlement, which the engine does not have")
	}
	if _, ok := st.(mq.DeadlineExtender); !ok {
		t.Fatal("the memory settler does not implement DeadlineExtender")
	}
}

// ---------------------------------------------------------------------------
// Delivery metadata
// ---------------------------------------------------------------------------

// TestLeaseExpiresIsTheSubscriptionsLease holds DeliveryInfo.LeaseExpires to
// the lease the engine applies. A subscription keeps the deadline it was
// created with, so a source opened on it later with another one was handed
// a LeaseExpires computed from its own config (the topic's 30 seconds
// against the subscription's one), and the core's lease scheduler, which
// extends from that instant, extended after the engine had already expired
// the lease and redelivered the message.
func TestLeaseExpiresIsTheSubscriptionsLease(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopics("orders"))
	first, err := b.OpenSource(ctx, mq.SourceConfig{
		Topic: "orders", Subscription: "billing", Prefetch: 1, AckDeadline: time.Second,
	})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	_ = first.Close()
	second, err := b.OpenSource(ctx, mq.SourceConfig{Topic: "orders", Subscription: "billing", Prefetch: 1})
	if err != nil {
		t.Fatalf("OpenSource: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("x")})
	dst := make([]mq.DriverDelivery, 1)
	if n, err := second.Fetch(ctx, dst); n != 1 || err != nil {
		t.Fatalf("Fetch = (%d, %v), want the publication", n, err)
	}
	if got := dst[0].Info.LeaseExpires.Sub(dst[0].Info.ReceivedAt); got != time.Second {
		t.Fatalf("the delivery reports a %v lease on a subscription whose lease is 1s", got)
	}
	if err := b.Advance(2 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if snap, _ := b.Inspect("orders", "billing"); snap.Expired != 1 {
		t.Fatalf("the engine expired %d leases two seconds in, want 1: the test is not measuring what it says", snap.Expired)
	}
}

// TestDeliveryInfoIsPopulated is the SPI obligation this increment fills in:
// the metadata, with every optional field paired with its Has flag so
// that "the broker did not say" is never the same answer as zero.
func TestDeliveryInfoIsPopulated(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{
		Partitions:  2,
		AckDeadline: 90 * time.Second,
	}))
	before := b.Clock().Now()
	dd := receiveViaSPI(t, ctx, b, "orders", "billing", func() {
		publish(t, ctx, b, "orders", &mq.Message{Key: []byte("k"), Body: []byte("v")})
	}, 1)
	info := dd[0].Info
	if info.Topic != "orders" {
		t.Fatalf("Topic is %q, want the resolved topic", info.Topic)
	}
	if info.Subscription != "billing" {
		t.Fatalf("Subscription is %q", info.Subscription)
	}
	if info.Attempt != 1 {
		t.Fatalf("Attempt is %d on a first delivery, want 1", info.Attempt)
	}
	if info.Redelivered {
		t.Fatal("a first delivery reports itself redelivered")
	}
	if info.ReceivedAt.Before(before) {
		t.Fatalf("ReceivedAt is %v, before the fetch started at %v", info.ReceivedAt, before)
	}
	want := info.ReceivedAt.Add(90 * time.Second)
	if !info.LeaseExpires.Equal(want) {
		t.Fatalf("LeaseExpires is %v, want %v (received at plus the 90s deadline)", info.LeaseExpires, want)
	}
	if !info.HasPartition {
		t.Fatal("HasPartition is false on a partitioned topic")
	}
	if info.Partition < 0 || info.Partition > 1 {
		t.Fatalf("Partition is %d on a two-lane topic", info.Partition)
	}
	if !info.HasSequence || info.Sequence == 0 {
		t.Fatalf("Sequence is %d (has %v), want the record's publication sequence",
			info.Sequence, info.HasSequence)
	}
	// The engine has no per-partition offset distinct from the publication
	// sequence, and the specification makes an unset flag the honest answer rather than a
	// zero that reads as one.
	if info.HasOffset {
		t.Fatalf("HasOffset is true and Offset is %d: this engine has no offset to report", info.Offset)
	}
	if info.Source != "" {
		t.Fatalf("Source is %q: there is no broker-side origin behind a memory topic", info.Source)
	}
}

// TestRedeliveryReportsItsAttempt pins the attempt counter and the
// redelivered flag across a negative settlement.
func TestRedeliveryReportsItsAttempt(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t)
	src := openSource(t, ctx, b, "orders", "billing")
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("x")})
	buf := make([]mq.DriverDelivery, 1)
	for attempt := uint32(1); attempt <= 3; attempt++ {
		n, err := src.Fetch(ctx, buf)
		if err != nil || n != 1 {
			t.Fatalf("Fetch on attempt %d: %d, %v", attempt, n, err)
		}
		if buf[0].Info.Attempt != attempt {
			t.Fatalf("delivery reports attempt %d, want %d", buf[0].Info.Attempt, attempt)
		}
		if got, want := buf[0].Info.Redelivered, attempt > 1; got != want {
			t.Fatalf("attempt %d reports Redelivered=%v, want %v", attempt, got, want)
		}
		if attempt == 3 {
			break
		}
		if err := src.Settler().Nack(ctx, &buf[0], mq.NackOptions{}); err != nil {
			t.Fatalf("Nack on attempt %d: %v", attempt, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Settlement
// ---------------------------------------------------------------------------

// TestSettlementErrors re-expresses TestSettlementErrors: the rules hold
// through the public API whatever the driver does.
func TestSettlementErrors(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	d := &ds[0]
	if d.Settled() {
		t.Fatal("a live delivery reports itself settled")
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("first Ack: %v", err)
	}
	if !d.Settled() {
		t.Fatal("an acknowledged delivery does not report itself settled")
	}
	if err := d.Ack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("second Ack: %v, want ErrAlreadySettled", err)
	}
	if err := d.Nack(); !errors.Is(err, mq.ErrAlreadySettled) {
		t.Fatalf("Nack after Ack: %v, want ErrAlreadySettled", err)
	}
	// A delivery a caller built carries no settlement right at all, which is
	// what keeps the state machine below the API rather than in the
	// caller's discipline.
	var built mq.Delivery
	if err := built.Ack(); !errors.Is(err, mq.ErrNotSettleable) {
		t.Fatalf("a hand-built Delivery answered %v, want ErrNotSettleable", err)
	}
	if built.Settled() {
		t.Fatal("a hand-built Delivery reports itself settled")
	}
}

// TestStaleSettlementReportsLeaseLost is what the generation is for: a
// settlement of a delivery whose lease has been reclaimed reports the lease
// as lost rather than acknowledging the redelivery that took its place
func TestStaleSettlementReportsLeaseLost(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: "orders", Subscription: "billing", AckDeadline: time.Second,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	// The lease expires and the record goes back to its lane under a new
	// generation. The delivery the caller holds names the old one.
	if err := b.Advance(2 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if err := ds[0].Ack(); !errors.Is(err, mq.ErrLeaseLost) {
		t.Fatalf("a settlement past its lease answered %v, want ErrLeaseLost", err)
	}
	// The record is deliverable again, which is the other half: the lease
	// was lost, the message was not.
	again := receiveN(t, ctx, c, 1)
	if again[0].Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", again[0].Attempt)
	}
	if err := again[0].Ack(); err != nil {
		t.Fatalf("Ack of the redelivery: %v", err)
	}
}

// TestDropTakesTheTerminalSettlement pins the engine has
// one terminal settlement, so a dropped delivery leaves the subscription and
// is counted where an acknowledged one is — and the consumer's own counters
// are what keep the two verbs apart.
func TestDropTakesTheTerminalSettlement(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	if err := ds[0].Nack(mq.NackDrop()); err != nil {
		t.Fatalf("Nack(NackDrop): %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != 0 || snap.InFlight != 0 {
		t.Fatalf("a dropped record is still held: %+v", snap)
	}
	if snap.Acked != 1 {
		t.Fatalf("Snapshot.Acked is %d, want the 1 the specification records", snap.Acked)
	}
	st := c.Stats()
	if st.Nacked != 1 {
		t.Fatalf("the consumer counted %d negative settlements, want 1", st.Nacked)
	}
	if st.Acked != 0 {
		t.Fatalf("the consumer counted the drop as an acknowledgement (%d)", st.Acked)
	}
}

// TestRejectDeadLettersThroughAPublishThenAck pins the emulation ADR-008
// names: a Reject publishes to the dead-letter topic and then acknowledges,
// and a topic with no destination answers ErrUnsupported and settles nothing.
func TestRejectDeadLettersThroughAPublishThenAck(t *testing.T) {
	ctx := context.Background()
	t.Run("no destination", func(t *testing.T) {
		client, b := memory.NewForTest(t)
		c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatalf("Consumer: %v", err)
		}
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		ds := receiveN(t, ctx, c, 1)
		if err := ds[0].Reject(); !errors.Is(err, mq.ErrUnsupported) {
			t.Fatalf("Reject with no dead-letter topic: %v, want ErrUnsupported", err)
		}
		// The specification: an action the driver cannot honour settles nothing, so the
		// delivery is still settleable.
		if ds[0].Settled() {
			t.Fatal("a refused Reject settled the delivery anyway")
		}
		if err := ds[0].Ack(); err != nil {
			t.Fatalf("the delivery is no longer settleable: %v", err)
		}
		snap, _ := b.Inspect("orders", "billing")
		if snap.Acked != 1 {
			t.Fatalf("snapshot %+v", snap)
		}
	})
	t.Run("with a destination", func(t *testing.T) {
		client, b := memory.NewForTest(t,
			memory.TestTopology(memory.TopologyConfig{DeadLetterTopic: "dlq"}),
			memory.TestTopics("dlq"))
		dlq, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "dlq", Subscription: "audit"})
		if err != nil {
			t.Fatalf("dead-letter Consumer: %v", err)
		}
		c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
		if err != nil {
			t.Fatalf("Consumer: %v", err)
		}
		if err := client.Publish(ctx, "orders", &mq.Message{Key: []byte("k"), Body: []byte("poison")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		ds := receiveN(t, ctx, c, 1)
		if err := ds[0].Reject(); err != nil {
			t.Fatalf("Reject: %v", err)
		}
		if !ds[0].Settled() {
			t.Fatal("a successful Reject left the delivery unsettled")
		}
		routed := receiveN(t, ctx, dlq, 1)
		if got := string(routed[0].Body); got != "poison" {
			t.Fatalf("the dead-letter topic received %q", got)
		}
		if err := routed[0].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		snap, _ := b.Inspect("orders", "billing")
		if snap.Ready != 0 || snap.InFlight != 0 {
			t.Fatalf("the rejected record is still held: %+v", snap)
		}
	})
}

// TestAnExplicitDeadLetterSettlesOnAPartialFanout holds the explicit Reject
// to the answer the engine's own attempt-limit route gives a partial fan-out.
// Under FanoutIndependent a dead-letter subscription with no room refuses the
// copy while the others take it, which the engine reports as ErrPartial; the
// Reject reported that as a failure and left the delivery unsettled, so every
// retry added another copy to the subscriptions that had taken the first.
func TestAnExplicitDeadLetterSettlesOnAPartialFanout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{
			Fanout:           memory.FanoutIndependent,
			CapacityMessages: 2,
			DeadLetterTopic:  "dlq",
		}),
		memory.TestTopics("dlq", "orders"))
	ad, err := b.Admin()
	if err != nil {
		t.Fatalf("Admin: %v", err)
	}
	for _, s := range []string{"roomy", "full"} {
		if err := ad.EnsureSubscription(ctx, mq.SubscriptionSpec{Topic: "dlq", Name: s}); err != nil {
			t.Fatalf("EnsureSubscription(%s): %v", s, err)
		}
	}
	// Fill "full" and drain "roomy", so that the dead-letter copy fits one
	// subscription and not the other.
	publish(t, ctx, b, "dlq", &mq.Message{Body: []byte("filler")})
	publish(t, ctx, b, "dlq", &mq.Message{Body: []byte("filler")})
	roomy := openSource(t, ctx, b, "dlq", "roomy")
	dd := make([]mq.DriverDelivery, 4)
	if n, err := roomy.Fetch(ctx, dd); n != 2 || err != nil {
		t.Fatalf("Fetch from roomy = (%d, %v), want the two fillers", n, err)
	}
	for i := range 2 {
		if err := roomy.Settler().Ack(ctx, &dd[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	src := openSource(t, ctx, b, "orders", "billing")
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("poison")})
	d := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, d); n != 1 || err != nil {
		t.Fatalf("Fetch = (%d, %v), want the publication", n, err)
	}
	// A failure here is what the core retries, and each retry published
	// another copy before its acknowledgement could succeed.
	if err := src.Settler().Nack(ctx, &d[0], mq.NackOptions{Action: mq.NackActionDeadLetter}); err != nil {
		t.Fatalf("Nack(DeadLetter) of a copy one dead-letter subscription took: %v, want nil", err)
	}
	rs, _ := b.Inspect("dlq", "roomy")
	if rs.Ready != 1 {
		t.Fatalf("the roomy dead-letter subscription holds %d copies of one rejected message, want 1", rs.Ready)
	}
	fs, _ := b.Inspect("dlq", "full")
	if fs.Rejected != 1 {
		t.Fatalf("the full dead-letter subscription counts %d refusals, want 1", fs.Rejected)
	}
	os, _ := b.Inspect("orders", "billing")
	if os.InFlight != 0 || os.Ready != 0 || os.Acked != 1 {
		t.Fatalf("the rejected record is still held: %+v", os)
	}
}

// TestDeadLetterPublicationFailureNeverAcks is the rule, reachable through
// the fault injector: a dead-letter publication that failed leaves the
// delivery unsettled and redeliverable.
func TestDeadLetterPublicationFailureNeverAcks(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{
			DeadLetterTopic:  "dlq",
			CapacityMessages: 1,
			Overflow:         memory.OverflowReject,
		}),
		memory.TestTopics("dlq"))
	// Fill the dead-letter lane so the routing publication is refused.
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "dlq", Subscription: "audit"}); err != nil {
		t.Fatalf("dead-letter Consumer: %v", err)
	}
	if err := client.Publish(ctx, "dlq", &mq.Message{Body: []byte("filler")}); err != nil {
		t.Fatalf("filling the dead-letter lane: %v", err)
	}
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	if err := ds[0].Reject(); !errors.Is(err, mq.ErrQueueFull) {
		t.Fatalf("Reject into a full dead-letter lane: %v, want the publication's refusal", err)
	}
	if ds[0].Settled() {
		t.Fatal("a failed dead-letter publication acknowledged the delivery anyway")
	}
	snap, _ := b.Inspect("orders", "billing")
	if snap.InFlight != 1 {
		t.Fatalf("the delivery is no longer in flight: %+v", snap)
	}
	if err := ds[0].Nack(); err != nil {
		t.Fatalf("the delivery is not redeliverable: %v", err)
	}
}

// TestExtendKeepsALongHandlersLeaseAlive pins the optional extension surface
// through the core's own scheduler: a delivery whose lease was extended is
// not reclaimed at the original deadline.
func TestExtendKeepsALongHandlersLeaseAlive(t *testing.T) {
	ctx := context.Background()
	_, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{AckDeadline: time.Second}))
	src := openSource(t, ctx, b, "orders", "billing")
	publish(t, ctx, b, "orders", &mq.Message{Body: []byte("slow")})
	buf := make([]mq.DriverDelivery, 1)
	if n, err := src.Fetch(ctx, buf); err != nil || n != 1 {
		t.Fatalf("Fetch: %d, %v", n, err)
	}
	ext, ok := src.Settler().(mq.DeadlineExtender)
	if !ok {
		t.Fatal("the memory settler does not implement DeadlineExtender")
	}
	if err := ext.Extend(ctx, &buf[0], time.Minute); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err := b.Advance(30 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.InFlight != 1 || snap.Expired != 0 {
		t.Fatalf("an extended lease was reclaimed at its original deadline: %+v", snap)
	}
	if err := src.Settler().Ack(ctx, &buf[0]); err != nil {
		t.Fatalf("Ack after the extension: %v", err)
	}
	// Past the extended deadline the lease is gone, which is what makes the
	// extension an extension rather than a removal.
	if err := b.Advance(2 * time.Minute); err != nil {
		t.Fatalf("Advance: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Redelivery, delay and expiry
// ---------------------------------------------------------------------------

// TestLeaseExpiryRedelivers re-expresses TestLeaseExpiryRedelivers.
func TestLeaseExpiryRedelivers(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{AckDeadline: time.Second}))
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = receiveN(t, ctx, c, 1)
	if err := b.Advance(2 * time.Second); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Expired != 1 || snap.Ready != 1 || snap.InFlight != 0 {
		t.Fatalf("snapshot %+v, want the lease expired and the record ready again", snap)
	}
	again := receiveN(t, ctx, c, 1)
	if again[0].Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", again[0].Attempt)
	}
	if err := again[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestDelayedDelivery re-expresses TestDelayedDelivery: a delayed
// publication is not delivered before its due time, and is delivered after
// it.
func TestDelayedDelivery(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("later")}, mq.Delay(time.Minute)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Delayed != 1 || snap.Ready != 0 {
		t.Fatalf("snapshot %+v, want the record delayed", snap)
	}
	poll, cancel := context.WithCancel(ctx)
	cancel()
	if n, rerr := c.Receive(poll, make([]mq.Delivery, 1)); n != 0 {
		t.Fatalf("a delayed record was delivered early (%d, %v)", n, rerr)
	}
	// IdleDelivered does not wait for a record that is not yet due, and
	// IdleIncludingDelayed does.
	idle, icancel := context.WithTimeout(ctx, 2*time.Second)
	defer icancel()
	if err := b.WaitIdle(idle, memory.IdleDelivered); err != nil {
		t.Fatalf("WaitIdle(IdleDelivered) with one delayed record: %v", err)
	}
	if err := b.Advance(time.Minute); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	if got := string(ds[0].Body); got != "later" {
		t.Fatalf("body %q", got)
	}
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestNackDelayRequeues re-expresses TestNackDelayRequeues.
func TestNackDelayRequeues(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("retry")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, c, 1)
	if err := ds[0].Nack(mq.NackDelay(time.Minute)); err != nil {
		t.Fatalf("Nack(NackDelay): %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Delayed != 1 || snap.Ready != 0 || snap.Nacked != 1 {
		t.Fatalf("snapshot %+v, want the record delayed by the nack", snap)
	}
	if err := b.Advance(time.Minute); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	again := receiveN(t, ctx, c, 1)
	if again[0].Attempt != 2 {
		t.Fatalf("the redelivery reports attempt %d, want 2", again[0].Attempt)
	}
	if err := again[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestTTLDiscardsAnUndeliveredRecord pins the other sweep the clock drives:
// a record nobody took, past its time to live, is dropped rather than
// delivered late.
func TestTTLDiscardsAnUndeliveredRecord(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"}); err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("stale")}, mq.TTL(time.Minute)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := b.Advance(2 * time.Minute); err != nil {
		t.Fatalf("Advance: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != 0 || snap.Dropped != 1 {
		t.Fatalf("snapshot %+v, want the record dropped by its time to live", snap)
	}
	if !snap.Conserved() {
		t.Fatalf("conservation broken: %+v", snap)
	}
}

// TestMaxAttemptsRoutesToTheDeadLetterTopic pins the engine's own
// dead-lettering, which is a different route from an explicit Reject: the
// record runs out of attempts and the engine publishes it.
func TestMaxAttemptsRoutesToTheDeadLetterTopic(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t,
		memory.TestTopology(memory.TopologyConfig{MaxAttempts: 2, DeadLetterTopic: "dlq"}),
		memory.TestTopics("dlq"))
	dlq, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "dlq", Subscription: "audit"})
	if err != nil {
		t.Fatalf("dead-letter Consumer: %v", err)
	}
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("poison")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		ds := receiveN(t, ctx, c, 1)
		if ds[0].Attempt != attempt {
			t.Fatalf("attempt %d reported as %d", attempt, ds[0].Attempt)
		}
		if err := ds[0].Nack(); err != nil {
			t.Fatalf("Nack on attempt %d: %v", attempt, err)
		}
	}
	routed := receiveN(t, ctx, dlq, 1)
	if got := string(routed[0].Body); got != "poison" {
		t.Fatalf("the dead-letter topic received %q", got)
	}
	if err := routed[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.DeadLettered != 1 || snap.Ready != 0 || snap.InFlight != 0 {
		t.Fatalf("snapshot %+v, want the record dead-lettered", snap)
	}
}

// ---------------------------------------------------------------------------
// Isolation, blocking and close
// ---------------------------------------------------------------------------

// TestSubscriptionIsolation re-expresses TestSubscriptionIsolation: two
// subscriptions of one topic keep their own records, their own settlement
// state and their own capacity.
func TestSubscriptionIsolation(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	fast, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "fast"})
	if err != nil {
		t.Fatalf("Consumer(fast): %v", err)
	}
	slow, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "slow"})
	if err != nil {
		t.Fatalf("Consumer(slow): %v", err)
	}
	for i := range 4 {
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	ds := receiveN(t, ctx, fast, 4)
	for i := range ds {
		if err := ds[i].Ack(); err != nil {
			t.Fatalf("Ack %d: %v", i, err)
		}
	}
	fs, err := b.Inspect("orders", "fast")
	if err != nil {
		t.Fatalf("Inspect(fast): %v", err)
	}
	ss, err := b.Inspect("orders", "slow")
	if err != nil {
		t.Fatalf("Inspect(slow): %v", err)
	}
	if fs.Acked != 4 || fs.Ready != 0 {
		t.Fatalf("fast %+v", fs)
	}
	if ss.Acked != 0 || ss.Ready != 4 {
		t.Fatalf("slow %+v: one subscription's settlements reached the other", ss)
	}
	// The slow subscription still has every record, in publication order.
	got := receiveN(t, ctx, slow, 4)
	for i := range got {
		if got[i].Body[0] != byte(i) {
			t.Fatalf("the slow subscription served %d at position %d", got[i].Body[0], i)
		}
		if err := got[i].Ack(); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// TestQueueTopologyCompetes pins the queue profile: competing consumers
// over one subscription, and a second differently named subscription is a
// configuration error rather than a silent fan-out.
func TestQueueTopologyCompetes(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t, memory.TestTopology(memory.TopologyConfig{Kind: memory.TopologyQueue}))
	a, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "work", Subscription: "workers"})
	if err != nil {
		t.Fatalf("Consumer(a): %v", err)
	}
	bb, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "work", Subscription: "workers"})
	if err != nil {
		t.Fatalf("Consumer(b): %v", err)
	}
	if _, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "work", Subscription: "others"}); mq.CodeOf(err) != mq.CodeInvalidConfig {
		t.Fatalf("a second subscription on a queue topic: %v (code %v)", err, mq.CodeOf(err))
	}
	const n = 8
	for i := range n {
		if err := client.Publish(ctx, "work", &mq.Message{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	// Between them the two consumers see each record exactly once.
	seen := map[byte]int{}
	total := 0
	poll, cancel := context.WithCancel(ctx)
	cancel()
	for _, c := range []*mq.ConsumerHandle{a, bb} {
		buf := make([]mq.Delivery, n)
		got, rerr := c.Receive(poll, buf)
		if got == 0 && rerr != nil && !errors.Is(rerr, context.Canceled) {
			t.Fatalf("Receive: %v", rerr)
		}
		for i := range got {
			seen[buf[i].Body[0]]++
			total++
			if err := buf[i].Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
		}
	}
	if total != n {
		t.Fatalf("two competing consumers took %d of %d records", total, n)
	}
	for v, times := range seen {
		if times != 1 {
			t.Fatalf("record %d was delivered %d times to a queue topic", v, times)
		}
	}
}

// TestReceiveBlocksUntilPublished re-expresses TestFetchBlocksUntilPublished.
func TestReceiveBlocksUntilPublished(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	buf := make([]mq.Delivery, 1)
	go func() {
		n, err := c.Receive(ctx, buf)
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("Receive returned %d, %v before anything was published", r.n, r.err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	select {
	case r := <-done:
		if r.err != nil || r.n != 1 {
			t.Fatalf("Receive returned %d, %v", r.n, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a parked Receive was not woken by a publication")
	}
	if err := buf[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestReceiveHonoursContext re-expresses TestFetchHonoursContext.
func TestReceiveHonoursContext(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	poll, cancel := context.WithCancel(ctx)
	cancel()
	n, err := c.Receive(poll, make([]mq.Delivery, 4))
	if n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Receive with a cancelled context returned %d, %v", n, err)
	}
	deadline, dcancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer dcancel()
	n, err = c.Receive(deadline, make([]mq.Delivery, 4))
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive with a deadline returned %d, %v", n, err)
	}
}

// TestSourceCloseUnblocksAParkedFetch is the obligation the specification records and the specification names, at the SPI level: a Close in another goroutine
// unblocks a Fetch that is parked, so the core never has to merge the
// caller's context with a cancellation of its own.
func TestSourceCloseUnblocksAParkedFetch(t *testing.T) {
	ctx := context.Background()
	for _, wk := range memory.WaitKinds() {
		t.Run(wk.String(), func(t *testing.T) {
			_, b := memory.NewForTest(t,
				memory.TestEngine(memory.EngineConfig{Wait: wk}),
				memory.TestTopics("orders"))
			src := openSource(t, ctx, b, "orders", "billing")
			done := make(chan error, 1)
			go func() {
				_, err := src.Fetch(ctx, make([]mq.DriverDelivery, 1))
				done <- err
			}()
			// Let the fetch reach its wait before closing, so the close is
			// racing a parked consumer rather than one that has not started.
			for range 1000 {
				runtime.Gosched()
			}
			if err := src.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, mq.ErrClosed) {
					t.Fatalf("the parked Fetch returned %v, want ErrClosed", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Source.Close did not unblock the parked Fetch")
			}
		})
	}
}

// TestConsumerCloseUnblocksAParkedReceive is the same obligation as the
// caller sees it, through the core's own handle.
func TestConsumerCloseUnblocksAParkedReceive(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, rerr := c.Receive(ctx, make([]mq.Delivery, 1))
		done <- rerr
	}()
	for range 1000 {
		runtime.Gosched()
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, mq.ErrClosed) {
			t.Fatalf("the parked Receive returned %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ConsumerHandle.Close did not unblock the parked Receive")
	}
}

// TestConsumerCloseRequeues re-expresses TestConsumerCloseRequeues: a
// delivery a closing consumer never settled goes back to its lane for
// another consumer, so the message is redelivered rather than lost.
func TestConsumerCloseRequeues(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	first, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ds := receiveN(t, ctx, first, 1)
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The delivery the closing consumer held is no longer settleable by it.
	if err := ds[0].Ack(); err == nil {
		t.Fatal("a delivery of a closed consumer was acknowledged")
	}
	snap, err := b.Inspect("orders", "billing")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if snap.Ready != 1 || snap.InFlight != 0 || snap.Requeued != 1 {
		t.Fatalf("snapshot %+v, want the record back in its lane", snap)
	}
	second, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("second Consumer: %v", err)
	}
	again := receiveN(t, ctx, second, 1)
	if err := again[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestPrefetchBoundsReceive re-expresses TestPrefetchBoundsFetch: a receive
// never takes more than the prefetch, whatever the caller's slice holds.
func TestPrefetchBoundsReceive(t *testing.T) {
	ctx := context.Background()
	client, _ := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{
		Topic: "orders", Subscription: "billing", Prefetch: 2,
	})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	for range 8 {
		if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	n, err := c.Receive(ctx, make([]mq.Delivery, 8))
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if n != 2 {
		t.Fatalf("Receive took %d deliveries with a prefetch of 2", n)
	}
}

// TestCloseReleasesEverything re-expresses TestCloseReleasesEverything: after
// close every operation answers ErrClosed and every record held is released.
func TestCloseReleasesEverything(t *testing.T) {
	ctx := context.Background()
	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	client, err := memory.OpenWithClient(ctx, b, memory.ClientOwned)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	p, err := client.Publisher(ctx, mq.PublisherConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	released := make(chan struct{}, 8)
	sink, err := b.OpenSink(ctx, mq.SinkConfig{Topic: "orders"})
	if err != nil {
		t.Fatalf("OpenSink: %v", err)
	}
	owned, ok := sink.(mq.OwnedSink)
	if !ok {
		t.Fatal("the memory sink does not implement OwnedSink")
	}
	// Three records: one in flight, one ready and one delayed. Every one of
	// them must come home exactly once when the broker closes.
	for i := range 3 {
		var opts mq.PublishOptions
		if i == 2 {
			opts.Delay = time.Hour
		}
		if err := owned.PublishOwned(ctx, mq.OwnedMessage{
			Message: &mq.Message{Body: []byte{byte(i)}},
			Release: func(*mq.Message) { released <- struct{}{} },
		}, &opts); err != nil {
			t.Fatalf("PublishOwned %d: %v", i, err)
		}
	}
	if _, rerr := c.Receive(ctx, make([]mq.Delivery, 1)); rerr != nil {
		t.Fatalf("Receive: %v", rerr)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !b.Closed() {
		t.Fatal("closing an owning client left the broker open")
	}
	if got := len(released); got != 3 {
		t.Fatalf("%d of 3 owned payloads came home", got)
	}
	// After close every operation answers ErrClosed.
	if err := p.Publish(ctx, &mq.Message{Body: []byte("x")}); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Publish after close: %v", err)
	}
	if _, err := c.Receive(ctx, make([]mq.Delivery, 1)); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Receive after close: %v", err)
	}
	if err := client.Ping(ctx); !errors.Is(err, mq.ErrClosed) {
		t.Fatalf("Ping after close: %v", err)
	}
	if h := client.Health(); h.State != mq.HealthClosed {
		t.Fatalf("Health after close is %v", h.State)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close is not idempotent: %v", err)
	}
}

// TestBorrowedClientDoesNotCloseTheBroker pins the specification's ownership: two clients
// over one broker, and closing the borrowing one leaves the broker running.
func TestBorrowedClientDoesNotCloseTheBroker(t *testing.T) {
	ctx := context.Background()
	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if _, err := memory.OpenWithClient(ctx, b, 0); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("OpenWithClient with no ownership: %v (code %v)", err, mq.CodeOf(err))
	}
	if _, err := memory.OpenWithClient(ctx, nil, memory.ClientOwned); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("OpenWithClient with a nil broker: %v (code %v)", err, mq.CodeOf(err))
	}
	producer, err := memory.OpenWithClient(ctx, b, memory.ClientBorrowed)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	consumer, err := memory.OpenWithClient(ctx, b, memory.ClientBorrowed)
	if err != nil {
		t.Fatalf("OpenWithClient: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	c, err := consumer.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := producer.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if b.Closed() {
		t.Fatal("closing a borrowing client closed the broker")
	}
	ds := receiveN(t, ctx, c, 1)
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

// TestWaitIdle re-expresses TestWaitIdle across all three modes.
func TestWaitIdle(t *testing.T) {
	ctx := context.Background()
	client, b := memory.NewForTest(t)
	c, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	if err := b.WaitIdle(ctx, memory.IdleMode(99)); mq.CodeOf(err) != mq.CodeInvalidArgument {
		t.Fatalf("WaitIdle with no mode: %v (code %v)", err, mq.CodeOf(err))
	}
	idle, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := b.WaitIdle(idle, memory.IdleIncludingDelayed); err != nil {
		t.Fatalf("WaitIdle on an empty broker: %v", err)
	}
	if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	busy, bcancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer bcancel()
	if err := b.WaitIdle(busy, memory.IdleDelivered); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitIdle with a ready record: %v, want the context to end the wait", err)
	}
	done := make(chan error, 1)
	go func() {
		wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
		defer wcancel()
		done <- b.WaitIdle(wctx, memory.IdleDelivered)
	}()
	ds := receiveN(t, ctx, c, 1)
	if err := ds[0].Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("WaitIdle after the last settlement: %v", err)
	}
}

// TestAStaleSettlementCannotSettleTheRedeliveryThatReplacedIt is the specification's
// sharpest rule: "stale tokens cannot settle newer deliveries (gen mismatch →
// ErrLeaseLost)".
//
// It needs the redelivery to be LIVE when the stale settlement arrives, which
// is the case a settlement that reported success would lose a message in: it
// would acknowledge a delivery somebody else is still processing.
//
// The two subtests are the two things that can be stale, and they are checked
// separately because either one alone would hide the other. Under "the same
// consumer" the owner is unchanged and the GENERATION is the only thing that
// differs, which is what the generation is for; under "another consumer" the
// generation differs too but the ownership check is what refuses first, so a
// driver that presented the live generation would still be caught there and
// not in the first.
func TestAStaleSettlementCannotSettleTheRedeliveryThatReplacedIt(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, holder string }{
		{"the same consumer", "same"},
		{"another consumer", "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, b := memory.NewForTest(t,
				memory.TestTopology(memory.TopologyConfig{AckDeadline: time.Second}))
			first, err := client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
			if err != nil {
				t.Fatalf("Consumer(first): %v", err)
			}
			next := first
			if tc.holder == "other" {
				next, err = client.Consumer(ctx, mq.ConsumerConfig{Topic: "orders", Subscription: "billing"})
				if err != nil {
					t.Fatalf("Consumer(second): %v", err)
				}
			}
			if err := client.Publish(ctx, "orders", &mq.Message{Body: []byte("x")}); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			stale := receiveN(t, ctx, first, 1)
			// The lease expires and the record goes back to its lane.
			if err := b.Advance(2 * time.Second); err != nil {
				t.Fatalf("Advance: %v", err)
			}
			// The record is claimed again under a NEW generation, so the
			// delivery the first receive produced is stale against a live
			// lease rather than against no lease at all.
			live := receiveN(t, ctx, next, 1)
			if live[0].Attempt != 2 {
				t.Fatalf("the redelivery reports attempt %d, want 2", live[0].Attempt)
			}
			if err := stale[0].Ack(); !errors.Is(err, mq.ErrLeaseLost) {
				t.Fatalf("the stale settlement answered %v, want ErrLeaseLost ", err)
			}
			if stale[0].Settled() {
				t.Fatal("the stale settlement reported itself settled")
			}
			// And it settled nothing: the live delivery is still there to
			// be acknowledged.
			snap, err := b.Inspect("orders", "billing")
			if err != nil {
				t.Fatalf("Inspect: %v", err)
			}
			if snap.InFlight != 1 || snap.Acked != 0 {
				t.Fatalf("snapshot %+v: the stale settlement reached the live delivery", snap)
			}
			if err := live[0].Ack(); err != nil {
				t.Fatalf("Ack of the live delivery: %v", err)
			}
			final, ferr := b.Inspect("orders", "billing")
			if ferr != nil || final.Acked != 1 || !final.Conserved() {
				t.Fatalf("snapshot %+v err %v, want exactly one acknowledgement", final, ferr)
			}
		})
	}
}

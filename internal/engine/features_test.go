package engine

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// pub publishes one record and fails the test on an unexpected error.
func pub(t *testing.T, top *Topic[int64], p Publish[int64]) error {
	t.Helper()
	return top.Publish(context.Background(), p)
}

// TestWildcardRouting: a topic with wildcards routes each publication to the
// subscriptions whose filter matches its subject, and to the ones with no
// filter always.
func TestWildcardRouting(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 16})
	top, err := e.CreateTopic("t", TopologyConfig{Wildcards: true})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	subs := map[string]*Subscription[int64]{}
	for name, filter := range map[string]string{
		"all":    "",
		"orders": "orders.*",
		"tail":   "orders.eu.>",
		"exact":  "orders.eu.de",
	} {
		s, err := top.Subscribe(name, SubscriptionConfig{Filter: filter})
		if err != nil {
			t.Fatalf("Subscribe(%s, %q): %v", name, filter, err)
		}
		subs[name] = s
	}
	for _, subj := range []string{"orders.us", "orders.eu.de", "shipments.us"} {
		if err := pub(t, top, Publish[int64]{Payload: 1, Size: 1, Subject: subj}); err != nil {
			t.Fatalf("Publish(%s): %v", subj, err)
		}
	}
	want := map[string]int{"all": 3, "orders": 1, "tail": 1, "exact": 1}
	for name, n := range want {
		if got := subs[name].Inspect().Ready; got != n {
			t.Fatalf("subscription %s holds %d records, want %d", name, got, n)
		}
	}
	// A publication no subscription matches is discarded and counted as an
	// orphan, exactly like one to a topic with no subscription at all.
	filtered, err := e.CreateTopic("f", TopologyConfig{Wildcards: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := filtered.Subscribe("eu", SubscriptionConfig{Filter: "orders.eu.>"}); err != nil {
		t.Fatal(err)
	}
	orphans := func() int64 { _, _, d := e.Totals(); return d }
	before := orphans()
	released := 0
	if err := pub(t, filtered, Publish[int64]{
		Payload: 9, Size: 1, Subject: "nothing.here", Mode: CopyOwned,
		Release: func(int64) { released++ },
	}); err != nil {
		t.Fatalf("unmatched publication: %v", err)
	}
	if after := orphans(); after != before+1 {
		t.Fatalf("unmatched publication counted %d orphans, want %d", after, before+1)
	}
	if released != 1 {
		t.Fatalf("unmatched publication released %d times", released)
	}
	// An invalid subject is refused rather than routed to nobody.
	if err := pub(t, top, Publish[int64]{Payload: 1, Size: 1, Subject: "a..b"}); err == nil {
		t.Fatal("an invalid subject was accepted")
	}
}

// TestWildcardFilterRefusedWithoutWildcards: a filter a topic would ignore is
// a configuration error, not a subscription that quietly sees everything.
func TestWildcardFilterRefusedWithoutWildcards(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := top.Subscribe("s", SubscriptionConfig{Filter: "a.*"}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("a filter without wildcards = %v, want ErrInvalidConfig", err)
	}
	wt, err := e.CreateTopic("w", TopologyConfig{Wildcards: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Subscribe("s", SubscriptionConfig{Filter: "a..b"}); err == nil {
		t.Fatal("an invalid pattern was accepted")
	}
	// Wildcards and a shared partition log do not compose.
	shared := newTestEngine(t, KindSet{Arch: LogCursor}, TopologyConfig{Partitions: 1})
	if _, err := shared.CreateTopic("w", TopologyConfig{Wildcards: true}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("wildcards under a shared log = %v, want ErrInvalidConfig", err)
	}
}

// TestDedupWindow: a publication carrying a key the topic has seen inside the
// window is discarded, released and counted; one carrying it after the window
// has closed is admitted.
func TestDedupWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 16})
		top, err := e.CreateTopic("t", TopologyConfig{DedupWindow: 10 * tick})
		if err != nil {
			t.Fatal(err)
		}
		sub, err := top.Subscribe("s", SubscriptionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		released := 0
		send := func(key string) error {
			return pub(t, top, Publish[int64]{
				Payload: 1, Size: 1, DedupKey: []byte(key), Mode: CopyOwned,
				Release: func(int64) { released++ },
			})
		}
		if err := send("a"); err != nil {
			t.Fatal(err)
		}
		if err := send("a"); err != nil {
			t.Fatalf("a duplicate must be reported as accepted: %v", err)
		}
		if err := send("b"); err != nil {
			t.Fatal(err)
		}
		if got := sub.Inspect().Ready; got != 2 {
			t.Fatalf("the subscription holds %d records, want 2", got)
		}
		if got := top.Deduplicated(); got != 1 {
			t.Fatalf("Deduplicated = %d, want 1", got)
		}
		if got := sub.Inspect().Deduplicated; got != 1 {
			t.Fatalf("Snapshot.Deduplicated = %d, want 1", got)
		}
		if released != 1 {
			t.Fatalf("the discarded duplicate released %d payloads, want 1", released)
		}
		// Past the window the same key is admitted again.
		time.Sleep(11 * tick)
		if err := send("a"); err != nil {
			t.Fatal(err)
		}
		if got := sub.Inspect().Ready; got != 3 {
			t.Fatalf("after the window the subscription holds %d records, want 3", got)
		}
		// A publication with no key is never deduplicated.
		for range 3 {
			if err := send(""); err != nil {
				t.Fatal(err)
			}
		}
		if got := top.Deduplicated(); got != 1 {
			t.Fatalf("keyless publications were deduplicated: %d", got)
		}
	})
}

// TestDedupKeySurvivesRefusal: a publication the engine refused leaves its
// dedup key free. The window has to be written before the capacity is
// reserved — two publications of one key must not both pass the check — so a
// refusal arrives with the key already registered; keeping it there would
// make the retry the ErrQueueFull asks for a duplicate: reported as success,
// its owned payload released and nothing admitted, which is a lost message.
func TestDedupKeySurvivesRefusal(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 1, CapacityMessages: 16})
	top, err := e.CreateTopic("t", TopologyConfig{
		Partitions: 1, CapacityMessages: 1, DedupWindow: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	released := map[int64]int{}
	send := func(v int64, key string) error {
		return top.Publish(context.Background(), Publish[int64]{
			Payload: v, Size: 1, Partition: 0, DedupKey: []byte(key), Mode: CopyOwned,
			Release: func(x int64) { released[x]++ },
		})
	}
	// The lane holds one record, so the keyed publication is refused.
	if err := send(1, ""); err != nil {
		t.Fatalf("the first publication: %v", err)
	}
	if err := send(2, "k"); !errors.Is(err, errcode.ErrQueueFull) {
		t.Fatalf("the keyed publication into a full lane = %v, want ErrQueueFull", err)
	}
	if released[2] != 0 {
		t.Fatalf("the refused payload was released %d times: the caller still owns it", released[2])
	}
	// Empty the lane and make the retry the error asked for.
	dst := make([]Claim[int64], 4)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Fetch: %v", err)
	}
	for i := range n {
		if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if err := send(2, "k"); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if got := top.Deduplicated(); got != 0 {
		t.Fatalf("the retry was discarded as a duplicate: Deduplicated = %d", got)
	}
	if got := sub.Inspect().Ready; got != 1 {
		t.Fatalf("the retry admitted %d records, want 1", got)
	}
	if released[2] != 0 {
		t.Fatalf("the retried payload was released early: %v", released)
	}
	// The key is registered again by the admission, so a genuine duplicate
	// is still discarded.
	if err := send(3, "k"); err != nil {
		t.Fatalf("the duplicate: %v", err)
	}
	if got := top.Deduplicated(); got != 1 {
		t.Fatalf("the duplicate after a successful retry was admitted: Deduplicated = %d", got)
	}
}

// TestDedupWindowPrunes: the key table does not grow without bound.
func TestDedupWindowPrunes(t *testing.T) {
	d := newDedupWindow(10)
	for i := range 4 * dedupPruneFloor {
		key := []byte{byte(i), byte(i >> 8)}
		if !d.admit(key, int64(i)) {
			t.Fatalf("key %d was refused", i)
		}
	}
	if n := d.len(); n > 2*dedupPruneFloor {
		t.Fatalf("the window holds %d keys after %d publications", n, 4*dedupPruneFloor)
	}
}

// TestPriorityRings: a lane serves its most urgent ring first, a redelivery
// keeps its level, and a full lane displaces the least urgent record.
func TestPriorityRings(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1, CapacityMessages: 8})
	top, err := e.CreateTopic("t", TopologyConfig{Priority: true})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	// Published low first, high last; the high one comes out first.
	for i, prio := range []uint8{0, 1, PriorityLevels - 1, 9 /* clamped */} {
		if err := pub(t, top, Publish[int64]{
			Payload: int64(i), Size: 1, Priority: prio,
		}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 4)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("fetched %d records, want 4", n)
	}
	got := make([]int64, n)
	for i := range n {
		got[i] = dst[i].Payload
	}
	// Levels: payload 2 and 3 are both at the top level and keep their
	// publication order; then 1, then 0.
	want := []int64{2, 3, 1, 0}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("priority order %v, want %v", got, want)
		}
	}
	// Priorities order the lane's store, not its redeliveries: a record
	// that failed is served before the store whatever the levels are (family
	// The specification), and the levels then decide among the records nobody has tried.
	if err := c.Nack(dst[3].Env, dst[3].Gen, 0); err != nil { // level 0
		t.Fatal(err)
	}
	if err := pub(t, top, Publish[int64]{Payload: 7, Size: 1, Priority: 3}); err != nil {
		t.Fatal(err)
	}
	if err := pub(t, top, Publish[int64]{Payload: 8, Size: 1, Priority: 1}); err != nil {
		t.Fatal(err)
	}
	// Settle the remaining leases so the lane is free to serve.
	for _, i := range []int{0, 1, 2} {
		if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
			t.Fatal(err)
		}
	}
	// The redelivery comes first. Asking for one record is what makes that
	// the general contract rather than one placement: every
	// placement serves its redelivery queue before the lane's store, but only
	// a *sealing* one serves the redelivery alone, so a fetch with room for
	// three takes 7 and 8 along with it under a placement that does not seal
	// and leaves nothing for the priority assertion below. That is how the
	// campaign's PushFront binding turned this test into "priority order
	// among fresh records [], want [7 8]" — an empty list, because the
	// records it wanted to order had already been served and acked by the
	// fetch above (adr/ADR-017-assumptions.md). The seal
	// itself is TestModel/<placement>/blocks-lane.
	n, err = c.Fetch(poll(t), dst[:1])
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 1 || dst[0].Payload != 0 {
		t.Fatalf("the redelivery was not served first: %d records, first %d", n, dst[0].Payload)
	}
	for i := range n {
		if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
			t.Fatal(err)
		}
	}
	got = got[:0]
	for len(got) < 2 {
		n, err = c.Fetch(poll(t), dst[:2])
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		for i := range n {
			got = append(got, dst[i].Payload)
			if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(got) != 2 || got[0] != 7 || got[1] != 8 {
		t.Fatalf("priority order among fresh records %v, want [7 8]", got)
	}
}

// TestPriorityDisplacesLeastUrgent: OverflowDropOldest gives up bulk work
// before expedited work.
func TestPriorityDisplacesLeastUrgent(t *testing.T) {
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{
		Partitions: 1, CapacityMessages: 2, Overflow: OverflowDropOldest,
	})
	top, err := e.CreateTopic("t", TopologyConfig{Priority: true})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pub(t, top, Publish[int64]{Payload: 1, Size: 1, Priority: 3}); err != nil {
		t.Fatal(err)
	}
	if err := pub(t, top, Publish[int64]{Payload: 2, Size: 1, Priority: 0}); err != nil {
		t.Fatal(err)
	}
	// The lane is full; the newcomer displaces the least urgent record.
	if err := pub(t, top, Publish[int64]{Payload: 3, Size: 1, Priority: 2}); err != nil {
		t.Fatal(err)
	}
	dst := make([]Claim[int64], 4)
	n, err := c.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	got := make([]int64, n)
	for i := range n {
		got[i] = dst[i].Payload
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("lane holds %v, want the urgent records [1 3]", got)
	}
}

// TestPriorityRefusals: priorities do not compose with a shared partition log
// or with strict ordering.
func TestPriorityRefusals(t *testing.T) {
	shared := newTestEngine(t, KindSet{Arch: LogCursor}, TopologyConfig{Partitions: 1})
	if _, err := shared.CreateTopic("p", TopologyConfig{Priority: true}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("priorities under a shared log = %v, want ErrInvalidConfig", err)
	}
	// Pinned: under a shared log the topic would be refused for the log
	// rather than for the ordering, so the assertion would pass without
	// observing what it names.
	e := newTestEngine(t, KindSet{Arch: featureArch}, TopologyConfig{Partitions: 1})
	if _, err := e.CreateTopic("p", TopologyConfig{
		Priority: true, Ordering: OrderingStrict,
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("priorities under strict ordering = %v, want ErrInvalidConfig", err)
	}
}

// TestStickyAssignment: each lane belongs to one consumer, a join and a leave
// rebalance and bump the generation, and a consumer never claims from a lane
// it does not own.
func TestStickyAssignment(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 4, CapacityMessages: 16})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Sticky: true})
	if err != nil {
		t.Fatal(err)
	}
	if !sub.Sticky() {
		t.Fatal("the subscription is not sticky")
	}
	c1, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	v1, owners := sub.Assignment()
	for i, o := range owners {
		if o != c1.ID() {
			t.Fatalf("lane %d belongs to %d, want the only consumer %d", i, o, c1.ID())
		}
	}
	c2, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	v2, owners := sub.Assignment()
	if v2 <= v1 {
		t.Fatalf("a join did not bump the generation: %d then %d", v1, v2)
	}
	mine := map[uint32]int{}
	for _, o := range owners {
		mine[o]++
	}
	if mine[c1.ID()] != 2 || mine[c2.ID()] != 2 {
		t.Fatalf("two consumers over four lanes hold %v", mine)
	}
	for part := range 4 {
		if err := pub(t, top, Publish[int64]{Payload: int64(part), Size: 1, Partition: part}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 8)
	n, err := c1.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("the first consumer claimed %d records, want its two lanes", n)
	}
	for i := range n {
		if got := owners[dst[i].Info.Partition]; got != c1.ID() {
			t.Fatalf("claimed from lane %d, which belongs to %d", dst[i].Info.Partition, got)
		}
	}
	// A leave hands the lanes back and revokes the leases held on them.
	if err := c2.Close(); err != nil {
		t.Fatal(err)
	}
	v3, owners := sub.Assignment()
	if v3 <= v2 {
		t.Fatalf("a leave did not bump the generation: %d then %d", v2, v3)
	}
	for i, o := range owners {
		if o != c1.ID() {
			t.Fatalf("after the leave lane %d belongs to %d", i, o)
		}
	}
	// The leases the departing consumer held are back in their lanes; the
	// surviving consumer's own leases are untouched.
	if snap := sub.Inspect(); snap.InFlight != 2 || snap.Ready != 2 {
		t.Fatalf("after the leave: %+v", snap)
	}
}

// TestRebalanceRevokesLeases: a rebalance that moves a lane takes the leases
// held on it away from the consumer that no longer owns it, and never counts
// the revocation as a delivery failure.
func TestRebalanceRevokesLeases(t *testing.T) {
	e := newTestEngine(t, KindSet{}, TopologyConfig{
		Partitions: 2, CapacityMessages: 8, MaxAttempts: 1,
	})
	top, err := e.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Sticky: true})
	if err != nil {
		t.Fatal(err)
	}
	c1, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for part := range 2 {
		if err := pub(t, top, Publish[int64]{Payload: int64(part), Size: 1, Partition: part}); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Claim[int64], 4)
	if n, err := c1.Fetch(poll(t), dst); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	} else if n != 2 {
		t.Fatalf("claimed %d records, want 2", n)
	}
	// A second consumer takes one lane; the lease held on it is revoked.
	c2, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	snap := sub.Inspect()
	if snap.InFlight != 1 || snap.Ready != 1 {
		t.Fatalf("after the rebalance: %+v", snap)
	}
	if snap.DeadLettered != 0 {
		t.Fatalf("a rebalance dead-lettered %d records", snap.DeadLettered)
	}
	// The record the rebalance took back is deliverable to its new owner,
	// even though MaxAttempts is 1 and it has already been delivered once:
	// a revocation is not a failure.
	n, err := c2.Fetch(poll(t), dst)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the new owner claimed %d records, want 1", n)
	}
	// A stale settlement from the previous owner is refused.
	if err := c1.Ack(dst[0].Env, dst[0].Gen); err == nil {
		t.Fatal("the previous owner settled a revoked delivery")
	}
}

// TestRebalanceAfterCloseKeepsAssignment: a close stops every control
// operation, Rebalance among them. Once the subscription or the engine is
// closed nothing is admitted and there is no lease left to revoke, so the
// table is what it was: moving lanes nobody can claim from and bumping a
// generation nothing reads would make Rebalance the one control operation a
// close does not stop (the memory engine and).
//
// This is the contributor notes's recurring defect class read on the control path
// rather than the placement path — an operation that mutates state the engine
// holds without first observing the closed flags — and it is what the
// FuzzEngineRetention corpus entry rebalance-after-close pins differentially:
// internal/model/memq refuses a Rebalance on a closed model, so an engine
// that accepts one reports a generation the model never reaches.
func TestRebalanceAfterCloseKeepsAssignment(t *testing.T) {
	for _, tc := range []struct {
		name  string
		close func(t *testing.T, e *Engine[int64], sub *Subscription[int64])
	}{
		{"subscription", func(t *testing.T, _ *Engine[int64], sub *Subscription[int64]) {
			if err := sub.Close(); err != nil {
				t.Fatalf("subscription close: %v", err)
			}
		}},
		{"engine", func(t *testing.T, e *Engine[int64], _ *Subscription[int64]) {
			if err := e.Close(); err != nil {
				t.Fatalf("engine close: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newTestEngine(t, KindSet{}, TopologyConfig{Partitions: 2, CapacityMessages: 8})
			top, err := e.CreateTopic("t", TopologyConfig{})
			if err != nil {
				t.Fatal(err)
			}
			sub, err := top.Subscribe("s", SubscriptionConfig{Sticky: true})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sub.Attach(ConsumerConfig{}); err != nil {
				t.Fatal(err)
			}
			want, owners := sub.Assignment()
			tc.close(t, e, sub)
			// Every call is refused, not merely the first: a closed
			// subscription has no membership to deal lanes out to.
			for i := range 4 {
				if got := sub.Rebalance(); got != want {
					t.Fatalf("Rebalance %d after the %s close returned generation %d, want the standing %d",
						i, tc.name, got, want)
				}
			}
			got, gotOwners := sub.Assignment()
			if got != want || !slices.Equal(gotOwners, owners) {
				t.Fatalf("after the %s close the assignment moved from %d%v to %d%v",
					tc.name, want, owners, got, gotOwners)
			}
		})
	}
}

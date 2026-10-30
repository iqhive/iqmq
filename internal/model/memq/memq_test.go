package memq

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
)

// newModel builds a model with one lane and a small capacity.
func newModel(t *testing.T, cfg Config) *Model {
	t.Helper()
	if cfg.Partitions == 0 {
		cfg.Partitions = 1
	}
	if cfg.AckDeadline == 0 {
		cfg.AckDeadline = 10 * time.Millisecond
	}
	cfg.Ordered = true
	m := New(cfg)
	if err := m.Invariants(); err != nil {
		t.Fatalf("fresh model: %v", err)
	}
	return m
}

// step applies op and asserts the invariants still hold.
func step(t *testing.T, m *Model, op Op) Result {
	t.Helper()
	res := m.Step(op)
	if err := m.Invariants(); err != nil {
		t.Fatalf("%v: invariant %v", op.Kind, err)
	}
	return res
}

func TestStateAndKindNames(t *testing.T) {
	for s := State(0); s < stateCount; s++ {
		if s.String() == "" || strings.Contains(s.String(), "?") {
			t.Fatalf("state %d has no name", s)
		}
	}
	if State(99).String() != "state(?)" {
		t.Fatalf("undefined state renders as %q", State(99))
	}
	if !Acked.Terminal() || !Dropped.Terminal() || Ready.Terminal() {
		t.Fatal("Terminal is wrong")
	}
	for k := Kind(0); k < kindCount; k++ {
		if k.String() == "" || strings.Contains(k.String(), "?") {
			t.Fatalf("kind %d has no name", k)
		}
	}
	if Kind(99).String() != "op(?)" {
		t.Fatalf("undefined kind renders as %q", Kind(99))
	}
}

func TestUndefinedOperationFails(t *testing.T) {
	if res := New(Config{}).Step(Op{Kind: Kind(99)}); res.Err == nil {
		t.Fatal("an undefined operation did not fail")
	}
}

// TestPublishFetchAck walks the happy path and checks the counters.
func TestPublishFetchAck(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	for range 2 {
		if res := step(t, m, Op{Kind: Publish, Partition: 0, Size: 8}); res.Err != nil {
			t.Fatalf("publish: %v", res.Err)
		}
	}
	res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 2, Observed: []uint64{1, 2}})
	if res.Err != nil || res.N != 2 {
		t.Fatalf("fetch: %+v", res)
	}
	if res.Claims[0].Attempts != 1 || res.Claims[0].Gen != 1 {
		t.Fatalf("claim: %+v", res.Claims[0])
	}
	if r := step(t, m, Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}); r.Err != nil {
		t.Fatalf("ack: %v", r.Err)
	}
	if m.Released(1) != 1 {
		t.Fatalf("record 1 released %d times", m.Released(1))
	}
	snap, ok := m.Sub("s")
	if !ok || snap.Published != 2 || snap.Acked != 1 || snap.InFlight != 1 || !snap.Conserved() {
		t.Fatalf("%+v", snap)
	}
	if got := m.Snapshot(); len(got.Subs) != 1 || got.Steps != m.Steps() || got.Messages != 1 {
		t.Fatalf("%+v", got)
	}
	if _, ok := m.Sub("missing"); ok {
		t.Fatal("Sub returned an unknown subscription")
	}
	if names := m.Subscriptions(); len(names) != 1 || names[0] != "s" {
		t.Fatalf("Subscriptions() = %v", names)
	}
}

// TestSettlementErrors covers the settlement error table.
func TestSettlementErrors(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 2})
	step(t, m, Op{Kind: Publish, Size: 1})
	step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{1}})
	cases := []struct {
		name string
		op   Op
		want error
	}{
		{"foreign consumer", Op{Kind: Ack, Sub: "s", Consumer: 2, Seq: 1, Gen: 1}, errcode.ErrLeaseLost},
		{"stale generation", Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 2}, errcode.ErrLeaseLost},
		{"unknown record", Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 9, Gen: 1}, errcode.ErrNotSettleable},
		{"unknown subscription", Op{Kind: Ack, Sub: "x", Consumer: 1, Seq: 1, Gen: 1}, errcode.ErrLeaseLost},
		{"extend foreign", Op{Kind: Extend, Sub: "s", Consumer: 2, Seq: 1, Gen: 1}, errcode.ErrLeaseLost},
		{"extend stale", Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 5}, errcode.ErrLeaseLost},
		{"extend unknown", Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 9, Gen: 1}, errcode.ErrNotSettleable},
		{"extend unknown sub", Op{Kind: Extend, Sub: "x", Consumer: 1, Seq: 1, Gen: 1}, errcode.ErrLeaseLost},
	}
	for _, c := range cases {
		if res := step(t, m, c.op); !errors.Is(res.Err, c.want) {
			t.Fatalf("%s: %v, want %v", c.name, res.Err, c.want)
		}
	}
	if res := step(t, m, Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 1, Dur: time.Second}); res.Err != nil {
		t.Fatalf("extend: %v", res.Err)
	}
	if res := step(t, m, Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}); res.Err != nil {
		t.Fatalf("ack: %v", res.Err)
	}
	for _, c := range []struct {
		name string
		op   Op
		want error
	}{
		{"double ack", Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}, errcode.ErrAlreadySettled},
		{"nack after ack", Op{Kind: Nack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}, errcode.ErrAlreadySettled},
		{"extend after ack", Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}, errcode.ErrAlreadySettled},
	} {
		res := step(t, m, c.op)
		if !errors.Is(res.Err, c.want) || !res.Stale {
			t.Fatalf("%s: %v (stale=%v), want %v", c.name, res.Err, res.Stale, c.want)
		}
	}
}

// TestNackAndExpiryRequeue checks redelivery through both routes.
func TestNackAndExpiryRequeue(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4, AckDeadline: 10 * time.Millisecond})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	step(t, m, Op{Kind: Publish, Size: 1})
	step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{1}})
	step(t, m, Op{Kind: Nack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1, Delay: 5 * time.Millisecond})
	if s, _ := m.Sub("s"); s.Delayed != 1 || s.Nacked != 1 || s.Requeued != 1 {
		t.Fatalf("%+v", s)
	}
	if res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{}}); res.N != 0 {
		t.Fatalf("delivered before the due time: %+v", res)
	}
	step(t, m, Op{Kind: Advance, Dt: 5 * time.Millisecond})
	res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{1}})
	if res.N != 1 || res.Claims[0].Attempts != 2 || res.Claims[0].Gen != 2 {
		t.Fatalf("%+v", res)
	}
	if n := step(t, m, Op{Kind: ExpireLeases}).N; n != 0 {
		t.Fatalf("a live lease expired: %d", n)
	}
	// Advance does the scheduler's work as the clock passes, so the deadline
	// reclaims the lease on its own and an explicit sweep finds nothing.
	step(t, m, Op{Kind: Advance, Dt: 20 * time.Millisecond})
	if n := step(t, m, Op{Kind: ExpireLeases}).N; n != 0 {
		t.Fatalf("the clock move left an expired lease behind: %d", n)
	}
	if s, _ := m.Sub("s"); s.Expired != 1 || s.Ready != 1 {
		t.Fatalf("%+v", s)
	}
	if res := step(t, m, Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 2}); !errors.Is(res.Err, errcode.ErrLeaseLost) {
		t.Fatalf("ack after expiry: %v", res.Err)
	}
	if m.Now() != 25*time.Millisecond {
		t.Fatalf("clock = %v", m.Now())
	}
}

// TestCapacityAndFanout: the atomic policy refuses for every subscription.
func TestCapacityAndFanout(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 1, MaxTotalMessages: 4, MaxTotalBytes: 32})
	step(t, m, Op{Kind: Subscribe, Sub: "a"})
	step(t, m, Op{Kind: Subscribe, Sub: "b"})
	if res := step(t, m, Op{Kind: Publish, Size: 8}); res.Err != nil {
		t.Fatalf("first publish: %v", res.Err)
	}
	res := step(t, m, Op{Kind: Publish, Size: 8})
	if !errors.Is(res.Err, errcode.ErrQueueFull) {
		t.Fatalf("second publish: %v", res.Err)
	}
	for _, name := range []string{"a", "b"} {
		s, _ := m.Sub(name)
		if s.Published != 1 || s.Rejected != 1 || s.Ready != 1 {
			t.Fatalf("%s: %+v", name, s)
		}
	}
	// The engine-wide byte budget refuses even when the lane has room.
	big := newModel(t, Config{CapacityMessages: 10, MaxTotalBytes: 4})
	step(t, big, Op{Kind: Subscribe, Sub: "s"})
	if res := big.Step(Op{Kind: Publish, Size: 8}); !errors.Is(res.Err, errcode.ErrQueueFull) {
		t.Fatalf("byte budget: %v", res.Err)
	}
}

// TestDropOldestSparesALaneThatCannotMakeEngineWideRoom: a lane gives back
// only its own share of the engine-wide limits, so under DropOldest a record
// that would not fit even with the lane emptied displaces nothing, and one
// that would displaces as many records as it needs (engine
// Topic.reserveTotalIn).
func TestDropOldestSparesALaneThatCannotMakeEngineWideRoom(t *testing.T) {
	m := newModel(t, Config{
		Partitions: 2, CapacityMessages: 16, MaxTotalBytes: 100, Overflow: OverflowDropOldest,
	})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	if res := step(t, m, Op{Kind: Publish, Partition: 1, Size: 95}); res.Err != nil {
		t.Fatalf("publish to the other lane: %v", res.Err)
	}
	for i := range 5 {
		if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
			t.Fatalf("publish %d: %v", i, res.Err)
		}
	}
	if res := step(t, m, Op{Kind: Publish, Size: 10}); !errors.Is(res.Err, errcode.ErrQueueFull) {
		t.Fatalf("publication no drop in the lane makes room for: %v", res.Err)
	}
	if s, _ := m.Sub("s"); s.Dropped != 0 || s.Ready != 6 {
		t.Fatalf("the lane was emptied for a refused publication: %+v", s)
	}
	if res := step(t, m, Op{Kind: Publish, Size: 3}); res.Err != nil {
		t.Fatalf("publication three drops make room for: %v", res.Err)
	}
	if s, _ := m.Sub("s"); s.Dropped != 3 || s.Ready != 4 {
		t.Fatalf("after the displacement: %+v", s)
	}
}

// TestDropOldestDisplacesOnlyForAnAdmission: one subscription's refusal
// refuses the publication for all of them, so under an engine-wide limit the
// subscriptions make room for the whole publication together or not at all
// (engine Topic.reserveAll). A subscription with nothing to drop refused the
// publication after the one before it had displaced records for it.
func TestDropOldestDisplacesOnlyForAnAdmission(t *testing.T) {
	setup := func(t *testing.T, maxMsgs int, maxBytes, size int64, n int) *Model {
		t.Helper()
		m := newModel(t, Config{
			CapacityMessages: 16, AckDeadline: time.Hour, MaxTotalMessages: maxMsgs, MaxTotalBytes: maxBytes,
			Overflow: OverflowDropOldest,
		})
		step(t, m, Op{Kind: Subscribe, Sub: "a"})
		step(t, m, Op{Kind: Subscribe, Sub: "b"})
		step(t, m, Op{Kind: Attach, Sub: "b", Consumer: 1})
		seqs := make([]uint64, n)
		for i := range n {
			if res := step(t, m, Op{Kind: Publish, Size: size}); res.Err != nil {
				t.Fatalf("publish %d: %v", i, res.Err)
			}
			seqs[i] = uint64(i + 1)
		}
		if res := step(t, m, Op{Kind: Fetch, Sub: "b", Consumer: 1, Max: n, Observed: seqs}); res.Err != nil {
			t.Fatalf("b fetch: %v", res.Err)
		}
		return m
	}
	t.Run("refused", func(t *testing.T) {
		// b leases all five two-byte records; emptying a frees ten of the
		// sixteen bytes an eight-byte publication needs.
		m := setup(t, 0, 20, 2, 5)
		if res := step(t, m, Op{Kind: Publish, Size: 8}); !errors.Is(res.Err, errcode.ErrQueueFull) {
			t.Fatalf("publication no drop makes room for: %v", res.Err)
		}
		if a, _ := m.Sub("a"); a.Dropped != 0 || a.Ready != 5 {
			t.Fatalf("a lost records to a refused publication: %+v", a)
		}
	})
	t.Run("admitted", func(t *testing.T) {
		// b leases both records; a alone gives back the two the third needs.
		m := setup(t, 4, 0, 1, 2)
		if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
			t.Fatalf("publication a's records make room for: %v", res.Err)
		}
		if a, _ := m.Sub("a"); a.Dropped != 2 || a.Ready != 1 {
			t.Fatalf("a after the displacement: %+v", a)
		}
		if b, _ := m.Sub("b"); b.Dropped != 0 || b.InFlight != 2 || b.Ready != 1 {
			t.Fatalf("b after the displacement: %+v", b)
		}
	})
}

// TestDropOldestSpreadsTheDisplacementOverTheLanes: the lanes of a
// publication make engine-wide room for it in turn, one record each per pass,
// as a shared log does (engine Topic.makeRoomTogether). The ones that hold
// records pay for the one that holds none evenly.
func TestDropOldestSpreadsTheDisplacementOverTheLanes(t *testing.T) {
	for _, shared := range []bool{false, true} {
		m := newModel(t, Config{
			CapacityMessages: 16, AckDeadline: time.Hour, MaxTotalMessages: 6,
			Overflow: OverflowDropOldest, SharedLog: shared,
		})
		for _, name := range []string{"a", "b", "c"} {
			step(t, m, Op{Kind: Subscribe, Sub: name})
		}
		step(t, m, Op{Kind: Attach, Sub: "b", Consumer: 1})
		for i := range 2 {
			if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
				t.Fatalf("shared=%v: publish %d: %v", shared, i, res.Err)
			}
		}
		if res := step(t, m, Op{Kind: Fetch, Sub: "b", Consumer: 1, Max: 2, Observed: []uint64{1, 2}}); res.Err != nil {
			t.Fatalf("shared=%v: b fetch: %v", shared, res.Err)
		}
		if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
			t.Fatalf("shared=%v: publication the lanes make room for together: %v", shared, res.Err)
		}
		for name, want := range map[string]int64{"a": 2, "b": 0, "c": 2} {
			if s, _ := m.Sub(name); s.Dropped != want {
				t.Fatalf("shared=%v: %s dropped %d, want %d", shared, name, s.Dropped, want)
			}
		}
	}
}

// TestSharedLogDropOldestDisplacesOnlyTheBaseSlot: a full shared log has room
// only its base can make, so DropOldest takes the base slot from every
// subscription still holding it and nothing from one that has read past it
// (engine Topic.reclaimBase). The model dropped every subscription's own
// oldest record, which took the newest one of a subscription that had caught
// up and reclaimed nothing.
func TestSharedLogDropOldestDisplacesOnlyTheBaseSlot(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4, AckDeadline: time.Hour, SharedLog: true, Overflow: OverflowDropOldest})
	step(t, m, Op{Kind: Subscribe, Sub: "a"})
	step(t, m, Op{Kind: Subscribe, Sub: "b"})
	step(t, m, Op{Kind: Attach, Sub: "b", Consumer: 1})
	for i := range 4 {
		if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
			t.Fatalf("publish %d: %v", i, res.Err)
		}
	}
	step(t, m, Op{Kind: Fetch, Sub: "b", Consumer: 1, Max: 1, Observed: []uint64{1}})
	if res := step(t, m, Op{Kind: Ack, Sub: "b", Consumer: 1, Seq: 1, Gen: 1}); res.Err != nil {
		t.Fatalf("ack: %v", res.Err)
	}
	if res := step(t, m, Op{Kind: Publish, Size: 1}); res.Err != nil {
		t.Fatalf("publish into the full log: %v", res.Err)
	}
	b, _ := m.Sub("b")
	if held, _ := m.Held("b"); b.Dropped != 0 || !slices.Equal(held, []uint64{2, 3, 4, 5}) {
		t.Fatalf("b, which had read past the base, dropped %d and holds %v, want 0 and [2 3 4 5]", b.Dropped, held)
	}
	a, _ := m.Sub("a")
	if held, _ := m.Held("a"); a.Dropped != 1 || !slices.Equal(held, []uint64{2, 3, 4, 5}) {
		t.Fatalf("a, which held the base, dropped %d and holds %v, want 1 and [2 3 4 5]", a.Dropped, held)
	}
	// A lease on the base keeps it, and nothing is dropped for a
	// publication the log then refuses.
	step(t, m, Op{Kind: Attach, Sub: "a", Consumer: 2})
	step(t, m, Op{Kind: Fetch, Sub: "a", Consumer: 2, Max: 1, Observed: []uint64{2}})
	if res := step(t, m, Op{Kind: Publish, Size: 1}); !errors.Is(res.Err, errcode.ErrQueueFull) {
		t.Fatalf("publication behind a leased base: %v", res.Err)
	}
	for _, name := range []string{"a", "b"} {
		if s, _ := m.Sub(name); s.Dropped != map[string]int64{"a": 1, "b": 0}[name] {
			t.Fatalf("%s lost records to a refused publication: %+v", name, s)
		}
	}
}

// TestOrphanPublishReleases: no subscription means the record is discarded.
func TestOrphanPublishReleases(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4})
	if res := step(t, m, Op{Kind: Publish, Size: 4}); res.Err != nil {
		t.Fatalf("orphan publish: %v", res.Err)
	}
	if m.Orphans() != 1 {
		t.Fatalf("orphans = %d", m.Orphans())
	}
}

// TestCloseConsumerRequeues follows the close policy.
func TestCloseConsumerRequeues(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	step(t, m, Op{Kind: Publish, Size: 1})
	step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{1}})
	step(t, m, Op{Kind: CloseConsumer, Sub: "s", Consumer: 1})
	s, _ := m.Sub("s")
	if s.Ready != 1 || s.Requeued != 1 || s.ActiveConsumers != 0 {
		t.Fatalf("%+v", s)
	}
	if res := step(t, m, Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: 1, Gen: 1}); !errors.Is(res.Err, errcode.ErrClosed) {
		t.Fatalf("ack from a closed consumer: %v", res.Err)
	}
	// Closing a consumer that is not attached, or of an unknown
	// subscription, is a no-op.
	step(t, m, Op{Kind: CloseConsumer, Sub: "s", Consumer: 9})
	step(t, m, Op{Kind: CloseConsumer, Sub: "x", Consumer: 1})
	if res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1}); !errors.Is(res.Err, errcode.ErrClosed) {
		t.Fatalf("fetch from a closed consumer: %v", res.Err)
	}
}

// TestCloseDropsAndReleases checks the close policy of the whole engine and
// the post-close contract.
func TestCloseDropsAndReleases(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4})
	step(t, m, Op{Kind: Subscribe, Sub: "a"})
	step(t, m, Op{Kind: Subscribe, Sub: "b"})
	step(t, m, Op{Kind: Attach, Sub: "a", Consumer: 1})
	step(t, m, Op{Kind: Publish, Size: 2})
	step(t, m, Op{Kind: Publish, Size: 2, Delay: time.Second})
	step(t, m, Op{Kind: Fetch, Sub: "a", Consumer: 1, Max: 1, Observed: []uint64{1}})
	step(t, m, Op{Kind: CloseSubscription, Sub: "b"})
	if s, _ := m.Sub("b"); s.Dropped != 2 || s.Ready != 0 {
		t.Fatalf("b: %+v", s)
	}
	if m.Released(1) != 0 {
		t.Fatal("a record was released while subscription a still holds it")
	}
	step(t, m, Op{Kind: CloseSubscription, Sub: "b"}) // idempotent
	step(t, m, Op{Kind: CloseEngine})
	step(t, m, Op{Kind: CloseEngine}) // idempotent
	if !m.Closed() {
		t.Fatal("Closed() is false")
	}
	for seq := uint64(1); seq <= 2; seq++ {
		if m.Released(seq) != 1 {
			t.Fatalf("record %d released %d times", seq, m.Released(seq))
		}
	}
	for _, op := range []Op{
		{Kind: Publish, Size: 1},
		{Kind: Subscribe, Sub: "c"},
		{Kind: Attach, Sub: "a", Consumer: 2},
		{Kind: Fetch, Sub: "a", Consumer: 1, Max: 1},
	} {
		if res := step(t, m, op); !errors.Is(res.Err, errcode.ErrClosed) {
			t.Fatalf("%v after close: %v", op.Kind, res.Err)
		}
	}
}

// TestObservationIsChecked: an observation the model does not allow is
// reported as a mismatch rather than silently adopted.
func TestObservationIsChecked(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	step(t, m, Op{Kind: Publish, Size: 1})
	step(t, m, Op{Kind: Publish, Size: 1})
	if res := m.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 2, Observed: []uint64{2, 1}}); !errors.Is(res.Err, ErrMismatch) {
		t.Fatalf("out-of-order observation: %v", res.Err)
	}
	if res := m.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{1, 2}}); !errors.Is(res.Err, ErrMismatch) {
		t.Fatalf("over-long observation: %v", res.Err)
	}
	// An unordered ready queue accepts either order.
	u := New(Config{Partitions: 1, CapacityMessages: 4, AckDeadline: time.Second})
	u.Step(Op{Kind: Subscribe, Sub: "s"})
	u.Step(Op{Kind: Attach, Sub: "s", Consumer: 1})
	u.Step(Op{Kind: Publish, Size: 1})
	u.Step(Op{Kind: Publish, Size: 1})
	if res := u.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 2, Observed: []uint64{2, 1}}); res.Err != nil {
		t.Fatalf("unordered observation: %v", res.Err)
	}
	if res := u.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{9}}); !errors.Is(res.Err, ErrMismatch) {
		t.Fatalf("invented record: %v", res.Err)
	}
}

// TestRoundRobinAndPartitions checks lane selection and the per-consumer
// rotation.
func TestRoundRobinAndPartitions(t *testing.T) {
	m := newModel(t, Config{Partitions: 2, CapacityMessages: 4})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	for range 4 {
		step(t, m, Op{Kind: Publish, Partition: -1, Size: 1})
	}
	res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 4, Observed: []uint64{1, 3, 2, 4}})
	if res.Err != nil {
		t.Fatalf("round-robin fetch: %v", res.Err)
	}
	if res.Claims[0].Partition != 0 || res.Claims[2].Partition != 1 {
		t.Fatalf("lanes: %+v", res.Claims)
	}
	// The next fetch starts at the following lane.
	step(t, m, Op{Kind: Publish, Partition: 1, Size: 1})
	step(t, m, Op{Kind: Publish, Partition: 0, Size: 1})
	res = step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 2, Observed: []uint64{5, 6}})
	if res.Err != nil || res.Claims[0].Partition != 1 {
		t.Fatalf("rotation: %+v %v", res.Claims, res.Err)
	}
	// An explicit partition wraps.
	step(t, m, Op{Kind: Publish, Partition: 5, Size: 1})
	if s, _ := m.Sub("s"); s.Published != 7 {
		t.Fatalf("%+v", s)
	}
}

// TestInvariantsCatchCorruption: the invariants are not vacuous.
func TestInvariantsCatchCorruption(t *testing.T) {
	build := func() (*Model, *sub) {
		m := New(Config{Partitions: 1, CapacityMessages: 4, AckDeadline: time.Second, Ordered: true})
		m.Step(Op{Kind: Subscribe, Sub: "s"})
		m.Step(Op{Kind: Attach, Sub: "s", Consumer: 1})
		m.Step(Op{Kind: Publish, Size: 2})
		_, s := m.find("s")
		return m, s
	}
	cases := []struct {
		name    string
		corrupt func(m *Model, s *sub)
		want    string
	}{
		{"state disagrees with the lane", func(_ *Model, s *sub) { s.recs[1].state = InFlight }, "OneState"},
		{"record queued twice", func(_ *Model, s *sub) {
			s.lanes[0].ready.push(1, 0)
			s.lanes[0].depth++
		}, "OneState"},
		{"depth accounting drifts", func(_ *Model, s *sub) { s.lanes[0].depth = 5 }, "CapacityNonNegative"},
		{"negative byte accounting", func(_ *Model, s *sub) { s.lanes[0].bytes = -1 }, "CapacityNonNegative"},
		{"conservation broken", func(_ *Model, s *sub) { s.st.published = 9 }, "Conservation"},
		{"released early", func(m *Model, _ *sub) { m.release[1] = 1 }, "ReleaseOnce"},
		{"engine totals negative", func(m *Model, _ *sub) { m.msgs = -1 }, "CapacityNonNegative"},
		{"engine totals over the limit", func(m *Model, _ *sub) { m.cfg.MaxTotalMessages = 0; m.msgs = 5; m.cfg.MaxTotalMessages = 1 }, "CapacityNonNegative"},
		{"lease without an active cell", func(_ *Model, s *sub) {
			s.recs[1].state = InFlight
			s.lanes[0].ready.reset()
			s.lanes[0].live = []uint64{1}
		}, "LeaseUnique"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, s := build()
			if err := m.Invariants(); err != nil {
				t.Fatalf("clean model: %v", err)
			}
			c.corrupt(m, s)
			err := m.Invariants()
			if err == nil {
				t.Fatal("corruption went unnoticed")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q does not name %s", err, c.want)
			}
		})
	}
}

// TestInFlightOrder checks the deadline ordering of the in-flight list, which
// is what makes expiry a head check.
func TestInFlightOrder(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 8, AckDeadline: 100 * time.Millisecond})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	for range 3 {
		step(t, m, Op{Kind: Publish, Size: 1})
	}
	step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 3, Observed: []uint64{1, 2, 3}})
	// Lengthen the first lease: it must move to the tail of the list.
	step(t, m, Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 1, Dur: 200 * time.Millisecond})
	_, s := m.find("s")
	if got := s.lanes[0].live; len(got) != 3 || got[0] != 2 || got[2] != 1 {
		t.Fatalf("in-flight order = %v", got)
	}
	// An extension shorter than what the lease has left changes nothing.
	extended := s.recs[1].deadline
	step(t, m, Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 1, Dur: time.Millisecond})
	if s.recs[1].deadline != extended {
		t.Fatalf("a short extension moved the deadline from %v to %v", extended, s.recs[1].deadline)
	}
	step(t, m, Op{Kind: Advance, Dt: 150 * time.Millisecond})
	if n := step(t, m, Op{Kind: ExpireLeases}).N; n != 0 {
		t.Fatalf("the clock move left %d expired leases behind", n)
	}
	if _, sub := m.find("s"); sub.st.expired != 2 {
		t.Fatalf("the leases the extension did not cover were not reclaimed: %d", sub.st.expired)
	}
	// A non-positive duration falls back to the acknowledgement deadline.
	step(t, m, Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 1})
	if s.recs[1].deadline != m.Now()+100*time.Millisecond {
		t.Fatalf("deadline = %v at %v", s.recs[1].deadline, m.Now())
	}
	// With no deadline at all a lease never expires.
	never := newModel(t, Config{CapacityMessages: 2, AckDeadline: -1})
	never.Step(Op{Kind: Subscribe, Sub: "s"})
	never.Step(Op{Kind: Attach, Sub: "s", Consumer: 1})
	never.Step(Op{Kind: Publish, Size: 1})
	never.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{1}})
	// Extending it gives it no deadline either.
	never.Step(Op{Kind: Extend, Sub: "s", Consumer: 1, Seq: 1, Gen: 1, Dur: time.Second})
	never.Step(Op{Kind: Advance, Dt: time.Hour})
	if n := never.Step(Op{Kind: ExpireLeases}).N; n != 0 {
		t.Fatalf("a lease without a deadline expired: %d", n)
	}
	if _, sub := never.find("s"); sub.st.expired != 0 {
		t.Fatalf("a lease without a deadline expired after an extension: %d", sub.st.expired)
	}
}

// TestSafeCopyRelease: in safe mode the publisher's payload comes back at
// once and the clones are engine-owned.
func TestSafeCopyRelease(t *testing.T) {
	m := newModel(t, Config{CapacityMessages: 4, SafeCopy: true})
	step(t, m, Op{Kind: Subscribe, Sub: "a"})
	step(t, m, Op{Kind: Subscribe, Sub: "b"})
	step(t, m, Op{Kind: Publish, Size: 1})
	if m.Released(1) != 1 {
		t.Fatalf("safe publication released %d times", m.Released(1))
	}
	step(t, m, Op{Kind: CloseEngine})
	if m.Released(1) != 1 {
		t.Fatalf("safe publication released %d times after close", m.Released(1))
	}
}

// TestPromotionTiesAreUnordered: records that come due at the same instant
// are promoted together and may be claimed in either order, while a record
// due later can never jump the group ahead of it.
func TestPromotionTiesAreUnordered(t *testing.T) {
	build := func(t *testing.T) *Model {
		t.Helper()
		m := newModel(t, Config{CapacityMessages: 8})
		step(t, m, Op{Kind: Subscribe, Sub: "s"})
		step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
		step(t, m, Op{Kind: Publish, Size: 1, Delay: 2 * time.Millisecond})
		step(t, m, Op{Kind: Publish, Size: 1, Delay: 2 * time.Millisecond})
		step(t, m, Op{Kind: Publish, Size: 1, Delay: 5 * time.Millisecond})
		step(t, m, Op{Kind: Advance, Dt: 5 * time.Millisecond})
		return m
	}
	m := build(t)
	if res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 3, Observed: []uint64{2, 1, 3}}); res.Err != nil {
		t.Fatalf("tied promotion in reverse order: %v", res.Err)
	}
	m = build(t)
	if res := m.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 3, Observed: []uint64{3, 1, 2}}); !errors.Is(res.Err, ErrMismatch) {
		t.Fatalf("a later record jumped its group: %v", res.Err)
	}
	// A record published after the clock moved is queued behind the ones the
	// scheduler promoted when they came due, not in front of them: Advance
	// does the scheduler's work at the instant each record was due.
	m = build(t)
	step(t, m, Op{Kind: Publish, Size: 1})
	if res := m.Step(Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 1, Observed: []uint64{4}}); !errors.Is(res.Err, ErrMismatch) {
		t.Fatalf("a record published after the promotion jumped the queue: %v", res.Err)
	}
	m = build(t)
	step(t, m, Op{Kind: Publish, Size: 1})
	if res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 2, Observed: []uint64{2, 1}}); res.Err != nil {
		t.Fatalf("the promoted group is not at the head: %v", res.Err)
	}
}

// TestRebalanceRevokesAndCloseCloses: the two ways a lease is withdrawn leave
// the two settlement answers the message contract gives. A lane a sticky
// reassignment takes from a consumer that is still open answers a late
// settlement with ErrLeaseLost ("a stale delivery generation", the specification's
// "rebalance generations invalidate stale deliveries"); the records of a
// consumer that closed answer ErrClosed ("settlement after Consumer.Close").
// The model answered ErrClosed for both, which the engine stopped doing in.
func TestRebalanceRevokesAndCloseCloses(t *testing.T) {
	m := newModel(t, Config{Partitions: 2, CapacityMessages: 4, Sticky: true})
	step(t, m, Op{Kind: Subscribe, Sub: "s"})
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 1})
	step(t, m, Op{Kind: Publish, Partition: 0, Size: 1})
	step(t, m, Op{Kind: Publish, Partition: 1, Size: 1})
	res := step(t, m, Op{Kind: Fetch, Sub: "s", Consumer: 1, Max: 2})
	if res.Err != nil || res.N != 2 {
		t.Fatalf("fetch: n=%d err=%v", res.N, res.Err)
	}
	byLane := map[int]Claim{}
	for _, c := range res.Claims {
		byLane[c.Partition] = c
	}
	// Consumer 2 joins: the table deals lane 1 to it, and consumer 1, still
	// open, loses the lease it held there.
	step(t, m, Op{Kind: Attach, Sub: "s", Consumer: 2})
	if _, owners := m.Assignment("s"); len(owners) != 2 || owners[0] != 1 || owners[1] != 2 {
		t.Fatalf("assignment after the join: %v", owners)
	}
	moved := byLane[1]
	for _, op := range []Op{
		{Kind: Extend, Sub: "s", Consumer: 1, Seq: moved.Seq, Gen: moved.Gen},
		{Kind: Ack, Sub: "s", Consumer: 1, Seq: moved.Seq, Gen: moved.Gen},
		{Kind: Nack, Sub: "s", Consumer: 1, Seq: moved.Seq, Gen: moved.Gen},
	} {
		got := step(t, m, op)
		if !errors.Is(got.Err, errcode.ErrLeaseLost) || got.Stale {
			t.Fatalf("%v of a delivery a rebalance moved from an open consumer: err=%v stale=%v, want %v",
				op.Kind, got.Err, got.Stale, errcode.ErrLeaseLost)
		}
	}
	// The lease consumer 1 kept is its own until it closes, and then the
	// settlement is told the consumer closed.
	kept := byLane[0]
	step(t, m, Op{Kind: CloseConsumer, Sub: "s", Consumer: 1})
	if got := step(t, m, Op{Kind: Ack, Sub: "s", Consumer: 1, Seq: kept.Seq, Gen: kept.Gen}); !errors.Is(got.Err, errcode.ErrClosed) {
		t.Fatalf("ack after the consumer closed: %v, want %v", got.Err, errcode.ErrClosed)
	}
}

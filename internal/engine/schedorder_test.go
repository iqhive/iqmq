package engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"bitbucket.org/iqhive/iqmq/internal/queue"
	"bitbucket.org/iqhive/iqmq/internal/timer"
)

// TestStrictOrderingHoldsOnEverySchedulerKind is the guard for the half of
// the message contract that could take away: a strict-ordering lane
// serves its delayed records in one order under every scheduler kind,
// including the three wheels whose timer.Kind.Ordered() is false.
//
// That order is due time, then publication sequence — the specification lists delayed
// delivery among the things that may alter order, and sortFired is where the
// engine says which order a delay produces. What matters to a binding is
// that it is the same order for all six kinds.
//
// The wheels round a due time up to their 1 ms tick, so a firing collapses
// records the exact kinds separate and hands them back in the order its
// bucket list happens to hold — but no wheel fires a record while one due
// earlier is still armed (internal/timer's TestFireNeverInvertsDueOrder),
// and every store sorts what it promotes. The delivery order is therefore
// the topology's, which is why KindSet.QueueOrdered does not fold the
// scheduler kind in.
//
// The cases are the four ways a wheel could differ from a heap: one tick
// holding records whose due times differ, one tick holding records whose due
// order is the reverse of their publication order, one tick holding records
// that are due at the very same instant (where only the publication sequence
// can decide), and a firing that spans more ticks than a wheel has slots.
//
// Watched failing at CreateTopic ("iqmq/engine: invalid configuration") for
// hwheel, hashedwheel and hybrid while QueueOrdered folded in the scheduler
// kind, and — with that term gone and sortFired's body removed — on the
// three wheels in every case, e.g.
//
//	same-instant/hashedwheel: served [7 6 5 4 3 2 1 0], want [0 1 2 3 4 5 6 7]
func TestStrictOrderingHoldsOnEverySchedulerKind(t *testing.T) {
	const n = 8
	for _, tc := range []struct {
		name string
		// delay returns the delay of the i-th publication.
		delay func(i int) time.Duration
		// advance is how far the clock moves before the fetch.
		advance time.Duration
	}{
		{
			// Eight distinct due times inside one 1 ms tick: every wheel
			// rounds all eight to the same tick and fires them together.
			name:    "within-one-tick",
			delay:   func(i int) time.Duration { return time.Duration(i+1) * 100 * time.Microsecond },
			advance: 2 * time.Millisecond,
		},
		{
			// Due times that fall as the publication sequence rises, all
			// inside one tick: the due order is the reverse of the
			// publication order, so a lane that served either its buckets'
			// order or the publication order would be wrong.
			name:    "due-order-reverses-publication-order",
			delay:   func(i int) time.Duration { return time.Duration(n-i) * 100 * time.Microsecond },
			advance: 2 * time.Millisecond,
		},
		{
			// One due time for all eight: nothing but the publication
			// sequence can order them, and a wheel's bucket list is a stack.
			name:    "same-instant",
			delay:   func(int) time.Duration { return 400 * time.Microsecond },
			advance: 2 * time.Millisecond,
		},
		{
			// One second between records is 1,000 ticks, so the fetch's
			// firing spans more slots than the hybrid holds (1,024) and, at
			// eight records, most of the hashed wheel's lap (4,096), which is
			// where a wheel visits a slot from a later lap before one from
			// this lap.
			name:    "across-more-ticks-than-a-wheel-has-slots",
			delay:   func(i int) time.Duration { return time.Duration(i+1) * time.Second },
			advance: 2 * n * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The order the engine defines: due time, then publication
			// sequence. It is computed from the case and not from a kind, so
			// no kind is the yardstick for the others.
			want := make([]int64, n)
			for i := range want {
				want[i] = int64(i)
			}
			slices.SortStableFunc(want, func(a, b int64) int {
				return int(tc.delay(int(a)) - tc.delay(int(b)))
			})
			for _, tk := range timer.Kinds() {
				t.Run(tk.String(), func(t *testing.T) {
					// The ready queue is pinned, not left at the specification's
					// binding: ShardedMPMC is a full member of that family
					// and queue.Kind.Ordered() is false for it, so a campaign
					// that bound it would fail all twenty-four arms of this
					// guard at the QueueOrdered check below -- for a reason
					// that has nothing to do with the scheduler kind the arm
					// names. The subject is (adr/ADR-017-assumptions.md).
					ks := KindSet{Arch: featureArch, Queue: queue.MutexRing, Timer: tk}
					if !ks.QueueOrdered() {
						t.Fatalf("KindSet{Timer: %v}.QueueOrdered() = false: the message contract's"+
							" strict ordering is not available under this scheduler kind", tk)
					}
					e, fake := fakeEngine(t, ks, TopologyConfig{
						Partitions: 1, CapacityMessages: 2 * n, Ordering: OrderingStrict,
					})
					top, err := e.CreateTopic("t", TopologyConfig{Ordering: OrderingStrict})
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
					for i := range n {
						if err := top.Publish(context.Background(), Publish[int64]{
							Payload: int64(i), Size: 1, Delay: tc.delay(i),
						}); err != nil {
							t.Fatalf("publish %d: %v", i, err)
						}
					}
					fake.Advance(tc.advance)
					dst := make([]Claim[int64], n)
					got := make([]int64, 0, n)
					for len(got) < n {
						m, err := c.Fetch(poll(t), dst)
						if m == 0 {
							t.Fatalf("served %v after %d of %d records: %v", got, len(got), n, err)
						}
						for i := range m {
							got = append(got, dst[i].Payload)
							if err := c.Ack(dst[i].Env, dst[i].Gen); err != nil {
								t.Fatalf("ack: %v", err)
							}
						}
					}
					if !slices.Equal(got, want) {
						t.Fatalf("%s/%v: served %v, want %v: a strict-ordering lane's delivery"+
							" order may not depend on the scheduler kind "+
							" ", tc.name, tk, got, want)
					}
				})
			}
		})
	}
}

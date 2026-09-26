package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/clock"
	"github.com/iqhive/iqmq/internal/errcode"
)

// TestConsumerCloseRacingARebalanceClosesItsLeases is the message contract's
// "settlement after Consumer.Close returns ErrClosed" for a close that a
// rebalance overtakes.
//
// Consumer.Close stores a table without the consumer (Subscription.leave) and
// releases the membership mutex before its own revoke walks the lanes. A join
// or a Rebalance that lands in that window walks them first, finds the closing
// consumer's leases on lanes it no longer owns, and used to withdraw them as a
// reassignment -- Revoke, whose settlement is told ErrLeaseLost -- because its
// gone was 0. No order of the two operations gives that answer: with the
// rebalance first, the consumer still owns what it keeps and its close closes
// it; with the close first, everything it held is closed. A review race of
// close against join reproduced it 3 times in 20,000 rounds.
//
// The reconnect fault's revokeAll is the third overtaker, on a subscription
// without a sticky assignment, where it is the only walk that withdraws
// anything: every lease it finds belongs to an open consumer and takes Revoke
// , except that of a consumer the table no longer names,
// which is a close in progress.
//
// The window is forced rather than raced: the revoke that Subscription.leave
// runs reads the engine clock before it takes its first lane, and a clockTrap
// parks that one read while the test joins a second consumer, rebalances or
// reconnects, and only then lets the close finish. Every round lands in the
// window.
func TestConsumerCloseRacingARebalanceClosesItsLeases(t *testing.T) {
	for _, overtake := range []struct {
		name   string
		sticky bool
		run    func(t *testing.T, eng *Engine[int64], sub *Subscription[int64])
	}{
		{"join", true, func(t *testing.T, _ *Engine[int64], sub *Subscription[int64]) {
			if _, err := sub.Attach(ConsumerConfig{}); err != nil {
				t.Fatalf("Attach: %v", err)
			}
		}},
		{"rebalance", true, func(_ *testing.T, _ *Engine[int64], sub *Subscription[int64]) { sub.Rebalance() }},
		{"reconnect", false, func(t *testing.T, eng *Engine[int64], _ *Subscription[int64]) {
			if n := eng.Faults().Reconnect(); n != 1 {
				t.Fatalf("Reconnect reached %d subscriptions, want 1", n)
			}
		}},
	} {
		t.Run(overtake.name, func(t *testing.T) {
			for _, arch := range Arches() {
				for round := range 20 {
					closeRacingRebalance(t, arch, round, overtake.sticky, overtake.run)
				}
			}
		})
	}
}

func closeRacingRebalance(t *testing.T, arch Arch, round int, sticky bool,
	overtake func(*testing.T, *Engine[int64], *Subscription[int64]),
) {
	t.Helper()
	trap := newClockTrap(clock.NewFake(time.Unix(0, 0)), "(*Subscription[", ").leave")
	eng, err := New(Config[int64]{
		Kinds: KindSet{Arch: arch},
		Clock: trap,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: 2, CapacityMessages: 8, AckDeadline: time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Sticky: sticky})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	c1, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	for p := range 2 {
		if err := top.Publish(context.Background(), Publish[int64]{Payload: int64(p), Size: 1, Partition: p}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	dst := make([]Claim[int64], 2)
	if n, err := c1.Fetch(context.Background(), dst); n != 2 || err != nil {
		t.Fatalf("%v round %d: fetch n=%d err=%v, want one record from each lane", arch, round, n, err)
	}

	trap.arm()
	var wg sync.WaitGroup
	wg.Go(func() { _ = c1.Close() })
	select {
	case <-trap.parked:
	case <-time.After(10 * time.Second):
		t.Fatalf("%v round %d: Consumer.Close never read the clock inside Subscription.leave; the trap "+
			"no longer reaches the window it is meant to hold", arch, round)
	}
	// The close has stored its table and let the membership mutex go; its
	// own revoke has not walked a lane yet.
	overtake(t, eng, sub)
	trap.let()
	wg.Wait()

	for i, cl := range dst {
		if err := c1.Ack(cl.Env, cl.Gen); !errors.Is(err, errcode.ErrClosed) {
			t.Fatalf("%v round %d: Ack of claim %d (lane %d) after Consumer.Close returned: %v, want %v "+
				"; a rebalance that overtook the close withdrew it as a reassignment",
				arch, round, i, cl.Env.lane.idx, err, errcode.ErrClosed)
		}
	}
	if st := sub.Inspect(); st.Ready != 2 || st.InFlight != 0 {
		t.Fatalf("%v round %d: after the close the lanes hold ready=%d in-flight=%d, want both records back",
			arch, round, st.Ready, st.InFlight)
	}
}

// TestRevocationReadsTheTableUnderTheLaneMutex is the other side of the rule
// above: a holder the table does not name is a closing consumer only if the
// table is at least as new as the holder's join. A revocation that read the
// table before taking the lane mutex could find a consumer that joined and
// claimed in between, missing from that table, and closed its lease -- an
// open consumer told ErrClosed, the defect class, reachable through
// the public reconnect fault. A review found it 308 times in 214,529 unforced
// rounds on the first version of.
//
// The revocation is parked at the clock read it makes before taking a lane's
// mutex; a new consumer then joins and claims that lane's record; then the
// revocation goes on. It must see the new consumer as a member:
//   - reconnect, no sticky assignment: its lease is revoked, and its Ack is
//     told ErrLeaseLost;
//   - Rebalance, sticky: the table it reads gives the lane to the new
//     consumer, whose Ack succeeds.
func TestRevocationReadsTheTableUnderTheLaneMutex(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sticky bool
		frame  string
		run    func(eng *Engine[int64], sub *Subscription[int64])
		want   error
	}{
		{"reconnect", false, ").revokeAll", func(eng *Engine[int64], _ *Subscription[int64]) { eng.Faults().Reconnect() }, errcode.ErrLeaseLost},
		{"rebalance", true, ").Rebalance", func(_ *Engine[int64], sub *Subscription[int64]) { sub.Rebalance() }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arch := range Arches() {
				for round := range 20 {
					revocationRacingAJoin(t, arch, round, tc.sticky, tc.frame, tc.run, tc.want)
				}
			}
		})
	}
}

func revocationRacingAJoin(t *testing.T, arch Arch, round int, sticky bool, frame string,
	revoke func(*Engine[int64], *Subscription[int64]), want error,
) {
	t.Helper()
	trap := newClockTrap(clock.NewFake(time.Unix(0, 0)), "(*Subscription[", frame)
	eng, err := New(Config[int64]{
		Kinds: KindSet{Arch: arch},
		Clock: trap,
		Clone: func(v int64) int64 { return v },
		DefaultTopology: TopologyConfig{
			Partitions: 1, CapacityMessages: 8, AckDeadline: time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = eng.Close() }()
	top, err := eng.CreateTopic("t", TopologyConfig{})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := top.Subscribe("s", SubscriptionConfig{Sticky: sticky})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := top.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	trap.arm()
	var wg sync.WaitGroup
	wg.Go(func() { revoke(eng, sub) })
	select {
	case <-trap.parked:
	case <-time.After(10 * time.Second):
		t.Fatalf("%v round %d: the revocation never read the clock inside %s; the trap no longer "+
			"reaches the window it is meant to hold", arch, round, frame)
	}
	// The revocation has stored whatever table it was going to and is about
	// to take the lane's mutex. A consumer joins -- a newer table -- and
	// claims the lane's record before it does.
	c, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	dst := make([]Claim[int64], 1)
	if n, err := c.Fetch(context.Background(), dst); n != 1 || err != nil {
		t.Fatalf("%v round %d: the new consumer's fetch n=%d err=%v, want the record", arch, round, n, err)
	}
	trap.let()
	wg.Wait()

	if err := c.Ack(dst[0].Env, dst[0].Gen); !errors.Is(err, want) {
		t.Fatalf("%v round %d: the Ack of an open consumer that joined and claimed while a revocation "+
			"was on its way to the lane: %v, want %v; ErrClosed means the revocation judged it by a "+
			"table older than its join", arch, round, err, want)
	}
}

package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iqhive/iqmq/internal/errcode"
	"github.com/iqhive/iqmq/internal/wait"
)

// preparedWaits counts the tickets a subscription's fetches have registered,
// so a test can tell which of two fetches parked first: the wake-one kinds
// queue their waiters in Prepare order, and the order is the whole defect.
type preparedWaits struct {
	wait.Strategy
	prepared atomic.Int64
}

func (p *preparedWaits) Prepare() wait.Ticket {
	t := p.Strategy.Prepare()
	p.prepared.Add(1)
	return t
}

// waitPrepared blocks until n tickets have been registered.
func waitPrepared(t *testing.T, p *preparedWaits, n int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for p.prepared.Load() < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d fetches parked within 15s", p.prepared.Load(), n)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// TestAPublicationWakesTheStickyOwnerOfItsLane pins the wake a sticky
// subscription used to spend on the wrong consumer.
//
// The publish path announced a ready record with one Notify, and the default
// wait kind gives that to the oldest parked fetch. Under a sticky assignment
// the oldest may own a different lane: it took nothing and parked again, and
// the owner of the record's lane stayed parked beside it until its context
// ended, because a ready record with no lease and no due time is never ticked.
//
// Two lanes and two consumers deal one lane to each. The consumer that does
// not own the record's lane parks first, so it is the head of the waiter list
// when the record arrives.
func TestAPublicationWakesTheStickyOwnerOfItsLane(t *testing.T) {
	top := TopologyConfig{Partitions: 2, AckDeadline: -1}
	e := newTestEngine(t, KindSet{Arch: archFor(top), Wait: wait.Chan}, top)
	tp, err := e.CreateTopic("t", top)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := tp.Subscribe("s", SubscriptionConfig{Sticky: true})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	pw := &preparedWaits{Strategy: sub.waiters}
	sub.waiters = pw
	first, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach(first): %v", err)
	}
	second, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach(second): %v", err)
	}
	_, owners := sub.Assignment()
	lane := -1
	for i, id := range owners {
		if id == second.id {
			lane = i
		}
	}
	if lane < 0 {
		t.Fatalf("the second consumer owns no lane (owners=%v)", owners)
	}

	fetch := func(ctx context.Context, c *Consumer[int64]) <-chan error {
		got := make(chan error, 1)
		go func() {
			dst := make([]Claim[int64], 1)
			n, err := c.Fetch(ctx, dst)
			switch {
			case err != nil:
				got <- err
			case n != 1:
				got <- errcode.ErrPartial
			default:
				got <- c.Ack(dst[0].Env, dst[0].Gen)
			}
		}()
		return got
	}
	firstCtx, stopFirst := context.WithCancel(context.Background())
	defer stopFirst()
	firstGot := fetch(firstCtx, first)
	waitPrepared(t, pw, 1)
	secondCtx, stopSecond := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopSecond()
	secondGot := fetch(secondCtx, second)
	waitPrepared(t, pw, 2)

	if err := tp.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 8, Partition: lane}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := <-secondGot; err != nil {
		t.Fatalf("the owner of lane %d was not woken by the record published to it: %v\n"+
			"    a sticky subscription's wake has to reach the consumer that may claim the record, "+
			"not whichever fetch parked first", lane, err)
	}
	stopFirst()
	if err := <-firstGot; !errors.Is(err, context.Canceled) {
		t.Fatalf("the first consumer's fetch returned %v, want context.Canceled: it claimed from a lane it does not own", err)
	}
}

// claimHook runs hook once, inside the first claim of its store that takes a
// record: the window in which a fetch holds a record it has taken out of its
// lane and has not leased yet.
type claimHook[P any] struct {
	partitionStore[P]
	once sync.Once
	hook func()
}

func (h *claimHook[P]) Claim(now int64, dst []*Envelope[P]) int {
	n := h.partitionStore.Claim(now, dst)
	if n > 0 {
		h.once.Do(h.hook)
	}
	return n
}

// TestALeaseARebalanceRefusesWakesTheNewOwner pins the wake a sticky claim
// used to swallow.
//
// A fetch takes a record out of its lane before it leases it, and re-checks
// under the lane mutex that the lane is still its own. A rebalance landing in
// between deals the lane to another consumer, and the claim puts the record
// back. It did so without a wake: the new owner, woken by the rebalance, had
// looked at the lane while the record was out of it, found nothing and parked
// again, and stayed parked beside a ready record that no lease or due time
// would ever tick.
//
// The first consumer owns both lanes until the second joins, and the join is
// made from inside the first one's claim of lane 1.
func TestALeaseARebalanceRefusesWakesTheNewOwner(t *testing.T) {
	const lane = 1
	top := TopologyConfig{Partitions: 2, AckDeadline: -1}
	e := newTestEngine(t, KindSet{Arch: archFor(top), Wait: wait.Chan}, top)
	tp, err := e.CreateTopic("t", top)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	sub, err := tp.Subscribe("s", SubscriptionConfig{Sticky: true})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	pw := &preparedWaits{Strategy: sub.waiters}
	sub.waiters = pw
	first, err := sub.Attach(ConsumerConfig{})
	if err != nil {
		t.Fatalf("Attach(first): %v", err)
	}

	fetch := func(ctx context.Context, c *Consumer[int64], got chan<- error) {
		dst := make([]Claim[int64], 1)
		n, err := c.Fetch(ctx, dst)
		switch {
		case err != nil:
			got <- err
		case n != 1:
			got <- errcode.ErrPartial
		default:
			got <- c.Ack(dst[0].Env, dst[0].Gen)
		}
	}
	secondCtx, stopSecond := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopSecond()
	secondGot := make(chan error, 1)
	ln := sub.lanes[lane]
	ln.store = &claimHook[int64]{partitionStore: ln.store, hook: func() {
		second, err := sub.Attach(ConsumerConfig{})
		if err != nil {
			secondGot <- err
			return
		}
		if _, owners := sub.Assignment(); owners[lane] != second.id {
			secondGot <- errors.New("the join did not deal lane 1 to the second consumer")
			return
		}
		go fetch(secondCtx, second, secondGot)
		// The second consumer's fetch has found the lane empty (the record
		// is in this claim's hands) and parked.
		deadline := time.Now().Add(15 * time.Second)
		for pw.prepared.Load() < 1 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Microsecond)
		}
	}}

	if err := tp.Publish(context.Background(), Publish[int64]{Payload: 1, Size: 8, Partition: lane}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	firstCtx, stopFirst := context.WithCancel(context.Background())
	defer stopFirst()
	firstGot := make(chan error, 1)
	go fetch(firstCtx, first, firstGot)
	if err := <-secondGot; err != nil {
		t.Fatalf("the new owner of lane %d did not receive the record its rebalance moved: %v (%+v)\n"+
			"    the claim that lost the lane put the record back without waking the consumer parked on it",
			lane, err, sub.Inspect())
	}
	stopFirst()
	if err := <-firstGot; !errors.Is(err, context.Canceled) {
		t.Fatalf("the first consumer's fetch returned %v, want context.Canceled: it kept a record from a lane it lost", err)
	}
}

package kafkago

import (
	"errors"
	"sync"
	"testing"
	"time"

	kgo "github.com/segmentio/kafka-go"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// stopFixture is a source owning partition 0, with offset 10 handed out and
// in flight.
func stopFixture(t *testing.T) (*source, *partState, *mq.DriverDelivery) {
	t.Helper()
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	p := &partState{partition: 0, tr: tr, inflight: 1, committed: 10, firstAt: map[int64]time.Time{}}
	b := &Backend{errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	s := &source{
		b: b, topic: "t", recs: newRecQueue(4), wake: make(chan struct{}, 1),
		parts: map[int]*partState{0: p}, order: []*partState{p}, gens: 1,
	}
	s.cond = sync.NewCond(&s.mu)
	s.released = make(chan struct{}, 1)
	d := &mq.DriverDelivery{
		Token: 0, Gen: tr.Gen(), Native: &kgo.Message{Topic: "t", Offset: 10},
		Info: mq.DeliveryInfo{Source: "t", Offset: 10, Attempt: 1},
	}
	return s, p, d
}

// TestAStopNackWakesTheWaitingPoll: a stop-the-consumer nack has to end the
// wait a Fetch is in, as every other settlement that changes what Fetch would
// answer does. It did not, so a Fetch waiting on a partition with nothing more
// to read kept waiting after the stop, and Run did not return until the next
// record arrived, which it then handed out.
func TestAStopNackWakesTheWaitingPoll(t *testing.T) {
	s, p, d := stopFixture(t)
	if err := s.apply(t.Context(), p, d, nack.Plan{Op: nack.OpStop}); err != nil {
		t.Fatalf("the stop = %v, want nil", err)
	}
	select {
	case <-s.wake:
	default:
		t.Fatal("the stop left the waiting poll waiting")
	}
	if err := s.state(t.Context()); !errors.Is(err, errStopped) {
		t.Fatalf("state after the stop = %v, want errStopped", err)
	}
}

// TestAStoppedSourceHandsNothingOut: once a stop-the-consumer nack has landed,
// nothing more leaves the source, whether it was already holding it (a due
// redelivery, a backlog) or a wait that began before the stop brings it. Fetch
// checks the stop only before it looks, so a wait that was already under way
// handed out the record that ended it.
func TestAStoppedSourceHandsNothingOut(t *testing.T) {
	s, p, d := stopFixture(t)
	if err := s.apply(t.Context(), p, d, nack.Plan{Op: nack.OpStop}); err != nil {
		t.Fatalf("the stop = %v, want nil", err)
	}

	late := &kgo.Message{Topic: "t", Partition: 0, Offset: 11, Value: []byte("late")}
	dst := make([]mq.DriverDelivery, 4)
	if n, err := s.absorb(fetched{msg: late, gen: s.gens}, dst, 0); n != 0 || err != nil {
		t.Fatalf("a wait that ended after the stop handed out %d records (%v), want none", n, err)
	}
	if p.inflight != 0 {
		t.Fatalf("the stopped source counts %d deliveries in flight, want 0", p.inflight)
	}

	s.mu.Lock()
	p.held = append(p.held, heldRecord{msg: &kgo.Message{Topic: "t", Offset: 10}, attempt: 2, due: time.Now().Add(-time.Second)})
	p.backlog = append(p.backlog, late)
	s.mu.Unlock()
	if n := s.takeLocal(dst); n != 0 {
		t.Fatalf("the stopped source handed out %d of the records it was holding (offset %d first), want none", n, dst[0].Info.Offset)
	}
}

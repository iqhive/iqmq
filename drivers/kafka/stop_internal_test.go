package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	"github.com/iqhive/iqmq/driverkit/nack"
	"github.com/iqhive/iqmq/driverkit/offset"
)

// stopFixture is a source owning one partition, with offset 10 handed out and
// in flight, a later record of the partition kept back in its backlog and a
// nacked one held for a redelivery that is already due.
func stopFixture(t *testing.T) (*source, *partState, *mq.DriverDelivery) {
	t.Helper()
	tr, err := offset.New(offset.MapScan, offset.Config{Base: 10, Window: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Track(tr.Gen(), 10); err != nil {
		t.Fatal(err)
	}
	key := tp{"t", 0}
	p := &partState{tp: key, tr: tr, inflight: 1, committed: 10, firstAt: map[int64]time.Time{}}
	b := &Backend{errs: newErrorMapper()}
	b.health = driverkit.NewHealth(b.onHealth)
	s := &source{
		b: b, topic: "t", cfg: mq.SourceConfig{Prefetch: 8},
		parts: map[tp]*partState{key: p}, order: []*partState{p},
	}
	d := &mq.DriverDelivery{
		Token: 0, Gen: tr.Gen(), Native: &kgo.Record{Topic: "t", Offset: 10},
		Info: mq.DeliveryInfo{Source: "t", Offset: 10, Attempt: 1},
	}
	return s, p, d
}

// TestAStopNackWakesTheWaitingPoll: a stop-the-consumer nack has to end the
// poll a Fetch is waiting in, as every other settlement that changes what
// Fetch would answer does. It did not, so a Fetch parked on a partition with
// nothing more to read kept waiting after the stop, and Run did not return
// until the next record arrived, which it then handed out.
func TestAStopNackWakesTheWaitingPoll(t *testing.T) {
	s, p, d := stopFixture(t)
	poll, cancel := context.WithCancel(t.Context())
	defer cancel()
	if !s.armWake(cancel) {
		t.Fatal("the fixture's poll could not be armed")
	}
	if err := s.apply(t.Context(), p, d, nack.Plan{Op: nack.OpStop}); err != nil {
		t.Fatalf("the stop = %v, want nil", err)
	}
	if poll.Err() == nil {
		t.Fatal("the stop left the waiting poll waiting")
	}
	if err := s.state(t.Context()); !errors.Is(err, errStopped) {
		t.Fatalf("state after the stop = %v, want errStopped", err)
	}
}

// TestAStoppedSourceHandsNothingOut: once a stop-the-consumer nack has landed,
// nothing more leaves the source, whether it was already holding it (a due
// redelivery, a backlog) or a poll that began before the stop brings it.
// Fetch checks the stop only before it looks, so a poll that was already
// waiting handed out the records that ended it.
func TestAStoppedSourceHandsNothingOut(t *testing.T) {
	s, p, d := stopFixture(t)
	if err := s.apply(t.Context(), p, d, nack.Plan{Op: nack.OpStop}); err != nil {
		t.Fatalf("the stop = %v, want nil", err)
	}

	late := &kgo.Record{Topic: "t", Partition: 0, Offset: 11, Value: []byte("late")}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      "t",
		Partitions: []kgo.FetchPartition{{Partition: 0, Records: []*kgo.Record{late}}},
	}}}}
	dst := make([]mq.DriverDelivery, 4)
	if n, err := s.absorb(fetches, dst, false, 0); n != 0 || err != nil {
		t.Fatalf("a poll that ended after the stop handed out %d records (%v), want none", n, err)
	}
	if p.inflight != 0 {
		t.Fatalf("the stopped source counts %d deliveries in flight, want 0", p.inflight)
	}

	s.mu.Lock()
	p.held = append(p.held, heldRecord{rec: &kgo.Record{Topic: "t", Offset: 10}, attempt: 2, due: time.Now().Add(-time.Second)})
	p.backlog = append(p.backlog, late)
	s.mu.Unlock()
	if n := s.takeLocal(dst); n != 0 {
		t.Fatalf("the stopped source handed out %d of the records it was holding (offset %d first), want none", n, dst[0].Info.Offset)
	}
}

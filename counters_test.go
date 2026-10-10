package iqmq_test

import (
	"context"
	"errors"
	"testing"

	mq "github.com/iqhive/iqmq"
)

// TestCountersReturnToZero is the defect class the contributor notes names —
// "counters that do not return to zero" — over a workload that reaches every
// settlement outcome: acked, nacked, dropped, dead-lettered, refused,
// ambiguous and lease-lost.
func TestCountersReturnToZero(t *testing.T) {
	cl, b := openFake(t, fakeConfig{deadLetter: true})
	c := consume(t, cl, mq.ConsumerConfig{Prefetch: 8})
	ctx := context.Background()
	publishN(t, cl, 6)
	dst := make([]mq.Delivery, 6)
	n, err := c.Receive(ctx, dst)
	if err != nil || n != 6 {
		t.Fatalf("Receive = (%d, %v)", n, err)
	}
	if got := c.Stats().InFlight; got != 6 {
		t.Fatalf("InFlight = %d, want 6", got)
	}
	if err := dst[0].Ack(); err != nil {
		t.Fatal(err)
	}
	if err := dst[1].Nack(); err != nil {
		t.Fatal(err)
	}
	if err := dst[2].Nack(mq.NackDrop()); err != nil {
		t.Fatal(err)
	}
	if err := dst[3].Reject(); err != nil {
		t.Fatal(err)
	}

	b.setAmbiguous(dst[4].Info().Sequence, true)
	if err := dst[4].Ack(); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("the ambiguous Ack = %v", err)
	}
	if got := c.Stats().InFlight; got != 2 {
		t.Fatalf("InFlight = %d after four terminal settlements, want 2", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	st := c.Stats()
	if st.InFlight != 0 {
		t.Fatalf("InFlight = %d after close, want 0", st.InFlight)
	}
	if st.Received != 6 || st.Acked != 1 || st.Nacked != 3 || st.DeadLettered != 1 {
		t.Fatalf("Stats = %+v", st)
	}
	if got := b.counts("orders", "billing"); !got.conserved() {
		t.Fatalf("the broker holds %+v: conservation is broken", got)
	}
}

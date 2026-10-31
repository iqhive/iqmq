package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestFlushWaitsOnlyForItsOwnPublications: a publisher's Flush was
// kgo.Client.Flush, which waits until the CLIENT has no buffered record, and
// the client is shared by every publisher of the backend and by the nack
// moves. So a publisher with nothing left to flush waited for another
// publisher's record on another topic, and when its context ended it answered
// ErrSettlementUnknown and counted an ambiguous publication that does not
// exist. Under steady traffic from anyone else its Flush, and so its
// Shutdown, could never succeed.
func TestFlushWaitsOnlyForItsOwnPublications(t *testing.T) {
	ta, tb := newTopic("flush-own-a"), newTopic("flush-own-b")
	cl := cluster(t, 1, []string{ta, tb})
	c, b := openPair(t, cl)
	pa, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: ta, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher A: %v", err)
	}
	pb, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: tb})
	if err != nil {
		t.Fatalf("Publisher B: %v", err)
	}
	if err := pa.Publish(t.Context(), &mq.Message{Body: []byte("a1")}); err != nil {
		t.Fatalf("A's Publish: %v", err)
	}
	if err := pa.Flush(t.Context()); err != nil {
		t.Fatalf("A's first Flush: %v", err)
	}
	if hw := cl.PartitionInfo(ta, 0).HighWatermark; hw != 1 {
		t.Fatalf("A's record is not in the log (high watermark %d)", hw)
	}
	// B, on another topic, has a publication waiting at the broker.
	held, release := holdFirstProduce(t, cl)
	berr := make(chan error, 1)
	go func() { berr <- pb.Publish(context.Background(), &mq.Message{Body: []byte("b1")}) }()
	<-held
	amb := b.AmbiguousPublications()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	ferr := pa.Flush(ctx)
	cancel()
	release()
	if err := <-berr; err != nil {
		t.Fatalf("B's Publish: %v", err)
	}
	if ferr != nil {
		t.Errorf("A's Flush, with every record it admitted in the log, = %v (code %v): it waited for publisher B's record on another topic", ferr, mq.CodeOf(ferr))
	}
	if n := b.AmbiguousPublications() - amb; n != 0 {
		t.Errorf("A's Flush counted %d ambiguous publications, want 0", n)
	}
}

// TestFlushWaitsForItsOwnPublication is the other half: a Flush still waits
// for a record its own publisher admitted, and a Flush whose context ends
// first reports that record's outcome as unknown.
func TestFlushWaitsForItsOwnPublication(t *testing.T) {
	topic := newTopic("flush-held")
	cl := cluster(t, 1, []string{topic})
	c, _ := openPair(t, cl)
	p, err := c.Publisher(t.Context(), mq.PublisherConfig{Topic: topic, Confirmation: mq.ConfirmNone})
	if err != nil {
		t.Fatalf("Publisher: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("warm")})
	held, release := holdFirstProduce(t, cl)
	if err := p.Publish(t.Context(), &mq.Message{Body: []byte("held")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	<-held
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	err = p.Flush(ctx)
	cancel()
	if !errors.Is(err, mq.ErrSettlementUnknown) || !mq.IsAmbiguous(err) {
		t.Errorf("a Flush whose context ended before its publisher's record was answered = %v, want ErrSettlementUnknown", err)
	}
	done := make(chan error, 1)
	go func() { done <- p.Flush(t.Context()) }()
	select {
	case err := <-done:
		t.Fatalf("Flush returned %v while its publisher's record was still at the broker", err)
	case <-time.After(200 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Errorf("Flush once the record was answered = %v, want nil", err)
	}
	if hw := cl.PartitionInfo(topic, 0).HighWatermark; hw != 2 {
		t.Errorf("high watermark %d, want 2 (warm and held)", hw)
	}
}

package jetstream_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestSmokeRoundTrip is the shortest proof that the whole pipeline works: an
// embedded JetStream server, a stream, a client, a durable consumer, a
// publication with a PubAck and a delivery that is acknowledged.
func TestSmokeRoundTrip(t *testing.T) {
	c := openClient(t)
	topic := newTopic("smoke")
	cons := consumerOn(t, c, topic, "workers")
	publish(t, c, topic, &mq.Message{Body: []byte("hello")})
	d := receiveOne(t, cons, 10*time.Second)
	if string(d.Body) != "hello" {
		t.Fatalf("body = %q, want hello", d.Body)
	}
	if d.Topic != topic {
		t.Errorf("delivery topic = %q, want %q", d.Topic, topic)
	}
	if got := d.Info().Attempt; got != 1 {
		t.Errorf("Attempt = %d, want 1: the specification counts attempts from 1 and JetStream's NumDelivered does too", got)
	}
	if d.Info().Redelivered {
		t.Error("a first delivery is reported as a redelivery")
	}
	if !d.Info().HasSequence || d.Info().Sequence == 0 {
		t.Errorf("Sequence = %d (has %v), want the stream sequence", d.Info().Sequence, d.Info().HasSequence)
	}
	if lease := d.Info().LeaseExpires; lease.IsZero() {
		t.Error("LeaseExpires is the zero time: the specification makes that \"the broker grants no lease\", and this one does")
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !d.Settled() {
		t.Error("Settled reported false after a successful Ack")
	}
}

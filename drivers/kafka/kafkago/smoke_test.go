package kafkago_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestRoundTrip is the first thing that has to work: a publication reaches a
// consumer, byte for byte, and its acknowledgement stops it coming back.
func TestRoundTrip(t *testing.T) {
	c, topic := openClient(t)
	cons := consumerOn(t, c, topic, "g1")
	publish(t, c, topic, &mq.Message{
		Key:  []byte("k"),
		Body: []byte("hello"),
	})
	d := receiveOne(t, cons, 15*time.Second)
	if string(d.Body) != "hello" || string(d.Key) != "k" {
		t.Fatalf("delivery key %q body %q", d.Key, d.Body)
	}
	if d.Topic != topic {
		t.Fatalf("delivery topic %q, want %q", d.Topic, topic)
	}
	if !d.Info().HasOffset || !d.Info().HasPartition {
		t.Fatalf("delivery info %+v: Kafka always says both", d.Info())
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	expectNothing(t, cons, 500*time.Millisecond, "an acknowledged record")
}

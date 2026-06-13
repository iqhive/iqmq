package nats_test

import (
	"testing"
	"time"

	mq "bitbucket.org/iqhive/iqmq"
)

// TestSmokeRoundTrip is the shortest proof that the whole pipeline works:
// an embedded server, a client, a subscription, a publication and a delivery.
func TestSmokeRoundTrip(t *testing.T) {
	c := openClient(t)
	topic := newTopic("smoke")
	cons := consumerOn(t, c, topic, "workers")
	if err := c.Publish(t.Context(), topic, &mq.Message{Body: []byte("hello")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	d := receiveOne(t, cons, 5*time.Second)
	if string(d.Body) != "hello" {
		t.Fatalf("body = %q, want hello", d.Body)
	}
	if d.Topic != topic {
		t.Errorf("delivery topic = %q, want %q", d.Topic, topic)
	}
}

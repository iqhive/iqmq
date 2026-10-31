package kafka_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestClosingAConsumerLeavesTheClientReady: every source's group client
// carries the backend's hooks, and closing it disconnects each of its
// connections while the backend stays open. The hook read every disconnect as
// a lost link and set the backend Degraded ("a broker connection was lost; the
// client is reconnecting"), and only a new connection set it back, so closing
// a consumer, an ordinary operation, left a healthy client reporting Degraded.
func TestClosingAConsumerLeavesTheClientReady(t *testing.T) {
	topic := newTopic("close-health")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	h, err := c.Consumer(t.Context(), mq.ConsumerConfig{Topic: topic, Subscription: "g-close-health"})
	if err != nil {
		t.Fatalf("Consumer: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("one")})
	d := receiveOne(t, h, 10*time.Second)
	if err := d.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if s := b.Health(); s.State != mq.HealthReady {
		t.Fatalf("Health before the close = %v (%v), want Ready", s.State, s.Err)
	}
	degraded, reconnects := b.DegradedTransitions(), b.Reconnects()
	if err := h.Close(); err != nil {
		t.Fatalf("closing the consumer: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	publish(t, c, topic, &mq.Message{Body: []byte("two")})
	if s := b.Health(); s.State != mq.HealthReady {
		t.Errorf("Health after a consumer's Close = %v (%v), and the producer publishes: the group client's own disconnects were read as a lost link", s.State, s.Err)
	}
	if n := b.DegradedTransitions() - degraded; n != 0 {
		t.Errorf("a consumer's Close counted %d degraded transitions, want 0", n)
	}
	if n := b.Reconnects() - reconnects; n != 0 {
		t.Errorf("a consumer's Close counted %d reconnections, want 0", n)
	}
}

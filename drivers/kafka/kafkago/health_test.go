package kafkago_test

import (
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// TestClosingAConsumerLeavesTheClientReady is kf-17.
//
// A consumer the application closes ends its consumer group generation, and
// the end of a generation reported the whole client Degraded ("the client is
// rejoining") whenever the backend itself was open. Nothing rejoins after a
// close, and only another consumer's next generation set the state back, so a
// client whose consumer was closed on purpose read Degraded for the rest of
// its life.
func TestClosingAConsumerLeavesTheClientReady(t *testing.T) {
	topic := newTopic("health-close")
	cl := cluster(t, 1, []string{topic})
	c, b := openPair(t, cl)
	cons := consumerOn(t, c, topic, "health-close-g")
	publish(t, c, topic, &mq.Message{Body: []byte("x")})
	if err := receiveOne(t, cons, 10*time.Second).Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if h := c.Health(); h.State != mq.HealthReady {
		t.Fatalf("Health with the consumer running = %v (%v), want Ready", h.State, h.Err)
	}
	degraded := b.DegradedTransitions()
	if err := cons.Close(); err != nil {
		t.Fatalf("consumer Close: %v", err)
	}
	publish(t, c, topic, &mq.Message{Body: []byte("y")})
	if h := c.Health(); h.State != mq.HealthReady {
		t.Errorf("closing a consumer left the open client %v (%v): nothing is rejoining", h.State, h.Err)
	}
	if n := b.DegradedTransitions() - degraded; n != 0 {
		t.Errorf("closing a consumer counted %d Degraded transitions", n)
	}
}

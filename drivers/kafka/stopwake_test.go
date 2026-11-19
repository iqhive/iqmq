package kafka_test

import (
	"context"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit/nack"
	kq "github.com/iqhive/iqmq/drivers/kafka"
)

// TestAStopNackEndsAWaitingReceive: a stop-the-consumer nack of the last
// record a partition has must end a Receive that is already waiting on the
// broker, with CodePoisonMessage and at once, and the record published after
// the stop must not be handed out.
//
// The review found both broken on e323771: the stop did not wake the waiting
// poll, so Run returned only when another record arrived, and that record was
// handed to the handler first.
func TestAStopNackEndsAWaitingReceive(t *testing.T) {
	c, topic := openClient(t, func(cfg *kq.Config) { cfg.Nack = nack.Stop })
	cons := consumerOn(t, c, topic, "g-stopwake")
	publish(t, c, topic, &mq.Message{Body: []byte("last")})
	d := receiveOne(t, cons, 10*time.Second)

	type result struct {
		n    int
		body string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		dst := make([]mq.Delivery, 1)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		n, err := cons.Receive(ctx, dst)
		r := result{n: n, err: err}
		if n > 0 {
			r.body = string(dst[0].Body)
		}
		got <- r
	}()
	// Nothing is left to hand out, so by now the Receive is waiting on the
	// broker.
	time.Sleep(500 * time.Millisecond)
	stopped := time.Now()
	if err := d.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	var r result
	select {
	case r = <-got:
	case <-time.After(3 * time.Second):
		publish(t, c, topic, &mq.Message{Body: []byte("late")})
		r = <-got
		t.Errorf("the waiting Receive returned %v after the stop, and only once another record was published", time.Since(stopped).Round(10*time.Millisecond))
	}
	if r.n > 0 {
		t.Fatalf("Receive handed out %q after the stop", r.body)
	}
	if mq.CodeOf(r.err) != mq.CodePoisonMessage {
		t.Fatalf("the waiting Receive = %v (code %v), want CodePoisonMessage", r.err, mq.CodeOf(r.err))
	}
}

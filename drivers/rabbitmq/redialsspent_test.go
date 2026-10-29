package rabbitmq_test

import (
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	mq "github.com/iqhive/iqmq"
	rq "github.com/iqhive/iqmq/drivers/rabbitmq"
)

// TestAClientWhoseDialLoopGaveUpDoesNotAskForARetry holds the refusals after
// MaxReconnects to what is true then: nothing will reconnect the client.
//
// A publisher without a channel is refused as retryable while "the driver is
// attaching another", and the driver read that from the connection alone: no
// connection meant a redial was on its way. Once the dial loop had spent
// MaxReconnects it was not, so every Publish was still refused as retryable
// and a caller that retries a retryable refusal spun for ever. Opening a new
// publisher was refused the same way, with the dial loop "retrying with
// backoff".
func TestAClientWhoseDialLoopGaveUpDoesNotAskForARetry(t *testing.T) {
	const maxReconnects = 2
	broker, cl := openFake(t, func(c *rq.Config) {
		c.Declare = true
		c.ReconnectBase, c.ReconnectMax = time.Millisecond, time.Millisecond
		c.MaxReconnects = maxReconnects
	})
	pub := publishOn(t, cl, "orders")
	if err := pub.Publish(t.Context(), &mq.Message{Body: []byte("before")}); err != nil {
		t.Fatal(err)
	}

	before := len(broker.Dials())
	broker.SetDown(true)
	broker.LastConnection().CloseWith(&amqp.Error{Code: amqp.ConnectionForced, Reason: "shutdown", Server: true})
	waitFor(t, 5*time.Second, func() bool { return len(broker.Dials())-before >= maxReconnects })

	// The last dial's failure and the loop giving up are a moment apart, so
	// the refusal is read until it settles or the bound runs out.
	var err error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		err = pub.Publish(t.Context(), &mq.Message{Body: []byte("after")})
		if err != nil && !mq.IsRetryable(err) {
			break
		}
	}
	refusedForGood(t, "a Publish", err)

	_, err = cl.Publisher(t.Context(), mq.PublisherConfig{Topic: "orders"})
	refusedForGood(t, "opening a publisher", err)
	if got := len(broker.Dials()) - before; got != maxReconnects {
		t.Errorf("the client dialled %d times after the connection was lost, want MaxReconnects (%d)", got, maxReconnects)
	}
}

// refusedForGood fails unless err is a non-retryable CodeUnavailable that
// names the spent MaxReconnects.
func refusedForGood(t *testing.T, what string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Fatalf("%s succeeded with no connection and no dial loop", what)
	case mq.IsRetryable(err):
		t.Errorf("%s was refused as retryable after the dial loop gave up: %v", what, err)
	case mq.CodeOf(err) != mq.CodeUnavailable:
		t.Errorf("%s was refused with %v, want CodeUnavailable: %v", what, mq.CodeOf(err), err)
	case !strings.Contains(err.Error(), "MaxReconnects"):
		t.Errorf("%s was refused with %q, which does not say the dial loop gave up", what, err)
	}
}

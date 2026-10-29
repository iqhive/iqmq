package pulsar_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
)

// newStuckRetryRig is a stuck-router rig whose delayed requeues go through
// the retry topic.
func newStuckRetryRig(t *testing.T) (*pd.Backend, *stuckRouterClient) {
	t.Helper()
	return newStuckRouterRig(t, func(c *pd.Config) { c.RetryEnable = true })
}

// requeueAsync runs a delayed requeue and hands back its result.
func requeueAsync(ctx context.Context, src mq.Source, d *mq.DriverDelivery) <-chan error {
	out := make(chan error, 1)
	go func() {
		out <- src.Settler().Nack(ctx, d, mq.NackOptions{Action: mq.NackActionRequeue, Delay: time.Second})
	}()
	return out
}

// TestADelayedRequeueHonoursItsContext.
//
// A delayed requeue through the retry topic is a send on the client's retry
// router, whose channel is as unbuffered as the dead-letter router's, and a
// router that cannot create its producer retries for ever without taking it.
// The Nack has to give up when its context does, and report the outcome as
// unknown, because the router may still republish the message and acknowledge
// the original.
func TestADelayedRequeueHonoursItsContext(t *testing.T) {
	b, _ := newStuckRetryRig(t)
	src := sourceOf(t, b, "stuck", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "stuck", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("later")})
	d := fetchOne(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var err error
	select {
	case err = <-requeueAsync(ctx, src, &d):
	case <-time.After(5 * time.Second):
		t.Fatal("a delayed requeue outlived its context by seconds: the settling goroutine is " +
			"blocked on the client's retry router")
	}
	if !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("an abandoned delayed requeue reported %v (code %v), want ErrSettlementUnknown",
			err, mq.CodeOf(err))
	}
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("an Ack after an abandoned delayed requeue reported %v, want ErrSettlementUnknown: "+
			"the router may still take the message, so the delivery must not be settleable again", err)
	}
}

// TestADelayedRequeueReturnsWhenTheSourceCloses.
//
// A delayed requeue that passed its claim while Close stops the consumer sends
// to a router that has exited and will never take it, and the consumer's
// shutdown waits behind the Nack.
func TestADelayedRequeueReturnsWhenTheSourceCloses(t *testing.T) {
	b, cl := newStuckRetryRig(t)
	src := sourceOf(t, b, "closing", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "closing", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("later")})
	d := fetchOne(t, src)

	result := requeueAsync(context.Background(), src, &d)
	select {
	case <-cl.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the delayed requeue never reached the client")
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, mq.ErrSettlementUnknown) {
			t.Fatalf("a delayed requeue cut off by Close reported %v (code %v), want ErrSettlementUnknown",
				err, mq.CodeOf(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a delayed requeue did not return after its source closed: it is blocked on a " +
			"router that will never take the message")
	}
}

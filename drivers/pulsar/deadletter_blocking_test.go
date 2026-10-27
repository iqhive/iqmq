package pulsar_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/driverkit"
	pd "github.com/iqhive/iqmq/drivers/pulsar"
	"github.com/iqhive/iqmq/drivers/pulsar/internal/pulsarapi"
	"github.com/iqhive/iqmq/drivers/pulsar/pulsarfake"
)

// stuckRouterClient is the in-process model with one change to its consumers:
// ReconsumeLaterWithCustomProperties blocks until released, the way
// pulsar-client-go's does when its dead-letter router is retrying a producer
// it cannot create, or has exited under a concurrent Close.
type stuckRouterClient struct {
	pulsarapi.Client
	entered chan struct{}
	release chan struct{}
}

func (c *stuckRouterClient) Subscribe(o pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	cons, err := c.Client.Subscribe(o)
	if err != nil {
		return nil, err
	}
	return &stuckRouterConsumer{Consumer: cons, c: c}, nil
}

type stuckRouterConsumer struct {
	pulsar.Consumer
	c *stuckRouterClient
}

func (s *stuckRouterConsumer) ReconsumeLaterWithCustomProperties(pulsar.Message, map[string]string, time.Duration) {
	s.c.entered <- struct{}{}
	<-s.c.release
}

// newStuckRouterRig is a rig with a dead-letter topic whose router never
// takes a message until the test ends.
func newStuckRouterRig(t *testing.T) (*pd.Backend, *stuckRouterClient) {
	t.Helper()
	server := pulsarfake.New()
	cl := &stuckRouterClient{
		Client:  server.Client(),
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	cfg := rigConfig(server)
	cfg.DeadLetterTopic = "stuck-dlq"
	cfg.MaxAttempts = 4
	b, err := pd.BackendWithClient(context.Background(), cl, driverkit.ClientOwned, cfg)
	if err != nil {
		t.Fatalf("BackendWithClient: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	// Registered after the backend's, so it runs first and the blocked
	// call returns before anything is closed underneath it.
	t.Cleanup(func() { close(cl.release) })
	return b, cl
}

// rejectAsync runs a dead-letter settlement and hands back its result.
func rejectAsync(ctx context.Context, src mq.Source, d *mq.DriverDelivery) <-chan error {
	out := make(chan error, 1)
	go func() {
		out <- src.Settler().Nack(ctx, d, mq.NackOptions{Action: mq.NackActionDeadLetter})
	}()
	return out
}

// TestADeadLetterRejectHonoursItsContext.
//
// The client's dead-letter route is a send on an unbuffered channel, and a
// router that cannot create its producer retries for ever without taking it.
// A Reject has to give up when its context does, and report the outcome as
// unknown, because the router may still take the message later.
func TestADeadLetterRejectHonoursItsContext(t *testing.T) {
	b, _ := newStuckRouterRig(t)
	src := sourceOf(t, b, "stuck", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "stuck", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("poison")})
	d := fetchOne(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var err error
	select {
	case err = <-rejectAsync(ctx, src, &d):
	case <-time.After(5 * time.Second):
		t.Fatal("a dead-letter Reject outlived its context by seconds: the settling goroutine is " +
			"blocked on the client's dead-letter router")
	}
	if !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("an abandoned dead-letter Reject reported %v (code %v), want ErrSettlementUnknown",
			err, mq.CodeOf(err))
	}
	if err := src.Settler().Ack(context.Background(), &d); !errors.Is(err, mq.ErrSettlementUnknown) {
		t.Fatalf("an Ack after an abandoned dead-letter Reject reported %v, want ErrSettlementUnknown: "+
			"the router may still take the message, so the delivery must not be settleable again", err)
	}
}

// TestADeadLetterRejectReturnsWhenTheSourceCloses.
//
// A Reject that passed its claim while Close stops the consumer sends to a
// router that has exited and will never take it.
func TestADeadLetterRejectReturnsWhenTheSourceCloses(t *testing.T) {
	b, cl := newStuckRouterRig(t)
	src := sourceOf(t, b, "closing", "workers", mq.MetadataNativeStrict, 4)
	snk := sinkOf(t, b, "closing", mq.MetadataNativeStrict)
	publish(t, snk, &mq.Message{Body: []byte("poison")})
	d := fetchOne(t, src)

	result := rejectAsync(context.Background(), src, &d)
	select {
	case <-cl.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the dead-letter Reject never reached the client")
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, mq.ErrSettlementUnknown) {
			t.Fatalf("a dead-letter Reject cut off by Close reported %v (code %v), want ErrSettlementUnknown",
				err, mq.CodeOf(err))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a dead-letter Reject did not return after its source closed: it is blocked on a " +
			"router that will never take the message")
	}
}

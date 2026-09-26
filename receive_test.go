package iqmq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mq "github.com/iqhive/iqmq"
)

// The first Receive parks in the driver, which is what a fetch does when the
// subscription is empty. The second arrives with a context that expires long
// before the first one's, and must come back with that context's error.
func TestReceiveRefusesToOutlastItsContextBehindAnotherReceive(t *testing.T) {
	cl, _ := openFakeWith(t, fakeConfig{}, "orders")
	c := consume(t, cl, mq.ConsumerConfig{})

	parked, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := make(chan struct{})
	go func() {
		defer close(first)
		dst := make([]mq.Delivery, 1)
		_, _ = c.Receive(parked, dst)
	}()
	// The first Receive has to be inside the driver's Fetch before the
	// second one asks for the gate, or the test would be checking nothing.
	waitFor(t, "the first Receive to park in the driver", func() bool {
		return c.Stats().Received == 0 && len(first) == 0
	})
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		dst := make([]mq.Delivery, 1)
		_, err := c.Receive(ctx, dst)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second Receive = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(runWait):
		t.Fatal("the second Receive never returned: it is waiting on a gate that ignores its context")
	}
	cancelFirst()
	<-first
}

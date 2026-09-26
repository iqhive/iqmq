package iqmq_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	mq "github.com/iqhive/iqmq"
	"github.com/iqhive/iqmq/memory"
)

// TestConcurrentPublicationsPairTheirOwnEvents is the pairing token the specification
// asked for: two publications to one topic used to produce two starts and
// two dones that nothing could match, so a hook could measure a duration
// only by assuming the events arrived in pairs — which under concurrency
// they do not.
func TestConcurrentPublicationsPairTheirOwnEvents(t *testing.T) {
	var mu sync.Mutex
	starts := map[uint64]int{}
	dones := map[uint64]int{}
	cl := memoryClientWithHooks(t, mq.Hooks{
		OnPublishStart: func(_ context.Context, e mq.PublishStartEvent) {
			mu.Lock()
			starts[e.Seq]++
			mu.Unlock()
		},
		OnPublishDone: func(_ context.Context, e mq.PublishDoneEvent) {
			mu.Lock()
			dones[e.Seq]++
			mu.Unlock()
		},
	})

	const n = 8
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cl.Publish(context.Background(), "orders", &mq.Message{Body: []byte{byte(i)}}); err != nil {
				t.Errorf("Publish: %v", err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(starts) != n {
		t.Errorf("%d publications produced %d distinct start tokens, want %d: without a token the events cannot "+
			"be paired at all", n, len(starts), n)
	}
	for seq, count := range starts {
		if count != 1 {
			t.Errorf("start token %d appeared %d times, want once", seq, count)
		}
		if dones[seq] != 1 {
			t.Errorf("start token %d has %d matching done events, want exactly 1: a token that pairs nothing "+
				"is no better than no token", seq, dones[seq])
		}
	}
	if len(dones) != n {
		t.Errorf("%d publications produced %d distinct done tokens, want %d", n, len(dones), n)
	}
	if _, zero := dones[0]; zero {
		t.Errorf("a done event carried token 0, which is reserved for a publication that never reached a driver")
	}
}

// TestAPublicationThatNeverReachedADriverIsReported is the other half of a publication that fails because its sink cannot be opened reached
// no hook, so the failure count was blind to the failure a misconfigured
// deployment meets first.
func TestAPublicationThatNeverReachedADriverIsReported(t *testing.T) {
	want := errors.New("the sink cannot be opened")
	var got []mq.PublishDoneEvent
	var mu sync.Mutex
	cl := mq.NewClient(newFakeBroker(fakeConfig{openSinkErr: want}, "orders"),
		mq.WithHooks(mq.Hooks{
			OnPublishDone: func(_ context.Context, e mq.PublishDoneEvent) {
				mu.Lock()
				got = append(got, e)
				mu.Unlock()
			},
		}))
	t.Cleanup(func() { _ = cl.Close() })

	err := cl.Publish(context.Background(), "orders", &mq.Message{Body: []byte("one")})
	if err == nil {
		t.Fatal("Publish succeeded with a sink that cannot be opened")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("the failed publication produced %d done event(s), want 1: a publication that never reached a "+
			"driver is still a publication that failed, and the failure count is what reads these", len(got))
	}
	e := got[0]
	if e.Err == nil || !errors.Is(e.Err, want) {
		t.Errorf("the done event carries Err %v, want the sink's own error %v", e.Err, want)
	}
	if e.Topic != "orders" {
		t.Errorf("the done event names topic %q, want \"orders\"", e.Topic)
	}
	if e.Seq != 0 {
		t.Errorf("the done event carries token %d, want 0: there was no start event to pair with", e.Seq)
	}
}

// memoryClientWithHooks is a memory-backed client with hooks installed.
func memoryClientWithHooks(t *testing.T, h mq.Hooks) *mq.Client {
	t.Helper()
	b, err := memory.NewBroker(memory.Config{AutoCreate: true})
	if err != nil {
		t.Fatalf("building the memory broker: %v", err)
	}
	cl := mq.NewClient(b, mq.WithHooks(h))
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}
